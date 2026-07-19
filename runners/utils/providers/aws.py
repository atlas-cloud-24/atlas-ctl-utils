"""AWS provider adapter for the Atlas ctl engine.

Owns every AWS-specific concept: identity schema, provider catalogs
(providers.aws.*), profile/STS credential acquisition, the ctl role chain,
target_run runtime binding and assertion metadata, the S3 ctl-state syncer, and
derived provider facts. Engine-core modules never import AWS vocabulary; they
dispatch through utils.providers.get_adapter().
"""

import configparser
import functools
import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

import yaml

from utils import common


def _registry_account_id(account_registry: dict[str, str] | None, account_key: str, *, label: str) -> str:
    if account_registry is None:
        raise RuntimeError(f"❌ AWS account registry is required for {label}")
    account_id = account_registry.get(account_key)
    if account_id is None:
        raise RuntimeError(f"❌ AWS account registry has no key {account_key!r} ({label})")
    return account_id

AWS_CREDENTIAL_ENV_VARS = (
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
)


AWS_ACCESS_TARGET_ENV_VARS = (
    "ATLAS_AWS_ASSERT_ACCESS",
    "ATLAS_AWS_PROFILE_ONLY_ACCESS",
    "ATLAS_AWS_ROLE_CHAIN",
    "ATLAS_EXECUTION_IDENTITY_KEY",
    "ATLAS_AWS_ACCOUNT_KEY",
    "ATLAS_AWS_CREDENTIAL_SOURCE_KEY",
    "ATLAS_AWS_IMPLEMENTATION_KEY",
    "ATLAS_AWS_EXPECT_ACCOUNT_ID",
    "ATLAS_AWS_EXPECT_PERMISSION_SET_NAME",
    "ATLAS_AWS_EXPECT_ROLE_NAME",
)


def normalize_optional_aws_profile(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("❌ --aws-profile must be a non-empty profile name when provided")
    return value.strip()


# The aws implementation owns its catalog schema; the engine core knows no
# provider names or sections.
_AWS_PROVIDER_CATALOG_SECTIONS = {
    "credential_sources",
    "accounts",
    "target_roles",
    "ctl_state_roles",
    "ctl_role_chain",
}


def _load_aws_provider_catalog(ctl_cfg_root: Path) -> dict:
    catalog = common.load_provider_catalogs(ctl_cfg_root).get("aws", {})
    unknown = sorted(set(catalog) - _AWS_PROVIDER_CATALOG_SECTIONS)
    if unknown:
        raise RuntimeError(f"❌ providers.aws has unknown sections {unknown}: {ctl_cfg_root}")
    return catalog


def load_aws_credential_sources_cfg(ctl_cfg_root: Path) -> dict:
    """Load logical AWS credential sources and runner-specific implementations."""
    credential_sources = _load_aws_provider_catalog(ctl_cfg_root).get("credential_sources", {})

    for credential_source_key, credential_source_cfg in credential_sources.items():
        if not isinstance(credential_source_key, str) or not credential_source_key.strip():
            raise RuntimeError(f"❌ AWS credential-source keys must be non-empty strings: {ctl_cfg_root}")
        if not isinstance(credential_source_cfg, dict) or not credential_source_cfg:
            raise RuntimeError(
                f"❌ AWS credential source {credential_source_key!r} must be a non-empty mapping: {ctl_cfg_root}"
            )
        for implementation_key, implementation_cfg in credential_source_cfg.items():
            _validate_aws_credential_source_implementation(
                credential_source_key,
                implementation_key,
                implementation_cfg,
                ctl_cfg_root,
            )

    return credential_sources


def load_aws_account_registry_cfg(
    ctl_cfg_root: Path,
    *,
    execution_context: dict[str, object] | None = None,
    strict_selected: bool = True,
) -> dict[str, str]:
    """Load account_key -> account_id. Selector-membered logical keys are
    resolved exactly once from the frozen execution context. With no context,
    the function performs structural validation and returns static entries only.

    With `strict_selected=False` a resolved account id that is not 12 digits (a
    placeholder) is returned as-is instead of raising — the 12-digit check is
    then the caller's concern (stage-1 cfg validation / per-target resolution),
    not the whole catalog load's.
    """
    accounts = _load_aws_provider_catalog(ctl_cfg_root).get("accounts", {})
    registry: dict[str, str] = {}

    def validate_account_id(value: object, label: str, *, selected: bool) -> str:
        account_id = common._require_non_empty_string(value, label, ctl_cfg_root)
        if re.fullmatch(r"\d{12}", account_id):
            return account_id
        placeholder = re.fullmatch(r"<[^<>]+-account-id>", account_id)
        if placeholder and (not selected or not strict_selected):
            return account_id
        raise RuntimeError(f"❌ {label} must be a 12-digit account id")

    for account_key, account_cfg in accounts.items():
        if not isinstance(account_key, str) or not account_key.strip():
            raise RuntimeError(f"❌ aws account keys must be non-empty strings: {ctl_cfg_root}")
        if not isinstance(account_cfg, dict):
            raise RuntimeError(f"❌ aws account {account_key!r} must be a mapping: {ctl_cfg_root}")
        unknown = sorted(set(account_cfg) - {"account_id", "members"})
        if unknown:
            raise RuntimeError(f"❌ aws account {account_key!r} has unknown fields {unknown}: {ctl_cfg_root}")
        has_static = "account_id" in account_cfg
        has_members = "members" in account_cfg
        if has_static == has_members:
            raise RuntimeError(
                f"❌ aws account {account_key!r} must declare exactly one of account_id or members"
            )
        if has_static:
            registry[account_key] = validate_account_id(
                account_cfg.get("account_id"),
                f"providers.aws.accounts.{account_key}.account_id",
                selected=True,
            )
            continue

        members = account_cfg.get("members")
        if not isinstance(members, list) or not members:
            raise RuntimeError(
                f"❌ providers.aws.accounts.{account_key}.members must be a non-empty list"
            )
        matches: list[str] = []
        branch_account_ids: list[str] = []
        for index, member in enumerate(members):
            label = f"providers.aws.accounts.{account_key}.members[{index}]"
            if not isinstance(member, dict):
                raise RuntimeError(f"❌ {label} must be a mapping")
            unknown_member = sorted(set(member) - {"selectors", "account_id"})
            if unknown_member:
                raise RuntimeError(f"❌ {label} has unknown fields {unknown_member}")
            selectors = member.get("selectors")
            common.selector_requirements(
                selectors, label=f"{label}.selectors", structured_only=True
            )
            account_id = validate_account_id(
                member.get("account_id"), f"{label}.account_id", selected=False
            )
            branch_account_ids.append(account_id)
            if execution_context is not None and common.selector_matches(
                selectors, execution_context, label=f"{label}.selectors", structured_only=True
            ):
                matches.append(account_id)
        duplicates = sorted({
            account_id
            for account_id in branch_account_ids
            if branch_account_ids.count(account_id) > 1
        })
        if duplicates:
            raise RuntimeError(
                f"❌ providers.aws.accounts.{account_key}.members resolve duplicate "
                f"physical account ids across selector branches: {duplicates}"
            )
        if execution_context is None:
            continue
        if len(matches) != 1:
            raise RuntimeError(
                f"❌ providers.aws.accounts.{account_key} must resolve exactly one member; "
                f"matched {len(matches)}"
            )
        registry[account_key] = validate_account_id(
            matches[0], f"providers.aws.accounts.{account_key}.resolved.account_id", selected=True
        )
    return registry


def collect_provider_cfg_findings(
    ctl_cfg_root: Path, *, execution_context: dict[str, object]
) -> list[dict]:
    """Stage-1 provider cfg well-formedness, collected (never raised) and keyed by
    cfg path. Runs once for the whole run: the account registry is resolved for
    the frozen context, and every resolved account id is checked for 12-digit
    shape. A placeholder/malformed id is a finding here, not a per-target error.
    """
    findings: list[dict] = []
    try:
        registry = load_aws_account_registry_cfg(
            ctl_cfg_root,
            execution_context=execution_context,
            strict_selected=False,
        )
    except Exception as error:
        # A structural defect (schema, unresolved selector) blocks the whole
        # registry — one finding under the accounts cfg path.
        return [
            {
                "cfg_path": "providers.aws.accounts",
                "status": "failed",
                "error": common.credential_free_preflight_failure_reason(error),
            }
        ]
    for account_key, account_id in sorted(registry.items()):
        cfg_path = f"providers.aws.accounts.{account_key}.account_id"
        if re.fullmatch(r"\d{12}", account_id):
            findings.append({"cfg_path": cfg_path, "status": "passed"})
        else:
            findings.append(
                {
                    "cfg_path": cfg_path,
                    "status": "failed",
                    "error": f"{cfg_path} must be a 12-digit account id",
                }
            )
    return findings


def _validate_aws_role_registry(section: str, entries, ctl_cfg_root: Path, *, allow_account_key: bool) -> dict:
    """Validate a role registry section: {key: {role_name, [account_key]}}."""
    if entries is None:
        entries = {}
    if not isinstance(entries, dict):
        raise RuntimeError(f"❌ providers.aws.{section} must be a mapping: {ctl_cfg_root}")
    allowed = {"role_name", "account_key"} if allow_account_key else {"role_name"}
    for role_key, role_cfg in entries.items():
        if not isinstance(role_key, str) or not role_key.strip():
            raise RuntimeError(f"❌ providers.aws.{section} keys must be non-empty strings: {ctl_cfg_root}")
        if not isinstance(role_cfg, dict):
            raise RuntimeError(f"❌ providers.aws.{section}.{role_key} must be a mapping: {ctl_cfg_root}")
        unknown = sorted(set(role_cfg) - allowed)
        if unknown:
            raise RuntimeError(f"❌ providers.aws.{section}.{role_key} has unknown fields {unknown}: {ctl_cfg_root}")
        common._require_non_empty_string(
            role_cfg.get("role_name"), f"providers.aws.{section}.{role_key}.role_name", ctl_cfg_root
        )
        if "account_key" in role_cfg:
            common._require_non_empty_string(
                role_cfg.get("account_key"), f"providers.aws.{section}.{role_key}.account_key", ctl_cfg_root
            )
    return entries


def load_aws_target_roles_cfg(ctl_cfg_root: Path) -> dict:
    """Roles the engine assumes for target_run execution (chain mode): account-agnostic
    fan-out roles (no account_key — the identity's account supplies it) plus
    account-bound roles (e.g. the runner, the management StackSet-admin role)."""
    return _validate_aws_role_registry(
        "target_roles",
        _load_aws_provider_catalog(ctl_cfg_root).get("target_roles", {}),
        ctl_cfg_root,
        allow_account_key=True,
    )


def load_aws_ctl_state_roles_cfg(ctl_cfg_root: Path) -> dict:
    """Roles the engine assumes for ctl-state sync (chain mode): account-agnostic
    constant names; the backend's account supplies the ARN account."""
    return _validate_aws_role_registry(
        "ctl_state_roles",
        _load_aws_provider_catalog(ctl_cfg_root).get("ctl_state_roles", {}),
        ctl_cfg_root,
        allow_account_key=False,
    )


def load_aws_ctl_role_chain_cfg(ctl_cfg_root: Path) -> dict | None:
    """The ONE ctl role chain (singleton by design; §Skip Model D): entry credential
    source, runner role, and the default target_run role. Absent = chain mode is not
    configured; runs must use direct or bypass execution access."""
    chain = _load_aws_provider_catalog(ctl_cfg_root).get("ctl_role_chain")
    if chain is None:
        return None
    if not isinstance(chain, dict):
        raise RuntimeError(f"❌ providers.aws.ctl_role_chain must be a mapping: {ctl_cfg_root}")
    # Phase 15: no universal target_run-role fallback. Every target-target_run identity
    # names its own ctl_target_role_key (authorization class); the chain declares
    # only the entry source and the runner hub.
    if "target_role_key" in chain:
        raise RuntimeError(
            "❌ providers.aws.ctl_role_chain.target_role_key is removed (Phase 15); "
            "each execution identity must declare its own ctl_target_role_key"
        )
    required = ("entry_credential_source_key", "runner_role_key")
    unknown = sorted(set(chain) - set(required))
    if unknown:
        raise RuntimeError(f"❌ providers.aws.ctl_role_chain has unknown fields {unknown}: {ctl_cfg_root}")
    for field in required:
        common._require_non_empty_string(chain.get(field), f"providers.aws.ctl_role_chain.{field}", ctl_cfg_root)
    return chain


def _validate_aws_credential_source_implementation(
    credential_source_key: str,
    implementation_key: str,
    implementation_cfg: dict,
    path: Path,
) -> None:
    if not isinstance(implementation_key, str) or not implementation_key.strip():
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r} implementation keys must be non-empty strings: {path}"
        )
    if not isinstance(implementation_cfg, dict):
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key} must be a mapping: {path}"
        )

    credential_keys = [
        key for key in ("profile_name", "iam_role_key")
        if key in implementation_cfg
    ]
    if len(credential_keys) != 1:
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key} must define exactly one of "
            f"profile_name or iam_role_key: {path}"
        )
    credential_key = credential_keys[0]
    common._require_non_empty_string(
        implementation_cfg[credential_key],
        f"AWS credential source {credential_source_key!r}.{implementation_key}.{credential_key}",
        path,
    )

    if implementation_key == "local" and credential_key != "profile_name":
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.local must use profile_name: {path}"
        )
    if implementation_key == "ci" and credential_key != "iam_role_key":
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.ci must use iam_role_key: {path}"
        )

    expect_cfg = implementation_cfg.get("expect")
    if credential_key == "profile_name":
        # Phase 14: EVERY local credential source declares its real principal.
        # An account check proves where credentials landed; the principal
        # expectation proves which permission set / role produced them.
        if expect_cfg is None:
            raise RuntimeError(
                f"❌ AWS credential source {credential_source_key!r}.{implementation_key} must declare "
                f"expect (exactly one of permission_set_name or role_name): {path}"
            )
        _validate_profile_expect(credential_source_key, implementation_key, expect_cfg, path)
    elif expect_cfg is not None:
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key} must not duplicate expect "
            f"beside {credential_key}: {path}"
        )

    unknown = sorted(set(implementation_cfg) - {credential_key, "expect"})
    if unknown:
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key} has unknown fields {unknown}: {path}"
        )


def _validate_profile_expect(
    credential_source_key: str,
    implementation_key: str,
    expect_cfg,
    path: Path,
) -> None:
    if not isinstance(expect_cfg, dict):
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key}.expect must be a mapping "
            "when present"
        )
    principal_keys = [key for key in ("permission_set_name", "role_name") if key in expect_cfg]
    if len(principal_keys) != 1:
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key}.expect must define exactly one "
            f"of permission_set_name or role_name: {path}"
        )
    common._require_non_empty_string(
        expect_cfg[principal_keys[0]],
        f"AWS credential source {credential_source_key!r}.{implementation_key}.expect.{principal_keys[0]}",
        path,
    )
    if "account_id" in expect_cfg:
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key}.expect.account_id is deprecated; "
            "put account IDs in providers.aws.accounts keyed by execution identity account_key"
        )
    if "account_key" in expect_cfg:
        common._require_non_empty_string(
            expect_cfg["account_key"],
            f"AWS credential source {credential_source_key!r}.{implementation_key}.expect.account_key",
            path,
        )
    unknown = sorted(set(expect_cfg) - set(principal_keys) - {"account_key"})
    if unknown:
        raise RuntimeError(
            f"❌ AWS credential source {credential_source_key!r}.{implementation_key}.expect has unknown fields "
            f"{unknown}: {path}"
        )


def aws_credential_source_override_env_name(credential_source_key: str) -> str:
    suffix = re.sub(r"[^A-Za-z0-9]", "_", credential_source_key).upper()
    return f"ATLAS_AWS_PROFILE_{suffix}"


def _read_aws_profile_setting(profile_name: str, setting: str) -> str | None:
    aws_env = os.environ.copy()
    aws_env.pop("AWS_CONFIG_FILE", None)
    aws_env.pop("AWS_SHARED_CREDENTIALS_FILE", None)
    try:
        result = subprocess.run(
            ["aws", "configure", "get", setting, "--profile", profile_name],
            text=True,
            capture_output=True,
            check=False,
            env=aws_env,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("❌ AWS CLI is required for local AWS access resolution") from exc
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


@functools.lru_cache(maxsize=None)
def resolve_configured_profile_account_id(profile_name: str) -> str:
    account_id = _read_aws_profile_setting(profile_name, "sso_account_id")
    if account_id:
        if not re.fullmatch(r"\d{12}", account_id):
            raise RuntimeError(
                f"❌ AWS profile {profile_name!r} has invalid sso_account_id {account_id!r}"
            )
        return account_id

    role_arn = _read_aws_profile_setting(profile_name, "role_arn")
    if role_arn:
        match = re.fullmatch(r"arn:[^:]+:iam::(\d{12}):role/.+", role_arn)
        if not match:
            raise RuntimeError(f"❌ AWS profile {profile_name!r} has invalid role_arn {role_arn!r}")
        return match.group(1)

    raise RuntimeError(
        f"❌ Cannot derive an AWS account ID from canonical profile {profile_name!r}; "
        "configure sso_account_id or role_arn in ~/.aws/config"
    )


_ASSERTION_MODULE = None


def _assertion():
    """ONE assertion implementation for entry, synchronizer, and target_run checks:
    the target_run-side assert_aws_access module (exact assumed-role ARN parsing,
    anchored AWSReservedSSO matching)."""
    global _ASSERTION_MODULE
    if _ASSERTION_MODULE is None:
        import importlib.util

        script = Path(__file__).resolve().parents[3] / "step_utils" / "assert_aws_access.py"
        spec = importlib.util.spec_from_file_location("atlas_assert_aws_access", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _ASSERTION_MODULE = module
    return _ASSERTION_MODULE


def _resolve_expect_principal(expect_cfg: dict, context: dict, *, label: str) -> dict[str, str]:
    """Resolve the declared principal expectation ({permission_set_name|role_name})."""
    resolved: dict[str, str] = {}
    for field in ("permission_set_name", "role_name"):
        if field in expect_cfg:
            resolved[field] = common.resolve_runtime_scalar(
                expect_cfg[field], context, label=f"{label}.expect.{field}"
            )
    return resolved


def assert_profile_caller(
    profile_name: str,
    *,
    expected_account_id: str,
    expect_principal: dict[str, str],
    label: str,
) -> dict:
    """Engine-side caller assertion (shared implementation with the target_run check)."""
    module = _assertion()
    caller = module.get_caller_identity(profile_name)
    try:
        module.validate_caller_identity(
            caller,
            expected_account_id=expected_account_id,
            expected_permission_set_name=expect_principal.get("permission_set_name"),
            expected_role_name=expect_principal.get("role_name"),
        )
    except RuntimeError as error:
        raise RuntimeError(f"❌ {label}: {error}") from error
    return caller


def validate_credential_path(hop_role_arns: list[str]) -> None:
    """§12.3: validate the ordered AWS credential path (implementation type, not
    cfg). Reject an empty path or repeated/cyclic role ARNs. The executor makes
    no assumption about the NUMBER of hops (production resolves exactly two:
    runner + final role; unit tests exercise one/two/three)."""
    if not hop_role_arns:
        raise RuntimeError("❌ AWS credential path has no role hops")
    seen: set[str] = set()
    for arn in hop_role_arns:
        if not isinstance(arn, str) or not arn.strip():
            raise RuntimeError("❌ AWS credential path hop role ARN must be a non-empty string")
        if arn in seen:
            raise RuntimeError(f"❌ AWS credential path repeats a role ARN (cyclic): {arn}")
        seen.add(arn)


def _run_aws_json(
    cmd: list[str], env_extra: dict[str, str] | None = None
) -> dict:
    env = os.environ.copy()
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    if env_extra:
        env.update(env_extra)
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        operation = " ".join(cmd[:4])
        raise RuntimeError(f"❌ {operation} failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def _assume_role_credentials(
    role_arn: str,
    *,
    session_name: str,
    entry_profile_name: str,
    env_extra: dict[str, str] | None,
    use_profile: bool,
    session_policy: dict | None = None,
) -> tuple[dict[str, str], dict]:
    cmd = [
        "aws",
        "sts",
        "assume-role",
        "--output",
        "json",
        "--role-arn",
        role_arn,
        "--role-session-name",
        session_name,
    ]
    if use_profile:
        cmd += ["--profile", entry_profile_name]
    if session_policy is not None:
        cmd += ["--policy", json.dumps(session_policy, separators=(",", ":"))]
    response = _run_aws_json(cmd, env_extra)
    raw = response["Credentials"]
    credentials = {
        "AWS_ACCESS_KEY_ID": raw["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": raw["SecretAccessKey"],
        "AWS_SESSION_TOKEN": raw["SessionToken"],
    }
    return credentials, response.get("AssumedRoleUser") or {}


def assume_ctl_role_chain(
    entry_profile_name: str,
    hop_role_arns: list[str],
    *,
    session_name: str,
    entry_expected_account_id: str,
    entry_permission_set_name: str | None = None,
    entry_role_name: str | None = None,
    final_session_policy: dict | None = None,
) -> dict[str, str]:
    """Execute and validate an arbitrary-length entry-profile role path."""
    validate_credential_path(hop_role_arns)

    caller = _run_aws_json(
        [
            "aws",
            "sts",
            "get-caller-identity",
            "--output",
            "json",
            "--profile",
            entry_profile_name,
        ]
    )
    try:
        _assertion().validate_caller_identity(
            caller,
            expected_account_id=entry_expected_account_id,
            expected_permission_set_name=entry_permission_set_name,
            expected_role_name=entry_role_name,
        )
    except RuntimeError as error:
        raise RuntimeError(
            f"❌ entry profile {entry_profile_name!r}: {error}"
        ) from error

    credentials: dict[str, str] = {}
    for index, role_arn in enumerate(hop_role_arns):
        credentials, _ = _assume_role_credentials(
            role_arn,
            session_name=session_name,
            entry_profile_name=entry_profile_name,
            env_extra=credentials or None,
            use_profile=index == 0,
            session_policy=(final_session_policy if index == len(hop_role_arns) - 1 else None),
        )
    return credentials


def resolve_target_aws_access(
    target_run: dict,
    execution_identities: dict,
    aws_credential_sources: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str] | None = None,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    ctl_role_chain: dict | None = None,
    target_roles: dict | None = None,
    ctl_state_roles: dict | None = None,
    validate_local_credential: bool = True,
    require_valid_account_id: bool = True,
) -> dict | None:
    """Resolve one target_run's AWS access per §Skip Model D.

    Three modes (§12): bypass (--execution-access-mode force_bypass + substitute
    credential; nothing checked), direct (--execution-access-mode agreed_direct; the identity's
    direct_credential_source_key profile + account and principal checks), and
    chain (default; entry profile -> runner role -> target_run role, entry expect
    checked, target_run asserts account + target_run-role principal).
    """
    for legacy_field in ("aws_account_key", "aws_access_context_key"):
        if legacy_field in target_run:
            raise RuntimeError(f"❌ target_run uses deprecated {legacy_field}; use execution_identity_key")

    if execution_access_mode == "force_bypass":
        profile_name = (provider_credential or "").strip()
        if not profile_name:
            raise RuntimeError("❌ bypass execution access requires the --provider-credential substitute credential")
        return {
            "provider": "aws",
            "execution_identity_key": "substitute",
            "implementation_key": "substitute",
            "credential_provider_kind": "substitute_credential",
            "profile_name": profile_name,
            "identity_bypass": "true",
        }

    identity_key = target_run.get("execution_identity_key")
    if identity_key is None:
        # Coverage is validated by common.validate_execution_identity_coverage; a lone
        # resolve call for an identity-less target_run has nothing to resolve.
        return None
    if not isinstance(identity_key, str) or not identity_key.strip():
        raise RuntimeError("❌ target_run execution_identity_key must be a non-empty string")
    identity_key = identity_key.strip()

    # a group execution_identity_key resolves to its one concrete member for this
    # run's context (§Phase 10); a concrete key returns itself
    identity_key, identity_cfg = common.resolve_execution_identity_entry(
        execution_identities, identity_key, execution_context
    )
    if not isinstance(identity_cfg, dict):
        raise RuntimeError(
            f"❌ target_run execution_identity_key {identity_key!r} is not defined in execution_identities.yaml"
        )
    provider = identity_cfg.get("provider")
    runtime_provider = execution_context.get("execution_context.params.provider")
    if runtime_provider is not None and str(runtime_provider) != provider:
        raise RuntimeError(
            f"❌ execution identity {identity_key!r} provider {provider!r} does not match "
            f"runtime provider {runtime_provider!r}"
        )
    if provider != "aws":
        raise RuntimeError(
            f"❌ execution identity {identity_key!r} provider {provider!r} is not implemented by this runner"
        )

    context = dict(execution_context)
    account_key = common.resolve_runtime_scalar(
        identity_cfg.get("account_key"),
        context,
        label=f"execution_identities.{identity_key}.account_key",
    )
    # Identity-field interpolation (Phase 8): credential-source values may carry
    # the resolving identity's account facet.
    context["identity.account_key"] = account_key
    expected_account_id = _registry_account_id(
        account_registry, account_key, label=f"execution identity {identity_key!r}"
    )
    if require_valid_account_id and not re.fullmatch(r"\d{12}", expected_account_id):
        # The account resolves in cfg but its id is a placeholder/malformed — a
        # live check cannot proceed. Blocked (not a genuine identity failure);
        # the exact cfg defect is reported per target.
        raise common.ProviderConfigBlockedError(
            f"providers.aws.accounts.{account_key}.account_id must be a 12-digit account id"
        )

    resolved: dict[str, str] = {
        "provider": "aws",
        "execution_identity_key": identity_key,
        "account_key": account_key,
        "implementation_key": implementation_key,
        "expected_account_id": expected_account_id,
    }

    if execution_access_mode == "agreed_direct":
        # Direct mode (Phase 14): both independent facts are validated — the
        # destination account (identity account_key -> registry) AND the actual
        # principal (the credential source's declared expect).
        direct_source_key = identity_cfg.get("direct_credential_source_key")
        if not direct_source_key:
            raise RuntimeError(
                f"❌ execution identity {identity_key!r} declares no direct_credential_source_key; "
                "it cannot run with direct execution access"
            )
        credential_source_cfg = aws_credential_sources.get(direct_source_key)
        if not isinstance(credential_source_cfg, dict):
            raise RuntimeError(
                f"❌ AWS credential source {direct_source_key!r} is not defined in the providers.aws.credential_sources catalog"
            )
        implementation_cfg = credential_source_cfg.get(implementation_key)
        if not isinstance(implementation_cfg, dict):
            raise RuntimeError(
                f"❌ AWS credential source {direct_source_key!r} has no {implementation_key!r} implementation"
            )
        if "profile_name" not in implementation_cfg:
            raise RuntimeError(
                f"❌ AWS credential source {direct_source_key!r}.{implementation_key} has no profile_name; "
                "direct mode requires a profile binding"
            )
        canonical_profile_name = common.resolve_runtime_scalar(
            implementation_cfg["profile_name"],
            context,
            label=f"providers.aws.credential_sources.{direct_source_key}.{implementation_key}.profile_name",
        )
        override_name = aws_credential_source_override_env_name(direct_source_key)
        selected_profile_name = os.getenv(override_name, "").strip() or canonical_profile_name
        if validate_local_credential:
            canonical_account_id = resolve_configured_profile_account_id(canonical_profile_name)
            if expected_account_id != canonical_account_id:
                raise RuntimeError(
                    f"❌ AWS account registry maps {account_key!r} to {expected_account_id}, but canonical "
                    f"profile {canonical_profile_name!r} resolves to {canonical_account_id}"
                )
            selected_account_id = resolve_configured_profile_account_id(selected_profile_name)
            if selected_account_id != expected_account_id:
                raise RuntimeError(
                    f"❌ AWS profile override {selected_profile_name!r} resolves to account {selected_account_id}, "
                    f"but canonical profile {canonical_profile_name!r} resolves to {expected_account_id}"
                )
        direct_expect = implementation_cfg.get("expect") or {}
        direct_expect_account_key = direct_expect.get("account_key")
        if direct_expect_account_key:
            declared_account_id = _registry_account_id(
                account_registry, str(direct_expect_account_key).strip(),
                label=f"credential source {direct_source_key!r} expect.account_key",
            )
            if declared_account_id != expected_account_id:
                raise RuntimeError(
                    f"❌ credential source {direct_source_key!r} expect.account_key resolves to "
                    f"{declared_account_id}, but execution identity {identity_key!r} expects {expected_account_id}"
                )
        direct_principal = _resolve_expect_principal(
            direct_expect, context,
            label=f"providers.aws.credential_sources.{direct_source_key}.{implementation_key}",
        )
        resolved.update(direct_principal)
        resolved["credential_source_key"] = direct_source_key
        resolved["credential_provider_kind"] = "direct_profile"
        resolved["profile_name"] = selected_profile_name
        return resolved

    # Chain mode (steady state).
    if ctl_role_chain is None:
        raise RuntimeError(
            "❌ steady-state runs require providers.aws.ctl_role_chain; define it, or run "
            "with --execution-access-mode agreed_direct (bootstrap) or --execution-access-mode force_bypass"
        )
    target_roles = target_roles or {}
    entry_source_key = ctl_role_chain["entry_credential_source_key"]
    entry_source_cfg = aws_credential_sources.get(entry_source_key)
    if not isinstance(entry_source_cfg, dict):
        raise RuntimeError(
            f"❌ ctl_role_chain entry credential source {entry_source_key!r} is not defined"
        )
    entry_implementation_cfg = entry_source_cfg.get(implementation_key)
    if not isinstance(entry_implementation_cfg, dict) or "profile_name" not in entry_implementation_cfg:
        raise RuntimeError(
            f"❌ ctl_role_chain entry credential source {entry_source_key!r} has no "
            f"{implementation_key!r} profile binding"
        )
    entry_profile_name = common.resolve_runtime_scalar(
        entry_implementation_cfg["profile_name"],
        context,
        label=f"providers.aws.credential_sources.{entry_source_key}.{implementation_key}.profile_name",
    )
    entry_expect = entry_implementation_cfg.get("expect") or {}
    # Phase 14: the entry account cannot be inferred from the target identity —
    # entry and target may be different accounts. Account-bound entry sources
    # declare expect.account_key, resolved through the registry.
    entry_account_key = entry_expect.get("account_key")
    if not entry_account_key:
        raise RuntimeError(
            f"❌ ctl_role_chain entry credential source {entry_source_key!r} must declare "
            "expect.account_key (the entry account is not the target account)"
        )
    resolved["entry_account_id"] = _registry_account_id(
        account_registry, str(entry_account_key).strip(),
        label=f"entry credential source {entry_source_key!r}",
    )
    entry_principal = _resolve_expect_principal(
        entry_expect, context,
        label=f"providers.aws.credential_sources.{entry_source_key}.{implementation_key}",
    )
    for expect_field, resolved_key in (
        ("permission_set_name", "entry_permission_set_name"),
        ("role_name", "entry_role_name"),
    ):
        if expect_field in entry_principal:
            resolved[resolved_key] = entry_principal[expect_field]

    def _chain_role_arn(role_key: str, *, label: str, default_account_key: str) -> tuple[str, str]:
        role_cfg = target_roles.get(role_key)
        if not isinstance(role_cfg, dict):
            raise RuntimeError(f"❌ providers.aws.target_roles has no key {role_key!r} ({label})")
        role_name = common.resolve_runtime_scalar(
            role_cfg["role_name"], context, label=f"providers.aws.target_roles.{role_key}.role_name"
        )
        role_account_key = role_cfg.get("account_key", default_account_key)
        role_account_id = _registry_account_id(account_registry, role_account_key, label=label)
        return role_name, f"arn:aws:iam::{role_account_id}:role/{role_name}"

    _, runner_role_arn = _chain_role_arn(
        ctl_role_chain["runner_role_key"], label="ctl_role_chain.runner_role_key",
        default_account_key=account_key,
    )
    # The final authorization class is explicit and belongs to exactly one
    # catalog: target execution or ctl-state operation access.
    target_role_key = identity_cfg.get("ctl_target_role_key")
    state_role_key = identity_cfg.get("ctl_state_role_key")
    if bool(target_role_key) == bool(state_role_key):
        raise RuntimeError(
            f"❌ execution identity {identity_key!r} must declare exactly one of "
            "ctl_target_role_key or ctl_state_role_key for standard access"
        )
    if target_role_key:
        final_role_key = target_role_key
        final_role_name, final_role_arn = _chain_role_arn(
            target_role_key, label=f"execution identity {identity_key!r} target role",
            default_account_key=account_key,
        )
        resolved["target_role_key"] = target_role_key
        resolved["final_role_kind"] = "target"
    else:
        ctl_state_roles = ctl_state_roles or {}
        role_cfg = ctl_state_roles.get(state_role_key)
        if not isinstance(role_cfg, dict):
            raise RuntimeError(
                f"❌ providers.aws.ctl_state_roles has no key {state_role_key!r} "
                f"(execution identity {identity_key!r})"
            )
        final_role_key = state_role_key
        final_role_name = common.resolve_runtime_scalar(
            role_cfg["role_name"], context,
            label=f"providers.aws.ctl_state_roles.{state_role_key}.role_name",
        )
        final_role_arn = f"arn:aws:iam::{expected_account_id}:role/{final_role_name}"
        resolved["ctl_state_role_key"] = state_role_key
        resolved["final_role_kind"] = "ctl_state"

    resolved["credential_source_key"] = entry_source_key
    resolved["credential_provider_kind"] = "role_chain"
    resolved["entry_profile_name"] = entry_profile_name
    # Ordered credential path (runner, then final role); the executor makes no
    # assumption about path length.
    resolved["hop_role_arns"] = [runner_role_arn, final_role_arn]
    resolved["final_role_key"] = final_role_key
    resolved["role_name"] = final_role_name
    validate_credential_path(resolved["hop_role_arns"])
    return resolved



_ROLE_ARN_PATTERN = re.compile(
    r"^arn:[^:]+:iam::(?P<account_id>[0-9]{12}):role/(?P<role_name>.+)$"
)


def _preflight_failure_reason(error: BaseException) -> str:
    detail = " ".join(str(error).split())
    detail = re.sub(
        r"(?i)((?:access[ _-]?key|secret|token|password)\s*[:=]\s*)\S+",
        r"\1<redacted>",
        detail,
    )
    # report statuses carry the ❌ mark; the reason text stays plain
    detail = detail.lstrip("❌ ").strip()
    return detail or error.__class__.__name__


def _preflight_session_name(
    target_run_id: str, execution_context: dict[str, object]
) -> str:
    consumer = str(
        execution_context.get("execution_context.params.main_tag") or "ctl"
    )
    digest_input = json.dumps(execution_context, sort_keys=True, default=str) + target_run_id
    digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()[:10]
    raw = f"{consumer}-identity-preflight-{digest}"
    normalized = re.sub(r"[^A-Za-z0-9+=,.@-]", "-", raw).strip("-")
    return (normalized or f"ctl-identity-preflight-{digest}")[:64]


def _path_node(
    node_type: str,
    *,
    cfg_key: str | None,
    expected_name: str | None,
    expected_account: str | None,
    status: str,
) -> dict:
    name = expected_name or cfg_key or "<configured>"
    node = {
        "node_type": node_type,
        "display": f"{node_type}: {name}",
        "status": status,
    }
    if cfg_key:
        node["cfg_key"] = cfg_key
    if expected_name:
        node["expected_name"] = expected_name
    if expected_account:
        node["expected_account"] = expected_account
    return node


def _role_arn_parts(role_arn: str) -> tuple[str | None, str]:
    match = _ROLE_ARN_PATTERN.fullmatch(role_arn)
    if not match:
        return None, role_arn.rsplit("/", 1)[-1]
    return match.group("account_id"), match.group("role_name")


def resolve_target_cfg_references(
    target_run_id: str,
    target_run: dict,
    catalogs: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
) -> dict:
    """Static per-target cfg reference resolution (no live calls, account-id
    format left to stage-1 cfg validation): does this target's identity and
    account resolve in cfg? Returns rows for the identity and account refs."""
    del target_run_id
    try:
        resolved = resolve_target_aws_access(
            target_run,
            catalogs["execution_identities"],
            catalogs["credential_sources"],
            execution_context=execution_context,
            implementation_key=implementation_key,
            account_registry=catalogs["account_registry"],
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
            ctl_role_chain=catalogs["ctl_role_chain"],
            target_roles=catalogs["target_roles"],
            validate_local_credential=False,
            require_valid_account_id=False,
        )
    except Exception as error:
        return {
            "status": "failed",
            "rows": [],
            "failure_reason": common.credential_free_preflight_failure_reason(error),
        }
    if not resolved or resolved.get("identity_bypass") == "true":
        return {"status": "passed", "rows": []}
    rows = [
        {
            "name": f"identity_ref: {resolved.get('execution_identity_key')}",
            "status": "passed",
        }
    ]
    if resolved.get("account_key"):
        rows.append(
            {"name": f"account_ref: {resolved['account_key']}", "status": "passed"}
        )
    return {"status": "passed", "rows": rows}


def preflight_execution_identity(
    target_run_id: str,
    target_run: dict,
    catalogs: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    live_check: bool = True,
) -> dict:
    """Resolve one target path and optionally prove it with read-only STS calls."""
    raw_identity_key = target_run.get("execution_identity_key")
    try:
        resolved = resolve_target_aws_access(
            target_run,
            catalogs["execution_identities"],
            catalogs["credential_sources"],
            execution_context=execution_context,
            implementation_key=implementation_key,
            account_registry=catalogs["account_registry"],
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
            ctl_role_chain=catalogs["ctl_role_chain"],
            target_roles=catalogs["target_roles"],
            validate_local_credential=live_check,
        )
    except common.ProviderConfigBlockedError as error:
        return {
            "execution_identity_key": raw_identity_key,
            "provider": "aws",
            "access_mode": execution_access_mode,
            "status": "not_evaluated",
            "provider_path": [],
            "blocked": common.credential_free_preflight_failure_reason(error),
        }
    except Exception as error:
        reason = _preflight_failure_reason(error)
        # the report nests this error under its execution_identity line, so a
        # trailing "(execution identity '<same key>')" label is redundant here
        if raw_identity_key:
            suffix = f"(execution identity {raw_identity_key!r})"
            if reason.endswith(suffix):
                reason = reason[: -len(suffix)].rstrip()
        return {
            "execution_identity_key": raw_identity_key,
            "provider": "aws",
            "access_mode": execution_access_mode,
            "status": "failed",
            "provider_path": [],
            "failure_reason": reason,
        }

    if not resolved:
        return {
            "execution_identity_key": raw_identity_key,
            "provider": "aws",
            "access_mode": execution_access_mode,
            "status": "failed",
            "provider_path": [],
            "failure_reason": "selected target has no execution identity",
        }
    if resolved.get("identity_bypass") == "true":
        return {
            "execution_identity_key": (
                raw_identity_key
                or resolved.get("execution_identity_key")
                or "<unresolved>"
            ),
            "provider": "aws",
            "access_mode": execution_access_mode,
            "status": "not_applicable",
            "provider_path": [],
            "reason": "execution identity was bypassed for this run",
        }

    result = {
        "execution_identity_key": resolved["execution_identity_key"],
        "provider": "aws",
        "access_mode": execution_access_mode,
        "status": "force_skipped" if not live_check else "passed",
        "provider_path": [],
    }
    if not live_check:
        result["reason"] = (
            "execution-identity live preflight was force-skipped for this run"
        )
    skipped_status = "force_skipped"

    if resolved["credential_provider_kind"] == "direct_profile":
        principal_type = (
            "permission_set" if resolved.get("permission_set_name") else "role"
        )
        principal_name = resolved.get("permission_set_name") or resolved.get("role_name")
        nodes = [
            _path_node(
                "credential_source",
                cfg_key=resolved["credential_source_key"],
                expected_name=None,
                expected_account=resolved["expected_account_id"],
                status=skipped_status if not live_check else "passed",
            ),
            _path_node(
                principal_type,
                cfg_key=resolved["credential_source_key"],
                expected_name=principal_name,
                expected_account=resolved["expected_account_id"],
                status=skipped_status if not live_check else "passed",
            ),
        ]
        nodes[-1]["purpose"] = "target"
        nodes[-1]["display"] = f"required_{principal_type}: {principal_name}"
        result["provider_path"] = nodes
        if not live_check:
            return result
        try:
            caller = assert_profile_caller(
                resolved["profile_name"],
                expected_account_id=resolved["expected_account_id"],
                expect_principal={
                    key: resolved[key]
                    for key in ("permission_set_name", "role_name")
                    if resolved.get(key)
                },
                label=f"execution identity {resolved['execution_identity_key']!r}",
            )
            nodes[-1]["observed_account"] = caller.get("Account")
            nodes[-1]["observed_principal"] = caller.get("Arn")
            return result
        except Exception as error:
            reason = _preflight_failure_reason(error)
            nodes[-1]["status"] = "failed"
            nodes[-1]["failure_reason"] = reason
            result["status"] = "failed"
            result["failure_reason"] = reason
            return result

    entry_name = resolved.get("entry_permission_set_name") or resolved.get(
        "entry_role_name"
    )
    entry_type = (
        "permission_set" if resolved.get("entry_permission_set_name") else "role"
    )
    nodes = [
        _path_node(
            "credential_source",
            cfg_key=resolved["credential_source_key"],
            expected_name=None,
            expected_account=resolved["entry_account_id"],
            status=skipped_status if not live_check else "passed",
        ),
        _path_node(
            entry_type,
            cfg_key=resolved["credential_source_key"],
            expected_name=entry_name,
            expected_account=resolved["entry_account_id"],
            status=skipped_status if not live_check else "passed",
        ),
    ]
    hop_arns = resolved["hop_role_arns"]
    role_keys = [catalogs["ctl_role_chain"]["runner_role_key"]]
    if len(hop_arns) > 2:
        role_keys.extend(f"hop_{index}" for index in range(2, len(hop_arns)))
    role_keys.append(resolved["target_role_key"])
    hop_nodes = []
    for index, role_arn in enumerate(hop_arns):
        account_id, role_name = _role_arn_parts(role_arn)
        hop_nodes.append(
            _path_node(
                "role",
                cfg_key=role_keys[index] if index < len(role_keys) else None,
                expected_name=role_name,
                expected_account=account_id,
                status=skipped_status if not live_check else "passed",
            )
        )
    nodes.extend(hop_nodes)
    if hop_nodes:
        hop_nodes[-1]["purpose"] = "target"
        hop_nodes[-1]["display"] = (
            f"required_role: {hop_nodes[-1]['expected_name']}"
        )
    result["provider_path"] = nodes
    if not live_check:
        return result

    try:
        caller = assert_profile_caller(
            resolved["entry_profile_name"],
            expected_account_id=resolved["entry_account_id"],
            expect_principal={
                "permission_set_name": resolved.get("entry_permission_set_name"),
                "role_name": resolved.get("entry_role_name"),
            },
            label=f"entry credential source {resolved['credential_source_key']!r}",
        )
        nodes[1]["observed_account"] = caller.get("Account")
        nodes[1]["observed_principal"] = caller.get("Arn")
    except Exception as error:
        reason = _preflight_failure_reason(error)
        nodes[1]["status"] = "failed"
        nodes[1]["failure_reason"] = reason
        result["provider_path"] = nodes[:2]
        result["status"] = "failed"
        result["failure_reason"] = reason
        return result

    credentials: dict[str, str] = {}
    session_name = _preflight_session_name(target_run_id, execution_context)
    for index, role_arn in enumerate(hop_arns):
        try:
            credentials, assumed_user = _assume_role_credentials(
                role_arn,
                session_name=session_name,
                entry_profile_name=resolved["entry_profile_name"],
                env_extra=credentials or None,
                use_profile=index == 0,
            )
            if assumed_user.get("Arn"):
                hop_nodes[index]["observed_principal"] = assumed_user["Arn"]
        except Exception as error:
            reason = _preflight_failure_reason(error)
            hop_nodes[index]["status"] = "failed"
            hop_nodes[index]["failure_reason"] = reason
            result["provider_path"] = nodes[: 3 + index]
            result["status"] = "failed"
            result["failure_reason"] = reason
            return result

    try:
        final_caller = _run_aws_json(
            ["aws", "sts", "get-caller-identity", "--output", "json"],
            credentials,
        )
        _assertion().validate_caller_identity(
            final_caller,
            expected_account_id=resolved["expected_account_id"],
            expected_role_name=resolved["role_name"],
        )
        hop_nodes[-1]["observed_account"] = final_caller.get("Account")
        hop_nodes[-1]["observed_principal"] = final_caller.get("Arn")
        return result
    except Exception as error:
        reason = _preflight_failure_reason(error)
        hop_nodes[-1]["status"] = "failed"
        hop_nodes[-1]["failure_reason"] = reason
        result["status"] = "failed"
        result["failure_reason"] = reason
        return result

def validate_active_target_run_aws_access(
    active_target_runs: dict,
    execution_identities: dict,
    aws_credential_sources: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str] | None = None,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    ctl_role_chain: dict | None = None,
    target_roles: dict | None = None,
) -> dict[str, str]:
    """Validate selected bindings and return the normalized account-key registry used by target_runs."""
    common.validate_execution_identity_coverage(
        active_target_runs,
        execution_access_mode=execution_access_mode,
    )
    if execution_access_mode != "force_bypass" and any(
        target_run.get("execution_identity_key") is not None for target_run in active_target_runs.values()
    ):
        if account_registry is None:
            raise RuntimeError("❌ AWS account registry is required for declared execution identities")
        expected_account_registry = dict(account_registry)
    else:
        expected_account_registry = account_registry or {}

    validated_account_registry: dict[str, str] = {}
    for target_run_id, target_run in active_target_runs.items():
        resolved = resolve_target_aws_access(
            target_run,
            execution_identities,
            aws_credential_sources,
            execution_context=execution_context,
            implementation_key=implementation_key,
            account_registry=expected_account_registry,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
                ctl_role_chain=ctl_role_chain,
            target_roles=target_roles,
        )
        if resolved is None:
            continue
        if resolved.get("identity_bypass") == "true":
            logging.info(
                "Using the substitute provider credential for target_run %s (bypass): profile=%s",
                target_run_id,
                resolved["profile_name"],
            )
            continue
        account_key = resolved["account_key"]
        account_id = resolved["expected_account_id"]
        previous = validated_account_registry.get(account_key)
        if previous is not None and previous != account_id:
            raise RuntimeError(
                f"❌ Conflicting AWS account IDs for {account_key!r}: {previous} and {account_id}"
            )
        validated_account_registry[account_key] = account_id
        logging.info(
            "Validated AWS access for target_run %s: execution_identity_key=%s account_key=%s "
            "credential_source_key=%s implementation_key=%s credential_provider_kind=%s",
            target_run_id,
            resolved["execution_identity_key"],
            account_key,
            resolved["credential_source_key"],
            resolved["implementation_key"],
            resolved["credential_provider_kind"],
        )
    return validated_account_registry



def _collect_profile_sections(profile_name: str) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    """Collect the selected profile's config/credentials sections (following
    source_profile and sso-session references) from the host AWS files."""
    config_path = Path(os.environ.get("AWS_CONFIG_FILE", "~/.aws/config")).expanduser()
    credentials_path = Path(os.environ.get("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials")).expanduser()

    config = configparser.RawConfigParser()
    if config_path.is_file():
        config.read(config_path)
    credentials = configparser.RawConfigParser()
    if credentials_path.is_file():
        credentials.read(credentials_path)

    def config_section(name: str) -> str | None:
        for candidate in (f"profile {name}", name):
            if config.has_section(candidate):
                return candidate
        return None

    config_out: dict[str, dict[str, str]] = {}
    credentials_out: dict[str, dict[str, str]] = {}
    pending = [profile_name]
    seen: set[str] = set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        section = config_section(name)
        if section is not None:
            values = dict(config.items(section))
            config_out[section] = values
            source = values.get("source_profile")
            if source:
                pending.append(source.strip())
            sso_session = values.get("sso_session")
            if sso_session and config.has_section(f"sso-session {sso_session.strip()}"):
                sso_name = f"sso-session {sso_session.strip()}"
                config_out[sso_name] = dict(config.items(sso_name))
        if credentials.has_section(name):
            credentials_out[name] = dict(credentials.items(name))
    if not config_out and not credentials_out:
        raise RuntimeError(
            f"❌ AWS profile {profile_name!r} not found in {config_path} or {credentials_path}"
        )
    return config_out, credentials_out


def _write_ini(path: Path, sections: dict[str, dict[str, str]]) -> None:
    parser = configparser.RawConfigParser()
    for name, values in sections.items():
        parser.add_section(name)
        for key, value in values.items():
            parser.set(name, key, value)
    with open(path, "w") as handle:
        parser.write(handle)
    os.chmod(path, 0o600)


def materialize_profile_binding(binding_dir: Path, profile_name: str, target_env: dict[str, str]) -> None:
    """Isolated per-target_run credential binding: a generated AWS config holding ONLY
    the selected profile's sections. The target_run can no longer select any other
    host profile after the initial assertion (exec-identity review item 2)."""
    binding_dir.mkdir(parents=True, exist_ok=True)
    config_out, credentials_out = _collect_profile_sections(profile_name)
    _write_ini(binding_dir / "config", config_out)
    _write_ini(binding_dir / "credentials", credentials_out)
    target_env["ATLAS_PROVIDER_BINDING_DIR"] = str(binding_dir)


def derive_ctl_runner_arn(
    ctl_role_chain: dict | None,
    target_roles: dict | None,
    account_registry: dict[str, str],
    execution_context: dict[str, object],
) -> str | None:
    """Compose the ctl-runner role ARN from the ctl registries (§Phase 9).

    Resolved from ctl_role_chain.runner_role_key -> target_roles entry
    (account_key + role_name) -> account registry id. Returns None when any
    link is missing (e.g. the registry id is not populated yet) — consumers
    treat the fact as absent, never guess."""
    if not ctl_role_chain or not target_roles:
        return None
    runner_entry = target_roles.get(ctl_role_chain.get("runner_role_key")) or {}
    account_key = runner_entry.get("account_key")
    role_name = runner_entry.get("role_name")
    if not account_key or not role_name:
        return None
    account_id = (account_registry or {}).get(account_key)
    if not account_id:
        return None
    role_name = str(
        common.resolve_runtime_scalar(
            role_name, execution_context, label="target_roles runner role_name"
        )
    )
    return f"arn:aws:iam::{account_id}:role/{role_name}"


def configure_target_aws_env(
    target_run_id: str,
    target_run: dict,
    target_env: dict[str, str],
    execution_identities: dict,
    aws_credential_sources: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str],
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    ctl_role_chain: dict | None = None,
    target_roles: dict | None = None,
    run_dir: Path | None = None,
) -> None:
    """Apply one target_run's selected AWS access implementation and assertion metadata."""
    for var_name in AWS_ACCESS_TARGET_ENV_VARS:
        target_env.pop(var_name, None)

    for var_name in AWS_CREDENTIAL_ENV_VARS:
        target_env.pop(var_name, None)

    resolved = resolve_target_aws_access(
        target_run,
        execution_identities,
        aws_credential_sources,
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=account_registry,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=ctl_role_chain,
        target_roles=target_roles,
    )
    if resolved is None:
        return

    target_env["AWS_EC2_METADATA_DISABLED"] = "true"
    target_env["ATLAS_AWS_ASSERT_ACCESS"] = "true"
    target_env["ATLAS_EXECUTION_IDENTITY_KEY"] = resolved["execution_identity_key"]

    if resolved.get("identity_bypass") == "true":
        target_env["AWS_PROFILE"] = resolved["profile_name"]
        if run_dir is not None:
            materialize_profile_binding(
                run_dir / "provider_binding" / target_run_id, resolved["profile_name"], target_env
            )
        target_env["ATLAS_AWS_PROFILE_ONLY_ACCESS"] = "true"
        logging.info(
            "Resolved substitute-credential access for target_run %s (bypass): profile=%s",
            target_run_id,
            resolved["profile_name"],
        )
        return

    target_env["ATLAS_AWS_ACCOUNT_KEY"] = resolved["account_key"]
    target_env["ATLAS_AWS_CREDENTIAL_SOURCE_KEY"] = resolved["credential_source_key"]
    target_env["ATLAS_AWS_IMPLEMENTATION_KEY"] = resolved["implementation_key"]
    target_env["ATLAS_AWS_EXPECT_ACCOUNT_ID"] = resolved["expected_account_id"]

    # §Phase 9: adapter-derived run fact — the trusted ctl-runner ARN, composed
    # from the ctl registries; never authored in plt cfg. Absent when the
    # runner's account id is not registered yet.
    ctl_runner_arn = derive_ctl_runner_arn(
        ctl_role_chain, target_roles, account_registry, execution_context
    )
    if ctl_runner_arn:
        target_env["ATLAS_AWS_CTL_RUNNER_ARN"] = ctl_runner_arn

    if resolved["credential_provider_kind"] == "role_chain":
        # Standard mode: hand the target_run the FINAL role's assumed credentials
        # (produced by iterating the ordered hop path); the target_run asserts
        # account + final-role principal.
        chain_creds = assume_ctl_role_chain(
            resolved["entry_profile_name"],
            resolved["hop_role_arns"],
            session_name=f"atlas-ctl-{target_run_id}"[:64],
            entry_expected_account_id=resolved["entry_account_id"],
            entry_permission_set_name=resolved.get("entry_permission_set_name"),
            entry_role_name=resolved.get("entry_role_name"),
        )
        target_env.update(chain_creds)
        target_env["ATLAS_AWS_ROLE_CHAIN"] = "true"
        target_env["ATLAS_AWS_EXPECT_ROLE_NAME"] = resolved["role_name"]
        logging.info(
            "Resolved AWS standard (role-path) access for target_run %s: entry=%s hops=%s",
            target_run_id,
            resolved["entry_profile_name"],
            resolved["hop_role_arns"],
        )
        return

    # Direct mode: the target_run asserts BOTH facts — destination account and the
    # credential source's declared principal (Phase 14).
    target_env["AWS_PROFILE"] = resolved["profile_name"]
    if resolved.get("permission_set_name"):
        target_env["ATLAS_AWS_EXPECT_PERMISSION_SET_NAME"] = resolved["permission_set_name"]
    if resolved.get("role_name"):
        target_env["ATLAS_AWS_EXPECT_ROLE_NAME"] = resolved["role_name"]
    if run_dir is not None:
        materialize_profile_binding(
            run_dir / "provider_binding" / target_run_id, resolved["profile_name"], target_env
        )

    logging.info(
        "Resolved AWS access for target_run %s: execution_identity_key=%s account_key=%s "
        "credential_source_key=%s implementation_key=%s credential_provider_kind=%s expected_account_id=%s",
        target_run_id,
        resolved["execution_identity_key"],
        resolved["account_key"],
        resolved["credential_source_key"],
        resolved["implementation_key"],
        resolved["credential_provider_kind"],
        resolved["expected_account_id"],
    )


class CtlStateSyncer:
    """Incremental mirror of the local ctl-state namespace tree to its backend.

    Forward sync is add/update only — never deletes remote objects (the local
    root is ephemeral; remote cleanup is bucket lifecycle rules only).
    """

    STATE_LAYER_INCLUDES = (
        "*/RUN.yaml", "*/STATUS.yaml", "*/MANIFEST.yaml",
        # §Phase 31: the committed.yaml pointer + snapshot.yaml are part of the
        # lightweight state layer hydrated at run start.
        "*/committed.yaml", "*/snapshot.yaml",
    )

    def __init__(self, results_root: Path, bucket_name: str, bucket_region: str, credential: str | dict[str, str], run_dir: Path, *, required: bool):
        self.results_root = Path(results_root).resolve()
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.credential = credential
        self.required = required
        self.state = "pending"
        self.detail: str | None = None
        self.ready = False
        self.run_dir = Path(run_dir).resolve()
        self.object_etags: dict[str, str] = {}

    def _aws_env(self) -> dict[str, str]:
        env = os.environ.copy()
        if isinstance(self.credential, dict):
            env.pop("AWS_PROFILE", None)
            env.update(self.credential)
        else:
            env["AWS_PROFILE"] = self.credential
        return env

    def _run_aws(self, args: list[str]) -> subprocess.CompletedProcess:
        # Region is explicit (ctl-owned registry), never the profile default.
        return subprocess.run(
            ["aws", "--region", self.bucket_region, *args],
            env=self._aws_env(),
            capture_output=True,
            text=True,
        )

    def bucket_exists(self) -> bool:
        result = self._run_aws(["s3api", "head-bucket", "--bucket", self.bucket_name])
        return result.returncode == 0

    def _fail(self, action: str, detail: str) -> None:
        self.state = "failed"
        self.detail = f"{action}: {detail}"
        message = f"ctl-state sync {action} failed for s3://{self.bucket_name}: {detail}"
        if self.required:
            raise RuntimeError(f"❌ {message}")
        logging.warning("%s (sync not strict; continuing)", message)

    def ensure_ready(self, reason: str) -> bool:
        """Confirm backend readiness without hydrating the whole namespace."""
        if self.ready:
            return True
        if not self.bucket_exists():
            self.state = "local"
            self.detail = f"{reason}: bucket s3://{self.bucket_name} not present yet"
            if self.required:
                logging.warning(
                    "ctl-state bucket s3://%s not present at %r; results stay local",
                    self.bucket_name, reason,
                )
            return False
        self.ready = True
        return True

    def pull_object(self, key: str) -> bool:
        """Hydrate one required object and remember its concurrency token."""
        key = key.strip("/")
        local_path = self.results_root / key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        result = self._run_aws(
            ["s3api", "get-object", "--bucket", self.bucket_name, "--key", key, str(local_path)]
        )
        if result.returncode != 0:
            local_path.unlink(missing_ok=True)
            stderr = result.stderr.lower()
            if "nosuchkey" in stderr or "not found" in stderr or "404" in stderr:
                return False
            self._fail(
                f"pull object {key}",
                result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error",
            )
            return False
        try:
            response = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            response = {}
        etag = response.get("ETag")
        if isinstance(etag, str) and etag:
            self.object_etags[key] = etag
        return True

    def hydrate_instance(self, instance_prefix: str, child_prefixes: list[str] | None = None) -> None:
        """Read only identity/committed pointers needed by this invocation."""
        if not self.ensure_ready("instance hydration"):
            return
        prefixes = [instance_prefix, *(child_prefixes or [])]
        for prefix in dict.fromkeys(p.strip("/") for p in prefixes if p):
            self.pull_object(f"{prefix}/identity.yaml")
            self.pull_object(f"{prefix}/committed.yaml")

    def push_run(self, run_dir: Path, reason: str) -> None:
        """Upload one immutable run prefix; never mirror the namespace."""
        if not self.ensure_ready(f"push ({reason})"):
            return
        run_dir = Path(run_dir).resolve()
        rel_run = run_dir.relative_to(self.results_root).as_posix()
        result = self._run_aws(
            ["s3", "sync", str(run_dir), f"s3://{self.bucket_name}/{rel_run}", "--no-progress"]
        )
        if result.returncode != 0:
            self._fail(
                f"run push ({reason})",
                result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error",
            )
        else:
            self.state = "synced"
            self.detail = reason
            logging.info("Ctl-state run prefix synced to s3://%s/%s", self.bucket_name, rel_run)

    def push(self, reason: str) -> None:
        self.push_run(self.run_dir, reason)

    def _head_object_etag(self, key: str) -> str | None:
        result = self._run_aws(
            ["s3api", "head-object", "--bucket", self.bucket_name, "--key", key]
        )
        if result.returncode != 0:
            lowered = (result.stderr or "").lower()
            if "404" in lowered or "nosuchkey" in lowered or "not found" in lowered:
                return None
            self._fail(
                f"head object {key}",
                result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error",
            )
            return None
        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            payload = {}
        etag = payload.get("ETag")
        return etag if isinstance(etag, str) and etag else None

    def publish_identity(self, identity_path: Path) -> None:
        """Conditionally create an instance identity or validate the existing one."""
        if not self.ensure_ready("identity publication"):
            self._fail("identity publication", "backend is not ready")
            return
        key = identity_path.resolve().relative_to(self.results_root).as_posix()
        etag = self._head_object_etag(key)
        if etag is not None:
            with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as handle:
                remote_path = Path(handle.name)
            try:
                result = self._run_aws(
                    ["s3api", "get-object", "--bucket", self.bucket_name, "--key", key, str(remote_path)]
                )
                if result.returncode != 0:
                    self._fail(
                        f"read identity {key}",
                        result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error",
                    )
                    return
                if remote_path.read_bytes() != identity_path.read_bytes():
                    raise RuntimeError(
                        f"❌ ctl-state identity conflict for s3://{self.bucket_name}/{key}"
                    )
                self.object_etags[key] = etag
                return
            finally:
                remote_path.unlink(missing_ok=True)
        result = self._run_aws(
            [
                "s3api", "put-object", "--bucket", self.bucket_name, "--key", key,
                "--body", str(identity_path), "--if-none-match", "*",
            ]
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"❌ ctl-state identity publication conflict for "
                f"s3://{self.bucket_name}/{key}"
            )

    def publish_committed_pointer(self, pointer_path: Path) -> None:
        """Conditionally swap exactly one instance committed pointer."""
        if not self.ensure_ready("committed publication"):
            self._fail("committed publication", "backend is not ready")
            return
        key = pointer_path.resolve().relative_to(self.results_root).as_posix()
        args = [
            "s3api", "put-object", "--bucket", self.bucket_name, "--key", key,
            "--body", str(pointer_path),
        ]
        previous_etag = self.object_etags.get(key)
        if previous_etag is None:
            previous_etag = self._head_object_etag(key)
        args += ["--if-match", previous_etag] if previous_etag else ["--if-none-match", "*"]
        result = self._run_aws(args)
        if result.returncode != 0:
            raise RuntimeError(
                f"❌ ctl-state committed pointer conflict for s3://{self.bucket_name}/{key}; "
                "another run published this instance first"
            )
        self.state = "synced"
        self.detail = "committed pointer published"

    def list_object_keys(self, prefix: str = "") -> list[str]:
        args = [
            "s3api", "list-objects-v2", "--bucket", self.bucket_name,
            "--output", "json",
        ]
        if prefix:
            args += ["--prefix", prefix]
        result = self._run_aws(args)
        if result.returncode != 0:
            self._fail(
                f"list prefix {prefix!r}",
                result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error",
            )
            return []
        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError as error:
            raise RuntimeError("❌ invalid S3 list-objects response") from error
        return [
            item["Key"]
            for item in (payload.get("Contents") or [])
            if isinstance(item, dict) and isinstance(item.get("Key"), str)
        ]

    def put_object(self, key: str, path: Path) -> None:
        result = self._run_aws(
            [
                "s3api", "put-object", "--bucket", self.bucket_name,
                "--key", key.strip("/"), "--body", str(path),
            ]
        )
        if result.returncode != 0:
            self._fail(
                f"put object {key}",
                result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error",
            )

    def delete_object_keys(self, keys: list[str]) -> None:
        for key in keys:
            result = self._run_aws(
                ["s3api", "delete-object", "--bucket", self.bucket_name, "--key", key]
            )
            if result.returncode != 0:
                self._fail(
                    f"delete object {key}",
                    result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error",
                )

    # ── §Phase 31 Q1b: interim global mutation lock (locks/mutation.yaml at
    #    the namespace bucket root). Conditional create via S3 If-None-Match;
    #    stale-break is delete-then-create (a benign race for the interim
    #    single-operator model — per-instance locking replaces this later).
    MUTATION_LOCK_KEY = "locks/mutation.yaml"

    def read_mutation_lock(self) -> dict | None:
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as handle:
            tmp = handle.name
        try:
            result = self._run_aws(
                ["s3api", "get-object", "--bucket", self.bucket_name,
                 "--key", self.MUTATION_LOCK_KEY, tmp]
            )
            if result.returncode != 0:
                return None  # absent (or unreadable — treated as free; create still guards)
            with open(tmp, encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            return data if isinstance(data, dict) else None
        finally:
            Path(tmp).unlink(missing_ok=True)

    def write_mutation_lock(self, lock_doc: dict) -> bool:
        """Conditionally create the lock object; False when another writer won."""
        with tempfile.NamedTemporaryFile(
            "w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as handle:
            yaml.safe_dump(lock_doc, handle, sort_keys=False)
            tmp = handle.name
        try:
            result = self._run_aws(
                ["s3api", "put-object", "--bucket", self.bucket_name,
                 "--key", self.MUTATION_LOCK_KEY, "--if-none-match", "*",
                 "--body", tmp]
            )
            return result.returncode == 0
        finally:
            Path(tmp).unlink(missing_ok=True)

    def delete_mutation_lock(self) -> None:
        self._run_aws(
            ["s3api", "delete-object", "--bucket", self.bucket_name,
             "--key", self.MUTATION_LOCK_KEY]
        )

    def remove_prefix(self, rel_prefix: str) -> None:
        """Explicit slot-transition removal of one remote prefix.

        Distinct from the mirror (which never deletes): state slots are
        pointers, and a slot removed locally must not linger remotely.
        """
        if not self.ensure_ready(f"slot removal ({rel_prefix})"):
            return
        result = self._run_aws(
            ["s3", "rm", f"s3://{self.bucket_name}/{rel_prefix.strip('/')}", "--recursive"]
        )
        if result.returncode != 0:
            self._fail(f"slot removal ({rel_prefix})", result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error")

    def summary(self) -> dict[str, str]:
        payload = {"mode": "enabled", "bucket": self.bucket_name, "state": self.state}
        if self.detail:
            payload["detail"] = self.detail
        return payload


# ---------------------------------------------------------------------------
# Adapter contract (see utils/providers/__init__.py)
# ---------------------------------------------------------------------------

def validate_catalog(ctl_cfg_root: Path) -> None:
    """Validate the complete providers.aws.* subtree."""
    load_aws_credential_sources_cfg(ctl_cfg_root)
    load_aws_account_registry_cfg(ctl_cfg_root)
    load_aws_ctl_role_chain_cfg(ctl_cfg_root)
    load_aws_target_roles_cfg(ctl_cfg_root)
    load_aws_ctl_state_roles_cfg(ctl_cfg_root)


def validate_execution_identity(identity_key: str, identity_cfg: dict, ctl_cfg_root: Path) -> None:
    """Validate the AWS identity payload (everything except the generic envelope)."""
    for removed_field in ("access_context_key", "direct_access_context_key"):
        if removed_field in identity_cfg:
            raise RuntimeError(
                f"❌ execution identity {identity_key!r} uses removed {removed_field}; "
                "rename it to direct_credential_source_key (§Skip Model D)"
            )
    allowed_fields = {
        "provider", "account_key", "direct_credential_source_key",
        "ctl_target_role_key", "ctl_state_role_key",
    }
    unknown = sorted(set(identity_cfg) - allowed_fields)
    if unknown:
        raise RuntimeError(f"❌ execution identity {identity_key!r} has unknown fields {unknown}")
    common._require_non_empty_string(
        identity_cfg.get("account_key"),
        f"execution_identities.{identity_key}.account_key",
        ctl_cfg_root,
    )
    for optional_field in (
        "direct_credential_source_key", "ctl_target_role_key", "ctl_state_role_key"
    ):
        if optional_field in identity_cfg:
            common._require_non_empty_string(
                identity_cfg.get(optional_field),
                f"execution_identities.{identity_key}.{optional_field}",
                ctl_cfg_root,
            )


def load_runtime_catalogs(
    ctl_cfg_root: Path,
    *,
    execution_context: dict[str, object] | None = None,
    strict_account_ids: bool = True,
) -> dict:
    """Load the run-scoped AWS catalogs bundle (opaque to the engine core).

    With `strict_account_ids=False` the registry loads even with placeholder
    account ids, so per-target resolution/preflight can report the defect per
    target (blocked) rather than the whole load raising up front."""
    return {
        "execution_identities": common.load_execution_identities_cfg(ctl_cfg_root),
        "credential_sources": load_aws_credential_sources_cfg(ctl_cfg_root),
        "account_registry": load_aws_account_registry_cfg(
            ctl_cfg_root,
            execution_context=execution_context,
            strict_selected=strict_account_ids,
        ),
        "ctl_role_chain": load_aws_ctl_role_chain_cfg(ctl_cfg_root),
        "target_roles": load_aws_target_roles_cfg(ctl_cfg_root),
        "ctl_state_roles": load_aws_ctl_state_roles_cfg(ctl_cfg_root),
    }


def validate_active_target_access(
    active_target_runs: dict,
    catalogs: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
) -> None:
    catalogs["validated_account_registry"] = validate_active_target_run_aws_access(
        active_target_runs,
        catalogs["execution_identities"],
        catalogs["credential_sources"],
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=catalogs["account_registry"],
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=catalogs["ctl_role_chain"],
        target_roles=catalogs["target_roles"],
    )


def materialize_target_binding(
    target_run_id: str,
    target_run: dict,
    target_env: dict[str, str],
    catalogs: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    run_dir: Path | None = None,
) -> None:
    configure_target_aws_env(
        target_run_id,
        target_run,
        target_env,
        catalogs["execution_identities"],
        catalogs["credential_sources"],
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=catalogs.get("validated_account_registry") or catalogs["account_registry"],
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=catalogs["ctl_role_chain"],
        target_roles=catalogs["target_roles"],
        run_dir=run_dir,
    )


def target_assertion_argv(step_utils_dir: Path) -> list[str]:
    return ["python3", str(step_utils_dir / "assert_aws_access.py")]


def _credential_from_resolved_access(
    resolved: dict, *, session_name: str
) -> str | dict[str, str]:
    """Materialize one already-resolved AWS access path for ctl-owned operations."""
    kind = resolved.get("credential_provider_kind")
    if kind in {"substitute_credential", "direct_profile"}:
        return resolved["profile_name"]
    if kind != "role_chain":
        raise RuntimeError(
            f"❌ unsupported AWS credential kind for ctl operation: {kind!r}"
        )
    return assume_ctl_role_chain(
        resolved["entry_profile_name"],
        resolved["hop_role_arns"],
        session_name=session_name[:64],
        entry_expected_account_id=resolved["entry_account_id"],
        entry_permission_set_name=resolved.get("entry_permission_set_name"),
        entry_role_name=resolved.get("entry_role_name"),
    )


def resolve_state_backend_probe_credential(
    target_run: dict,
    catalogs: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str,
    provider_credential: str | None,
) -> str | dict[str, str]:
    """Resolve the backend-provisioning target credential used by readiness probes."""
    resolved = resolve_target_aws_access(
        target_run,
        catalogs["execution_identities"],
        catalogs["credential_sources"],
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=(
            catalogs.get("validated_account_registry")
            or catalogs["account_registry"]
        ),
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=catalogs["ctl_role_chain"],
        target_roles=catalogs["target_roles"],
        ctl_state_roles=catalogs["ctl_state_roles"],
    )
    if not resolved:
        raise RuntimeError("❌ backend provisioner has no execution identity")
    return _credential_from_resolved_access(
        resolved, session_name="atlas-ctl-state-backend-probe"
    )


def probe_state_backend(
    bucket_name: str,
    bucket_region: str,
    credential: str | dict[str, str],
) -> dict[str, str]:
    """Classify S3 HeadBucket without converting access/network failures to absence."""
    env = os.environ.copy()
    if isinstance(credential, dict):
        env.pop("AWS_PROFILE", None)
        env.update(credential)
    else:
        env["AWS_PROFILE"] = credential
    result = subprocess.run(
        [
            "aws",
            "--region",
            bucket_region,
            "s3api",
            "head-bucket",
            "--bucket",
            bucket_name,
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return {"status": "ready"}
    detail = " ".join((result.stderr or result.stdout or "").split())
    lowered = detail.lower()
    if (
        "nosuchbucket" in lowered
        or "not found" in lowered
        or "(404)" in lowered
        or "status code: 404" in lowered
    ):
        return {"status": "absent", "detail": detail or "S3 returned 404"}
    if (
        "accessdenied" in lowered
        or "forbidden" in lowered
        or "(403)" in lowered
        or "status code: 403" in lowered
    ):
        return {"status": "denied", "detail": detail or "S3 returned 403"}
    return {"status": "failed", "detail": detail or f"aws exited {result.returncode}"}


def validate_state_backend_entry(namespace_key: str, entry: dict, path) -> None:
    if entry.get("backend_type", "").strip() != "s3":
        raise RuntimeError(
            f"❌ ctl_state_backends.{namespace_key} backend_type {entry.get('backend_type')!r} is not supported "
            f"by the aws adapter; available: s3 ({path})"
        )


def _ctl_state_policy_path(value: str, *, label: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"❌ {label} must be a string")
    normalized = value.strip("/")
    if not normalized and allow_empty:
        return ""
    if not normalized or ".." in normalized.split("/"):
        raise RuntimeError(f"❌ {label} is not a safe ctl-state relative path: {value!r}")
    return normalized


def build_ctl_state_session_policy(
    bucket_name: str,
    operation: str,
    *,
    object_keys: list[str] | tuple[str, ...] = (),
    object_prefixes: list[str] | tuple[str, ...] = (),
) -> dict:
    """Least-privilege session boundary for one ctl-state operation.

    Read may intentionally inspect the complete namespace. Sync and maintenance
    must enumerate the exact object keys/prefixes approved for this credential.
    """
    if operation not in {"read", "sync", "maintenance"}:
        raise RuntimeError(f"❌ unsupported ctl-state operation {operation!r}")
    bucket_arn = f"arn:aws:s3:::{bucket_name}"
    keys = sorted({
        _ctl_state_policy_path(value, label="ctl-state object key")
        for value in object_keys
    })
    prefixes = sorted({
        _ctl_state_policy_path(
            value, label="ctl-state object prefix", allow_empty=(operation == "read")
        )
        for value in object_prefixes
    })
    if operation == "read" and not keys and not prefixes:
        prefixes = [""]
    if operation in {"sync", "maintenance"} and not keys and not prefixes:
        raise RuntimeError(
            f"❌ ctl-state {operation} session policy requires approved object keys or prefixes"
        )

    list_prefixes = sorted({
        *(key for key in keys),
        *(f"{prefix}/*" if prefix else "*" for prefix in prefixes),
    })
    statements: list[dict] = [
        {
            "Effect": "Allow",
            "Action": ["s3:GetBucketLocation"],
            "Resource": bucket_arn,
        },
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": bucket_arn,
            "Condition": {"StringLike": {"s3:prefix": list_prefixes}},
        },
    ]
    object_resources = [f"{bucket_arn}/{key}" for key in keys]
    object_resources.extend(
        f"{bucket_arn}/{prefix}/*" if prefix else f"{bucket_arn}/*"
        for prefix in prefixes
    )
    object_resources = sorted(set(object_resources))
    object_actions = {
        "read": ["s3:GetObject"],
        "sync": ["s3:GetObject", "s3:PutObject"],
        "maintenance": ["s3:GetObject", "s3:DeleteObject"],
    }[operation]
    statements.append(
        {"Effect": "Allow", "Action": object_actions, "Resource": object_resources}
    )
    if operation == "maintenance":
        report_resources = [
            resource
            for resource in object_resources
            if resource.startswith(f"{bucket_arn}/_maintenance/")
        ]
        if report_resources:
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:PutObject"],
                    "Resource": report_resources,
                }
            )
    # The active mutation lock is the only object normal synchronization may delete.
    if operation == "sync":
        statements.append(
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                "Resource": f"{bucket_arn}/locks/mutation.yaml",
            }
        )
    return {"Version": "2012-10-17", "Statement": statements}

def resolve_ctl_state_credential(
    identity_key: str,
    ctl_cfg_root: Path,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    operation: str,
    bucket_name: str,
    object_keys: list[str] | tuple[str, ...] = (),
    object_prefixes: list[str] | tuple[str, ...] = (),
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
) -> str | dict[str, str]:
    if execution_access_mode == "force_bypass":
        if not provider_credential:
            raise RuntimeError(
                "❌ bypass execution access requires the --provider-credential substitute credential"
            )
        return provider_credential.strip()

    identities = common.load_execution_identities_cfg(ctl_cfg_root)
    credential_sources = load_aws_credential_sources_cfg(ctl_cfg_root)
    account_registry = load_aws_account_registry_cfg(
        ctl_cfg_root, execution_context=execution_context
    )
    ctl_role_chain = load_aws_ctl_role_chain_cfg(ctl_cfg_root)
    target_roles = load_aws_target_roles_cfg(ctl_cfg_root)
    ctl_state_roles = load_aws_ctl_state_roles_cfg(ctl_cfg_root)
    resolved = resolve_target_aws_access(
        {"execution_identity_key": identity_key},
        identities,
        credential_sources,
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=account_registry,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=ctl_role_chain,
        target_roles=target_roles,
        ctl_state_roles=ctl_state_roles,
    )
    if not resolved:
        raise RuntimeError(f"❌ ctl-state identity {identity_key!r} did not resolve")

    if resolved.get("credential_provider_kind") == "direct_profile":
        assert_profile_caller(
            resolved["profile_name"],
            expected_account_id=resolved["expected_account_id"],
            expect_principal={
                key: resolved[key]
                for key in ("permission_set_name", "role_name")
                if resolved.get(key)
            },
            label=f"ctl-state {operation} identity {identity_key!r}",
        )
        return resolved["profile_name"]

    if resolved.get("credential_provider_kind") != "role_chain":
        raise RuntimeError(
            f"❌ ctl-state identity {identity_key!r} resolved unsupported credential kind "
            f"{resolved.get('credential_provider_kind')!r}"
        )
    credentials = assume_ctl_role_chain(
        resolved["entry_profile_name"],
        resolved["hop_role_arns"],
        session_name=f"atlas-ctl-state-{operation}"[:64],
        entry_expected_account_id=resolved["entry_account_id"],
        entry_permission_set_name=resolved.get("entry_permission_set_name"),
        entry_role_name=resolved.get("entry_role_name"),
        final_session_policy=build_ctl_state_session_policy(
            bucket_name,
            operation,
            object_keys=object_keys,
            object_prefixes=object_prefixes,
        ),
    )
    caller = _run_aws_json(
        ["aws", "sts", "get-caller-identity", "--output", "json"], credentials
    )
    _assertion().validate_caller_identity(
        caller,
        expected_account_id=resolved["expected_account_id"],
        expected_role_name=resolved["role_name"],
    )
    return credentials


def create_state_syncer(results_root, bucket_name: str, bucket_region: str, credential, run_dir, *, required: bool):
    return CtlStateSyncer(
        results_root, bucket_name, bucket_region, credential, run_dir, required=required
    )


def normalize_provider_credential(value: str | None) -> str | None:
    return normalize_optional_aws_profile(value)
