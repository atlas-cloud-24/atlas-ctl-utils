"""AWS provider adapter for the Atlas ctl engine.

Owns every AWS-specific concept: identity schema, provider catalogs
(providers.aws.*), profile/STS credential acquisition, the ctl role chain,
stage runtime binding and assertion metadata, the S3 ctl-state syncer, and
derived provider facts. Engine-core modules never import AWS vocabulary; they
dispatch through utils.providers.get_adapter().
"""

import configparser
import functools
import json
import logging
import os
import re
import subprocess
from pathlib import Path

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


AWS_ACCESS_STAGE_ENV_VARS = (
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
    "stage_roles",
    "ctl_state_synchronizer_roles",
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


def load_aws_account_registry_cfg(ctl_cfg_root: Path) -> dict[str, str]:
    """Load the provider-owned AWS account registry: account_key -> account_id."""
    accounts = _load_aws_provider_catalog(ctl_cfg_root).get("accounts", {})
    registry: dict[str, str] = {}
    for account_key, account_cfg in accounts.items():
        if not isinstance(account_key, str) or not account_key.strip():
            raise RuntimeError(f"❌ aws account keys must be non-empty strings: {ctl_cfg_root}")
        if not isinstance(account_cfg, dict):
            raise RuntimeError(f"❌ aws account {account_key!r} must be a mapping: {ctl_cfg_root}")
        unknown = sorted(set(account_cfg) - {"account_id"})
        if unknown:
            raise RuntimeError(f"❌ aws account {account_key!r} has unknown fields {unknown}: {ctl_cfg_root}")
        account_id = common._require_non_empty_string(
            account_cfg.get("account_id"),
            f"providers.aws.accounts.{account_key}.account_id",
            ctl_cfg_root,
        )
        if not re.fullmatch(r"\d{12}", account_id):
            raise RuntimeError(
                f"❌ providers.aws.accounts.{account_key}.account_id must be a 12-digit account id"
            )
        registry[account_key] = account_id
    return registry


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


def load_aws_stage_roles_cfg(ctl_cfg_root: Path) -> dict:
    """Roles the engine assumes for stage execution (chain mode): account-agnostic
    fan-out roles (no account_key — the identity's account supplies it) plus
    account-bound roles (e.g. the runner, the management StackSet-admin role)."""
    return _validate_aws_role_registry(
        "stage_roles",
        _load_aws_provider_catalog(ctl_cfg_root).get("stage_roles", {}),
        ctl_cfg_root,
        allow_account_key=True,
    )


def load_aws_ctl_state_synchronizer_roles_cfg(ctl_cfg_root: Path) -> dict:
    """Roles the engine assumes for ctl-state sync (chain mode): account-agnostic
    constant names; the backend's account supplies the ARN account."""
    return _validate_aws_role_registry(
        "ctl_state_synchronizer_roles",
        _load_aws_provider_catalog(ctl_cfg_root).get("ctl_state_synchronizer_roles", {}),
        ctl_cfg_root,
        allow_account_key=False,
    )


def load_aws_ctl_role_chain_cfg(ctl_cfg_root: Path) -> dict | None:
    """The ONE ctl role chain (singleton by design; §Skip Model D): entry credential
    source, runner role, and the default stage role. Absent = chain mode is not
    configured; runs must use direct or bypass execution access."""
    chain = _load_aws_provider_catalog(ctl_cfg_root).get("ctl_role_chain")
    if chain is None:
        return None
    if not isinstance(chain, dict):
        raise RuntimeError(f"❌ providers.aws.ctl_role_chain must be a mapping: {ctl_cfg_root}")
    # Phase 15: no universal stage-role fallback. Every target-stage identity
    # names its own ctl_stage_role_key (authorization class); the chain declares
    # only the entry source and the runner hub.
    if "stage_role_key" in chain:
        raise RuntimeError(
            "❌ providers.aws.ctl_role_chain.stage_role_key is removed (Phase 15); "
            "each execution identity must declare its own ctl_stage_role_key"
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
    """ONE assertion implementation for entry, synchronizer, and stage checks:
    the stage-side assert_aws_access module (exact assumed-role ARN parsing,
    anchored AWSReservedSSO matching)."""
    global _ASSERTION_MODULE
    if _ASSERTION_MODULE is None:
        import importlib.util

        script = Path(__file__).resolve().parents[3] / "stage_utils" / "assert_aws_access.py"
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
) -> None:
    """Engine-side caller assertion (shared implementation with the stage check)."""
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


def assume_ctl_role_chain(
    entry_profile_name: str,
    hop_role_arns: list[str],
    *,
    session_name: str,
    entry_expected_account_id: str,
    entry_permission_set_name: str | None = None,
    entry_role_name: str | None = None,
) -> dict[str, str]:
    """Execute the entry-profile -> [ordered role hops] AssumeRole path via the
    AWS CLI, iterating an arbitrary-length hop list (§12.3). Returns the FINAL
    role's temporary credentials as env-var names. The entry credential's expect
    is asserted against its caller ARN before the first hop.
    """
    validate_credential_path(hop_role_arns)

    def _aws(cmd: list[str], env_extra: dict[str, str] | None = None) -> dict:
        env = os.environ.copy()
        env["AWS_EC2_METADATA_DISABLED"] = "true"
        if env_extra:
            env.update(env_extra)
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"❌ {' '.join(cmd[:4])} failed: {result.stderr.strip()}")
        return json.loads(result.stdout)

    # Phase 14: exact shared validation (assumed-role ARN parsing, anchored
    # AWSReservedSSO pattern, entry account) — no substring matching.
    caller = _aws(["aws", "sts", "get-caller-identity", "--output", "json", "--profile", entry_profile_name])
    try:
        _assertion().validate_caller_identity(
            caller,
            expected_account_id=entry_expected_account_id,
            expected_permission_set_name=entry_permission_set_name,
            expected_role_name=entry_role_name,
        )
    except RuntimeError as error:
        raise RuntimeError(f"❌ entry profile {entry_profile_name!r}: {error}") from error

    def _assume(role_arn: str, env_extra: dict[str, str] | None, use_profile: bool) -> dict[str, str]:
        cmd = ["aws", "sts", "assume-role", "--output", "json",
               "--role-arn", role_arn, "--role-session-name", session_name]
        if use_profile:
            cmd += ["--profile", entry_profile_name]
        creds = _aws(cmd, env_extra)["Credentials"]
        return {
            "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
            "AWS_SESSION_TOKEN": creds["SessionToken"],
        }

    # Iterate the ordered path: the first hop assumes from the entry profile,
    # each subsequent hop chains from the previous hop's credentials.
    creds: dict[str, str] = {}
    for index, role_arn in enumerate(hop_role_arns):
        creds = _assume(role_arn, creds or None, use_profile=(index == 0))
    return creds


def resolve_stage_aws_access(
    stage: dict,
    execution_identities: dict,
    aws_credential_sources: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str] | None = None,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    ctl_role_chain: dict | None = None,
    stage_roles: dict | None = None,
) -> dict[str, str] | None:
    """Resolve one stage's AWS access per §Skip Model D.

    Three modes (§12): bypass (--force-bypass-execution-identity + substitute
    credential; nothing checked), direct (--agreed-use-direct-execution-access; the identity's
    direct_credential_source_key profile + account and principal checks), and
    chain (default; entry profile -> runner role -> stage role, entry expect
    checked, stage asserts account + stage-role principal).
    """
    for legacy_field in ("aws_account_key", "aws_access_context_key"):
        if legacy_field in stage:
            raise RuntimeError(f"❌ stage uses deprecated {legacy_field}; use execution_identity_key")

    if execution_access_mode == "bypass":
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

    identity_key = stage.get("execution_identity_key")
    if identity_key is None:
        # Coverage is validated by common.validate_execution_identity_coverage; a lone
        # resolve call for an identity-less stage has nothing to resolve.
        return None
    if not isinstance(identity_key, str) or not identity_key.strip():
        raise RuntimeError("❌ stage execution_identity_key must be a non-empty string")
    identity_key = identity_key.strip()

    identity_cfg = execution_identities.get(identity_key)
    if not isinstance(identity_cfg, dict):
        raise RuntimeError(
            f"❌ stage execution_identity_key {identity_key!r} is not defined in execution_identities.yaml"
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

    resolved: dict[str, str] = {
        "provider": "aws",
        "execution_identity_key": identity_key,
        "account_key": account_key,
        "implementation_key": implementation_key,
        "expected_account_id": expected_account_id,
    }

    if execution_access_mode == "direct":
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
        canonical_account_id = resolve_configured_profile_account_id(canonical_profile_name)
        if expected_account_id != canonical_account_id:
            raise RuntimeError(
                f"❌ AWS account registry maps {account_key!r} to {expected_account_id}, but canonical "
                f"profile {canonical_profile_name!r} resolves to {canonical_account_id}"
            )
        override_name = aws_credential_source_override_env_name(direct_source_key)
        selected_profile_name = os.getenv(override_name, "").strip() or canonical_profile_name
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
            "with --agreed-use-direct-execution-access (bootstrap) or --force-bypass-execution-identity"
        )
    stage_roles = stage_roles or {}
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
        role_cfg = stage_roles.get(role_key)
        if not isinstance(role_cfg, dict):
            raise RuntimeError(f"❌ providers.aws.stage_roles has no key {role_key!r} ({label})")
        role_name = common.resolve_runtime_scalar(
            role_cfg["role_name"], context, label=f"providers.aws.stage_roles.{role_key}.role_name"
        )
        role_account_key = role_cfg.get("account_key", default_account_key)
        role_account_id = _registry_account_id(account_registry, role_account_key, label=label)
        return role_name, f"arn:aws:iam::{role_account_id}:role/{role_name}"

    _, runner_role_arn = _chain_role_arn(
        ctl_role_chain["runner_role_key"], label="ctl_role_chain.runner_role_key",
        default_account_key=account_key,
    )
    # Phase 15: the stage role is the identity's authorization class — required,
    # no fallback. (Synchronizer identities never reach chain mode: they resolve
    # in direct mode.)
    stage_role_key = identity_cfg.get("ctl_stage_role_key")
    if not stage_role_key:
        raise RuntimeError(
            f"❌ execution identity {identity_key!r} declares no ctl_stage_role_key; "
            "a steady-state target identity must select an authorization class "
            "(e.g. ctl_stage_readonly or ctl_stage_deploy)"
        )
    stage_role_name, stage_role_arn = _chain_role_arn(
        stage_role_key, label=f"execution identity {identity_key!r} stage role",
        default_account_key=account_key,
    )

    resolved["credential_source_key"] = entry_source_key
    resolved["credential_provider_kind"] = "role_chain"
    resolved["entry_profile_name"] = entry_profile_name
    # §12.3: ordered credential path (runner, then final role). The executor
    # iterates this list and makes no assumption about its length.
    resolved["hop_role_arns"] = [runner_role_arn, stage_role_arn]
    resolved["stage_role_key"] = stage_role_key
    resolved["role_name"] = stage_role_name
    validate_credential_path(resolved["hop_role_arns"])
    return resolved


def validate_active_stage_aws_access(
    active_stages: dict,
    execution_identities: dict,
    aws_credential_sources: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str] | None = None,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    ctl_role_chain: dict | None = None,
    stage_roles: dict | None = None,
) -> dict[str, str]:
    """Validate selected bindings and return the normalized account-key registry used by stages."""
    common.validate_execution_identity_coverage(
        active_stages,
        execution_access_mode=execution_access_mode,
    )
    if execution_access_mode != "bypass" and any(
        stage.get("execution_identity_key") is not None for stage in active_stages.values()
    ):
        if account_registry is None:
            raise RuntimeError("❌ AWS account registry is required for declared execution identities")
        expected_account_registry = dict(account_registry)
    else:
        expected_account_registry = account_registry or {}

    validated_account_registry: dict[str, str] = {}
    for stage_id, stage in active_stages.items():
        resolved = resolve_stage_aws_access(
            stage,
            execution_identities,
            aws_credential_sources,
            execution_context=execution_context,
            implementation_key=implementation_key,
            account_registry=expected_account_registry,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
                ctl_role_chain=ctl_role_chain,
            stage_roles=stage_roles,
        )
        if resolved is None:
            continue
        if resolved.get("identity_bypass") == "true":
            logging.info(
                "Using the substitute provider credential for stage %s (bypass): profile=%s",
                stage_id,
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
            "Validated AWS access for stage %s: execution_identity_key=%s account_key=%s "
            "credential_source_key=%s implementation_key=%s credential_provider_kind=%s",
            stage_id,
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


def materialize_profile_binding(binding_dir: Path, profile_name: str, stage_env: dict[str, str]) -> None:
    """Isolated per-stage credential binding: a generated AWS config holding ONLY
    the selected profile's sections. The stage can no longer select any other
    host profile after the initial assertion (exec-identity review item 2)."""
    binding_dir.mkdir(parents=True, exist_ok=True)
    config_out, credentials_out = _collect_profile_sections(profile_name)
    _write_ini(binding_dir / "config", config_out)
    _write_ini(binding_dir / "credentials", credentials_out)
    stage_env["ATLAS_PROVIDER_BINDING_DIR"] = str(binding_dir)


def configure_stage_aws_env(
    stage_id: str,
    stage: dict,
    stage_env: dict[str, str],
    execution_identities: dict,
    aws_credential_sources: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str],
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    ctl_role_chain: dict | None = None,
    stage_roles: dict | None = None,
    run_dir: Path | None = None,
) -> None:
    """Apply one stage's selected AWS access implementation and assertion metadata."""
    for var_name in AWS_ACCESS_STAGE_ENV_VARS:
        stage_env.pop(var_name, None)

    for var_name in AWS_CREDENTIAL_ENV_VARS:
        stage_env.pop(var_name, None)

    resolved = resolve_stage_aws_access(
        stage,
        execution_identities,
        aws_credential_sources,
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=account_registry,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=ctl_role_chain,
        stage_roles=stage_roles,
    )
    if resolved is None:
        return

    stage_env["AWS_EC2_METADATA_DISABLED"] = "true"
    stage_env["ATLAS_AWS_ASSERT_ACCESS"] = "true"
    stage_env["ATLAS_EXECUTION_IDENTITY_KEY"] = resolved["execution_identity_key"]

    if resolved.get("identity_bypass") == "true":
        stage_env["AWS_PROFILE"] = resolved["profile_name"]
        if run_dir is not None:
            materialize_profile_binding(
                run_dir / "provider_binding" / stage_id, resolved["profile_name"], stage_env
            )
        stage_env["ATLAS_AWS_PROFILE_ONLY_ACCESS"] = "true"
        logging.info(
            "Resolved substitute-credential access for stage %s (bypass): profile=%s",
            stage_id,
            resolved["profile_name"],
        )
        return

    stage_env["ATLAS_AWS_ACCOUNT_KEY"] = resolved["account_key"]
    stage_env["ATLAS_AWS_CREDENTIAL_SOURCE_KEY"] = resolved["credential_source_key"]
    stage_env["ATLAS_AWS_IMPLEMENTATION_KEY"] = resolved["implementation_key"]
    stage_env["ATLAS_AWS_EXPECT_ACCOUNT_ID"] = resolved["expected_account_id"]

    if resolved["credential_provider_kind"] == "role_chain":
        # Standard mode: hand the stage the FINAL role's assumed credentials
        # (produced by iterating the ordered hop path); the stage asserts
        # account + final-role principal.
        chain_creds = assume_ctl_role_chain(
            resolved["entry_profile_name"],
            resolved["hop_role_arns"],
            session_name=f"atlas-ctl-{stage_id}"[:64],
            entry_expected_account_id=resolved["entry_account_id"],
            entry_permission_set_name=resolved.get("entry_permission_set_name"),
            entry_role_name=resolved.get("entry_role_name"),
        )
        stage_env.update(chain_creds)
        stage_env["ATLAS_AWS_ROLE_CHAIN"] = "true"
        stage_env["ATLAS_AWS_EXPECT_ROLE_NAME"] = resolved["role_name"]
        logging.info(
            "Resolved AWS standard (role-path) access for stage %s: entry=%s hops=%s",
            stage_id,
            resolved["entry_profile_name"],
            resolved["hop_role_arns"],
        )
        return

    # Direct mode: the stage asserts BOTH facts — destination account and the
    # credential source's declared principal (Phase 14).
    stage_env["AWS_PROFILE"] = resolved["profile_name"]
    if resolved.get("permission_set_name"):
        stage_env["ATLAS_AWS_EXPECT_PERMISSION_SET_NAME"] = resolved["permission_set_name"]
    if resolved.get("role_name"):
        stage_env["ATLAS_AWS_EXPECT_ROLE_NAME"] = resolved["role_name"]
    if run_dir is not None:
        materialize_profile_binding(
            run_dir / "provider_binding" / stage_id, resolved["profile_name"], stage_env
        )

    logging.info(
        "Resolved AWS access for stage %s: execution_identity_key=%s account_key=%s "
        "credential_source_key=%s implementation_key=%s credential_provider_kind=%s expected_account_id=%s",
        stage_id,
        resolved["execution_identity_key"],
        resolved["account_key"],
        resolved["credential_source_key"],
        resolved["implementation_key"],
        resolved["credential_provider_kind"],
        resolved["expected_account_id"],
    )


def derive_provider_account_fact(
    execution_context: dict[str, object],
    workflow_cfg: dict,
    inventory_cfg: dict,
    ctl_cfg_root: Path,
) -> None:
    """Provider-adapter hook: derive read-only execution_context.provider.* facts.

    AWS resolves each active target's execution_identity_key account_key through
    providers.aws.accounts and exposes `execution_context.provider.account_id`
    when the active targets resolve exactly ONE registry account id. When they
    do not (no identities, mixed accounts, or unpopulated registry ids), the
    fact stays absent: a rendered scope referencing it then hard-fails with the
    standard missing-reference error. CLI/cfg params can never write the
    `provider` namespace (params only ever land in execution_context.params.*).
    """
    identities = common.load_execution_identities_cfg(ctl_cfg_root)
    registry = load_aws_account_registry_cfg(ctl_cfg_root) or {}
    targets = inventory_cfg.get("stage_targets", {})
    account_ids: set[str] = set()
    for target_name in common.active_target_names(workflow_cfg):
        target_cfg = targets.get(target_name) or {}
        identity_ref = target_cfg.get("execution_identity_key")
        if identity_ref is None:
            continue
        identity_key = common.resolve_runtime_scalar(
            identity_ref, execution_context,
            label=f"target {target_name} execution_identity_key",
        )
        identity_cfg = identities.get(identity_key) or {}
        if identity_cfg.get("provider") != "aws":
            continue
        account_key = identity_cfg.get("account_key")
        if not account_key:
            continue
        account_id = registry.get(account_key)
        if account_id:
            account_ids.add(str(account_id))
    if len(account_ids) == 1:
        execution_context[f"{common.EXECUTION_CONTEXT_ROOT}.provider.account_id"] = account_ids.pop()
        logging.info(
            "Derived provider fact execution_context.provider.account_id=%s",
            execution_context[f"{common.EXECUTION_CONTEXT_ROOT}.provider.account_id"],
        )


class CtlStateSyncer:
    """Incremental mirror of the local ctl-state tree to the domain bucket.

    Forward sync is add/update only — never deletes remote objects (the local
    root is ephemeral; remote cleanup is bucket lifecycle rules only).
    """

    STATE_LAYER_INCLUDES = ("*/RUN.yaml", "*/STATUS.yaml", "*/MANIFEST.yaml")

    def __init__(self, results_root: Path, bucket_name: str, bucket_region: str, aws_profile: str, *, required: bool):
        self.results_root = Path(results_root).resolve()
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.aws_profile = aws_profile
        self.required = required
        self.state = "pending"
        self.detail: str | None = None
        self.ready = False

    def _aws_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["AWS_PROFILE"] = self.aws_profile
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
        """Confirm the bucket exists, re-checked at every sync point (not once at
        run start). On first confirmation, hydrate the state layer. A run that
        *creates* its bucket therefore mirrors itself at finalization; only a run
        whose bucket never appears stays local. Mirrors `terraform init
        -migrate-state`: create with a local backend, migrate in once it exists."""
        if self.ready:
            return True
        if not self.bucket_exists():
            self.state = "local"
            self.detail = f"{reason}: bucket s3://{self.bucket_name} not present yet"
            if self.required:
                logging.warning(
                    "ctl-state bucket s3://%s not present at %r; results stay local (bootstrap run?)",
                    self.bucket_name,
                    reason,
                )
            return False
        self.ready = True
        self.pull_state_layer()
        return True

    def pull_state_layer(self) -> None:
        """Reverse sync: hydrate slots + RUN/STATUS/MANIFEST from the bucket (small files only)."""
        args = ["s3", "sync", f"s3://{self.bucket_name}", str(self.results_root), "--exclude", "*"]
        for pattern in self.STATE_LAYER_INCLUDES:
            args += ["--include", pattern]
        result = self._run_aws(args)
        if result.returncode != 0:
            self._fail("state pull", result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error")
        else:
            logging.info("Ctl results state layer pulled from s3://%s", self.bucket_name)

    def push(self, reason: str) -> None:
        """Forward incremental mirror of the whole local results tree (never deletes)."""
        if not self.ensure_ready(f"push ({reason})"):
            return
        result = self._run_aws(["s3", "sync", str(self.results_root), f"s3://{self.bucket_name}", "--no-progress"])
        if result.returncode != 0:
            self._fail(f"push ({reason})", result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "unknown error")
        else:
            self.state = "synced"
            self.detail = reason
            logging.info("Ctl-state synced to s3://%s (%s)", self.bucket_name, reason)

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
    load_aws_stage_roles_cfg(ctl_cfg_root)
    load_aws_ctl_state_synchronizer_roles_cfg(ctl_cfg_root)


def validate_execution_identity(identity_key: str, identity_cfg: dict, ctl_cfg_root: Path) -> None:
    """Validate the AWS identity payload (everything except the generic envelope)."""
    for removed_field in ("access_context_key", "direct_access_context_key"):
        if removed_field in identity_cfg:
            raise RuntimeError(
                f"❌ execution identity {identity_key!r} uses removed {removed_field}; "
                "rename it to direct_credential_source_key (§Skip Model D)"
            )
    allowed_fields = {"provider", "account_key", "direct_credential_source_key", "ctl_stage_role_key"}
    unknown = sorted(set(identity_cfg) - allowed_fields)
    if unknown:
        raise RuntimeError(f"❌ execution identity {identity_key!r} has unknown fields {unknown}")
    common._require_non_empty_string(
        identity_cfg.get("account_key"),
        f"execution_identities.{identity_key}.account_key",
        ctl_cfg_root,
    )
    for optional_field in ("direct_credential_source_key", "ctl_stage_role_key"):
        if optional_field in identity_cfg:
            common._require_non_empty_string(
                identity_cfg.get(optional_field),
                f"execution_identities.{identity_key}.{optional_field}",
                ctl_cfg_root,
            )


def load_runtime_catalogs(ctl_cfg_root: Path) -> dict:
    """Load the run-scoped AWS catalogs bundle (opaque to the engine core)."""
    return {
        "execution_identities": common.load_execution_identities_cfg(ctl_cfg_root),
        "credential_sources": load_aws_credential_sources_cfg(ctl_cfg_root),
        "account_registry": load_aws_account_registry_cfg(ctl_cfg_root),
        "ctl_role_chain": load_aws_ctl_role_chain_cfg(ctl_cfg_root),
        "stage_roles": load_aws_stage_roles_cfg(ctl_cfg_root),
    }


def validate_active_stage_access(
    active_stages: dict,
    catalogs: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
) -> None:
    catalogs["validated_account_registry"] = validate_active_stage_aws_access(
        active_stages,
        catalogs["execution_identities"],
        catalogs["credential_sources"],
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=catalogs["account_registry"],
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=catalogs["ctl_role_chain"],
        stage_roles=catalogs["stage_roles"],
    )


def materialize_stage_binding(
    stage_id: str,
    stage: dict,
    stage_env: dict[str, str],
    catalogs: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    run_dir: Path | None = None,
) -> None:
    configure_stage_aws_env(
        stage_id,
        stage,
        stage_env,
        catalogs["execution_identities"],
        catalogs["credential_sources"],
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=catalogs.get("validated_account_registry") or catalogs["account_registry"],
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        ctl_role_chain=catalogs["ctl_role_chain"],
        stage_roles=catalogs["stage_roles"],
        run_dir=run_dir,
    )


def stage_assertion_argv(stage_utils_dir: Path) -> list[str]:
    return ["python3", str(stage_utils_dir / "assert_aws_access.py")]


def validate_state_backend_entry(domain: str, entry: dict, path) -> None:
    if entry.get("backend_type", "").strip() != "s3":
        raise RuntimeError(
            f"❌ ctl_state_backends.{domain} backend_type {entry.get('backend_type')!r} is not supported "
            f"by the aws adapter; available: s3 ({path})"
        )


def resolve_synchronizer_credential(
    identity_key: str,
    ctl_cfg_root: Path,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
) -> str:
    if execution_access_mode == "bypass":
        if not provider_credential:
            raise RuntimeError(
                "❌ bypass execution access requires the --provider-credential substitute credential"
            )
        return provider_credential.strip()
    identities = common.load_execution_identities_cfg(ctl_cfg_root)
    credential_sources = load_aws_credential_sources_cfg(ctl_cfg_root)
    account_registry = load_aws_account_registry_cfg(ctl_cfg_root)
    # Synchronizer standard-chain leg (entry -> runner -> synchronizer role) is
    # Phase 6; until then the synchronizer resolves in direct mode.
    resolved = resolve_stage_aws_access(
        {"execution_identity_key": identity_key},
        identities,
        credential_sources,
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=account_registry,
        execution_access_mode="direct",
    )
    if not resolved or not resolved.get("profile_name"):
        raise RuntimeError(
            f"❌ ctl-state synchronizer identity {identity_key!r} did not resolve to an AWS profile"
        )
    # Phase 14: assert the synchronizer's actual caller (account + principal)
    # with the shared implementation before arming the syncer.
    assert_profile_caller(
        resolved["profile_name"],
        expected_account_id=resolved["expected_account_id"],
        expect_principal={
            key: resolved[key] for key in ("permission_set_name", "role_name") if resolved.get(key)
        },
        label=f"ctl-state synchronizer identity {identity_key!r}",
    )
    return resolved["profile_name"]


def create_state_syncer(results_root, bucket_name: str, bucket_region: str, credential: str, *, required: bool):
    return CtlStateSyncer(results_root, bucket_name, bucket_region, credential, required=required)


def derive_provider_facts(execution_context, workflow_cfg, inventory_cfg, ctl_cfg_root) -> None:
    derive_provider_account_fact(execution_context, workflow_cfg, inventory_cfg, ctl_cfg_root)


def synthesize_validation_provider_facts(execution_context: dict[str, object], ctl_cfg_root: Path) -> None:
    """Provider fact for validation renders: real runs derive it from the run's
    identities; here it comes from the accounts registry via params.account when
    resolvable, a uniform registry id, or a placeholder."""
    registry = load_aws_account_registry_cfg(ctl_cfg_root)
    account_key = execution_context.get("execution_context.params.account")
    unique_ids = set(registry.values())
    execution_context["execution_context.provider.account_id"] = (
        registry.get(account_key) if account_key and registry.get(account_key)
        else (unique_ids.pop() if len(unique_ids) == 1 else "000000000000")
    )


def normalize_provider_credential(value: str | None) -> str | None:
    return normalize_optional_aws_profile(value)
