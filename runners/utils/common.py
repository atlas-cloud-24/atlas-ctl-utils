"""Shared utilities for local runner entrypoints."""

import argparse
import collections
import fcntl
import functools
import hashlib
import json
import logging
import logging.handlers
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml

from utils.git_meta import write_git_meta_to_file

REQUIRED_TOOLING_REFS = ("ctl-utils", "plt-utils")
ADAPTER_DIR = "atlas_ctl_adapter"
TOOLING_ENV_PREFIXES = {
    "ctl-utils": "ATLAS_CTL_UTILS",
    "plt-utils": "ATLAS_PLT_UTILS",
}
TOOLING_DEFAULT_REPO_URLS = {
    "ctl-utils": "https://github.com/atlas-cloud-24/atlas-ctl-utils.git",
    "plt-utils": "https://github.com/atlas-cloud-24/atlas-plt-utils.git",
}
RUN_ACTIONS = ("pipeline", "maintenance")
MAINTENANCE_ACTIONS = ("force-unlock",)
FORCE_UNLOCK_STAGE_SCRIPT_CANDIDATES = (
    Path("atlas_ctl_adapter/stages/plan/infra/src/stage.sh"),
    Path("atlas_ctl_adapter/stages/provision/infra/src/stage.sh"),
    Path("atlas_ctl_adapter/stages/destroy/infra/src/stage.sh"),
)
FORCE_UNLOCK_KEY_RE = re.compile(r"\./bin/tf\.sh\s+infra\s+init\s+\$?([A-Za-z_][A-Za-z0-9_]*)")
FORCE_UNLOCK_URI_RE = re.compile(r'echo\s+"Using\s+\$([A-Za-z_][A-Za-z0-9_]*)"')
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
    "ATLAS_EXECUTION_IDENTITY_KEY",
    "ATLAS_AWS_ACCOUNT_KEY",
    "ATLAS_AWS_ACCESS_CONTEXT_KEY",
    "ATLAS_AWS_IMPLEMENTATION_KEY",
    "ATLAS_AWS_EXPECT_ACCOUNT_ID",
    "ATLAS_AWS_EXPECT_PERMISSION_SET_NAME",
    "ATLAS_AWS_EXPECT_ROLE_NAME",
)

SERVICE_ID = "atlas-ctl-orchestrator-local"
CTL_RESULTS_LOCK_FILENAME = ".ctl.lock"
CTL_RESULTS_LOCK_META_FILENAME = ".ctl.lock.yaml"
RUN_METADATA_FILENAME = "RUN.yaml"
EXECUTION_CONTEXT_FILENAME = "execution_context.yaml"

PLT_GUARDRAILS_FILENAME = "__guardrails__.yaml"
PLT_GUARDRAILS_DIRNAME = "__guardrails__"
CFG_ROOT_META_FILENAME = "__cfg__.yaml"
MUTATING_ACTIONS = ("provision", "destroy")
_UUID7_LAST_TIMESTAMP_MS = -1
_UUID7_COUNTER = 0

# ANSI escape code pattern
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')


class UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def merge_cfg_values(base, overlay):
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            if key in merged:
                merged[key] = merge_cfg_values(merged[key], value)
            else:
                merged[key] = value
        return merged
    return overlay


def load_cfg_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    if not raw.strip():
        return {}

    data = yaml.load(raw, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"cfg file must contain a mapping: {path}")
    return data


def render_merged_cfg_header(
    dest_path: str | Path,
    sources: list[str],
    source_log_roots: tuple[Path, ...] = (),
    dest_log_roots: tuple[Path, ...] = (),
) -> str:
    rendered_dest = format_path_for_log(dest_path, dest_log_roots)
    rendered_sources = [format_path_for_log(src, source_log_roots) for src in sources]

    dest_rel = Path(rendered_dest)
    section_name = dest_rel.parent.name if dest_rel.parent.name else dest_rel.stem
    section_name = section_name.replace("_", " ").upper()

    lines = [
        "###################################",
        f"# {section_name}",
        "###################################",
        "# =================================",
        f"# {dest_rel.stem} ({rendered_dest})",
        "# =================================",
        "# merged from:",
    ]
    lines.extend(f"# - {src}" for src in rendered_sources)
    return "\n".join(lines) + "\n\n"


def write_cfg_yaml(path: str, data: dict, *, header_comment: str | None = None) -> None:
    rendered = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    with open(path, "w", encoding="utf-8") as f:
        if header_comment:
            f.write(header_comment)
        f.write(rendered)


def bool2str(v: bool) -> str:
    """Convert boolean to 'true'/'false' string."""
    if isinstance(v, bool):
        return "true" if v else "false"
    raise argparse.ArgumentTypeError(f"Expected bool, got: {type(v).__name__} ({v!r})")

def str2bool(v: str) -> bool:
    """Convert 'true'/'false' string to boolean."""
    if isinstance(v, str):
        value = v.lower()
        if value == "true":
            return True
        if value == "false":
            return False

    raise argparse.ArgumentTypeError(
        f"Expected 'true' or 'false', got: {type(v).__name__} ({v!r})"
    )

def validate_uuid7(v: str) -> str:
    """Validate that a string is a valid UUID version 7."""
    try:
        parsed = uuid.UUID(v)
        if parsed.version != 7:
            raise argparse.ArgumentTypeError(f"UUID must be version 7, got version {parsed.version}: {v}")
        return v
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid UUID format: {v}")

def parse_selector_arg(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(f"Selector must use key=value format, got: {value!r}")
    key, selector_value = value.split("=", 1)
    key = key.strip()
    selector_value = selector_value.strip()
    if not key or not selector_value:
        raise argparse.ArgumentTypeError(f"Selector must use non-empty key=value, got: {value!r}")
    return key, selector_value


def selectors_to_map(items: list[tuple[str, str]], *, label: str) -> dict[str, str]:
    selectors: dict[str, str] = {}
    for key, value in items:
        if key in selectors:
            raise RuntimeError(f"❌ duplicate {label} selector {key!r}")
        selectors[key] = value
    return selectors


def require_selector(selectors: dict[str, str], key: str, *, label: str) -> str:
    value = selectors.get(key)
    if not value:
        raise RuntimeError(f"❌ missing required {label} selector {key!r}")
    return value


def load_cfg_root_meta(cfg_root: Path) -> dict:
    path = cfg_root / CFG_ROOT_META_FILENAME
    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ {CFG_ROOT_META_FILENAME} must contain a mapping: {path}")
    return data


def collect_top_level_sections(cfg_root: Path, key: str) -> list[tuple[Path, object]]:
    sections: list[tuple[Path, object]] = []
    for yf in sorted(cfg_root.rglob("*.yaml")):
        data = load_yaml(yf) or {}
        if not isinstance(data, dict):
            continue
        if key in data:
            sections.append((yf, data[key]))
    return sections


def load_ctl_profiles(ctl_cfg_root: Path) -> dict[str, dict]:
    """Load the ctl profile catalog (content key: ctl_profiles) — named policy
    bundles governing ctl behavior (ref_policy, allow_aws_profile_only,
    allow_skip_ctl_state_backend_sync, ...)."""
    profiles: dict[str, dict] = {}
    for path, section in collect_top_level_sections(ctl_cfg_root, "ctl_profiles"):
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ ctl_profiles must be a mapping: {path}")
        for profile_name, policy in section.items():
            if profile_name in profiles:
                raise RuntimeError(f"❌ duplicate ctl profile {profile_name!r}: {path}")
            if not isinstance(profile_name, str) or not profile_name.strip():
                raise RuntimeError(f"❌ ctl profile names must be non-empty strings: {path}")
            if policy is not None and not isinstance(policy, dict):
                raise RuntimeError(f"❌ ctl profile {profile_name!r} policy must be a mapping: {path}")
            profiles[profile_name] = policy or {}
    return profiles


def ctl_profile_policy(ctl_cfg_root: Path, ctl_profile: str) -> dict:
    profiles = load_ctl_profiles(ctl_cfg_root)
    if ctl_profile not in profiles:
        known = ", ".join(sorted(profiles)) or "none"
        raise RuntimeError(f"❌ unknown ctl profile {ctl_profile!r}; known profiles: {known}")
    return profiles[ctl_profile]


def ctl_ref_policy(ctl_cfg_root: Path, ctl_profile: str) -> str:
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    ref_policy = policy.get("ref_policy")
    if not isinstance(ref_policy, str) or not ref_policy.strip():
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} must define non-empty ref_policy")
    return ref_policy.strip()


def ctl_allows_aws_profile_only(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    return policy.get("allow_aws_profile_only") is True


def ref_policy_requires_commits(ref_policy: str) -> bool:
    return ref_policy == "commit_required"


def load_execution_context_constraints(ctl_cfg_root: Path) -> list[dict]:
    constraint_entries: list[tuple[dict, Path]] = []
    for path, section in collect_top_level_sections(ctl_cfg_root, "execution_context_constraints"):
        if not isinstance(section, list):
            raise RuntimeError(f"❌ execution_context_constraints must be a list: {path}")
        constraint_entries.extend((constraint, path) for constraint in section)

    constraints: list[dict] = []
    for idx, (constraint, path) in enumerate(constraint_entries, start=1):
        if not isinstance(constraint, dict):
            raise RuntimeError(f"❌ execution context constraint #{idx} must be a mapping: {path}")
        when = constraint.get("when") or {}
        require_present = constraint.get("require_present") or []
        allowed_values = constraint.get("allowed_values") or {}
        if not isinstance(when, dict):
            raise RuntimeError(f"❌ execution context constraint #{idx} when must be a mapping: {path}")
        if not isinstance(require_present, list) or not all(isinstance(item, str) and item for item in require_present):
            raise RuntimeError(f"❌ execution context constraint #{idx} require_present must be a list of non-empty strings: {path}")
        if not isinstance(allowed_values, dict):
            raise RuntimeError(f"❌ execution context constraint #{idx} allowed_values must be a mapping: {path}")
        constraints.append(constraint)
    return constraints


def validate_execution_context_constraints(ctl_cfg_root: Path, execution_context: dict[str, object]) -> None:
    for idx, constraint in enumerate(load_execution_context_constraints(ctl_cfg_root), start=1):
        when = constraint.get("when") or {}
        if not selector_matches(when, execution_context, label=f"execution_context_constraints[{idx}].when"):
            continue

        for ref in constraint.get("require_present") or []:
            validate_execution_context_ref(ref, label=f"execution_context_constraints[{idx}].require_present")
            if ref not in execution_context:
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} requires {ref!r} when {when} matches; "
                    f"{execution_context_miss_message(execution_context, ref)}"
                )

        for ref, expected in (constraint.get("allowed_values") or {}).items():
            validate_execution_context_ref(ref, label=f"execution_context_constraints[{idx}].allowed_values")
            allowed = selector_expected_values(expected, label=f"execution_context_constraints[{idx}].allowed_values.{ref}")
            if ref in execution_context and str(execution_context[ref]) not in allowed:
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} allows {ref} only in {allowed}, got {execution_context[ref]!r}"
                )

def normalize_ctl_state_local_root(value: str) -> Path:
    """Normalize the operator-provided local ctl-state root directory."""
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("❌ --ctl-state-local-root must be a non-empty directory path")
    root = Path(value.strip()).expanduser().resolve()
    if root.exists() and not root.is_dir():
        raise RuntimeError(f"❌ --ctl-state-local-root exists but is not a directory: {root}")
    return root


def normalize_optional_aws_profile(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("❌ --aws-profile must be a non-empty profile name when provided")
    return value.strip()


def finalize_common_args(args: argparse.Namespace) -> None:
    """Normalize execution-params CLI args into a map and common values."""
    args.execution_params = selectors_to_map(args.execution_param, label="execution param")
    args.ctl_state_local_root = normalize_ctl_state_local_root(args.ctl_state_local_root)
    args.aws_profile = normalize_optional_aws_profile(args.aws_profile)
    args.run_id = generate_uuid7()


def _uuid7_timestamp_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000) & ((1 << 48) - 1)


def generate_uuid7() -> str:
    """Generate a monotonic UUIDv7 string for one ctl run execution."""
    global _UUID7_LAST_TIMESTAMP_MS, _UUID7_COUNTER

    timestamp_ms = _uuid7_timestamp_ms()
    if timestamp_ms > _UUID7_LAST_TIMESTAMP_MS:
        _UUID7_LAST_TIMESTAMP_MS = timestamp_ms
        _UUID7_COUNTER = 0
    else:
        timestamp_ms = _UUID7_LAST_TIMESTAMP_MS
        _UUID7_COUNTER += 1
        if _UUID7_COUNTER >= (1 << 12):
            while timestamp_ms <= _UUID7_LAST_TIMESTAMP_MS:
                time.sleep(0.001)
                timestamp_ms = _uuid7_timestamp_ms()
            _UUID7_LAST_TIMESTAMP_MS = timestamp_ms
            _UUID7_COUNTER = 0

    rand_a = _UUID7_COUNTER
    rand_b = uuid.uuid4().int & ((1 << 62) - 1)
    value = (timestamp_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return str(uuid.UUID(int=value))


def load_yaml(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    if not raw.strip():
        return {}

    data = yaml.load(raw, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML file must contain a mapping: {path}")
    return data


def format_path_for_log(path: str | Path, relative_roots: tuple[Path, ...] = ()) -> str:
    """Prefer a relative display path when the path is under a known root."""
    path_obj = Path(path).expanduser()
    if not path_obj.is_absolute():
        return str(path_obj)

    for root in relative_roots:
        try:
            return str(path_obj.relative_to(root))
        except ValueError:
            continue

    return str(path_obj)


def strip_ansi(text: str) -> str:
    """Remove ANSI color codes from text."""
    return ANSI_ESCAPE.sub('', text)


def log_stage_banner(stage_id: str, *, ch: str = "#", min_width: int = 70) -> None:
    title = f" {stage_id} "
    width = max(min_width, len(title) + 2)  # ensure it always fits
    line = ch * width
    mid  = title.center(width, ch)
    logging.info(line)
    logging.info(mid)
    logging.info(line)


def run_and_log(cmd, shell=False, cwd=None, env=None, check=True):
    """Run subprocess and log all output in real-time."""
    process = subprocess.Popen(
        cmd,
        shell=shell,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1  # Line buffered
    )

    # Stream output in real-time
    for line in process.stdout:
        line_stripped = line.rstrip()
        # Print colored output to terminal
        print(f"  {line_stripped}", flush=True)
        # Log clean output to file (strip ANSI codes)
        clean_line = strip_ansi(line_stripped)
        # Only log to file handlers, not console
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                handler.emit(logging.LogRecord(
                    name=logging.getLogger().name,
                    level=logging.INFO,
                    pathname="",
                    lineno=0,
                    msg=f"  {clean_line}",
                    args=(),
                    exc_info=None
                ))

    # Wait for process to complete
    returncode = process.wait()

    if check and returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)

    return returncode


def validate_workflow_args(args: argparse.Namespace) -> None:
    """Validate args for a declared workflow run."""
    if not getattr(args, "workflow", None):
        raise RuntimeError("❌ workflow runner requires --workflow")
    if getattr(args, "target", None):
        raise RuntimeError("❌ workflow runner does not accept --target")
    if any(getattr(args, field, None) for field in ("source", "ref", "cfg_file_set", "sub_workflow", "execution_identity_key", "ctl_state_backend_key", "affected_target_keys")):
        raise RuntimeError("❌ workflow runner does not accept sub-workflow synthetic target args")


def validate_target_args(args: argparse.Namespace) -> None:
    """Validate args for a declared single-target run."""
    if not getattr(args, "target", None):
        raise RuntimeError("❌ target runner requires --target")
    if getattr(args, "workflow", None):
        raise RuntimeError("❌ target runner does not accept --workflow")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for target runs")
    if any(getattr(args, field, None) for field in ("source", "ref", "cfg_file_set", "sub_workflow", "execution_identity_key", "ctl_state_backend_key", "affected_target_keys")):
        raise RuntimeError("❌ target runner does not accept sub-workflow synthetic target args")


def validate_maintenance_args(args: argparse.Namespace) -> None:
    """Validate args for a maintenance run."""
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for maintenance")
    if any(getattr(args, field, None) for field in ("source", "ref", "cfg_file_set", "sub_workflow", "execution_identity_key", "ctl_state_backend_key", "affected_target_keys")):
        raise RuntimeError("❌ maintenance runner does not accept sub-workflow synthetic target args")
    if not getattr(args, "maintenance_action", None):
        raise RuntimeError("❌ --maintenance-action is required for maintenance")
    if args.maintenance_action == "force-unlock" and not getattr(args, "lock_id", None):
        raise RuntimeError("❌ --lock-id is required for --maintenance-action=force-unlock")
    if args.maintenance_action == "force-unlock" and ctl_state_lock_matches(args.ctl_state_local_root, args.lock_id):
        return
    if not getattr(args, "target", None):
        raise RuntimeError("❌ --target is required for maintenance")


def validate_sub_workflow_args(args: argparse.Namespace) -> None:
    """Validate args for a synthetic repo-local sub_workflow run."""
    if getattr(args, "workflow", None) or getattr(args, "target", None):
        raise RuntimeError("❌ sub_workflow runner does not accept --workflow or --target")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for sub_workflow runs")
    missing = [f for f in ("source", "ref", "cfg_file_set", "sub_workflow") if not getattr(args, f, None)]
    if missing:
        raise RuntimeError(
            "❌ sub_workflow needs " + ", ".join(f"--{m.replace('_', '-')}" for m in missing)
        )
    identity_key = getattr(args, "execution_identity_key", None)
    if identity_key is not None and not identity_key.strip():
        raise RuntimeError("❌ --execution-identity-key must be a non-empty string when provided")
    affected_target_keys = getattr(args, "affected_target_keys", None) or []
    if affected_target_keys:
        args.affected_target_keys = normalize_target_keys(affected_target_keys, label="--affected-target-key")
    if args.action in MUTATING_ACTIONS and not getattr(args, "affected_target_keys", None):
        raise RuntimeError("❌ mutating sub_workflow runs require at least one --affected-target-key")





def validate_stages_have_commits(active_stages: dict, ref_policy: str) -> None:
    """Validate that all resolved stages and modules have explicit commits when required.

    Commit-required policy disallows branch references for executable code. Validation
    runs after workflow patches and refs have been resolved into active stages.
    """
    if not ref_policy_requires_commits(ref_policy):
        return

    stages_without_commit = []
    modules_without_commit = []
    for stage_id, stage_cfg in active_stages.items():
        if not stage_cfg.get("commit"):
            stages_without_commit.append(stage_id)

        raw_modules = stage_cfg.get("modules") or {}
        if not isinstance(raw_modules, dict):
            modules_without_commit.append(f"{stage_id}:<invalid-modules>")
            continue

        for module_name, module_cfg in raw_modules.items():
            if not module_cfg.get("commit"):
                modules_without_commit.append(f"{stage_id}:{module_name}")

    if stages_without_commit or modules_without_commit:
        details = []
        if stages_without_commit:
            details.append(f"Stages missing 'commit': {stages_without_commit}")
        if modules_without_commit:
            details.append(f"Modules missing 'commit': {modules_without_commit}")
        raise RuntimeError(
            "❌ ref_policy=commit_required requires all stages and modules to have explicit 'commit' specified.\n"
            f"   {'; '.join(details)}\n"
            "   Using branch references is not allowed for reproducibility."
        )

def validate_cfg_refs_have_commits(
    ref_policy: str,
    ctl_cfg_branch: str | None,
    ctl_cfg_commit: str | None,
    plt_cfg_branch: str | None,
    plt_cfg_commit: str | None,
) -> None:
    """Validate that cfg repos use commits when ref_policy requires it."""
    if not ref_policy_requires_commits(ref_policy):
        return

    errors = []
    if ctl_cfg_branch and not ctl_cfg_commit:
        errors.append(f"--ctl-cfg uses branch='{ctl_cfg_branch}' but commit is required")
    if plt_cfg_branch and not plt_cfg_commit:
        errors.append(f"--plt-cfg uses branch='{plt_cfg_branch}' but commit is required")

    if errors:
        raise RuntimeError(
            "❌ ref_policy=commit_required requires cfg repos to use @commit=sha (not @branch=name).\n"
            f"   {'; '.join(errors)}"
        )


def validate_tooling_refs_have_commits(tooling_refs: dict, ref_policy: str) -> None:
    """Validate that tooling refs use commits when ref_policy requires it."""
    if not ref_policy_requires_commits(ref_policy):
        return

    errors = []
    for tooling_name in REQUIRED_TOOLING_REFS:
        tooling_ref = tooling_refs.get(tooling_name) or {}
        if not isinstance(tooling_ref, dict):
            errors.append(f"tooling '{tooling_name}' ref must be a mapping")
            continue

        if tooling_ref.get("commit"):
            continue

        if tooling_ref.get("branch"):
            errors.append(f"tooling '{tooling_name}' uses branch='{tooling_ref['branch']}' but commit is required")
        else:
            errors.append(f"tooling '{tooling_name}' is missing commit")

    if errors:
        raise RuntimeError(
            "❌ ref_policy=commit_required requires tooling refs to use commits:\n"
            f"   {'; '.join(errors)}"
        )


def git_clone(repo_url: str, branch: str | None, commit: str | None, dest: Path, token: str | None = None):
    env = os.environ.copy()
    askpass_path: str | None = None
    if token:
        fd, askpass_path = tempfile.mkstemp(suffix=".sh")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(
                    "#!/bin/sh\n"
                    "case \"$1\" in\n"
                    "  *Username*) printf '%s\\n' \"${GIT_HTTP_USERNAME:-x-access-token}\" ;;\n"
                    "  *Password*) printf '%s\\n' \"${GIT_HTTP_PASSWORD:-}\" ;;\n"
                    "  *) printf '\\n' ;;\n"
                    "esac\n"
                )
            os.chmod(askpass_path, 0o700)
        except Exception:
            os.unlink(askpass_path)
            raise

        env["GIT_ASKPASS"] = askpass_path
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_HTTP_USERNAME"] = "x-access-token"
        env["GIT_HTTP_PASSWORD"] = token

    try:
        # commit pinned → checkout exact commit
        if commit:
            cmd = ["git", "clone", repo_url, str(dest)]
            logging.info(f"Running command: git clone {repo_url} {dest}")
            run_and_log(cmd, env=env)

            cmd = f"git checkout {commit}"
            logging.info(f"Running command: {cmd}")
            run_and_log(cmd.split(), cwd=dest, env=env)
            return

        # no commit → use branch HEAD
        if not branch:
            raise RuntimeError(f"❌ Either branch or commit must be provided for repo {repo_url}")

        cmd = ["git", "clone", "--branch", branch, "--depth", "1", repo_url, str(dest)]
        logging.info(f"Running command: git clone --branch {branch} --depth 1 {repo_url} {dest}")
        run_and_log(cmd, env=env)
    finally:
        if askpass_path:
            os.unlink(askpass_path)


def parse_repo_url_ref(value: str) -> tuple[str, str | None, str | None]:
    """
    Parse URL@branch=name or URL@commit=sha format into (url, branch, commit).

    Examples:
        https://github.com/org/repo@branch=main -> (url, "main", None)
        https://github.com/org/repo@commit=abc123 -> (url, None, "abc123")

    Returns:
        tuple: (repo_url, branch, commit) where one of branch/commit is None
    """
    if '@' not in value:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Expected URL@branch=name or URL@commit=sha"
        )

    # Split on last @ to handle URLs that might contain @
    idx = value.rfind('@')
    repo_url = value[:idx]
    ref_part = value[idx + 1:]

    if not repo_url or not ref_part:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Both URL and ref are required."
        )

    parsed = urlparse(repo_url)
    if not parsed.scheme or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Remote cfg must use URL@branch=name or URL@commit=sha"
        )

    if ref_part.startswith("branch="):
        branch = ref_part[7:]  # len("branch=") = 7
        if not branch:
            raise argparse.ArgumentTypeError(f"Invalid format: '{value}'. Branch name cannot be empty.")
        return repo_url, branch, None
    elif ref_part.startswith("commit="):
        commit = ref_part[7:]  # len("commit=") = 7
        if not commit:
            raise argparse.ArgumentTypeError(f"Invalid format: '{value}'. Commit sha cannot be empty.")
        return repo_url, None, commit
    else:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Expected @branch=name or @commit=sha"
        )


def parse_relative_paths_arg(value: str, *, root_dir_name: str, item_label: str) -> list[str]:
    """Parse comma-separated relative paths under a cfg root directory."""
    if value is None:
        return []

    raw = [v.strip() for v in value.split(",") if v.strip()]
    if not raw:
        return []
    if len(raw) == 1 and raw[0].lower() in ("none", "null", "-"):
        return []

    for item in raw:
        path = Path(item)
        if path.is_absolute():
            raise argparse.ArgumentTypeError(
                f"{item_label} path must be relative to {root_dir_name}/: {item}"
            )
        if ".." in path.parts:
            raise argparse.ArgumentTypeError(
                f"{item_label} path must not contain '..': {item}"
            )

    duplicates = [item for item, count in collections.Counter(raw).items() if count > 1]
    if duplicates:
        raise argparse.ArgumentTypeError(
            f"{item_label} paths must be unique under {root_dir_name}/; duplicates: {', '.join(sorted(duplicates))}"
        )

    return raw


def parse_overlays_arg(value: str) -> list[str]:
    """Parse comma-separated plt overlay names."""
    if value is None:
        return []

    raw = [v.strip() for v in value.split(",") if v.strip()]
    if not raw:
        return []
    if len(raw) == 1 and raw[0].lower() in ("none", "null", "-"):
        return []

    for item in raw:
        if "/" in item or "\\" in item:
            raise argparse.ArgumentTypeError(
                f"Overlay must be a metadata name, not a path: {item}"
            )
        if item in (".", ".."):
            raise argparse.ArgumentTypeError(f"Overlay name is invalid: {item}")

    duplicates = [item for item, count in collections.Counter(raw).items() if count > 1]
    if duplicates:
        raise argparse.ArgumentTypeError(
            f"Overlay names must be unique; duplicates: {', '.join(sorted(duplicates))}"
        )

    return raw


def parse_ctl_variants_arg(value: str) -> list[str]:
    """Parse comma-separated ctl variant paths under variants/."""
    return parse_relative_paths_arg(
        value,
        root_dir_name="variants",
        item_label="Ctl variant",
    )

def build_active_stages(
    workflow_cfg: dict,
    inventory_cfg: dict,
    repo_key: str = "repo_url",
    require_branch_or_commit: bool = True,
    refs: dict | None = None,
    execution_context: dict[str, object] | None = None,
    require_commit_refs: bool = False,
) -> dict:
    inventory_stage_sources = inventory_cfg.get("stage_sources", {})
    if not isinstance(inventory_stage_sources, dict):
        raise RuntimeError("'stage_sources' in inventory must be a mapping: source -> meta")

    inventory_stage_targets = inventory_cfg.get("stage_targets", {})
    if not isinstance(inventory_stage_targets, dict):
        raise RuntimeError("'stage_targets' in inventory must be a mapping: target -> meta")

    refs = refs or {}
    scoped_refs = refs.get("scoped") or {}
    ref_context_values = execution_context or {}
    active = {}

    def normalize_cfg_root(raw_value, *, stage_target: str) -> str:
        value = raw_value if raw_value is not None else "/"
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Stage target {stage_target!r} cfg_root must be a non-empty string")
        value = value.strip()
        if "\\" in value:
            raise RuntimeError(f"Stage target {stage_target!r} cfg_root must use forward slashes: {value}")
        if not value.startswith("/"):
            raise RuntimeError(f"Stage target {stage_target!r} cfg_root must start with /: {value}")
        parts = [part for part in value.split("/") if part]
        if any(part in (".", "..") for part in parts):
            raise RuntimeError(f"Stage target {stage_target!r} cfg_root must not contain . or ..: {value}")
        # cfg_root is freed (Phase 2d): any safe path incl. "/" (root) and multi-segment; it no
        # longer must be a single scope segment and is independent of the target's ref/context.
        return "/" + "/".join(parts)

    for st in workflow_cfg.get("stages", []):
        if isinstance(st, str):
            stage_id = st
            stage_target = st
            stage_over = {}
        else:
            stage_id = st.get("id")
            if not stage_id:
                raise RuntimeError("Stage entry missing required field 'id'")
            stage_target = st.get("target")
            if not stage_target:
                raise RuntimeError(f"Stage {stage_id!r} has empty 'target'")
            stage_over = st

        if stage_target not in inventory_stage_targets:
            raise RuntimeError(
                f"Stage target {stage_target!r} (stage id={stage_id!r}) not found in inventory {workflow_cfg.get('inventory')!r}"
            )

        target_cfg = inventory_stage_targets[stage_target]
        if not isinstance(target_cfg, dict):
            raise RuntimeError(
                f"Stage target {stage_target!r} metadata must be a mapping, got: {type(target_cfg).__name__}"
            )

        stage_source = target_cfg.get("source")
        if not isinstance(stage_source, str) or not stage_source:
            raise RuntimeError(f"Stage target {stage_target!r} must define non-empty 'source'")
        if stage_source not in inventory_stage_sources:
            raise RuntimeError(
                f"Stage target {stage_target!r} references missing source {stage_source!r} in inventory {workflow_cfg.get('inventory')!r}"
            )

        source_cfg = inventory_stage_sources[stage_source]
        if not isinstance(source_cfg, dict):
            raise RuntimeError(
                f"Stage source {stage_source!r} metadata must be a mapping, got: {type(source_cfg).__name__}"
            )

        # Phase 2d: resolve this target's ref context → per-context source/module pins.
        target_ref = target_cfg.get("ref")
        ctx_stage_refs: dict = {}
        ctx_module_refs: dict = {}
        if scoped_refs and target_ref:
            ctx = resolve_ref_context(target_ref, ref_context_values)
            ctx_block = scoped_refs.get(ctx)
            if ctx_block is None:
                raise RuntimeError(
                    f"Stage target {stage_target!r} ref context {ctx!r} not found in refs.scoped"
                )
            ctx_stage_refs = ctx_block.get("stage_sources") or {}
            ctx_module_refs = ctx_block.get("modules") or {}

        stage_ref = ctx_stage_refs.get(stage_source) or {}
        if not isinstance(stage_ref, dict):
            raise RuntimeError(
                f"Stage source refs for {stage_source!r} must be a mapping, got: {type(stage_ref).__name__}"
            )

        branch = stage_over.get("branch") or stage_ref.get("branch")
        commit = stage_over.get("commit") or stage_ref.get("commit")
        # fat target carries the repo-local sub_workflow; a dict stage entry may still override
        child_workflow = stage_over.get("workflow") or target_cfg.get("sub_workflow")

        if branch and commit:
            raise RuntimeError(
                f"Stage {stage_id!r} resolved both branch={branch!r} and commit={commit!r}. "
                "Only one ref type may be set."
            )

        if require_branch_or_commit and not branch and not commit:
            raise RuntimeError(f"Stage {stage_id!r} source {stage_source!r} has neither branch nor commit configured")
        if require_branch_or_commit and require_commit_refs and not commit:
            raise RuntimeError(
                f"Stage {stage_id!r} ref {target_ref!r} requires an explicit commit (not a branch) for reproducibility"
            )

        repo_value = source_cfg.get(repo_key)
        if not repo_value:
            raise RuntimeError(
                f"Stage {stage_id!r} (target={stage_target!r}, source={stage_source!r}) missing {repo_key!r} in inventory {workflow_cfg.get('inventory')!r}"
            )

        cfg_files = target_cfg.get("cfg_files", [])
        if cfg_files is None:
            cfg_files = []
        if not isinstance(cfg_files, list):
            raise RuntimeError(f"Stage target {stage_target!r} cfg_files must be a list")

        active_stage = {
            "target": stage_target,
            "source": stage_source,
            "ref": target_ref,
            "branch": branch,
            "commit": commit,
            "workflow": child_workflow,
            "cfg_root": normalize_cfg_root(target_cfg.get("cfg_root", "/"), stage_target=stage_target),
            "cfg_files": cfg_files,
        }

        for legacy_field in ("aws_account_key", "aws_access_context_key"):
            if legacy_field in stage_over or legacy_field in target_cfg:
                raise RuntimeError(
                    f"Stage {stage_id!r} uses deprecated {legacy_field}; use execution_identity_key"
                )

        execution_identity_key = stage_over.get("execution_identity_key") or target_cfg.get("execution_identity_key")
        if execution_identity_key is not None:
            if not isinstance(execution_identity_key, str) or not execution_identity_key.strip():
                raise RuntimeError(f"Stage {stage_id!r} execution_identity_key must be a non-empty string")
            active_stage["execution_identity_key"] = execution_identity_key.strip()

        # State domain: which ctl-state bucket this target's results/lock live in.
        # Absent (commented in dev cfg) = no domain for this run → sync skippable.
        ctl_state_backend_key = target_cfg.get("ctl_state_backend_key")
        if ctl_state_backend_key is not None:
            if not isinstance(ctl_state_backend_key, str) or not ctl_state_backend_key.strip():
                raise RuntimeError(f"Stage {stage_id!r} ctl_state_backend_key must be a non-empty string")
            active_stage["ctl_state_backend_key"] = ctl_state_backend_key.strip()

        if repo_key == "repo_path":
            repo_path = Path(repo_value).expanduser()
            if not repo_path.is_absolute():
                raise RuntimeError(
                    f"Stage {stage_id!r} source {stage_source!r} repo_path must be absolute, got: {repo_value}"
                )
            active_stage["repo_path"] = str(repo_path.resolve())
        else:
            active_stage["repo_url"] = repo_value
            active_stage["token_type"] = source_cfg.get("token_type")

        raw_modules = source_cfg.get("modules") or {}
        if raw_modules and not isinstance(raw_modules, dict):
            raise RuntimeError(
                f"Stage {stage_id!r} source {stage_source!r} modules must be a mapping, got: {type(raw_modules).__name__}"
            )

        resolved_modules = {}
        for module_name, module_meta in raw_modules.items():
            if not isinstance(module_name, str):
                raise RuntimeError(
                    f"Stage {stage_id!r} module names must be strings, got: {type(module_name).__name__}"
                )
            if module_meta is None:
                module_meta = {}
            if not isinstance(module_meta, dict):
                raise RuntimeError(
                    f"Stage {stage_id!r} module {module_name!r} metadata must be a mapping, got: {type(module_meta).__name__}"
                )

            module_ref = ctx_module_refs.get(module_name) or {}
            if not isinstance(module_ref, dict):
                raise RuntimeError(
                    f"Module refs for {module_name!r} must be a mapping, got: {type(module_ref).__name__}"
                )

            module_branch = module_ref.get("branch")
            module_commit = module_ref.get("commit")
            if module_branch and module_commit:
                raise RuntimeError(
                    f"Module {module_name!r} resolved both branch={module_branch!r} and commit={module_commit!r}. "
                    "Only one ref type may be set."
                )
            if require_branch_or_commit and not module_branch and not module_commit:
                raise RuntimeError(
                    f"Stage {stage_id!r} module {module_name!r} has neither branch nor commit configured"
                )
            if require_branch_or_commit and require_commit_refs and not module_commit:
                raise RuntimeError(
                    f"Stage {stage_id!r} module {module_name!r} ref {target_ref!r} requires an explicit commit"
                )

            dest = module_meta.get("dest")
            if not isinstance(dest, str) or not dest.strip():
                raise RuntimeError(
                    f"Stage {stage_id!r} module {module_name!r} must define non-empty 'dest'"
                )
            dest_path = Path(dest)
            if dest_path.is_absolute() or ".." in dest_path.parts:
                raise RuntimeError(
                    f"Stage {stage_id!r} module {module_name!r} dest must stay within the stage repo: {dest}"
                )

            module_repo_value = module_meta.get(repo_key)
            if not module_repo_value:
                raise RuntimeError(
                    f"Stage {stage_id!r} module {module_name!r} missing {repo_key!r} in inventory {workflow_cfg.get('inventory')!r}"
                )

            resolved_module = {
                "dest": dest,
                "branch": module_branch,
                "commit": module_commit,
            }
            if repo_key == "repo_path":
                module_repo_path = Path(module_repo_value).expanduser()
                if not module_repo_path.is_absolute():
                    raise RuntimeError(
                        f"Stage {stage_id!r} module {module_name!r} repo_path must be absolute, got: {module_repo_value}"
                    )
                resolved_module["repo_path"] = str(module_repo_path.resolve())
            else:
                resolved_module["repo_url"] = module_repo_value
                resolved_module["token_type"] = module_meta.get("token_type")

            resolved_modules[module_name] = resolved_module

        if resolved_modules:
            active_stage["modules"] = resolved_modules

        active[stage_id] = active_stage

    return active


# After the Phase 2d cutover, targets / workflows / refs are all content-key
# resources (identified by their top-level key, not by a dir), so nothing is skipped.
_CONTENT_KEY_SKIP_TOP = ()


def collect_resource(ctl_cfg_root: Path, key: str, *, entry_depth: int = 1) -> dict:
    """Merge a top-level resource map identified by `key` across every cfg file.

    A resource's type is its top-level YAML key (content-key), not its filename: a
    file with a `cfg_file_sets:` key contributes cfg-file-sets wherever it lives. The maps are
    unioned across all `*.yaml` under `ctl_cfg_root`; a duplicate entry is a load
    error (same rule as targets), order-independent. `entry_depth` is how deep the
    unique entries sit: 1 for flat catalogs (stage_sources/cfg_file_sets),
    2 for action-keyed `variants`, 3 for `workflows.<action>.<scope>.<name>` and
    `providers.<name>.<section>.<entry>`.
    Intermediate levels merge; the entry level collides. Dir-routed trees (see
    `_CONTENT_KEY_SKIP_TOP`) are skipped — they have dedicated loaders.
    """
    merged: dict = {}
    origin: dict = {}

    def _merge(dst: dict, src: dict, prefix: str, yf: Path) -> None:
        for name, val in src.items():
            path = f"{prefix}.{name}" if prefix else str(name)
            depth = path.count(".") + 1
            if depth < entry_depth:
                if not isinstance(val, dict):
                    raise RuntimeError(f"❌ {key} {path!r} must be a mapping: {yf}")
                node = dst.setdefault(name, {})
                if not isinstance(node, dict):
                    raise RuntimeError(f"❌ {key} {path!r} must be a mapping: {yf}")
                _merge(node, val, path, yf)
            else:
                if name in dst:
                    raise RuntimeError(
                        f"❌ duplicate {key} entry {path!r}: {yf} (also defined in {origin[path]})"
                    )
                dst[name] = val
                origin[path] = yf

    for yf in sorted(ctl_cfg_root.rglob("*.yaml")):
        rel = yf.relative_to(ctl_cfg_root)
        if rel.parts and rel.parts[0] in _CONTENT_KEY_SKIP_TOP:
            continue
        data = load_yaml(yf) or {}
        if not isinstance(data, dict):
            continue
        section = data.get(key)
        if section is None:
            continue
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ '{key}' must be a mapping: {yf}")
        _merge(merged, section, "", yf)

    return merged


def _deep_merge_refs(dst: dict, src: dict, yf: Path, path: str = "") -> None:
    """Deep-merge a `refs` subtree across files; a duplicate leaf is a load error."""
    for k, v in src.items():
        cur = f"{path}.{k}" if path else str(k)
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge_refs(dst[k], v, yf, cur)
        elif k in dst:
            raise RuntimeError(f"❌ duplicate refs entry {cur!r}: {yf}")
        else:
            dst[k] = v


def load_refs_cfg(ctl_cfg_root: Path) -> dict:
    """Collect the content-key `refs` resource (deep tree-merge).

    Returns `{global: {tooling...}, scoped: {<ctx>: {stage_sources, modules}}}`.
    `global` = run-level shared pins (the engine/tooling, one version everywhere).
    `scoped` = per-context pins keyed by a flat dotted context. Both optional; in
    dev the refs may be absent entirely → `{}`.
    """
    merged: dict = {}
    for yf in sorted(ctl_cfg_root.rglob("*.yaml")):
        data = load_yaml(yf) or {}
        if not isinstance(data, dict):
            continue
        section = data.get("refs")
        if section is None:
            continue
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ 'refs' must be a mapping: {yf}")
        _deep_merge_refs(merged, section, yf)
    return merged


def resolve_ref_context(target_ref: str, context: dict[str, object]) -> str:
    """Resolve placeholders in a target ref into a refs.scoped context key."""
    return resolve_runtime_scalar(
        target_ref,
        context,
        label="target ref_key",
    )


def expand_workflow_imports(action_workflows: dict, name: str, _stack: tuple = ()) -> list:
    """Resolve import_workflow_keys in order, then append the workflow target_keys."""
    if name in _stack:
        raise RuntimeError(f"❌ workflow import cycle: {' -> '.join([*_stack, name])}")
    wf = action_workflows.get(name)
    if wf is None:
        raise RuntimeError(f"❌ workflow {name!r} not found (imported)")
    if not isinstance(wf, dict):
        raise RuntimeError(f"❌ workflow {name!r} must be a mapping")
    import_keys = wf.get("import_workflow_keys") or []
    target_keys = wf.get("target_keys") or []
    for field, values in (("import_workflow_keys", import_keys), ("target_keys", target_keys)):
        if not isinstance(values, list) or not all(isinstance(value, str) and value for value in values):
            raise RuntimeError(f"❌ workflow {name!r} {field} must be a list of non-empty strings")
    stages: list = []
    for workflow_key in import_keys:
        stages.extend(expand_workflow_imports(action_workflows, workflow_key, (*_stack, name)))
    stages.extend(target_keys)
    seen: set = set()
    for target_key in stages:
        if target_key in seen:
            raise RuntimeError(f"❌ workflow {name!r} has duplicate target key {target_key!r} after import expansion")
        seen.add(target_key)
    return stages


def workflow_effective_selectors(action_workflows: dict, name: str, _stack: tuple = ()) -> dict:
    """A workflow's selectors intersected with all imported workflows' selectors
    (an import cannot widen availability)."""
    if name in _stack:
        return {}
    wf = action_workflows.get(name) or {}
    effective = selector_requirements(wf.get("selectors") or {}, label=f"workflow {name} selectors")
    for workflow_key in (wf.get("import_workflow_keys") or []):
        imported = selector_requirements(
            workflow_effective_selectors(action_workflows, workflow_key, (*_stack, name)),
            label=f"workflow {workflow_key} effective selectors",
        )
        for ref, values in imported.items():
            effective[ref] = effective[ref] & values if ref in effective else set(values)
            if not effective[ref]:
                raise RuntimeError(
                    f"❌ workflow {name!r} selectors have empty intersection for {ref!r} "
                    f"after importing {workflow_key!r}"
                )
    return selectors_to_in_shape(effective)


def resolve_runtime_scalar(value, context: dict[str, object], *, label: str) -> str:
    """Resolve ${execution_context.<ns>.<key>} placeholders from the flat
    execution context (dotted keys)."""
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty string")

    token_re = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        resolved = context.get(key)
        if resolved is None or str(resolved) == "":
            raise RuntimeError(
                f"❌ {label}: {execution_context_miss_message(context, key)}"
            )
        return str(resolved)

    resolved = token_re.sub(replace, value.strip())
    if "${" in resolved:
        raise RuntimeError(f"❌ {label} contains an unsupported or unresolved placeholder: {value!r}")
    if not resolved:
        raise RuntimeError(f"❌ {label} resolved to an empty string")
    return resolved


def load_execution_identities_cfg(ctl_cfg_root: Path) -> dict:
    """Load provider-neutral execution identities from ctl cfg."""
    identities = collect_resource(ctl_cfg_root, "execution_identities")

    for identity_key, identity_cfg in identities.items():
        if not isinstance(identity_key, str) or not identity_key.strip():
            raise RuntimeError(f"❌ execution identity keys must be non-empty strings: {ctl_cfg_root}")
        if not isinstance(identity_cfg, dict) or not identity_cfg:
            raise RuntimeError(
                f"❌ execution identity {identity_key!r} must be a non-empty mapping: {ctl_cfg_root}"
            )
        provider = identity_cfg.get("provider")
        if not isinstance(provider, str) or not provider.strip():
            raise RuntimeError(f"❌ execution identity {identity_key!r} must define non-empty provider")
        provider = provider.strip()
        if provider == "aws":
            allowed_fields = {"provider", "account_key", "access_context_key"}
            unknown = sorted(set(identity_cfg) - allowed_fields)
            if unknown:
                raise RuntimeError(f"❌ execution identity {identity_key!r} has unknown fields {unknown}")
            for field in ("account_key", "access_context_key"):
                _require_non_empty_string(
                    identity_cfg.get(field),
                    f"execution_identities.{identity_key}.{field}",
                    ctl_cfg_root,
                )
        else:
            provider_fields = sorted(set(identity_cfg) - {"provider"})
            if not provider_fields:
                raise RuntimeError(
                    f"❌ execution identity {identity_key!r} provider {provider!r} must define "
                    "provider-specific fields"
                )

    return identities


def load_provider_catalogs(ctl_cfg_root: Path) -> dict:
    """Load the `providers` collection: providers.<name>.<section>.<entry>.

    One collection for all provider-owned catalogs, indexed by provider name —
    never by assembling key names from prefixes (Phase 20 provider-catalog
    end-state). Entries collide at depth 3, so multiple files may contribute to
    one provider section. This loader is engine-generic: it validates structure
    only and knows no provider names or section vocabularies — each provider
    implementation validates its OWN subtree.
    """
    providers = collect_resource(ctl_cfg_root, "providers", entry_depth=3)
    for provider_name in providers:
        if not isinstance(provider_name, str) or not provider_name.strip():
            raise RuntimeError(f"❌ providers keys must be non-empty strings: {ctl_cfg_root}")
    return providers


# The aws implementation owns its catalog schema; the engine core knows no
# provider names or sections.
_AWS_PROVIDER_CATALOG_SECTIONS = {"access_contexts", "accounts"}


def _load_aws_provider_catalog(ctl_cfg_root: Path) -> dict:
    catalog = load_provider_catalogs(ctl_cfg_root).get("aws", {})
    unknown = sorted(set(catalog) - _AWS_PROVIDER_CATALOG_SECTIONS)
    if unknown:
        raise RuntimeError(f"❌ providers.aws has unknown sections {unknown}: {ctl_cfg_root}")
    return catalog


def load_aws_access_contexts_cfg(ctl_cfg_root: Path) -> dict:
    """Load logical AWS access contexts and runner-specific implementations."""
    access_contexts = _load_aws_provider_catalog(ctl_cfg_root).get("access_contexts", {})

    for access_context_key, access_context_cfg in access_contexts.items():
        if not isinstance(access_context_key, str) or not access_context_key.strip():
            raise RuntimeError(f"❌ AWS access-context keys must be non-empty strings: {ctl_cfg_root}")
        if not isinstance(access_context_cfg, dict) or not access_context_cfg:
            raise RuntimeError(
                f"❌ AWS access context {access_context_key!r} must be a non-empty mapping: {ctl_cfg_root}"
            )
        for implementation_key, implementation_cfg in access_context_cfg.items():
            _validate_aws_access_implementation(
                access_context_key,
                implementation_key,
                implementation_cfg,
                ctl_cfg_root,
            )

    return access_contexts


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
        account_id = _require_non_empty_string(
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


def _validate_aws_access_implementation(
    access_context_key: str,
    implementation_key: str,
    implementation_cfg: dict,
    path: Path,
) -> None:
    if not isinstance(implementation_key, str) or not implementation_key.strip():
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r} implementation keys must be non-empty strings: {path}"
        )
    if not isinstance(implementation_cfg, dict):
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key} must be a mapping: {path}"
        )

    credential_keys = [
        key for key in ("profile_name", "iam_role_key")
        if key in implementation_cfg
    ]
    if len(credential_keys) != 1:
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key} must define exactly one of "
            f"profile_name or iam_role_key: {path}"
        )
    credential_key = credential_keys[0]
    _require_non_empty_string(
        implementation_cfg[credential_key],
        f"AWS access context {access_context_key!r}.{implementation_key}.{credential_key}",
        path,
    )

    if implementation_key == "local" and credential_key != "profile_name":
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.local must use profile_name: {path}"
        )
    if implementation_key == "ci" and credential_key != "iam_role_key":
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.ci must use iam_role_key: {path}"
        )

    expect_cfg = implementation_cfg.get("expect")
    if credential_key == "profile_name":
        _validate_direct_profile_expect(access_context_key, implementation_key, expect_cfg, path)
    elif expect_cfg is not None:
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key} must not duplicate expect "
            f"beside {credential_key}: {path}"
        )

    unknown = sorted(set(implementation_cfg) - {credential_key, "expect"})
    if unknown:
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key} has unknown fields {unknown}: {path}"
        )


def _validate_direct_profile_expect(
    access_context_key: str,
    implementation_key: str,
    expect_cfg,
    path: Path,
) -> None:
    if not isinstance(expect_cfg, dict):
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key}.expect must be a mapping "
            "for direct profile_name bindings"
        )
    principal_keys = [key for key in ("permission_set_name", "role_name") if key in expect_cfg]
    if len(principal_keys) != 1:
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key}.expect must define exactly one "
            f"of permission_set_name or role_name: {path}"
        )
    _require_non_empty_string(
        expect_cfg[principal_keys[0]],
        f"AWS access context {access_context_key!r}.{implementation_key}.expect.{principal_keys[0]}",
        path,
    )
    if "account_id" in expect_cfg:
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key}.expect.account_id is deprecated; "
            "put account IDs in providers.aws.accounts keyed by execution identity account_key"
        )
    unknown = sorted(set(expect_cfg) - set(principal_keys))
    if unknown:
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key}.expect has unknown fields "
            f"{unknown}: {path}"
        )


def _require_non_empty_string(value, label: str, path: Path | None = None) -> str:
    suffix = f": {path}" if path is not None else ""
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty string{suffix}")
    return value.strip()


def aws_access_override_env_name(access_context_key: str) -> str:
    suffix = re.sub(r"[^A-Za-z0-9]", "_", access_context_key).upper()
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


def resolve_stage_aws_access(
    stage: dict,
    execution_identities: dict,
    aws_access_contexts: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str] | None = None,
    allow_profile_only: bool = False,
    profile_only_aws_profile: str | None = None,
) -> dict[str, str] | None:
    for legacy_field in ("aws_account_key", "aws_access_context_key"):
        if legacy_field in stage:
            raise RuntimeError(f"❌ stage uses deprecated {legacy_field}; use execution_identity_key")

    identity_key = stage.get("execution_identity_key")
    if identity_key is None:
        profile_name = (profile_only_aws_profile or "").strip()
        if allow_profile_only and profile_name:
            return {
                "provider": "aws",
                "execution_identity_key": "profile_only",
                "implementation_key": "profile_only",
                "credential_provider_kind": "aws_profile_only",
                "profile_name": profile_name,
                "profile_only": "true",
            }
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

    raw_account_key = identity_cfg.get("account_key")
    raw_access_context_key = identity_cfg.get("access_context_key")

    context = dict(execution_context)
    account_key = resolve_runtime_scalar(
        raw_account_key,
        context,
        label=f"execution_identities.{identity_key}.account_key",
    )
    access_context_key = resolve_runtime_scalar(
        raw_access_context_key,
        context,
        label=f"execution_identities.{identity_key}.access_context_key",
    )

    access_context_cfg = aws_access_contexts.get(access_context_key)
    if not isinstance(access_context_cfg, dict):
        raise RuntimeError(
            f"❌ Stage AWS access context {access_context_key!r} is not defined in the aws_access_contexts catalog"
        )
    implementation_cfg = access_context_cfg.get(implementation_key)
    if not isinstance(implementation_cfg, dict):
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r} has no {implementation_key!r} implementation"
        )

    resolved: dict[str, str] = {
        "provider": "aws",
        "execution_identity_key": identity_key,
        "account_key": account_key,
        "access_context_key": access_context_key,
        "implementation_key": implementation_key,
    }

    if "profile_name" in implementation_cfg:
        canonical_profile_name = resolve_runtime_scalar(
            implementation_cfg["profile_name"],
            context,
            label=f"providers.aws.access_contexts.{access_context_key}.{implementation_key}.profile_name",
        )
        expect_cfg = implementation_cfg["expect"]
        if "permission_set_name" in expect_cfg:
            resolved["permission_set_name"] = resolve_runtime_scalar(
                expect_cfg["permission_set_name"],
                context,
                label=f"providers.aws.access_contexts.{access_context_key}.{implementation_key}.expect.permission_set_name",
            )
            resolved["credential_provider_kind"] = "identity_center_profile"
        else:
            resolved["role_name"] = resolve_runtime_scalar(
                expect_cfg["role_name"],
                context,
                label=f"providers.aws.access_contexts.{access_context_key}.{implementation_key}.expect.role_name",
            )
            resolved["credential_provider_kind"] = "assume_role_profile"
    elif "iam_role_key" in implementation_cfg:
        raise RuntimeError(
            f"❌ AWS implementation {implementation_key!r} uses iam_role_key, but CI OIDC activation is deferred"
        )
    else:
        raise RuntimeError(
            f"❌ AWS access context {access_context_key!r}.{implementation_key} has no supported credential binding"
        )

    canonical_account_id = resolve_configured_profile_account_id(canonical_profile_name)
    if account_registry is None:
        raise RuntimeError("❌ AWS account registry is required for declared execution identities")
    expected_account_id = account_registry.get(account_key)
    if expected_account_id is None:
        raise RuntimeError(f"❌ AWS account registry has no key {account_key!r}")
    if expected_account_id != canonical_account_id:
        raise RuntimeError(
            f"❌ AWS account registry maps {account_key!r} to {expected_account_id}, but canonical "
            f"profile {canonical_profile_name!r} resolves to {canonical_account_id}"
        )
    override_name = aws_access_override_env_name(access_context_key)
    selected_profile_name = os.getenv(override_name, "").strip() or canonical_profile_name
    selected_account_id = resolve_configured_profile_account_id(selected_profile_name)
    if selected_account_id != expected_account_id:
        raise RuntimeError(
            f"❌ AWS profile override {selected_profile_name!r} resolves to account {selected_account_id}, "
            f"but canonical profile {canonical_profile_name!r} resolves to {expected_account_id}"
        )

    resolved["profile_name"] = selected_profile_name
    resolved["expected_account_id"] = expected_account_id
    return resolved


def validate_profile_only_request(
    active_stages: dict,
    *,
    allow_profile_only: bool,
    profile_only_aws_profile: str | None,
) -> None:
    stages_with_identity = sorted(
        stage_id for stage_id, stage in active_stages.items()
        if stage.get("execution_identity_key") is not None
    )
    stages_without_identity = sorted(
        stage_id for stage_id, stage in active_stages.items()
        if stage.get("execution_identity_key") is None
    )

    if profile_only_aws_profile and stages_with_identity:
        raise RuntimeError(
            "❌ --aws-profile can be used only when every selected stage has no "
            "execution_identity_key; declared execution identities cannot be overridden. "
            f"Stages with identity: {', '.join(stages_with_identity)}"
        )

    if stages_with_identity and stages_without_identity:
        raise RuntimeError(
            "❌ selected stages mix declared execution_identity_key with missing identities. "
            "Either declare execution_identity_key for every selected stage, or comment/remove it "
            "from every selected stage and use profile-only fallback. "
            f"With identity: {', '.join(stages_with_identity)}; "
            f"without identity: {', '.join(stages_without_identity)}"
        )

    if not stages_without_identity:
        return

    rendered = ", ".join(stages_without_identity)
    if not allow_profile_only:
        raise RuntimeError(
            "❌ selected stages have no execution_identity_key and profile-only fallback is not "
            f"allowed by ctl cfg policy: {rendered}"
        )
    if not profile_only_aws_profile:
        raise RuntimeError(
            "❌ selected stages have no execution_identity_key and require --aws-profile because "
            f"profile-only fallback is enabled for this ctl policy: {rendered}"
        )


def validate_active_stage_aws_access(
    active_stages: dict,
    execution_identities: dict,
    aws_access_contexts: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str] | None = None,
    allow_profile_only: bool = False,
    profile_only_aws_profile: str | None = None,
) -> dict[str, str]:
    """Validate selected bindings and return the normalized account-key registry used by stages."""
    validate_profile_only_request(
        active_stages,
        allow_profile_only=allow_profile_only,
        profile_only_aws_profile=profile_only_aws_profile,
    )
    if any(stage.get("execution_identity_key") is not None for stage in active_stages.values()):
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
            aws_access_contexts,
            execution_context=execution_context,
            implementation_key=implementation_key,
            account_registry=expected_account_registry,
            allow_profile_only=allow_profile_only,
            profile_only_aws_profile=profile_only_aws_profile,
        )
        if resolved is None:
            continue
        if resolved.get("profile_only") == "true":
            logging.info(
                "Using temporary explicit --aws-profile access for stage %s: profile=%s",
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
            "access_context_key=%s implementation_key=%s credential_provider_kind=%s",
            stage_id,
            resolved["execution_identity_key"],
            account_key,
            resolved["access_context_key"],
            resolved["implementation_key"],
            resolved["credential_provider_kind"],
        )
    return validated_account_registry


def configure_stage_aws_env(
    stage_id: str,
    stage: dict,
    stage_env: dict[str, str],
    execution_identities: dict,
    aws_access_contexts: dict,
    *,
    execution_context: dict[str, object],
    implementation_key: str,
    account_registry: dict[str, str],
    allow_profile_only: bool = False,
    profile_only_aws_profile: str | None = None,
) -> None:
    """Apply one stage's selected AWS access implementation and assertion metadata."""
    for var_name in AWS_ACCESS_STAGE_ENV_VARS:
        stage_env.pop(var_name, None)

    for var_name in AWS_CREDENTIAL_ENV_VARS:
        stage_env.pop(var_name, None)

    resolved = resolve_stage_aws_access(
        stage,
        execution_identities,
        aws_access_contexts,
        execution_context=execution_context,
        implementation_key=implementation_key,
        account_registry=account_registry,
        allow_profile_only=allow_profile_only,
        profile_only_aws_profile=profile_only_aws_profile,
    )
    if resolved is None:
        return

    stage_env["AWS_EC2_METADATA_DISABLED"] = "true"
    stage_env["AWS_PROFILE"] = resolved["profile_name"]
    stage_env["ATLAS_AWS_ASSERT_ACCESS"] = "true"
    stage_env["ATLAS_EXECUTION_IDENTITY_KEY"] = resolved["execution_identity_key"]

    if resolved.get("profile_only") == "true":
        stage_env["ATLAS_AWS_PROFILE_ONLY_ACCESS"] = "true"
        logging.info(
            "Resolved temporary explicit --aws-profile access for stage %s: profile=%s",
            stage_id,
            resolved["profile_name"],
        )
        return

    stage_env["ATLAS_AWS_ACCOUNT_KEY"] = resolved["account_key"]
    stage_env["ATLAS_AWS_ACCESS_CONTEXT_KEY"] = resolved["access_context_key"]
    stage_env["ATLAS_AWS_IMPLEMENTATION_KEY"] = resolved["implementation_key"]
    stage_env["ATLAS_AWS_EXPECT_ACCOUNT_ID"] = resolved["expected_account_id"]
    if resolved.get("permission_set_name"):
        stage_env["ATLAS_AWS_EXPECT_PERMISSION_SET_NAME"] = resolved["permission_set_name"]
    if resolved.get("role_name"):
        stage_env["ATLAS_AWS_EXPECT_ROLE_NAME"] = resolved["role_name"]

    logging.info(
        "Resolved AWS access for stage %s: execution_identity_key=%s account_key=%s "
        "access_context_key=%s implementation_key=%s credential_provider_kind=%s expected_account_id=%s",
        stage_id,
        resolved["execution_identity_key"],
        resolved["account_key"],
        resolved["access_context_key"],
        resolved["implementation_key"],
        resolved["credential_provider_kind"],
        resolved["expected_account_id"],
    )


def merge_config_dirs(
    source_dirs: list[str],
    dest_dir: str,
    clear_dest: bool = True,
    *,
    source_log_roots: tuple[Path, ...] = (),
    dest_log_roots: tuple[Path, ...] = (),
    merged_files: dict[str, list[str]] | None = None,
    skip_filenames: set[str] | None = None,
) -> dict[str, list[str]]:
    """Merge config directories in sequence using YAML-aware overlay semantics."""
    if clear_dest and os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)

    if merged_files is None:
        merged_files = {}

    for source_dir in source_dirs:
        for root, dirs, files in os.walk(source_dir):
            # scope-local baseline dirs are guard artifacts, never cfg payload
            dirs[:] = [d for d in dirs if d != PLT_GUARDRAILS_DIRNAME]
            rel_root = os.path.relpath(root, source_dir)
            dest_root = os.path.join(dest_dir, rel_root) if rel_root != "." else dest_dir

            os.makedirs(dest_root, exist_ok=True)

            for file in files:
                if skip_filenames and file in skip_filenames:
                    continue
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_root, file)

                if os.path.exists(dest_file):
                    merged_data = merge_cfg_values(load_cfg_yaml(dest_file), load_cfg_yaml(src_file))
                    source_list = merged_files.setdefault(dest_file, [])
                    source_list.append(src_file)
                    header_comment = None
                    if len(source_list) > 1 and (source_log_roots or dest_log_roots):
                        header_comment = render_merged_cfg_header(
                            dest_file,
                            source_list,
                            source_log_roots=source_log_roots,
                            dest_log_roots=dest_log_roots,
                        )
                    write_cfg_yaml(dest_file, merged_data, header_comment=header_comment)
                else:
                    shutil.copy2(src_file, dest_file)
                    merged_files[dest_file] = [src_file]

    for dest_path, sources in merged_files.items():
        if len(sources) > 1:
            rendered_sources = [format_path_for_log(src, source_log_roots) for src in sources]
            rendered_dest = format_path_for_log(dest_path, dest_log_roots)
            logging.info("Merged:")
            logging.info("  %s", rendered_sources[0])
            for src in rendered_sources[1:]:
                logging.info("  + %s", src)
            logging.info("  = %s", rendered_dest)

    return merged_files


def _flatten_yaml_leaf_values(value, path: tuple[object, ...] = ()) -> dict[tuple[object, ...], object]:
    if isinstance(value, dict):
        leaves: dict[tuple[object, ...], object] = {}
        for key, child in value.items():
            leaves.update(_flatten_yaml_leaf_values(child, path + (key,)))
        return leaves
    return {path: value}


def _scope_final_yaml_leaves(scope: dict, *, skip_filenames: set[str]) -> dict[tuple[str, tuple[object, ...]], object]:
    with tempfile.TemporaryDirectory(prefix="atlas-scope-leaves-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        merge_config_dirs(
            source_dirs=scope["source_dirs"],
            dest_dir=str(tmp_path),
            clear_dest=True,
            skip_filenames=skip_filenames,
        )
        leaves: dict[tuple[str, tuple[object, ...]], object] = {}
        for yaml_path in sorted(tmp_path.rglob("*.yaml")):
            rel_path = yaml_path.relative_to(tmp_path).as_posix()
            data = load_cfg_yaml(str(yaml_path))
            for leaf_path, leaf_value in _flatten_yaml_leaf_values(data).items():
                leaves[(rel_path, leaf_path)] = leaf_value
        return leaves


def validate_cross_scope_leaf_conflicts(scopes: list[dict], *, target_path: str, skip_filenames: set[str]) -> None:
    """Reject shared-target producers that define different final values for one YAML leaf."""
    if len(scopes) < 2:
        return
    owners: dict[tuple[str, tuple[object, ...]], tuple[object, dict]] = {}
    for scope in scopes:
        for leaf_key, leaf_value in _scope_final_yaml_leaves(scope, skip_filenames=skip_filenames).items():
            previous = owners.get(leaf_key)
            if previous is None:
                owners[leaf_key] = (leaf_value, scope)
                continue
            previous_value, previous_scope = previous
            if previous_value != leaf_value:
                rel_path, yaml_path = leaf_key
                rendered_path = ".".join(str(part) for part in yaml_path) or "<root>"
                raise RuntimeError(
                    f"❌ cross-scope cfg conflict for target_path {target_path!r} at "
                    f"{rel_path}:{rendered_path}: {previous_scope['scope_id']} and {scope['scope_id']} "
                    "produce different final values"
                )


def _add_workflow_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--ctl-variants",
        required=False,
        default=[],
        dest="ctl_variants",
        type=parse_ctl_variants_arg,
        help="Optional comma-separated ctl variant paths under variants/",
    )
    parser.add_argument(
        "--workflow",
        required=True,
        help="declared ctl workflow name",
    )


def _add_target_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--target",
        required=True,
        help="declared target name",
    )


def _add_maintenance_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--maintenance-action",
        required=True,
        choices=list(MAINTENANCE_ACTIONS),
        help="maintenance action",
    )
    parser.add_argument(
        "--lock-id",
        help="ctl run ID or Terraform state lock ID to force-unlock",
    )
    parser.add_argument(
        "--target",
        help="declared target to operate on; required for Terraform lock force-unlock",
    )


def _add_sub_workflow_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source",
        required=True,
        help="source repo for a synthetic target",
    )
    parser.add_argument(
        "--ref",
        required=True,
        help="ref context (a key in refs.scoped, e.g. env/${env_type} or org) for a synthetic target",
    )
    parser.add_argument(
        "--cfg-file-set",
        required=True,
        dest="cfg_file_set",
        help="cfg_file_set for a synthetic target",
    )
    parser.add_argument(
        "--sub-workflow",
        required=True,
        dest="sub_workflow",
        help="repo-local sub_workflow to run",
    )
    parser.add_argument(
        "--execution-identity-key",
        dest="execution_identity_key",
        default=None,
        help="execution identity key for a synthetic target",
    )
    parser.add_argument(
        "--ctl-state-backend-key",
        dest="ctl_state_backend_key",
        default=None,
        help="state domain for a synthetic target (ctl_state_backends key); "
        "omit = no domain (sync skippable via the skip triad)",
    )
    parser.add_argument(
        "--affected-target-key",
        dest="affected_target_keys",
        action="append",
        default=[],
        help="affected declared target key; repeatable and required for mutating synthetic runs",
    )


def add_common_args(parser: argparse.ArgumentParser, *, run_type: str) -> None:
    """Add shared and runner-specific arguments for local runner entrypoints."""
    parser.add_argument(
        "--ctl-state-local-root",
        required=True,
        help="Local ctl-state root (run results tree); runner appends <action>/<run_type>/<name>",
    )
    parser.add_argument(
        "--aws-profile",
        default=None,
        help="Temporary profile-only fallback for runs where every selected stage lacks execution_identity_key",
    )
    parser.add_argument(
        "--plt-overlays",
        required=False,
        default=[],
        dest="plt_overlays",
        type=parse_overlays_arg,
        help="Optional comma-separated plt overlay names",
    )
    parser.add_argument(
        "--ctl-profile",
        required=True,
        help="Ctl profile name (named policy bundle from the ctl_profiles catalog)",
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["provision", "plan", "destroy", "readonly"],
        help="Lifecycle action (provision|plan|destroy|readonly)",
    )
    parser.add_argument(
        "--execution-params",
        dest="execution_param",
        action="append",
        default=[],
        type=parse_selector_arg,
        help="Execution param in key=value form; repeatable; lands in execution_context.params.*",
    )
    parser.add_argument(
        "--skip-ctl-state-backend-sync",
        action="store_true",
        help="Run without ctl-state backend sync; honored only when the ctl profile "
        "sets allow_skip_ctl_state_backend_sync: true AND no active target declares "
        "a ctl_state_backend_key (a declared state domain always syncs)",
    )

    if run_type == "workflow":
        _add_workflow_args(parser)
    elif run_type == "target":
        _add_target_args(parser)
    elif run_type == "maintenance":
        _add_maintenance_args(parser)
    elif run_type == "sub_workflow":
        _add_sub_workflow_args(parser)
    else:
        raise RuntimeError(f"❌ unknown runner run_type {run_type!r}")

def setup_logging() -> logging.handlers.MemoryHandler:
    """Setup logging with memory handler to capture early logs."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    memory_handler = logging.handlers.MemoryHandler(capacity=1000, flushLevel=logging.CRITICAL)
    logging.getLogger().addHandler(memory_handler)
    logging.info(f"Command: {' '.join(sys.argv)}")
    return memory_handler


def load_workflow_cfg(
    ctl_cfg_root: Path,
    ctl_profile: str,
    inventory_name: str,
    workflow_name: str,
    execution_context: dict[str, object],
) -> dict:
    """Load a content-key workflow: `workflows.<action>.<name>` (imports + selectors).

    Expands `import_workflow_keys` (ordered, recursive) then the workflow's own `target_keys`; applies
    `selectors` (intersected through imports) through the generic selector matcher.
    The workflow name is an opaque key (slashes are cosmetic). `ctl_profile` is retained
    for the generated workflow metadata.
    """
    workflows = collect_resource(ctl_cfg_root, "workflows", entry_depth=2)
    action_workflows = workflows.get(inventory_name)
    if not isinstance(action_workflows, dict) or not action_workflows:
        raise RuntimeError(f"❌ no workflows defined for action {inventory_name!r}")
    if workflow_name not in action_workflows:
        raise RuntimeError(
            f"❌ workflow {workflow_name!r} not found for action {inventory_name!r}"
        )

    effective_selectors = workflow_effective_selectors(action_workflows, workflow_name)
    if not selector_matches(
        effective_selectors,
        execution_context,
        label=f"workflow {inventory_name}/{workflow_name}",
    ):
        raise RuntimeError(
            f"❌ workflow {inventory_name}/{workflow_name} is not available for "
            f"runtime selectors {execution_context} (selectors {effective_selectors})"
        )

    stages = expand_workflow_imports(action_workflows, workflow_name)
    return {
        "meta": {"name": f"{inventory_name}/{workflow_name}", "action": inventory_name},
        "stages": stages,
    }


def get_ctl_variants_root(ctl_cfg_root: Path) -> Path:
    """Return ctl variant root dir under variants/."""
    ctl_variants_root = (ctl_cfg_root / "variants").resolve()
    if ctl_variants_root.is_dir():
        return ctl_variants_root
    raise RuntimeError(f"Ctl variants dir not found under: {ctl_cfg_root}")


def get_ctl_variant_root(ctl_cfg_root: Path, ctl_variant: str) -> Path:
    """Resolve one selected ctl variant path under variants/."""
    ctl_variants_root = get_ctl_variants_root(ctl_cfg_root)
    variant_root = (ctl_variants_root / ctl_variant).resolve()
    try:
        variant_root.relative_to(ctl_variants_root)
    except ValueError as exc:
        raise RuntimeError(f"Ctl variant path escapes variants/: {ctl_variant}") from exc
    if not variant_root.exists():
        raise RuntimeError(f"Ctl variant path not found: {variant_root}")
    if not variant_root.is_dir():
        raise RuntimeError(f"Ctl variant path must be a directory: {variant_root}")
    return variant_root


def load_optional_yaml_mapping(path: Path) -> dict:
    """Load an optional YAML mapping, returning {} when the file is absent."""
    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML file must contain a mapping: {path}")
    return data


def _load_meta_string_list(meta: dict, key: str, item_kind: str, item_label: str) -> list[str]:
    raw = meta.get(key) or []
    if not isinstance(raw, list) or not all(isinstance(item, str) and item for item in raw):
        raise RuntimeError(
            f"❌ {item_kind} '{item_label}' meta key '{key}' must be a list of non-empty strings"
        )
    return raw


def validate_ctl_variant_meta(
    meta: dict,
    *,
    variant_label: str,
    ctl_profile: str,
    plt_overlays: list[str],
) -> None:
    """Validate ctl variant metadata against selected plt overlays."""
    if not isinstance(meta, dict):
        raise RuntimeError(f"❌ ctl variant '{variant_label}' __meta__.yaml must contain a mapping")

    allowed_envs = _load_meta_string_list(meta, "allowed_envs", "ctl variant", variant_label)
    if allowed_envs:
        if ctl_profile not in allowed_envs:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' is not allowed for ctl context '{ctl_profile}'; "
                f"allowed_envs={allowed_envs}"
            )

    required_plt_overlays = _load_meta_string_list(meta, "requires_plt_overlays", "ctl variant", variant_label)
    if required_plt_overlays:
        missing = [overlay for overlay in required_plt_overlays if overlay not in plt_overlays]
        if missing:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' requires plt overlays {missing}, "
                f"but selected plt overlays are {plt_overlays}"
            )



def get_workflow_stage_id(stage_entry) -> str:
    """Return the stage id from a workflow stage entry."""
    if isinstance(stage_entry, str):
        return stage_entry
    if isinstance(stage_entry, dict):
        stage_id = stage_entry.get("id")
        if isinstance(stage_id, str) and stage_id:
            return stage_id
    raise RuntimeError(f"❌ invalid workflow stage entry: {stage_entry!r}")


def validate_ctl_variant_stage_patch_entry(raw_stage: dict, variant_label: str) -> tuple[str, dict]:
    """Validate one ctl variant stage patch entry and return its op plus stage payload."""
    if not isinstance(raw_stage, dict):
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' workflow patch entries must be mappings"
        )

    add_before = raw_stage.get("add_before")
    add_after = raw_stage.get("add_after")
    op_keys = [key for key, value in (("add_before", add_before), ("add_after", add_after)) if value is not None]
    if len(op_keys) != 1:
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' stage patch entry must define exactly one of "
            f"'add_before' or 'add_after': {raw_stage}"
        )

    anchor_stage_id = raw_stage[op_keys[0]]
    if not isinstance(anchor_stage_id, str) or not anchor_stage_id:
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' {op_keys[0]} value must be a non-empty stage id"
        )

    stage_entry = {k: v for k, v in raw_stage.items() if k not in ("add_before", "add_after")}
    stage_id = stage_entry.get("id")
    stage_target = stage_entry.get("target")
    stage_workflow = stage_entry.get("workflow")
    if not isinstance(stage_id, str) or not stage_id:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' inserted stage must define non-empty 'id'")
    if not isinstance(stage_target, str) or not stage_target:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' stage '{stage_id}' must define non-empty 'target'")
    if not isinstance(stage_workflow, str) or not stage_workflow:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' stage '{stage_id}' must define non-empty 'workflow'")
    if stage_entry.get("branch") and stage_entry.get("commit"):
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' stage '{stage_id}' cannot define both 'branch' and 'commit'"
        )

    return op_keys[0], stage_entry


def apply_ctl_variant_workflow_patch(
    workflow_cfg: dict,
    patch_cfg: dict,
    *,
    variant_label: str,
    patch_label: str,
) -> dict:
    """Apply add_before/add_after workflow patch entries from one ctl variant patch file."""
    stages = workflow_cfg.get("stages")
    if not isinstance(stages, list):
        raise RuntimeError(f"❌ workflow cfg must contain a 'stages' list before applying ctl variants")

    patch_stages = patch_cfg.get("stages") or []
    if not isinstance(patch_stages, list):
        raise RuntimeError(
            f"❌ ctl variant patch '{patch_label}' must contain a 'stages' list"
        )

    resolved_stages = list(stages)
    for raw_stage in patch_stages:
        op, stage_entry = validate_ctl_variant_stage_patch_entry(raw_stage, variant_label)
        anchor_stage_id = raw_stage[op]
        stage_id = stage_entry["id"]

        stage_ids = [get_workflow_stage_id(stage) for stage in resolved_stages]
        if anchor_stage_id not in stage_ids:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' patch '{patch_label}' references missing anchor "
                f"stage id '{anchor_stage_id}'"
            )
        if stage_id in stage_ids:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' patch '{patch_label}' inserts duplicate stage id '{stage_id}'"
            )

        anchor_index = stage_ids.index(anchor_stage_id)
        insert_index = anchor_index if op == "add_before" else anchor_index + 1
        resolved_stages.insert(insert_index, stage_entry)
        logging.info(
            "Applied ctl variant '%s': %s stage '%s' %s '%s'",
            variant_label,
            op,
            stage_id,
            "before" if op == "add_before" else "after",
            anchor_stage_id,
        )

    patched_workflow_cfg = dict(workflow_cfg)
    patched_workflow_cfg["stages"] = resolved_stages
    return patched_workflow_cfg


def load_variants_cfg(ctl_cfg_root: Path) -> dict:
    """Load action-keyed variant placements discovered by content key."""
    return collect_resource(ctl_cfg_root, "variants", entry_depth=2)


def variant_source_action(action: str) -> str:
    """Which action's variants apply. `plan` previews `provision`, so a plan run
    resolves variants against `provision` rather than a separate `plan` block."""
    return "provision" if action == "plan" else action


def _selectors_subset(child: dict | None, parent: dict | None):
    """(ok, reason) — True if child selectors are a subset of parent's, per dimension."""
    return selector_subset(child, parent, child_label="variant selectors", parent_label="target selectors")


def apply_ctl_variants_to_workflow_cfg(
    ctl_cfg_root: Path,
    workflow_cfg: dict,
    inventory_cfg: dict,
    *,
    execution_context: dict[str, object],
    inventory_name: str,
    workflow_name: str,
    ctl_variants: list[str],
) -> dict:
    """Apply selected variant placements to a loaded workflow cfg.

    A variant is `variants.<action>.<name> = {target_key, workflow_key, after_target_key|before_target_key, [selectors]}`.
    For each selected variant whose `workflow_key` matches the running one: validate its target
    exists and `variant.selectors ⊆ target.selectors`, gate through runtime selectors (the target
    ceiling AND the variant subset), then insert the target name at the after/before anchor
    (skip + log if the anchor is absent in this workflow).
    """
    if not ctl_variants:
        return workflow_cfg

    variant_action = variant_source_action(inventory_name)
    variants = load_variants_cfg(ctl_cfg_root).get(variant_action, {})
    targets = inventory_cfg.get("stage_targets", {})
    stages = list(workflow_cfg.get("stages") or [])

    for name in ctl_variants:
        v = variants.get(name)
        if v is None:
            raise RuntimeError(
                f"❌ variant {name!r} not found under action {variant_action!r} in variant config"
            )
        if not isinstance(v, dict):
            raise RuntimeError(f"❌ variant {name!r} must be a mapping")

        if v.get("workflow_key") != workflow_name:
            logging.info(
                "Variant '%s' targets workflow '%s', not the running '%s' — skipped",
                name, v.get("workflow_key"), workflow_name,
            )
            continue

        target_name = v.get("target_key")
        target = targets.get(target_name)
        if target is None:
            raise RuntimeError(
                f"❌ variant {name!r} references missing target {target_name!r} (action {inventory_name!r})"
            )

        ok, why = _selectors_subset(v.get("selectors"), target.get("selectors"))
        if not ok:
            raise RuntimeError(f"❌ variant {name!r} selectors exceed target {target_name!r}: {why}")

        if not selector_matches(
            target.get("selectors"),
            execution_context,
            label=f"target {target_name}",
        ):
            logging.info("Variant '%s' target is not available for selectors %s — skipped", name, execution_context)
            continue
        if not selector_matches(
            v.get("selectors"),
            execution_context,
            label=f"variant {name}",
        ):
            logging.info("Variant '%s' placement gated off for selectors %s — skipped", name, execution_context)
            continue

        before, after = v.get("before_target_key"), v.get("after_target_key")
        if before and after:
            raise RuntimeError(f"❌ variant {name!r} cannot set both 'before_target_key' and 'after_target_key'")
        anchor = before or after
        if anchor is None:
            raise RuntimeError(f"❌ variant {name!r} must define 'after_target_key' or 'before_target_key'")
        if anchor not in stages:
            logging.info("Variant '%s' anchor '%s' absent from '%s' — skipped", name, anchor, workflow_name)
            continue
        if target_name in stages:
            raise RuntimeError(f"❌ variant {name!r} inserts duplicate target {target_name!r}")
        idx = stages.index(anchor)
        stages.insert(idx if before else idx + 1, target_name)
        logging.info(
            "Applied variant '%s': inserted '%s' %s '%s'",
            name, target_name, "before" if before else "after", anchor,
        )

    patched = dict(workflow_cfg)
    patched["stages"] = stages
    return patched


def resolve_cfg_file_set_files(
    cfg_file_set_key: str,
    cfg_file_sets: dict,
    cfg_file_sets_path: Path,
    _stack: tuple = (),
) -> list:
    """Resolve cfg_file_set_keys in order, then append the selected cfg_file_set's cfg_files."""
    if cfg_file_set_key in _stack:
        cycle = " -> ".join([*_stack, cfg_file_set_key])
        raise RuntimeError(f"❌ cfg_file_set key cycle: {cycle} ({cfg_file_sets_path})")
    cfg_file_set = cfg_file_sets.get(cfg_file_set_key)
    if cfg_file_set is None:
        raise RuntimeError(f"❌ missing cfg_file_set key {cfg_file_set_key!r}: {cfg_file_sets_path}")
    if not isinstance(cfg_file_set, dict):
        raise RuntimeError(f"❌ cfg_file_set {cfg_file_set_key!r} must be a mapping: {cfg_file_sets_path}")

    included_keys = cfg_file_set.get("cfg_file_set_keys") or []
    cfg_files = cfg_file_set.get("cfg_files") or []
    if not isinstance(included_keys, list) or not all(isinstance(key, str) and key for key in included_keys):
        raise RuntimeError(f"❌ cfg_file_set {cfg_file_set_key!r} cfg_file_set_keys must be a list of non-empty strings")
    if not isinstance(cfg_files, list) or not all(isinstance(path, str) and path for path in cfg_files):
        raise RuntimeError(f"❌ cfg_file_set {cfg_file_set_key!r} cfg_files must be a list of non-empty strings")

    resolved: list = []
    for included_key in included_keys:
        resolved.extend(resolve_cfg_file_set_files(included_key, cfg_file_sets, cfg_file_sets_path, (*_stack, cfg_file_set_key)))
    resolved.extend(cfg_files)
    return resolved


def load_inventory_cfg(ctl_cfg_root: Path, inventory_name: str) -> dict:
    """Compose action cfg from stage_sources + cfg_file_sets + targets/<action>/*.yaml.

    `inventory_name` is the action (provision/plan/destroy/readonly). Layout:
      - stage_sources.yaml  source repos: source key -> meta
      - cfg_file_sets.yaml       config views: cfg-file-set key -> {cfg_root, cfg_file_set_keys, cfg_files}
      - targets/<action>/*.yaml  fat targets (the directory IS the action). Each
            file is a flat `targets:` map; all files for an action merge (duplicate
            names rejected). A target is self-contained:
              {source_key, ref_key, sub_workflow_key, cfg_file_set_key,
               [execution_identity_key], [cfg_files], [selectors],
               [required_plt_overlay_keys]}.

    Returns the flat shape build_active_stages consumes ({stage_sources,
    stage_targets}), where each target carries source + cfg_root + cfg_files
    (resolved from its cfg_file_set_key) + sub_workflow + execution identity requirement
    (+ selectors /
    requires_plt_overlays when present).
    """
    # global resources + targets are content-key (collected by top-level key)
    stage_sources = collect_resource(ctl_cfg_root, "stage_sources")
    cfg_file_sets = collect_resource(ctl_cfg_root, "cfg_file_sets")
    cfg_file_sets_path = ctl_cfg_root  # label for include/error messages
    if not stage_sources:
        raise RuntimeError(f"❌ no 'stage_sources' defined under: {ctl_cfg_root}")
    if not cfg_file_sets:
        raise RuntimeError(f"❌ no 'cfg_file_sets' defined under: {ctl_cfg_root}")

    all_targets = collect_resource(ctl_cfg_root, "targets", entry_depth=2)
    stage_targets = all_targets.get(inventory_name)
    if not isinstance(stage_targets, dict) or not stage_targets:
        raise RuntimeError(f"❌ no targets defined for action {inventory_name!r}")

    resolved_targets: dict = {}
    for target_name, target_def in stage_targets.items():
        if not isinstance(target_def, dict):
            raise RuntimeError(f"❌ target {target_name!r} must be a mapping (action {inventory_name!r})")

        source = target_def.get("source_key")
        if not isinstance(source, str) or not source:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'source_key'")

        target_ref = target_def.get("ref_key")
        if not isinstance(target_ref, str) or not target_ref.strip():
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'ref_key'")

        cfg_file_set_name = target_def.get("cfg_file_set_key")
        if not isinstance(cfg_file_set_name, str) or not cfg_file_set_name:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'cfg_file_set_key'")
        cfg_file_set = cfg_file_sets.get(cfg_file_set_name)
        if cfg_file_set is None:
            raise RuntimeError(
                f"❌ target {target_name!r} references missing cfg_file_set {cfg_file_set_name!r}: {cfg_file_sets_path}"
            )
        if not isinstance(cfg_file_set, dict):
            raise RuntimeError(f"❌ cfg_file_set {cfg_file_set_name!r} must be a mapping: {cfg_file_sets_path}")

        sub_workflow = target_def.get("sub_workflow_key")
        if not isinstance(sub_workflow, str) or not sub_workflow:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'sub_workflow_key'")

        for legacy_field in ("aws_account_key", "aws_access_context_key"):
            if legacy_field in target_def:
                raise RuntimeError(
                    f"❌ target {target_name!r} uses deprecated {legacy_field}; use execution_identity_key"
                )
        execution_identity_key = target_def.get("execution_identity_key")
        if execution_identity_key is not None and (
            not isinstance(execution_identity_key, str) or not execution_identity_key.strip()
        ):
            raise RuntimeError(
                f"❌ target {target_name!r} execution_identity_key must be a non-empty string"
            )

        extra_files = target_def.get("cfg_files", []) or []
        if not isinstance(extra_files, list):
            raise RuntimeError(f"❌ target {target_name!r} cfg_files must be a list")

        resolved = {
            "source": source,
            "ref": target_ref.strip(),
            "sub_workflow": sub_workflow,
            "cfg_root": cfg_file_set.get("cfg_root", "/"),
            "cfg_files": [
                *resolve_cfg_file_set_files(cfg_file_set_name, cfg_file_sets, cfg_file_sets_path),
                *extra_files,
            ],
        }
        if execution_identity_key is not None:
            resolved["execution_identity_key"] = execution_identity_key.strip()
        ctl_state_backend_key = target_def.get("ctl_state_backend_key")
        if ctl_state_backend_key is not None:
            if not isinstance(ctl_state_backend_key, str) or not ctl_state_backend_key.strip():
                raise RuntimeError(
                    f"❌ target {target_name!r} ctl_state_backend_key must be a non-empty string"
                )
            resolved["ctl_state_backend_key"] = ctl_state_backend_key.strip()
        if target_def.get("provisions_ctl_state_bucket") is True:
            resolved["provisions_ctl_state_bucket"] = True
        if "selectors" in target_def:
            resolved["selectors"] = target_def["selectors"]
        if "required_plt_overlay_keys" in target_def:
            overlay_keys = target_def["required_plt_overlay_keys"]
            if not isinstance(overlay_keys, list) or not all(
                isinstance(key, str) and key for key in overlay_keys
            ):
                raise RuntimeError(
                    f"❌ target {target_name!r} required_plt_overlay_keys must be "
                    "a list of non-empty strings"
                )
            resolved["requires_plt_overlays"] = overlay_keys
        resolved_targets[target_name] = resolved

    return {
        "stage_sources": stage_sources,
        "stage_targets": resolved_targets,
    }


def load_local_tooling_cfg(ctl_cfg_root: Path) -> dict:
    """Load local tooling repo paths discovered by content key for local_dev runs."""
    raw_tooling_cfg = collect_resource(ctl_cfg_root, "tooling")
    tooling_path = ctl_cfg_root
    if not raw_tooling_cfg:
        logging.info("No local tooling config found")
        return {}

    tooling_refs = {}
    for tooling_name, tooling_entry in raw_tooling_cfg.items():
        if not isinstance(tooling_name, str):
            raise RuntimeError(f"❌ local tooling keys must be strings: {tooling_path}")
        if tooling_entry is None:
            tooling_entry = {}
        if not isinstance(tooling_entry, dict):
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must be a mapping: {tooling_path}"
            )

        if tooling_entry.get("repo_url"):
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must use repo_path, not repo_url: {tooling_path}"
            )

        repo_path = tooling_entry.get("repo_path")
        if not repo_path:
            continue
        if not isinstance(repo_path, str):
            raise RuntimeError(
                f"❌ local tooling repo path for '{tooling_name}' must be a string: {tooling_path}"
            )

        branch = tooling_entry.get("branch")
        commit = tooling_entry.get("commit")
        if branch or commit:
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must not define branch or commit: {tooling_path}"
            )

        repo_path_obj = Path(repo_path).expanduser()
        if not repo_path_obj.is_absolute():
            repo_path_obj = (ctl_cfg_root / repo_path_obj).resolve()

        tooling_refs[tooling_name] = {"repo_path": str(repo_path_obj)}

    logging.info("Using local tooling config discovered by content key")
    return tooling_refs


def build_tooling_env(tooling_refs: dict) -> dict[str, str]:
    """Translate tooling refs into environment variables for setup scripts."""
    env_updates: dict[str, str] = {}

    for tooling_name, env_prefix in TOOLING_ENV_PREFIXES.items():
        tooling_ref = tooling_refs.get(tooling_name) or {}
        if not isinstance(tooling_ref, dict):
            continue

        repo_path = tooling_ref.get("repo_path")
        repo_url = tooling_ref.get("repo_url") or (
            None if repo_path else TOOLING_DEFAULT_REPO_URLS.get(tooling_name)
        )
        branch = tooling_ref.get("branch")
        commit = tooling_ref.get("commit")

        if repo_path:
            env_updates[f"{env_prefix}_REPO_PATH"] = repo_path
        if repo_url:
            env_updates[f"{env_prefix}_REPO_URL"] = repo_url
        if branch:
            env_updates[f"{env_prefix}_BRANCH"] = branch
        if commit:
            env_updates[f"{env_prefix}_COMMIT"] = commit

    return env_updates


def normalize_result_name(value: str, *, label: str) -> str:
    """Normalize a result key name as a safe relative slash path."""
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty result name")
    path = Path(value.strip())
    if path.is_absolute() or ".." in path.parts:
        raise RuntimeError(f"❌ {label} must be a relative path without '..': {value}")
    parts = [part for part in path.parts if part not in ("", ".")]
    if not parts:
        raise RuntimeError(f"❌ {label} must contain at least one path segment: {value}")
    return "/".join(parts)


def ref_context_to_result_path(ref_context: str) -> str:
    return ref_context.replace(".", "/")


def resolve_result_name(args: argparse.Namespace, run_type: str) -> str:
    """Resolve the stable ctl result name for the selected runner mode."""
    if run_type == "workflow":
        if getattr(args, "target", None):
            raise RuntimeError("❌ workflow runner does not accept --target")
        raw_name = getattr(args, "workflow", None)
    elif run_type == "target":
        if getattr(args, "workflow", None):
            raise RuntimeError("❌ target runner does not accept --workflow")
        raw_name = getattr(args, "target", None)
    elif run_type == "sub_workflow":
        if getattr(args, "workflow", None) or getattr(args, "target", None):
            raise RuntimeError("❌ sub_workflow runner does not accept --workflow or --target")
        ref = getattr(args, "ref", None)
        ref_context = resolve_ref_context(ref, args.execution_params) if ref else "sub_workflow"
        raw_name = f"{ref_context_to_result_path(ref_context)}/{getattr(args, 'source', None) or 'unknown'}/{getattr(args, 'sub_workflow', None) or 'unknown'}"
    elif run_type == "maintenance":
        maintenance_target = getattr(args, "target", None) or getattr(args, "lock_id", None) or "unknown"
        raw_name = f"{getattr(args, 'maintenance_action', None) or 'maintenance'}/{maintenance_target}"
    else:
        raise RuntimeError(f"❌ unknown runner run_type {run_type!r}")

    return normalize_result_name(raw_name, label=f"{run_type} result name")


def setup_run_dirs(
    run_id: str,
    action: str,
    run_type: str,
    result_name: str,
    ctl_state_local_root: Path,
    memory_handler: logging.handlers.MemoryHandler,
) -> tuple[Path, Path, Path, Path]:
    """Create run directories under the stable ctl result key and setup file logging."""
    result_name = normalize_result_name(result_name, label="ctl result name")
    ctl_state_dir = Path(ctl_state_local_root) / action / run_type / result_name
    runs_dir = ctl_state_dir / "runs"
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Using ctl_state_dir: {ctl_state_dir}")
    logging.info(f"Using run_dir: {run_dir}")

    # Materialize the pinned ctl stage runtime once, up front — it is a run-scoped
    # (workspace-scoped) precondition, not a per-stage step. Idempotent thereafter.
    stage_utils_dir = materialize_stage_utils(run_dir)
    logging.info(f"Using ctl stage runtime: {stage_utils_dir}")

    # artifacts/ splits into general/ (run-level metadata + logs) and
    # stages/<stage>/ (per-stage outputs, created when stages run).
    artifacts_dir = run_dir / "artifacts" / "general"
    os.makedirs(artifacts_dir, exist_ok=True)

    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    os.makedirs(cfg_dir)

    stages_source_dir = run_dir / "stages_source"
    if stages_source_dir.exists():
        shutil.rmtree(stages_source_dir)

    plt_merged_dir = cfg_dir / "plt" / "merged"
    os.makedirs(plt_merged_dir)

    logs_dir = artifacts_dir / "logs"
    os.makedirs(logs_dir, exist_ok=True)
    logs_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "_" + uuid.uuid4().hex[:6]
    log_file = logs_dir / f"{SERVICE_ID}_{logs_run_id}.log"
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(file_handler)

    memory_handler.setTarget(file_handler)
    memory_handler.flush()
    logging.getLogger().removeHandler(memory_handler)

    write_run_metadata(
        run_dir,
        {
            "run_id": run_id,
            "action": action,
            "run_type": run_type,
            "result_name": result_name,
            "result_key": f"{action}/{run_type}/{result_name}",
            "ctl_state_local_root": str(Path(ctl_state_local_root)),
            "ctl_state_dir": str(ctl_state_dir),
            "run_dir": str(run_dir),
            "log_path": str(log_file),
            "target_keys": [],
            "mutation_started": False,
        },
    )

    logging.info(f"Using artifacts_dir: {artifacts_dir}")
    logging.info(f"Logging to: {log_file}")

    return run_dir, artifacts_dir, plt_merged_dir, log_file


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ctl_state_dir_from_run_dir(run_dir: Path) -> Path:
    if run_dir.parent.name != "runs":
        raise RuntimeError(f"run_dir must be under a runs/ directory: {run_dir}")
    return run_dir.parent.parent


def run_metadata_path(run_dir: Path) -> Path:
    return Path(run_dir) / RUN_METADATA_FILENAME


def write_yaml_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def load_run_metadata(run_dir: Path) -> dict:
    path = run_metadata_path(run_dir)
    if not path.is_file():
        return {"run_id": Path(run_dir).name}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ run metadata must be a mapping: {path}")
    data.setdefault("run_id", Path(run_dir).name)
    return data


def write_run_metadata(run_dir: Path, metadata: dict) -> None:
    write_yaml_file(run_metadata_path(run_dir), metadata)


def update_run_metadata(run_dir: Path, updates: dict) -> dict:
    metadata = load_run_metadata(run_dir)
    metadata.update(updates)
    write_run_metadata(run_dir, metadata)
    return metadata


def normalize_target_keys(values: list[str], *, label: str) -> list[str]:
    if not isinstance(values, list):
        raise RuntimeError(f"❌ {label} must be a list")
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = normalize_result_name(value, label=label)
        if key in seen:
            raise RuntimeError(f"❌ duplicate target key in {label}: {key}")
        seen.add(key)
        normalized.append(key)
    return normalized


def target_keys_from_active_stages(active_stages: dict) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for stage in active_stages.values():
        target_key = stage.get("target")
        if not isinstance(target_key, str) or not target_key:
            continue
        target_key = normalize_result_name(target_key, label="resolved target key")
        if target_key not in seen:
            seen.add(target_key)
            keys.append(target_key)
    return keys


def build_status_payload(run_dir: Path, status: str, extra: dict | None = None) -> dict:
    payload = dict(load_run_metadata(run_dir))
    payload["run_id"] = Path(run_dir).name
    payload["status"] = status
    payload["updated_at"] = utc_timestamp()
    if extra:
        payload.update(extra)
    return payload


def current_status_path(run_dir: Path) -> Path:
    return Path(run_dir) / "STATUS.yaml"


def load_current_status(run_dir: Path) -> dict:
    path = current_status_path(run_dir)
    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ run STATUS.yaml must be a mapping: {path}")
    return data


def write_current_status(run_dir: Path, payload: dict) -> None:
    write_yaml_file(current_status_path(run_dir), payload)


def remove_state_slot(run_dir: Path, state: str) -> None:
    slot_dir = ctl_state_dir_from_run_dir(run_dir) / state
    if slot_dir.exists():
        shutil.rmtree(slot_dir)


def write_state_slot(run_dir: Path, state: str, payload: dict) -> None:
    slot_dir = ctl_state_dir_from_run_dir(run_dir) / state
    slot_payload = dict(payload)
    slot_payload["state_slot"] = state
    slot_payload["run_path"] = f"runs/{run_dir.name}"
    write_yaml_file(slot_dir / "STATUS.yaml", slot_payload)
    write_yaml_file(
        slot_dir / "MANIFEST.yaml",
        {
            "run_id": run_dir.name,
            "run_path": f"runs/{run_dir.name}",
            "status_path": f"runs/{run_dir.name}/STATUS.yaml",
            "artifacts_path": f"runs/{run_dir.name}/artifacts",
            "updated_at": slot_payload["updated_at"],
        },
    )


def rewrite_in_progress_slot_if_present(run_dir: Path, payload: dict) -> None:
    slot_dir = ctl_state_dir_from_run_dir(run_dir) / "in_progress"
    if slot_dir.exists():
        write_state_slot(run_dir, "in_progress", payload)


def mark_run_started(run_dir: Path) -> None:
    payload = build_status_payload(run_dir, "in_progress")
    write_current_status(run_dir, payload)
    write_state_slot(run_dir, "in_progress", payload)


def record_run_target_keys(run_dir: Path, target_keys: list[str]) -> None:
    normalized = normalize_target_keys(target_keys, label="target_keys")
    metadata = update_run_metadata(run_dir, {"target_keys": normalized})
    status = load_current_status(run_dir)
    if status:
        status.update({"target_keys": normalized, "updated_at": utc_timestamp()})
        for key in ("action", "run_type", "result_name", "result_key", "ctl_state_local_root", "ctl_state_dir", "run_dir", "log_path"):
            if key in metadata:
                status[key] = metadata[key]
        write_current_status(run_dir, status)
        if status.get("status") == "in_progress":
            rewrite_in_progress_slot_if_present(run_dir, status)


def mark_mutation_started(run_dir: Path, stage_id: str) -> None:
    metadata = update_run_metadata(
        run_dir,
        {
            "mutation_started": True,
            "mutation_started_at": utc_timestamp(),
            "mutation_stage_id": stage_id,
        },
    )
    status = load_current_status(run_dir)
    if status:
        status.update(
            {
                "mutation_started": True,
                "mutation_started_at": metadata["mutation_started_at"],
                "mutation_stage_id": stage_id,
                "updated_at": utc_timestamp(),
            }
        )
        write_current_status(run_dir, status)
        if status.get("status") == "in_progress":
            rewrite_in_progress_slot_if_present(run_dir, status)
    ctl_state_push(f"mutation started ({stage_id})")


def tail_log_lines(log_path: str | None, limit: int = 40) -> list[str]:
    if not log_path:
        return []
    path = Path(log_path)
    if not path.is_file():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    return lines[-limit:]


def extract_error_summary(log_path: str | None, fallback: str) -> dict:
    tail = tail_log_lines(log_path)
    summary = fallback
    for line in reversed(tail):
        stripped = line.strip()
        if not stripped:
            continue
        if "Error:" in stripped or "CalledProcessError" in stripped or "failed" in stripped.lower():
            summary = stripped
            break
    return {"summary": summary, "tail_lines": tail}


def print_failure_summary(payload: dict) -> None:
    error = payload.get("error") or {}
    print("Run failed", file=sys.stderr)
    if payload.get("result_key"):
        print(f"result: {payload['result_key']}", file=sys.stderr)
    if payload.get("mutation_stage_id"):
        print(f"stage: {payload['mutation_stage_id']}", file=sys.stderr)
    if error.get("summary"):
        print(f"error: {error['summary']}", file=sys.stderr)
    if payload.get("log_path"):
        print(f"log: {payload['log_path']}", file=sys.stderr)


def mark_run_succeeded(run_dir: Path) -> None:
    payload = build_status_payload(run_dir, "ok", {"ctl_state_sync": ctl_state_sync_summary()})
    write_current_status(run_dir, payload)
    write_state_slot(run_dir, "committed", payload)
    remove_state_slot(run_dir, "in_progress")
    remove_state_slot(run_dir, "failed")
    mark_outdated_for_run(run_dir, include_current_result=False)
    ctl_state_push("run succeeded")
    ctl_state_remove_slots(run_dir, ("in_progress", "failed"))


def mark_run_failed(run_dir: Path, exc: BaseException) -> None:
    metadata = load_run_metadata(run_dir)
    extracted = extract_error_summary(metadata.get("log_path"), str(exc))
    payload = build_status_payload(
        run_dir,
        "failed",
        {
            "error": {
                "type": type(exc).__name__,
                "summary": extracted["summary"],
            },
            "log_path": metadata.get("log_path"),
            "tail_lines": extracted["tail_lines"],
            "ctl_state_sync": ctl_state_sync_summary(),
        },
    )
    write_current_status(run_dir, payload)
    write_state_slot(run_dir, "failed", payload)
    remove_state_slot(run_dir, "in_progress")
    mark_outdated_for_run(run_dir, include_current_result=True)
    ctl_state_push("run failed")
    ctl_state_remove_slots(run_dir, ("in_progress",))
    print_failure_summary(payload)


def parse_result_dir(ctl_state_local_root: Path, result_dir: Path) -> dict | None:
    try:
        rel = Path(result_dir).resolve().relative_to(Path(ctl_state_local_root).resolve())
    except ValueError:
        return None
    parts = rel.parts
    if len(parts) < 3:
        return None
    action, run_type = parts[0], parts[1]
    result_name = "/".join(parts[2:])
    if not action or not run_type or not result_name:
        return None
    return {
        "action": action,
        "run_type": run_type,
        "result_name": result_name,
        "result_key": f"{action}/{run_type}/{result_name}",
    }


def iter_committed_status_paths(ctl_state_local_root: Path):
    root = Path(ctl_state_local_root)
    if not root.is_dir():
        return
    yield from sorted(root.rglob("committed/STATUS.yaml"))


def load_status_mapping(path: Path) -> dict:
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ STATUS.yaml must contain a mapping: {path}")
    return data


def status_result_info(ctl_state_local_root: Path, status_path: Path, status: dict) -> dict | None:
    result_dir = status_path.parent.parent
    parsed = parse_result_dir(ctl_state_local_root, result_dir)
    if parsed is None:
        return None
    info = dict(parsed)
    for key in ("action", "run_type", "result_name", "result_key"):
        if isinstance(status.get(key), str) and status[key]:
            info[key] = status[key]
    return info


def status_target_keys(status: dict) -> list[str]:
    raw = status.get("target_keys") or []
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, str) and item]


def update_committed_manifest(status_path: Path, payload: dict) -> None:
    manifest_path = status_path.parent / "MANIFEST.yaml"
    manifest = {
        "run_id": payload.get("run_id"),
        "run_path": payload.get("run_path") or (f"runs/{payload.get('run_id')}" if payload.get("run_id") else None),
        "status_path": payload.get("status_path") or (f"runs/{payload.get('run_id')}/STATUS.yaml" if payload.get("run_id") else None),
        "artifacts_path": payload.get("artifacts_path") or (f"runs/{payload.get('run_id')}/artifacts" if payload.get("run_id") else None),
        "updated_at": payload.get("updated_at"),
    }
    write_yaml_file(manifest_path, {k: v for k, v in manifest.items() if v is not None})


def mark_committed_status_outdated(status_path: Path, status: dict, *, reason: str, caused_by: dict | None = None) -> None:
    payload = dict(status)
    payload["status"] = "outdated"
    payload["updated_at"] = utc_timestamp()
    outdated = {
        "reason": reason,
        "at": payload["updated_at"],
    }
    if caused_by is not None:
        outdated["caused_by"] = caused_by
    payload["outdated"] = outdated
    write_yaml_file(status_path, payload)
    update_committed_manifest(status_path, payload)


def mark_outdated_for_run(run_dir: Path, *, include_current_result: bool, force: bool = False) -> None:
    metadata = load_run_metadata(run_dir)
    action = metadata.get("action")
    if action not in MUTATING_ACTIONS:
        return
    if not force and metadata.get("mutation_started") is not True:
        return

    affected_target_keys = status_target_keys(metadata)
    if not affected_target_keys:
        return
    affected = set(affected_target_keys)

    ctl_state_local_root = metadata.get("ctl_state_local_root")
    current_result_key = metadata.get("result_key")
    if not isinstance(ctl_state_local_root, str) or not ctl_state_local_root:
        return

    caused_by = {
        "action": metadata.get("action"),
        "run_type": metadata.get("run_type"),
        "result_name": metadata.get("result_name"),
        "result_key": metadata.get("result_key"),
        "run_id": metadata.get("run_id") or Path(run_dir).name,
        "target_keys": affected_target_keys,
    }

    for status_path in iter_committed_status_paths(Path(ctl_state_local_root)):
        status = load_status_mapping(status_path)
        info = status_result_info(Path(ctl_state_local_root), status_path, status)
        if info is None:
            continue
        if info.get("action") == "readonly":
            continue
        if not include_current_result and info.get("result_key") == current_result_key:
            continue
        committed_keys = set(status_target_keys(status))
        if not committed_keys or not committed_keys.intersection(affected):
            continue
        mark_committed_status_outdated(
            status_path,
            status,
            reason="affected_by_mutating_run",
            caused_by=caused_by,
        )


def mark_removed_definitions_outdated(ctl_state_local_root: Path, ctl_cfg_root: Path) -> None:
    try:
        workflows = collect_resource(ctl_cfg_root, "workflows", entry_depth=2)
    except Exception as exc:
        logging.warning("Skipping definition_removed scan: failed to load workflows: %s", exc)
        workflows = {}
    try:
        targets = collect_resource(ctl_cfg_root, "targets", entry_depth=2)
    except Exception as exc:
        logging.warning("Skipping definition_removed scan: failed to load targets: %s", exc)
        targets = {}

    for status_path in iter_committed_status_paths(Path(ctl_state_local_root)):
        status = load_status_mapping(status_path)
        info = status_result_info(Path(ctl_state_local_root), status_path, status)
        if info is None:
            continue
        run_type = info.get("run_type")
        action = info.get("action")
        result_name = info.get("result_name")
        if run_type == "workflow":
            exists = result_name in (workflows.get(action) or {})
        elif run_type == "target":
            exists = result_name in (targets.get(action) or {})
        else:
            continue
        if exists or status.get("status") == "outdated":
            continue
        mark_committed_status_outdated(
            status_path,
            status,
            reason="definition_removed",
            caused_by={
                "action": action,
                "run_type": run_type,
                "result_name": result_name,
                "result_key": info.get("result_key"),
            },
        )


def ctl_state_lock_path(ctl_state_local_root: Path) -> Path:
    return Path(ctl_state_local_root) / CTL_RESULTS_LOCK_FILENAME


def ctl_state_lock_metadata_path(ctl_state_local_root: Path) -> Path:
    return Path(ctl_state_local_root) / CTL_RESULTS_LOCK_META_FILENAME


def load_ctl_state_lock_metadata(ctl_state_local_root: Path) -> dict:
    path = ctl_state_lock_metadata_path(ctl_state_local_root)
    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ ctl-state lock metadata must be a mapping: {path}")
    return data


def ctl_state_lock_matches(ctl_state_local_root: Path, lock_id: str | None) -> bool:
    if not lock_id:
        return False
    metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
    return metadata.get("run_id") == lock_id


def format_ctl_state_lock_error(ctl_state_local_root: Path, metadata: dict, *, reason: str) -> str:
    lock_id = metadata.get("run_id") or "unknown"
    details = [
        f"❌ ctl-state local root is locked: {ctl_state_local_root}",
        f"reason: {reason}",
        f"lock_id/run_id: {lock_id}",
    ]
    for key in ("action", "run_type", "result_name", "run_dir", "host", "pid", "started_at"):
        value = metadata.get(key)
        if value not in (None, ""):
            details.append(f"{key}: {value}")
    details.append("If the owning ctl process is gone, run maintenance force-unlock with --lock-id " + str(lock_id))
    return "\n".join(details)


class CtlResultsLock:
    """Local ctl-state root lock backed by flock plus explicit metadata."""

    def __init__(self, ctl_state_local_root: Path):
        self.ctl_state_local_root = Path(ctl_state_local_root)
        self.lock_path = ctl_state_lock_path(self.ctl_state_local_root)
        self.metadata_path = ctl_state_lock_metadata_path(self.ctl_state_local_root)
        self._file = None
        self.run_id: str | None = None

    def acquire(self, *, allow_stale_metadata: bool = False) -> "CtlResultsLock":
        self.ctl_state_local_root.mkdir(parents=True, exist_ok=True)
        self._file = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            metadata = load_ctl_state_lock_metadata(self.ctl_state_local_root)
            self._file.close()
            self._file = None
            raise RuntimeError(
                format_ctl_state_lock_error(
                    self.ctl_state_local_root,
                    metadata,
                    reason="another ctl process still holds the OS lock",
                )
            ) from exc

        if not allow_stale_metadata:
            metadata = load_ctl_state_lock_metadata(self.ctl_state_local_root)
            if metadata:
                self.release(clear_metadata=False)
                raise RuntimeError(
                    format_ctl_state_lock_error(
                        self.ctl_state_local_root,
                        metadata,
                        reason="stale ctl lock metadata exists",
                    )
                )
        return self

    def write_metadata(self, payload: dict) -> None:
        if self._file is None:
            raise RuntimeError("❌ cannot write ctl lock metadata before acquiring the lock")
        self.run_id = payload.get("run_id")
        write_yaml_file(self.metadata_path, payload)
        self._file.seek(0)
        self._file.truncate()
        self._file.write(f"run_id: {payload.get('run_id', '')}\n")
        self._file.flush()
        os.fsync(self._file.fileno())

    def release(self, *, clear_metadata: bool = True) -> None:
        if self._file is None:
            return
        remove_lock_file = clear_metadata
        try:
            if clear_metadata and self.metadata_path.exists():
                metadata = load_ctl_state_lock_metadata(self.ctl_state_local_root)
                if not self.run_id or metadata.get("run_id") == self.run_id:
                    self.metadata_path.unlink()
                else:
                    remove_lock_file = False
            elif not clear_metadata:
                remove_lock_file = False

            if clear_metadata and remove_lock_file:
                self._file.seek(0)
                self._file.truncate()
                self._file.flush()
                os.fsync(self._file.fileno())
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None


def acquire_ctl_state_lock(ctl_state_local_root: Path) -> CtlResultsLock:
    return CtlResultsLock(ctl_state_local_root).acquire()


def release_ctl_state_lock(lock: CtlResultsLock | None) -> None:
    if lock is not None:
        lock.release()


def should_bypass_ctl_state_lock(args: argparse.Namespace, run_type: str) -> bool:
    return (
        run_type == "maintenance"
        and getattr(args, "maintenance_action", None) == "force-unlock"
        and ctl_state_lock_matches(args.ctl_state_local_root, getattr(args, "lock_id", None))
    )


def write_ctl_state_lock_metadata(
    lock: CtlResultsLock,
    *,
    run_id: str,
    action: str,
    run_type: str,
    result_name: str,
    run_dir: Path,
) -> None:
    lock.write_metadata(
        {
            "run_id": run_id,
            "action": action,
            "run_type": run_type,
            "result_name": result_name,
            "run_dir": str(run_dir),
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": utc_timestamp(),
        }
    )


def mark_run_force_unlocked(run_dir: Path, metadata: dict, maintenance_run_dir: Path) -> None:
    run_metadata = load_run_metadata(run_dir)
    metadata_updates = {}
    for key in ("action", "run_type", "result_name", "run_dir"):
        if not run_metadata.get(key) and metadata.get(key):
            metadata_updates[key] = metadata[key]
    if metadata_updates:
        run_metadata = update_run_metadata(run_dir, metadata_updates)

    payload = build_status_payload(
        run_dir,
        "failed",
        {
            "failure_reason": "force_unlocked",
            "error": {
                "type": "ForceUnlocked",
                "summary": "ctl-state lock was cleared by maintenance force-unlock",
            },
            "force_unlocked": {
                "at": utc_timestamp(),
                "maintenance_run_id": maintenance_run_dir.name,
                "lock_metadata": metadata,
            },
        },
    )
    write_current_status(run_dir, payload)
    write_state_slot(run_dir, "failed", payload)
    remove_state_slot(run_dir, "in_progress")

    mutating = payload.get("action") in MUTATING_ACTIONS
    force_outdated = mutating and payload.get("mutation_started") is not False
    mark_outdated_for_run(run_dir, include_current_result=True, force=force_outdated)


def force_unlock_ctl_state_lock(ctl_state_local_root: Path, lock_id: str, maintenance_run_dir: Path) -> bool:
    metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
    if not metadata:
        return False

    active_run_id = metadata.get("run_id")
    if active_run_id != lock_id:
        raise RuntimeError(
            f"❌ ctl-state lock id mismatch: active lock_id/run_id is {active_run_id!r}, got {lock_id!r}"
        )

    lock = CtlResultsLock(ctl_state_local_root).acquire(allow_stale_metadata=True)
    try:
        metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
        if metadata.get("run_id") != lock_id:
            raise RuntimeError(
                f"❌ ctl-state lock changed while force-unlock was starting: expected {lock_id!r}, got {metadata.get('run_id')!r}"
            )

        raw_run_dir = metadata.get("run_dir")
        if not isinstance(raw_run_dir, str) or not raw_run_dir:
            raise RuntimeError("❌ ctl-state lock metadata is missing run_dir")
        run_dir = Path(raw_run_dir).expanduser().resolve()
        root = Path(ctl_state_local_root).resolve()
        try:
            run_dir.relative_to(root)
        except ValueError as exc:
            raise RuntimeError(f"❌ ctl-state lock run_dir is outside ctl_state_local_root: {run_dir}") from exc

        mark_run_force_unlocked(run_dir, metadata, maintenance_run_dir)
        logging.warning("Ctl results lock force-unlocked for run_id=%s", lock_id)
        lock.run_id = lock_id
        return True
    finally:
        lock.release(clear_metadata=True)


SCOPE_META_FILENAME = "__meta__.yaml"
SCOPE_COMPOSITION_FILENAME = "__scope_composition__.yaml"
SCOPE_META_SKIP_FILENAMES = {SCOPE_META_FILENAME, PLT_GUARDRAILS_FILENAME, SCOPE_COMPOSITION_FILENAME}

def selector_expected_values(expected, *, label: str) -> list[str]:
    if isinstance(expected, str) and expected:
        return [expected]
    if isinstance(expected, list) and all(isinstance(item, str) and item for item in expected):
        return expected
    raise RuntimeError(f"❌ {label} must be a non-empty string or list of non-empty strings")


EXECUTION_CONTEXT_ROOT = "execution_context"
EXECUTION_CONTEXT_NAMESPACES = ("ctl", "params")
EXECUTION_CONTEXT_REF_RE = re.compile(
    rf"^{EXECUTION_CONTEXT_ROOT}\.(?:{'|'.join(EXECUTION_CONTEXT_NAMESPACES)})\.[A-Za-z_][A-Za-z0-9_]*$"
)


def validate_execution_context_ref(ref: str, *, label: str) -> str:
    """Selector/constraint/interpolation references into the execution context are
    always fully-qualified paths starting at the root key."""
    if not isinstance(ref, str) or not ref.strip():
        raise RuntimeError(f"❌ {label}: execution-context reference must be a non-empty string")
    value = ref.strip()
    if not EXECUTION_CONTEXT_REF_RE.fullmatch(value):
        raise RuntimeError(
            f"❌ {label}: reference {value!r} must be a fully-qualified execution-context path "
            f"({EXECUTION_CONTEXT_ROOT}.<{'|'.join(EXECUTION_CONTEXT_NAMESPACES)}>.<key>)"
        )
    return value


def execution_context_miss_message(execution_context: dict[str, object], ref: str) -> str:
    available = ", ".join(sorted(execution_context)) or "none"
    return f"{ref!r} not found in execution context; available: {available}"


def selector_requirements(selectors: dict | None, *, label: str, structured_only: bool = False) -> dict[str, set[str]]:
    """Normalize selector requirements to ref -> allowed string values.

    New selector metadata uses {match: {ref: scalar}, in: {ref: [values]}}.
    Constraint `when` maps still use the direct {ref: [values]} shape, so the
    legacy direct map is accepted only when structured_only is false.
    """
    if not selectors:
        return {}
    if not isinstance(selectors, dict):
        raise RuntimeError(f"❌ selectors must be a mapping: {label}")

    uses_structured = any(key in selectors for key in ("match", "in"))
    if uses_structured:
        unknown = sorted(set(selectors) - {"match", "in"})
        if unknown:
            raise RuntimeError(f"❌ selectors has unsupported keys {unknown}: {label}")
        requirements: dict[str, set[str]] = {}
        raw_match = selectors.get("match") or {}
        raw_in = selectors.get("in") or {}
        if not isinstance(raw_match, dict):
            raise RuntimeError(f"❌ selectors.match must be a mapping: {label}")
        if not isinstance(raw_in, dict):
            raise RuntimeError(f"❌ selectors.in must be a mapping: {label}")
        overlap = set(raw_match) & set(raw_in)
        if overlap:
            raise RuntimeError(f"❌ selector refs cannot appear in both match and in: {sorted(overlap)} ({label})")
        for ref, expected in raw_match.items():
            ref = validate_execution_context_ref(ref, label=f"{label}.match")
            values = selector_expected_values(expected, label=f"{label}.match.{ref}")
            if len(values) != 1:
                raise RuntimeError(f"❌ {label}.match.{ref} must be one exact value")
            requirements[ref] = set(values)
        for ref, expected in raw_in.items():
            ref = validate_execution_context_ref(ref, label=f"{label}.in")
            requirements[ref] = set(selector_expected_values(expected, label=f"{label}.in.{ref}"))
        return requirements

    if structured_only:
        raise RuntimeError(f"❌ selectors must use match/in form: {label}")

    requirements = {}
    for ref, expected in selectors.items():
        ref = validate_execution_context_ref(ref, label=label)
        requirements[ref] = set(selector_expected_values(expected, label=f"{label}.{ref}"))
    return requirements


def selector_matches(
    selectors: dict | None,
    execution_context: dict[str, object],
    *,
    label: str,
    structured_only: bool = False,
) -> bool:
    """Return whether selector constraints match the execution context.

    Uniform surface: any fully-qualified execution-context path is usable; a
    missing key means no match (the gated entry is simply inactive), never an
    error here. The miss is logged with the available keys so a typo'd
    execution input is self-evident.
    """
    requirements = selector_requirements(selectors, label=label, structured_only=structured_only)
    for ref, allowed_values in requirements.items():
        if ref not in execution_context:
            logging.info("Selector %s: %s", label, execution_context_miss_message(execution_context, ref))
            return False
        if str(execution_context[ref]) not in allowed_values:
            return False
    return True


def selectors_to_in_shape(requirements: dict[str, set[str]]) -> dict:
    if not requirements:
        return {}
    return {"in": {ref: sorted(values) for ref, values in sorted(requirements.items())}}


def selector_subset(child: dict | None, parent: dict | None, *, child_label: str, parent_label: str) -> tuple[bool, str | None]:
    child_req = selector_requirements(child, label=child_label)
    parent_req = selector_requirements(parent, label=parent_label)
    for ref, child_values in child_req.items():
        parent_values = parent_req.get(ref)
        if parent_values is None:
            continue
        extra = sorted(child_values - parent_values)
        if extra:
            return False, f"{ref}={extra} not allowed by target {ref}={sorted(parent_values)}"
    return True, None


def selector_requirements_cover_scope(declaration_selectors: dict | None, scope_selectors: dict | None, *, label: str) -> bool:
    declaration_req = selector_requirements(declaration_selectors, label=label)
    scope_req = selector_requirements(scope_selectors, label=f"{label} scope", structured_only=True)
    for ref, declaration_values in declaration_req.items():
        scope_values = scope_req.get(ref)
        if scope_values is None or not scope_values <= declaration_values:
            return False
    return True

CONTEXT_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXECUTION_PARAMS_KEY = "execution_params"
EXECUTION_CONTEXT_PARAM_REF_RE = re.compile(
    rf"^\$\{{({EXECUTION_CONTEXT_ROOT}\.(?:ctl|params)\.[A-Za-z_][A-Za-z0-9_]*)\}}$"
)


def _context_scalar(value, *, label: str):
    if isinstance(value, (str, int, float, bool)):
        return value
    raise RuntimeError(f"❌ {label} must resolve to a scalar string/number/bool value")


def load_execution_params(ctl_cfg_root: Path) -> dict[str, object]:
    """Read consumer param declarations discovered by the `execution_params`
    content key. Each value is a literal scalar or a whole-value fully-qualified
    reference into ctl/params (resolved against CLI params + promoted args)."""
    entries: dict[str, object] = {}
    origins: dict[str, Path] = {}
    for path, section in collect_top_level_sections(ctl_cfg_root, EXECUTION_PARAMS_KEY):
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ {EXECUTION_PARAMS_KEY} must be a mapping: {path}")
        for key, raw in section.items():
            if key in entries:
                raise RuntimeError(
                    f"❌ duplicate {EXECUTION_PARAMS_KEY}.{key}: {path} (also defined in {origins[key]})"
                )
            if not isinstance(key, str) or not CONTEXT_KEY_RE.fullmatch(key):
                raise RuntimeError(f"❌ {EXECUTION_PARAMS_KEY} key must be a valid identifier: {key!r}")
            entries[key] = raw
            origins[key] = path
    return entries


def build_execution_context(
    ctl_cfg_root: Path,
    *,
    action: str | None,
    ctl_profile: str | None,
    execution_params: dict[str, str],
) -> dict[str, object]:
    """Build the flat dotted execution context: the closed, namespaced facts of
    this execution. Two namespaces — `ctl` (promoted engine args) and `params`
    (consumer values, merged from --execution-params CLI + the execution_params
    cfg block). Keys look like 'execution_context.params.env_type'."""
    context: dict[str, object] = {}

    def put(namespace: str, key: str, value, *, label: str) -> None:
        if not CONTEXT_KEY_RE.fullmatch(key):
            raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
        context[f"{EXECUTION_CONTEXT_ROOT}.{namespace}.{key}"] = _context_scalar(value, label=label)

    if action is not None:
        put("ctl", "action", action, label="promoted --action")
    if ctl_profile is not None:
        put("ctl", "profile", ctl_profile, label="promoted --ctl-profile")

    # cfg-declared params are inserted first (so they lead the rendered
    # context), but CLI values are staged up front so cfg params may still
    # reference them. Collision semantics are unchanged (hard error).
    staged_cli: dict[str, str] = {}
    for key, value in execution_params.items():
        label = f"--execution-params {key}"
        if not CONTEXT_KEY_RE.fullmatch(key):
            raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
        staged_cli[key] = _context_scalar(value, label=label)
    lookup = dict(context)
    lookup.update({f"{EXECUTION_CONTEXT_ROOT}.params.{key}": value for key, value in staged_cli.items()})

    for key, raw in load_execution_params(ctl_cfg_root).items():
        label = f"{EXECUTION_PARAMS_KEY}.{key}"
        if key in staged_cli:
            raise RuntimeError(
                f"❌ {label} collides with a --execution-params CLI value; define it in one place"
            )
        if isinstance(raw, str):
            match = EXECUTION_CONTEXT_PARAM_REF_RE.match(raw.strip())
            if match:
                ref = match.group(1)
                if ref not in lookup:
                    continue
                put("params", key, lookup[ref], label=label)
                lookup[f"{EXECUTION_CONTEXT_ROOT}.params.{key}"] = lookup[ref]
                continue
            if "${" in raw:
                raise RuntimeError(
                    f"❌ {label}: only a literal or a whole-value "
                    f"${{{EXECUTION_CONTEXT_ROOT}.<ctl|params>.<key>}} reference is allowed, got {raw!r}"
                )
        put("params", key, raw, label=label)
        lookup[f"{EXECUTION_CONTEXT_ROOT}.params.{key}"] = context[f"{EXECUTION_CONTEXT_ROOT}.params.{key}"]

    for key, value in staged_cli.items():
        put("params", key, value, label=f"--execution-params {key}")
    return context


def execution_context_nested(execution_context: dict[str, object]) -> dict[str, dict[str, object]]:
    """Nested {execution_context: {ctl: {...}, params: {...}}} view."""
    nested: dict[str, dict[str, object]] = {ns: {} for ns in EXECUTION_CONTEXT_NAMESPACES}
    for ref, value in execution_context.items():
        _, namespace, key = ref.split(".", 2)
        nested[namespace][key] = value
    return {EXECUTION_CONTEXT_ROOT: nested}


def scope_params_from_context(execution_context: dict[str, object]) -> dict[str, str]:
    """Bare param map used for scope-identity activation (scope mechanism)."""
    prefix = f"{EXECUTION_CONTEXT_ROOT}.params."
    return {ref[len(prefix):]: str(value) for ref, value in execution_context.items() if ref.startswith(prefix)}


def write_execution_context_artifact(run_dir: Path, execution_context: dict[str, object]) -> Path:
    path = run_dir / "execution" / EXECUTION_CONTEXT_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    write_yaml_file(path, execution_context_nested(execution_context))
    return path



GUARD_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def guard_value_text(value, *, label: str) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        text = str(value)
        if text:
            return text
    raise RuntimeError(f"❌ guarded var {label} must resolve to a non-empty scalar value")


def guard_value_hash(value, *, label: str) -> str:
    return hashlib.sha256(guard_value_text(value, label=label).encode("utf-8")).hexdigest()


def guard_entry(value, *, label: str) -> dict[str, str]:
    """Baseline entry for a guarded value: plaintext + its sha256.

    The value makes baselines/diffs reviewable and errors readable; the hash
    stays the comparison primitive and doubles as the entry's self-integrity
    check (a hand-edited value with a stale hash is rejected at load)."""
    text = guard_value_text(value, label=label)
    return {"value": text, "hash": hashlib.sha256(text.encode("utf-8")).hexdigest()}


def validate_guard_hash(raw_hash, *, label: str) -> str:
    if not isinstance(raw_hash, str) or not GUARD_HASH_RE.fullmatch(raw_hash):
        raise RuntimeError(f"❌ {label} must be a lowercase sha256 hex string")
    return raw_hash


def merge_guarded_vars(dst: dict[str, dict[str, str]], raw_guarded_vars, *, origin: Path) -> None:
    if raw_guarded_vars is None:
        return
    if not isinstance(raw_guarded_vars, dict):
        raise RuntimeError(f"❌ guarded_vars must be a mapping: {origin}")
    for var_name, raw_entry in raw_guarded_vars.items():
        if not isinstance(var_name, str) or not var_name.strip():
            raise RuntimeError(f"❌ guarded_vars keys must be non-empty strings: {origin}")
        if var_name in dst:
            raise RuntimeError(f"❌ duplicate guarded var {var_name!r}: {origin}")
        label = f"guarded_vars.{var_name}"
        if not isinstance(raw_entry, dict) or set(raw_entry) != {"value", "hash"}:
            raise RuntimeError(f"❌ {label} must be a mapping with exactly value + hash: {origin}")
        value = raw_entry["value"]
        if not isinstance(value, str) or not value:
            raise RuntimeError(f"❌ {label}.value must be a non-empty string: {origin}")
        entry_hash = validate_guard_hash(raw_entry["hash"], label=f"{label}.hash")
        computed = hashlib.sha256(value.encode("utf-8")).hexdigest()
        if computed != entry_hash:
            raise RuntimeError(
                f"❌ {label} is self-inconsistent: value {value!r} hashes to {computed}, "
                f"but hash says {entry_hash} (regenerate the baseline): {origin}"
            )
        dst[var_name] = {"value": value, "hash": entry_hash}


def load_plt_guard_declarations(plt_cfg_root: Path) -> list[dict]:
    """Load root plt guard declarations: declare -> [{path, match_target_path, selectors}].

    The root declarations live either in a single `__guardrails__.yaml` file or,
    split by concern, in a `__guardrails__/` directory of *.yaml files merged by
    structure with duplicate detection — never both.
    """
    file_path = plt_cfg_root / PLT_GUARDRAILS_FILENAME
    dir_path = plt_cfg_root / PLT_GUARDRAILS_DIRNAME
    if file_path.is_file() and dir_path.is_dir():
        raise RuntimeError(
            f"❌ both {file_path.name} and {dir_path.name}/ exist at the plt cfg root; keep exactly one: {plt_cfg_root}"
        )
    if dir_path.is_dir():
        sources = sorted(p for p in dir_path.glob("*.yaml") if p.is_file())
        if not sources:
            raise RuntimeError(f"❌ {dir_path.name}/ contains no *.yaml declaration files: {dir_path}")
    elif file_path.is_file():
        sources = [file_path]
    else:
        return []

    declarations: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for path in sources:
        _load_guard_declarations_file(path, declarations, seen)
    return declarations


def _load_guard_declarations_file(path: Path, declarations: list[dict], seen: set[tuple[str, str]]) -> None:
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ {path.name} must contain a mapping: {path}")
    unknown = set(data) - {"declare", "baseline_axes"}
    if unknown:
        raise RuntimeError(f"❌ root {path.name} has unsupported keys {sorted(unknown)}: {path}")
    raw_declarations = data.get("declare") or []
    if not isinstance(raw_declarations, list):
        raise RuntimeError(f"❌ declare must be a list: {path}")

    # baseline_axes: execution params this file's baselines vary by. A scope
    # matched by any declaration carrying axes stores one baseline file PER
    # axis-value combination (<scope>/__guardrails__/<v1>[__<v2>].yaml) instead
    # of the flat <scope>/__guardrails__.yaml — the per-dir mechanism can't
    # help when one scope dir serves several param values (e.g. env scopes
    # across landing zones). Axes are consumer vocabulary: any params ref.
    raw_axes = data.get("baseline_axes") or []
    if not isinstance(raw_axes, list) or not all(isinstance(a, str) and a.strip() for a in raw_axes):
        raise RuntimeError(f"❌ baseline_axes must be a list of non-empty strings: {path}")
    axes: list[str] = []
    for axis in raw_axes:
        axis = validate_execution_context_ref(axis.strip(), label=f"baseline_axes in {path}")
        if not axis.startswith(EXECUTION_CONTEXT_PARAMS_PREFIX):
            raise RuntimeError(
                f"❌ baseline_axes entries must be params refs ({EXECUTION_CONTEXT_PARAMS_PREFIX}<key>): {axis!r} in {path}"
            )
        if axis in axes:
            raise RuntimeError(f"❌ duplicate baseline axis {axis!r}: {path}")
        axes.append(axis)

    for index, raw in enumerate(raw_declarations):
        label = f"declare[{index}] in {path}"
        if not isinstance(raw, dict):
            raise RuntimeError(f"❌ {label} must be a mapping")
        unknown = set(raw) - {"path", "match_target_path", "selectors"}
        if unknown:
            raise RuntimeError(f"❌ {label} has unsupported keys {sorted(unknown)}")
        var_name = raw.get("path")
        if not isinstance(var_name, str) or not var_name.strip():
            raise RuntimeError(f"❌ {label} path must be a non-empty string")
        var_name = var_name.strip()
        if "." in var_name:
            raise RuntimeError(f"❌ {label} path must be a top-level key (no dots): {var_name!r}")
        match_target_path = normalize_cfg_absolute_path(
            raw.get("match_target_path"),
            label=f"{label} match_target_path",
        )
        selectors = raw.get("selectors")
        if selectors is not None and not isinstance(selectors, dict):
            raise RuntimeError(f"❌ {label} selectors must be a mapping")
        key = (var_name, match_target_path)
        if key in seen:
            raise RuntimeError(f"❌ duplicate declaration for {var_name!r} at {match_target_path!r}: {path}")
        seen.add(key)
        declarations.append(
            {
                "path": var_name,
                "match_target_path": match_target_path,
                "selectors": selectors or {},
                "baseline_axes": tuple(axes),
            }
        )


def load_scope_guarded_vars(path: Path, *, allow_missing: bool = False) -> dict[str, dict[str, str]]:
    """Load a scope-local generated baseline file: guarded_vars -> {var: {value, hash}}."""
    if not path.is_file():
        if allow_missing:
            return {}
        raise RuntimeError(f"❌ guardrails baseline file not found: {path}")
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ {path.name} must contain a mapping: {path}")
    unknown = set(data) - {"guarded_vars"}
    if unknown:
        raise RuntimeError(f"❌ scope {path.name} has unsupported keys {sorted(unknown)}: {path}")
    guarded: dict[str, dict[str, str]] = {}
    merge_guarded_vars(guarded, data.get("guarded_vars"), origin=path)
    return guarded


def scope_guard_baseline_path(scope: dict, matching_declarations: list[dict], scope_params: dict[str, str]) -> Path:
    """Baseline file for a scope: flat `__guardrails__.yaml`, or one file per
    axis-value combination under `__guardrails__/` when any matching
    declaration carries baseline_axes (values read from the run's params)."""
    axes = sorted({axis for d in matching_declarations for axis in d.get("baseline_axes", ())})
    scope_root = scope["scope_root"]
    flat = scope_root / PLT_GUARDRAILS_FILENAME
    axis_dir = scope_root / PLT_GUARDRAILS_DIRNAME
    if not axes:
        if axis_dir.is_dir():
            raise RuntimeError(
                f"❌ scope {scope['scope_path']} has a {PLT_GUARDRAILS_DIRNAME}/ dir but no declaration "
                f"defines baseline_axes; remove the dir or declare the axes"
            )
        return flat
    if flat.is_file():
        raise RuntimeError(
            f"❌ scope {scope['scope_path']} has a flat {PLT_GUARDRAILS_FILENAME} but its declarations "
            f"define baseline_axes {axes}; remove the stale flat baseline"
        )
    values: list[str] = []
    for axis in axes:
        key = axis[len(EXECUTION_CONTEXT_PARAMS_PREFIX):]
        value = scope_params.get(key)
        if not value:
            raise RuntimeError(
                f"❌ baseline axis {axis!r} for scope {scope['scope_path']} has no value in this run's "
                f"params; pass --execution-params {key}=<value>"
            )
        if not re.fullmatch(r"[A-Za-z0-9_-]+", str(value)):
            raise RuntimeError(f"❌ baseline axis {axis!r} value {value!r} is not filename-safe")
        values.append(str(value))
    return axis_dir / ("__".join(values) + ".yaml")


def guard_declaration_matches_scope(declaration: dict, scope: dict) -> bool:
    if declaration["match_target_path"] != scope["target_path"]:
        return False
    return selector_requirements_cover_scope(
        declaration["selectors"],
        scope.get("selectors") or {},
        label=f"guard declaration {declaration['path']!r}",
    )


def load_ctl_guarded_vars(ctl_cfg_root: Path) -> dict[str, dict[str, str]]:
    guarded: dict[str, dict[str, str]] = {}
    for path, section in collect_top_level_sections(ctl_cfg_root, "guardrails"):
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ guardrails must be a mapping: {path}")
        unknown = set(section) - {"guarded_vars"}
        if unknown:
            raise RuntimeError(f"❌ guardrails has unsupported keys {sorted(unknown)}: {path}")
        merge_guarded_vars(guarded, section.get("guarded_vars"), origin=path)
    return guarded


def verify_guarded_value(
    owner: str,
    var_name: str,
    value,
    expected: dict[str, str],
    *,
    label: str,
) -> None:
    actual = guard_entry(value, label=f"{owner}.{var_name}")
    if actual["hash"] != expected["hash"]:
        raise RuntimeError(
            f"❌ guarded {owner} var {var_name!r} changed: "
            f"expected {expected['value']!r} ({expected['hash']}), "
            f"got {actual['value']!r} ({actual['hash']})"
        )


CTL_STATE_BACKENDS_GUARD_PREFIX = "ctl_state_backends."
CTL_STATE_BUCKETS_GUARD_PREFIX = "ctl_state_buckets."  # legacy guard ref alias
EXECUTION_CONTEXT_PARAMS_PREFIX = "execution_context.params."
CTL_STATE_BACKENDS_GUARDABLE_FIELDS = ("bucket_name", "bucket_region")


def validate_ctl_guard_ref(ref: str, *, label: str = "ctl guarded_vars") -> str:
    """A ctl guarded var is keyed by one of two fully-qualified ref forms:

    - an execution-context ref (`execution_context.params.<key>`) —
      `execution_context.ctl.*` is forbidden: action/profile are per-run
      switches, so hashing them can never be correct;
    - a ctl-state registry ref (`ctl_state_backends.<domain>.<bucket_name|bucket_region>`)
      — resolved from the registry (env-invariant values only, e.g. the
      org_state bucket)."""
    if not isinstance(ref, str) or not ref.strip():
        raise RuntimeError(f"❌ {label}: guard ref must be a non-empty string")
    value = ref.strip()
    if value.startswith(CTL_STATE_BACKENDS_GUARD_PREFIX) or value.startswith(CTL_STATE_BUCKETS_GUARD_PREFIX):
        parts = value.split(".")
        if len(parts) != 3 or parts[2] not in CTL_STATE_BACKENDS_GUARDABLE_FIELDS:
            raise RuntimeError(
                f"❌ {label}: {value!r} must be ctl_state_backends.<domain>."
                f"<{'|'.join(CTL_STATE_BACKENDS_GUARDABLE_FIELDS)}>"
            )
        if value.startswith(CTL_STATE_BUCKETS_GUARD_PREFIX):
            return CTL_STATE_BACKENDS_GUARD_PREFIX + value[len(CTL_STATE_BUCKETS_GUARD_PREFIX):]
        return value
    value = validate_execution_context_ref(value, label=label)
    _, namespace, _ = value.split(".", 2)
    if namespace == "ctl":
        raise RuntimeError(
            f"❌ {label}: {value!r} guards an {EXECUTION_CONTEXT_ROOT}.ctl.* value; "
            f"action/profile are per-run switches and can never be guarded"
        )
    return value


def resolve_ctl_guard_value(ref: str, ctl_cfg_root: Path, execution_context: dict[str, object]):
    """Resolve a validated ctl guard ref to its current value.

    Registry refs return the RAW registry pattern (un-interpolated): registry
    values may legitimately vary per run param (env_type, landing_zone), so the
    guard pins the cfg TEXT — a silent edit of the pattern is caught, while
    param-driven variation stays free (params are ctl-owned; main_tag is
    guarded separately as a context ref). Context refs read the execution
    context directly."""
    if ref.startswith(CTL_STATE_BACKENDS_GUARD_PREFIX) or ref.startswith(CTL_STATE_BUCKETS_GUARD_PREFIX):
        if ref.startswith(CTL_STATE_BUCKETS_GUARD_PREFIX):
            ref = CTL_STATE_BACKENDS_GUARD_PREFIX + ref[len(CTL_STATE_BUCKETS_GUARD_PREFIX):]
        _, domain, field = ref.split(".")
        buckets = load_ctl_state_backends_cfg(ctl_cfg_root) or {}
        entry = buckets.get(domain)
        if entry is None:
            known = ", ".join(sorted(buckets)) or "none"
            raise RuntimeError(f"❌ ctl guarded var {ref!r}: unknown domain {domain!r}; known: {known}")
        return str(entry[field])
    if ref not in execution_context:
        raise RuntimeError(
            f"❌ ctl guarded var {ref!r}: {execution_context_miss_message(execution_context, ref)}"
        )
    return execution_context[ref]


def verify_ctl_guardrails(ctl_cfg_root: Path, execution_context: dict[str, object]) -> None:
    guarded = load_ctl_guarded_vars(ctl_cfg_root)
    if not guarded:
        return
    for ref, expected in guarded.items():
        validate_ctl_guard_ref(ref)
        value = resolve_ctl_guard_value(ref, ctl_cfg_root, execution_context)
        verify_guarded_value(
            "ctl",
            ref,
            value,
            expected,
            label=ref,
        )
    logging.info("Ctl guardrails verified: %s", sorted(guarded))


def load_rendered_cfg_top_level_values(rendered_dir: Path, var_name: str) -> list[tuple[Path, object]]:
    values: list[tuple[Path, object]] = []
    for path in sorted(rendered_dir.rglob("*.yaml")):
        if path.name in SCOPE_META_SKIP_FILENAMES or PLT_GUARDRAILS_DIRNAME in path.parts:
            continue
        data = load_yaml(path) or {}
        if not isinstance(data, dict) or var_name not in data:
            continue
        values.append((path, data[var_name]))
    return values


def read_rendered_guard_value(rendered_target_dir: Path, var_name: str, *, label: str):
    """Read one guarded top-level value from a rendered scope target dir.

    Requires presence, cross-file agreement, and a fully-resolved value —
    a leftover ${...} placeholder is a hard error, never hashed.
    """
    values = load_rendered_cfg_top_level_values(rendered_target_dir, var_name)
    if not values:
        raise RuntimeError(f"❌ {label} guarded var {var_name!r} was not found in {rendered_target_dir}")
    first_path, first_value = values[0]
    first_text = guard_value_text(first_value, label=f"{label}.{var_name} at {first_path}")
    for path, value in values[1:]:
        value_text = guard_value_text(value, label=f"{label}.{var_name} at {path}")
        if value_text != first_text:
            raise RuntimeError(
                f"❌ {label} guarded var {var_name!r} has multiple active values: "
                f"{first_path}={first_text!r}, {path}={value_text!r}"
            )
    if "${" in first_text:
        raise RuntimeError(
            f"❌ {label} guarded var {var_name!r} is not fully resolved after render: {first_text!r}"
        )
    return first_value


def rendered_scope_target_dir(plt_rendered_dir: Path, target_path: str) -> Path:
    target_rel = target_path.lstrip("/")
    target_dir = (plt_rendered_dir / target_rel).resolve()
    try:
        target_dir.relative_to(plt_rendered_dir.resolve())
    except ValueError as exc:
        raise RuntimeError(f"Scope target_path escapes rendered cfg dir: {target_path}") from exc
    return target_dir


def required_target_paths_for_stages(active_stages: dict) -> set[str] | None:
    """Target paths this run's stages consume, from their cfg_roots.

    Scopes are independent units: a run merges/renders/guard-verifies only the
    scopes serving its stages' cfg_roots. Returns None when any stage consumes
    the root ("/") — the deliberate escape hatch meaning every scope."""
    paths: set[str] = set()
    for stage in active_stages.values():
        cfg_root = str(stage.get("cfg_root") or "/")
        segments = [part for part in cfg_root.split("/") if part]
        if not segments:
            return None
        paths.add(f"/{segments[0]}")
    return paths


def verify_plt_guardrails(
    plt_cfg_root: Path,
    plt_rendered_dir: Path,
    scope_params: dict[str, str],
    required_target_paths: set[str] | None = None,
) -> None:
    """Verify declared plt guards against the rendered cfg of every active scope.

    Coverage: each declaration matching an active scope must have a baseline
    hash in that scope's local guardrails file, and every baseline hash must
    correspond to a matching declaration. With `required_target_paths` set,
    only the scopes this run merged/rendered are verified (scopes are
    independent; the full tree is checked by the validate-all action).
    """
    declarations = load_plt_guard_declarations(plt_cfg_root)
    if not declarations:
        return
    if not discover_cfg_meta_paths(plt_cfg_root):
        raise RuntimeError(f"❌ plt guard declarations exist but no cfg scopes found under: {plt_cfg_root}")

    for scope in discover_active_cfg_scopes(plt_cfg_root, scope_params=scope_params):
        if required_target_paths is not None and scope["target_path"] not in required_target_paths:
            continue
        matching = [d for d in declarations if guard_declaration_matches_scope(d, scope)]
        baseline_path = scope_guard_baseline_path(scope, matching, scope_params)
        baseline = load_scope_guarded_vars(baseline_path, allow_missing=True)
        if not matching and not baseline:
            continue

        declared_names = {d["path"] for d in matching}
        unbaselined = sorted(declared_names - set(baseline))
        if unbaselined:
            raise RuntimeError(
                f"❌ guarded vars {unbaselined} declared for scope {scope['scope_path']} have no baseline "
                f"in {baseline_path}; run regenerate_guardrails.py for this variation"
            )
        undeclared = sorted(set(baseline) - declared_names)
        if undeclared:
            raise RuntimeError(
                f"❌ baseline hashes {undeclared} in {baseline_path} have no matching declaration; "
                f"remove them or declare them at the plt cfg root"
            )

        target_dir = rendered_scope_target_dir(plt_rendered_dir, scope["target_path"])
        if not target_dir.is_dir():
            raise RuntimeError(
                f"❌ rendered target dir not found for scope {scope['scope_path']}: {target_dir}"
            )
        label = f"plt scope {scope['scope_path']}->{scope['target_path']}"
        for declaration in matching:
            var_name = declaration["path"]
            value = read_rendered_guard_value(target_dir, var_name, label=label)
            verify_guarded_value("plt", var_name, value, baseline[var_name], label=f"{label}.{var_name}")
        logging.info("Plt guardrails verified for %s: %s", scope["scope_path"], sorted(declared_names))


def verify_guardrails(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
    required_target_paths: set[str] | None = None,
) -> None:
    verify_ctl_guardrails(ctl_cfg_root, execution_context)
    verify_plt_guardrails(plt_cfg_root, plt_rendered_dir, scope_params, required_target_paths)



def normalize_cfg_absolute_path(raw_value, *, label: str, allow_root: bool = False) -> str:
    """Normalize a cfg-root absolute path used by plt metadata."""
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise RuntimeError(f"{label} must be a non-empty string")
    value = raw_value.strip()
    if "\\" in value:
        raise RuntimeError(f"{label} must use forward slashes: {value}")
    if not value.startswith("/"):
        raise RuntimeError(f"{label} must start with /: {value}")

    parts = [part for part in value.split("/") if part]
    if any(part in (".", "..") for part in parts):
        raise RuntimeError(f"{label} must not contain . or ..: {value}")
    normalized = "/" + "/".join(parts)
    if normalized == "/" and not allow_root:
        raise RuntimeError(f"{label} must not be /")
    return normalized


def cfg_abs_path_to_dir(cfg_root: Path, abs_path: str, *, label: str) -> Path:
    """Resolve a normalized cfg-root absolute path to a directory under cfg_root."""
    normalized = normalize_cfg_absolute_path(abs_path, label=label, allow_root=True)
    rel = normalized.lstrip("/")
    path = (cfg_root / rel).resolve() if rel else cfg_root.resolve()
    try:
        path.relative_to(cfg_root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"{label} escapes cfg root: {abs_path}") from exc
    return path


def discover_cfg_meta_paths(plt_cfg_root: Path) -> list[Path]:
    """Find cfg metadata files, excluding git internals."""
    cfg_root = plt_cfg_root.resolve()
    meta_paths: list[Path] = []
    for meta_path in sorted(cfg_root.rglob(SCOPE_META_FILENAME)):
        rel = meta_path.relative_to(cfg_root)
        if ".git" in rel.parts:
            continue
        meta_paths.append(meta_path)
    return meta_paths


def load_cfg_meta(meta_path: Path) -> dict:
    """Load typed cfg metadata from __meta__.yaml."""
    meta_cfg = load_yaml(meta_path) or {}
    if not isinstance(meta_cfg, dict):
        raise RuntimeError(f"{SCOPE_META_FILENAME} must contain a mapping: {meta_path}")

    meta_type = meta_cfg.get("type")
    if meta_type not in ("scope", "overlay"):
        raise RuntimeError(
            f"{SCOPE_META_FILENAME} type must be 'scope' or 'overlay': {meta_path}"
        )
    return meta_cfg


def find_nested_cfg_meta(root: Path, *, exclude: Path | None = None) -> Path | None:
    """Return a nested metadata file under root, ignoring an optional root meta."""
    root_resolved = root.resolve()
    exclude_resolved = exclude.resolve() if exclude is not None else None
    for meta_path in sorted(root_resolved.rglob(SCOPE_META_FILENAME)):
        if exclude_resolved is not None and meta_path.resolve() == exclude_resolved:
            continue
        rel = meta_path.relative_to(root_resolved)
        if ".git" in rel.parts:
            continue
        return meta_path
    return None


def execution_context_from_scope_params(scope_params: dict[str, str]) -> dict[str, object]:
    return {f"{EXECUTION_CONTEXT_PARAMS_PREFIX}{key}": value for key, value in (scope_params or {}).items()}


def scope_prefix_matches(scope_id: str, prefix: str) -> bool:
    return scope_id == prefix or scope_id.startswith(prefix + "/")


def load_scope_composition(plt_cfg_root: Path) -> dict[str, list[str]]:
    path = plt_cfg_root / SCOPE_COMPOSITION_FILENAME
    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ {SCOPE_COMPOSITION_FILENAME} must contain a mapping: {path}")
    unknown = set(data) - {"scope_composition"}
    if unknown:
        raise RuntimeError(f"❌ {SCOPE_COMPOSITION_FILENAME} has unsupported keys {sorted(unknown)}: {path}")
    raw_rules = data.get("scope_composition") or []
    if not isinstance(raw_rules, list):
        raise RuntimeError(f"❌ scope_composition must be a list: {path}")

    rules: dict[str, list[str]] = {}
    for index, raw_rule in enumerate(raw_rules):
        label = f"scope_composition[{index}] in {path}"
        if not isinstance(raw_rule, dict):
            raise RuntimeError(f"❌ {label} must be a mapping")
        unknown = set(raw_rule) - {"target_path", "scopes"}
        if unknown:
            raise RuntimeError(f"❌ {label} has unsupported keys {sorted(unknown)}")
        target_path = normalize_cfg_absolute_path(raw_rule.get("target_path"), label=f"{label}.target_path")
        if target_path in rules:
            raise RuntimeError(f"❌ duplicate scope composition target_path {target_path!r}: {path}")
        raw_scopes = raw_rule.get("scopes") or []
        if not isinstance(raw_scopes, list) or not raw_scopes:
            raise RuntimeError(f"❌ {label}.scopes must be a non-empty list")
        prefixes: list[str] = []
        for raw_scope in raw_scopes:
            prefix = normalize_cfg_absolute_path(raw_scope, label=f"{label}.scopes")
            if prefix in prefixes:
                raise RuntimeError(f"❌ duplicate scope composition prefix {prefix!r}: {label}")
            prefixes.append(prefix)
        rules[target_path] = prefixes
    return rules


def validate_scope_composition(active_scopes: list[dict], composition: dict[str, list[str]]) -> None:
    target_scopes: dict[str, list[dict]] = collections.defaultdict(list)
    for scope in active_scopes:
        target_scopes[scope["target_path"]].append(scope)

    for target_path, scopes in target_scopes.items():
        prefixes = composition.get(target_path)
        if prefixes is None:
            if len(scopes) > 1:
                rendered = ", ".join(str(scope["meta_path"]) for scope in scopes)
                raise RuntimeError(f"Duplicate active cfg target_path {target_path!r}: {rendered}")
            continue

        seen_prefixes: dict[str, dict] = {}
        seen_match: dict[tuple[tuple[str, tuple[str, ...]], ...], dict] = {}
        for scope in scopes:
            matches = [prefix for prefix in prefixes if scope_prefix_matches(scope["scope_id"], prefix)]
            if len(matches) != 1:
                raise RuntimeError(
                    f"❌ active cfg scope {scope['scope_id']} -> {target_path} must match exactly one "
                    f"scope_composition prefix {prefixes}; matched {matches}"
                )
            prefix = matches[0]
            previous = seen_prefixes.get(prefix)
            if previous is not None:
                raise RuntimeError(
                    f"❌ multiple active cfg scopes for target_path {target_path!r} and prefix {prefix!r}: "
                    f"{previous['meta_path']} and {scope['meta_path']}"
                )
            seen_prefixes[prefix] = scope

            match_req = selector_requirements((scope.get("selectors") or {}).get("match") and {"match": (scope.get("selectors") or {}).get("match")}, label=f"scope {scope['scope_id']} match")
            match_key = tuple(sorted((ref, tuple(sorted(values))) for ref, values in match_req.items()))
            previous_match = seen_match.get(match_key)
            if match_key and previous_match is not None:
                raise RuntimeError(
                    f"❌ duplicate active cfg scope match for target_path {target_path!r}: "
                    f"{previous_match['meta_path']} and {scope['meta_path']}"
                )
            if match_key:
                seen_match[match_key] = scope


def validate_no_cfg_meta_inside_data_dir(src: Path, *, import_path: str, meta_path: Path) -> None:
    """Reject imports that point at another metadata-owned tree."""
    nested_meta = find_nested_cfg_meta(src)
    if nested_meta is not None:
        raise RuntimeError(
            f"Import path must be a data directory, not a tree containing {SCOPE_META_FILENAME}: "
            f"{import_path} ({meta_path}); found {nested_meta}"
        )


def load_scope_candidate(
    plt_cfg_root: Path,
    meta_path: Path,
    meta_cfg: dict,
    execution_context: dict[str, object],
) -> dict | None:
    """Load one scope __meta__.yaml and return an active merge scope, or None."""
    cfg_root = plt_cfg_root.resolve()
    scope_root = meta_path.parent.resolve()
    try:
        scope_root.relative_to(cfg_root)
    except ValueError as exc:
        raise RuntimeError(f"Scope metadata escapes plt cfg root: {meta_path}") from exc

    scope_rel = scope_root.relative_to(cfg_root).as_posix()
    scope_id = "/" + scope_rel if scope_rel != "." else "/"

    for legacy in ("scope_identity", "identity_selectors"):
        if legacy in meta_cfg:
            raise RuntimeError(
                f"scope {SCOPE_META_FILENAME} must use selectors.match/selectors.in, not {legacy}: {meta_path}"
            )

    selectors = meta_cfg.get("selectors") or {}
    if not selector_matches(selectors, execution_context, label=str(meta_path), structured_only=True):
        logging.info("Skipping inactive cfg scope %s for execution context %s", meta_path, execution_context)
        return None

    nested = find_nested_cfg_meta(scope_root, exclude=meta_path)
    if nested is not None:
        raise RuntimeError(f"❌ nested cfg metadata is not allowed under scope {scope_id}: {nested}")

    if "target_path" not in meta_cfg:
        raise RuntimeError(f"target_path is required in scope {SCOPE_META_FILENAME}: {meta_path}")
    target_path = normalize_cfg_absolute_path(
        meta_cfg["target_path"],
        label=f"target_path in {meta_path}",
        allow_root=False,
    )

    raw_imports = meta_cfg.get("imports") or []
    if not isinstance(raw_imports, list):
        raise RuntimeError(f"imports must be a list: {meta_path}")

    source_dirs: list[str] = []
    seen_imports: set[Path] = set()
    for raw_import in raw_imports:
        import_path = normalize_cfg_absolute_path(
            raw_import,
            label=f"import path in {meta_path}",
            allow_root=False,
        )
        src = cfg_abs_path_to_dir(cfg_root, import_path, label=f"import path in {meta_path}")
        if src in seen_imports:
            raise RuntimeError(f"Duplicate import path in {meta_path}: {import_path}")
        if not src.exists():
            raise RuntimeError(f"Import path not found: {src}")
        if not src.is_dir():
            raise RuntimeError(f"Import path must be a directory: {src}")
        if not any(p.is_file() and ".git" not in p.relative_to(src).parts for p in src.rglob("*.yaml")):
            raise RuntimeError(f"Import path must contain at least one yaml cfg file: {src} ({meta_path})")
        validate_no_cfg_meta_inside_data_dir(src, import_path=import_path, meta_path=meta_path)

        seen_imports.add(src)
        source_dirs.append(str(src))

    if scope_root in seen_imports:
        raise RuntimeError(f"Scope imports itself in {meta_path}: {scope_id}")

    source_dirs.append(str(scope_root))
    return {
        "meta_path": meta_path,
        "scope_root": scope_root,
        "scope_path": scope_id,
        "scope_id": scope_id,
        "target_path": target_path,
        "selectors": selectors,
        "source_dirs": source_dirs,
    }


def discover_active_cfg_scopes(
    plt_cfg_root: Path,
    *,
    scope_params: dict[str, str],
    execution_context: dict[str, object] | None = None,
) -> list[dict]:
    """Discover active cfg merge scopes from type: scope metadata."""
    cfg_root = plt_cfg_root.resolve()
    runtime_context = execution_context or execution_context_from_scope_params(scope_params)
    active_scopes: list[dict] = []

    for meta_path in discover_cfg_meta_paths(cfg_root):
        meta_cfg = load_cfg_meta(meta_path)
        if meta_cfg["type"] == "overlay":
            continue

        scope = load_scope_candidate(cfg_root, meta_path, meta_cfg, runtime_context)
        if scope is None:
            continue
        active_scopes.append(scope)

    if not active_scopes:
        raise RuntimeError(f"No active cfg scopes found under: {cfg_root}")

    validate_scope_composition(active_scopes, load_scope_composition(cfg_root))

    logging.info(
        "Active cfg scopes: %s",
        [f"{scope['scope_id']} -> {scope['target_path']}" for scope in active_scopes],
    )
    return active_scopes


def normalize_overlay_name(raw_value, *, label: str) -> str:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise RuntimeError(f"{label} must be a non-empty string")
    value = raw_value.strip()
    if "/" in value or "\\" in value:
        raise RuntimeError(f"{label} must be a metadata name, not a path: {value}")
    if value in (".", ".."):
        raise RuntimeError(f"{label} is invalid: {value}")
    return value


def validate_overlay_data_tree(overlay_root: Path, *, meta_path: Path) -> None:
    """Reject overlay payloads that can change cfg topology or escape by symlink."""
    root_resolved = overlay_root.resolve()
    for path in sorted(root_resolved.rglob("*")):
        rel = path.relative_to(root_resolved)
        if ".git" in rel.parts:
            continue
        if path.is_symlink():
            raise RuntimeError(f"Overlay data must not contain symlinks: {path}")
        if path.name == SCOPE_META_FILENAME and path.resolve() != meta_path.resolve():
            raise RuntimeError(
                f"Overlay data must not contain nested {SCOPE_META_FILENAME}: {path}"
            )
        if path.name == PLT_GUARDRAILS_FILENAME:
            raise RuntimeError(f"Overlay data must not contain {PLT_GUARDRAILS_FILENAME}: {path}")


def load_overlay_candidate(
    plt_cfg_root: Path,
    meta_path: Path,
    meta_cfg: dict,
    execution_context: dict[str, object],
) -> dict:
    """Load one overlay metadata file."""
    cfg_root = plt_cfg_root.resolve()
    overlay_root = meta_path.parent.resolve()
    try:
        overlay_root.relative_to(cfg_root)
    except ValueError as exc:
        raise RuntimeError(f"Overlay metadata escapes plt cfg root: {meta_path}") from exc

    overlay_name = normalize_overlay_name(
        meta_cfg.get("name"),
        label=f"overlay name in {meta_path}",
    )
    selectors = meta_cfg.get("selectors") or {}
    matches = selector_matches(selectors, execution_context, label=str(meta_path))
    validate_overlay_data_tree(overlay_root, meta_path=meta_path)

    return {
        "name": overlay_name,
        "root": overlay_root,
        "meta_path": meta_path,
        "selectors": selectors,
        "matches": matches,
    }


def discover_overlay_candidates(
    plt_cfg_root: Path,
    *,
    execution_context: dict[str, object],
) -> dict[str, dict]:
    """Discover all type: overlay metadata entries by unique overlay name."""
    cfg_root = plt_cfg_root.resolve()
    candidates: dict[str, dict] = {}

    for meta_path in discover_cfg_meta_paths(cfg_root):
        meta_cfg = load_cfg_meta(meta_path)
        if meta_cfg["type"] == "scope":
            continue

        overlay = load_overlay_candidate(cfg_root, meta_path, meta_cfg, execution_context)
        previous = candidates.get(overlay["name"])
        if previous is not None:
            raise RuntimeError(
                f"Duplicate plt overlay name {overlay['name']!r}: {previous['meta_path']} and {meta_path}"
            )
        candidates[overlay["name"]] = overlay

    return candidates


def copy_cfg_root_without_overlay_catalog(plt_cfg_root: Path, dest_root: Path) -> None:
    """Copy cfg source to a temp root, excluding git metadata and overlay catalog."""
    cfg_root = plt_cfg_root.resolve()

    def ignore(src_dir: str, names: list[str]) -> set[str]:
        ignored: set[str] = set()
        src_path = Path(src_dir).resolve()
        if ".git" in names:
            ignored.add(".git")
        if src_path == cfg_root and "_overlays" in names:
            ignored.add("_overlays")
        return ignored

    shutil.copytree(cfg_root, dest_root, ignore=ignore)


def apply_selected_overlays_to_cfg_root(
    plt_cfg_root: Path,
    effective_cfg_root: Path,
    plt_overlays: list[str],
    *,
    execution_context: dict[str, object],
) -> None:
    """Apply selected overlay data to a temporary cfg root before scope merge."""
    if not plt_overlays:
        return

    duplicates = [item for item, count in collections.Counter(plt_overlays).items() if count > 1]
    if duplicates:
        raise RuntimeError(f"plt overlays must be unique; duplicates: {', '.join(sorted(duplicates))}")

    candidates = discover_overlay_candidates(plt_cfg_root, execution_context=execution_context)
    for overlay_name in plt_overlays:
        overlay = candidates.get(overlay_name)
        if overlay is None:
            available = ", ".join(sorted(candidates)) or "none"
            raise RuntimeError(
                f"Unknown plt overlay {overlay_name!r}; available overlays: {available}"
            )
        if not overlay["matches"]:
            raise RuntimeError(
                f"plt overlay {overlay_name!r} is not allowed for this execution context; "
                f"selectors={overlay['selectors']}"
            )

        logging.info("Applying plt overlay %s from %s", overlay_name, overlay["root"])
        merge_config_dirs(
            source_dirs=[str(overlay["root"])],
            dest_dir=str(effective_cfg_root),
            clear_dest=False,
            skip_filenames=SCOPE_META_SKIP_FILENAMES,
        )

def merge_plt_cfg_dirs(
    plt_cfg_root: Path,
    plt_merged_dir: Path,
    ctl_profile: str,
    plt_overlays: list[str] | None = None,
    scope_params: dict[str, str] | None = None,
    *,
    execution_context: dict[str, object] | None = None,
    source_log_roots: tuple[Path, ...] | None = None,
    dest_log_roots: tuple[Path, ...] | None = None,
    merged_files: dict[str, list[str]] | None = None,
    required_target_paths: set[str] | None = None,
) -> dict[str, list[str]]:
    """Build scoped merged cfg trees from typed __meta__.yaml metadata.

    Scope and overlay activation both use the uniform selectors.match/selectors.in
    execution-context selector model.
    With `required_target_paths` set, only the scopes serving those target
    paths merge (selective merge: a run composes only the cfg its stages
    consume); None = every active scope."""
    if plt_merged_dir.exists():
        shutil.rmtree(plt_merged_dir)
    os.makedirs(plt_merged_dir, exist_ok=True)

    if dest_log_roots is None:
        dest_log_roots = (plt_merged_dir.resolve(),)
    if merged_files is None:
        merged_files = {}

    selected_overlays = plt_overlays or []
    runtime_selectors = scope_params or {}
    composition_files = set(SCOPE_META_SKIP_FILENAMES)

    def merge_scopes(effective_cfg_root: Path, effective_source_log_roots: tuple[Path, ...]) -> None:
        active_scopes = discover_active_cfg_scopes(
            effective_cfg_root,
            scope_params=runtime_selectors,
            execution_context=execution_context,
        )
        selected_scopes: list[dict] = []
        for scope in active_scopes:
            target_path = scope["target_path"]
            if required_target_paths is not None and target_path not in required_target_paths:
                logging.info(
                    "Skipping cfg scope %s -> %s (not consumed by this run's stages)",
                    scope["scope_path"],
                    target_path,
                )
                continue
            selected_scopes.append(scope)

        scopes_by_target: dict[str, list[dict]] = collections.defaultdict(list)
        for scope in selected_scopes:
            scopes_by_target[scope["target_path"]].append(scope)
        for target_path, scopes in scopes_by_target.items():
            validate_cross_scope_leaf_conflicts(
                scopes,
                target_path=target_path,
                skip_filenames=composition_files,
            )

        merged_target_paths: set[str] = set()

        for scope in selected_scopes:
            target_path = scope["target_path"]
            target_rel = target_path.lstrip("/")
            target_dest = (plt_merged_dir / target_rel).resolve()
            try:
                target_dest.relative_to(plt_merged_dir.resolve())
            except ValueError as exc:
                raise RuntimeError(f"Scope target_path escapes merged cfg dir: {target_path}") from exc

            logging.info(
                "Merging cfg scope %s to %s",
                scope["scope_path"],
                target_dest,
            )
            merge_config_dirs(
                source_dirs=scope["source_dirs"],
                dest_dir=str(target_dest),
                clear_dest=target_path not in merged_target_paths,
                source_log_roots=effective_source_log_roots,
                dest_log_roots=dest_log_roots,
                merged_files=merged_files,
                skip_filenames=composition_files,
            )
            merged_target_paths.add(target_path)

    if selected_overlays:
        with tempfile.TemporaryDirectory(prefix="atlas-plt-cfg-") as tmp_dir:
            effective_cfg_root = Path(tmp_dir) / "source"
            copy_cfg_root_without_overlay_catalog(plt_cfg_root, effective_cfg_root)
            if execution_context is None:
                raise RuntimeError("❌ plt overlays require the execution context for selector gating")
            apply_selected_overlays_to_cfg_root(
                plt_cfg_root,
                effective_cfg_root,
                selected_overlays,
                execution_context=execution_context,
            )
            effective_source_log_roots = source_log_roots or (
                effective_cfg_root.resolve(),
                plt_cfg_root.resolve(),
            )
            merge_scopes(effective_cfg_root, effective_source_log_roots)
    else:
        effective_source_log_roots = source_log_roots or (plt_cfg_root.resolve(),)
        merge_scopes(plt_cfg_root, effective_source_log_roots)

    return merged_files

def prepare_pipeline_cfg(
    plt_cfg_root: Path,
    workflow_cfg: dict,
    inventory_cfg: dict,
    plt_merged_dir: Path,
    artifacts_dir: Path,
    ctl_profile: str,
    plt_overlays: list[str],
    scope_params: dict[str, str] | None = None,
    execution_context: dict[str, object] | None = None,
    stage_repo_key: str = "repo_url",
    require_stage_ref: bool = True,
    require_commit_refs: bool = False,
    refs: dict | None = None,
) -> tuple[dict, Path]:
    """
    Merge config dirs, build active stages, and write pipeline_run_cfg.

    Returns:
        tuple: (active_stages, pipeline_run_cfg_path)
    """
    source_log_roots = (plt_cfg_root.resolve(),)
    dest_log_roots = (plt_merged_dir.parent.parent.resolve(),)

    # Resolve active stages first (needs no plt cfg), so the merge composes only
    # the scopes this run's stages consume (selective merge by cfg_root).
    active_stages = build_active_stages(
        workflow_cfg,
        inventory_cfg,
        repo_key=stage_repo_key,
        require_branch_or_commit=require_stage_ref,
        refs=refs,
        execution_context=execution_context,
        require_commit_refs=require_commit_refs,
    )

    merged_files = merge_plt_cfg_dirs(
        plt_cfg_root=plt_cfg_root,
        plt_merged_dir=plt_merged_dir,
        ctl_profile=ctl_profile,
        plt_overlays=plt_overlays,
        scope_params=scope_params,
        execution_context=execution_context,
        source_log_roots=source_log_roots,
        dest_log_roots=dest_log_roots,
        required_target_paths=required_target_paths_for_stages(active_stages),
    )

    write_stage_flow_artifact(
        artifacts_dir / "resolved_stages_flow.yaml",
        workflow_cfg.get("meta"),
        active_stages,
    )

    # create and write pipeline_run_cfg
    pipeline_run_cfg = {
        "meta": workflow_cfg.get("meta"),
        "stages": active_stages
    }
    pipeline_run_cfg_path = artifacts_dir / "pipeline_run_cfg.yaml"
    with pipeline_run_cfg_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(pipeline_run_cfg, f, sort_keys=False)

    return active_stages, pipeline_run_cfg_path


def write_stage_flow_artifact(path: Path, workflow_meta: dict | None, active_stages: dict) -> None:
    """Write a compact ordered stage-flow artifact."""
    stage_flow = {
        "meta": workflow_meta,
        "stages": [
            {
                "id": stage_id,
                "target": stage.get("target"),
                "source": stage["source"],
                "workflow": stage["workflow"],
                "execution_identity_key": stage.get("execution_identity_key"),
                "branch": stage.get("branch"),
                "commit": stage.get("commit"),
            }
            for stage_id, stage in active_stages.items()
        ],
    }
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(stage_flow, f, sort_keys=False)


def write_target_stage_flow_artifact(
    ctl_cfg_root: Path,
    artifacts_dir: Path,
    *,
    ctl_profile: str,
    execution_context: dict[str, object],
    inventory_name: str,
    workflow_name: str | None,
    ctl_variants: list[str],
    plt_overlays: list[str],
    stage_repo_key: str,
    require_stage_ref: bool,
    require_commit_refs: bool,
    refs: dict | None,
) -> None:
    """For plan runs, write the matching create-flow preview artifact."""
    if inventory_name != "plan" or not workflow_name:
        return

    target_inventory_name = "provision"
    try:
        target_workflow_cfg = load_workflow_cfg(
            ctl_cfg_root,
            ctl_profile,
            target_inventory_name,
            workflow_name,
            execution_context,
        )
        target_inventory_cfg = load_inventory_cfg(ctl_cfg_root, target_inventory_name)
        target_workflow_cfg = apply_ctl_variants_to_workflow_cfg(
            ctl_cfg_root,
            target_workflow_cfg,
            target_inventory_cfg,
            execution_context=execution_context,
            inventory_name=target_inventory_name,
            workflow_name=workflow_name,
            ctl_variants=ctl_variants,
        )
        validate_workflow_target_selectors(target_workflow_cfg, target_inventory_cfg, execution_context)
        target_active_stages = build_active_stages(
            target_workflow_cfg,
            target_inventory_cfg,
            repo_key=stage_repo_key,
            require_branch_or_commit=require_stage_ref,
            refs=refs,
            execution_context=execution_context,
            require_commit_refs=require_commit_refs,
        )
    except Exception as exc:
        logging.warning(
            "Skipping target_stages_flow.yaml generation for plan/%s: %s",
            workflow_name,
            exc,
        )
        return

    write_stage_flow_artifact(
        artifacts_dir / "target_stages_flow.yaml",
        target_workflow_cfg.get("meta"),
        target_active_stages,
    )


def resolve_ctl_structure(value, execution_context: dict[str, object], *, label: str = "ctl cfg"):
    """Deep-resolve every ${execution_context.<ns>.<key>} placeholder in a ctl
    cfg structure, leaving all other leaves untouched. Used to snapshot the ctl
    cfg that drove the run with its vars filled in (e.g. ref_key env/${…} →
    env/dev)."""
    if isinstance(value, dict):
        return {k: resolve_ctl_structure(v, execution_context, label=f"{label}.{k}") for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_ctl_structure(v, execution_context, label=f"{label}[{i}]") for i, v in enumerate(value)]
    if isinstance(value, str) and "${" in value:
        return resolve_runtime_scalar(value, execution_context, label=label)
    return value


def write_ctl_cfg_snapshot(
    run_dir: Path,
    *,
    ctl_profile: str,
    ctl_profile_policy_cfg: dict,
    inventory_name: str,
    workflow_cfg: dict,
    inventory_cfg: dict,
    active_stages: dict,
    refs: dict,
    execution_context: dict[str, object],
) -> Path:
    """Write a resolved snapshot of the ctl cfg that drove the run to
    run_dir/cfg/ctl/, so the run is self-describing next to cfg/plt/. Vars are
    resolved against the execution context; active_stages is already resolved."""
    ctl_dir = run_dir / "cfg" / "ctl"
    if ctl_dir.exists():
        shutil.rmtree(ctl_dir)
    ctl_dir.mkdir(parents=True)
    write_yaml_file(ctl_dir / "profile.yaml", {"ctl_profile": ctl_profile, "policy": ctl_profile_policy_cfg})
    write_yaml_file(ctl_dir / "workflow.yaml", resolve_ctl_structure(workflow_cfg, execution_context, label="workflow"))
    write_yaml_file(
        ctl_dir / "inventory.yaml",
        resolve_ctl_structure(inventory_cfg, execution_context, label=f"inventory.{inventory_name}"),
    )
    write_yaml_file(ctl_dir / "active_stages.yaml", active_stages)
    write_yaml_file(ctl_dir / "refs.yaml", resolve_ctl_structure(refs, execution_context, label="refs"))
    logging.info("Wrote resolved ctl cfg snapshot: %s", ctl_dir)
    return ctl_dir


def write_git_metas(ctl_cfg_root: Path, plt_cfg_root: Path, artifacts_dir: Path) -> None:
    """Write all git meta files to artifacts directory."""
    # ctl_cfg_git_meta
    write_git_meta_to_file(
        git_dir=ctl_cfg_root,
        dest_dir=artifacts_dir,
        filename="piepeline_orchestrator_cfg_git_meta.yaml",
        generator=SERVICE_ID
    )

    # orchestrator_git_meta
    write_git_meta_to_file(
        git_dir=os.getcwd(),
        dest_dir=artifacts_dir,
        filename="piepeline_orchestrator_git_meta.yaml",
        generator=SERVICE_ID
    )

    # plt_cfg_git_meta
    write_git_meta_to_file(
        git_dir=plt_cfg_root,
        dest_dir=artifacts_dir,
        filename="plt_cfg_git_meta.yaml",
        generator=SERVICE_ID
    )


# ---------------------------------------------------------------------------
# Ctl-state sync: mirror the local ctl-state tree to the domain bucket.
# S3 bucket. Local-first mechanics, remote system of record after final push.
# ---------------------------------------------------------------------------

def load_ctl_state_backends_cfg(ctl_cfg_root: Path) -> dict | None:
    """Load the optional ctl-state backend registry.

    Canonical schema is ``ctl_state_backends``:
    {domain: {provider, backend_type, bucket_name, bucket_region,
    [execution_identity_key]}}. ``ctl_state_buckets`` is accepted as a
    temporary compatibility alias for older cfg and is normalized to AWS/S3.
    """
    merged: dict = {}
    seen_sources: dict[str, Path] = {}
    sections = list(collect_top_level_sections(ctl_cfg_root, "ctl_state_backends"))
    legacy_sections = list(collect_top_level_sections(ctl_cfg_root, "ctl_state_buckets"))
    if sections and legacy_sections:
        raise RuntimeError("❌ ctl cfg must not define both ctl_state_backends and legacy ctl_state_buckets")
    section_name = "ctl_state_backends" if sections else "ctl_state_buckets"
    entries = sections if sections else legacy_sections
    for path, section in entries:
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ {section_name} must be a mapping: {path}")
        for domain, entry in section.items():
            # Domains are consumer-defined vocabulary (the engine stays cfg-shape
            # agnostic): any non-empty snake_case key is a valid state domain.
            if not isinstance(domain, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", domain):
                raise RuntimeError(f"❌ {section_name} domain must be a snake_case key: {domain!r} in {path}")
            if domain in merged:
                raise RuntimeError(f"❌ duplicate {section_name} domain {domain!r}: {path} (first: {seen_sources[domain]})")
            if not isinstance(entry, dict):
                raise RuntimeError(f"❌ {section_name}.{domain} must be a mapping: {path}")
            allowed = {"provider", "backend_type", "bucket_name", "bucket_region", "execution_identity_key"}
            unknown = set(entry) - allowed
            if unknown:
                raise RuntimeError(f"❌ {section_name}.{domain} has unsupported keys {sorted(unknown)}: {path}")
            provider = entry.get("provider", "aws" if section_name == "ctl_state_buckets" else None)
            backend_type = entry.get("backend_type", "s3" if section_name == "ctl_state_buckets" else None)
            for field, value in (("provider", provider), ("backend_type", backend_type)):
                if not isinstance(value, str) or not value.strip():
                    raise RuntimeError(f"❌ {section_name}.{domain}.{field} must be a non-empty string: {path}")
            for field in ("bucket_name", "bucket_region"):
                if not isinstance(entry.get(field), str) or not entry[field].strip():
                    raise RuntimeError(f"❌ {section_name}.{domain}.{field} must be a non-empty string: {path}")
            if provider.strip() != "aws" or backend_type.strip() != "s3":
                raise RuntimeError(
                    f"❌ {section_name}.{domain} provider/backend_type {provider!r}/{backend_type!r} is not supported yet; "
                    "available ctl-state backend: aws/s3"
                )
            resolved = {
                "provider": provider.strip(),
                "backend_type": backend_type.strip(),
                "bucket_name": entry["bucket_name"].strip(),
                "bucket_region": entry["bucket_region"].strip(),
            }
            identity_key = entry.get("execution_identity_key")
            if identity_key is not None:
                if not isinstance(identity_key, str) or not identity_key.strip():
                    raise RuntimeError(
                        f"❌ {section_name}.{domain}.execution_identity_key must be a non-empty string: {path}"
                    )
                resolved["execution_identity_key"] = identity_key.strip()
            merged[domain] = resolved
            seen_sources[domain] = path
    return merged or None

def ctl_allows_skip_ctl_state_backend_sync(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    """Profile policy bool: may a run skip the ctl-state backend sync?

    Absent = false (strict). This only *permits* the per-run
    --skip-ctl-state-backend-sync decision; skipping additionally requires that no
    active target declares a ctl_state_backend_key (the sync-skip triad, mirroring
    the profile-only identity fallback)."""
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    value = policy.get("allow_skip_ctl_state_backend_sync", False)
    if not isinstance(value, bool):
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} allow_skip_ctl_state_backend_sync must be a bool: {value!r}"
        )
    return value


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


_CTL_STATE_SYNCER: CtlStateSyncer | None = None
_CTL_STATE_SYNC_NOTE: dict[str, str] = {"mode": "disabled"}


def configure_ctl_state_sync(
    ctl_cfg_root: Path,
    ctl_profile: str,
    domain: str | None,
    execution_context: dict[str, object],
    run_dir: Path,
    *,
    skip_ctl_state_backend_sync: bool,
    provisions_ctl_state_bucket: bool = False,
    profile_only_aws_profile: str | None = None,
    execution_identities: dict | None = None,
    aws_access_contexts: dict | None = None,
    aws_implementation_key: str = "local",
) -> dict[str, str] | None:
    """Resolve the run's ctl-state bucket (by domain) and arm the global syncer.

    Routes by the run's single state `domain` (from resolve_run_domain): the
    bucket name+region come straight from the ctl_state_backends registry — no
    execution-context involvement. Existence is re-checked at each sync point, so
    a bucket-creating run mirrors itself at finalization.

    Skip semantics — a three-condition triad, mirroring the profile-only identity
    fallback: sync is skipped only when the profile allows it
    (allow_skip_ctl_state_backend_sync: true), no active target declares a
    ctl_state_backend_key (domain is None), AND --skip-ctl-state-backend-sync is
    passed. A declared domain always syncs (the arg is rejected); a missing
    domain never syncs silently (the skip must be explicit).
    """
    global _CTL_STATE_SYNCER, _CTL_STATE_SYNC_NOTE
    _CTL_STATE_SYNCER = None
    _CTL_STATE_SYNC_NOTE = {"mode": "disabled"}

    buckets = load_ctl_state_backends_cfg(ctl_cfg_root)
    if buckets is None and domain is None:
        # Consumer has no ctl-state feature at all: nothing to sync or skip.
        if skip_ctl_state_backend_sync:
            logging.info("--skip-ctl-state-backend-sync has no effect: no ctl_state_backends registry defined")
        return None
    if domain is not None and buckets is None:
        raise RuntimeError(
            f"❌ targets declare ctl_state_backend_key {domain!r} but no ctl_state_backends registry is defined"
        )

    allow_skip = ctl_allows_skip_ctl_state_backend_sync(ctl_cfg_root, ctl_profile)
    if skip_ctl_state_backend_sync and not allow_skip:
        raise RuntimeError(
            "❌ --skip-ctl-state-backend-sync is not allowed: ctl profile sets "
            "allow_skip_ctl_state_backend_sync: false"
        )

    if domain is None:
        # No active target declares a state domain (keys commented, dev cfg).
        if not skip_ctl_state_backend_sync:
            raise RuntimeError(
                "❌ no active target declares ctl_state_backend_key; pass "
                "--skip-ctl-state-backend-sync to run without ctl-state sync "
                "(or restore the keys)"
            )
        _CTL_STATE_SYNC_NOTE = {"mode": "skipped", "reason": "no_domain"}
        return None

    if skip_ctl_state_backend_sync:
        raise RuntimeError(
            f"❌ --skip-ctl-state-backend-sync is not allowed: active targets declare "
            f"ctl_state_backend_key {domain!r} — a declared state domain always syncs"
        )

    entry = buckets[domain]  # domain existence validated by resolve_run_domain
    bucket_name = str(
        resolve_runtime_scalar(
            entry["bucket_name"], execution_context, label=f"ctl_state_backends.{domain}.bucket_name"
        )
    )
    bucket_region = entry["bucket_region"]
    result = {"domain": domain, "bucket_name": bucket_name, "bucket_region": bucket_region}

    # Resolve the writer's AWS profile. The registry entry declares an
    # execution_identity_key or omits it — mirroring stages: a declared identity
    # is resolved (and --aws-profile never overrides it); an omitted one falls
    # back to the run's --aws-profile, but only under a profile that permits
    # profile-only access (e.g. local dev).
    identity_key = entry.get("execution_identity_key")
    if identity_key:
        identities = execution_identities if execution_identities is not None else load_execution_identities_cfg(ctl_cfg_root)
        contexts = aws_access_contexts if aws_access_contexts is not None else load_aws_access_contexts_cfg(ctl_cfg_root)
        account_registry = load_aws_account_registry_cfg(ctl_cfg_root)
        resolved = resolve_stage_aws_access(
            {"execution_identity_key": identity_key},
            identities,
            contexts,
            execution_context=execution_context,
            implementation_key=aws_implementation_key,
            account_registry=account_registry,
        )
        if not resolved or not resolved.get("profile_name"):
            raise RuntimeError(
                f"❌ ctl-state writer identity {identity_key!r} did not resolve to an AWS profile"
            )
        writer_profile = resolved["profile_name"]
    elif ctl_allows_aws_profile_only(ctl_cfg_root, ctl_profile) and profile_only_aws_profile:
        writer_profile = profile_only_aws_profile.strip()
    else:
        raise RuntimeError(
            f"❌ ctl_state_backends.{domain} declares no execution_identity_key; "
            f"provide --aws-profile under a profile that allows profile-only access, "
            f"or add an execution_identity_key"
        )

    results_root_value = load_run_metadata(run_dir).get("ctl_state_local_root")
    if not isinstance(results_root_value, str) or not results_root_value:
        raise RuntimeError("❌ run metadata is missing ctl_state_local_root; cannot sync ctl-state")

    # A declared domain always syncs: the syncer is strict and a missing bucket is
    # a hard error unless this run provisions it. Existence is re-checked at every
    # sync point, so a bootstrap run mirrors itself once the bucket exists.
    syncer = CtlStateSyncer(
        Path(results_root_value),
        bucket_name,
        bucket_region,
        writer_profile,
        required=True,
    )
    _CTL_STATE_SYNCER = syncer
    bucket_ready = syncer.ensure_ready("run started")
    if not bucket_ready and not provisions_ctl_state_bucket:
        raise RuntimeError(
            f"❌ ctl-state bucket s3://{bucket_name} ({bucket_region}) not found; "
            f"provision it via the ctl-state bootstrap target first"
        )
    syncer.push("run started")
    _CTL_STATE_SYNC_NOTE = syncer.summary()
    return result


def ctl_state_push(reason: str) -> None:
    if _CTL_STATE_SYNCER is not None:
        _CTL_STATE_SYNCER.push(reason)


def ctl_state_remove_slots(run_dir: Path, states: tuple[str, ...]) -> None:
    """Remove stale remote state-slot prefixes after a local slot transition."""
    if _CTL_STATE_SYNCER is None:
        return
    result_dir = ctl_state_dir_from_run_dir(run_dir)
    rel_result = result_dir.resolve().relative_to(_CTL_STATE_SYNCER.results_root).as_posix()
    for state in states:
        _CTL_STATE_SYNCER.remove_prefix(f"{rel_result}/{state}")


def ctl_state_sync_summary() -> dict[str, str]:
    if _CTL_STATE_SYNCER is not None:
        return _CTL_STATE_SYNCER.summary()
    return dict(_CTL_STATE_SYNC_NOTE)


def _stage_utils_module(name: str):
    """Import a stage_utils/ctl python module by file path (shared primitives:
    Resolver, merge_values, cfg-entry refs). The module stays self-contained in
    stage_utils because it also executes inside stage containers."""
    import importlib.util
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, source_stage_utils_dir() / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def render_scope_tree(scope_dir: Path, dest_dir: Path, env_ctx: dict) -> None:
    """Render one scope: merge all scope YAML for lookups, interpolate,
    normalize cfg-entry refs whole-scope, write back per-file YAML, copy
    non-YAML verbatim. Engine logic (folded from the former stage-side
    render_cfg.py)."""
    brc = _stage_utils_module("build_runtime_cfg")
    yaml_files = sorted(p for p in scope_dir.rglob("*.yaml") if p.is_file())
    scope_merged: dict = {}
    for path in yaml_files:
        doc = brc.load_yaml_mapping(path)
        if EXECUTION_CONTEXT_ROOT in doc:
            raise RuntimeError(
                f"❌ plt payload must not define reserved top-level key "
                f"{EXECUTION_CONTEXT_ROOT!r}: {path}"
            )
        scope_merged = brc.merge_values(scope_merged, doc)

    resolver = brc.Resolver(scope_merged, env_ctx)
    scope_resolved: dict = {}
    for key in scope_merged:
        value = resolver.lookup(key)
        if value is brc.OMIT:
            continue
        scope_resolved[key] = value
    scope_resolved = brc.resolve_cfg_entry_refs(scope_resolved)

    for path in sorted(p for p in scope_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(scope_dir)
        dest = dest_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix != ".yaml":
            shutil.copy2(path, dest)
            continue
        doc = brc.load_yaml_mapping(path)
        rendered = resolver.resolve_value(doc)
        rendered = brc.resolve_cfg_entry_refs(rendered, lookup_root=scope_resolved)
        dest.write_text(
            yaml.safe_dump(rendered, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )


def render_plt_cfg(plt_merged_dir: Path, run_dir: Path, execution_context: dict[str, object]) -> Path:
    """Render merged/ into rendered/ (whole-scope). In-process engine step —
    no subprocess, no stage costume."""
    plt_rendered_dir = run_dir / "cfg" / "plt" / "rendered"
    if plt_rendered_dir.exists():
        shutil.rmtree(plt_rendered_dir)
    plt_rendered_dir.mkdir(parents=True)
    env_ctx = dict(execution_context)
    for entry in sorted(plt_merged_dir.iterdir()):
        if entry.is_dir():
            render_scope_tree(entry, plt_rendered_dir / entry.name, env_ctx)
        else:
            shutil.copy2(entry, plt_rendered_dir / entry.name)
    logging.info("Rendered plt cfg: %s", plt_rendered_dir)
    return plt_rendered_dir


def run_cfg_distribution(pipeline_run_cfg_path: Path, plt_rendered_dir: Path, run_dir: Path) -> Path:
    """Distribute stage input views from the rendered tree (in-process engine
    step — folded from the former dockerized prepare/cfg stage).

    Single derivation chain: rendered/ derives from merged/; each
    stages/<stage>/input/ view is selected from rendered/ only.
    """
    plt_stages_dir_path = run_dir / "cfg" / "plt" / "stages"
    cfg = load_yaml(pipeline_run_cfg_path) or {}
    stages = cfg.get("stages") or {}
    if not isinstance(stages, dict):
        raise RuntimeError("pipeline_run_cfg.yaml stages must be a mapping")
    plt_stages_dir_path.mkdir(parents=True, exist_ok=True)

    for stage_name, stage_cfg in stages.items():
        if not isinstance(stage_cfg, dict):
            raise RuntimeError(f"Stage {stage_name!r} config must be a mapping")
        cfg_files = stage_cfg.get("cfg_files") or []
        if not cfg_files:
            continue
        if not isinstance(cfg_files, list):
            raise RuntimeError(f"Stage {stage_name!r} cfg_files must be a list")

        cfg_root = normalize_cfg_absolute_path(
            stage_cfg.get("cfg_root", "/"), label=f"stage {stage_name!r} cfg_root", allow_root=False
        )
        if len([part for part in cfg_root.split("/") if part]) != 1:
            raise RuntimeError(
                f"Stage {stage_name!r} cfg_root must be exactly one top-level scope "
                f"(a single path segment), not {cfg_root!r} — a stage may not span scopes"
            )
        scope_root = cfg_abs_path_to_dir(plt_rendered_dir, cfg_root, label=f"stage {stage_name!r} cfg_root")
        if not scope_root.is_dir():
            logging.info("[WARN] cfg root %r not found for stage %r: %s", cfg_root, stage_name, scope_root)
            continue

        stage_input_dir = plt_stages_dir_path / stage_name / "input"
        stage_input_dir.mkdir(parents=True, exist_ok=True)

        for pattern in cfg_files:
            if not isinstance(pattern, str) or not pattern.strip():
                raise RuntimeError(f"Stage {stage_name!r} cfg_files entries must be non-empty strings")
            pattern_norm = pattern.strip().lstrip("/")
            if pattern_norm == "*":
                sources = [p for p in scope_root.iterdir()]
            elif pattern_norm.endswith("/*"):
                src_dir = cfg_abs_path_to_dir(scope_root, "/" + pattern_norm[:-2], label=f"stage {stage_name!r} cfg_files pattern")
                if not src_dir.is_dir():
                    logging.info("[WARN] cfg dir %r not found under %s", pattern_norm, cfg_root)
                    continue
                sources = [p for p in src_dir.iterdir()]
            else:
                sources = [cfg_abs_path_to_dir(scope_root, "/" + pattern_norm, label=f"stage {stage_name!r} cfg_files entry")]

            for src in sources:
                if not src.exists():
                    logging.info("[WARN] cfg entry does not exist under %s: %s", cfg_root, src)
                    continue
                rel = src.relative_to(scope_root)
                dst = stage_input_dir / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                if src.is_dir():
                    if dst.exists():
                        shutil.rmtree(dst)
                    shutil.copytree(dst if False else src, dst)
                else:
                    shutil.copy2(src, dst)

    logging.info("Prepared stage input cfg views under %s", plt_stages_dir_path)
    return plt_stages_dir_path



def _remove_path(path: Path) -> None:
    """Remove an existing file, directory, or symlink."""
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def materialize_stage_modules(stage_id: str, stage: dict, repo_path: Path) -> None:
    """Populate stage-local child modules before setup runs."""
    modules = stage.get("modules") or {}
    if not modules:
        return

    repo_root = repo_path.resolve()
    for module_name, module_cfg in modules.items():
        dest_path = repo_path / module_cfg["dest"]
        try:
            dest_path.relative_to(repo_path)
        except ValueError as exc:
            raise RuntimeError(
                f"Stage '{stage_id}' module '{module_name}' dest escapes the stage repo: {module_cfg['dest']}"
            ) from exc

        if dest_path.exists() or dest_path.is_symlink():
            _remove_path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if "repo_path" in module_cfg:
            module_src = Path(module_cfg["repo_path"]).expanduser()
            if not module_src.is_dir():
                raise RuntimeError(
                    f"Stage '{stage_id}' module '{module_name}' repo_path not found: {module_src}"
                )
            # Copy the local working tree snapshot so Dockerized stage runners can read it.
            shutil.copytree(module_src, dest_path, symlinks=True)
        else:
            git_clone(
                repo_url=module_cfg["repo_url"],
                branch=module_cfg["branch"],
                commit=module_cfg["commit"],
                dest=dest_path,
                token=os.getenv(module_cfg["token_type"]) if module_cfg.get("token_type") else None,
            )


def ctl_utils_root() -> Path:
    return Path(__file__).resolve().parents[2]


def source_stage_utils_dir() -> Path:
    utils_dir = ctl_utils_root() / "stage_utils"
    if not utils_dir.is_dir():
        raise RuntimeError(f"❌ stage utils source dir not found: {utils_dir}")
    return utils_dir


def materialize_stage_utils(run_dir: Path) -> Path:
    """Copy the ctl-owned stage support scripts into this run's stage_utils area.

    Rule: stage_utils/ctl holds only files consumed by stages (host wrappers,
    in-container setup, the per-stage resolver, access assert, dockerfiles).
    """
    utils_dir = run_dir / "stage_utils" / "ctl"
    if utils_dir.is_dir():
        return utils_dir
    utils_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        source_stage_utils_dir(),
        utils_dir,
        symlinks=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return utils_dir


def prepare_stage_repo(
    stage_id: str,
    stage: dict,
    run_dir: Path,
    tooling_env: dict[str, str],
    execution_identities: dict | None = None,
    aws_access_contexts: dict | None = None,
    aws_account_registry: dict[str, str] | None = None,
    execution_context: dict[str, object] | None = None,
    aws_implementation_key: str | None = None,
    allow_aws_profile_only: bool = False,
    profile_only_aws_profile: str | None = None,
) -> tuple[Path, dict[str, str]]:
    """Clone/copy a stage repo, materialize child modules, and prepare its execution env."""
    repo_path = run_dir / "stages_source" / stage_id
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)

    if "repo_path" in stage:
        repo_path_value = stage["repo_path"]
        if not repo_path_value:
            raise RuntimeError(f"Stage '{stage_id}' has empty repo_path")
        repo_src = Path(repo_path_value).expanduser()
        if not repo_src.is_dir():
            raise RuntimeError(f"Stage '{stage_id}' repo_path not found: {repo_src}")
        shutil.copytree(repo_src, repo_path, symlinks=True)
    else:
        git_clone(
            repo_url=stage["repo_url"],
            branch=stage["branch"],
            commit=stage["commit"],
            dest=repo_path,
            token=os.getenv(stage["token_type"]),
        )

    materialize_stage_modules(stage_id, stage, repo_path)

    stage_env = os.environ.copy()
    stage_env.update(tooling_env)
    stage_env["ATLAS_STAGE_UTILS_DIR"] = str(materialize_stage_utils(run_dir).parent)
    if aws_access_contexts is not None:
        if (
            execution_identities is None
            or aws_account_registry is None
            or execution_context is None
            or aws_implementation_key is None
        ):
            raise RuntimeError("❌ incomplete AWS access context for stage preparation")
        configure_stage_aws_env(
            stage_id,
            stage,
            stage_env,
            execution_identities,
            aws_access_contexts,
            execution_context=execution_context,
            implementation_key=aws_implementation_key,
            account_registry=aws_account_registry,
            allow_profile_only=allow_aws_profile_only,
            profile_only_aws_profile=profile_only_aws_profile,
        )
    return repo_path, stage_env


def _repo_local_active_stages(action_manifest: dict, active_ids: list[str], repo_root: Path) -> list[dict]:
    active: list[dict] = []
    for stage_id in active_ids:
        entry = action_manifest.get(stage_id)
        if not isinstance(entry, dict):
            raise RuntimeError(f"Stage {stage_id!r} not declared in manifest")
        stage_path = entry.get("path")
        if not isinstance(stage_path, str) or not stage_path:
            raise RuntimeError(f"Stage {stage_id!r} manifest entry must define a non-empty path")

        stage_meta_path = repo_root / stage_path / "stage.yaml"
        if not stage_meta_path.is_file():
            raise RuntimeError(f"Stage metadata not found: {stage_meta_path}")
        stage_meta = load_yaml(stage_meta_path) or {}
        runtime_cfg = stage_meta.get("runtime") or {}
        if not isinstance(runtime_cfg, dict):
            raise RuntimeError(f"Stage metadata runtime must be a mapping: {stage_meta_path}")
        values_json = runtime_cfg.get("values_json", True)
        env_sh = runtime_cfg.get("env_sh", True)
        if not isinstance(values_json, bool) or not isinstance(env_sh, bool):
            raise RuntimeError(f"Stage metadata runtime flags must be booleans: {stage_meta_path}")
        cfg_files = stage_meta.get("cfg_files", [])
        if cfg_files is None:
            cfg_files = []
        if not isinstance(cfg_files, list):
            raise RuntimeError(f"Stage metadata cfg_files must be a list: {stage_meta_path}")

        active.append(
            {
                "id": stage_id,
                "path": stage_path,
                "cfg_files": cfg_files,
                "runtime": {
                    "values_json": values_json,
                    "env_sh": env_sh,
                },
                "env_vars": {
                    "inventory": {},
                    "stage": stage_meta.get("env_vars", {}),
                },
            }
        )
    return active


def get_repo_local_stages(repo_path: Path, action: str, workflow_name: str) -> tuple[list[str], list[dict]]:
    manifest_file = repo_path / ADAPTER_DIR / "manifest.yaml"
    if not manifest_file.is_file():
        raise RuntimeError(f"❌ manifest file not found: {manifest_file}")
    workflows_file = repo_path / ADAPTER_DIR / "workflows.yaml"
    if not workflows_file.is_file():
        raise RuntimeError(f"❌ workflows file not found: {workflows_file}")

    manifest = (load_yaml(manifest_file) or {}).get("manifest", {})
    workflows = (load_yaml(workflows_file) or {}).get("workflows", {})

    action_manifest = manifest.get(action)
    if not isinstance(action_manifest, dict) or not action_manifest:
        raise RuntimeError(f"manifest {manifest_file} declares no stages for action {action!r}")

    action_workflows = workflows.get(action)
    if not isinstance(action_workflows, dict) or workflow_name not in action_workflows:
        raise RuntimeError(f"workflow {action}/{workflow_name} not found in {workflows_file}")
    workflow = action_workflows[workflow_name]
    if not isinstance(workflow, dict) or "stages" not in workflow:
        raise RuntimeError(f"workflow {action}/{workflow_name} must define stages")

    active_ids: list[str] = []
    for stage_id in workflow.get("stages", []):
        if stage_id not in action_manifest:
            raise RuntimeError(f"Stage {stage_id!r} not declared in manifest for action {action!r}")
        active_ids.append(stage_id)

    return active_ids, _repo_local_active_stages(action_manifest, active_ids, repo_path)


def ensure_repo_execution_context(repo_path: Path, execution_context_path: Path) -> bool:
    repo_execution_context_path = repo_path / EXECUTION_CONTEXT_FILENAME
    if execution_context_path.resolve() == repo_execution_context_path.resolve():
        return False
    shutil.copy2(execution_context_path, repo_execution_context_path)
    return True


def resolve_force_unlock_tfstate_vars(repo_path: Path) -> tuple[str, str | None]:
    """Extract tfstate key/uri variable names from standard infra stage scripts."""
    key_var = None
    uri_var = None

    for rel_path in FORCE_UNLOCK_STAGE_SCRIPT_CANDIDATES:
        script_path = repo_path / rel_path
        if not script_path.is_file():
            continue

        content = script_path.read_text(encoding="utf-8")
        key_match = FORCE_UNLOCK_KEY_RE.search(content)
        uri_match = FORCE_UNLOCK_URI_RE.search(content)

        if key_match:
            candidate = key_match.group(1)
            if key_var and candidate != key_var:
                raise RuntimeError(
                    f"❌ Conflicting tfstate key variables found for force-unlock in '{repo_path}': "
                    f"'{key_var}' vs '{candidate}'"
                )
            key_var = candidate

        if uri_match:
            candidate = uri_match.group(1)
            if uri_var and candidate != uri_var:
                raise RuntimeError(
                    f"❌ Conflicting tfstate uri variables found for force-unlock in '{repo_path}': "
                    f"'{uri_var}' vs '{candidate}'"
                )
            uri_var = candidate

    if not key_var:
        raise RuntimeError(
            f"❌ force-unlock is not supported for stage repo '{repo_path}'. "
            "Expected one of atlas_ctl_adapter/stages/{plan,provision,destroy}/infra/src/stage.sh to call './bin/tf.sh infra init $..._tfstate_key'."
        )

    if uri_var is None and key_var.endswith("_key"):
        uri_var = f"{key_var[:-4]}_uri"

    return key_var, uri_var


def run_stages(
    active_stages: dict,
    run_dir: Path,
    plt_stages_dir_path: Path,
    execution_context_path: Path,
    inventory_name: str,
    execution_context: dict[str, object],
    run_id: str,
    tooling_refs: dict,
    use_local_tooling_cfg: bool,
    execution_identities: dict,
    aws_access_contexts: dict,
    aws_account_registry: dict[str, str],
    aws_implementation_key: str,
    allow_aws_profile_only: bool,
    profile_only_aws_profile: str | None,
) -> None:
    """Clone and run all active stages."""
    os.chdir(run_dir)
    tooling_env = build_tooling_env(tooling_refs)
    stage_runner_script = "local_dev.sh" if use_local_tooling_cfg else "local.sh"
    mutation_marked = False
    for stage_id, stage in active_stages.items():
        log_stage_banner(f"[{inventory_name}] [{stage_id}]")
        repo_path, stage_env = prepare_stage_repo(
            stage_id,
            stage,
            run_dir,
            tooling_env,
            execution_identities=execution_identities,
            aws_access_contexts=aws_access_contexts,
            aws_account_registry=aws_account_registry,
            execution_context=execution_context,
            aws_implementation_key=aws_implementation_key,
            allow_aws_profile_only=allow_aws_profile_only,
            profile_only_aws_profile=profile_only_aws_profile,
        )

        workflow_name = stage.get("workflow")
        if not isinstance(workflow_name, str) or not workflow_name:
            raise RuntimeError(f"❌ stage {stage_id!r} must define a non-empty repo-local workflow")
        origin_cfg_path = plt_stages_dir_path / stage_id / "input"
        if not origin_cfg_path.is_dir():
            raise RuntimeError(f"❌ stage input cfg dir not found for stage {stage_id!r}: {origin_cfg_path}")
        stage_cfg_dir = plt_stages_dir_path / stage_id / "resolved"
        os.makedirs(stage_cfg_dir, exist_ok=True)
        stage_artifacts_dir = run_dir / "artifacts" / "stages" / stage_id
        os.makedirs(stage_artifacts_dir, exist_ok=True)

        copied_execution_context = ensure_repo_execution_context(repo_path, execution_context_path)
        try:
            repo_stage_ids, repo_stages = get_repo_local_stages(repo_path, inventory_name, workflow_name)
            run_manifest = {
                "run_id": run_id,
                "branch": stage.get("branch"),
                "commit": stage.get("commit"),
                "action": inventory_name,
                "workflow": workflow_name,
                "active_stages": repo_stage_ids,
                "origin_cfg": str(origin_cfg_path),
                "execution_context_file": str(execution_context_path),
                "execution_context_keys": sorted(execution_context),
            }
            logging.info(json.dumps(run_manifest, indent=4))

            if inventory_name in MUTATING_ACTIONS and not mutation_marked:
                mark_mutation_started(run_dir, stage_id)
                mutation_marked = True

            for repo_stage in repo_stages:
                repo_stage_id = repo_stage["id"]
                repo_stage_path = repo_stage["path"]
                log_stage_banner(f"[{inventory_name}] [{stage_id}] [{repo_stage_id}]", ch="-")
                stage_run_cmd = [f"./{repo_stage_path}/run/{stage_runner_script}"]
                repo_stage_env = dict(stage_env)
                repo_stage_env["ATLAS_EXECUTION_CONTEXT_FILE"] = EXECUTION_CONTEXT_FILENAME
                repo_stage_env["cfg_files"] = json.dumps(repo_stage.get("cfg_files"))
                repo_stage_env["STAGE_WRITE_VALUES_JSON"] = (
                    "true" if repo_stage.get("runtime", {}).get("values_json", True) else "false"
                )
                repo_stage_env["STAGE_WRITE_ENV_SH"] = (
                    "true" if repo_stage.get("runtime", {}).get("env_sh", True) else "false"
                )
                repo_stage_env["origin_cfg_base_dir_path"] = str(origin_cfg_path)
                repo_stage_env["STAGE_CFG_DIR"] = str(stage_cfg_dir)
                repo_stage_env["STAGE_ARTIFACTS_DIR"] = str(stage_artifacts_dir)

                logging.info(" ".join(stage_run_cmd))
                run_and_log(
                    stage_run_cmd,
                    cwd=repo_path,
                    env=repo_stage_env,
                )
            ctl_state_push(f"stage {stage_id} completed")
        finally:
            repo_execution_context_path = repo_path / EXECUTION_CONTEXT_FILENAME
            if copied_execution_context and repo_execution_context_path.is_file():
                repo_execution_context_path.unlink()


def print_run_summary(run_id: str, log_file: Path) -> None:
    """Print run summary at the end."""
    print(f"Run id: {run_id}")
    print(f"Log file: {log_file}")


def run_maintenance(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    ctl_state_local_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    inventory_name: str,
    maintenance_action: str,
    stage_target: str,
    lock_id: str,
    run_id: str,
    plt_overlays: list[str],
    stage_repo_key: str,
    require_stage_ref: bool,
    use_local_tooling_cfg: bool,
    aws_implementation_key: str,
    run_dir: Path,
    artifacts_dir: Path,
    plt_merged_dir: Path,
    log_file: Path,
    profile_only_aws_profile: str | None,
    skip_ctl_state_backend_sync: bool = False,
) -> None:
    """Run a maintenance action against a single stage target."""
    if maintenance_action == "force-unlock" and force_unlock_ctl_state_lock(ctl_state_local_root, lock_id, run_dir):
        print_run_summary(run_id, log_file)
        return

    execution_context = build_execution_context(
        ctl_cfg_root,
        action=inventory_name,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
    )
    scope_params = scope_params_from_context(execution_context)
    validate_execution_context_constraints(ctl_cfg_root, execution_context)
    inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name)
    ctl_state_domain = resolve_run_domain(
        {"stages": [{"target": stage_target}]},
        inventory_cfg,
        load_ctl_state_backends_cfg(ctl_cfg_root),
    )
    configure_ctl_state_sync(
        ctl_cfg_root,
        ctl_profile,
        ctl_state_domain,
        execution_context,
        run_dir,
        skip_ctl_state_backend_sync=skip_ctl_state_backend_sync,
        profile_only_aws_profile=profile_only_aws_profile,
        aws_implementation_key=aws_implementation_key,
    )
    execution_context_path = write_execution_context_artifact(run_dir, execution_context)
    require_commit_refs = ref_policy_requires_commits(ctl_ref_policy)

    refs = load_refs_cfg(ctl_cfg_root)
    if use_local_tooling_cfg:
        tooling_refs = load_local_tooling_cfg(ctl_cfg_root)
    else:
        tooling_refs = refs.get("global") or {}
        validate_tooling_refs_have_commits(tooling_refs, ctl_ref_policy)

    logging.info(f"Selector policy validation passed: ctl_profile={ctl_profile}")

    workflow_cfg = {
        "meta": {
            "name": f"{ctl_profile}/{inventory_name}/maintenance/{maintenance_action}/{stage_target}",
            "inventory": inventory_name,
        },
        "stages": [
            {
                "id": stage_target,
                "target": stage_target,
            }
        ],
    }
    validate_workflow_target_selectors(workflow_cfg, inventory_cfg, execution_context)

    active_stages, pipeline_run_cfg_path = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
        plt_merged_dir,
        artifacts_dir,
        ctl_profile,
        plt_overlays,
        scope_params=scope_params,
        execution_context=execution_context,
        stage_repo_key=stage_repo_key,
        require_stage_ref=require_stage_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )
    record_run_target_keys(run_dir, target_keys_from_active_stages(active_stages))
    plt_rendered_dir = render_plt_cfg(plt_merged_dir, run_dir, execution_context)
    verify_guardrails(
        ctl_cfg_root, plt_cfg_root, plt_rendered_dir, execution_context, scope_params,
        required_target_paths=required_target_paths_for_stages(active_stages),
    )

    validate_stages_have_commits(active_stages, ctl_ref_policy)
    execution_identities = load_execution_identities_cfg(ctl_cfg_root)
    aws_access_contexts = load_aws_access_contexts_cfg(ctl_cfg_root)
    aws_account_registry_cfg = load_aws_account_registry_cfg(ctl_cfg_root)
    allow_aws_profile_only = ctl_allows_aws_profile_only(ctl_cfg_root, ctl_profile)
    aws_account_registry = validate_active_stage_aws_access(
        active_stages,
        execution_identities,
        aws_access_contexts,
        execution_context=execution_context,
        implementation_key=aws_implementation_key,
        account_registry=aws_account_registry_cfg,
        allow_profile_only=allow_aws_profile_only,
        profile_only_aws_profile=profile_only_aws_profile,
    )
    write_git_metas(ctl_cfg_root, plt_cfg_root, artifacts_dir)
    plt_stages_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path,
        plt_rendered_dir,
        run_dir,
    )

    os.chdir(run_dir)
    tooling_env = build_tooling_env(tooling_refs)
    if len(active_stages) != 1:
        raise RuntimeError(
            f"❌ maintenance action '{maintenance_action}' expected exactly one active stage, got: {list(active_stages)}"
        )

    stage_id, stage = next(iter(active_stages.items()))
    log_stage_banner(f"[{inventory_name}] [maintenance/{maintenance_action}/{stage_id}]")
    repo_path, stage_env = prepare_stage_repo(
        stage_id,
        stage,
        run_dir,
        tooling_env,
        execution_identities=execution_identities,
        aws_access_contexts=aws_access_contexts,
        aws_account_registry=aws_account_registry,
        execution_context=execution_context,
        aws_implementation_key=aws_implementation_key,
        allow_aws_profile_only=allow_aws_profile_only,
        profile_only_aws_profile=profile_only_aws_profile,
    )
    run_and_log(
        ["python3", str(materialize_stage_utils(run_dir) / "assert_aws_access.py")],
        cwd=repo_path,
        env=stage_env,
    )

    stage_cfg_dir = plt_stages_dir_path / stage_id / "input"
    if not stage_cfg_dir.is_dir():
        raise RuntimeError(f"❌ stage input cfg dir not found for stage '{stage_id}': {stage_cfg_dir}")

    if maintenance_action != "force-unlock":
        raise RuntimeError(f"❌ Unsupported maintenance action: {maintenance_action}")

    tfstate_key_var, tfstate_uri_var = resolve_force_unlock_tfstate_vars(repo_path)
    stage_env["GITHUB_WORKSPACE"] = str(repo_path)
    stage_env["MAINTENANCE_STAGE_CFG_DIR"] = str(stage_cfg_dir)
    stage_env["TFSTATE_KEY_VAR"] = tfstate_key_var
    stage_env["LOCK_ID"] = lock_id
    execution_context_repo_path = repo_path / EXECUTION_CONTEXT_FILENAME
    shutil.copy2(execution_context_path, execution_context_repo_path)
    stage_env["ATLAS_EXECUTION_CONTEXT_FILE"] = EXECUTION_CONTEXT_FILENAME

    maintenance_cmd = [
        "bash",
        "-lc",
        """
set -euo pipefail
source "$ATLAS_STAGE_UTILS_DIR/ctl/prepare_stage_runtime.sh"
prepare_stage_runtime "${MAINTENANCE_STAGE_CFG_DIR}"
./bin/tf.sh infra init "$TFSTATE_KEY_VAR"
./bin/tf.sh infra force-unlock "$TFSTATE_KEY_VAR" "$LOCK_ID"
""",
    ]
    logging.info("bash -lc <force-unlock-script>")
    run_and_log(
        maintenance_cmd,
        cwd=repo_path,
        env=stage_env,
    )

    print_run_summary(run_id, log_file)


def resolve_run_domain(
    workflow_cfg: dict,
    inventory_cfg: dict,
    ctl_state_backends_cfg: dict | None,
) -> str | None:
    """Resolve the run's single ctl-state domain from its active targets.

    One state domain per run: all active targets that declare a
    `ctl_state_backend_key` must declare the *same* one — that is the run's domain
    (and it must exist in the ctl_state_backends registry). Targets that omit it
    (commented in dev cfg) are the skip hatch. A run whose active targets declare
    *different* domains is a hard error (a run is one state/lock/outdate boundary).
    Returns the domain key, or None when no active target declares one."""
    targets = inventory_cfg.get("stage_targets", {})
    seen: dict[str, str] = {}  # domain -> first target that declared it
    for entry in workflow_cfg.get("stages", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        domain = (targets.get(target_name) or {}).get("ctl_state_backend_key")
        if domain is None:
            continue
        seen.setdefault(domain, target_name)
    if not seen:
        return None
    if len(seen) > 1:
        detail = ", ".join(f"{d} ({t})" for d, t in sorted(seen.items()))
        raise RuntimeError(
            f"❌ run spans multiple ctl-state domains [{detail}]; a run is one state "
            f"domain — split it into separate single-domain runs"
        )
    domain = next(iter(seen))
    if ctl_state_backends_cfg is not None and domain not in ctl_state_backends_cfg:
        known = ", ".join(sorted(ctl_state_backends_cfg)) or "none"
        raise RuntimeError(
            f"❌ ctl_state_backend_key {domain!r} has no ctl_state_backends entry; known domains: {known}"
        )
    return domain


def run_provisions_ctl_state_bucket(workflow_cfg: dict, inventory_cfg: dict) -> bool:
    """Whether any target in this run is the ctl-state bucket-creating target
    (declares provisions_ctl_state_bucket: true). Such a run may legitimately start
    before its results bucket exists; every other run must find it already there
    under a `required` sync policy."""
    targets = inventory_cfg.get("stage_targets", {})
    for entry in workflow_cfg.get("stages", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        target_cfg = targets.get(target_name) or {}
        if target_cfg.get("provisions_ctl_state_bucket") is True:
            return True
    return False


def validate_workflow_target_selectors(
    workflow_cfg: dict,
    inventory_cfg: dict,
    execution_context: dict[str, object],
) -> None:
    targets = inventory_cfg.get("stage_targets", {})
    for entry in workflow_cfg.get("stages", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        target_cfg = targets.get(target_name)
        if target_cfg is None:
            continue
        selectors = target_cfg.get("selectors")
        if not selector_matches(selectors, execution_context, label=f"target {target_name}"):
            raise RuntimeError(
                f"❌ target {target_name!r} is not available for runtime selectors {execution_context}; "
                f"selectors={selectors}"
            )

def build_sub_workflow_cfg(
    ctl_cfg_root: Path,
    action: str,
    *,
    source: str,
    ref: str,
    cfg_file_set_name: str,
    sub_workflow: str,
    execution_identity_key: str | None,
    ctl_state_backend_key: str | None = None,
) -> tuple[dict, dict]:
    """Build a one-target cfg for a synthetic repo-local sub_workflow run.

    The synthetic target is composed directly from CLI args and need not exist
    in targets/<action>/. --ctl-state-backend-key supplies the state domain the
    declared targets carry in cfg; omitted = no domain (skip triad applies).
    """
    stage_sources = collect_resource(ctl_cfg_root, "stage_sources")
    cfg_file_sets = collect_resource(ctl_cfg_root, "cfg_file_sets")
    cfg_file_sets_path = ctl_cfg_root
    cfg_file_set = cfg_file_sets.get(cfg_file_set_name)
    if not isinstance(cfg_file_set, dict):
        raise RuntimeError(f"❌ sub_workflow cfg_file_set {cfg_file_set_name!r} not found under {cfg_file_sets_path}")
    resolved = {
        "source": source,
        "ref": ref,
        "sub_workflow": sub_workflow,
        "cfg_root": cfg_file_set.get("cfg_root", "/"),
        "cfg_files": resolve_cfg_file_set_files(cfg_file_set_name, cfg_file_sets, cfg_file_sets_path),
    }
    if execution_identity_key:
        resolved["execution_identity_key"] = execution_identity_key
    if ctl_state_backend_key:
        resolved["ctl_state_backend_key"] = ctl_state_backend_key
    name = "sub_workflow"
    inventory_cfg = {"stage_sources": stage_sources, "stage_targets": {name: resolved}}
    workflow_cfg = {
        "meta": {"name": f"sub_workflow/{source}/{sub_workflow}", "action": action},
        "stages": [name],
    }
    return workflow_cfg, inventory_cfg


def run_pipeline(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    inventory_name: str,
    workflow_name: str | None,
    run_id: str,
    plt_overlays: list[str],
    ctl_variants: list[str],
    stage_repo_key: str,
    require_stage_ref: bool,
    use_local_tooling_cfg: bool,
    aws_implementation_key: str,
    run_dir: Path,
    artifacts_dir: Path,
    plt_merged_dir: Path,
    log_file: Path,
    profile_only_aws_profile: str | None,
    target_name: str | None = None,
    sub_workflow_run: dict | None = None,
    skip_ctl_state_backend_sync: bool = False,
) -> None:
    """
    Run a declared workflow, declared target, or synthetic repo-local sub_workflow.

    The caller passes stage repo settings and pre-created run/log directories.
    """
    # Build the execution context (flat dotted namespaces) and validate constraints.
    execution_context = build_execution_context(
        ctl_cfg_root,
        action=inventory_name,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
    )
    scope_params = scope_params_from_context(execution_context)
    validate_execution_context_constraints(ctl_cfg_root, execution_context)
    require_commit_refs = ref_policy_requires_commits(ctl_ref_policy)

    # Load workflow + inventory (validate before creating dirs).
    if sub_workflow_run:
        workflow_cfg, inventory_cfg = build_sub_workflow_cfg(
            ctl_cfg_root,
            inventory_name,
            source=sub_workflow_run["source"],
            ref=sub_workflow_run["ref"],
            cfg_file_set_name=sub_workflow_run["cfg_file_set"],
            sub_workflow=sub_workflow_run["sub_workflow"],
            execution_identity_key=sub_workflow_run.get("execution_identity_key"),
            ctl_state_backend_key=sub_workflow_run.get("ctl_state_backend_key"),
        )
    elif target_name:
        inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name)
        workflow_cfg = {
            "meta": {"name": f"{ctl_profile}/{inventory_name}/{target_name}", "action": inventory_name},
            "stages": [target_name],
        }
    else:
        workflow_cfg = load_workflow_cfg(ctl_cfg_root, ctl_profile, inventory_name, workflow_name, execution_context)
        inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name)
        workflow_cfg = apply_ctl_variants_to_workflow_cfg(
            ctl_cfg_root,
            workflow_cfg,
            inventory_cfg,
            execution_context=execution_context,
            inventory_name=inventory_name,
            workflow_name=workflow_name,
            ctl_variants=ctl_variants,
        )
    if not sub_workflow_run:
        validate_workflow_target_selectors(workflow_cfg, inventory_cfg, execution_context)

    # Ctl-state sync: resolve the run's single state domain from its active
    # targets (one-domain-per-run; hard error if they disagree), then arm the
    # syncer for that domain's bucket. Done after the target is known so a missing
    # bucket is tolerated only for the ctl-state bootstrap target.
    ctl_state_domain = resolve_run_domain(
        workflow_cfg, inventory_cfg, load_ctl_state_backends_cfg(ctl_cfg_root)
    )
    configure_ctl_state_sync(
        ctl_cfg_root,
        ctl_profile,
        ctl_state_domain,
        execution_context,
        run_dir,
        skip_ctl_state_backend_sync=skip_ctl_state_backend_sync,
        provisions_ctl_state_bucket=run_provisions_ctl_state_bucket(workflow_cfg, inventory_cfg),
        profile_only_aws_profile=profile_only_aws_profile,
        aws_implementation_key=aws_implementation_key,
    )
    execution_context_path = write_execution_context_artifact(run_dir, execution_context)

    refs = load_refs_cfg(ctl_cfg_root)
    if use_local_tooling_cfg:
        tooling_refs = load_local_tooling_cfg(ctl_cfg_root)
    else:
        tooling_refs = refs.get("global") or {}
        validate_tooling_refs_have_commits(tooling_refs, ctl_ref_policy)

    logging.info(f"Selector policy validation passed: ctl_profile={ctl_profile}")

    # Prepare pipeline config
    active_stages, pipeline_run_cfg_path = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
        plt_merged_dir,
        artifacts_dir,
        ctl_profile,
        plt_overlays,
        scope_params=scope_params,
        execution_context=execution_context,
        stage_repo_key=stage_repo_key,
        require_stage_ref=require_stage_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )
    # Single derivation chain: render the merged tree, then verify guards
    # against rendered values, then distribute stage input views from it.
    plt_rendered_dir = render_plt_cfg(plt_merged_dir, run_dir, execution_context)
    verify_guardrails(
        ctl_cfg_root, plt_cfg_root, plt_rendered_dir, execution_context, scope_params,
        required_target_paths=required_target_paths_for_stages(active_stages),
    )

    if sub_workflow_run:
        target_keys = sub_workflow_run.get("affected_target_keys") or []
        if inventory_name in MUTATING_ACTIONS and not target_keys:
            raise RuntimeError("❌ mutating sub_workflow runs require affected_target_keys")
    else:
        target_keys = target_keys_from_active_stages(active_stages)
    record_run_target_keys(run_dir, target_keys)
    run_metadata = load_run_metadata(run_dir)
    ctl_state_local_root_value = run_metadata.get("ctl_state_local_root")
    if isinstance(ctl_state_local_root_value, str) and ctl_state_local_root_value:
        mark_removed_definitions_outdated(Path(ctl_state_local_root_value), ctl_cfg_root)

    # Validate stages have commits and resolvable AWS access before execution.
    validate_stages_have_commits(active_stages, ctl_ref_policy)
    execution_identities = load_execution_identities_cfg(ctl_cfg_root)
    aws_access_contexts = load_aws_access_contexts_cfg(ctl_cfg_root)
    aws_account_registry_cfg = load_aws_account_registry_cfg(ctl_cfg_root)
    allow_aws_profile_only = ctl_allows_aws_profile_only(ctl_cfg_root, ctl_profile)
    aws_account_registry = validate_active_stage_aws_access(
        active_stages,
        execution_identities,
        aws_access_contexts,
        execution_context=execution_context,
        implementation_key=aws_implementation_key,
        account_registry=aws_account_registry_cfg,
        allow_profile_only=allow_aws_profile_only,
        profile_only_aws_profile=profile_only_aws_profile,
    )

    write_target_stage_flow_artifact(
        ctl_cfg_root,
        artifacts_dir,
        ctl_profile=ctl_profile,
        execution_context=execution_context,
        inventory_name=inventory_name,
        workflow_name=workflow_name,
        ctl_variants=ctl_variants,
        plt_overlays=plt_overlays,
        stage_repo_key=stage_repo_key,
        require_stage_ref=require_stage_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )

    # Write git metas
    write_git_metas(ctl_cfg_root, plt_cfg_root, artifacts_dir)

    # Resolved ctl cfg snapshot (self-describing run, next to cfg/plt/)
    write_ctl_cfg_snapshot(
        run_dir,
        ctl_profile=ctl_profile,
        ctl_profile_policy_cfg=ctl_profile_policy(ctl_cfg_root, ctl_profile),
        inventory_name=inventory_name,
        workflow_cfg=workflow_cfg,
        inventory_cfg=inventory_cfg,
        active_stages=active_stages,
        refs=refs,
        execution_context=execution_context,
    )

    # Distribute stage input views from the rendered tree
    plt_stages_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path, plt_rendered_dir, run_dir
    )
    # Prepared snapshot: cfg layers + run-level metadata are immutable from here.
    ctl_state_push("preparation complete")

    # Run stages
    run_stages(
        active_stages, run_dir, plt_stages_dir_path, execution_context_path,
        inventory_name, execution_context, run_id,
        tooling_refs=tooling_refs,
        use_local_tooling_cfg=use_local_tooling_cfg,
        execution_identities=execution_identities,
        aws_access_contexts=aws_access_contexts,
        aws_account_registry=aws_account_registry,
        aws_implementation_key=aws_implementation_key,
        allow_aws_profile_only=allow_aws_profile_only,
        profile_only_aws_profile=profile_only_aws_profile,
    )

    print_run_summary(run_id, log_file)
