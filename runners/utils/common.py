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
from datetime import datetime, timedelta, timezone
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
MAINTENANCE_ACTIONS = ("force-unlock", "status-sweep", "history-prune")
FORCE_UNLOCK_INIT_RE = re.compile(
    r"(?m)^\s*\./bin/tf\.sh\s+"
    r"(?P<stack_dir>[A-Za-z0-9_.\/-]+)\s+"
    r"init(?:-upgrade)?\s+\$?(?P<state_key>[A-Za-z_][A-Za-z0-9_]*)\b"
)

SERVICE_ID = "atlas-ctl-orchestrator-local"
CTL_RESULTS_LOCK_FILENAME = ".ctl.lock"
CTL_RESULTS_LOCK_META_FILENAME = ".ctl.lock.yaml"
RUN_METADATA_FILENAME = "RUN.yaml"
EXECUTION_CONTEXT_FILENAME = "execution_context.yaml"

PLT_GUARDRAILS_FILENAME = "__guardrails__.yaml"
PLT_GUARDRAILS_DIRNAME = "__guardrails__"
CFG_SOURCE_KEYS = ("plt", "guardrails")
CFG_ROOT_META_FILENAME = "__cfg__.yaml"
MUTATING_ACTIONS = ("provision", "destroy")
RUN_ACTIONS = ("provision", "plan", "destroy", "readonly")
RUN_TYPES = ("workflow", "target", "step_sequence", "maintenance", "fan_out")
# §Phase 30: reserved local-only ctl-state root — never synced, never a locator.
# Locator segments must start alphanumeric, so "_local" cannot collide.
LOCAL_ONLY_LOCATOR = ("_local",)
LOCATOR_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
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
    bundles governing ctl behavior: ref_policy, allowed_execution_access_modes
    (§12), and the ctl-state sync skip permissions
    (allow_agreed_defer_ctl_state_backend_sync, allow_force_skip_ctl_state_backend_sync)."""
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


# The closed set of ref_policy values. The engine branches strict-vs-permissive
# on `commit_required` (ref_policy_requires_commits); every other value is the
# permissive path. Validating against this set at load makes a typo fail loud
# instead of silently degrading to permissive (the unsafe direction).
REF_POLICY_COMMIT_REQUIRED = "commit_required"
REF_POLICY_LOCAL_DIRTY_ALLOWED = "local_dirty_allowed"
REF_POLICIES = frozenset({REF_POLICY_COMMIT_REQUIRED, REF_POLICY_LOCAL_DIRTY_ALLOWED})


def ctl_ref_policy(ctl_cfg_root: Path, ctl_profile: str) -> str:
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    ref_policy = policy.get("ref_policy")
    if not isinstance(ref_policy, str) or not ref_policy.strip():
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} must define non-empty ref_policy")
    ref_policy = ref_policy.strip()
    if ref_policy not in REF_POLICIES:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} has unknown ref_policy {ref_policy!r}; "
            f"expected one of {sorted(REF_POLICIES)}"
        )
    return ref_policy


def ctl_profile_bool(ctl_cfg_root: Path, ctl_profile: str, key: str) -> bool:
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    value = policy.get(key, False)
    if not isinstance(value, bool):
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} {key} must be a bool: {value!r}")
    return value


EXECUTION_ACCESS_MODES = ("standard", "agreed_direct", "force_bypass")

# Phase 26 — execution runtime (WHERE a target_run's box is produced). CTL selects one
# runtime for the whole run (always explicit, no default); the target_run declares
# which it can run in (a constraint).
EXECUTION_RUNTIME_MODES = ("local", "ci")
# Target run box images (step.yaml runtime.image). CTL owns how each is built/run.
STEP_IMAGES = ("infra", "ops")


def step_supported_execution_runtime_modes(runtime_cfg: dict, *, label: str) -> set[str]:
    """Runtimes a target_run can run in (§Phase 26); absent = all EXECUTION_RUNTIME_MODES."""
    raw = runtime_cfg.get("supported_execution_runtime_modes")
    if raw is None:
        return set(EXECUTION_RUNTIME_MODES)
    if not isinstance(raw, list) or not all(isinstance(r, str) for r in raw):
        raise RuntimeError(f"❌ target_run runtime.supported_execution_runtime_modes must be a list of strings: {label}")
    runtimes = set(raw)
    unknown = runtimes - set(EXECUTION_RUNTIME_MODES)
    if unknown:
        raise RuntimeError(f"❌ target_run runtime.supported_execution_runtime_modes has unknown runtimes {sorted(unknown)}: {label}")
    if not runtimes:
        raise RuntimeError(f"❌ target_run runtime.supported_execution_runtime_modes must not be empty: {label}")
    return runtimes


def ctl_allowed_execution_runtime_modes(ctl_cfg_root: Path, ctl_profile: str) -> set[str]:
    """Runtimes the ctl profile authorizes (§Phase 26). Absent = all EXECUTION_RUNTIME_MODES."""
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    raw = policy.get("allowed_execution_runtime_modes")
    if raw is None:
        return set(EXECUTION_RUNTIME_MODES)
    if not isinstance(raw, list) or not all(isinstance(r, str) for r in raw):
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} allowed_execution_runtime_modes must be a list of strings")
    runtimes = set(raw)
    unknown = runtimes - set(EXECUTION_RUNTIME_MODES)
    if unknown:
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} allowed_execution_runtime_modes has unknown runtimes {sorted(unknown)}")
    if not runtimes:
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} allowed_execution_runtime_modes must not be empty")
    return runtimes


def validate_execution_runtime_mode(ctl_cfg_root: Path, ctl_profile: str, execution_runtime_mode: str) -> None:
    """Reconcile the selected runtime against the ctl profile (§Phase 26): a known
    runtime, allowed by the profile. Per-target_run `supported_execution_runtime_modes` is enforced in
    run_steps, where the repo-local target_run manifest is loaded."""
    if execution_runtime_mode not in EXECUTION_RUNTIME_MODES:
        raise RuntimeError(f"❌ unknown execution runtime {execution_runtime_mode!r} (known: {sorted(EXECUTION_RUNTIME_MODES)})")
    allowed = ctl_allowed_execution_runtime_modes(ctl_cfg_root, ctl_profile)
    if execution_runtime_mode not in allowed:
        raise RuntimeError(
            f"❌ execution runtime {execution_runtime_mode!r} is not allowed by ctl profile {ctl_profile!r} (allowed: {sorted(allowed)})"
        )


def ctl_allowed_execution_access_modes(ctl_cfg_root: Path, ctl_profile: str) -> set[str]:
    """Modes the ctl profile authorizes (§12). Absent = {standard} only."""
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    raw = policy.get("allowed_execution_access_modes")
    if raw is None:
        return {"standard"}
    if not isinstance(raw, list) or not all(isinstance(m, str) for m in raw):
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} allowed_execution_access_modes must be a list of strings"
        )
    modes = set(raw)
    unknown = modes - set(EXECUTION_ACCESS_MODES)
    if unknown:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} allowed_execution_access_modes has unknown modes {sorted(unknown)}"
        )
    modes.add("standard")  # standard is always permitted
    return modes


def ctl_allows_agreed_defer_ctl_state_backend_sync(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_agreed_defer_ctl_state_backend_sync")


def ctl_allows_ctl_state_history_maintenance(
    ctl_cfg_root: Path, ctl_profile: str
) -> bool:
    return ctl_profile_bool(
        ctl_cfg_root, ctl_profile, "allow_ctl_state_history_maintenance"
    )


def ctl_allows_force_skip_ctl_state_backend_sync(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_force_skip_ctl_state_backend_sync")


def ctl_allows_force_skip_guardrails(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_force_skip_guardrails")


def ctl_allows_force_skip_full_cfg_validation_gate(
    ctl_cfg_root: Path, ctl_profile: str
) -> bool:
    return ctl_profile_bool(
        ctl_cfg_root,
        ctl_profile,
        "allow_force_skip_full_cfg_validation_gate",
    )


def validate_force_skip_full_cfg_validation_gate_policy(
    ctl_cfg_root: Path, ctl_profile: str, requested: bool
) -> None:
    if requested and not ctl_allows_force_skip_full_cfg_validation_gate(
        ctl_cfg_root, ctl_profile
    ):
        raise RuntimeError(
            "❌ --force-skip-full-cfg-validation-gate was requested, but ctl "
            f"profile {ctl_profile!r} does not grant "
            "allow_force_skip_full_cfg_validation_gate"
        )


def ctl_allows_force_skip_execution_identity_preflight_check(
    ctl_cfg_root: Path, ctl_profile: str
) -> bool:
    return ctl_profile_bool(
        ctl_cfg_root,
        ctl_profile,
        "allow_force_skip_execution_identity_preflight_check",
    )


def ref_policy_requires_commits(ref_policy: str) -> bool:
    return ref_policy == REF_POLICY_COMMIT_REQUIRED


def validate_reuse_committed_ref_policy(
    reuse_committed: bool | None, ref_policy: str, ctl_profile: str
) -> None:
    """Fail loud on a --reuse-committed=true that can never reuse.

    The reuse gate only reuses a committed result when ref_policy is
    commit_required (a clean, commit-pinned source). Under any other policy
    (e.g. local_dirty_allowed) reuse is structurally impossible, so
    --reuse-committed true would be a silent no-op — every run re-executes.
    Reject it instead of silently ignoring the flag."""
    if reuse_committed and not ref_policy_requires_commits(ref_policy):
        raise RuntimeError(
            f"❌ --reuse-committed true cannot reuse under ctl profile {ctl_profile!r} "
            f"(ref_policy {ref_policy!r}): reuse requires ref_policy 'commit_required' "
            "(a clean, commit-pinned source). Pass --reuse-committed false, or use a "
            "commit_required profile."
        )


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




def finalize_common_args(args: argparse.Namespace) -> None:
    """Normalize execution-params CLI args into a map and common values."""
    args.execution_params = selectors_to_map(args.execution_param, label="execution param")
    args.ctl_state_local_root = normalize_ctl_state_local_root(args.ctl_state_local_root)
    if getattr(args, "provider_credential", None) is not None:
        value = args.provider_credential.strip()
        args.provider_credential = value or None
    args.execution_access_mode = normalize_execution_access_mode(args)
    # §Phase 50: --status is gone from the run parsers (status is standalone).
    if (
        getattr(args, "execution_identity_preflight_check_only", False)
        and getattr(args, "force_skip_execution_identity_preflight_check", False)
    ):
        raise RuntimeError(
            "❌ --execution-identity-preflight-check-only and "
            "--force-skip-execution-identity-preflight-check are mutually exclusive"
        )
    if (
        args.execution_access_mode == "force_bypass"
        and getattr(args, "force_skip_execution_identity_preflight_check", False)
    ):
        raise RuntimeError(
            "❌ --force-skip-execution-identity-preflight-check is not applicable "
            "with --execution-access-mode force_bypass"
        )
    # --reuse-committed is an explicit true/false with no default. A normal run
    # reaches the reuse-vs-rerun decision and must state intent; the exit-early
    # preflight-only mode never does, so it stays optional there.
    if hasattr(args, "reuse_committed"):
        exits_before_execution = getattr(
            args, "execution_identity_preflight_check_only", False
        )
        if args.reuse_committed is None and not exits_before_execution:
            raise RuntimeError(
                "❌ --reuse-committed is required (true or false) for a normal run; "
                "omit it only with --execution-identity-preflight-check-only"
            )
    args.run_id = generate_uuid7()


def normalize_execution_access_mode(args: argparse.Namespace) -> str:
    """Validate the selected access mode's credential pairing (§12)."""
    mode = getattr(args, "execution_access_mode", "standard") or "standard"
    if mode == "force_bypass" and not getattr(args, "provider_credential", None):
        raise RuntimeError(
            "❌ --execution-access-mode force_bypass requires the substitute credential "
            "(--provider-credential): nothing to run with"
        )
    if getattr(args, "provider_credential", None) and mode != "force_bypass":
        raise RuntimeError(
            "❌ --provider-credential cannot override declared execution identities; "
            "pass it only together with --execution-access-mode force_bypass"
        )
    return mode


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


def log_target_run_banner(target_run_id: str, *, ch: str = "#", min_width: int = 70) -> None:
    title = f" {target_run_id} "
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
    if any(getattr(args, field, None) for field in ("source", "ref", "cfg_file_set", "step_sequence", "execution_identity_key", "affected_target_keys")):
        raise RuntimeError("❌ workflow runner does not accept step-sequence synthetic target args")


def validate_target_args(args: argparse.Namespace) -> None:
    """Validate args for a declared single-target run."""
    if not getattr(args, "target", None):
        raise RuntimeError("❌ target runner requires --target")
    if getattr(args, "workflow", None):
        raise RuntimeError("❌ target runner does not accept --workflow")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for target runs")
    if any(getattr(args, field, None) for field in ("source", "ref", "cfg_file_set", "step_sequence", "execution_identity_key", "affected_target_keys")):
        raise RuntimeError("❌ target runner does not accept step-sequence synthetic target args")


def validate_maintenance_args(args: argparse.Namespace) -> None:
    """Validate args for one explicit maintenance operation."""
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for maintenance")
    if any(
        getattr(args, field, None)
        for field in (
            "source", "ref", "cfg_file_set", "step_sequence",
            "execution_identity_key", "affected_target_keys",
        )
    ):
        raise RuntimeError(
            "❌ maintenance runner does not accept synthetic target args"
        )
    action = getattr(args, "maintenance_action", None)
    if not action:
        raise RuntimeError("❌ --maintenance-action is required for maintenance")
    if action == "force-unlock":
        if not getattr(args, "lock_id", None):
            raise RuntimeError(
                "❌ --lock-id is required for --maintenance-action=force-unlock"
            )
        if not getattr(args, "target", None) and not ctl_state_lock_matches(
            args.ctl_state_local_root, args.lock_id
        ):
            raise RuntimeError("❌ --target is required for Terraform force-unlock")
        return
    if getattr(args, "target", None):
        raise RuntimeError(f"❌ --target is not valid for {action}")
    if action == "status-sweep":
        return
    if action == "history-prune":
        if not args.prune_run_id and not args.prune_before:
            raise RuntimeError(
                "❌ history-prune requires --prune-run-id or --prune-before"
            )
        if args.apply_history_prune != args.agree_history_prune:
            raise RuntimeError(
                "❌ applying history prune requires both --apply-history-prune "
                "and --agree-history-prune"
            )
        return
    raise RuntimeError(f"❌ unsupported maintenance action: {action}")


def validate_step_sequence_args(args: argparse.Namespace) -> None:
    """Validate args for a synthetic repo-local step_sequence run."""
    if getattr(args, "workflow", None) or getattr(args, "target", None):
        raise RuntimeError("❌ step_sequence runner does not accept --workflow or --target")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for step_sequence runs")
    missing = [f for f in ("source", "ref", "cfg_file_set", "step_sequence") if not getattr(args, f, None)]
    if missing:
        raise RuntimeError(
            "❌ step_sequence needs " + ", ".join(f"--{m.replace('_', '-')}" for m in missing)
        )
    identity_key = getattr(args, "execution_identity_key", None)
    if identity_key is not None and not identity_key.strip():
        raise RuntimeError("❌ --execution-identity-key must be a non-empty string when provided")
    affected_target_keys = getattr(args, "affected_target_keys", None) or []
    if affected_target_keys:
        args.affected_target_keys = normalize_target_keys(affected_target_keys, label="--affected-target-key")
    if args.action in MUTATING_ACTIONS and not getattr(args, "affected_target_keys", None):
        raise RuntimeError("❌ mutating step_sequence runs require at least one --affected-target-key")





def validate_target_runs_have_commits(active_target_runs: dict, ref_policy: str) -> None:
    """Validate that all resolved target_runs and modules have explicit commits when required.

    Commit-required policy disallows branch references for executable code. Validation
    runs after workflow patches and refs have been resolved into active target_runs.
    """
    if not ref_policy_requires_commits(ref_policy):
        return

    target_runs_without_commit = []
    modules_without_commit = []
    for target_run_id, target_run_cfg in active_target_runs.items():
        if not target_run_cfg.get("commit"):
            target_runs_without_commit.append(target_run_id)

        raw_modules = target_run_cfg.get("modules") or {}
        if not isinstance(raw_modules, dict):
            modules_without_commit.append(f"{target_run_id}:<invalid-modules>")
            continue

        for module_name, module_cfg in raw_modules.items():
            if not module_cfg.get("commit"):
                modules_without_commit.append(f"{target_run_id}:{module_name}")

    if target_runs_without_commit or modules_without_commit:
        details = []
        if target_runs_without_commit:
            details.append(f"Target runs missing 'commit': {target_runs_without_commit}")
        if modules_without_commit:
            details.append(f"Modules missing 'commit': {modules_without_commit}")
        raise RuntimeError(
            "❌ ref_policy=commit_required requires all target_runs and modules to have explicit 'commit' specified.\n"
            f"   {'; '.join(details)}\n"
            "   Using branch references is not allowed for reproducibility."
        )

def validate_ctl_cfg_ref_has_commit(
    ref_policy: str,
    ctl_cfg_branch: str | None,
    ctl_cfg_commit: str | None,
) -> None:
    """Validate the CLI-selected ctl cfg ref under a strict ref policy."""
    if ref_policy_requires_commits(ref_policy) and ctl_cfg_branch and not ctl_cfg_commit:
        raise RuntimeError(
            "❌ ref_policy=commit_required requires --ctl-cfg to use @commit=sha "
            f"(not branch={ctl_cfg_branch!r})"
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

def build_active_target_runs(
    workflow_cfg: dict,
    inventory_cfg: dict,
    repo_key: str = "repo_url",
    require_branch_or_commit: bool = True,
    refs: dict | None = None,
    execution_context: dict[str, object] | None = None,
    require_commit_refs: bool = False,
) -> dict:
    inventory_target_sources = inventory_cfg.get("target_sources", {})
    if not isinstance(inventory_target_sources, dict):
        raise RuntimeError("'target_sources' in inventory must be a mapping: source -> meta")

    inventory_targets = inventory_cfg.get("targets", {})
    if not isinstance(inventory_targets, dict):
        raise RuntimeError("'targets' in inventory must be a mapping: target -> meta")

    refs = refs or {}
    scoped_refs = refs.get("scoped") or {}
    ref_context_values = execution_context or {}
    active = {}

    def normalize_cfg_root(raw_value, *, target_key: str) -> str:
        value = raw_value if raw_value is not None else "/"
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must be a non-empty string")
        value = value.strip()
        if "\\" in value:
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must use forward slashes: {value}")
        if not value.startswith("/"):
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must start with /: {value}")
        parts = [part for part in value.split("/") if part]
        if any(part in (".", "..") for part in parts):
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must not contain . or ..: {value}")
        # cfg_root is freed (Phase 2d): any safe path incl. "/" (root) and multi-segment; it no
        # longer must be a single scope segment and is independent of the target's ref/context.
        return "/" + "/".join(parts)

    for st in workflow_cfg.get("target_runs", []):
        if isinstance(st, str):
            target_run_id = st
            target_key = st
            target_override = {}
        else:
            target_run_id = st.get("id")
            if not target_run_id:
                raise RuntimeError("Target run entry missing required field 'id'")
            target_key = st.get("target")
            if not target_key:
                raise RuntimeError(f"Target run {target_run_id!r} has empty 'target'")
            target_override = st

        if target_key not in inventory_targets:
            raise RuntimeError(
                f"Target run target {target_key!r} (target_run id={target_run_id!r}) not found in inventory {workflow_cfg.get('inventory')!r}"
            )

        target_cfg = inventory_targets[target_key]
        if not isinstance(target_cfg, dict):
            raise RuntimeError(
                f"Target run target {target_key!r} metadata must be a mapping, got: {type(target_cfg).__name__}"
            )

        target_source = target_cfg.get("source")
        if not isinstance(target_source, str) or not target_source:
            raise RuntimeError(f"Target run target {target_key!r} must define non-empty 'source'")
        if target_source not in inventory_target_sources:
            raise RuntimeError(
                f"Target run target {target_key!r} references missing source {target_source!r} in inventory {workflow_cfg.get('inventory')!r}"
            )

        source_cfg = inventory_target_sources[target_source]
        if not isinstance(source_cfg, dict):
            raise RuntimeError(
                f"Target run source {target_source!r} metadata must be a mapping, got: {type(source_cfg).__name__}"
            )

        # Phase 2d: resolve this target's ref context → per-context source/module pins.
        target_ref = target_cfg.get("ref")
        ctx_target_source_refs: dict = {}
        ctx_module_refs: dict = {}
        if scoped_refs and target_ref:
            ctx = resolve_ref_context(target_ref, ref_context_values)
            ctx_block = scoped_refs.get(ctx)
            if ctx_block is None:
                raise RuntimeError(
                    f"Target run target {target_key!r} ref context {ctx!r} not found in refs.scoped"
                )
            # §Phase 31 3c: a scoped-ref group resolves to one concrete scoped
            # entry (the member ref_key may carry ${execution_context.*}
            # placeholders, rendered here before the second lookup).
            if selector_group_is_group(ctx_block):
                member_ref = resolve_selector_group_member(
                    ctx_block, ref_context_values,
                    value_field="ref_key",
                    label=f"refs.scoped group {ctx!r}",
                )
                concrete_ctx = resolve_ref_context(member_ref, ref_context_values)
                ctx_block = scoped_refs.get(concrete_ctx)
                if ctx_block is None:
                    raise RuntimeError(
                        f"Target run target {target_key!r} refs.scoped group {ctx!r} member "
                        f"resolved to {concrete_ctx!r}, which is not in refs.scoped"
                    )
                if selector_group_is_group(ctx_block):
                    raise RuntimeError(
                        f"Target run target {target_key!r} refs.scoped group {ctx!r} member "
                        f"{concrete_ctx!r} is itself a group (no nested groups)"
                    )
            ctx_target_source_refs = ctx_block.get("target_sources") or {}
            ctx_module_refs = ctx_block.get("modules") or {}

        target_source_ref = ctx_target_source_refs.get(target_source) or {}
        if not isinstance(target_source_ref, dict):
            raise RuntimeError(
                f"Target run source refs for {target_source!r} must be a mapping, got: {type(target_source_ref).__name__}"
            )

        branch = target_override.get("branch") or target_source_ref.get("branch")
        commit = target_override.get("commit") or target_source_ref.get("commit")
        # fat target carries the repo-local step_sequence; a dict target_run entry may still override
        child_step_sequence = target_override.get("step_sequence") or target_cfg.get("step_sequence")

        if branch and commit:
            raise RuntimeError(
                f"Target run {target_run_id!r} resolved both branch={branch!r} and commit={commit!r}. "
                "Only one ref type may be set."
            )

        if require_branch_or_commit and not branch and not commit:
            raise RuntimeError(f"Target run {target_run_id!r} source {target_source!r} has neither branch nor commit configured")
        if require_branch_or_commit and require_commit_refs and not commit:
            raise RuntimeError(
                f"Target run {target_run_id!r} ref {target_ref!r} requires an explicit commit (not a branch) for reproducibility"
            )

        repo_value = source_cfg.get(repo_key)
        if not repo_value:
            raise RuntimeError(
                f"Target run {target_run_id!r} (target={target_key!r}, source={target_source!r}) missing {repo_key!r} in inventory {workflow_cfg.get('inventory')!r}"
            )

        cfg_files = target_cfg.get("cfg_files", [])
        if cfg_files is None:
            cfg_files = []
        if not isinstance(cfg_files, list):
            raise RuntimeError(f"Target run target {target_key!r} cfg_files must be a list")

        active_target_run = {
            "target": target_key,
            "source": target_source,
            "ref": target_ref,
            "branch": branch,
            "commit": commit,
            "step_sequence": child_step_sequence,
            "cfg_root": normalize_cfg_root(target_cfg.get("cfg_root", "/"), target_key=target_key),
            "cfg_files": cfg_files,
        }

        execution_identity_key = target_override.get("execution_identity_key") or target_cfg.get("execution_identity_key")
        if execution_identity_key is not None:
            if not isinstance(execution_identity_key, str) or not execution_identity_key.strip():
                raise RuntimeError(f"Target run {target_run_id!r} execution_identity_key must be a non-empty string")
            active_target_run["execution_identity_key"] = execution_identity_key.strip()

        # §Phase 31/32: the declared instance identity must ride on the target_run
        # so per-target reports and the target-instance locator see it (the
        # workflow-composition identity reads it from the inventory separately).
        if target_cfg.get("target_instance_params_unresolved"):
            raise RuntimeError(
                f"Target run target {target_key!r} has a members-shaped "
                "target_instance_params whose dispatch axis is unbound in this run"
            )
        instance_params = target_cfg.get("target_instance_params")
        if instance_params is not None:
            active_target_run["target_instance_params"] = instance_params

        if repo_key == "repo_path":
            repo_path = Path(repo_value).expanduser()
            if not repo_path.is_absolute():
                raise RuntimeError(
                    f"Target run {target_run_id!r} source {target_source!r} repo_path must be absolute, got: {repo_value}"
                )
            active_target_run["repo_path"] = str(repo_path.resolve())
        else:
            active_target_run["repo_url"] = repo_value
            active_target_run["token_type"] = source_cfg.get("token_type")

        raw_modules = source_cfg.get("modules") or {}
        if raw_modules and not isinstance(raw_modules, dict):
            raise RuntimeError(
                f"Target run {target_run_id!r} source {target_source!r} modules must be a mapping, got: {type(raw_modules).__name__}"
            )

        resolved_modules = {}
        for module_name, module_meta in raw_modules.items():
            if not isinstance(module_name, str):
                raise RuntimeError(
                    f"Target run {target_run_id!r} module names must be strings, got: {type(module_name).__name__}"
                )
            if module_meta is None:
                module_meta = {}
            if not isinstance(module_meta, dict):
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} metadata must be a mapping, got: {type(module_meta).__name__}"
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
                    f"Target run {target_run_id!r} module {module_name!r} has neither branch nor commit configured"
                )
            if require_branch_or_commit and require_commit_refs and not module_commit:
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} ref {target_ref!r} requires an explicit commit"
                )

            dest = module_meta.get("dest")
            if not isinstance(dest, str) or not dest.strip():
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} must define non-empty 'dest'"
                )
            dest_path = Path(dest)
            if dest_path.is_absolute() or ".." in dest_path.parts:
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} dest must stay within the target_run repo: {dest}"
                )

            module_repo_value = module_meta.get(repo_key)
            if not module_repo_value:
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} missing {repo_key!r} in inventory {workflow_cfg.get('inventory')!r}"
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
                        f"Target run {target_run_id!r} module {module_name!r} repo_path must be absolute, got: {module_repo_value}"
                    )
                resolved_module["repo_path"] = str(module_repo_path.resolve())
            else:
                resolved_module["repo_url"] = module_repo_value
                resolved_module["token_type"] = module_meta.get("token_type")

            resolved_modules[module_name] = resolved_module

        if resolved_modules:
            active_target_run["modules"] = resolved_modules

        active[target_run_id] = active_target_run

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
    unique entries sit: 1 for flat catalogs (target_sources/cfg_file_sets),
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


def load_cfg_sources(ctl_cfg_root: Path) -> dict[str, dict[str, object]]:
    """Load the ctl-owned one-to-one plt and guardrail source bindings."""
    entries = collect_resource(ctl_cfg_root, "cfg_sources")
    expected = set(CFG_SOURCE_KEYS)
    actual = set(entries)
    if actual != expected:
        raise RuntimeError(
            f"❌ cfg_sources must define exactly {sorted(expected)}; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    normalized: dict[str, dict[str, object]] = {}
    for key in CFG_SOURCE_KEYS:
        raw = entries[key]
        label = f"cfg_sources.{key}"
        if not isinstance(raw, dict) or not raw:
            raise RuntimeError(f"❌ {label} must be a non-empty mapping")
        keys = set(raw)
        if keys == {"repo_path"}:
            value = raw["repo_path"]
            if not isinstance(value, str) or not value.strip():
                raise RuntimeError(f"❌ {label}.repo_path must be a non-empty string")
            normalized[key] = {"repo_path": value.strip()}
            continue
        if keys != {"repo_url", "ref"}:
            raise RuntimeError(f"❌ {label} must contain either repo_path only or exactly repo_url + ref")
        url, ref = raw["repo_url"], raw["ref"]
        if not isinstance(url, str) or not url.strip():
            raise RuntimeError(f"❌ {label}.repo_url must be a non-empty string")
        if not isinstance(ref, dict) or len(ref) != 1:
            raise RuntimeError(f"❌ {label}.ref must contain exactly one of branch or commit")
        kind, value = next(iter(ref.items()))
        if kind not in {"branch", "commit"} or not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"❌ {label}.ref must contain a non-empty branch or commit")
        normalized[key] = {"repo_url": url.strip(), "ref": {kind: value.strip()}}
    return normalized


def validate_cfg_source_refs(
    sources: dict[str, dict[str, object]],
    ref_policy: str,
) -> None:
    """Require exact companion commits under commit_required."""
    if not ref_policy_requires_commits(ref_policy):
        return
    errors = []
    for key in CFG_SOURCE_KEYS:
        entry = sources[key]
        ref = entry.get("ref") or {}
        if "repo_path" in entry:
            errors.append(f"{key} uses repo_path")
        elif not isinstance(ref, dict) or not ref.get("commit"):
            errors.append(f"{key} is not commit-pinned")
    if errors:
        raise RuntimeError(
            "❌ ref_policy=commit_required requires commit-pinned cfg sources: "
            + ", ".join(errors)
        )


def materialize_cfg_sources(
    ctl_cfg_root: Path,
    *,
    ref_policy: str,
    run_cfg_dir: Path,
    token: str | None = None,
) -> dict[str, Path]:
    """Resolve local companion roots or clone their ctl-bound remote refs."""
    sources = load_cfg_sources(ctl_cfg_root)
    validate_cfg_source_refs(sources, ref_policy)
    run_cfg_dir.mkdir(parents=True, exist_ok=True)
    roots = {}
    for key in CFG_SOURCE_KEYS:
        entry = sources[key]
        if "repo_path" in entry:
            root = Path(str(entry["repo_path"])).expanduser()
            root = (ctl_cfg_root / root).resolve() if not root.is_absolute() else root.resolve()
            if not root.is_dir():
                raise RuntimeError(f"❌ cfg_sources.{key}.repo_path not found: {root}")
            roots[key] = root
            continue
        ref = entry["ref"]
        assert isinstance(ref, dict)
        root = (run_cfg_dir / f"{key}_cfg").resolve()
        try:
            root.relative_to(run_cfg_dir.resolve())
        except ValueError as exc:
            raise RuntimeError(f"❌ cfg source destination escapes run cfg dir: {root}") from exc
        if root.exists():
            shutil.rmtree(root)
        git_clone(str(entry["repo_url"]), ref.get("branch"), ref.get("commit"), root, token)
        roots[key] = root
    return roots


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

    Returns `{global: {tooling...}, scoped: {<ctx>: {target_sources, modules}}}`.
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
    target_runs: list = []
    for workflow_key in import_keys:
        target_runs.extend(expand_workflow_imports(action_workflows, workflow_key, (*_stack, name)))
    target_runs.extend(target_keys)
    seen: set = set()
    for target_key in target_runs:
        if target_key in seen:
            raise RuntimeError(f"❌ workflow {name!r} has duplicate target key {target_key!r} after import expansion")
        seen.add(target_key)
    return target_runs


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


def get_provider_adapter(provider: str):
    """Dispatch to the provider adapter (utils.providers); unknown = hard error."""
    from utils.providers import get_adapter
    return get_adapter(provider)


def run_provider_adapter(execution_context: dict[str, object]):
    """The run's adapter, selected by the required provider execution param."""
    provider = execution_context.get(f"{EXECUTION_CONTEXT_ROOT}.params.provider")
    if not isinstance(provider, str) or not provider.strip():
        raise RuntimeError("❌ execution param 'provider' is required to select the provider adapter")
    return get_provider_adapter(provider.strip())


def collect_provider_cfg_findings(
    ctl_cfg_root: Path, execution_context: dict[str, object]
) -> list[dict]:
    """Stage-1 provider cfg well-formedness findings (run once), via the adapter."""
    adapter = run_provider_adapter(execution_context)
    return adapter.collect_provider_cfg_findings(
        ctl_cfg_root, execution_context=execution_context
    )


def execution_identity_is_group(entry: object) -> bool:
    """A group entry selects one concrete member by selectors (§Phase 10)."""
    return isinstance(entry, dict) and "members" in entry


def _validate_execution_identity_group(
    group_key: str, group_cfg: dict, identities: dict, ctl_cfg_root: Path
) -> None:
    """A group is provider-homogeneous and declares its provider; members select
    concrete identities of the SAME provider (§Phase 10)."""
    unknown = set(group_cfg) - {"provider", "members"}
    if unknown:
        raise RuntimeError(
            f"❌ execution identity group {group_key!r} has unsupported keys {sorted(unknown)} "
            f"(a group is provider + members only): {ctl_cfg_root}"
        )
    members = group_cfg.get("members")
    if not isinstance(members, list) or not members:
        raise RuntimeError(f"❌ execution identity group {group_key!r} members must be a non-empty list: {ctl_cfg_root}")
    group_provider = group_cfg["provider"]
    seen: set[str] = set()
    for member in members:
        if not isinstance(member, dict) or set(member) - {"identity_key", "selectors"}:
            raise RuntimeError(
                f"❌ execution identity group {group_key!r} member must be {{identity_key, selectors}}: {ctl_cfg_root}"
            )
        member_key = member.get("identity_key")
        if not isinstance(member_key, str) or not member_key.strip():
            raise RuntimeError(f"❌ execution identity group {group_key!r} member identity_key must be a non-empty string: {ctl_cfg_root}")
        member_key = member_key.strip()
        if member_key in seen:
            raise RuntimeError(f"❌ execution identity group {group_key!r} lists member {member_key!r} twice: {ctl_cfg_root}")
        seen.add(member_key)
        # selectors must be the structured match/in form (generic evaluator)
        selector_requirements(member.get("selectors"), label=f"group {group_key} member {member_key}", structured_only=True)
        if member_key == group_key:
            raise RuntimeError(
                f"❌ execution identity group {group_key!r} references itself: {ctl_cfg_root}"
            )
        # §Phase 33: a member may be a concrete identity OR another group (one
        # dispatch axis per group; deep cycles fail at resolve time).
        concrete = identities.get(member_key)
        if not isinstance(concrete, dict):
            raise RuntimeError(
                f"❌ execution identity group {group_key!r} member {member_key!r} is not defined "
                f"in execution_identities: {ctl_cfg_root}"
            )
        if concrete.get("provider") != group_provider:
            raise RuntimeError(
                f"❌ execution identity group {group_key!r} is provider {group_provider!r} but member "
                f"{member_key!r} is provider {concrete.get('provider')!r} (groups are provider-homogeneous): {ctl_cfg_root}"
            )
    # a group resolves exactly one member — reject byte-identical member selectors.
    reject_duplicate_selectors(
        {m.get("identity_key"): m.get("selectors") for m in members},
        label=f"execution identity group {group_key}",
    )
    # §Phase 32 per-axis rule: identity dispatch may use only declarable params
    # axes or the path-encoded ctl.action — never other ctl.* facts.
    collect_member_dispatch_axes(
        members, label=f"execution identity group {group_key}"
    )


def selector_group_is_group(entry: object) -> bool:
    """§Phase 31 3c: a selector-membered group entry in a cfg collection
    (cfg_file_sets, refs.scoped) — same resolution semantics as execution
    identity groups: `members` select exactly one concrete value."""
    return isinstance(entry, dict) and "members" in entry


def resolve_selector_group_member(
    entry: dict,
    execution_context: dict[str, object],
    *,
    value_field: str,
    label: str,
    tolerate_none: bool = False,
) -> str | None:
    """Resolve a selector-membered group entry to its one matching member's
    `value_field` (§Phase 31 3c). Mirrors execution-identity group semantics:
    members are {<value_field>, selectors}; EXACTLY ONE member must match the
    frozen execution context. The returned value may still carry
    ${execution_context.*} placeholders — the caller renders them.

    With `tolerate_none=True`, ZERO matches returns None instead of raising (used
    for an inactive target whose selector axis isn't bound in this run); MORE than
    one match is always a hard error (a genuine cfg ambiguity)."""
    members = entry.get("members")
    if not isinstance(members, list) or not members:
        raise RuntimeError(f"❌ {label}: group members must be a non-empty list")
    for member in members:
        if not isinstance(member, dict) or set(member) - {value_field, "selectors"}:
            raise RuntimeError(
                f"❌ {label}: group member must be {{{value_field}, selectors}}"
            )
        value = member.get(value_field)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"❌ {label}: group member {value_field} must be a non-empty string")
    reject_duplicate_selectors(
        {m.get(value_field): m.get("selectors") for m in members}, label=label
    )
    matches = [
        member for member in members
        if selector_matches(
            member.get("selectors"), execution_context,
            label=f"{label} member {member.get(value_field)}",
            structured_only=True,
        )
    ]
    if len(matches) != 1:
        if tolerate_none and not matches:
            return None
        member_values = [member.get(value_field) for member in members]
        raise RuntimeError(
            f"❌ {label}: exactly one group member must match the execution context, "
            f"matched {len(matches)} (members: {member_values})"
        )
    return matches[0][value_field].strip()


def _selector_param_axes(members: object) -> set[str]:
    """The params-namespace axes referenced by a member list's selectors
    (§Phase 32 consumed-axes guard input). Non-params refs (ctl.*) are ignored.
    (EXECUTION_CONTEXT_ROOT is defined later in the module — reference it at
    call time, never at import time.)"""
    prefix = f"{EXECUTION_CONTEXT_ROOT}.params."
    axes: set[str] = set()
    for member in members or []:
        if not isinstance(member, dict):
            continue
        try:
            requirements = selector_requirements(
                member.get("selectors"), label="consumed-axes scan", structured_only=True
            )
        except Exception:
            continue
        for ref in requirements:
            if ref.startswith(prefix):
                axes.add(ref[len(prefix):])
    return axes


def _template_param_axes(value: object) -> set[str]:
    """${execution_context.params.X} variables in a raw template string."""
    if not isinstance(value, str):
        return set()
    pattern = (
        rf"\$\{{{re.escape(EXECUTION_CONTEXT_ROOT)}\.params\.([A-Za-z_][A-Za-z0-9_]*)\}}"
    )
    return set(re.findall(pattern, value))


def collect_member_dispatch_axes(members: object, *, label: str) -> set[str]:
    """§Phase 32 instance-uniqueness rule, field-agnostic: EVERY members-shaped
    dispatch in target resolution feeds the guard, and the violation test is
    per-AXIS, never per-field:

    - a params axis → returned (must be declared in target_instance_params
      unless path-encoded via the namespace);
    - `ctl.action` → safe (the action is its own ctl-state path segment);
    - any OTHER ctl.* fact (e.g. ctl.profile) → hard error: it is neither
      path-encoded nor declarable, so two runs differing only in it would
      collapse onto one instance path and self-override."""
    params_prefix = f"{EXECUTION_CONTEXT_ROOT}.params."
    action_ref = f"{EXECUTION_CONTEXT_ROOT}.ctl.action"
    axes: set[str] = set()
    for member in members or []:
        if not isinstance(member, dict):
            continue
        requirements = selector_requirements(
            member.get("selectors"), label=label, structured_only=True
        )
        for ref in requirements:
            if ref.startswith(params_prefix):
                axes.add(ref[len(params_prefix):])
            elif ref != action_ref:
                raise RuntimeError(
                    f"❌ {label}: dispatch on {ref!r} is not allowed — target "
                    "resolution may dispatch only on declarable params axes or "
                    "the path-encoded ctl.action"
                )
    return axes


def resolve_list_members(
    entry: dict,
    execution_context: dict[str, object] | None,
    *,
    value_field: str,
    label: str,
    allow_empty: bool = False,
) -> list | None:
    """Resolve a members-shaped LIST-valued declaration
    ({members: [{<value_field>: [...], selectors: {...}}, ...]}) to the ONE
    matching member's list (§Phase 32 instance schemas, §Phase 33 per-action
    cfg_files/target_keys). The scalar twin is resolve_selector_group_member.
    Returns None when no context is available or the dispatch axis is unbound
    (deferred — the caller decides whether that is an error)."""
    members = entry.get("members")
    if set(entry) != {"members"} or not isinstance(members, list) or not members:
        raise RuntimeError(f"❌ {label}: members-shaped declaration must be {{members: [...]}}")
    for member in members:
        if not isinstance(member, dict) or set(member) != {value_field, "selectors"}:
            raise RuntimeError(f"❌ {label}: each member must be {{{value_field}, selectors}}")
        if not isinstance(member[value_field], list) or (
            not member[value_field] and not allow_empty
        ):
            raise RuntimeError(f"❌ {label}: member {value_field} must be a non-empty list")
    if execution_context is None:
        return None
    matches = [
        member for member in members
        if selector_matches(
            member["selectors"], execution_context,
            label=f"{label} member", structured_only=True,
        )
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise RuntimeError(
            f"❌ {label}: exactly one member must match the execution context, matched {len(matches)}"
        )
    return list(matches[0][value_field])


def _resolve_instance_params_members(
    entry: dict, execution_context: dict[str, object] | None, *, target_name: str
) -> list[str] | None:
    return resolve_list_members(
        entry,
        execution_context,
        value_field="params",
        label=f"target {target_name!r} target_instance_params",
    )


def resolve_execution_identity_entry(
    identities: dict,
    identity_key: str,
    execution_context: dict[str, object],
    _stack: tuple = (),
) -> tuple[str, dict]:
    """Resolve an execution_identity_key to (concrete_key, concrete_cfg) (§Phase 10).

    A group entry selects EXACTLY ONE member by matching member selectors against
    the execution context; a concrete entry returns itself. §Phase 33: a member
    may itself be a GROUP (each group dispatches ONE axis, e.g. ctl.action →
    account), resolved recursively with cycle detection. The group's declared
    provider is checked against the run BEFORE matching, so a wrong-provider run
    fails precisely rather than as an ambiguous no-match."""
    if identity_key in _stack:
        raise RuntimeError(
            f"❌ execution identity group cycle: {' -> '.join([*_stack, identity_key])}"
        )
    entry = identities.get(identity_key)
    if not isinstance(entry, dict):
        raise RuntimeError(f"❌ execution_identity_key {identity_key!r} is not defined in execution_identities")
    if not execution_identity_is_group(entry):
        return identity_key, entry
    group_provider = entry.get("provider")
    run_provider = execution_context.get(f"{EXECUTION_CONTEXT_ROOT}.params.provider")
    if run_provider is not None and str(run_provider) != group_provider:
        raise RuntimeError(
            f"❌ execution identity group {identity_key!r} is provider {group_provider!r}, "
            f"but the run provider is {run_provider!r}"
        )
    matches = [
        member for member in entry["members"]
        if selector_matches(
            member.get("selectors"), execution_context,
            label=f"execution identity group {identity_key} member {member.get('identity_key')}",
            structured_only=True,
        )
    ]
    if len(matches) != 1:
        member_keys = [member.get("identity_key") for member in entry["members"]]
        raise RuntimeError(
            f"❌ execution identity group {identity_key!r}: exactly one member must match the execution "
            f"context, matched {len(matches)} (members: {member_keys})"
        )
    member_key = matches[0]["identity_key"].strip()
    return resolve_execution_identity_entry(
        identities, member_key, execution_context, (*_stack, identity_key)
    )


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
        if execution_identity_is_group(identity_cfg):
            # a GROUP is a provider-neutral selector envelope (§Phase 10); the
            # engine validates it generically — no provider adapter payload
            _validate_execution_identity_group(identity_key, identity_cfg, identities, ctl_cfg_root)
            continue
        # the generic loader owns only the envelope; the identity payload is the
        # selected provider adapter's schema
        get_provider_adapter(provider).validate_execution_identity(identity_key, identity_cfg, ctl_cfg_root)

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






















def _require_non_empty_string(value, label: str, path: Path | None = None) -> str:
    suffix = f": {path}" if path is not None else ""
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty string{suffix}")
    return value.strip()












def validate_execution_identity_coverage(
    active_target_runs: dict,
    *,
    execution_access_mode: str = "standard",
) -> None:
    """Every target_run declares its execution identity, always. The only run without
    identities is bypass mode (--execution-access-mode force_bypass + substitute
    credential), which covers identity-less target_runs too."""
    if execution_access_mode == "force_bypass":
        return
    stages_without_identity = sorted(
        target_run_id for target_run_id, target_run in active_target_runs.items()
        if target_run.get("execution_identity_key") is None
    )
    if stages_without_identity:
        raise RuntimeError(
            "❌ selected target_runs have no execution_identity_key: "
            + ", ".join(stages_without_identity)
            + "; declare it, or run with --execution-access-mode force_bypass + --provider-credential"
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


def _add_workflow_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--workflow",
        required=True,
        help="declared ctl workflow name",
    )


def _add_target_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--target",
        required=True,
        help="declared target name",
    )


def _add_fan_out_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--fan-out",
        required=True,
        dest="fan_out",
        help="declared fan_out key to expand and run",
    )


def _add_maintenance_args(parser: argparse._ActionsContainer) -> None:
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
    parser.add_argument(
        "--prune-run-id",
        action="append",
        default=[],
        help="run UUIDv7 to include in a history-prune selection; repeatable",
    )
    parser.add_argument(
        "--prune-before",
        help="prune run history older than this ISO-8601 timestamp",
    )
    parser.add_argument(
        "--prune-kind",
        choices=["target", "workflow"],
        help="limit history-prune to one state-owner kind",
    )
    parser.add_argument(
        "--cascade",
        action="store_true",
        help="also prune retained workflow runs that reference selected target runs",
    )
    parser.add_argument(
        "--apply-history-prune",
        action="store_true",
        help="apply the reported history deletion set",
    )
    parser.add_argument(
        "--agree-history-prune",
        action="store_true",
        help="explicitly acknowledge deletion of the reported unversioned object keys",
    )


def _add_step_sequence_args(parser: argparse._ActionsContainer) -> None:
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
        "--step-sequence",
        required=True,
        dest="step_sequence",
        help="repo-local step_sequence to run",
    )
    parser.add_argument(
        "--execution-identity-key",
        dest="execution_identity_key",
        default=None,
        help="execution identity key for a synthetic target",
    )
    parser.add_argument(
        "--affected-target-key",
        dest="affected_target_keys",
        action="append",
        default=[],
        help="affected declared target key; repeatable and required for mutating synthetic runs",
    )


def add_common_args(parser: argparse.ArgumentParser, *, run_type: str) -> None:
    """Add shared and runner-specific arguments for local runner entrypoints.

    Arguments are placed into titled argparse groups that drive --help
    presentation: ctl -> execution -> action & selector -> defer / force
    overrides -> cfg variation -> run modes -> misc, followed by suppressed
    internal args. Group creation order is the --help section order (it no
    longer depends on add order). Keep add_bootstrap_common_args (the
    pre-fetch --help duplicate) with the SAME groups in the SAME order."""
    ctl_group = parser.add_argument_group(
        "ctl",
        "control-plane authority & context: cfg/policy source, governing "
        "profile, state root — the profile declares what this run is allowed to do",
    )
    execution_group = parser.add_argument_group(
        "execution",
        "concrete execution choices (access, runtime, params) — the values this "
        "run actually uses, each honored only within what the ctl profile permits",
    )
    selector_group = parser.add_argument_group(
        "run",
        "the actual run: lifecycle action, the runner selector, and (for "
        "workflow/fan-out) whether to reuse committed children",
    )
    override_group = parser.add_argument_group(
        "defer / skip overrides",
        "authorized escalations; each also requires ctl-profile allowance",
    )
    variation_group = parser.add_argument_group(
        "cfg variation", "optional ctl variants and plt overlays"
    )
    mode_group = parser.add_argument_group(
        "checks & previews",
        "inspect or preview only; exit without executing targets",
    )
    misc_group = parser.add_argument_group("misc")
    # 1) ctl
    ctl_group.add_argument(
        "--ctl-cfg",
        required=True,
        help="git URL@ref or local path to the ctl cfg",
    )
    ctl_group.add_argument(
        "--ctl-profile",
        required=True,
        help="Ctl profile name (named policy bundle from the ctl_profiles catalog)",
    )
    ctl_group.add_argument(
        "--ctl-state-local-root",
        required=True,
        help="Local ctl-state root (run results tree); runner appends <action>/<run_type>/<name>",
    )
    # 2) execution access mode, then runtime.
    # Execution access is a provider-neutral MODE (§12): standard (normal),
    # agreed_direct (approved bootstrap/recovery access from the identity's
    # direct source), force_bypass (whole-run emergency substitute credential;
    # identity cfg is not resolved). One typed enum — the value itself carries
    # the escalation class. Required, no default: the operator states intent.
    execution_group.add_argument(
        "--execution-access-mode",
        choices=EXECUTION_ACCESS_MODES,
        required=True,
        dest="execution_access_mode",
        help="Execution access mode (required, no default): 'standard' normal adapter access; "
        "'agreed_direct' runs each target_run with its identity's direct_credential_source_key "
        "profile (account-id + principal checked, no chained roles; requires the ctl profile "
        "to allow it and every active target to set allow_agreed_direct_execution_access: "
        "true); 'force_bypass' runs every target_run with the --provider-credential substitute "
        "credential (identity cfg is not resolved and nothing is checked; requires the "
        "ctl profile to allow it and --provider-credential)",
    )
    # execution runtime (§Phase 26): WHERE CTL produces each target_run's clean box.
    execution_group.add_argument(
        "--execution-runtime-mode",
        choices=EXECUTION_RUNTIME_MODES,
        required=True,
        help="Execution runtime (required, no default): 'local' builds a fresh Docker "
        "box per target_run on this machine; 'ci' runs each target_run on the GitHub Actions "
        "runner (no Docker-in-Docker). Must be allowed by the ctl profile "
        "(allowed_execution_runtime_modes) and supported by every active target_run "
        "(step.yaml runtime.supported_execution_runtime_modes).",
    )
    # 3) execution params
    execution_group.add_argument(
        "--execution-params",
        dest="execution_param",
        action="append",
        default=[],
        type=parse_selector_arg,
        help="Execution param in key=value form; repeatable; lands in execution_context.params.*",
    )
    # 4) action
    selector_group.add_argument(
        "--action",
        required=True,
        choices=["provision", "plan", "destroy", "readonly"],
        help="Lifecycle action (provision|plan|destroy|readonly)",
    )
    # 5) run-type selector (--workflow / --fan-out / --target / ... and its
    #    run-specific siblings)
    if run_type == "workflow":
        _add_workflow_args(selector_group)
    elif run_type == "target":
        _add_target_args(selector_group)
    elif run_type == "maintenance":
        _add_maintenance_args(selector_group)
    elif run_type == "step_sequence":
        _add_step_sequence_args(selector_group)
    elif run_type == "fan_out":
        _add_fan_out_args(selector_group)
    else:
        raise RuntimeError(f"❌ unknown runner run_type {run_type!r}")
    # --reuse-committed parametrizes the actual run (reuse committed children vs
    # re-run), so it lives in the run group — not with the non-executing checks.
    if run_type in {"workflow", "fan_out"}:
        selector_group.add_argument(
            "--reuse-committed",
            default=None,
            type=str2bool,
            metavar="{true,false}",
            help="Explicit true/false (no default; required for a normal run, "
            "omit only with --status or --execution-identity-preflight-check-only): "
            "when true, reuse a workflow child's committed result (skip re-running "
            "it) only when its committed target instance is current, commit-pinned, "
            "clean, and matches the current source/cfg commits; when false, always re-run",
        )
    # 6) --agreed-* / --force-* overrides
    override_group.add_argument(
        "--agreed-defer-ctl-state-backend-sync",
        action="store_true",
        dest="agreed_defer_ctl_state_backend_sync",
        help="Agree to defer ctl-state publication while the selected namespace backend is "
        "absent during bootstrap; requires profile allow_agreed_defer_ctl_state_backend_sync "
        "and every active target to declare allow_agreed_defer_ctl_state_backend_sync: true",
    )
    override_group.add_argument(
        "--force-skip-ctl-state-backend-sync",
        action="store_true",
        dest="force_skip_ctl_state_backend_sync",
        help="Blanket override: skip ctl-state backend sync for EVERY active target, "
        "ignoring target keys; requires profile allow_force_skip_ctl_state_backend_sync",
    )
    override_group.add_argument(
        "--force-skip-guardrails",
        action="store_true",
        dest="force_skip_guardrails",
        help="Skip ctl + plt guardrail verification for this run; requires profile "
        "allow_force_skip_guardrails",
    )
    override_group.add_argument(
        "--force-skip-full-cfg-validation-gate",
        action="store_true",
        dest="force_skip_full_cfg_validation_gate",
        help="Keep the full cfg-validation report but do not let unrelated failed "
        "findings block this run; complete cfg structure and every selected-run "
        "dependency remain mandatory; requires profile "
        "allow_force_skip_full_cfg_validation_gate",
    )
    if run_type in {"workflow", "target", "fan_out"}:
        override_group.add_argument(
            "--force-skip-execution-identity-preflight-check",
            action="store_true",
            help="Resolve every selected execution identity but skip provider live checks: "
            "you accept the risk that any target fails MID-RUN on an incorrect "
            "execution identity that the preflight would have caught up front; "
            "requires ctl-profile authorization",
        )
    # 7) cfg variation
    if run_type in {"workflow", "fan_out"}:
        variation_group.add_argument(
            "--ctl-variants",
            required=False,
            default=[],
            dest="ctl_variants",
            type=parse_ctl_variants_arg,
            help="Optional comma-separated ctl variant paths under variants/",
        )
    variation_group.add_argument(
        "--plt-overlays",
        required=False,
        default=[],
        dest="plt_overlays",
        type=parse_overlays_arg,
        help="Optional comma-separated plt overlay names",
    )
    # 8) run modes
    if run_type == "fan_out":
        mode_group.add_argument(
            "--dry-run",
            action="store_true",
            help="print expanded child runner commands and exit",
        )
    # §Phase 50: status is no longer a mode on the run runners — it is the
    # standalone read-only status.py (its own slim parser). Removed here.
    if run_type in {"workflow", "target", "fan_out"}:
        mode_group.add_argument(
            "--execution-identity-preflight-check-only",
            action="store_true",
            help="Resolve and live-check every selected execution identity, write the "
            "preflight artifacts, and exit without state, guardrails, or target_runs",
        )
    # misc
    misc_group.add_argument(
        "--provider-credential",
        dest="provider_credential",
        default=None,
        help="Substitute provider credential (an opaque provider-specific selector, "
        "e.g. a local profile name); required with and only valid together with "
        "--execution-access-mode force_bypass",
    )
    # internal, engine-set (hidden from --help) — last
    parser.add_argument(
        "--parent-graph-provisions-ctl-state-backend",
        action="store_true",
        dest="parent_graph_provisions_ctl_state_backend",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--parent-ctl-state-backend-absence-confirmed",
        action="store_true",
        dest="parent_ctl_state_backend_absence_confirmed",
        help=argparse.SUPPRESS,
    )
    # Set by the fan-out runner on its child runs: the fan-out run id recorded
    # in child run metadata as the batch audit record (§Phase 31 — the fan-out
    # itself is stateless and owns no run history).
    parser.add_argument(
        "--parent-fan-out-run-id",
        dest="parent_fan_out_run_id",
        default=None,
        help=argparse.SUPPRESS,
    )

def redact_command_argv(argv: list[str]) -> list[str]:
    """Redact opaque credential selectors before command lines reach logs."""
    redacted: list[str] = []
    hide_next = False
    for value in argv:
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        if value == "--provider-credential":
            redacted.append(value)
            hide_next = True
            continue
        if value.startswith("--provider-credential="):
            redacted.append("--provider-credential=<redacted>")
            continue
        redacted.append(value)
    return redacted


def setup_logging() -> logging.handlers.MemoryHandler:
    """Setup logging with memory handler to capture early logs."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    memory_handler = logging.handlers.MemoryHandler(capacity=1000, flushLevel=logging.CRITICAL)
    logging.getLogger().addHandler(memory_handler)
    logging.info("Command: %s", " ".join(redact_command_argv(sys.argv)))
    return memory_handler


KNOWN_ACTIONS = ("provision", "plan", "destroy", "readonly")


def entry_actions(entry: dict, *, label: str) -> list[str]:
    """§Phase 33: the REQUIRED `actions:` allowlist on a target/workflow. A
    missing or empty list is a hard error — availability is default-CLOSED and
    must be explicit (never inferred from an optional selector)."""
    actions = entry.get("actions")
    if (
        not isinstance(actions, list)
        or not actions
        or not all(isinstance(a, str) and a in KNOWN_ACTIONS for a in actions)
        or len(set(actions)) != len(actions)
    ):
        raise RuntimeError(
            f"❌ {label} must declare 'actions': a non-empty, duplicate-free "
            f"subset of {list(KNOWN_ACTIONS)} (availability is default-closed)"
        )
    return actions


def load_workflow_cfg(
    ctl_cfg_root: Path,
    ctl_profile: str,
    inventory_name: str,
    workflow_name: str,
    execution_context: dict[str, object],
) -> dict:
    """Load a content-key workflow: `workflows.<name>` (imports + selectors).

    §Phase 33: workflows are declared ONCE with a required `actions:` allowlist;
    the action gates availability. `target_keys` may be members-shaped (dispatch
    by `execution_context.ctl.action`) when the apply-family composition differs
    per action. Expands `import_workflow_keys` (ordered, recursive) then the
    workflow's own `target_keys`; applies `selectors` (intersected through
    imports). The workflow name is an opaque key (slashes are cosmetic).
    """
    workflows = collect_resource(ctl_cfg_root, "workflows", entry_depth=1)
    if workflow_name not in workflows:
        raise RuntimeError(f"❌ workflow {workflow_name!r} not found")
    resolved_workflows: dict = {}
    for name, wf in workflows.items():
        if not isinstance(wf, dict):
            raise RuntimeError(f"❌ workflow {name!r} must be a mapping")
        if inventory_name not in entry_actions(wf, label=f"workflow {name!r}"):
            continue
        target_keys = wf.get("target_keys")
        if isinstance(target_keys, dict):
            target_keys = resolve_list_members(
                target_keys,
                execution_context,
                value_field="target_keys",
                label=f"workflow {name!r} target_keys",
            )
            if target_keys is None:
                raise RuntimeError(
                    f"❌ workflow {name!r} members-shaped target_keys did not "
                    "resolve for this execution context"
                )
            wf = {**wf, "target_keys": target_keys}
        resolved_workflows[name] = wf
    if workflow_name not in resolved_workflows:
        raise RuntimeError(
            f"❌ workflow {workflow_name!r} does not allow action {inventory_name!r}"
        )

    effective_selectors = workflow_effective_selectors(resolved_workflows, workflow_name)
    if not selector_matches(
        effective_selectors,
        execution_context,
        label=f"workflow {inventory_name}/{workflow_name}",
    ):
        raise RuntimeError(
            f"❌ workflow {inventory_name}/{workflow_name} is not available for "
            f"runtime selectors {execution_context} (selectors {effective_selectors})"
        )

    target_runs = expand_workflow_imports(resolved_workflows, workflow_name)
    return {
        "meta": {"name": f"{inventory_name}/{workflow_name}", "action": inventory_name},
        "target_runs": target_runs,
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



def get_workflow_target_run_id(target_run_entry) -> str:
    """Return the target_run id from a workflow target_run entry."""
    if isinstance(target_run_entry, str):
        return target_run_entry
    if isinstance(target_run_entry, dict):
        target_run_id = target_run_entry.get("id")
        if isinstance(target_run_id, str) and target_run_id:
            return target_run_id
    raise RuntimeError(f"❌ invalid workflow target_run entry: {target_run_entry!r}")


def validate_ctl_variant_target_run_patch_entry(raw_target_run: dict, variant_label: str) -> tuple[str, dict]:
    """Validate one ctl variant target_run patch entry and return its op plus target_run payload."""
    if not isinstance(raw_target_run, dict):
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' workflow patch entries must be mappings"
        )

    add_before = raw_target_run.get("add_before")
    add_after = raw_target_run.get("add_after")
    op_keys = [key for key, value in (("add_before", add_before), ("add_after", add_after)) if value is not None]
    if len(op_keys) != 1:
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' target_run patch entry must define exactly one of "
            f"'add_before' or 'add_after': {raw_target_run}"
        )

    anchor_target_run_id = raw_target_run[op_keys[0]]
    if not isinstance(anchor_target_run_id, str) or not anchor_target_run_id:
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' {op_keys[0]} value must be a non-empty target_run id"
        )

    target_run_entry = {k: v for k, v in raw_target_run.items() if k not in ("add_before", "add_after")}
    target_run_id = target_run_entry.get("id")
    target_key = target_run_entry.get("target")
    step_sequence_override = target_run_entry.get("workflow")
    if not isinstance(target_run_id, str) or not target_run_id:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' inserted target_run must define non-empty 'id'")
    if not isinstance(target_key, str) or not target_key:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' target_run '{target_run_id}' must define non-empty 'target'")
    if not isinstance(step_sequence_override, str) or not step_sequence_override:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' target_run '{target_run_id}' must define non-empty 'workflow'")
    if target_run_entry.get("branch") and target_run_entry.get("commit"):
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' target_run '{target_run_id}' cannot define both 'branch' and 'commit'"
        )

    return op_keys[0], target_run_entry


def apply_ctl_variant_workflow_patch(
    workflow_cfg: dict,
    patch_cfg: dict,
    *,
    variant_label: str,
    patch_label: str,
) -> dict:
    """Apply add_before/add_after workflow patch entries from one ctl variant patch file."""
    target_runs = workflow_cfg.get("target_runs")
    if not isinstance(target_runs, list):
        raise RuntimeError(f"❌ workflow cfg must contain a 'target_runs' list before applying ctl variants")

    patch_target_runs = patch_cfg.get("target_runs") or []
    if not isinstance(patch_target_runs, list):
        raise RuntimeError(
            f"❌ ctl variant patch '{patch_label}' must contain a 'target_runs' list"
        )

    resolved_target_runs = list(target_runs)
    for raw_target_run in patch_target_runs:
        op, target_run_entry = validate_ctl_variant_target_run_patch_entry(raw_target_run, variant_label)
        anchor_target_run_id = raw_target_run[op]
        target_run_id = target_run_entry["id"]

        target_run_ids = [get_workflow_target_run_id(target_run) for target_run in resolved_target_runs]
        if anchor_target_run_id not in target_run_ids:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' patch '{patch_label}' references missing anchor "
                f"target_run id '{anchor_target_run_id}'"
            )
        if target_run_id in target_run_ids:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' patch '{patch_label}' inserts duplicate target_run id '{target_run_id}'"
            )

        anchor_index = target_run_ids.index(anchor_target_run_id)
        insert_index = anchor_index if op == "add_before" else anchor_index + 1
        resolved_target_runs.insert(insert_index, target_run_entry)
        logging.info(
            "Applied ctl variant '%s': %s target_run '%s' %s '%s'",
            variant_label,
            op,
            target_run_id,
            "before" if op == "add_before" else "after",
            anchor_target_run_id,
        )

    patched_workflow_cfg = dict(workflow_cfg)
    patched_workflow_cfg["target_runs"] = resolved_target_runs
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
    targets = inventory_cfg.get("targets", {})
    target_runs = list(workflow_cfg.get("target_runs") or [])

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
        if anchor not in target_runs:
            logging.info("Variant '%s' anchor '%s' absent from '%s' — skipped", name, anchor, workflow_name)
            continue
        if target_name in target_runs:
            raise RuntimeError(f"❌ variant {name!r} inserts duplicate target {target_name!r}")
        idx = target_runs.index(anchor)
        target_runs.insert(idx if before else idx + 1, target_name)
        logging.info(
            "Applied variant '%s': inserted '%s' %s '%s'",
            name, target_name, "before" if before else "after", anchor,
        )

    patched = dict(workflow_cfg)
    patched["target_runs"] = target_runs
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
    if selector_group_is_group(cfg_file_set):
        raise RuntimeError(
            f"❌ cfg_file_set {cfg_file_set_key!r} is a selector group and cannot be "
            f"composed via cfg_file_set_keys (groups are target-level indirection only): {cfg_file_sets_path}"
        )
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


def load_inventory_cfg(
    ctl_cfg_root: Path,
    inventory_name: str,
    execution_context: dict[str, object] | None = None,
) -> dict:
    """Compose action cfg from target_sources + cfg_file_sets + targets/<action>/*.yaml.

    `inventory_name` is the action (provision/plan/destroy/readonly). Layout:
      - target_sources.yaml  source repos: source key -> meta
      - cfg_file_sets.yaml       config views: cfg-file-set key -> {cfg_root, cfg_file_set_keys, cfg_files}
      - targets/<action>/*.yaml  fat targets (the directory IS the action). Each
            file is a flat `targets:` map; all files for an action merge (duplicate
            names rejected). A target is self-contained:
              {source_key, ref_key, step_sequence_key, cfg_file_set_key,
               [execution_identity_key], [cfg_files], [selectors],
               [required_plt_overlay_keys]}.

    Returns the flat shape build_active_target_runs consumes ({target_sources,
    targets}), where each target carries source + cfg_root + cfg_files
    (resolved from its cfg_file_set_key) + step_sequence + execution identity requirement
    (+ selectors /
    requires_plt_overlays when present).
    """
    # global resources + targets are content-key (collected by top-level key)
    target_sources = collect_resource(ctl_cfg_root, "target_sources")
    cfg_file_sets = collect_resource(ctl_cfg_root, "cfg_file_sets")
    cfg_file_sets_path = ctl_cfg_root  # label for include/error messages
    if not target_sources:
        raise RuntimeError(f"❌ no 'target_sources' defined under: {ctl_cfg_root}")
    if not cfg_file_sets:
        raise RuntimeError(f"❌ no 'cfg_file_sets' defined under: {ctl_cfg_root}")

    # §Phase 33: targets are declared ONCE (no action level); each declares a
    # REQUIRED `actions:` allowlist (default-closed) and the inventory for a run
    # is the subset allowing this action.
    all_targets = collect_resource(ctl_cfg_root, "targets", entry_depth=1)
    targets = {}
    for target_name, target_def in all_targets.items():
        if not isinstance(target_def, dict):
            raise RuntimeError(f"❌ target {target_name!r} must be a mapping")
        if inventory_name in entry_actions(target_def, label=f"target {target_name!r}"):
            targets[target_name] = target_def
    if not targets:
        raise RuntimeError(f"❌ no targets allow action {inventory_name!r}")

    resolved_targets: dict = {}
    for target_name, target_def in targets.items():
        consumed_group_axes: set[str] = set()

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
        # §Phase 31 3c: a group entry resolves to one concrete cfg_file_set per
        # the frozen execution context (e.g. state_backend -> org). Without a
        # context (static/inventory-wide tools) the group stays unresolved; it
        # hard-errors only if such a target is actually materialized.
        if selector_group_is_group(cfg_file_set):
            if execution_context is None:
                cfg_file_set = {"cfg_root": None}
                cfg_file_set_name = None
            else:
                # A domain-specific target DECLARES its domain (`domain: env`);
                # a domain-generic target (e.g. tfstate_backend) takes it from the
                # execution context. If the axis isn't bound — a generic target in
                # a shared inventory that this run doesn't activate — resolution
                # is deferred (None) instead of failing an unrelated run; a
                # declared-domain target must still resolve.
                declared_domain = target_def.get("domain")
                group_context = execution_context
                if declared_domain is not None:
                    group_context = {
                        **execution_context,
                        f"{EXECUTION_CONTEXT_ROOT}.params.domain": str(declared_domain),
                    }
                # §Phase 32 guard input: the group's selector axes are axes this
                # target CONSUMES (unless satisfied by a declared domain).
                # collect_member_dispatch_axes also ENFORCES the per-axis rule
                # (params or ctl.action only) on every dispatch.
                group_axes = collect_member_dispatch_axes(
                    cfg_file_set.get("members"),
                    label=f"target {target_name!r} cfg_file_set group",
                )
                if declared_domain is None:
                    consumed_group_axes.update(group_axes)
                concrete_name = resolve_selector_group_member(
                    cfg_file_set, group_context,
                    value_field="cfg_file_set_key",
                    label=f"cfg_file_set group {cfg_file_set_name!r}",
                    tolerate_none=declared_domain is None,
                )
                if concrete_name is None:
                    cfg_file_set = {"cfg_root": None}
                    cfg_file_set_name = None
                else:
                    cfg_file_set = cfg_file_sets.get(concrete_name)
                    if not isinstance(cfg_file_set, dict) or selector_group_is_group(cfg_file_set):
                        raise RuntimeError(
                            f"❌ cfg_file_set group {cfg_file_set_name!r} member {concrete_name!r} must "
                            f"reference a concrete cfg_file_set (no nested groups): {cfg_file_sets_path}"
                        )
                    cfg_file_set_name = concrete_name

        step_sequence = target_def.get("step_sequence_key")
        if not isinstance(step_sequence, str) or not step_sequence:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'step_sequence_key'")

        # §Phase 33: per-action field variation via inline members dispatched by
        # execution_context.ctl.action. The resolved value may itself be a NAMED
        # group (e.g. env_readonly), resolved later by its own axis (two-stage).
        execution_identity_key = target_def.get("execution_identity_key")
        if isinstance(execution_identity_key, dict):
            consumed_group_axes.update(
                collect_member_dispatch_axes(
                    execution_identity_key.get("members"),
                    label=f"target {target_name!r} execution_identity_key members",
                )
            )
            if execution_context is None:
                execution_identity_key = None
            else:
                execution_identity_key = resolve_selector_group_member(
                    execution_identity_key,
                    execution_context,
                    value_field="identity_key",
                    label=f"target {target_name!r} execution_identity_key",
                )
        if execution_identity_key is not None and (
            not isinstance(execution_identity_key, str) or not execution_identity_key.strip()
        ):
            raise RuntimeError(
                f"❌ target {target_name!r} execution_identity_key must be a non-empty string"
            )

        extra_files = target_def.get("cfg_files", []) or []
        if isinstance(extra_files, dict):
            consumed_group_axes.update(
                collect_member_dispatch_axes(
                    extra_files.get("members"),
                    label=f"target {target_name!r} cfg_files members",
                )
            )
            extra_files = (
                resolve_list_members(
                    extra_files,
                    execution_context,
                    value_field="cfg_files",
                    label=f"target {target_name!r} cfg_files",
                    allow_empty=True,
                )
                or []
            )
        if not isinstance(extra_files, list):
            raise RuntimeError(f"❌ target {target_name!r} cfg_files must be a list")

        resolved = {
            "source": source,
            "ref": target_ref.strip(),
            "step_sequence": step_sequence,
            "cfg_root": cfg_file_set.get("cfg_root", "/"),
            "cfg_files": [
                *(
                    resolve_cfg_file_set_files(cfg_file_set_name, cfg_file_sets, cfg_file_sets_path)
                    if cfg_file_set_name is not None
                    else []
                ),
                *extra_files,
            ],
        }
        if cfg_file_set_name is None:
            # unresolved cfg_file_set group (no execution context at load time)
            resolved["cfg_file_set_group_unresolved"] = target_def.get("cfg_file_set_key")
        # §Phase 31: declared instance identity flows through to the resolved
        # target (consumed by resolve_run_instance_identity).
        # §Phase 32: a GENERIC target whose instance axes vary by another axis
        # dispatches its schema by `members` ({params: [...], selectors: {...}}),
        # the same pattern as its ref/cfg_file_set groups. Exactly one member
        # matches; an unbound dispatch axis defers (hard error only if the
        # target is actually activated in a run).
        instance_params = target_def.get("target_instance_params")
        if isinstance(instance_params, dict):
            # the dispatch axes of the schema itself are consumed axes too
            consumed_group_axes.update(
                collect_member_dispatch_axes(
                    instance_params.get("members"),
                    label=f"target {target_name!r} target_instance_params members",
                )
            )
            instance_params = _resolve_instance_params_members(
                instance_params, execution_context, target_name=target_name
            )
            if instance_params is None:
                resolved["target_instance_params_unresolved"] = True
        if consumed_group_axes:
            resolved["consumed_group_axes"] = sorted(consumed_group_axes)
        if instance_params is not None:
            if not isinstance(instance_params, list) or not all(
                isinstance(p, str) and p.strip() for p in instance_params
            ):
                raise RuntimeError(
                    f"❌ target {target_name!r} target_instance_params must be a list of non-empty strings"
                )
            resolved["target_instance_params"] = [p.strip() for p in instance_params]
        if execution_identity_key is not None:
            resolved["execution_identity_key"] = execution_identity_key.strip()
        if "provisions_ctl_state_bucket" in target_def:
            raise RuntimeError(
                f"❌ target {target_name!r} uses deprecated provisions_ctl_state_bucket; "
                "use provisions_ctl_state_backend"
            )
        if target_def.get("provisions_ctl_state_backend") is True:
            resolved["provisions_ctl_state_backend"] = True
        for legacy_flag in (  # removed keys
            "allow_skip_ctl_entry",
            "allow_skip_ctl_state_sync",
            "skip_ctl_role_chain",  # removed
            "execution_access_modes",
        ):
            if legacy_flag in target_def:
                raise RuntimeError(
                    f"❌ target {target_name!r} uses removed {legacy_flag}; "
                    "use allow_agreed_direct_execution_access (§12) for access, "
                    "allow_agreed_defer_ctl_state_backend_sync for deferred sync"
                )
        # Static policy: the target may participate in an explicitly agreed
        # deferred-sync bootstrap graph. Runtime agreement is a separate CLI fact.
        if "allow_agreed_defer_ctl_state_backend_sync" in target_def:
            value = target_def["allow_agreed_defer_ctl_state_backend_sync"]
            if value is not True:
                raise RuntimeError(
                    f"❌ target {target_name!r} allow_agreed_defer_ctl_state_backend_sync "
                    "must be literal true when present"
                )
            resolved["allow_agreed_defer_ctl_state_backend_sync"] = True
        # allow_agreed_direct_execution_access: does the target opt into DIRECT mode (§12); default False
        if "allow_agreed_direct_execution_access" in target_def:
            target_allows_agreed_direct_execution_access(target_def)  # validate
            resolved["allow_agreed_direct_execution_access"] = target_def["allow_agreed_direct_execution_access"]
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
        "target_sources": target_sources,
        "targets": resolved_targets,
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
    elif run_type == "step_sequence":
        if getattr(args, "workflow", None) or getattr(args, "target", None):
            raise RuntimeError("❌ step_sequence runner does not accept --workflow or --target")
        ref = getattr(args, "ref", None)
        ref_context = resolve_ref_context(ref, args.execution_params) if ref else "step_sequence"
        raw_name = f"{ref_context_to_result_path(ref_context)}/{getattr(args, 'source', None) or 'unknown'}/{getattr(args, 'step_sequence', None) or 'unknown'}"
    elif run_type == "maintenance":
        maintenance_target = getattr(args, "target", None) or getattr(args, "lock_id", None) or "unknown"
        raw_name = f"{getattr(args, 'maintenance_action', None) or 'maintenance'}/{maintenance_target}"
    elif run_type == "fan_out":
        raw_name = getattr(args, "fan_out", None)
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
    *,
    locator_segments: list[str],
    parent_fan_out_run_id: str | None = None,
    instance_segments: list[str] | None = None,
    instance_address: str | None = None,
    target_addresses: list[str] | None = None,
    identity_doc: dict | None = None,
    execution_access_mode: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Create run directories under the stable ctl result key and setup file logging.

    §Phase 31: results nest under the resolved ctl-state NAMESPACE tree
    (`_local` for stateless/synthetic runs), with the target/workflow instance
    layer between the key and `runs/`:
      <root>/<namespace>/<action>/<run_type>/<key>[/instances/<seg>...]/runs/<id>
    A parameterized instance writes its authoritative identity.yaml
    (manifest-first ordering, Q2) before any run content."""
    result_name = normalize_result_name(result_name, label="ctl result name")
    ctl_state_dir = Path(ctl_state_local_root).joinpath(*locator_segments) / action / run_type / result_name
    if instance_segments:
        ctl_state_dir = ctl_state_dir.joinpath("instances", *instance_segments)
    runs_dir = ctl_state_dir / "runs"
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    if instance_segments and identity_doc is not None:
        identity_path = ctl_state_dir / "identity.yaml"
        if not identity_path.exists():
            write_yaml_file(identity_path, identity_doc)
    logging.info(f"Using ctl_state_dir: {ctl_state_dir}")
    logging.info(f"Using run_dir: {run_dir}")

    # Materialize the pinned ctl target_run runtime once, up front — it is a run-scoped
    # (workspace-scoped) precondition, not a per-target_run step. Idempotent thereafter.
    step_utils_dir = materialize_step_utils(run_dir)
    logging.info(f"Using ctl target_run runtime: {step_utils_dir}")

    # artifacts/ splits into general/ (run-level validation reports + metadata)
    # and target_runs/<target_run>/ (per-target_run outputs, created when target_runs run).
    # Logs are a top-level run concern (run_dir/logs/), sibling of cfg/ — not buried
    # under artifacts/.
    artifacts_dir = run_dir / "artifacts" / "general"
    os.makedirs(artifacts_dir, exist_ok=True)

    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    os.makedirs(cfg_dir)

    target_sources_dir = run_dir / "target_sources"
    if target_sources_dir.exists():
        shutil.rmtree(target_sources_dir)

    plt_merged_dir = cfg_dir / "plt" / "merged"
    os.makedirs(plt_merged_dir)

    logs_dir = run_dir / "logs"
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
            "ctl_state_locator": list(locator_segments),
            "ctl_state_dir": str(ctl_state_dir),
            "run_dir": str(run_dir),
            "log_path": str(log_file),
            "target_keys": [],
            "mutation_started": False,
            # Degraded-mode audit: the access mode is persisted structurally (not
            # only in the logged command) so an audit of committed run records can
            # tell which runs ran profile-only/bypass. force_bypass == degraded.
            **({"execution_access_mode": execution_access_mode}
               if execution_access_mode else {}),
            # §Phase 31: instance identity + namespace facts of this run.
            **({"ctl_state_namespace": locator_segments[0]}
               if locator_segments and locator_segments[0] != LOCAL_ONLY_LOCATOR[0] else {}),
            **({"instance": list(instance_segments)} if instance_segments else {}),
            **({"instance_address": instance_address} if instance_address else {}),
            **({"target_addresses": list(target_addresses)} if target_addresses else {}),
            # §Phase 31 item 8: the stateless fan-out's batch audit record —
            # "these runs were one invocation" lives only in child metadata.
            **({"fan_out_run_id": parent_fan_out_run_id} if parent_fan_out_run_id else {}),
        },
    )

    logging.info(f"Using artifacts_dir: {artifacts_dir}")
    logging.info(f"Logging to: {log_file}")

    return run_dir, artifacts_dir, plt_merged_dir, log_file


def setup_run_workspace(run_dir: Path) -> Path:
    """Materialize the target_run runtime and mutable cfg workspace after preflight."""
    step_utils_dir = materialize_step_utils(run_dir)
    logging.info("Using ctl target_run runtime: %s", step_utils_dir)

    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    cfg_dir.mkdir(parents=True)

    target_sources_dir = run_dir / "target_sources"
    if target_sources_dir.exists():
        shutil.rmtree(target_sources_dir)

    plt_merged_dir = cfg_dir / "plt" / "merged"
    plt_merged_dir.mkdir(parents=True)
    return plt_merged_dir


def setup_preflight_run_dirs(
    run_id: str,
    action: str,
    run_type: str,
    result_name: str,
    ctl_state_local_root: Path,
    memory_handler: logging.handlers.MemoryHandler,
    *,
    locator_segments: list[str],
    check_only: bool = True,
    instance_segments: list[str] | None = None,
    instance_address: str | None = None,
    target_addresses: list[str] | None = None,
    identity_doc: dict | None = None,
    parent_fan_out_run_id: str | None = None,
    execution_access_mode: str | None = None,
) -> tuple[Path, Path, Path]:
    """Create a preflight result without target_run tooling or companion cfg."""
    result_name = normalize_result_name(result_name, label="ctl result name")
    ctl_state_dir = Path(ctl_state_local_root).joinpath(*locator_segments) / action / run_type / result_name
    if instance_segments:
        ctl_state_dir = ctl_state_dir.joinpath("instances", *instance_segments)
        if identity_doc is not None:
            identity_path = ctl_state_dir / "identity.yaml"
            if not identity_path.exists():
                ctl_state_dir.mkdir(parents=True, exist_ok=True)
                write_yaml_file(identity_path, identity_doc)
    run_dir = ctl_state_dir / "runs" / run_id
    artifacts_dir = run_dir / "artifacts" / "general"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    logs_run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        + "_"
        + uuid.uuid4().hex[:6]
    )
    log_file = logs_dir / f"{SERVICE_ID}_{logs_run_id}.log"
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
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
            "ctl_state_locator": list(locator_segments),
            "ctl_state_dir": str(ctl_state_dir),
            "run_dir": str(run_dir),
            "log_path": str(log_file),
            "target_keys": [],
            "mutation_started": False,
            "execution_identity_preflight_check_only": bool(check_only),
            # Degraded-mode audit (see setup_run_dirs): force_bypass == degraded.
            **({"execution_access_mode": execution_access_mode}
               if execution_access_mode else {}),
            # §Phase 31: instance identity + namespace facts of this run.
            **({"ctl_state_namespace": locator_segments[0]}
               if locator_segments and locator_segments[0] != LOCAL_ONLY_LOCATOR[0] else {}),
            **({"instance": list(instance_segments)} if instance_segments else {}),
            **({"instance_address": instance_address} if instance_address else {}),
            **({"target_addresses": list(target_addresses)} if target_addresses else {}),
            **({"fan_out_run_id": parent_fan_out_run_id} if parent_fan_out_run_id else {}),
        },
    )
    logging.info("Using preflight run_dir: %s", run_dir)
    logging.info("Using artifacts_dir: %s", artifacts_dir)
    return run_dir, artifacts_dir, log_file


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


def target_keys_from_active_target_runs(active_target_runs: dict) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for target_run in active_target_runs.values():
        target_key = target_run.get("target")
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
    # §consolidated: status is merged INTO RUN.yaml (was a separate STATUS.yaml).
    # RUN.yaml is written at start (in_progress) and updated with the outcome.
    return Path(run_dir) / RUN_METADATA_FILENAME


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


def cleanup_run_workspace(run_dir: Path) -> None:
    """Drop the run's target_sources workspace (the materialized repo checkout +
    .terraform provider cache). It is used only DURING the run and is fully
    reproducible from the pinned source_commit/cfg_source_commit in RUN.yaml;
    nothing reads it after the run. Called before ctl-state sync so run history
    never carries hundreds of MB of build cache."""
    ts = Path(run_dir) / "target_sources"
    if ts.exists():
        shutil.rmtree(ts, ignore_errors=True)


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
            "status_path": f"runs/{run_dir.name}/RUN.yaml",
            "artifacts_path": f"runs/{run_dir.name}/artifacts",
            "updated_at": slot_payload["updated_at"],
        },
    )


# ── §Phase 31 Q3/decision 20: committed publication. The multi-object
#    committed/ slot is replaced by ONE committed.yaml pointer at the instance
#    dir + an immutable snapshot.yaml under the run. Manifest-last ordering:
#    the snapshot is written before the pointer is published.
COMMITTED_POINTER_NAME = "committed.yaml"

# Facts denormalized onto committed.yaml so readers (outdate/status) need no
# second file open; the pointer is still the single publication object.
# Only facts NOT derivable from the pointer's own path/run_id: everything
# else (action, run_type, result name/key, instance segments/address,
# target_keys/addresses) is encoded in the instance dir path or duplicated in
# child_revisions — never denormalized into the pointer (§Phase 31 minimal files).
_COMMITTED_FACT_KEYS = (
    "child_revisions", "source_commit", "cfg_source_commit",
    "source_state", "ref_policy", "workflow_definition_sha256",
)


def committed_pointer_path(instance_dir: Path) -> Path:
    return Path(instance_dir) / COMMITTED_POINTER_NAME


def write_run_snapshot(run_dir: Path, payload: dict) -> str:
    """§consolidated: the run's RUN.yaml IS the snapshot — no separate
    snapshot.yaml. Write the frozen record (this run's payload) to RUN.yaml and
    return its sha256. Self-contained (does not assume write_current_status ran):
    RUN.yaml on disk always equals the hashed content, and the reuse-committed
    check re-reads RUN.yaml and verifies this digest. RUN.yaml must not change
    after commit or that verification breaks."""
    write_yaml_file(Path(run_dir) / RUN_METADATA_FILENAME, payload)
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def publish_committed_pointer(run_dir: Path, payload: dict) -> Path:
    """Publish the instance's committed.yaml pointer to this run's snapshot
    (§Phase 31). Writes the snapshot first (manifest-last), then the pointer
    with the denormalized facts + status readers need. The physical
    conditional write to the backend is the syncer's job; locally this is the
    authoritative record."""
    snapshot_sha = write_run_snapshot(run_dir, payload)
    run_id = Path(run_dir).name
    # snapshot key is derivable (runs/<run_id>/snapshot.yaml) — not stored.
    pointer = {
        "run_id": run_id,
        "snapshot_sha256": snapshot_sha,
        "committed_at": payload.get("updated_at") or utc_timestamp(),
        "status": payload.get("status", "ok"),
    }
    for key in _COMMITTED_FACT_KEYS:
        if payload.get(key) is not None:
            pointer[key] = payload[key]
    instance_dir = ctl_state_dir_from_run_dir(run_dir)
    write_yaml_file(committed_pointer_path(instance_dir), pointer)
    return committed_pointer_path(instance_dir)


def read_committed_pointer(instance_dir: Path) -> dict | None:
    path = committed_pointer_path(instance_dir)
    if not path.is_file():
        return None
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ committed.yaml must be a mapping: {path}")
    return data


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


def mark_mutation_started(run_dir: Path, target_run_id: str) -> None:
    metadata = update_run_metadata(
        run_dir,
        {
            "mutation_started": True,
            "mutation_started_at": utc_timestamp(),
            "mutation_target_run_id": target_run_id,
        },
    )
    status = load_current_status(run_dir)
    if status:
        status.update(
            {
                "mutation_started": True,
                "mutation_started_at": metadata["mutation_started_at"],
                "mutation_target_run_id": target_run_id,
                "updated_at": utc_timestamp(),
            }
        )
        write_current_status(run_dir, status)
        if status.get("status") == "in_progress":
            rewrite_in_progress_slot_if_present(run_dir, status)
    ctl_state_push(f"mutation started ({target_run_id})")


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
    if payload.get("mutation_target_run_id"):
        print(f"target_run: {payload['mutation_target_run_id']}", file=sys.stderr)
    if error.get("summary"):
        print(f"error: {error['summary']}", file=sys.stderr)
    if payload.get("log_path"):
        print(f"log: {payload['log_path']}", file=sys.stderr)


def mark_run_succeeded(run_dir: Path) -> None:
    payload = build_status_payload(run_dir, "ok", {"ctl_state_sync": ctl_state_sync_summary()})
    write_current_status(run_dir, payload)
    pointer_path = publish_committed_pointer(run_dir, payload)
    remove_state_slot(run_dir, "in_progress")
    remove_state_slot(run_dir, "failed")
    mark_outdated_for_run(run_dir, include_current_result=False)
    metadata = load_run_metadata(run_dir)
    cleanup_run_workspace(run_dir)
    publish_or_queue_ctl_state_run(
        run_dir,
        pointer_path,
        reason="run succeeded",
        dependencies=list(metadata.get("target_addresses") or []),
    )
    release_mutation_lock_if_held()


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
    cleanup_run_workspace(run_dir)
    publish_or_queue_ctl_state_run(run_dir, None, reason="run failed")
    release_mutation_lock_if_held()
    print_failure_summary(payload)


def parse_result_dir(ctl_state_local_root: Path, result_dir: Path) -> dict | None:
    """Parse a result dir path into its identity (§Phase 30 locator-aware).

    Layout: <root>/<locator...>/<action>/<run_type>/<result_name...>. The
    locator prefix has variable depth, so the boundary is found by scanning for
    the first <known action>/<known run_type> pair."""
    try:
        rel = Path(result_dir).resolve().relative_to(Path(ctl_state_local_root).resolve())
    except ValueError:
        return None
    parts = rel.parts
    for index in range(len(parts) - 2):
        if parts[index] in RUN_ACTIONS and parts[index + 1] in RUN_TYPES:
            action, run_type = parts[index], parts[index + 1]
            rest = list(parts[index + 2:])
            # §Phase 31: strip the optional instance layer from the key.
            instance_segments: list[str] = []
            if "instances" in rest:
                marker = rest.index("instances")
                after = rest[marker + 1:]
                if after and after[0].startswith("sha256-"):
                    instance_segments = [after[0]]
                else:
                    instance_segments, _ = split_instance_segments(after)
                rest = rest[:marker]
            result_name = "/".join(rest)
            if not result_name:
                return None
            address = result_name + (
                "/" + "/".join(instance_segments) if instance_segments else ""
            )
            return {
                "locator": list(parts[:index]),
                "action": action,
                "run_type": run_type,
                "result_name": result_name,
                "result_key": f"{action}/{run_type}/{result_name}",
                "instance": instance_segments,
                "address": address,
            }
    return None


def iter_committed_status_paths(ctl_state_local_root: Path):
    root = Path(ctl_state_local_root)
    if not root.is_dir():
        return
    # §Phase 31: the committed record is the committed.yaml pointer at the
    # instance dir (was committed/STATUS.yaml).
    yield from sorted(root.rglob(COMMITTED_POINTER_NAME))


def load_status_mapping(path: Path) -> dict:
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ STATUS.yaml must contain a mapping: {path}")
    return data


def status_result_info(ctl_state_local_root: Path, status_path: Path, status: dict) -> dict | None:
    # §Phase 31: committed.yaml lives directly in the instance dir (its parent),
    # unlike the old committed/STATUS.yaml (parent.parent).
    result_dir = status_path.parent
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
        "status_path": payload.get("status_path") or (f"runs/{payload.get('run_id')}/RUN.yaml" if payload.get("run_id") else None),
        "artifacts_path": payload.get("artifacts_path") or (f"runs/{payload.get('run_id')}/artifacts" if payload.get("run_id") else None),
        "updated_at": payload.get("updated_at"),
    }
    write_yaml_file(manifest_path, {k: v for k, v in manifest.items() if v is not None})


def mark_committed_status_outdated(status_path: Path, status: dict, *, reason: str, caused_by: dict | None = None) -> None:
    # §Phase 31: the outdate marker is written onto the committed.yaml pointer
    # itself (the target-instance's committed record, Q1c) — no separate slot.
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

    # §Phase 31: a mutation outdates results in ITS OWN namespace tree only,
    # and only for the SAME target-instance addresses (Q1c: sibling ACTIONS of
    # one instance, never sibling instances — dev's mutation must not touch
    # test's results even though the target keys match).
    locator = metadata.get("ctl_state_locator") or []
    scan_root = Path(ctl_state_local_root).joinpath(*locator)
    affected_addresses = {
        a for a in (metadata.get("target_addresses") or []) if isinstance(a, str)
    }
    run_instance = metadata.get("instance") or []

    for status_path in iter_committed_status_paths(scan_root):
        status = load_status_mapping(status_path)
        info = status_result_info(Path(ctl_state_local_root), status_path, status)
        if info is None:
            continue
        if info.get("action") == "readonly":
            continue
        if not include_current_result and info.get("result_key") == current_result_key:
            continue
        if affected_addresses:
            # instance-aware matching: a candidate target result matches when
            # its own address is affected; a candidate workflow result matches
            # when it shares this run's instance (its own sibling actions).
            candidate_address = info.get("address")
            candidate_instance = info.get("instance") or []
            candidate_is_run_sibling = (
                info.get("result_name") == metadata.get("result_name")
                and candidate_instance == run_instance
            )
            if candidate_address not in affected_addresses and not candidate_is_run_sibling:
                continue
            # §Phase 50.9: never outdate a result THIS run graph just committed
            # on its OWN action. A workflow provision commits its child target
            # provision pointers, then sweeps — without this guard it re-marks
            # its own fresh output stale (the child's own earlier sweep had
            # protected that pointer, but only via its own result_key, which the
            # workflow-level sweep does not match). Cross-action supersession
            # (this provision outdating the sibling DESTROY pointer) still fires,
            # because that pointer's action differs from this run's action.
            if (
                not include_current_result
                and info.get("action") == action
                and candidate_address in affected_addresses
            ):
                continue
        else:
            # legacy metadata without addresses: match by target-key overlap
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
        workflows = collect_resource(ctl_cfg_root, "workflows", entry_depth=1)
    except Exception as exc:
        logging.warning("Skipping definition_removed scan: failed to load workflows: %s", exc)
        workflows = {}
    try:
        targets = collect_resource(ctl_cfg_root, "targets", entry_depth=1)
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
            entry = workflows.get(result_name)
            exists = isinstance(entry, dict) and action in (entry.get("actions") or [])
        elif run_type == "target":
            entry = targets.get(result_name)
            exists = isinstance(entry, dict) and action in (entry.get("actions") or [])
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


def force_unlock_resource_kind(target_key: str | None) -> str:
    """Return the resource kind selected by the optional maintenance target."""
    return "terraform" if target_key else "ctl_state"


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
EXECUTION_CONTEXT_PARAMS_PREFIX = f"{EXECUTION_CONTEXT_ROOT}.params."
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


def reject_duplicate_selectors(selectors_by_key: dict[str, dict | None], *, label: str) -> None:
    """Load-time guard for structures where selectors pick EXACTLY ONE entry
    (namespaces, selector groups): reject two entries whose selectors are
    byte-identical. This catches literal duplicates before any run, instead of
    waiting for the resolve-time 'matched 2' error on the first context that
    happens to hit both. It does NOT detect general selector OVERLAP (that is a
    predicate-disjointness problem left to the resolve-time exactly-one guard)."""
    seen: dict[str, str] = {}
    for key, selectors in selectors_by_key.items():
        requirements = selector_requirements(
            selectors, label=f"{label}.{key}", structured_only=True
        )
        canonical = json.dumps(
            {ref: sorted(vals) for ref, vals in requirements.items()}, sort_keys=True
        )
        if canonical in seen:
            raise RuntimeError(
                f"❌ {label}: {key!r} and {seen[canonical]!r} have identical selectors "
                f"— they can never resolve to exactly one; make them distinct"
            )
        seen[canonical] = key


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
    error here. The miss is logged at DEBUG (a gated-inactive member is normal
    flow, so it must not flood INFO) with the available keys, so a typo'd
    execution input is still self-evident under --debug.
    """
    requirements = selector_requirements(selectors, label=label, structured_only=structured_only)
    for ref, allowed_values in requirements.items():
        if ref not in execution_context:
            logging.debug("Selector %s: %s", label, execution_context_miss_message(execution_context, ref))
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
    execution_access_mode: str = "standard",
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_runtime_mode: str,
    force_skip_execution_identity_preflight_check: bool = False,
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
    put("ctl", "execution_access_mode", execution_access_mode, label="promoted execution access mode")
    put("ctl", "agreed_defer_ctl_state_backend_sync", bool(agreed_defer_ctl_state_backend_sync), label="promoted --agreed-defer-ctl-state-backend-sync")
    put("ctl", "force_skip_ctl_state_backend_sync", bool(force_skip_ctl_state_backend_sync), label="promoted --force-skip-ctl-state-backend-sync")
    put("ctl", "force_skip_guardrails", bool(force_skip_guardrails), label="promoted --force-skip-guardrails")
    put(
        "ctl",
        "force_skip_full_cfg_validation_gate",
        bool(force_skip_full_cfg_validation_gate),
        label="promoted --force-skip-full-cfg-validation-gate",
    )
    put(
        "ctl",
        "force_skip_execution_identity_preflight_check",
        bool(force_skip_execution_identity_preflight_check),
        label="promoted --force-skip-execution-identity-preflight-check",
    )
    put("ctl", "execution_runtime_mode", execution_runtime_mode, label="promoted execution runtime")

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




def rendered_scope_target_dir(plt_rendered_dir: Path, target_path: str) -> Path:
    target_rel = target_path.lstrip("/")
    target_dir = (plt_rendered_dir / target_rel).resolve()
    try:
        target_dir.relative_to(plt_rendered_dir.resolve())
    except ValueError as exc:
        raise RuntimeError(f"Scope target_path escapes rendered cfg dir: {target_path}") from exc
    return target_dir


def required_target_paths_for_target_runs(active_target_runs: dict) -> set[str] | None:
    """Return the top-level PLT target paths consumed by target runs."""
    paths: set[str] = set()
    for target_run in active_target_runs.values():
        cfg_root = str(target_run.get("cfg_root") or "/")
        segments = [part for part in cfg_root.split("/") if part]
        if not segments:
            return None
        paths.add(f"/{segments[0]}")
    return paths


def verify_ctl_guardrails(
    ctl_cfg_root: Path,
    guardrails_cfg_root: Path,
    execution_context: dict[str, object],
) -> None:
    from utils import guardrails

    guardrails.verify_ctl_guardrails(
        ctl_cfg_root,
        guardrails_cfg_root,
        execution_context,
    )


def verify_plt_guardrails(
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
    required_target_paths: set[str] | None = None,
) -> None:
    from utils import guardrails

    guardrails.verify_plt_guardrails(
        plt_cfg_root,
        plt_cfg_root,
        guardrails_cfg_root,
        plt_rendered_dir,
        execution_context,
        scope_params,
        required_target_paths,
    )


def verify_guardrails(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
    required_target_paths: set[str] | None = None,
) -> None:
    if execution_context.get(
        f"{EXECUTION_CONTEXT_ROOT}.ctl.force_skip_guardrails"
    ):
        logging.info("guardrails: force-skipped")
        return
    verify_ctl_guardrails(
        ctl_cfg_root,
        guardrails_cfg_root,
        execution_context,
    )
    logging.info("ctl guardrails: passed")
    from utils import guardrails

    guardrails.verify_plt_guardrails(
        ctl_cfg_root,
        plt_cfg_root,
        guardrails_cfg_root,
        plt_rendered_dir,
        execution_context,
        scope_params,
        required_target_paths,
    )
    logging.info("plt guardrails: passed")


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


def load_scope_composition(plt_cfg_root: Path) -> dict[str, dict]:
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

    rules: dict[str, dict] = {}
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
        prefixes = []
        for raw_scope in raw_scopes:
            prefix = normalize_cfg_absolute_path(raw_scope, label=f"{label}.scopes")
            if prefix in prefixes:
                raise RuntimeError(f"❌ duplicate scope composition prefix {prefix!r}: {label}")
            prefixes.append(prefix)
        rules[target_path] = {
            "scopes": tuple(prefixes),
        }
    return rules


def validate_scope_composition(active_scopes: list[dict], composition: dict[str, dict]) -> None:
    target_scopes: dict[str, list[dict]] = collections.defaultdict(list)
    for scope in active_scopes:
        target_scopes[scope["target_path"]].append(scope)

    for target_path, scopes in target_scopes.items():
        rule = composition.get(target_path)
        if rule is None:
            if len(scopes) > 1:
                rendered = ", ".join(str(scope["meta_path"]) for scope in scopes)
                raise RuntimeError(f"Duplicate active cfg target_path {target_path!r}: {rendered}")
            continue

        prefixes = rule["scopes"]
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
    paths merge (selective merge: a run composes only the cfg its target_runs
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
                    "Skipping cfg scope %s -> %s (not consumed by this run's target_runs)",
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
    target_repo_key: str = "repo_url",
    require_target_ref: bool = True,
    require_commit_refs: bool = False,
    refs: dict | None = None,
    active_target_runs: dict | None = None,
) -> tuple[dict, Path]:
    """
    Merge config dirs, build active target_runs, and write pipeline_run_cfg.

    Returns:
        tuple: (active_target_runs, pipeline_run_cfg_path)
    """
    source_log_roots = (plt_cfg_root.resolve(),)
    dest_log_roots = (plt_merged_dir.parent.parent.resolve(),)

    # Resolve active target_runs first (needs no plt cfg), so the merge composes only
    # the scopes this run's target_runs consume (selective merge by cfg_root).
    if active_target_runs is None:
        active_target_runs = build_active_target_runs(
            workflow_cfg,
            inventory_cfg,
            repo_key=target_repo_key,
            require_branch_or_commit=require_target_ref,
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
        required_target_paths=required_target_paths_for_target_runs(active_target_runs),
    )

    write_target_run_flow_artifact(
        artifacts_dir / "resolved_target_runs_flow.yaml",
        workflow_cfg.get("meta"),
        active_target_runs,
    )

    # create and write pipeline_run_cfg
    pipeline_run_cfg = {
        "meta": workflow_cfg.get("meta"),
        "target_runs": active_target_runs
    }
    pipeline_run_cfg_path = artifacts_dir / "pipeline_run_cfg.yaml"
    with pipeline_run_cfg_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(pipeline_run_cfg, f, sort_keys=False)

    return active_target_runs, pipeline_run_cfg_path


def write_target_run_flow_artifact(path: Path, workflow_meta: dict | None, active_target_runs: dict) -> None:
    """Write a compact ordered target_run-flow artifact."""
    target_run_flow = {
        "meta": workflow_meta,
        "target_runs": [
            {
                "id": target_run_id,
                "target": target_run.get("target"),
                "source": target_run.get("source"),
                "workflow": target_run.get("workflow"),
                "execution_identity_key": target_run.get("execution_identity_key"),
                "branch": target_run.get("branch"),
                "commit": target_run.get("commit"),
            }
            for target_run_id, target_run in active_target_runs.items()
        ],
    }
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(target_run_flow, f, sort_keys=False)


def write_target_flow_artifact(
    ctl_cfg_root: Path,
    artifacts_dir: Path,
    *,
    ctl_profile: str,
    execution_context: dict[str, object],
    inventory_name: str,
    workflow_name: str | None,
    ctl_variants: list[str],
    plt_overlays: list[str],
    target_repo_key: str,
    require_target_ref: bool,
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
        target_inventory_cfg = load_inventory_cfg(ctl_cfg_root, target_inventory_name, execution_context)
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
        target_active_target_runs = build_active_target_runs(
            target_workflow_cfg,
            target_inventory_cfg,
            repo_key=target_repo_key,
            require_branch_or_commit=require_target_ref,
            refs=refs,
            execution_context=execution_context,
            require_commit_refs=require_commit_refs,
        )
    except Exception as exc:
        logging.warning(
            "Skipping target_runs_by_key_flow.yaml generation for plan/%s: %s",
            workflow_name,
            exc,
        )
        return

    write_target_run_flow_artifact(
        artifacts_dir / "target_runs_by_key_flow.yaml",
        target_workflow_cfg.get("meta"),
        target_active_target_runs,
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
    active_target_runs: dict,
    refs: dict,
    execution_context: dict[str, object],
) -> Path:
    """Write a resolved snapshot of the ctl cfg that drove the run to
    run_dir/cfg/ctl/, so the run is self-describing next to cfg/plt/. Vars are
    resolved against the execution context; active_target_runs is already resolved."""
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
    write_yaml_file(ctl_dir / "active_target_runs.yaml", active_target_runs)
    write_yaml_file(ctl_dir / "refs.yaml", resolve_ctl_structure(refs, execution_context, label="refs"))
    logging.info("Wrote resolved ctl cfg snapshot: %s", ctl_dir)
    return ctl_dir


def write_git_metas(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    artifacts_dir: Path,
) -> None:
    """Write ctl, plt, guardrail, and orchestrator git metadata."""
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
    write_git_meta_to_file(
        git_dir=guardrails_cfg_root,
        dest_dir=artifacts_dir,
        filename="guardrails_cfg_git_meta.yaml",
        generator=SERVICE_ID,
    )


# ---------------------------------------------------------------------------
# Ctl-state sync: mirror the local ctl-state namespace tree to its backend.
# S3 bucket. Local-first mechanics, remote system of record after final push.
# ---------------------------------------------------------------------------

def load_ctl_state_backends_cfg(ctl_cfg_root: Path) -> dict | None:
    """Load the optional ctl-state backend registry.

    Schema is ``ctl_state_backends``:
    {namespace: {selectors, provider, backend_type, bucket_name,
    bucket_region, execution_identity_keys}}.
    """
    merged: dict = {}
    seen_sources: dict[str, Path] = {}
    section_name = "ctl_state_backends"
    entries = list(collect_top_level_sections(ctl_cfg_root, section_name))
    for path, section in entries:
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ {section_name} must be a mapping: {path}")
        for namespace_key, entry in section.items():
            # Namespaces are consumer-defined vocabulary (the engine stays cfg-shape
            # agnostic): any non-empty snake_case key is a valid state namespace.
            if not isinstance(namespace_key, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", namespace_key):
                raise RuntimeError(f"❌ {section_name} namespace must be a snake_case key: {namespace_key!r} in {path}")
            if namespace_key in merged:
                raise RuntimeError(f"❌ duplicate {section_name} namespace {namespace_key!r}: {path} (first: {seen_sources[namespace_key]})")
            if not isinstance(entry, dict):
                raise RuntimeError(f"❌ {section_name}.{namespace_key} must be a mapping: {path}")
            allowed = {"provider", "backend_type", "bucket_name", "bucket_region", "execution_identity_keys", "selectors"}
            unknown = set(entry) - allowed
            if unknown:
                raise RuntimeError(f"❌ {section_name}.{namespace_key} has unsupported keys {sorted(unknown)}: {path}")
            provider = entry.get("provider")
            backend_type = entry.get("backend_type")
            for field, value in (("provider", provider), ("backend_type", backend_type)):
                if not isinstance(value, str) or not value.strip():
                    raise RuntimeError(f"❌ {section_name}.{namespace_key}.{field} must be a non-empty string: {path}")
            for field in ("bucket_name", "bucket_region"):
                if not isinstance(entry.get(field), str) or not entry[field].strip():
                    raise RuntimeError(f"❌ {section_name}.{namespace_key}.{field} must be a non-empty string: {path}")
            resolved = {
                "provider": provider.strip(),
                "backend_type": backend_type.strip(),
                "bucket_name": entry["bucket_name"].strip(),
                "bucket_region": entry["bucket_region"].strip(),
            }
            identity_keys = entry.get("execution_identity_keys")
            if identity_keys is not None:
                # §Phase 31 Q5: per-operation access identities (least privilege)
                if not isinstance(identity_keys, dict) or set(identity_keys) - {"read", "sync", "maintenance"}:
                    raise RuntimeError(
                        f"❌ {section_name}.{namespace_key}.execution_identity_keys must be a map with "
                        f"read/sync/maintenance keys only: {path}"
                    )
                cleaned: dict[str, str] = {}
                for op, key in identity_keys.items():
                    if not isinstance(key, str) or not key.strip():
                        raise RuntimeError(
                            f"❌ {section_name}.{namespace_key}.execution_identity_keys.{op} must be a non-empty string: {path}"
                        )
                    cleaned[op] = key.strip()
                resolved["execution_identity_keys"] = cleaned
            selectors = entry.get("selectors")
            if selectors is not None:
                # §Phase 31: a backend entry IS the namespace — its selectors
                # resolve exactly one entry per invocation (item 13c collapse).
                selector_requirements(
                    selectors, label=f"{section_name}.{namespace_key}.selectors", structured_only=True
                )
                resolved["selectors"] = selectors
            merged[namespace_key] = resolved
            seen_sources[namespace_key] = path
    # §Phase 31: namespaces resolve exactly one entry by selectors — reject
    # byte-identical selectors at load (before the resolve-time exactly-one guard).
    reject_duplicate_selectors(
        {k: v.get("selectors") for k, v in merged.items() if v.get("selectors") is not None},
        label=section_name,
    )
    return merged or None


def require_unique_fan_out_namespace(
    ctl_cfg_root: Path,
    children: list[dict],
    *,
    action: str,
    ctl_profile: str,
    execution_params: dict[str, str],
    execution_runtime_mode: str,
) -> str:
    """§Phase 31 item 3: a fan-out first expands, then resolves the namespace
    for EVERY child execution context and requires the unique set to contain
    exactly one member. Cross-namespace expansions are hard errors and must be
    partitioned into separate invocations. The fan-out runner never names or
    interprets selector parameters — it only compares resolved keys."""
    namespace_by_child: dict[str, str] = {}
    for child in children:
        child_params = dict(execution_params)
        child_params.update(child["params"])
        child_context = build_execution_context(
            ctl_cfg_root,
            action=action,
            ctl_profile=ctl_profile,
            execution_params=child_params,
            execution_runtime_mode=execution_runtime_mode,
        )
        namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, child_context)
        namespace_by_child[child["label"]] = namespace_key
    unique = sorted(set(namespace_by_child.values()))
    if len(unique) != 1:
        detail = ", ".join(f"{label} -> {ns}" for label, ns in sorted(namespace_by_child.items()))
        raise RuntimeError(
            f"❌ fan-out children resolve {len(unique)} ctl-state namespaces ({detail}); "
            "one invocation must not cross namespaces — partition the fan-out"
        )
    return unique[0]


def resolve_ctl_state_namespace(
    ctl_cfg_root: Path, execution_context: dict[str, object]
) -> tuple[str, dict]:
    """Resolve EXACTLY ONE ctl-state namespace from the frozen execution
    context (§Phase 31 item 3). A namespace IS a ctl_state_backends entry (item
    13c collapse): its `selectors` select it. Zero or multiple matches are hard
    errors; the selection is immutable for the whole top-level invocation and is
    recorded in run metadata by the caller. Returns (namespace_key,
    backend_entry)."""
    backends = load_ctl_state_backends_cfg(ctl_cfg_root) or {}
    if not backends:
        raise RuntimeError(f"❌ no 'ctl_state_backends' defined under: {ctl_cfg_root}")
    matches = [
        key for key, entry in backends.items()
        if entry.get("selectors") is not None
        and selector_matches(
            entry.get("selectors"), execution_context,
            label=f"ctl_state_backends.{key}.selectors", structured_only=True,
        )
    ]
    if len(matches) != 1:
        selectable = sorted(k for k, e in backends.items() if e.get("selectors") is not None)
        raise RuntimeError(
            f"❌ exactly one ctl-state namespace (backend with selectors) must match the "
            f"execution context, matched {len(matches)} of {selectable}"
        )
    return matches[0], backends[matches[0]]



_CTL_STATE_SYNCER = None
_CTL_STATE_SYNC_NOTE: dict[str, str] = {"mode": "disabled"}
_CTL_STATE_DEFER_CONFIG: dict | None = None
_CTL_STATE_SYNC_CONFIG: dict | None = None


def inspect_selected_graph_ctl_state_backend(
    selections: list[dict],
    ctl_cfg_root: Path,
    *,
    implementation_key: str,
    execution_access_mode: str,
    provider_credential: str | None,
) -> dict[str, object]:
    """Find the one backend provisioner and classify the selected backend."""
    provisioners: list[tuple[dict, str, dict]] = []
    for selection in selections:
        for target_run_id, target_run in selection["active_target_runs"].items():
            if target_run.get("provisions_ctl_state_backend") is True:
                provisioners.append((selection, target_run_id, target_run))
    if len(provisioners) != 1:
        raise RuntimeError(
            "❌ agreed ctl-state defer requires exactly one backend provisioner "
            f"in the complete selected graph; found {len(provisioners)}"
        )

    selection, target_run_id, target_run = provisioners[0]
    namespace_key, entry = resolve_ctl_state_namespace(
        ctl_cfg_root, selection["execution_context"]
    )
    adapter = get_provider_adapter(entry["provider"])
    adapter.validate_state_backend_entry(namespace_key, entry, ctl_cfg_root)
    bucket_name = str(
        resolve_runtime_scalar(
            entry["bucket_name"],
            selection["execution_context"],
            label=f"ctl_state_backends.{namespace_key}.bucket_name",
        )
    )
    bucket_region = str(entry["bucket_region"])
    credential = adapter.resolve_state_backend_probe_credential(
        target_run,
        selection["provider_catalogs"],
        execution_context=selection["execution_context"],
        implementation_key=implementation_key,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
    )
    probe = adapter.probe_state_backend(bucket_name, bucket_region, credential)
    status = probe.get("status")
    if status not in {"ready", "absent"}:
        raise RuntimeError(
            f"❌ ctl-state backend readiness probe for {namespace_key!r} "
            f"returned {status!r}: {probe.get('detail') or 'no detail'}"
        )
    return {
        "namespace": namespace_key,
        "bucket_name": bucket_name,
        "bucket_region": bucket_region,
        "provisioner_target_run_id": target_run_id,
        "status": status,
        "detail": probe.get("detail"),
    }


def _ctl_state_sync_config(
    ctl_cfg_root: Path,
    namespace_key: str,
    entry: dict,
    execution_context: dict[str, object],
    run_dir: Path,
    *,
    execution_access_mode: str,
    provider_credential: str | None,
    provider_implementation_key: str,
) -> dict:
    metadata = load_run_metadata(run_dir)
    results_root_value = metadata.get("ctl_state_local_root")
    if not isinstance(results_root_value, str) or not results_root_value:
        raise RuntimeError("❌ run metadata is missing ctl_state_local_root")
    locator = [namespace_key]
    if metadata.get("ctl_state_locator") != locator:
        raise RuntimeError(
            f"❌ run dirs use locator {metadata.get('ctl_state_locator')!r}, "
            f"but ctl-state namespace resolves to {locator!r}"
        )
    bucket_name = str(
        resolve_runtime_scalar(
            entry["bucket_name"],
            execution_context,
            label=f"ctl_state_backends.{namespace_key}.bucket_name",
        )
    )
    return {
        "ctl_cfg_root": Path(ctl_cfg_root),
        "namespace_key": namespace_key,
        "entry": entry,
        "execution_context": execution_context,
        "run_dir": Path(run_dir),
        "results_root": Path(results_root_value).joinpath(*locator),
        "bucket_name": bucket_name,
        "bucket_region": str(entry["bucket_region"]),
        "execution_access_mode": execution_access_mode,
        "provider_credential": provider_credential,
        "provider_implementation_key": provider_implementation_key,
    }


def _ctl_state_run_access_scope(config: dict) -> tuple[list[str], list[str]]:
    """Return exact object keys and immutable run prefixes for one publication."""
    results_root = Path(config["results_root"]).resolve()
    run_dir = Path(config["run_dir"]).resolve()
    run_prefix = run_dir.relative_to(results_root).as_posix()
    instance_dir = ctl_state_dir_from_run_dir(run_dir).resolve()
    instance_prefix = instance_dir.relative_to(results_root).as_posix()
    keys = [
        f"{instance_prefix}/identity.yaml",
        f"{instance_prefix}/committed.yaml",
    ]
    metadata = load_run_metadata(run_dir)
    action = str(metadata.get("action") or "")
    for address in metadata.get("target_addresses") or []:
        child_prefix = ctl_state_target_address_prefix(action, str(address))
        keys.extend(
            [f"{child_prefix}/identity.yaml", f"{child_prefix}/committed.yaml"]
        )
    return sorted(set(keys)), [run_prefix]


def _arm_ctl_state_sync(config: dict, *, tolerate_not_ready: bool) -> bool:
    global _CTL_STATE_SYNCER, _CTL_STATE_SYNC_NOTE
    entry = config["entry"]
    adapter = get_provider_adapter(entry["provider"])
    identity_key = (entry.get("execution_identity_keys") or {}).get("sync")
    if identity_key is None and config["execution_access_mode"] != "force_bypass":
        raise RuntimeError(
            f"❌ ctl_state_backends.{config['namespace_key']} declares no "
            "execution_identity_keys.sync"
        )
    if identity_key is not None:
        identity_key = str(
            resolve_runtime_scalar(
                identity_key,
                config["execution_context"],
                label=(
                    f"ctl_state_backends.{config['namespace_key']}."
                    "execution_identity_keys.sync"
                ),
            )
        )
    # Bootstrap target access may be direct, but ctl-state publication switches
    # to its normal role path as soon as the access role exists.
    sync_access_mode = (
        "force_bypass"
        if config["execution_access_mode"] == "force_bypass"
        else "standard"
    )
    try:
        object_keys, object_prefixes = _ctl_state_run_access_scope(config)
        credential = adapter.resolve_ctl_state_credential(
            identity_key,
            config["ctl_cfg_root"],
            execution_context=config["execution_context"],
            implementation_key=config["provider_implementation_key"],
            operation="sync",
            bucket_name=config["bucket_name"],
            object_keys=object_keys,
            object_prefixes=object_prefixes,
            execution_access_mode=sync_access_mode,
            provider_credential=config["provider_credential"],
        )
    except Exception as error:
        if not tolerate_not_ready:
            raise
        _CTL_STATE_SYNC_NOTE = {
            "mode": "deferred",
            "reason": "synchronizer_not_ready",
            "detail": credential_free_preflight_failure_reason(error),
        }
        return False
    syncer = adapter.create_state_syncer(
        config["results_root"],
        config["bucket_name"],
        config["bucket_region"],
        credential,
        config["run_dir"],
        required=not tolerate_not_ready,
    )
    if not syncer.ensure_ready("ctl-state publication readiness"):
        if not tolerate_not_ready:
            raise RuntimeError(
                f"❌ ctl-state backend {config['bucket_name']!r} is not ready"
            )
        _CTL_STATE_SYNC_NOTE = {
            "mode": "deferred",
            "reason": "backend_absent",
        }
        return False
    _CTL_STATE_SYNCER = syncer
    _CTL_STATE_SYNC_NOTE = syncer.summary()
    return True


def configure_ctl_state_sync(
    ctl_cfg_root: Path,
    ctl_profile: str,
    namespace_key: str | None,
    execution_context: dict[str, object],
    run_dir: Path,
    *,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    provisions_ctl_state_backend: bool = False,
    selected_graph_provisions_ctl_state_backend: bool = False,
    backend_absence_confirmed: bool = False,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    provider_implementation_key: str = "local",
) -> dict[str, str] | None:
    """Arm namespace publication or establish an explicitly proven defer queue."""
    del ctl_profile, provisions_ctl_state_backend
    global _CTL_STATE_SYNCER, _CTL_STATE_SYNC_NOTE, _CTL_STATE_DEFER_CONFIG, _CTL_STATE_SYNC_CONFIG
    _CTL_STATE_SYNCER = None
    _CTL_STATE_DEFER_CONFIG = None
    _CTL_STATE_SYNC_CONFIG = None
    _CTL_STATE_SYNC_NOTE = {"mode": "disabled"}

    backends = load_ctl_state_backends_cfg(ctl_cfg_root)
    if backends is None:
        if agreed_defer_ctl_state_backend_sync or force_skip_ctl_state_backend_sync:
            logging.info("ctl-state sync option has no effect: no backend registry")
        return None
    if force_skip_ctl_state_backend_sync:
        _CTL_STATE_SYNC_NOTE = {"mode": "skipped", "reason": "force_skip"}
        return None

    if namespace_key is None:
        namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, execution_context)
    entry = backends[namespace_key]
    adapter = get_provider_adapter(entry["provider"])
    adapter.validate_state_backend_entry(namespace_key, entry, ctl_cfg_root)
    config = _ctl_state_sync_config(
        ctl_cfg_root,
        namespace_key,
        entry,
        execution_context,
        run_dir,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        provider_implementation_key=provider_implementation_key,
    )
    _CTL_STATE_SYNC_CONFIG = config

    if agreed_defer_ctl_state_backend_sync:
        if not selected_graph_provisions_ctl_state_backend:
            raise RuntimeError(
                "❌ agreed ctl-state defer is valid only when the complete selected "
                "graph contains exactly one backend provisioner"
            )
        if not backend_absence_confirmed:
            raise RuntimeError(
                "❌ agreed ctl-state defer is not applicable: the provider did not "
                "confirm that the selected backend was absent at invocation start"
            )
        _CTL_STATE_DEFER_CONFIG = config
        if not _arm_ctl_state_sync(config, tolerate_not_ready=True):
            return {
                "namespace": namespace_key,
                "bucket_name": config["bucket_name"],
                "bucket_region": config["bucket_region"],
            }
    else:
        _arm_ctl_state_sync(config, tolerate_not_ready=False)

    syncer = _CTL_STATE_SYNCER
    if syncer is None:
        raise RuntimeError("❌ ctl-state syncer was not armed")
    instance_prefix = ctl_state_dir_from_run_dir(run_dir).resolve().relative_to(
        syncer.results_root
    ).as_posix()
    metadata = load_run_metadata(run_dir)
    child_prefixes = [
        ctl_state_target_address_prefix(
            str(metadata.get("action") or ""), str(address)
        )
        for address in (metadata.get("target_addresses") or [])
    ]
    syncer.hydrate_instance(instance_prefix, child_prefixes)
    enforce_mutation_lock(
        syncer,
        action=str(metadata.get("action") or ""),
        run_id=str(metadata.get("run_id") or Path(run_dir).name),
    )
    syncer.push("run started")
    _CTL_STATE_SYNC_NOTE = syncer.summary()
    return {
        "namespace_key": namespace_key,
        "bucket_name": config["bucket_name"],
        "bucket_region": config["bucket_region"],
    }


def _pending_manifest_path(run_dir: Path) -> tuple[Path, Path]:
    metadata = load_run_metadata(run_dir)
    local_root = Path(metadata["ctl_state_local_root"])
    locator = list(metadata.get("ctl_state_locator") or [])
    namespace_root = local_root.joinpath(*locator)
    top_level_run_id = (
        metadata.get("fan_out_run_id")
        or metadata.get("parent_workflow_run_id")
        or metadata.get("run_id")
        or Path(run_dir).name
    )
    manifest_dir = namespace_root / "_pending_sync" / str(top_level_run_id)
    return namespace_root, manifest_dir / "manifest.yaml"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def queue_ctl_state_run(
    run_dir: Path,
    pointer_path: Path | None,
    *,
    dependencies: list[str] | None = None,
) -> Path:
    namespace_root, manifest_path = _pending_manifest_path(run_dir)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = manifest_path.parent / ".lock"
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        manifest = load_yaml(manifest_path) if manifest_path.is_file() else {}
        if not isinstance(manifest, dict):
            raise RuntimeError(f"❌ pending-sync manifest must be a mapping: {manifest_path}")
        metadata = load_run_metadata(run_dir)
        run_rel = Path(run_dir).resolve().relative_to(namespace_root.resolve()).as_posix()
        instance_dir = ctl_state_dir_from_run_dir(run_dir)
        identity_path = instance_dir / "identity.yaml"
        object_paths = [
            path
            for path in sorted(Path(run_dir).rglob("*"))
            if path.is_file()
        ]
        if identity_path.is_file():
            object_paths.append(identity_path)
        if pointer_path is not None:
            object_paths.append(pointer_path)
        hashes = {
            path.resolve().relative_to(namespace_root.resolve()).as_posix(): _sha256_file(path)
            for path in dict.fromkeys(object_paths)
        }
        entry = {
            "run_id": Path(run_dir).name,
            "run_type": metadata.get("run_type"),
            "owner_address": metadata.get("instance_address") or metadata.get("result_name"),
            "run_path": run_rel,
            "identity_path": (
                identity_path.resolve().relative_to(namespace_root.resolve()).as_posix()
                if identity_path.is_file()
                else None
            ),
            "pointer_path": (
                pointer_path.resolve().relative_to(namespace_root.resolve()).as_posix()
                if pointer_path is not None
                else None
            ),
            "dependencies": list(dependencies or []),
            "objects": hashes,
            "status": "pending",
        }
        entries = [
            item for item in (manifest.get("entries") or [])
            if item.get("run_path") != run_rel
        ]
        entries.append(entry)
        write_yaml_file(
            manifest_path,
            {
                "version": 1,
                "namespace": (metadata.get("ctl_state_namespace") or (metadata.get("ctl_state_locator") or [None])[0]),
                "top_level_run_id": manifest_path.parent.name,
                "status": "pending",
                "updated_at": utc_timestamp(),
                "entries": entries,
            },
        )
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    return manifest_path


def _validate_pending_entry(namespace_root: Path, entry: dict) -> None:
    for relative_path, expected_sha in (entry.get("objects") or {}).items():
        path = namespace_root / relative_path
        if not path.is_file():
            raise RuntimeError(f"❌ pending ctl-state object is missing: {path}")
        actual = _sha256_file(path)
        if actual != expected_sha:
            raise RuntimeError(
                f"❌ pending ctl-state object hash changed: {relative_path}"
            )


def drain_pending_ctl_state_sync() -> int:
    """Drain pending manifests with one fresh, run-scoped credential per entry."""
    global _CTL_STATE_SYNCER
    syncer = _CTL_STATE_SYNCER
    if syncer is None:
        return 0
    base_config = _CTL_STATE_DEFER_CONFIG or _CTL_STATE_SYNC_CONFIG
    pending_root = syncer.results_root / "_pending_sync"
    if not pending_root.is_dir():
        return 0
    drained = 0
    for manifest_path in sorted(pending_root.glob("*/manifest.yaml")):
        lock_path = manifest_path.parent / ".lock"
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            manifest = load_yaml(manifest_path) or {}
            entries = list(manifest.get("entries") or [])
            for entry in entries:
                active_syncer = _CTL_STATE_SYNCER
                if active_syncer is None:
                    raise RuntimeError("❌ ctl-state syncer disappeared during catch-up")
                _validate_pending_entry(active_syncer.results_root, entry)
                if base_config is not None:
                    entry_config = dict(base_config)
                    entry_config["run_dir"] = active_syncer.results_root / entry["run_path"]
                    _arm_ctl_state_sync(entry_config, tolerate_not_ready=False)
                    active_syncer = _CTL_STATE_SYNCER
                identity_rel = entry.get("identity_path")
                if identity_rel:
                    active_syncer.publish_identity(active_syncer.results_root / identity_rel)
                active_syncer.push_run(
                    active_syncer.results_root / entry["run_path"],
                    f"deferred catch-up {entry['run_id']}",
                )
            priority = {"target": 0, "workflow": 1}
            for entry in sorted(
                entries,
                key=lambda item: (
                    priority.get(str(item.get("run_type")), 2),
                    str(item.get("run_id")),
                ),
            ):
                pointer_rel = entry.get("pointer_path")
                if not pointer_rel:
                    continue
                active_syncer = _CTL_STATE_SYNCER
                if base_config is not None:
                    entry_config = dict(base_config)
                    entry_config["run_dir"] = active_syncer.results_root / entry["run_path"]
                    _arm_ctl_state_sync(entry_config, tolerate_not_ready=False)
                    active_syncer = _CTL_STATE_SYNCER
                active_syncer.publish_committed_pointer(
                    active_syncer.results_root / pointer_rel
                )
            manifest_path.unlink()
            drained += len(entries)
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_path.unlink(missing_ok=True)
        try:
            manifest_path.parent.rmdir()
        except OSError:
            pass
    try:
        pending_root.rmdir()
    except OSError:
        pass
    if base_config is not None:
        _arm_ctl_state_sync(base_config, tolerate_not_ready=False)
    return drained

def retry_deferred_ctl_state_sync() -> bool:
    global _CTL_STATE_SYNC_NOTE
    if _CTL_STATE_DEFER_CONFIG is None:
        return _CTL_STATE_SYNCER is not None
    if _CTL_STATE_SYNCER is None and not _arm_ctl_state_sync(
        _CTL_STATE_DEFER_CONFIG, tolerate_not_ready=True
    ):
        return False
    drained = drain_pending_ctl_state_sync()
    _CTL_STATE_SYNC_NOTE = _CTL_STATE_SYNCER.summary()
    if drained:
        logging.info("drained %d deferred ctl-state run(s)", drained)
    return True


def publish_or_queue_ctl_state_run(
    run_dir: Path,
    pointer_path: Path | None,
    *,
    reason: str,
    dependencies: list[str] | None = None,
) -> None:
    global _CTL_STATE_SYNCER
    if _CTL_STATE_SYNC_CONFIG is not None and _CTL_STATE_SYNCER is not None:
        publication_config = dict(_CTL_STATE_SYNC_CONFIG)
        publication_config["run_dir"] = Path(run_dir)
        _arm_ctl_state_sync(publication_config, tolerate_not_ready=False)
    if _CTL_STATE_SYNCER is not None:
        instance_identity = ctl_state_dir_from_run_dir(run_dir) / "identity.yaml"
        if instance_identity.is_file():
            _CTL_STATE_SYNCER.publish_identity(instance_identity)
        _CTL_STATE_SYNCER.push_run(run_dir, reason)
        if pointer_path is not None:
            _CTL_STATE_SYNCER.publish_committed_pointer(pointer_path)
        return
    if _CTL_STATE_DEFER_CONFIG is None:
        return
    queue_ctl_state_run(run_dir, pointer_path, dependencies=dependencies)
    retry_deferred_ctl_state_sync()


def split_target_instance_address(address: str) -> tuple[str, list[str]]:
    """Split a path-form instance address into (key, segments): trailing
    components containing `=` (or a sha256- composition segment) are instance
    segments; key components never contain `=` (Q1j parse boundary)."""
    if not isinstance(address, str) or not address:
        raise RuntimeError("❌ target instance address must be a non-empty string")
    parts = address.split("/")
    idx = len(parts)
    while idx > 0 and ("=" in parts[idx - 1] or parts[idx - 1].startswith("sha256-")):
        idx -= 1
    if idx == 0:
        raise RuntimeError(f"❌ malformed target instance address: {address!r}")
    target_key = "/".join(parts[:idx])
    segments = parts[idx:]
    return normalize_result_name(target_key, label="target instance address"), segments


def ctl_state_target_address_prefix(action: str, address: str) -> str:
    target_key, segments = split_target_instance_address(address)
    return compose_state_relpath(action, "target", target_key, segments).as_posix()


def selection_state_spec(selection: dict) -> dict:
    action = str(selection["workflow_cfg"]["meta"]["action"])
    context = selection["execution_context"]
    target_specs: list[dict] = []
    for target_run in selection["active_target_runs"].values():
        target_key = normalize_result_name(
            target_run["target"], label="status target key"
        )
        segments = resolve_target_instance_segments(
            target_run.get("target_instance_params"),
            context,
            label=f"target {target_key}",
        )
        target_specs.append(
            {
                "kind": "target",
                "key": target_key,
                "segments": segments,
                "address": target_instance_address(target_key, segments),
                "prefix": compose_state_relpath(
                    action, "target", target_key, segments
                ).as_posix(),
            }
        )
    if selection["selection_kind"] == "target":
        if len(target_specs) != 1:
            raise RuntimeError("❌ target status selection must resolve one target instance")
        return target_specs[0]
    if selection["selection_kind"] != "workflow":
        raise RuntimeError(
            f"❌ status does not support selection kind {selection['selection_kind']!r}"
        )
    addresses = [item["address"] for item in target_specs]
    digest = workflow_composition_sha256(addresses)
    key = normalize_result_name(selection["selection_key"], label="status workflow key")
    segments = [f"sha256-{digest}"]
    definition_canonical = json.dumps(
        selection["workflow_cfg"], separators=(",", ":"), sort_keys=True
    )
    return {
        "kind": "workflow",
        "key": key,
        "segments": segments,
        "address": f"{key}/sha256-{digest}",
        "prefix": compose_state_relpath(
            action, "workflow", key, segments
        ).as_posix(),
        "target_specs": target_specs,
        "workflow_definition_sha256": hashlib.sha256(
            definition_canonical.encode("utf-8")
        ).hexdigest(),
    }


def validate_unique_fan_out_materializations(
    child_selections: list[dict],
) -> list[dict]:
    specs = [selection_state_spec(selection) for selection in child_selections]
    seen: dict[str, int] = {}
    duplicates: list[str] = []
    for index, spec in enumerate(specs):
        address = f"{spec['kind']}:{spec['address']}"
        if address in seen:
            duplicates.append(address)
        else:
            seen[address] = index
    if duplicates:
        raise RuntimeError(
            "❌ fan-out materializes duplicate state owners: "
            + ", ".join(sorted(set(duplicates)))
        )
    return specs


def _committed_pointer_verdict(pointer: dict | None) -> tuple[str, list[str]]:
    if pointer is None:
        return "never_ran", ["no committed revision"]
    if pointer.get("status") == "outdated" or pointer.get("outdated"):
        outdated = pointer.get("outdated") or {}
        return "outdated", [str(outdated.get("reason") or "target marker")]
    return "current", []


def compute_target_instance_status(
    namespace_root: Path, action: str, spec: dict
) -> dict:
    instance_dir = namespace_root / spec["prefix"]
    pointer = read_committed_pointer(instance_dir)
    verdict, reasons = _committed_pointer_verdict(pointer)
    lifecycle_candidates: list[tuple[str, str, dict]] = []
    for lifecycle_action in ("provision", "destroy"):
        candidate_dir = namespace_root / compose_state_relpath(
            lifecycle_action, "target", spec["key"], spec["segments"]
        )
        candidate = read_committed_pointer(candidate_dir)
        if candidate:
            order = str(candidate.get("committed_at") or candidate.get("run_id") or "")
            lifecycle_candidates.append((order, lifecycle_action, candidate))
    lifecycle = None
    if lifecycle_candidates:
        _, newest_action, newest = max(lifecycle_candidates, key=lambda item: item[0])
        lifecycle = "destroyed" if newest_action == "destroy" else "active"
        if lifecycle == "destroyed" and action != "destroy":
            verdict = "destroyed"
            reasons = [f"destroyed by {newest.get('run_id')}"]
    return {
        "kind": "target",
        "key": spec["key"],
        "address": spec["address"],
        "verdict": verdict,
        **({"lifecycle": lifecycle} if lifecycle else {}),
        **({"run_id": pointer.get("run_id")} if pointer else {}),
        **({"reasons": reasons} if reasons else {}),
    }


def compute_workflow_instance_status(
    namespace_root: Path, action: str, spec: dict
) -> dict:
    pointer = read_committed_pointer(namespace_root / spec["prefix"])
    verdict, reasons = _committed_pointer_verdict(pointer)
    children: list[dict] = []
    recorded = {
        item.get("address"): item
        for item in ((pointer or {}).get("child_revisions") or [])
        if isinstance(item, dict)
    }
    for target_spec in spec["target_specs"]:
        child = compute_target_instance_status(namespace_root, action, target_spec)
        child_pointer = read_committed_pointer(namespace_root / target_spec["prefix"])
        expected = recorded.get(target_spec["address"])
        if child["verdict"] != "current":
            reasons.append(
                f"{target_spec['address']}: {child['verdict']}"
            )
        elif expected is None:
            reasons.append(f"{target_spec['address']}: not recorded by workflow")
        elif (
            expected.get("run_id") != (child_pointer or {}).get("run_id")
            or expected.get("snapshot_sha256")
            != (child_pointer or {}).get("snapshot_sha256")
        ):
            reasons.append(f"{target_spec['address']}: committed revision changed")
        children.append(child)
    if pointer is not None:
        if pointer.get("workflow_definition_sha256") != spec[
            "workflow_definition_sha256"
        ]:
            reasons.append("workflow definition changed")
        pointer_addresses = [
            str(item.get("address"))
            for item in (pointer.get("child_revisions") or [])
            if isinstance(item, dict)
        ]
        if pointer_addresses != [
            item["address"] for item in spec["target_specs"]
        ]:
            reasons.append("workflow target order or set changed")
    all_destroyed = bool(children) and all(
        child["verdict"] == "destroyed" for child in children
    )
    if all_destroyed and verdict != "never_ran":
        # The whole composition is torn down — mirror the target-level rule
        # (newest event = destroy reads `destroyed`, not `outdated`). A PARTIAL
        # teardown or revision/definition drift still reads `outdated` below.
        verdict = "destroyed"
    elif reasons and verdict != "never_ran":
        verdict = "outdated"
    return {
        "kind": "workflow",
        "key": spec["key"],
        "address": spec["address"],
        "verdict": verdict,
        **({"run_id": pointer.get("run_id")} if pointer else {}),
        **({"reasons": list(dict.fromkeys(reasons))} if reasons else {}),
        "children": children,
    }


def _arm_ctl_state_operation(
    ctl_cfg_root: Path,
    execution_context: dict[str, object],
    ctl_state_local_root: Path,
    *,
    operation: str,
    provider_implementation_key: str,
    execution_access_mode: str,
    provider_credential: str | None,
    object_keys: list[str] | tuple[str, ...] = (),
    object_prefixes: list[str] | tuple[str, ...] = (),
):
    namespace_key, entry = resolve_ctl_state_namespace(
        ctl_cfg_root, execution_context
    )
    adapter = get_provider_adapter(entry["provider"])
    adapter.validate_state_backend_entry(namespace_key, entry, ctl_cfg_root)
    bucket_name = str(
        resolve_runtime_scalar(
            entry["bucket_name"],
            execution_context,
            label=f"ctl_state_backends.{namespace_key}.bucket_name",
        )
    )
    identity_key = (entry.get("execution_identity_keys") or {}).get(operation)
    if identity_key is None and execution_access_mode != "force_bypass":
        raise RuntimeError(
            f"❌ ctl_state_backends.{namespace_key} declares no "
            f"execution_identity_keys.{operation}"
        )
    operation_access_mode = (
        "force_bypass" if execution_access_mode == "force_bypass" else "standard"
    )
    credential = adapter.resolve_ctl_state_credential(
        identity_key,
        ctl_cfg_root,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        operation=operation,
        bucket_name=bucket_name,
        object_keys=object_keys,
        object_prefixes=object_prefixes,
        execution_access_mode=operation_access_mode,
        provider_credential=provider_credential,
    )
    namespace_root = Path(ctl_state_local_root) / namespace_key
    syncer = adapter.create_state_syncer(
        namespace_root,
        bucket_name,
        str(entry["bucket_region"]),
        credential,
        namespace_root,
        required=True,
    )
    if not syncer.ensure_ready(operation):
        raise RuntimeError(f"❌ ctl-state backend {namespace_key!r} is not ready")
    enforce_mutation_lock(
        syncer,
        action="readonly",
        run_id=f"{operation}-{generate_uuid7()}",
    )
    return namespace_key, namespace_root, syncer


def _arm_ctl_state_reader(
    ctl_cfg_root: Path,
    selection: dict,
    ctl_state_local_root: Path,
    *,
    provider_implementation_key: str,
    execution_access_mode: str,
    provider_credential: str | None,
):
    return _arm_ctl_state_operation(
        ctl_cfg_root,
        selection["execution_context"],
        ctl_state_local_root,
        operation="read",
        provider_implementation_key=provider_implementation_key,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
    )


def _resolve_local_ctl_state_scope(
    ctl_cfg_root: Path, execution_context: dict[str, object], ctl_state_local_root: Path
) -> tuple[str, Path]:
    """§Phase 42 `local` scope: the namespace resolves from cfg alone, so the
    local view needs no credentials and makes no bucket calls. Reads the tree
    exactly as it is — the ONLY way to see a force-skipped run, which exists
    locally and can never reach the bucket."""
    namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, execution_context)
    return namespace_key, Path(ctl_state_local_root) / namespace_key


def hydrate_ctl_state_index(syncer) -> list[str]:
    keys = syncer.list_object_keys()
    for key in keys:
        if key.endswith("/committed.yaml") or key.endswith("/RUN.yaml"):
            syncer.pull_object(key)
    return keys


def run_ctl_state_status_sweep(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    context = build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        force_skip_full_cfg_validation_gate=(
            args.force_skip_full_cfg_validation_gate
        ),
        execution_runtime_mode=args.execution_runtime_mode,
    )
    # §Phase 42: the sweep is a QUERY over bucket truth (its only output, an
    # advisory status_cache.yaml, belongs to the bucket). It hydrates every
    # pointer in the namespace, so running it against the real local root would
    # clobber local-only records wholesale — it works in a throwaway root and
    # pushes the caches from there.
    with tempfile.TemporaryDirectory(prefix="atlas-ctl-state-sweep-") as scratch:
        return _run_ctl_state_status_sweep_in(
            ctl_cfg_root,
            args,
            context,
            Path(scratch),
            provider_implementation_key=provider_implementation_key,
        )


def _run_ctl_state_status_sweep_in(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    context: dict[str, object],
    ctl_state_root: Path,
    *,
    provider_implementation_key: str,
) -> dict:
    namespace_key, namespace_root, reader = _arm_ctl_state_operation(
        ctl_cfg_root,
        context,
        ctl_state_root,
        operation="read",
        provider_implementation_key=provider_implementation_key,
        execution_access_mode=args.execution_access_mode,
        provider_credential=args.provider_credential,
    )
    hydrate_ctl_state_index(reader)
    # §Phase 50.10: ONE lean root-level map (advisory, bucket-owned), replacing
    # the old per-workflow-instance verbose docs. Flat address -> verdict over
    # every target and workflow instance, lifecycle-collapsed.
    instances = compute_namespace_status_map(namespace_root)
    # §Phase 50.10: same self-describing shape status.py --write-cache emits, so
    # a reader never has to guess which view / when produced this snapshot.
    cache = {
        "advisory": True,
        "source": "ctl-state self-consistency sweep",
        "namespace": namespace_key,
        "scope": "remote",
        "computed_at": utc_timestamp(),
        "instances": instances,
    }
    cache_path = namespace_root / "status_cache.yaml"
    write_yaml_file(cache_path, cache)
    cache_key = cache_path.relative_to(namespace_root).as_posix()
    _, _, writer = _arm_ctl_state_operation(
        ctl_cfg_root,
        context,
        ctl_state_root,
        operation="sync",
        object_keys=[cache_key],
        provider_implementation_key=provider_implementation_key,
        execution_access_mode=args.execution_access_mode,
        provider_credential=args.provider_credential,
    )
    writer.put_object(cache_key, cache_path)
    report = {
        "operation": "status-sweep",
        "namespace": namespace_key,
        "instances": instances,
    }
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report

def _uuid7_datetime(run_id: str) -> datetime | None:
    try:
        parsed = uuid.UUID(run_id)
    except (ValueError, AttributeError):
        return None
    if parsed.version != 7:
        return None
    timestamp_ms = parsed.int >> 80
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def run_ctl_state_history_prune(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    if not ctl_allows_ctl_state_history_maintenance(
        ctl_cfg_root, args.ctl_profile
    ):
        raise RuntimeError(
            f"❌ ctl profile {args.ctl_profile!r} does not grant "
            "allow_ctl_state_history_maintenance"
        )
    context = build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        force_skip_full_cfg_validation_gate=(
            args.force_skip_full_cfg_validation_gate
        ),
        execution_runtime_mode=args.execution_runtime_mode,
    )
    namespace_key, namespace_root, reader = _arm_ctl_state_operation(
        ctl_cfg_root,
        context,
        args.ctl_state_local_root,
        operation="read",
        provider_implementation_key=provider_implementation_key,
        execution_access_mode=args.execution_access_mode,
        provider_credential=args.provider_credential,
    )
    keys = hydrate_ctl_state_index(reader)
    selected_ids = set(args.prune_run_id or [])
    cutoff = None
    if args.prune_before:
        try:
            cutoff = datetime.fromisoformat(args.prune_before)
        except ValueError as error:
            raise RuntimeError("❌ --prune-before must be ISO-8601") from error
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)

    run_keys: dict[str, list[str]] = {}
    run_kinds: dict[str, str] = {}
    for key in keys:
        match = re.search(r"/(target|workflow)/.+?/runs/([^/]+)/", "/" + key)
        if not match:
            continue
        kind, run_id = match.group(1), match.group(2)
        run_keys.setdefault(run_id, []).append(key)
        run_kinds[run_id] = kind
        when = _uuid7_datetime(run_id)
        if cutoff is not None and when is not None and when < cutoff:
            selected_ids.add(run_id)
    if args.prune_kind:
        selected_ids = {
            run_id
            for run_id in selected_ids
            if run_kinds.get(run_id) == args.prune_kind
        }
    unknown = sorted(selected_ids - set(run_keys))
    if unknown:
        raise RuntimeError(
            "❌ selected prune run ids are not present in this namespace: "
            + ", ".join(unknown)
        )

    current_ids = set()
    for pointer_path in namespace_root.rglob("committed.yaml"):
        pointer = read_committed_pointer(pointer_path.parent)
        if pointer and pointer.get("run_id"):
            current_ids.add(str(pointer["run_id"]))
    protected = sorted(selected_ids & current_ids)
    if protected:
        raise RuntimeError(
            "❌ current committed revisions cannot be pruned: "
            + ", ".join(protected)
        )

    references: dict[str, set[str]] = {}
    for snapshot_path in namespace_root.rglob(RUN_METADATA_FILENAME):
        snapshot = load_yaml(snapshot_path) or {}
        workflow_run = str(
            snapshot.get("run_id") or snapshot_path.parent.name
        )
        for child in snapshot.get("child_revisions") or []:
            if isinstance(child, dict) and child.get("run_id"):
                references.setdefault(str(child["run_id"]), set()).add(workflow_run)

    candidates = set(selected_ids)
    changed = True
    while changed:
        changed = False
        for run_id in list(candidates):
            referrers = references.get(run_id, set()) - candidates
            if not referrers:
                continue
            if not args.cascade:
                raise RuntimeError(
                    f"❌ run {run_id} is referenced by retained workflow runs: "
                    + ", ".join(sorted(referrers))
                )
            current_referrers = referrers & current_ids
            if current_referrers:
                raise RuntimeError(
                    "❌ cascade would prune current workflow revisions: "
                    + ", ".join(sorted(current_referrers))
                )
            candidates.update(referrers)
            changed = True

    deletion_keys = sorted(
        key for run_id in candidates for key in run_keys.get(run_id, [])
    )
    maintenance_id = generate_uuid7()
    report = {
        "operation": "history-prune",
        "namespace": namespace_key,
        "maintenance_id": maintenance_id,
        "dry_run": not args.apply_history_prune,
        "selection": {
            "run_ids": sorted(args.prune_run_id or []),
            "before": args.prune_before,
            "kind": args.prune_kind,
            "cascade": bool(args.cascade),
        },
        "candidate_run_ids": sorted(candidates),
        "object_keys": deletion_keys,
        "delete_object_versions": False,
        "created_at": utc_timestamp(),
    }
    manifest_path = (
        namespace_root
        / "_maintenance"
        / "history-prune"
        / maintenance_id
        / "manifest.yaml"
    )
    write_yaml_file(manifest_path, report)
    manifest_key = manifest_path.relative_to(namespace_root).as_posix()
    _, _, maintainer = _arm_ctl_state_operation(
        ctl_cfg_root,
        context,
        args.ctl_state_local_root,
        operation="maintenance",
        object_keys=[manifest_key, *deletion_keys],
        provider_implementation_key=provider_implementation_key,
        execution_access_mode=args.execution_access_mode,
        provider_credential=args.provider_credential,
    )
    maintainer.put_object(manifest_key, manifest_path)
    if args.apply_history_prune:
        maintainer.delete_object_keys(deletion_keys)
        report["applied_at"] = utc_timestamp()
        write_yaml_file(manifest_path, report)
        maintainer.put_object(manifest_key, manifest_path)
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


def run_ctl_state_maintenance_command(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    validate_maintenance_args(args)
    context = build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        execution_access_mode=args.execution_access_mode,
        agreed_defer_ctl_state_backend_sync=args.agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=args.force_skip_ctl_state_backend_sync,
        force_skip_guardrails=args.force_skip_guardrails,
        force_skip_full_cfg_validation_gate=(
            args.force_skip_full_cfg_validation_gate
        ),
        execution_runtime_mode=args.execution_runtime_mode,
        force_skip_execution_identity_preflight_check=getattr(
            args, "force_skip_execution_identity_preflight_check", False
        ),
    )
    validate_force_skip_full_cfg_validation_gate_policy(
        ctl_cfg_root,
        args.ctl_profile,
        args.force_skip_full_cfg_validation_gate,
    )
    cfg_report = build_cfg_validation_report(
        collect_provider_cfg_findings(ctl_cfg_root, context)
    )
    apply_full_cfg_validation_gate(
        cfg_report, force_skip=args.force_skip_full_cfg_validation_gate
    )
    logging.info("\n%s", "\n".join(_cfg_validation_text_lines(cfg_report)))
    assert_full_cfg_validation_gate_accepted(cfg_report)
    if args.maintenance_action == "status-sweep":
        return run_ctl_state_status_sweep(
            ctl_cfg_root, args, provider_implementation_key=provider_implementation_key
        )
    if args.maintenance_action == "history-prune":
        return run_ctl_state_history_prune(
            ctl_cfg_root, args, provider_implementation_key=provider_implementation_key
        )
    raise RuntimeError(
        f"❌ {args.maintenance_action!r} is not a ctl-state-only maintenance operation"
    )



def _compute_status_results(
    namespace_root: Path, action: str, labels: list[str], specs: list[dict]
) -> list[dict]:
    results = []
    for label, spec in zip(labels, specs):
        computed = (
            compute_target_instance_status(namespace_root, action, spec)
            if spec["kind"] == "target"
            else compute_workflow_instance_status(namespace_root, action, spec)
        )
        computed["selection"] = label
        results.append(computed)
    return results


def run_status_command(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    run_type: str,
    provider_implementation_key: str = "local",
) -> dict:
    if run_type == "fan_out":
        expansion_context = build_execution_context(
            ctl_cfg_root,
            action=args.action,
            ctl_profile=args.ctl_profile,
            execution_params=args.execution_params,
            execution_runtime_mode=args.execution_runtime_mode,
        )
        plan = expand_fan_out(ctl_cfg_root, args.fan_out, expansion_context)
        validate_fan_out_param_collisions(
            ctl_cfg_root, plan["children"], args.execution_params
        )
        require_unique_fan_out_namespace(
            ctl_cfg_root,
            plan["children"],
            action=args.action,
            ctl_profile=args.ctl_profile,
            execution_params=args.execution_params,
            execution_runtime_mode=args.execution_runtime_mode,
        )
        selections = []
        labels = []
        for child in plan["children"]:
            params = dict(args.execution_params)
            params.update(child["params"])
            selections.append(
                resolve_pipeline_selection(
                    ctl_cfg_root,
                    args.ctl_profile,
                    params,
                    args.ctl_ref_policy,
                    args.action,
                    child["key"] if child["kind"] == "workflow" else None,
                    ctl_variants=(
                        args.ctl_variants if child["kind"] == "workflow" else []
                    ),
                    target_repo_key="repo_path",
                    require_target_ref=False,
                    execution_runtime_mode=args.execution_runtime_mode,
                    provider_credential=args.provider_credential,
                    execution_access_mode=args.execution_access_mode,
                    target_name=child["key"] if child["kind"] == "target" else None,
                    # §Phase 50: a status read needs only the cfg-level state
                    # spec (prefix/segments); it enforces no mutate policy and
                    # loads no provider catalogs (which would validate account
                    # ids a read never uses).
                    enforce_ctl_policy=False,
                    load_provider_catalogs=False,
                )
            )
            labels.append(child["label"])
        specs = validate_unique_fan_out_materializations(selections)
    else:
        selection = resolve_pipeline_selection(
            ctl_cfg_root,
            args.ctl_profile,
            args.execution_params,
            args.ctl_ref_policy,
            args.action,
            args.workflow if run_type == "workflow" else None,
            ctl_variants=getattr(args, "ctl_variants", None) or [],
            target_repo_key="repo_path",
            require_target_ref=False,
            execution_runtime_mode=args.execution_runtime_mode,
            provider_credential=args.provider_credential,
            execution_access_mode=args.execution_access_mode,
            target_name=args.target if run_type == "target" else None,
            # §Phase 50: cfg-level state spec only — no mutate policy, no
            # provider catalogs (a read never uses account ids).
            enforce_ctl_policy=False,
            load_provider_catalogs=False,
        )
        selections = [selection]
        specs = [selection_state_spec(selection)]
        labels = [selection["selection_key"]]

    # §Phase 42: a query must NEVER mutate local ctl-state. `remote` hydrates
    # into an auto-generated throwaway root (an implementation detail — never a
    # CLI argument) so pull_object's unconditional overwrite lands there instead
    # of clobbering a local-only pointer; `local` never touches the bucket.
    if args.status == "local":
        namespace_key, namespace_root = _resolve_local_ctl_state_scope(
            ctl_cfg_root,
            selections[0]["execution_context"],
            args.ctl_state_local_root,
        )
        results = _compute_status_results(namespace_root, args.action, labels, specs)
    else:
        with tempfile.TemporaryDirectory(
            prefix="atlas-ctl-state-remote-"
        ) as scratch_root:
            namespace_key, namespace_root, syncer = _arm_ctl_state_reader(
                ctl_cfg_root,
                selections[0],
                Path(scratch_root),
                provider_implementation_key=provider_implementation_key,
                execution_access_mode=args.execution_access_mode,
                provider_credential=args.provider_credential,
            )
            for spec in specs:
                child_prefixes = [
                    target["prefix"] for target in spec.get("target_specs", [])
                ]
                syncer.hydrate_instance(spec["prefix"], child_prefixes)
                # Lifecycle status needs sibling provision/destroy pointers.
                target_specs = spec.get("target_specs") or (
                    [spec] if spec["kind"] == "target" else []
                )
                for target_spec in target_specs:
                    for lifecycle_action in ("provision", "destroy"):
                        syncer.pull_object(
                            compose_state_relpath(
                                lifecycle_action,
                                "target",
                                target_spec["key"],
                                target_spec["segments"],
                            ).as_posix()
                            + "/committed.yaml"
                        )
            results = _compute_status_results(
                namespace_root, args.action, labels, specs
            )
    report = {
        "selection": {
            "kind": run_type,
            "key": (
                args.fan_out
                if run_type == "fan_out"
                else args.workflow
                if run_type == "workflow"
                else args.target
            ),
        },
        "namespace": namespace_key,
        # Which scope produced this view — local and bucket history legitimately
        # differ (a force-skipped run is local-only, permanently).
        "scope": args.status,
        "verdict": (
            "outdated"
            if any(item["verdict"] in {"outdated", "destroyed"} for item in results)
            else "never_ran"
            if any(item["verdict"] == "never_ran" for item in results)
            else "current"
        ),
        "results": results,
    }
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


def _workflow_instance_verdict(
    namespace_root: Path, key: str, segments: list[str]
) -> str | None:
    """Reconciled verdict for ONE workflow instance (§Phase 50.10), or None when
    the instance holds no reconciled state and must not appear in the map.

    Only a DEPLOYABLE COMPOSITION — a key that has ever been provisioned — owns a
    reconciled verdict. A destroy-only key (a pure teardown) creates nothing and
    owns no state, so it returns None and gets no row: its effect is already
    recorded on the TARGET rows it destroyed (and on the provision composition's
    own row). Teardown runs still persist for audit — that is run history, not
    reconciled status.

    For a deployable composition the newest of its provision/destroy pointers
    wins: a destroy newest (torn down via its own key) reads `destroyed`, else the
    verdict is projected from its child target markers."""
    candidates: list[tuple[str, str, Path, dict]] = []
    provision_pointer: dict | None = None
    for lifecycle_action in ("provision", "destroy"):
        candidate_dir = namespace_root / compose_state_relpath(
            lifecycle_action, "workflow", key, segments
        )
        pointer = read_committed_pointer(candidate_dir)
        if pointer:
            order = str(pointer.get("committed_at") or pointer.get("run_id") or "")
            candidates.append((order, lifecycle_action, candidate_dir, pointer))
            if lifecycle_action == "provision":
                provision_pointer = pointer
    # A pure teardown (destroy-only key) owns no reconciled state → no row.
    if provision_pointer is None:
        return None
    _, newest_action, newest_dir, pointer = max(candidates, key=lambda item: item[0])
    if newest_action == "destroy":
        return "destroyed"
    target_specs = []
    for item in pointer.get("child_revisions") or []:
        if not isinstance(item, dict):
            continue
        child_key, child_segments = split_target_instance_address(
            str(item.get("address"))
        )
        target_specs.append(
            {
                "kind": "target",
                "key": child_key,
                "segments": child_segments,
                "address": str(item.get("address")),
                "prefix": compose_state_relpath(
                    "provision", "target", child_key, child_segments
                ).as_posix(),
            }
        )
    spec = {
        "kind": "workflow",
        "key": key,
        "segments": segments,
        "address": "/".join([key, *segments]),
        "prefix": newest_dir.relative_to(namespace_root).as_posix(),
        "target_specs": target_specs,
        "workflow_definition_sha256": pointer.get("workflow_definition_sha256"),
    }
    return compute_workflow_instance_status(namespace_root, "provision", spec)["verdict"]


def compute_namespace_status_map(namespace_root: Path) -> dict[str, str]:
    """§Phase 50.10: a flat `address -> verdict` map over EVERY target and
    workflow instance under the namespace root, lifecycle-collapsed to one row
    per instance. Targets read their own event-driven marker (via the
    provision-perspective compute, which also folds a newer destroy into
    `destroyed`); a workflow row is a derived projection over child markers and
    exists only for a DEPLOYABLE COMPOSITION (a key ever provisioned). Pure
    teardowns (destroy-only workflow keys) own no reconciled state and never
    appear — their effect shows on the target rows. Fan-outs own no state and
    never appear. Verdict vocab: current | outdated | destroyed | never_ran (a
    target `status: ok` marker reads as `current`)."""
    namespace_root = Path(namespace_root)
    if not namespace_root.is_dir():
        return {}
    targets: set[tuple[str, tuple[str, ...]]] = set()
    workflows: set[tuple[str, tuple[str, ...]]] = set()
    for pointer_path in namespace_root.rglob("committed.yaml"):
        parsed = parse_state_relpath(namespace_root, pointer_path.parent)
        if parsed is None:
            continue
        instance = (parsed["key"], tuple(parsed["instance_segments"]))
        if parsed["kind"] == "target":
            targets.add(instance)
        elif parsed["kind"] == "workflow":
            workflows.add(instance)
    rows: dict[str, str] = {}
    for key, seg in targets:
        segments = list(seg)
        address = target_instance_address(key, segments)
        spec = {
            "kind": "target",
            "key": key,
            "segments": segments,
            "address": address,
            "prefix": compose_state_relpath(
                "provision", "target", key, segments
            ).as_posix(),
        }
        rows[f"target/{address}"] = compute_target_instance_status(
            namespace_root, "provision", spec
        )["verdict"]
    for key, seg in workflows:
        verdict = _workflow_instance_verdict(namespace_root, key, list(seg))
        if verdict is None:
            continue
        address = "/".join([key, *seg])
        rows[f"workflow/{address}"] = verdict
    return dict(sorted(rows.items()))


def add_status_args(parser: argparse.ArgumentParser) -> None:
    """§Phase 50: the slim, read-only status parser. Only what a read needs —
    namespace + breadth + scope (+ dir for local, dev substitute for remote).
    No runtime-mode, no access-mode enum, no reuse/force/defer. --ctl-profile
    stays: a read still consults its ref_policy (§Phase 50.7). Titled groups
    drive --help, mirroring the run runners; keep add_bootstrap_status_args (the
    pre-fetch --help duplicate) in sync with the SAME groups in the SAME order."""
    ctl_group = parser.add_argument_group(
        "ctl",
        "cfg/policy source and governing profile; a read consults only the "
        "profile's ref_policy (tooling/cfg pinning + dirty-vs-committed gate)",
    )
    query_group = parser.add_argument_group(
        "query",
        "what to read: the namespace/instance selectors, the breadth (whole "
        "namespace or one owner), and the optional lifecycle view",
    )
    scope_group = parser.add_argument_group(
        "scope",
        "where to read from: the local tree (offline, no credentials) or the "
        "authoritative bucket (hydrated into an auto temp). NEITHER mutates "
        "local ctl-state",
    )
    # 1) ctl
    ctl_group.add_argument(
        "--ctl-cfg", required=True, help="git URL@ref or local path to the ctl cfg"
    )
    ctl_group.add_argument(
        "--ctl-profile",
        required=True,
        help="Ctl profile name; a read consults its ref_policy only (§Phase 50.7)",
    )
    # 2) query — selectors, breadth, lifecycle view
    query_group.add_argument(
        "--execution-params",
        dest="execution_param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        type=parse_selector_arg,
        help="Execution param key=value; repeatable. Namespace selectors "
        "(provider, landing_zone) always; instance selectors (account, env_type, "
        "region) only for a targeted query",
    )
    breadth = query_group.add_mutually_exclusive_group(required=True)
    breadth.add_argument(
        "--all",
        action="store_true",
        help="whole-namespace: every target and workflow instance",
    )
    breadth.add_argument("--target", metavar="NAME", help="one declared target instance")
    breadth.add_argument(
        "--workflow", metavar="NAME", help="one declared workflow instance"
    )
    breadth.add_argument(
        "--fan-out",
        dest="fan_out",
        metavar="NAME",
        help="status of the targets/workflows a fan-out expands to",
    )
    query_group.add_argument(
        "--action",
        default=None,
        choices=["provision", "destroy"],
        help="lifecycle view; required for a targeted query, ignored by --all",
    )
    # 3) scope — where to read, and the local dir / dev substitute
    scope_group.add_argument(
        "--scope",
        required=True,
        choices=("local", "remote"),
        help="'local' reads the dir offline (no bucket, no credentials) — the "
        "only way to see force-skipped runs; 'remote' is the authoritative "
        "bucket view (hydrated into an auto temp, discarded)",
    )
    scope_group.add_argument(
        "--ctl-state-local-root",
        default=None,
        metavar="DIR",
        help="required for --scope local (the ctl-state tree to read); not "
        "valid for --scope remote (remote uses an auto temp)",
    )
    scope_group.add_argument(
        "--provider-credential",
        dest="provider_credential",
        default=None,
        metavar="PROFILE",
        help="dev/emergency substitute credential for --scope remote when no "
        "ctl-state read chain exists yet; passing it skips identity resolution "
        "(the read's force-bypass — same arg the run runners use)",
    )
    scope_group.add_argument(
        "--write-cache",
        action="store_true",
        help="also persist the computed map as an advisory, self-dated "
        "status_cache.yaml at the namespace root under --ctl-state-local-root "
        "(requires --all — the cache is a whole-namespace map). Default: "
        "print only, write nothing. Never touches committed pointers.",
    )


def finalize_status_args(args: argparse.Namespace) -> None:
    """Normalize + validate the slim status args, and synthesize the internal
    values the shared resolvers still expect (a read has ONE operation, so the
    access mode collapses to standard-chain, or the dev substitute)."""
    args.execution_params = selectors_to_map(
        args.execution_param, label="execution param"
    )
    args.status = args.scope
    write_cache = getattr(args, "write_cache", False)
    if write_cache and not args.all:
        raise RuntimeError(
            "❌ --write-cache requires --all: the status cache is a "
            "whole-namespace map"
        )
    if args.scope == "local":
        if not args.ctl_state_local_root:
            raise RuntimeError("❌ --scope local requires --ctl-state-local-root")
        if args.provider_credential:
            raise RuntimeError(
                "❌ --provider-credential is not valid with --scope local "
                "(local reads the dir — no bucket, no credentials)"
            )
        args.ctl_state_local_root = normalize_ctl_state_local_root(
            args.ctl_state_local_root
        )
    else:
        # remote reads pointers from the bucket into a throwaway temp, so it
        # needs no local root — UNLESS --write-cache, where the local root is the
        # cache write target (the derived map lands there; local pointers are
        # never read or clobbered).
        if write_cache:
            if not args.ctl_state_local_root:
                raise RuntimeError(
                    "❌ --write-cache with --scope remote requires "
                    "--ctl-state-local-root (the cache write target)"
                )
            args.ctl_state_local_root = normalize_ctl_state_local_root(
                args.ctl_state_local_root
            )
        elif args.ctl_state_local_root:
            raise RuntimeError(
                "❌ --ctl-state-local-root is not valid with --scope remote "
                "(remote hydrates into an auto temp; only --write-cache uses it)"
            )
        else:
            args.ctl_state_local_root = None
    if args.provider_credential:
        args.execution_access_mode = "force_bypass"
        args.provider_credential = args.provider_credential.strip() or None
    else:
        args.execution_access_mode = "standard"
        args.provider_credential = None
    # inert for a read (no box is built) but required by the shared context
    # builders / selection resolvers.
    args.execution_runtime_mode = "local"
    args.ctl_variants = []
    if not args.all and args.action is None:
        raise RuntimeError(
            "❌ a targeted status (--target/--workflow/--fan-out) requires "
            "--action provision|destroy"
        )


def run_status_all_command(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    """§Phase 50 whole-namespace status: resolve the namespace from the axes,
    then read every instance — local walks the dir offline; remote hydrates the
    whole namespace into a throwaway temp (never the local tree) and reads that.
    Prints a flat map. Read-only by default; --write-cache additionally persists
    the map as an advisory, self-dated status_cache.yaml at the namespace root
    (an additive file — it never touches committed pointers)."""
    execution_context = build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        execution_access_mode=args.execution_access_mode,
        execution_runtime_mode=args.execution_runtime_mode,
    )
    namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, execution_context)
    if args.status == "local":
        namespace_root = Path(args.ctl_state_local_root) / namespace_key
        instances = compute_namespace_status_map(namespace_root)
    else:
        with tempfile.TemporaryDirectory(
            prefix="atlas-ctl-state-remote-all-"
        ) as scratch_root:
            _, namespace_root, syncer = _arm_ctl_state_operation(
                ctl_cfg_root,
                execution_context,
                Path(scratch_root),
                operation="read",
                provider_implementation_key=provider_implementation_key,
                execution_access_mode=args.execution_access_mode,
                provider_credential=args.provider_credential,
            )
            hydrate_ctl_state_index(syncer)
            instances = compute_namespace_status_map(namespace_root)
    report = {
        "namespace": namespace_key,
        "scope": args.status,
        "computed_at": utc_timestamp(),
        "instances": instances,
    }
    if getattr(args, "write_cache", False):
        cache = {"advisory": True, "source": "status runner", **report}
        cache_path = (
            Path(args.ctl_state_local_root) / namespace_key / "status_cache.yaml"
        )
        write_yaml_file(cache_path, cache)
        report = {**report, "cache_written": cache_path.as_posix()}
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


def run_status(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    """§Phase 50 status dispatcher: whole-namespace (--all) or targeted."""
    if args.all:
        return run_status_all_command(
            ctl_cfg_root, args, provider_implementation_key=provider_implementation_key
        )
    run_type = (
        "workflow"
        if args.workflow
        else "fan_out"
        if args.fan_out
        else "target"
    )
    return run_status_command(
        ctl_cfg_root,
        args,
        run_type=run_type,
        provider_implementation_key=provider_implementation_key,
    )


def pending_ctl_state_manifest_paths(local_root: Path, namespace_key: str) -> list[Path]:
    root = Path(local_root) / namespace_key / "_pending_sync"
    return sorted(root.glob("*/manifest.yaml")) if root.is_dir() else []



_MUTATION_LOCK_HELD: dict | None = None


def enforce_mutation_lock(syncer, *, action: str, run_id: str) -> None:
    """§Phase 31 Q1b: the interim global mutation lock, enforced at the
    namespace bucket. Mutating runs acquire it exclusively; non-mutating runs
    check and fail fast with the holder's run id. No syncer (sync skipped or
    deferred) means no reachable backend — the lock is skipped with a log line
    (the bootstrap-defer window is single-operator by definition)."""
    global _MUTATION_LOCK_HELD
    if syncer is None:
        logging.info("mutation lock skipped: no armed ctl-state syncer")
        return
    existing = syncer.read_mutation_lock()
    outcome = evaluate_mutation_lock(existing, action=action, run_id=run_id)
    decision = outcome["decision"]
    if decision == "blocked":
        raise RuntimeError(
            f"❌ ctl-state namespace is locked by run {outcome['holder']!r} "
            f"(mutation in progress); retry after it completes or expires"
        )
    if decision == "proceed":
        return
    if decision == "break_and_acquire":
        logging.warning(
            "breaking stale mutation lock of run %s", outcome["lock_doc"].get("broke_lock_of")
        )
        syncer.delete_mutation_lock()
    if not syncer.write_mutation_lock(outcome["lock_doc"]):
        current = syncer.read_mutation_lock() or {}
        raise RuntimeError(
            f"❌ ctl-state namespace lock lost to run {current.get('run_id')!r}; "
            "retry after it completes"
        )
    _MUTATION_LOCK_HELD = {"syncer": syncer, "run_id": run_id}
    logging.info("mutation lock acquired (run %s)", run_id)


def release_mutation_lock_if_held(run_id: str | None = None) -> None:
    global _MUTATION_LOCK_HELD
    held = _MUTATION_LOCK_HELD
    if held is None:
        return
    if run_id is not None and held["run_id"] != run_id:
        return
    try:
        held["syncer"].delete_mutation_lock()
        logging.info("mutation lock released (run %s)", held["run_id"])
    finally:
        _MUTATION_LOCK_HELD = None


def ctl_state_push(reason: str) -> None:
    if _CTL_STATE_SYNCER is not None:
        _CTL_STATE_SYNCER.push(reason)


def ctl_state_publish_committed(pointer_path: Path) -> None:
    if _CTL_STATE_SYNCER is not None:
        _CTL_STATE_SYNCER.publish_committed_pointer(pointer_path)


def ctl_state_sync_summary() -> dict[str, str]:
    if _CTL_STATE_SYNCER is not None:
        return _CTL_STATE_SYNCER.summary()
    return dict(_CTL_STATE_SYNC_NOTE)


def _step_utils_module(name: str):
    """Import a step_utils/ctl python module by file path (shared primitives:
    Resolver, merge_values, cfg-entry refs). The module stays self-contained in
    step_utils because it also executes inside target_run containers."""
    import importlib.util
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, source_step_utils_dir() / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def render_scope_tree(scope_dir: Path, dest_dir: Path, env_ctx: dict) -> None:
    """Render one scope: merge all scope YAML for lookups, interpolate,
    normalize cfg-entry refs whole-scope, write back per-file YAML, copy
    non-YAML verbatim. Engine logic (folded from the former target_run-side
    render_cfg.py)."""
    brc = _step_utils_module("build_runtime_cfg")
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
    no subprocess, no target_run costume."""
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
    """Distribute target_run input views from the rendered tree (in-process engine
    step — folded from the former dockerized prepare/cfg target_run).

    Single derivation chain: rendered/ derives from merged/; each
    target_runs/<target_run>/input/ view is selected from rendered/ only.
    """
    plt_targets_dir_path = run_dir / "cfg" / "plt" / "targets"
    cfg = load_yaml(pipeline_run_cfg_path) or {}
    target_runs = cfg.get("target_runs") or {}
    if not isinstance(target_runs, dict):
        raise RuntimeError("pipeline_run_cfg.yaml target_runs must be a mapping")
    plt_targets_dir_path.mkdir(parents=True, exist_ok=True)

    for target_run_name, target_run_cfg in target_runs.items():
        if not isinstance(target_run_cfg, dict):
            raise RuntimeError(f"Target run {target_run_name!r} config must be a mapping")
        cfg_files = target_run_cfg.get("cfg_files") or []
        if not cfg_files:
            continue
        if not isinstance(cfg_files, list):
            raise RuntimeError(f"Target run {target_run_name!r} cfg_files must be a list")

        cfg_root = normalize_cfg_absolute_path(
            target_run_cfg.get("cfg_root", "/"), label=f"target_run {target_run_name!r} cfg_root", allow_root=False
        )
        if len([part for part in cfg_root.split("/") if part]) != 1:
            raise RuntimeError(
                f"Target run {target_run_name!r} cfg_root must be exactly one top-level scope "
                f"(a single path segment), not {cfg_root!r} — a target_run may not span scopes"
            )
        scope_root = cfg_abs_path_to_dir(plt_rendered_dir, cfg_root, label=f"target_run {target_run_name!r} cfg_root")
        if not scope_root.is_dir():
            logging.info("[WARN] cfg root %r not found for target_run %r: %s", cfg_root, target_run_name, scope_root)
            continue

        target_input_dir = plt_targets_dir_path / target_run_name / "input"
        target_input_dir.mkdir(parents=True, exist_ok=True)

        for pattern in cfg_files:
            if not isinstance(pattern, str) or not pattern.strip():
                raise RuntimeError(f"Target run {target_run_name!r} cfg_files entries must be non-empty strings")
            pattern_norm = pattern.strip().lstrip("/")
            if pattern_norm == "*":
                sources = [p for p in scope_root.iterdir()]
            elif pattern_norm.endswith("/*"):
                src_dir = cfg_abs_path_to_dir(scope_root, "/" + pattern_norm[:-2], label=f"target_run {target_run_name!r} cfg_files pattern")
                if not src_dir.is_dir():
                    logging.info("[WARN] cfg dir %r not found under %s", pattern_norm, cfg_root)
                    continue
                sources = [p for p in src_dir.iterdir()]
            else:
                sources = [cfg_abs_path_to_dir(scope_root, "/" + pattern_norm, label=f"target_run {target_run_name!r} cfg_files entry")]

            for src in sources:
                if not src.exists():
                    logging.info("[WARN] cfg entry does not exist under %s: %s", cfg_root, src)
                    continue
                rel = src.relative_to(scope_root)
                dst = target_input_dir / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                if src.is_dir():
                    if dst.exists():
                        shutil.rmtree(dst)
                    shutil.copytree(dst if False else src, dst)
                else:
                    shutil.copy2(src, dst)

    logging.info("Prepared target_run input cfg views under %s", plt_targets_dir_path)
    return plt_targets_dir_path



def _remove_path(path: Path) -> None:
    """Remove an existing file, directory, or symlink."""
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def materialize_target_modules(target_run_id: str, target_run: dict, repo_path: Path) -> None:
    """Populate target_run-local child modules before setup runs."""
    modules = target_run.get("modules") or {}
    if not modules:
        return

    repo_root = repo_path.resolve()
    for module_name, module_cfg in modules.items():
        dest_path = repo_path / module_cfg["dest"]
        try:
            dest_path.relative_to(repo_path)
        except ValueError as exc:
            raise RuntimeError(
                f"Target run '{target_run_id}' module '{module_name}' dest escapes the target_run repo: {module_cfg['dest']}"
            ) from exc

        if dest_path.exists() or dest_path.is_symlink():
            _remove_path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if "repo_path" in module_cfg:
            module_src = Path(module_cfg["repo_path"]).expanduser()
            if not module_src.is_dir():
                raise RuntimeError(
                    f"Target run '{target_run_id}' module '{module_name}' repo_path not found: {module_src}"
                )
            # Copy the local working tree snapshot so Dockerized target_run runners can read it.
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


def source_step_utils_dir() -> Path:
    utils_dir = ctl_utils_root() / "step_utils"
    if not utils_dir.is_dir():
        raise RuntimeError(f"❌ step utils source dir not found: {utils_dir}")
    return utils_dir


def materialize_step_utils(run_dir: Path) -> Path:
    """Copy the ctl-owned target_run support scripts into this run's step_utils area.

    Rule: step_utils/ctl holds only files consumed by target_runs (host wrappers,
    in-container setup, the per-target_run resolver, access assert, dockerfiles).
    """
    utils_dir = run_dir / "step_utils" / "ctl"
    if utils_dir.is_dir():
        return utils_dir
    utils_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        source_step_utils_dir(),
        utils_dir,
        symlinks=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return utils_dir


def prepare_target_repo(
    target_run_id: str,
    target_run: dict,
    run_dir: Path,
    tooling_env: dict[str, str],
    provider_adapter=None,
    provider_catalogs: dict | None = None,
    execution_context: dict[str, object] | None = None,
    provider_implementation_key: str | None = None,
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
) -> tuple[Path, dict[str, str]]:
    """Clone/copy a target_run repo, materialize child modules, and prepare its execution env."""
    repo_path = run_dir / "target_sources" / target_run_id
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)

    if "repo_path" in target_run:
        repo_path_value = target_run["repo_path"]
        if not repo_path_value:
            raise RuntimeError(f"Target run '{target_run_id}' has empty repo_path")
        repo_src = Path(repo_path_value).expanduser()
        if not repo_src.is_dir():
            raise RuntimeError(f"Target run '{target_run_id}' repo_path not found: {repo_src}")
        shutil.copytree(repo_src, repo_path, symlinks=True)
    else:
        git_clone(
            repo_url=target_run["repo_url"],
            branch=target_run["branch"],
            commit=target_run["commit"],
            dest=repo_path,
            token=os.getenv(target_run["token_type"]),
        )

    materialize_target_modules(target_run_id, target_run, repo_path)

    target_env = os.environ.copy()
    target_env.update(tooling_env)
    target_env["ATLAS_STEP_UTILS_DIR"] = str(materialize_step_utils(run_dir).parent)
    if provider_adapter is not None:
        if provider_catalogs is None or execution_context is None or provider_implementation_key is None:
            raise RuntimeError("❌ incomplete provider inputs for target_run preparation")
        provider_adapter.materialize_target_binding(
            target_run_id,
            target_run,
            target_env,
            provider_catalogs,
            execution_context=execution_context,
            implementation_key=provider_implementation_key,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
                run_dir=run_dir,
        )
    return repo_path, target_env


def _repo_local_active_steps(action_manifest: dict, active_ids: list[str], repo_root: Path) -> list[dict]:
    active: list[dict] = []
    for step_id in active_ids:
        entry = action_manifest.get(step_id)
        if not isinstance(entry, dict):
            raise RuntimeError(f"Step {step_id!r} not declared in manifest")
        step_path = entry.get("path")
        if not isinstance(step_path, str) or not step_path:
            raise RuntimeError(f"Step {step_id!r} manifest entry must define a non-empty path")

        step_meta_path = repo_root / step_path / "step.yaml"
        if not step_meta_path.is_file():
            raise RuntimeError(f"Step metadata not found: {step_meta_path}")
        step_meta = load_yaml(step_meta_path) or {}
        runtime_cfg = step_meta.get("runtime") or {}
        if not isinstance(runtime_cfg, dict):
            raise RuntimeError(f"Step metadata runtime must be a mapping: {step_meta_path}")
        values_json = runtime_cfg.get("values_json", True)
        env_sh = runtime_cfg.get("env_sh", True)
        if not isinstance(values_json, bool) or not isinstance(env_sh, bool):
            raise RuntimeError(f"Step metadata runtime flags must be booleans: {step_meta_path}")
        # Phase 26: the step declares its BOX (image + docker capability), CTL owns
        # how the box is run. image is required; docker_build defaults false.
        image = runtime_cfg.get("image")
        if not isinstance(image, str) or image not in STEP_IMAGES:
            raise RuntimeError(
                f"Step metadata runtime.image must be one of {sorted(STEP_IMAGES)}: {step_meta_path}"
            )
        docker_build = runtime_cfg.get("docker_build", False)
        if not isinstance(docker_build, bool):
            raise RuntimeError(f"Step metadata runtime.docker_build must be a boolean: {step_meta_path}")
        supported_execution_runtime_modes = step_supported_execution_runtime_modes(runtime_cfg, label=str(step_meta_path))
        cfg_files = step_meta.get("cfg_files", [])
        if cfg_files is None:
            cfg_files = []
        if not isinstance(cfg_files, list):
            raise RuntimeError(f"Step metadata cfg_files must be a list: {step_meta_path}")

        active.append(
            {
                "id": step_id,
                "path": step_path,
                "cfg_files": cfg_files,
                "runtime": {
                    "values_json": values_json,
                    "env_sh": env_sh,
                    "image": image,
                    "docker_build": docker_build,
                    "supported_execution_runtime_modes": sorted(supported_execution_runtime_modes),
                },
                "env_vars": {
                    "inventory": {},
                    "step": step_meta.get("env_vars", {}),
                },
            }
        )
    return active


def get_repo_local_steps(repo_path: Path, action: str, step_sequence_key: str) -> tuple[list[str], list[dict]]:
    manifest_file = repo_path / ADAPTER_DIR / "manifest.yaml"
    if not manifest_file.is_file():
        raise RuntimeError(f"❌ manifest file not found: {manifest_file}")
    step_sequences_file = repo_path / ADAPTER_DIR / "step_sequences.yaml"
    if not step_sequences_file.is_file():
        raise RuntimeError(f"❌ step_sequences file not found: {step_sequences_file}")

    manifest = (load_yaml(manifest_file) or {}).get("manifest", {})
    step_sequences = (load_yaml(step_sequences_file) or {}).get("step_sequences", {})

    action_manifest = manifest.get(action)
    if not isinstance(action_manifest, dict) or not action_manifest:
        raise RuntimeError(f"manifest {manifest_file} declares no steps for action {action!r}")

    action_step_sequences = step_sequences.get(action)
    if not isinstance(action_step_sequences, dict) or step_sequence_key not in action_step_sequences:
        raise RuntimeError(f"step sequence {action}/{step_sequence_key} not found in {step_sequences_file}")
    step_sequence = action_step_sequences[step_sequence_key]
    if not isinstance(step_sequence, dict) or "steps" not in step_sequence:
        raise RuntimeError(f"step sequence {action}/{step_sequence_key} must define steps")

    active_ids: list[str] = []
    for step_id in step_sequence.get("steps", []):
        if step_id not in action_manifest:
            raise RuntimeError(f"Step {step_id!r} not declared in manifest for action {action!r}")
        active_ids.append(step_id)

    return active_ids, _repo_local_active_steps(action_manifest, active_ids, repo_path)


def ensure_repo_execution_context(repo_path: Path, execution_context_path: Path) -> bool:
    repo_execution_context_path = repo_path / EXECUTION_CONTEXT_FILENAME
    if execution_context_path.resolve() == repo_execution_context_path.resolve():
        return False
    shutil.copy2(execution_context_path, repo_execution_context_path)
    return True


def resolve_force_unlock_tfstate_binding(
    repo_path: Path, action: str, step_sequence_key: str
) -> tuple[str, str, list[str]]:
    """Resolve one Terraform project, state-key variable, and cfg-file set."""
    _, repo_steps = get_repo_local_steps(repo_path, action, step_sequence_key)
    bindings: dict[tuple[str, str], list[str]] = {}

    for repo_step in repo_steps:
        script_path = repo_path / repo_step["path"] / "src" / "step.sh"
        if not script_path.is_file():
            raise RuntimeError(f"❌ Step script not found: {script_path}")
        content = script_path.read_text(encoding="utf-8")
        for match in FORCE_UNLOCK_INIT_RE.finditer(content):
            stack_dir = match.group("stack_dir")
            stack_path = Path(stack_dir)
            if stack_path.is_absolute() or ".." in stack_path.parts:
                raise RuntimeError(
                    "❌ force-unlock Terraform project must be a safe relative "
                    f"path: {stack_dir!r} in {script_path}"
                )
            if not (repo_path / stack_path).is_dir():
                raise RuntimeError(
                    "❌ force-unlock Terraform project does not exist: "
                    f"{stack_dir!r} from {script_path}"
                )
            binding = (stack_dir, match.group("state_key"))
            cfg_files = bindings.setdefault(binding, [])
            for cfg_file in repo_step["cfg_files"]:
                if cfg_file not in cfg_files:
                    cfg_files.append(cfg_file)

    if not bindings:
        raise RuntimeError(
            f"❌ force-unlock is not supported for target_run repo {str(repo_path)!r}: "
            f"selected step sequence {action!r}/{step_sequence_key!r} has no step "
            "calling ./bin/tf.sh <terraform-project> init <tfstate-key-variable>"
        )
    if len(bindings) != 1:
        rendered = ", ".join(
            f"{stack_dir}:{state_key}" for stack_dir, state_key in sorted(bindings)
        )
        raise RuntimeError(
            "❌ force-unlock requires exactly one Terraform state binding for "
            f"{action!r}/{step_sequence_key!r}; found: {rendered}"
        )
    binding = next(iter(bindings))
    return binding[0], binding[1], bindings[binding] or ["*"]


def _step_box_name(target_run_id: str, repo_step_id: str) -> str:
    """A valid, unique-per-run Docker tag / box name for a target_run (§Phase 26)."""
    raw = f"atlas-{target_run_id}-{repo_step_id}-target_run-local"
    name = re.sub(r"[^a-z0-9._-]+", "-", raw.lower())
    return re.sub(r"-{2,}", "-", name).strip("-")


def git_source_facts(path: Path) -> tuple[str | None, str]:
    """Return the checked-out commit and reproducibility state of one cfg source."""
    root = Path(path)
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
    )
    if commit.returncode != 0:
        return None, "dirty"
    status = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"],
        capture_output=True,
        text=True,
    )
    state = "clean" if status.returncode == 0 and not status.stdout.strip() else "dirty"
    return commit.stdout.strip(), state


def target_run_source_facts(target_run: dict) -> tuple[str | None, str]:
    commit = target_run.get("commit")
    repo_path = target_run.get("repo_path")
    if repo_path:
        actual_commit, state = git_source_facts(Path(repo_path))
        return (str(commit).strip() if commit else actual_commit), state
    return (str(commit).strip() if commit else None), ("clean" if commit else "dirty")


def target_instance_dir_for_run(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
) -> tuple[Path, str]:
    metadata = load_run_metadata(parent_run_dir)
    target_key = normalize_result_name(
        target_run.get("target"), label="workflow target key"
    )
    segments = resolve_target_instance_segments(
        target_run.get("target_instance_params"),
        execution_context,
        label=f"target {target_key}",
    )
    namespace_root = Path(metadata["ctl_state_local_root"]).joinpath(
        *(metadata.get("ctl_state_locator") or [])
    )
    return (
        namespace_root
        / compose_state_relpath(
            str(metadata["action"]), "target", target_key, segments
        ),
        target_instance_address(target_key, segments),
    )


def committed_target_revision_if_skippable(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
) -> dict | None:
    """Return the current committed child revision only under the Q1d contract."""
    if target_run.get("ref_policy") != "commit_required":
        return None
    if target_run.get("source_state") != "clean":
        return None
    source_commit = target_run.get("source_commit")
    cfg_source_commit = target_run.get("cfg_source_commit")
    if not source_commit or not cfg_source_commit:
        return None
    instance_dir, address = target_instance_dir_for_run(
        parent_run_dir, target_run, execution_context
    )
    pointer = read_committed_pointer(instance_dir)
    if not pointer or pointer.get("status") == "outdated" or pointer.get("outdated"):
        return None
    expected = {
        "source_commit": source_commit,
        "cfg_source_commit": cfg_source_commit,
        "source_state": "clean",
        "ref_policy": "commit_required",
    }
    if any(pointer.get(key) != value for key, value in expected.items()):
        return None
    snapshot_path = (
        instance_dir / "runs" / str(pointer.get("run_id") or "") / RUN_METADATA_FILENAME
    )
    if not snapshot_path.is_file():
        return None
    snapshot = load_yaml(snapshot_path) or {}
    if not isinstance(snapshot, dict):
        return None
    canonical = json.dumps(
        snapshot, separators=(",", ":"), sort_keys=True, default=str
    )
    if hashlib.sha256(canonical.encode("utf-8")).hexdigest() != pointer.get(
        "snapshot_sha256"
    ):
        return None
    return {
        "address": address,
        "run_id": pointer.get("run_id"),
        "snapshot_sha256": pointer.get("snapshot_sha256"),
        "status": pointer.get("status"),
        "skipped_committed_rerun": True,
    }


def begin_workflow_target_run(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
) -> tuple[Path, str | None]:
    """Materialize one workflow-selected target as its canonical target run."""
    parent_metadata = load_run_metadata(parent_run_dir)
    if parent_metadata.get("run_type") != "workflow":
        return parent_run_dir, None
    target_key = normalize_result_name(
        target_run.get("target"), label="workflow target key"
    )
    segments = resolve_target_instance_segments(
        target_run.get("target_instance_params"),
        execution_context,
        label=f"target {target_key}",
    )
    address = target_instance_address(target_key, segments)
    ctl_state_root = Path(parent_metadata["ctl_state_local_root"])
    locator = list(parent_metadata.get("ctl_state_locator") or [])
    namespace_root = ctl_state_root.joinpath(*locator)
    instance_dir = namespace_root / compose_state_relpath(
        str(parent_metadata["action"]), "target", target_key, segments
    )
    # a target instance's identity is fully encoded in its path
    # (<key>/instances/<seg>/…) — no identity.yaml is written (§minimal files).
    child_run_id = generate_uuid7()
    child_run_dir = instance_dir / "runs" / child_run_id
    write_run_metadata(
        child_run_dir,
        {
            "run_id": child_run_id,
            "action": parent_metadata["action"],
            "run_type": "target",
            "result_name": target_key,
            "result_key": f"{parent_metadata['action']}/target/{target_key}",
            "ctl_state_local_root": str(ctl_state_root),
            "ctl_state_locator": locator,
            "ctl_state_namespace": parent_metadata.get("ctl_state_namespace"),
            "ctl_state_dir": str(instance_dir),
            "run_dir": str(child_run_dir),
            "log_path": parent_metadata.get("log_path"),
            "target_keys": [target_key],
            "instance": segments,
            "instance_address": address,
            "parent_workflow_run_id": parent_metadata.get("run_id"),
            "fan_out_run_id": parent_metadata.get("fan_out_run_id"),
            "mutation_started": False,
            **{
                key: target_run[key]
                for key in (
                    "source_commit", "cfg_source_commit", "source_state", "ref_policy"
                )
                if target_run.get(key) is not None
            },
        },
    )
    mark_run_started(child_run_dir)
    return child_run_dir, address


def finish_workflow_target_run(
    child_run_dir: Path, *, error: BaseException | None = None
) -> dict | None:
    """Finalize and publish one workflow child without publishing the workflow."""
    if error is not None:
        payload = build_status_payload(
            child_run_dir, "failed",
            {"error": {"type": type(error).__name__, "summary": str(error)}},
        )
        write_current_status(child_run_dir, payload)
        write_state_slot(child_run_dir, "failed", payload)
        remove_state_slot(child_run_dir, "in_progress")
        publish_or_queue_ctl_state_run(
            child_run_dir, None, reason="workflow child failed"
        )
        return None

    payload = build_status_payload(
        child_run_dir, "ok", {"ctl_state_sync": ctl_state_sync_summary()}
    )
    write_current_status(child_run_dir, payload)
    pointer_path = publish_committed_pointer(child_run_dir, payload)
    remove_state_slot(child_run_dir, "in_progress")
    remove_state_slot(child_run_dir, "failed")
    publish_or_queue_ctl_state_run(
        child_run_dir,
        pointer_path,
        reason="workflow child succeeded",
    )
    pointer = read_committed_pointer(ctl_state_dir_from_run_dir(child_run_dir)) or {}
    return {
        "address": payload.get("instance_address") or payload.get("result_name"),
        "run_id": pointer.get("run_id"),
        "snapshot_sha256": pointer.get("snapshot_sha256"),
        "status": pointer.get("status"),
    }


def populate_workflow_child_slice(
    child_run_dir: Path,
    target_run: dict,
    target_run_id: str,
    plt_targets_dir_path: Path,
    execution_context: dict[str, object],
) -> None:
    """§Phase 49: make a workflow-child target run self-contained AT THE TARGET
    LEVEL — its own rendered cfg (input + resolved), frozen execution context,
    and the source refs it ran against — so a target result is independently
    inspectable without walking to the parent workflow run. Workflow-WIDE
    artifacts (whole-workflow plan, resolved flow, orchestrator logs) stay under
    the parent run, which the child references by `parent_workflow_run_id`.
    Additive: it only writes into the child run dir, never the workflow run."""
    write_execution_context_artifact(child_run_dir, execution_context)
    cfg_dst = child_run_dir / "cfg"
    for view in ("input", "resolved"):
        src = plt_targets_dir_path / target_run_id / view
        if src.is_dir():
            shutil.copytree(src, cfg_dst / view, dirs_exist_ok=True)
    source_refs = {
        key: target_run[key]
        for key in ("source", "ref", "branch", "commit", "step_sequence")
        if target_run.get(key) is not None
    }
    if source_refs:
        write_yaml_file(child_run_dir / "target_sources" / "source_refs.yaml", source_refs)


def run_steps(
    active_target_runs: dict,
    run_dir: Path,
    plt_targets_dir_path: Path,
    execution_context_path: Path,
    inventory_name: str,
    execution_context: dict[str, object],
    run_id: str,
    tooling_refs: dict,
    use_local_tooling_cfg: bool,
    provider_adapter,
    provider_catalogs: dict,
    provider_implementation_key: str,
    execution_runtime_mode: str,  # required, no default — the CLI (--execution-runtime-mode) supplies it
    execution_access_mode: str = "standard",
    provider_credential: str | None = None,
    skip_committed_rerun: bool = False,
) -> None:
    """Clone and run all active target runs."""
    os.chdir(run_dir)
    tooling_env = build_tooling_env(tooling_refs)
    # Phase 26: CTL owns the execution box. It invokes the ctl-owned runtime
    # dispatcher (run_step.sh) — never a per-target_run run script — passing the box
    # spec the target_run declared (image / docker_build) plus the active runtime and
    # tooling source. The target_run carries only src/step.sh + step.yaml.
    runtime_dispatcher = str(materialize_step_utils(run_dir) / "run_step.sh")
    tooling_mode = "repo_path" if use_local_tooling_cfg else "repo_url"
    mutation_marked = False
    child_revisions: list[dict] = []
    for target_run_id, target_run in active_target_runs.items():
        log_target_run_banner(f"[{inventory_name}] [{target_run_id}]")
        if skip_committed_rerun:
            revision = committed_target_revision_if_skippable(
                run_dir, target_run, execution_context
            )
            if revision is not None:
                logging.info(
                    "Skipping committed target instance %s (commits unchanged)",
                    revision["address"],
                )
                child_revisions.append(revision)
                continue
        repo_path, target_env = prepare_target_repo(
            target_run_id,
            target_run,
            run_dir,
            tooling_env,
            provider_adapter=provider_adapter,
            provider_catalogs=provider_catalogs,
            execution_context=execution_context,
            provider_implementation_key=provider_implementation_key,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
            )

        step_sequence_key = target_run.get("step_sequence")
        if not isinstance(step_sequence_key, str) or not step_sequence_key:
            raise RuntimeError(f"❌ target run {target_run_id!r} must define a non-empty step_sequence")
        origin_cfg_path = plt_targets_dir_path / target_run_id / "input"
        if not origin_cfg_path.is_dir():
            raise RuntimeError(f"❌ target_run input cfg dir not found for target_run {target_run_id!r}: {origin_cfg_path}")
        target_cfg_dir = plt_targets_dir_path / target_run_id / "resolved"
        os.makedirs(target_cfg_dir, exist_ok=True)
        target_state_run_dir, target_instance_address = begin_workflow_target_run(
            run_dir, target_run, execution_context
        )
        target_artifacts_dir = (
            target_state_run_dir / "artifacts"
            if target_instance_address is not None
            else run_dir / "artifacts" / "targets" / target_run_id
        )
        os.makedirs(target_artifacts_dir, exist_ok=True)

        copied_execution_context = ensure_repo_execution_context(repo_path, execution_context_path)
        try:
            repo_step_ids, repo_steps = get_repo_local_steps(repo_path, inventory_name, step_sequence_key)
            run_manifest = {
                "run_id": run_id,
                "branch": target_run.get("branch"),
                "commit": target_run.get("commit"),
                "action": inventory_name,
                "step_sequence": step_sequence_key,
                "active_steps": repo_step_ids,
                "origin_cfg": str(origin_cfg_path),
                "execution_context_file": str(execution_context_path),
                "execution_context_keys": sorted(execution_context),
            }
            logging.info(json.dumps(run_manifest, indent=4))

            if inventory_name in MUTATING_ACTIONS and not mutation_marked:
                mark_mutation_started(run_dir, target_run_id)
                mutation_marked = True

            for repo_step in repo_steps:
                repo_step_id = repo_step["id"]
                repo_step_path = repo_step["path"]
                log_target_run_banner(f"[{inventory_name}] [{target_run_id}] [{repo_step_id}]", ch="-")
                repo_step_runtime = repo_step.get("runtime", {})
                supported = set(repo_step_runtime.get("supported_execution_runtime_modes", EXECUTION_RUNTIME_MODES))
                if execution_runtime_mode not in supported:
                    raise RuntimeError(
                        f"❌ execution runtime {execution_runtime_mode!r} not supported by target_run "
                        f"{target_run_id}/{repo_step_id} (supported: {sorted(supported)})"
                    )
                step_run_cmd = [runtime_dispatcher]
                repo_step_env = dict(target_env)
                repo_step_env["ATLAS_EXECUTION_CONTEXT_FILE"] = EXECUTION_CONTEXT_FILENAME
                repo_step_env["cfg_files"] = json.dumps(repo_step.get("cfg_files"))
                repo_step_env["STEP_WRITE_VALUES_JSON"] = (
                    "true" if repo_step_runtime.get("values_json", True) else "false"
                )
                repo_step_env["STEP_WRITE_ENV_SH"] = (
                    "true" if repo_step_runtime.get("env_sh", True) else "false"
                )
                repo_step_env["origin_cfg_base_dir_path"] = str(origin_cfg_path)
                repo_step_env["TARGET_CFG_DIR"] = str(target_cfg_dir)
                repo_step_env["TARGET_ARTIFACTS_DIR"] = str(target_artifacts_dir)
                # Phase 26: CTL owns the box; hand the dispatcher the runtime + the
                # target_run's declared box spec. step_dir locates src/step.sh in the repo.
                repo_step_env["ATLAS_EXECUTION_RUNTIME_MODE"] = execution_runtime_mode
                repo_step_env["ATLAS_STEP_NAME"] = _step_box_name(target_run_id, repo_step_id)
                repo_step_env["ATLAS_STEP_IMAGE"] = repo_step_runtime["image"]
                repo_step_env["ATLAS_STEP_DOCKER_BUILD"] = (
                    "true" if repo_step_runtime.get("docker_build", False) else "false"
                )
                repo_step_env["step_dir"] = repo_step_path
                repo_step_env["local_step_tooling_mode"] = tooling_mode

                logging.info(" ".join(step_run_cmd))
                run_and_log(
                    step_run_cmd,
                    cwd=repo_path,
                    env=repo_step_env,
                )
            ctl_state_push(f"target_run {target_run_id} completed")
            if target_instance_address is not None:
                # §Phase 49: fill the child's target-level slice (cfg, execution
                # context, source refs) now that resolved cfg exists — before the
                # child pointer is published.
                populate_workflow_child_slice(
                    target_state_run_dir,
                    target_run,
                    target_run_id,
                    plt_targets_dir_path,
                    execution_context,
                )
                revision = finish_workflow_target_run(target_state_run_dir)
                if revision is not None:
                    child_revisions.append(revision)
        except BaseException as error:
            if target_instance_address is not None:
                finish_workflow_target_run(target_state_run_dir, error=error)
            raise
        finally:
            repo_execution_context_path = repo_path / EXECUTION_CONTEXT_FILENAME
            if copied_execution_context and repo_execution_context_path.is_file():
                repo_execution_context_path.unlink()

    if child_revisions:
        update_run_metadata(run_dir, {"child_revisions": child_revisions})


def print_run_summary(run_id: str, log_file: Path) -> None:
    """Print run summary at the end."""
    print(f"Run id: {run_id}")
    print(f"Log file: {log_file}")


def run_maintenance(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    ctl_state_local_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    inventory_name: str,
    maintenance_action: str,
    target_key: str,
    lock_id: str,
    run_id: str,
    plt_overlays: list[str],
    target_repo_key: str,
    require_target_ref: bool,
    use_local_tooling_cfg: bool,
    provider_implementation_key: str,
    run_dir: Path,
    artifacts_dir: Path,
    plt_merged_dir: Path,
    log_file: Path,
    provider_credential: str | None,
    execution_runtime_mode: str,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_access_mode: str = "standard",
) -> None:
    """Run a maintenance action against a single target_run target."""
    if (
        maintenance_action == "force-unlock"
        and force_unlock_resource_kind(target_key) == "ctl_state"
        and force_unlock_ctl_state_lock(ctl_state_local_root, lock_id, run_dir)
    ):
        print_run_summary(run_id, log_file)
        return

    execution_context = build_execution_context(
        ctl_cfg_root,
        action=inventory_name,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        execution_access_mode=execution_access_mode,
        execution_runtime_mode=execution_runtime_mode,
    )
    scope_params = scope_params_from_context(execution_context)
    validate_execution_context_constraints(ctl_cfg_root, execution_context)
    inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name, execution_context)
    maintenance_workflow_cfg = {"target_runs": [{"target": target_key}]}
    validate_target_policy_constraints(ctl_cfg_root, ctl_profile, maintenance_workflow_cfg, inventory_cfg)
    validate_execution_access(
        ctl_cfg_root,
        ctl_profile,
        maintenance_workflow_cfg,
        inventory_cfg,
        execution_context=execution_context,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
    )
    cfg_report = build_cfg_validation_report(
        collect_provider_cfg_findings(ctl_cfg_root, execution_context)
    )
    apply_full_cfg_validation_gate(
        cfg_report, force_skip=force_skip_full_cfg_validation_gate
    )
    write_cfg_validation_artifacts(artifacts_dir, cfg_report)
    assert_full_cfg_validation_gate_accepted(cfg_report)
    ctl_state_namespace_key, _ = resolve_ctl_state_namespace(
        ctl_cfg_root, execution_context
    )
    verify_ctl_guardrails(
        ctl_cfg_root,
        guardrails_cfg_root,
        execution_context,
    )
    configure_ctl_state_sync(
        ctl_cfg_root,
        ctl_profile,
        ctl_state_namespace_key,
        execution_context,
        run_dir,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        provider_implementation_key=provider_implementation_key,
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
            "name": f"{ctl_profile}/{inventory_name}/maintenance/{maintenance_action}/{target_key}",
            "inventory": inventory_name,
        },
        "target_runs": [
            {
                "id": target_key,
                "target": target_key,
            }
        ],
    }
    validate_workflow_target_selectors(workflow_cfg, inventory_cfg, execution_context)

    active_target_runs, pipeline_run_cfg_path = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
        plt_merged_dir,
        artifacts_dir,
        ctl_profile,
        plt_overlays,
        scope_params=scope_params,
        execution_context=execution_context,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )
    record_run_target_keys(run_dir, target_keys_from_active_target_runs(active_target_runs))
    plt_rendered_dir = render_plt_cfg(plt_merged_dir, run_dir, execution_context)
    verify_guardrails(
        ctl_cfg_root,
        plt_cfg_root,
        guardrails_cfg_root,
        plt_rendered_dir,
        execution_context,
        scope_params,
        required_target_paths=required_target_paths_for_target_runs(active_target_runs),
    )

    validate_target_runs_have_commits(active_target_runs, ctl_ref_policy)
    provider_adapter = run_provider_adapter(execution_context)
    provider_catalogs = provider_adapter.load_runtime_catalogs(
        ctl_cfg_root, execution_context=execution_context
    )
    provider_adapter.validate_active_target_access(
        active_target_runs,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
    )
    write_git_metas(ctl_cfg_root, plt_cfg_root, guardrails_cfg_root, artifacts_dir)
    plt_targets_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path,
        plt_rendered_dir,
        run_dir,
    )

    os.chdir(run_dir)
    tooling_env = build_tooling_env(tooling_refs)
    if len(active_target_runs) != 1:
        raise RuntimeError(
            f"❌ maintenance action '{maintenance_action}' expected exactly one active target_run, got: {list(active_target_runs)}"
        )

    target_run_id, target_run = next(iter(active_target_runs.items()))
    log_target_run_banner(f"[{inventory_name}] [maintenance/{maintenance_action}/{target_run_id}]")
    repo_path, target_env = prepare_target_repo(
        target_run_id,
        target_run,
        run_dir,
        tooling_env,
        provider_adapter=provider_adapter,
        provider_catalogs=provider_catalogs,
        execution_context=execution_context,
        provider_implementation_key=provider_implementation_key,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
    )
    assertion_argv = provider_adapter.target_assertion_argv(materialize_step_utils(run_dir))
    if assertion_argv:
        run_and_log(assertion_argv, cwd=repo_path, env=target_env)

    target_cfg_dir = plt_targets_dir_path / target_run_id / "input"
    if not target_cfg_dir.is_dir():
        raise RuntimeError(f"❌ target_run input cfg dir not found for target_run '{target_run_id}': {target_cfg_dir}")

    if maintenance_action != "force-unlock":
        raise RuntimeError(f"❌ Unsupported maintenance action: {maintenance_action}")

    step_sequence_key = target_run.get("step_sequence")
    if not isinstance(step_sequence_key, str) or not step_sequence_key:
        raise RuntimeError(
            f"❌ target run {target_run_id!r} must define a non-empty step_sequence"
        )
    tf_stack_dir, tfstate_key_var, maintenance_cfg_files = (
        resolve_force_unlock_tfstate_binding(
            repo_path, inventory_name, step_sequence_key
        )
    )
    target_env["GITHUB_WORKSPACE"] = str(repo_path)
    target_env["MAINTENANCE_TARGET_CFG_DIR"] = str(target_cfg_dir)
    target_env["MAINTENANCE_CFG_FILES_JSON"] = json.dumps(
        maintenance_cfg_files, separators=(",", ":")
    )
    target_env["TF_STACK_DIR"] = tf_stack_dir
    target_env["TFSTATE_KEY_VAR"] = tfstate_key_var
    target_env["LOCK_ID"] = lock_id
    execution_context_repo_path = repo_path / EXECUTION_CONTEXT_FILENAME
    shutil.copy2(execution_context_path, execution_context_repo_path)
    target_env["ATLAS_EXECUTION_CONTEXT_FILE"] = EXECUTION_CONTEXT_FILENAME

    maintenance_cmd = [
        "bash",
        "-lc",
        """
set -euo pipefail
source "$ATLAS_STEP_UTILS_DIR/ctl/prepare_step_runtime.sh"
prepare_step_runtime "${MAINTENANCE_TARGET_CFG_DIR}" "${MAINTENANCE_CFG_FILES_JSON}"
./bin/tf.sh "$TF_STACK_DIR" init "$TFSTATE_KEY_VAR"
./bin/tf.sh "$TF_STACK_DIR" force-unlock "$TFSTATE_KEY_VAR" "$LOCK_ID"
""",
    ]
    logging.info("bash -lc <force-unlock-script>")
    run_and_log(
        maintenance_cmd,
        cwd=repo_path,
        env=target_env,
    )

    print_run_summary(run_id, log_file)


# §Phase 31 Q1g/Q1j — target-instance identity path contract.
# Names and values in a Hive-style instance segment must match this charset
# verbatim (no percent-encoding, no sha fallback for targets); the whole
# instance suffix is capped so it stays well inside the S3 1024-byte key limit.
INSTANCE_TOKEN_RE = re.compile(r"[a-z0-9_.-]+")
INSTANCE_SUFFIX_MAX = 128


def resolve_target_instance_segments(
    target_instance_params, execution_context: dict[str, object], *, label: str
) -> list[str]:
    """Resolve declared target_instance_params to Hive-style path segments in
    declaration order (§Phase 31 Q1j): `["account=dev", "env_type=dev"]`.

    Empty/absent params => a singleton target (no instances/ layer, `[]`).
    Each param name and value must match INSTANCE_TOKEN_RE verbatim and be
    present in the execution context; the joined suffix is capped at
    INSTANCE_SUFFIX_MAX (hard error, never a sha fallback — Q1g)."""
    if target_instance_params is None:
        return []
    if not isinstance(target_instance_params, list):
        raise RuntimeError(f"❌ {label}: target_instance_params must be a list")
    segments: list[str] = []
    seen: set[str] = set()
    for param in target_instance_params:
        if not isinstance(param, str) or not INSTANCE_TOKEN_RE.fullmatch(param):
            raise RuntimeError(
                f"❌ {label}: target_instance_params name {param!r} must match [a-z0-9_.-]+"
            )
        if param in seen:
            raise RuntimeError(f"❌ {label}: target_instance_params lists {param!r} twice")
        seen.add(param)
        ref = f"{EXECUTION_CONTEXT_ROOT}.params.{param}"
        if ref not in execution_context:
            raise RuntimeError(
                f"❌ {label}: target_instance_params {param!r} is not in the execution context "
                "(instance identity params must be bound)"
            )
        value = str(execution_context[ref])
        if not INSTANCE_TOKEN_RE.fullmatch(value):
            raise RuntimeError(
                f"❌ {label}: instance value {param}={value!r} must match [a-z0-9_.-]+ "
                "(no percent-encoding; shorten or rename the value)"
            )
        segments.append(f"{param}={value}")
    suffix = "/".join(segments)
    if len(suffix) > INSTANCE_SUFFIX_MAX:
        raise RuntimeError(
            f"❌ {label}: instance suffix {suffix!r} exceeds {INSTANCE_SUFFIX_MAX} chars "
            "(shorten instance param values — no sha fallback for target instances, Q1g)"
        )
    return segments


# §Phase 31 Q1b — interim global mutation lock (deliberate tech debt, see
# tech-debt.md "Ctl-state locking"): ONE lock object per namespace at
# locks/mutation.yaml. Mutating runs acquire it exclusively (conditional
# create); non-mutating runs only check it and fail fast. Stale locks (past
# expires_at) may be broken; the breaker records broke_lock_of.
MUTATION_LOCK_RELPATH = "locks/mutation.yaml"
MUTATING_ACTIONS = ("provision", "destroy")
MUTATION_LOCK_TTL_SECONDS = 3600


def build_mutation_lock_doc(run_id: str, action: str, *, broke_lock_of: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    doc = {
        "run_id": run_id,
        "run_type": "mutation",
        "action": action,
        "acquired_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=MUTATION_LOCK_TTL_SECONDS)).isoformat(),
    }
    if broke_lock_of:
        doc["broke_lock_of"] = broke_lock_of
    return doc


def mutation_lock_is_stale(lock_doc: dict, *, now: datetime | None = None) -> bool:
    expires = lock_doc.get("expires_at")
    if not isinstance(expires, str):
        return True  # malformed locks are breakable, not deadlocks
    try:
        expiry = datetime.fromisoformat(expires)
    except ValueError:
        return True
    return (now or datetime.now(timezone.utc)) >= expiry


def evaluate_mutation_lock(
    existing_lock: dict | None, *, action: str, run_id: str
) -> dict:
    """Pure decision logic for the interim global mutation lock (§Phase 31 Q1b).

    Returns {decision, lock_doc?, holder?}: mutating actions ACQUIRE (or BREAK a
    stale lock, recording broke_lock_of); a live holder blocks them. Non-mutating
    actions only CHECK: they proceed when free, fail fast with the holder's run
    id while a mutation runs. The physical conditional write/read is the
    backend adapter's job."""
    mutating = action in MUTATING_ACTIONS
    if existing_lock is None:
        if mutating:
            return {"decision": "acquire", "lock_doc": build_mutation_lock_doc(run_id, action)}
        return {"decision": "proceed"}
    if mutation_lock_is_stale(existing_lock):
        if mutating:
            return {
                "decision": "break_and_acquire",
                "lock_doc": build_mutation_lock_doc(
                    run_id, action, broke_lock_of=str(existing_lock.get("run_id"))
                ),
            }
        return {"decision": "proceed"}
    holder = str(existing_lock.get("run_id"))
    return {"decision": "blocked", "holder": holder}


def target_instance_address(target_key: str, instance_segments: list[str]) -> str:
    """The canonical target-instance address (§Phase 31): `<target-key>` for a
    singleton, `<target-key>/<seg>/<seg>` for a parameterized instance — the
    SAME path form as the instance dir layout (segments contain `=`, key
    segments never do, so the split is unambiguous)."""
    if not instance_segments:
        return target_key
    return "/".join([target_key, *instance_segments])


def workflow_composition_sha256(target_instance_addresses: list[str]) -> str:
    """Workflow instance identity (§Phase 31 Q2): SHA-256 over the UTF-8 bytes
    of a whitespace-free canonical JSON array of the ORDERED resolved
    target-instance addresses, TRUNCATED to 8 hex chars. The digest is a
    deterministic index over a tiny per-namespace set (distinct workflow
    compositions), so 32 bits is ample; the accompanying identity.yaml records
    the full addresses and stays the authoritative identity source."""
    if not isinstance(target_instance_addresses, list) or not target_instance_addresses:
        raise RuntimeError("❌ workflow composition needs a non-empty ordered address list")
    for address in target_instance_addresses:
        if not isinstance(address, str) or not address.strip():
            raise RuntimeError("❌ workflow composition addresses must be non-empty strings")
    canonical = json.dumps(list(target_instance_addresses), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:8]


def build_workflow_identity_doc(
    workflow_key: str, target_instance_addresses: list[str], resolved_params: dict[str, str]
) -> dict:
    """The authoritative workflow-instance identity manifest (§Phase 31 Q2):
    facts only — the digest is never the only identity source."""
    # the composition sha is the instance DIR NAME — not duplicated here
    return {
        "workflow_instance": {
            "workflow": workflow_key,
            "targets": list(target_instance_addresses),
            "resolved_params": dict(resolved_params),
        }
    }


# §Phase 31 — the central-namespace ctl-state tree. A state owner is a target
# instance or a workflow instance; fan-outs are stateless (no bucket presence).
# Structural names never contain `=`, so they can never be mistaken for an
# instance segment (Q1j parse boundary).
RESULT_KINDS = ("target", "workflow")
STATE_STRUCTURAL_NAMES = frozenset({"runs", "committed.yaml", "identity.yaml", "locks"})


def compose_state_relpath(
    action: str, kind: str, key: str, instance_segments: list[str]
) -> Path:
    """Compose the namespace-relative instance directory for a state owner
    (§Phase 31): `<action>/<kind>/<key...>/instances/<seg>/<seg>` — or, for a
    singleton target, `<action>/<kind>/<key...>` with no instances/ layer. The
    namespace root is prepended by the caller."""
    if action not in RUN_ACTIONS:
        raise RuntimeError(f"❌ unknown action {action!r} composing state path")
    if kind not in RESULT_KINDS:
        raise RuntimeError(f"❌ unknown state kind {kind!r} (expected one of {RESULT_KINDS})")
    key_parts = [p for p in key.split("/") if p]
    if not key_parts:
        raise RuntimeError("❌ state key must be non-empty")
    parts = [action, kind, *key_parts]
    if instance_segments:
        parts += ["instances", *instance_segments]
    return Path(*parts)


def parse_state_relpath(namespace_root: Path, state_dir: Path) -> dict | None:
    """Inverse of compose_state_relpath: parse an instance directory back to its
    identity (§Phase 31). Returns None when the path is not under the namespace
    root or does not match the tree shape."""
    try:
        rel = Path(state_dir).resolve().relative_to(Path(namespace_root).resolve())
    except ValueError:
        return None
    parts = list(rel.parts)
    if len(parts) < 3 or parts[0] not in RUN_ACTIONS or parts[1] not in RESULT_KINDS:
        return None
    action, kind, rest = parts[0], parts[1], parts[2:]
    if "instances" in rest:
        idx = rest.index("instances")
        key_parts = rest[:idx]
        after = rest[idx + 1:]
        if after and after[0].startswith("sha256-"):
            # workflow composition instance: exactly one sha256-<digest> segment
            instance_segments = [after[0]]
        else:
            instance_segments, _ = split_instance_segments(after)
    else:
        # singleton: key runs until the first structural name
        key_parts = []
        for part in rest:
            if part in STATE_STRUCTURAL_NAMES:
                break
            key_parts.append(part)
        instance_segments = []
    if not key_parts:
        return None
    key = "/".join(key_parts)
    return {
        "action": action,
        "kind": kind,
        "key": key,
        "instance_segments": instance_segments,
        "instance": "/".join(instance_segments),
        "address": "/".join([kind, key, *instance_segments]),
    }


def instance_relpath(instance_segments: list[str]) -> str:
    """The `instances/<seg>/<seg>` relative path for a target instance, or ''
    for a singleton target (no instances/ layer, §Phase 31 Q1j)."""
    if not instance_segments:
        return ""
    return "/".join(["instances", *instance_segments])


def split_instance_segments(parts: list[str]) -> tuple[list[str], list[str]]:
    """Split a path fragment that begins after `instances/` into
    (instance_segments, remaining) using the deterministic `=` boundary
    (§Phase 31 Q1j): consume leading segments that contain `=`; the first
    segment without `=` is where structure (`runs`, `committed.yaml`,
    `identity.yaml`, `locks`) resumes. Structural names never contain `=`."""
    instance: list[str] = []
    for i, part in enumerate(parts):
        if "=" in part:
            instance.append(part)
        else:
            return instance, list(parts[i:])
    return instance, []


def resolve_run_locator_segments(
    ctl_cfg_root: Path,
    *,
    run_type: str,
    action: str,
    ctl_profile: str,
    execution_params: dict[str, str],
    execution_runtime_mode: str,
    workflow_name: str | None = None,
    target_name: str | None = None,
    ctl_variants: list[str] | tuple[str, ...] = (),
) -> list[str]:
    """Resolve a run's local ctl-state locator BEFORE its dirs exist (§Phase 30).

    Pure cfg resolution: the run's single ctl-state namespace maps through the
    provider adapter to the backend mirror tree the run lives in. The same
    namespace is re-resolved when the syncer is armed and must agree. Fan-out
    and namespace-less runs land under the reserved `_local` tree."""
    if run_type in ("fan_out", "step_sequence"):
        # §Phase 31: fan-outs are stateless (local artifacts only) and
        # step_sequence runs are synthetic dev-loop records — neither has a
        # bucket presence.
        return list(LOCAL_ONLY_LOCATOR)
    if run_type == "maintenance" and not target_name:
        return list(LOCAL_ONLY_LOCATOR)
    if run_type not in ("target", "workflow", "maintenance"):
        raise RuntimeError(f"❌ unknown run_type {run_type!r} for locator resolution")
    # §Phase 31: target/workflow state lives in the ONE resolved ctl-state
    # namespace tree — the local root scopes by namespace key, the synchronized
    # relative tree carries no provider locator segments.
    execution_context = build_execution_context(
        ctl_cfg_root,
        action=action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        execution_runtime_mode=execution_runtime_mode,
    )
    namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, execution_context)
    return [namespace_key]


def resolve_run_instance_identity(
    ctl_cfg_root: Path,
    *,
    run_type: str,
    action: str,
    ctl_profile: str,
    execution_params: dict[str, str],
    execution_runtime_mode: str,
    workflow_name: str | None = None,
    target_name: str | None = None,
    ctl_variants: list[str] | tuple[str, ...] = (),
) -> dict | None:
    """Resolve a run's target-instance identity BEFORE its dirs exist (§Phase 31 6b).

    target run: the target's declared target_instance_params -> Hive segments;
    workflow run: the ordered child target-instance addresses -> the sha256
    composition segment + the authoritative identity doc. Returns
    {instance_segments, address, target_addresses, identity_doc?} or None for
    run types without instance identity (fan_out/step_sequence/maintenance)."""
    if run_type not in ("target", "workflow"):
        return None
    execution_context = build_execution_context(
        ctl_cfg_root,
        action=action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        execution_runtime_mode=execution_runtime_mode,
    )
    inventory_cfg = load_inventory_cfg(ctl_cfg_root, action, execution_context)
    targets = inventory_cfg.get("targets", {})

    def target_segments(name: str) -> list[str]:
        target_def = targets.get(name) or {}
        return resolve_target_instance_segments(
            target_def.get("target_instance_params"),
            execution_context,
            label=f"target {name}",
        )

    if run_type == "target":
        if not target_name:
            return None
        segments = target_segments(target_name)
        address = target_instance_address(target_name, segments)
        resolved_params = {
            key: str(execution_context[f"execution_context.params.{key}"])
            for key in (targets.get(target_name) or {}).get("target_instance_params", [])
        }
        return {
            "instance_segments": segments,
            "address": address,
            "target_addresses": [address],
            "identity_doc": {
                "target_instance": {
                    "target": target_name,
                    "resolved_params": resolved_params,
                }
            },
        }
    workflow_cfg = load_workflow_cfg(ctl_cfg_root, ctl_profile, action, workflow_name, execution_context)
    workflow_cfg = apply_ctl_variants_to_workflow_cfg(
        ctl_cfg_root,
        workflow_cfg,
        inventory_cfg,
        execution_context=execution_context,
        inventory_name=action,
        workflow_name=workflow_name,
        ctl_variants=list(ctl_variants),
    )
    addresses: list[str] = []
    for entry in workflow_cfg.get("target_runs", []):
        name = entry if isinstance(entry, str) else entry.get("target")
        if not name:
            continue
        addresses.append(target_instance_address(name, target_segments(name)))
    if not addresses:
        raise RuntimeError(f"❌ workflow {workflow_name!r} resolves no target addresses")
    digest = workflow_composition_sha256(addresses)
    return {
        "instance_segments": [f"sha256-{digest}"],
        "address": f"{workflow_name}/sha256-{digest}",
        "target_addresses": addresses,
        "identity_doc": build_workflow_identity_doc(
            workflow_name, addresses, dict(execution_params)
        ),
    }


def run_provisions_ctl_state_backend(workflow_cfg: dict, inventory_cfg: dict) -> bool:
    """Whether any target in this run is the ctl-state bucket-creating target
    (declares provisions_ctl_state_backend: true). Such a run may legitimately start
    before its results bucket exists; every other run must find it already there
    under a `required` sync policy."""
    targets = inventory_cfg.get("targets", {})
    for entry in workflow_cfg.get("target_runs", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        target_cfg = targets.get(target_name) or {}
        if target_cfg.get("provisions_ctl_state_backend") is True:
            return True
    return False


def active_target_names(workflow_cfg: dict) -> list[str]:
    names: list[str] = []
    for entry in workflow_cfg.get("target_runs", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        if isinstance(target_name, str) and target_name:
            names.append(target_name)
    return names


def active_targets_missing_key(workflow_cfg: dict, inventory_cfg: dict, skip_key: str) -> list[str]:
    """Targets that do NOT declare the given skip_* key (presence = capability)."""
    targets = inventory_cfg.get("targets", {})
    missing: list[str] = []
    for target_name in active_target_names(workflow_cfg):
        target_cfg = targets.get(target_name) or {}
        if skip_key not in target_cfg:
            missing.append(target_name)
    return missing


def target_allows_agreed_direct_execution_access(target_cfg: dict) -> bool:
    """Whether a target opts into agreed_direct execution access (§12); default False.

    `standard` is always permitted and `force_bypass` is profile-gated (not
    per-target), so `agreed_direct` is the only mode a target actually gates —
    hence a boolean flag rather than a mode list."""
    raw = target_cfg.get("allow_agreed_direct_execution_access", False)
    if not isinstance(raw, bool):
        raise RuntimeError("❌ target allow_agreed_direct_execution_access must be a boolean")
    return raw


def validate_execution_access(
    ctl_cfg_root: Path,
    ctl_profile: str,
    workflow_cfg: dict,
    inventory_cfg: dict,
    *,
    execution_context: dict[str, object],
    execution_access_mode: str,
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
    provider_credential: str | None,
    force_skip_execution_identity_preflight_check: bool = False,
) -> None:
    """Validate the run's execution ACCESS MODE (§12) and the ctl-state sync
    skip actions. `standard` needs no permission; `direct`/`bypass` are gated by
    the ctl profile and (for direct) by each active target and its identity."""
    if execution_access_mode not in EXECUTION_ACCESS_MODES:
        raise RuntimeError(f"❌ unknown execution access mode {execution_access_mode!r}")

    # bypass: whole-run substitute credential, emergency/debug
    if execution_access_mode == "force_bypass":
        if not provider_credential:
            raise RuntimeError(
                "❌ bypass execution access requires the substitute credential (--provider-credential)"
            )
    elif provider_credential:
        raise RuntimeError(
            "❌ --provider-credential is valid only with --execution-access-mode force_bypass"
        )

    # profile authorizes the mode
    allowed_modes = ctl_allowed_execution_access_modes(ctl_cfg_root, ctl_profile)
    if execution_access_mode not in allowed_modes:
        raise RuntimeError(
            f"❌ execution access mode {execution_access_mode!r} is not allowed by ctl "
            f"profile {ctl_profile!r} (allowed: {sorted(allowed_modes)})"
        )

    # direct: every active target must allow direct and its identity must
    # declare a direct credential source
    if execution_access_mode == "agreed_direct":
        identities = load_execution_identities_cfg(ctl_cfg_root)
        targets = inventory_cfg.get("targets", {})
        for target_name in active_target_names(workflow_cfg):
            target_cfg = targets.get(target_name) or {}
            if not target_allows_agreed_direct_execution_access(target_cfg):
                raise RuntimeError(
                    f"❌ direct execution access requested, but target {target_name!r} does not "
                    "set allow_agreed_direct_execution_access: true"
                )
            identity_ref = target_cfg.get("execution_identity_key")
            if identity_ref is None:
                continue  # coverage is validated separately
            identity_key = resolve_runtime_scalar(
                identity_ref, execution_context,
                label=f"target {target_name} execution_identity_key",
            )
            # a group resolves to its concrete member for this run's context
            identity_key, identity_cfg = resolve_execution_identity_entry(
                identities, identity_key, execution_context
            )
            if not identity_cfg.get("direct_credential_source_key"):
                raise RuntimeError(
                    f"❌ direct execution access: target {target_name!r} identity "
                    f"{identity_key!r} declares no direct_credential_source_key"
                )

    # ctl-state sync skip (an operation, orthogonal to access mode)
    sync_permission_checks = (
        ("--agreed-defer-ctl-state-backend-sync", agreed_defer_ctl_state_backend_sync, "allow_agreed_defer_ctl_state_backend_sync", ctl_allows_agreed_defer_ctl_state_backend_sync),
        ("--force-skip-ctl-state-backend-sync", force_skip_ctl_state_backend_sync, "allow_force_skip_ctl_state_backend_sync", ctl_allows_force_skip_ctl_state_backend_sync),
    )
    for arg_name, requested, permission_key, profile_check in sync_permission_checks:
        if requested and not profile_check(ctl_cfg_root, ctl_profile):
            raise RuntimeError(
                f"❌ {arg_name} was requested, but ctl profile {ctl_profile!r} does not grant {permission_key}"
            )
    if execution_context.get(f"{EXECUTION_CONTEXT_ROOT}.ctl.force_skip_guardrails") and not ctl_allows_force_skip_guardrails(ctl_cfg_root, ctl_profile):
        raise RuntimeError(
            f"❌ --force-skip-guardrails was requested, but ctl profile {ctl_profile!r} does not grant allow_force_skip_guardrails"
        )
    validate_force_skip_full_cfg_validation_gate_policy(
        ctl_cfg_root,
        ctl_profile,
        bool(
            execution_context.get(
                f"{EXECUTION_CONTEXT_ROOT}.ctl.force_skip_full_cfg_validation_gate"
            )
        ),
    )
    if force_skip_execution_identity_preflight_check:
        if execution_access_mode == "force_bypass":
            raise RuntimeError(
                "❌ --force-skip-execution-identity-preflight-check is not applicable "
                "with bypass execution access"
            )
        if not ctl_allows_force_skip_execution_identity_preflight_check(
            ctl_cfg_root, ctl_profile
        ):
            raise RuntimeError(
                "❌ --force-skip-execution-identity-preflight-check was requested, "
                f"but ctl profile {ctl_profile!r} does not grant "
                "allow_force_skip_execution_identity_preflight_check"
            )
    if agreed_defer_ctl_state_backend_sync:
        missing = active_targets_missing_key(workflow_cfg, inventory_cfg, "allow_agreed_defer_ctl_state_backend_sync")
        if missing:
            raise RuntimeError(
                "❌ --agreed-defer-ctl-state-backend-sync was requested, but active targets do not "
                "declare allow_agreed_defer_ctl_state_backend_sync: true: " + ", ".join(sorted(missing))
            )


def load_target_policy_constraints(ctl_cfg_root: Path) -> list[dict]:
    constraints: list[dict] = []
    for path, section in collect_top_level_sections(ctl_cfg_root, "target_policy_constraints"):
        if not isinstance(section, list):
            raise RuntimeError(f"❌ target_policy_constraints must be a list: {path}")
        for idx, raw in enumerate(section, start=1):
            if not isinstance(raw, dict):
                raise RuntimeError(f"❌ target_policy_constraints entry #{idx} must be a mapping: {path}")
            target_prefix = raw.get("target_prefix")
            required_ref_policy = raw.get("required_ref_policy")
            if not isinstance(target_prefix, str) or not target_prefix.strip():
                raise RuntimeError(f"❌ target_policy_constraints entry #{idx} target_prefix must be a non-empty string: {path}")
            if not isinstance(required_ref_policy, str) or not required_ref_policy.strip():
                raise RuntimeError(f"❌ target_policy_constraints entry #{idx} required_ref_policy must be a non-empty string: {path}")
            constraints.append({
                "target_prefix": target_prefix.strip(),
                "required_ref_policy": required_ref_policy.strip(),
            })
    return constraints


def validate_target_policy_constraints(
    ctl_cfg_root: Path,
    ctl_profile: str,
    workflow_cfg: dict,
    inventory_cfg: dict,
) -> None:
    del inventory_cfg  # reserved for future target-policy dimensions
    active = active_target_names(workflow_cfg)
    if not active:
        return
    selected_ref_policy = ctl_ref_policy(ctl_cfg_root, ctl_profile)
    for constraint in load_target_policy_constraints(ctl_cfg_root):
        prefix = constraint["target_prefix"]
        matching = sorted(target for target in active if target.startswith(prefix))
        if not matching:
            continue
        required_ref_policy = constraint["required_ref_policy"]
        if selected_ref_policy != required_ref_policy:
            raise RuntimeError(
                f"❌ active targets under {prefix!r} require ref_policy {required_ref_policy!r}; "
                f"ctl profile {ctl_profile!r} uses {selected_ref_policy!r}. Targets: {', '.join(matching)}"
            )


def validate_target_policy_constraints_for_target(
    ctl_cfg_root: Path, ctl_profile: str, target_key: str
) -> None:
    """Per-target variant of validate_target_policy_constraints — one target's
    ref-policy requirement, so the ctl-policy report can attribute it per target."""
    selected_ref_policy = ctl_ref_policy(ctl_cfg_root, ctl_profile)
    for constraint in load_target_policy_constraints(ctl_cfg_root):
        prefix = constraint["target_prefix"]
        if not target_key.startswith(prefix):
            continue
        required_ref_policy = constraint["required_ref_policy"]
        if selected_ref_policy != required_ref_policy:
            raise RuntimeError(
                f"❌ target {target_key!r} under {prefix!r} requires ref_policy "
                f"{required_ref_policy!r}; ctl profile {ctl_profile!r} uses {selected_ref_policy!r}"
            )


def validate_workflow_target_selectors(
    workflow_cfg: dict,
    inventory_cfg: dict,
    execution_context: dict[str, object],
) -> None:
    targets = inventory_cfg.get("targets", {})
    for entry in workflow_cfg.get("target_runs", []):
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

def build_step_sequence_cfg(
    ctl_cfg_root: Path,
    action: str,
    *,
    source: str,
    ref: str,
    cfg_file_set_name: str,
    step_sequence: str,
    execution_identity_key: str | None,
) -> tuple[dict, dict]:
    """Build a one-target cfg for a synthetic repo-local step_sequence run.

    The synthetic target is composed directly from CLI args and need not exist
    in targets/<action>/. Synthetic runs are local-only and do not publish ctl state.
    """
    target_sources = collect_resource(ctl_cfg_root, "target_sources")
    cfg_file_sets = collect_resource(ctl_cfg_root, "cfg_file_sets")
    cfg_file_sets_path = ctl_cfg_root
    cfg_file_set = cfg_file_sets.get(cfg_file_set_name)
    if not isinstance(cfg_file_set, dict):
        raise RuntimeError(f"❌ step_sequence cfg_file_set {cfg_file_set_name!r} not found under {cfg_file_sets_path}")
    resolved = {
        "source": source,
        "ref": ref,
        "step_sequence": step_sequence,
        "cfg_root": cfg_file_set.get("cfg_root", "/"),
        "cfg_files": resolve_cfg_file_set_files(cfg_file_set_name, cfg_file_sets, cfg_file_sets_path),
    }
    if execution_identity_key:
        resolved["execution_identity_key"] = execution_identity_key
    name = "step_sequence"
    inventory_cfg = {"target_sources": target_sources, "targets": {name: resolved}}
    workflow_cfg = {
        "meta": {"name": f"step_sequence/{source}/{step_sequence}", "action": action},
        "target_runs": [name],
    }
    return workflow_cfg, inventory_cfg


def validate_fan_out_param_collisions(
    ctl_cfg_root: Path,
    children: list[dict],
    cli_execution_params: dict[str, str],
) -> None:
    """Reject fan-out params that would override an existing run param."""
    cfg_param_keys = set(load_execution_params(ctl_cfg_root))
    cli_param_keys = set(cli_execution_params)
    occupied_param_keys = cfg_param_keys | cli_param_keys
    collision_rows: list[str] = []
    for child in children:
        for key in sorted(occupied_param_keys & set(child.get("params") or {})):
            sources: list[str] = []
            if key in cli_param_keys:
                sources.append("--execution-params")
            if key in cfg_param_keys:
                sources.append("ctl execution_params")
            source = " and ".join(sources)
            collision_rows.append(f"{child['label']}: {key} ({source})")
    if collision_rows:
        raise RuntimeError(
            "❌ fan-out child params collide with existing execution params; "
            "fan-out params cannot override CLI or ctl cfg values: "
            + "; ".join(collision_rows)
        )


def load_domain_registry(ctl_cfg_root: Path) -> dict:
    """The authored domain registry (§Phase 31 Q11): bare conceptual
    declarations with flat keys. Every `domain` value appearing in cfg is
    validated against these keys, so a typo'd domain becomes a load error
    instead of a silent selector no-match."""
    return collect_resource(ctl_cfg_root, "domains", entry_depth=1)


def validate_domain_value(domains: dict, value: object, *, label: str) -> None:
    if str(value) not in domains:
        available = ", ".join(sorted(domains)) or "none"
        raise RuntimeError(f"❌ {label}: unknown domain {value!r}; registry declares: {available}")


def expand_fan_out(
    ctl_cfg_root: Path, fan_out_key: str, execution_context: dict[str, object]
) -> dict:
    """Expand a fan_out into concrete child runs — pure cfg logic, no execution and
    no state. Each child retains its optional parameter-set and entry keys so
    reports never conflate one declared workflow with its concrete expansions.
    Each child is one existing workflow/target run; the driver loops the runners.

    §Phase 31: a param-set member is {params, selectors?}. A member whose
    selectors do not match the frozen execution context is DROPPED before
    children are built — one fan-out serves every zone, the per-zone member
    set is resolved, not hardcoded. `domain` params are validated against the
    domain registry."""
    fan_outs = collect_resource(ctl_cfg_root, "fan_outs", entry_depth=1)
    fan_out = fan_outs.get(fan_out_key)
    if not isinstance(fan_out, dict):
        available = ", ".join(sorted(fan_outs)) or "none"
        raise RuntimeError(f"❌ fan-out {fan_out_key!r} not found; available: {available}")
    runs = fan_out.get("runs")
    if not isinstance(runs, list) or not runs:
        raise RuntimeError(f"❌ fan-out {fan_out_key!r} has no runs")
    param_sets = collect_resource(ctl_cfg_root, "fan_out_param_sets", entry_depth=1)
    domains = load_domain_registry(ctl_cfg_root)
    children: list[dict] = []
    for i, run in enumerate(runs):
        workflow_key, target_key = run.get("workflow_key"), run.get("target_key")
        if bool(workflow_key) == bool(target_key):
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} run[{i}] must set exactly one of workflow_key / target_key"
            )
        kind = "workflow" if workflow_key else "target"
        key = workflow_key or target_key
        param_set_key = run.get("fan_out_param_set_key")
        if param_set_key is None:
            children.append(
                {
                    "kind": kind,
                    "key": key,
                    "params": {},
                    "label": key,
                    "fan_out_param_set_key": None,
                    "fan_out_param_entry_key": None,
                }
            )
            continue
        param_set = param_sets.get(param_set_key)
        if not isinstance(param_set, dict) or not param_set:
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} run[{i}] references unknown fan_out_param_set {param_set_key!r}"
            )
        matched_members = 0
        for entry_name, member in param_set.items():
            member_label = f"fan_out_param_set {param_set_key!r}.{entry_name}"
            if not isinstance(member, dict):
                raise RuntimeError(f"❌ {member_label} must be a mapping")
            unknown = set(member) - {"params", "selectors"}
            if unknown:
                raise RuntimeError(
                    f"❌ {member_label} has unsupported keys {sorted(unknown)} "
                    "(a member is params + optional selectors; selectors must NOT "
                    "sit inside params)"
                )
            params = member.get("params")
            if not isinstance(params, dict) or not params:
                raise RuntimeError(f"❌ {member_label} params must be a non-empty map")
            if "selectors" in params:
                raise RuntimeError(f"❌ {member_label}: selectors must be a member field, not a param")
            if "domain" in params:
                validate_domain_value(domains, params["domain"], label=member_label)
            if not selector_matches(
                member.get("selectors"), execution_context,
                label=member_label, structured_only=True,
            ):
                continue
            children.append(
                {
                    "kind": kind,
                    "key": key,
                    "params": dict(params),
                    "label": f"{key}[{entry_name}]",
                    "fan_out_param_set_key": param_set_key,
                    "fan_out_param_entry_key": entry_name,
                }
            )
            matched_members += 1
        if matched_members == 0:
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} run[{i}]: no member of fan_out_param_set "
                f"{param_set_key!r} matches the execution context (a run entry must "
                "contribute at least one child)"
            )
    max_parallel = fan_out.get("max_parallel", 1)
    if isinstance(max_parallel, bool) or not isinstance(max_parallel, int) or max_parallel < 1:
        raise RuntimeError(f"❌ fan-out {fan_out_key!r} max_parallel must be a positive integer")
    failure_mode = fan_out.get("failure_mode", "stop")
    if failure_mode not in ("stop", "continue"):
        raise RuntimeError(f"❌ fan-out {fan_out_key!r} failure_mode must be 'stop' or 'continue'")
    return {"max_parallel": max_parallel, "failure_mode": failure_mode, "children": children}



PREFLIGHT_RESULT_STATUSES = {
    "passed",
    "failed",
    "force_skipped",
    "not_applicable",
    "not_evaluated",
}


class ProviderConfigBlockedError(RuntimeError):
    """A live check could not be evaluated because an upstream cfg defect (e.g. a
    malformed account id) blocks it. Surfaced per target as 'not_evaluated' with
    the exact blocking reason — never as a genuine identity failure."""
PREFLIGHT_SKIPPED_STATUSES = {
    "bypassed",
    "force_skipped",
    "not_applicable",
    "skipped",
}


def resolve_pipeline_selection(
    ctl_cfg_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    inventory_name: str,
    workflow_name: str | None,
    *,
    ctl_variants: list[str],
    target_repo_key: str,
    require_target_ref: bool,
    execution_runtime_mode: str,
    provider_credential: str | None,
    execution_access_mode: str,
    target_name: str | None = None,
    step_sequence_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    force_skip_execution_identity_preflight_check: bool = False,
    enforce_ctl_policy: bool = True,
    load_provider_catalogs: bool = True,
) -> dict:
    """Resolve a run through active target_runs without touching state or plt cfg.

    Policy-free resolution is used only to produce independent ctl-policy and
    execution-identity preflight artifacts. Callers must enforce both reports
    before executing the returned selection.

    With `load_provider_catalogs=False` the provider adapter and its runtime
    catalogs are NOT loaded (`provider_adapter`/`provider_catalogs` come back
    None). The cfg-level result is enough for the provider-independent ctl-policy
    preflight; call `load_selection_provider_catalogs` afterwards for the
    execution-identity preflight, which does need catalogs. This split keeps a
    provider-catalog failure (e.g. a malformed account id) from masquerading as a
    ctl-policy failure.
    """
    execution_context = build_execution_context(
        ctl_cfg_root,
        action=inventory_name,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        execution_access_mode=execution_access_mode,
        execution_runtime_mode=execution_runtime_mode,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
    )
    if enforce_ctl_policy:
        validate_execution_context_constraints(ctl_cfg_root, execution_context)
    require_commit_refs = ref_policy_requires_commits(ctl_ref_policy)

    if step_sequence_run:
        workflow_cfg, inventory_cfg = build_step_sequence_cfg(
            ctl_cfg_root,
            inventory_name,
            source=step_sequence_run["source"],
            ref=step_sequence_run["ref"],
            cfg_file_set_name=step_sequence_run["cfg_file_set"],
            step_sequence=step_sequence_run["step_sequence"],
            execution_identity_key=step_sequence_run.get("execution_identity_key"),
        )
        selection_kind = "step_sequence"
        selection_key = step_sequence_run["step_sequence"]
    elif target_name:
        inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name, execution_context)
        workflow_cfg = {
            "meta": {
                "name": f"{ctl_profile}/{inventory_name}/{target_name}",
                "action": inventory_name,
            },
            "target_runs": [target_name],
        }
        selection_kind = "target"
        selection_key = target_name
    else:
        workflow_cfg = load_workflow_cfg(
            ctl_cfg_root,
            ctl_profile,
            inventory_name,
            workflow_name,
            execution_context,
        )
        inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name, execution_context)
        workflow_cfg = apply_ctl_variants_to_workflow_cfg(
            ctl_cfg_root,
            workflow_cfg,
            inventory_cfg,
            execution_context=execution_context,
            inventory_name=inventory_name,
            workflow_name=workflow_name,
            ctl_variants=ctl_variants,
        )
        selection_kind = "workflow"
        selection_key = workflow_name

    if not step_sequence_run:
        validate_workflow_target_selectors(
            workflow_cfg, inventory_cfg, execution_context
        )
    if enforce_ctl_policy:
        validate_target_policy_constraints(
            ctl_cfg_root, ctl_profile, workflow_cfg, inventory_cfg
        )
        validate_execution_access(
            ctl_cfg_root,
            ctl_profile,
            workflow_cfg,
            inventory_cfg,
            execution_context=execution_context,
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
            force_skip_execution_identity_preflight_check=(
                force_skip_execution_identity_preflight_check
            ),
        )
        validate_execution_runtime_mode(ctl_cfg_root, ctl_profile, execution_runtime_mode)

    refs = load_refs_cfg(ctl_cfg_root)
    active_target_runs = build_active_target_runs(
        workflow_cfg,
        inventory_cfg,
        repo_key=target_repo_key,
        require_branch_or_commit=require_target_ref,
        refs=refs,
        execution_context=execution_context,
        require_commit_refs=require_commit_refs if enforce_ctl_policy else False,
    )
    if enforce_ctl_policy:
        validate_target_runs_have_commits(active_target_runs, ctl_ref_policy)
    provider_adapter = None
    provider_catalogs = None
    if load_provider_catalogs:
        provider_adapter = run_provider_adapter(execution_context)
        provider_catalogs = provider_adapter.load_runtime_catalogs(
            ctl_cfg_root, execution_context=execution_context
        )
    return {
        "selection_kind": selection_kind,
        "selection_key": selection_key,
        "execution_context": execution_context,
        "scope_params": scope_params_from_context(execution_context),
        "require_commit_refs": require_commit_refs,
        "workflow_cfg": workflow_cfg,
        "inventory_cfg": inventory_cfg,
        "refs": refs,
        "active_target_runs": active_target_runs,
        "provider_adapter": provider_adapter,
        "provider_catalogs": provider_catalogs,
    }


def load_selection_provider_catalogs(selection: dict, ctl_cfg_root: Path) -> dict:
    """Attach the provider adapter + runtime catalogs to a selection resolved with
    `load_provider_catalogs=False`.

    Runtime catalogs are structurally validated but permit unresolved concrete
    provider values. The adapter validates concrete values reachable from the
    selected target runs during target-cfg and execution-identity preflight.
    """
    execution_context = selection["execution_context"]
    provider_adapter = run_provider_adapter(execution_context)
    selection["provider_adapter"] = provider_adapter
    selection["provider_catalogs"] = provider_adapter.load_runtime_catalogs(
        ctl_cfg_root,
        execution_context=execution_context,
    )
    return selection


def credential_free_preflight_failure_reason(error: BaseException) -> str:
    detail = " ".join(str(error).split())
    detail = re.sub(
        r"(?i)((?:access[ _-]?key|secret|token|password)\s*[:=]\s*)\S+",
        r"\1<redacted>",
        detail,
    )
    # report statuses carry the ❌ mark; the reason text stays plain
    detail = detail.lstrip("❌ ").strip()
    return detail or error.__class__.__name__


def target_instance_display(
    target_run: dict, execution_context: dict[str, object]
) -> str:
    """The target-instance identity for a report row: Hive segments joined
    (`account=dev/env_type=dev`), `<singleton>` when the target has no instance
    layer, or `<unresolved>` if its instance params don't bind."""
    try:
        segments = resolve_target_instance_segments(
            target_run.get("target_instance_params"),
            execution_context,
            label="target instance",
        )
    except Exception:
        return "<unresolved>"
    return "/".join(segments) if segments else "<singleton>"


def aggregate_execution_identity_preflight_status(statuses: list[str]) -> str:
    """Container (fan-out/workflow/target) status rolls up its children without
    ever a false green OR a false 'nothing checked':

    - any `failed` child                         → failed;
    - else no `not_evaluated` child              → passed;
    - else a block is present, and:
        - at least one GENUINE `passed` child    → partial (honest mixed state);
        - no genuine pass (only blocks + neutral
          non-checks)                            → not_evaluated (fully blocked).

    A `not_evaluated` child is blocked upstream (e.g. a malformed account id) so
    it could not be checked. Deliberate non-checks (bypassed, force-skipped,
    not-applicable, skipped) are NEUTRAL — they neither block nor count as a
    verification: a container of blocked-plus-skipped is `not_evaluated`, not
    `partial` (only a real `passed` sibling makes it partial). Neither `partial`
    nor `not_evaluated` fails the run — only `failed` gates; these are honest
    summaries. The per-identity rows keep their own raw statuses."""
    if any(status == "failed" for status in statuses):
        return "failed"
    if not any(status == "not_evaluated" for status in statuses):
        return "passed"
    # a block is present; `partial` requires a GENUINE pass alongside it —
    # deliberate non-checks (skipped/force_skipped/not_applicable/bypassed) are
    # neutral, NOT verifications, so blocked + only-neutral is fully not_evaluated.
    if any(status == "passed" for status in statuses):
        return "partial"
    return "not_evaluated"


def build_ctl_policy_preflight_report(
    selection: dict,
    *,
    ctl_cfg_root: Path,
    ctl_profile: str,
    ctl_ref_policy: str,
    execution_runtime_mode: str,
    execution_access_mode: str,
    provider_credential: str | None,
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
    force_skip_execution_identity_preflight_check: bool,
) -> dict:
    """Evaluate run policy independently from provider identity reachability."""
    checks: list[dict] = []

    def check(name: str, validator) -> None:
        try:
            validator()
            checks.append({"name": name, "status": "passed"})
        except Exception as error:
            checks.append(
                {
                    "name": name,
                    "status": "failed",
                    "failure_reason": credential_free_preflight_failure_reason(error),
                }
            )

    workflow_cfg = selection["workflow_cfg"]
    inventory_cfg = selection["inventory_cfg"]
    execution_context = selection["execution_context"]
    check(
        "execution_context_constraints",
        lambda: validate_execution_context_constraints(
            ctl_cfg_root, execution_context
        ),
    )
    check(
        "execution_access_policy",
        lambda: validate_execution_access(
            ctl_cfg_root,
            ctl_profile,
            workflow_cfg,
            inventory_cfg,
            execution_context=execution_context,
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
            force_skip_execution_identity_preflight_check=(
                force_skip_execution_identity_preflight_check
            ),
        ),
    )
    check(
        "execution_runtime_mode_policy",
        lambda: validate_execution_runtime_mode(
            ctl_cfg_root, ctl_profile, execution_runtime_mode
        ),
    )
    check(
        "ref_policy",
        lambda: validate_target_runs_have_commits(
            selection["active_target_runs"], ctl_ref_policy
        ),
    )
    # Per-target policy checks (hybrid: selection-scoped checks above, target-
    # scoped checks here).
    targets: list[dict] = []
    for target_key in sorted(active_target_names(workflow_cfg)):
        target_checks: list[dict] = []

        def target_check(name: str, validator) -> None:
            try:
                validator()
                target_checks.append({"name": name, "status": "passed"})
            except Exception as error:
                target_checks.append(
                    {
                        "name": name,
                        "status": "failed",
                        "failure_reason": credential_free_preflight_failure_reason(error),
                    }
                )

        target_check(
            "target_policy_constraints",
            lambda tk=target_key: validate_target_policy_constraints_for_target(
                ctl_cfg_root, ctl_profile, tk
            ),
        )
        targets.append(
            {
                "target_key": target_key,
                "status": (
                    "failed"
                    if any(c["status"] == "failed" for c in target_checks)
                    else "passed"
                ),
                "checks": target_checks,
            }
        )
    status = (
        "failed"
        if any(item["status"] == "failed" for item in checks)
        or any(target["status"] == "failed" for target in targets)
        else "passed"
    )
    return {
        "selection": {
            "kind": selection["selection_kind"],
            "key": selection["selection_key"],
        },
        "status": status,
        "checks": checks,
        "targets": targets,
    }


def wrap_fan_out_preflight_child(
    report: dict,
    child: dict,
    *,
    effective_params: dict[str, str] | None = None,
) -> dict:
    """Fold the child's own (per-member) params onto its workflow/target node,
    and wrap it in a parameter-set node when one was expanded. Run-constant params
    (provider, landing_zone, …) live on the fan-out header, not here."""
    del effective_params  # per-member params are child["params"]; constants hoist
    per_member = dict(child.get("params") or {})
    param_set_key = child.get("fan_out_param_set_key")
    entry_key = child.get("fan_out_param_entry_key")
    if param_set_key is None:
        if not per_member:
            return report
        return {**report, "params": per_member}
    report_node = dict(report)
    if per_member:
        report_node["params"] = per_member
    return {
        "selection": {
            "kind": "fan_out_param_set",
            "key": f"{param_set_key}.{entry_key}",
        },
        "status": report["status"],
        "children": [report_node],
    }


def build_ctl_state_backend_preflight_result(
    selection: dict,
    *,
    ctl_cfg_root: Path,
    implementation_key: str,
    execution_access_mode: str,
    provider_credential: str | None,
    force_skip: bool,
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
) -> dict:
    """Preflight the run's ctl-state backend synchronizer identity.

    Mirrors the sync semantics: force-skip-sync and namespace-less runs are
    not_applicable (sync will not happen); agreed-skip still CHECKS the identity
    (only a missing bucket is tolerated, and only for a provisioning run — noted
    here, never failed; the syncer re-checks the bucket at every sync point)."""
    buckets = load_ctl_state_backends_cfg(ctl_cfg_root)
    result: dict = {
        "ctl_state_backend": None,
        "execution_identity_key": None,
        "provider": None,
        "access_mode": "agreed_direct",
        "status": "not_applicable",
        "provider_path": [],
    }
    if not buckets:
        result["reason"] = "no ctl-state backend registry"
        return result
    try:
        namespace_key, _ = resolve_ctl_state_namespace(
            ctl_cfg_root, selection["execution_context"]
        )
    except Exception as error:
        result["status"] = "failed"
        result["failure_reason"] = credential_free_preflight_failure_reason(error)
        return result
    result["ctl_state_backend"] = namespace_key
    if force_skip_ctl_state_backend_sync:
        result["reason"] = "ctl-state sync force-skipped for this run"
        return result
    if agreed_defer_ctl_state_backend_sync:
        result["reason"] = (
            "ctl-state sync readiness is validated once for the complete selected graph"
        )
        return result
    entry = buckets[namespace_key]
    result["provider"] = entry.get("provider")
    identity_key = (entry.get("execution_identity_keys") or {}).get("sync")
    if identity_key is None:
        if execution_access_mode == "force_bypass":
            result["reason"] = "identity bypass: synchronizer uses the substitute credential"
            return result
        result["status"] = "failed"
        result["failure_reason"] = (
            f"ctl_state_backends.{namespace_key} declares no execution_identity_keys.sync"
        )
        return result
    identity_key = str(
        resolve_runtime_scalar(
            identity_key,
            selection["execution_context"],
            label=f"ctl_state_backends.{namespace_key}.execution_identity_keys.sync",
        )
    )
    # Ctl-state publication always uses its normal operation role path.
    sync_access_mode = "force_bypass" if execution_access_mode == "force_bypass" else "standard"
    provider_adapter = selection["provider_adapter"]
    try:
        checked = provider_adapter.preflight_execution_identity(
            f"ctl_state_backend/{namespace_key}",
            {"execution_identity_key": identity_key},
            selection["provider_catalogs"],
            execution_context=selection["execution_context"],
            implementation_key=implementation_key,
            execution_access_mode=sync_access_mode,
            provider_credential=provider_credential,
            live_check=not force_skip,
        )
        if not isinstance(checked, dict) or checked.get("status") not in PREFLIGHT_RESULT_STATUSES:
            raise RuntimeError("provider preflight returned an invalid result")
    except Exception as error:
        result["status"] = "failed"
        result["execution_identity_key"] = identity_key
        result["failure_reason"] = credential_free_preflight_failure_reason(error)
        return result
    checked = dict(checked)
    checked["ctl_state_backend"] = namespace_key
    return checked


def build_execution_identity_preflight_report(
    selection: dict,
    *,
    implementation_key: str,
    execution_access_mode: str,
    provider_credential: str | None,
    force_skip: bool,
    ctl_cfg_root: Path | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
) -> dict:
    """Run one adapter preflight per selected target and aggregate every result.

    When `ctl_cfg_root` is provided, the run's ctl-state backend synchronizer is
    checked as one more result row (same aggregate rules)."""
    active_target_runs = selection["active_target_runs"]
    provider_adapter = selection["provider_adapter"]
    catalogs = selection["provider_catalogs"]
    execution_context = selection["execution_context"]

    target_runs_by_key: dict[str, tuple[str, dict]] = {}
    for target_run_id, target_run in active_target_runs.items():
        target_key = target_run.get("target") or target_run_id
        target_runs_by_key.setdefault(target_key, (target_run_id, target_run))

    results: list[dict] = []
    for target_key, (target_run_id, target_run) in target_runs_by_key.items():
        try:
            result = provider_adapter.preflight_execution_identity(
                target_run_id,
                target_run,
                catalogs,
                execution_context=execution_context,
                implementation_key=implementation_key,
                execution_access_mode=execution_access_mode,
                provider_credential=provider_credential,
                live_check=not force_skip,
            )
            if not isinstance(result, dict):
                raise RuntimeError("provider preflight returned a non-mapping result")
        except Exception as error:
            result = {
                "execution_identity_key": target_run.get("execution_identity_key"),
                "provider": execution_context.get(
                    f"{EXECUTION_CONTEXT_ROOT}.params.provider"
                ),
                "access_mode": execution_access_mode,
                "status": "failed",
                "provider_path": [],
                "failure_reason": credential_free_preflight_failure_reason(error),
            }
        status = result.get("status")
        if status not in PREFLIGHT_RESULT_STATUSES:
            result = {
                "execution_identity_key": result.get("execution_identity_key"),
                "provider": result.get("provider"),
                "access_mode": result.get("access_mode", execution_access_mode),
                "status": "failed",
                "provider_path": [],
                "failure_reason": f"provider preflight returned invalid status {status!r}",
            }
        result = dict(result)
        result["target_key"] = target_key
        result["instance"] = target_instance_display(target_run, execution_context)
        results.append(result)

    if ctl_cfg_root is not None:
        results.append(
            build_ctl_state_backend_preflight_result(
                selection,
                ctl_cfg_root=ctl_cfg_root,
                implementation_key=implementation_key,
                execution_access_mode=execution_access_mode,
                provider_credential=provider_credential,
                force_skip=force_skip,
                agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
                force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            )
        )

    status = aggregate_execution_identity_preflight_status(
        [str(result["status"]) for result in results]
    )
    return {
        "selection": {
            "kind": selection["selection_kind"],
            "key": selection["selection_key"],
        },
        "status": status,
        "results": results,
    }


def _preflight_status_tag(status: str) -> str:
    if status in PREFLIGHT_SKIPPED_STATUSES:
        return "[ skipped ⏭ ]"
    if status == "not_evaluated":
        return "[ not evaluated ⚠️ ]"
    if status == "partial":
        return "[ partial ⚠️ ]"
    marks = {"passed": "✅", "failed": "❌"}
    mark = marks.get(status)
    return f"[ {status} {mark} ]" if mark else f"[ {status} ]"


# ── report rendering as a `tree`-style ASCII tree ──────────────────────────
# Every report is converted to nodes {"label", "children"} then rendered with
# └──/├──/│ connectors. Node labels already carry the status tag.
def _node(label: str, children: list | None = None) -> dict:
    return {"label": label, "children": children or []}


def _tree_child_lines(children: list, prefix: str) -> list[str]:
    lines: list[str] = []
    for index, child in enumerate(children):
        last = index == len(children) - 1
        lines.append(prefix + ("└── " if last else "├── ") + child["label"])
        lines.extend(
            _tree_child_lines(child["children"], prefix + ("    " if last else "│   "))
        )
    return lines


def _render_tree(root: dict) -> list[str]:
    return [root["label"]] + _tree_child_lines(root["children"], "")


def _report_node_label(report: dict) -> str:
    selection = report["selection"]
    label = f"{selection['kind']}: {selection['key']}"
    params = report.get("params") or {}
    if params:
        label += "  (" + ", ".join(f"{k}={v}" for k, v in sorted(params.items())) + ")"
    return f"{label} {_preflight_status_tag(report['status'])}"


def _nested_report_node(report: dict, leaf_builder) -> dict:
    node = _node(_report_node_label(report))
    if report.get("failure_reason"):
        # a not_evaluated node did not fail here — it just could not be checked
        label = "not evaluated" if report.get("status") == "not_evaluated" else "error"
        node["children"].append(_node(f"{label}: {report['failure_reason']}"))
    for child in report.get("children", []):
        node["children"].append(_nested_report_node(child, leaf_builder))
    node["children"].extend(leaf_builder(report))
    return node


def _identity_result_nodes(report: dict) -> list[dict]:
    nodes: list[dict] = []
    for result in report.get("results", []):
        status = result["status"]
        tag = _preflight_status_tag
        if "ctl_state_backend" in result:
            children: list[dict] = []
            if result.get("execution_identity_key"):
                children.append(
                    _node(
                        f"execution_identity: {result['execution_identity_key']} {tag(status)}"
                    )
                )
            if result.get("reason"):
                children.append(_node(f"reason: {result['reason']}"))
            nodes.append(
                _node(
                    f"ctl_state_backend: {result.get('ctl_state_backend') or '<none>'} {tag(status)}",
                    children,
                )
            )
            continue
        # container row carries only passed/failed/not_evaluated; the identity
        # row keeps the raw status (bypassed/skipped/not-applicable count as passed)
        row_status = status if status in ("failed", "not_evaluated") else "passed"
        identity_key = result.get("execution_identity_key") or "<unresolved>"
        identity_children: list[dict] = []
        for path_node in result.get("provider_path") or []:
            display = (
                path_node.get("display")
                or path_node.get("cfg_key")
                or path_node.get("node_type")
                or "path"
            )
            pchildren = (
                [_node(f"error: {path_node['failure_reason']}")]
                if path_node.get("failure_reason")
                else []
            )
            identity_children.append(
                _node(f"{display} {tag(path_node.get('status', status))}", pchildren)
            )
        if result.get("reason"):
            reason = result["reason"]
            if status in PREFLIGHT_SKIPPED_STATUSES:
                reason = "execution identity was skipped for this run"
            identity_children.append(_node(f"reason: {reason}"))
        if result.get("failure_reason"):
            identity_children.append(_node(f"error: {result['failure_reason']}"))
        if result.get("blocked"):
            identity_children.append(_node(f"blocked: {result['blocked']}"))
        identity_node = _node(
            f"execution_identity: {identity_key} {tag(status)}", identity_children
        )
        # the identity is checked FOR this instance, so nest it under the instance
        if result.get("instance"):
            target_children = [_node(f"instance: {result['instance']}", [identity_node])]
        else:
            target_children = [identity_node]
        nodes.append(
            _node(f"target: {result['target_key']} {tag(row_status)}", target_children)
        )
    return nodes


def _preflight_text_lines(report: dict) -> list[str]:
    return _render_tree(_nested_report_node(report, _identity_result_nodes))


def _policy_check_nodes(report: dict) -> list[dict]:
    tag = _preflight_status_tag

    def check_node(check: dict) -> dict:
        children = (
            [_node(f"error: {check['failure_reason']}")]
            if check.get("failure_reason")
            else []
        )
        return _node(f"check: {check['name']} {tag(check['status'])}", children)

    nodes = [check_node(check) for check in report.get("checks", [])]
    for target in report.get("targets", []):
        nodes.append(
            _node(
                f"target: {target['target_key']} {tag(target['status'])}",
                [check_node(check) for check in target.get("checks", [])],
            )
        )
    return nodes


def _ctl_policy_preflight_text_lines(report: dict) -> list[str]:
    return _render_tree(_nested_report_node(report, _policy_check_nodes))


def write_ctl_policy_preflight_artifacts(
    artifacts_dir: Path, report: dict
) -> None:
    text_path = artifacts_dir / "ctl_policy_validation.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text(
        "\n".join(_ctl_policy_preflight_text_lines(report)) + "\n",
        encoding="utf-8",
    )


def build_cfg_validation_report(findings: list[dict]) -> dict:
    """General cfg validation (run once): a flat list of cfg-path-keyed
    well-formedness findings. Failed if any finding failed."""
    status = "failed" if any(f.get("status") == "failed" for f in findings) else "passed"
    return {"kind": "cfg_validation", "status": status, "findings": list(findings)}


def apply_full_cfg_validation_gate(
    report: dict, *, force_skip: bool
) -> dict:
    """Annotate whether whole-cfg findings gate this run.

    Structural and unclassified findings are never skippable. The force flag only
    accepts failed
    concrete bindings outside the selected run; selected bindings are enforced
    independently by target_cfg_validation.
    """
    unskippable_failure = any(
        finding.get("status") == "failed" and finding.get("structural") is not False
        for finding in report.get("findings", [])
    )
    if force_skip and report.get("status") == "failed" and not unskippable_failure:
        report["gate"] = {
            "status": "force_skipped",
            "reason": "unrelated full-cfg failures were accepted for this run",
        }
    else:
        report["gate"] = {"status": report.get("status", "failed")}
    return report


def assert_full_cfg_validation_gate_accepted(report: dict) -> None:
    gate_status = (report.get("gate") or {}).get(
        "status", report.get("status", "failed")
    )
    if gate_status != "failed":
        return
    failures = [
        str(finding.get("cfg_path", "<unknown>"))
        for finding in report.get("findings", [])
        if finding.get("status") == "failed"
    ]
    raise RuntimeError(
        "❌ full cfg validation failed for: "
        + ", ".join(failures or ["unknown cfg path"])
    )


def _cfg_validation_text_lines(report: dict) -> list[str]:
    root = _node(f"cfg validation {_preflight_status_tag(report['status'])}")
    gate = report.get("gate") or {}
    if gate:
        gate_children = (
            [_node(f"reason: {gate['reason']}")] if gate.get("reason") else []
        )
        root["children"].append(
            _node(
                f"full cfg validation gate {_preflight_status_tag(gate['status'])}",
                gate_children,
            )
        )
    for finding in report.get("findings", []):
        children = (
            [_node(f"error: {finding['error']}")] if finding.get("error") else []
        )
        root["children"].append(
            _node(
                f"{finding['cfg_path']} {_preflight_status_tag(finding['status'])}",
                children,
            )
        )
    return _render_tree(root)


def write_cfg_validation_artifacts(artifacts_dir: Path, report: dict) -> None:
    text_path = artifacts_dir / "cfg_validation.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text(
        "\n".join(_cfg_validation_text_lines(report)) + "\n", encoding="utf-8"
    )


def collect_target_consumed_axes(
    target_key: str,
    inventory_target: dict,
    *,
    refs: dict,
    execution_identities: dict,
    execution_context: dict[str, object],
) -> set[str]:
    """§Phase 32 guard: statically derive the params-namespace axes a target
    consumes at its three resolution chokepoints — ref template / ref group,
    cfg_file_set + instance-schema group selectors (recorded at inventory load),
    and identity group selectors + account_key template."""
    consumed: set[str] = set(inventory_target.get("consumed_group_axes") or [])
    raw_ref = inventory_target.get("ref")
    consumed |= _template_param_axes(raw_ref)
    scoped_refs = (refs or {}).get("scoped") or {}
    ref_entry = scoped_refs.get(raw_ref)
    if selector_group_is_group(ref_entry):
        consumed |= _selector_param_axes(ref_entry.get("members"))
        try:
            member_template = resolve_selector_group_member(
                ref_entry, execution_context,
                value_field="ref_key",
                label=f"target {target_key!r} ref group",
                tolerate_none=True,
            )
        except Exception:
            member_template = None
        consumed |= _template_param_axes(member_template)
    identity_key = inventory_target.get("execution_identity_key")
    identity_entry = (execution_identities or {}).get(identity_key)
    # §Phase 33: groups may nest (one dispatch axis per level) — gather every
    # level's selector axes while walking to the concrete identity.
    seen: set = set()
    while (
        isinstance(identity_entry, dict)
        and "members" in identity_entry
        and identity_key not in seen
    ):
        seen.add(identity_key)
        consumed |= _selector_param_axes(identity_entry.get("members"))
        next_key = None
        for member in identity_entry["members"]:
            if isinstance(member, dict) and selector_matches(
                member.get("selectors"), execution_context,
                label=f"consumed-axes scan {identity_key}", structured_only=True,
            ):
                next_key = member.get("identity_key")
                break
        if not next_key:
            identity_entry = None
            break
        identity_key = next_key
        identity_entry = (execution_identities or {}).get(identity_key)
    if isinstance(identity_entry, dict):
        consumed |= _template_param_axes(identity_entry.get("account_key"))
    return consumed


def instance_axis_exclusions(ctl_cfg_root: Path | None) -> set[str]:
    """Axes that are NEVER instance axes: the provider dispatch key and the
    namespace axes (already encoded in the ctl-state bucket choice, e.g.
    landing_zone) — derived from the ctl_state_backends selectors, not a
    hand-list."""
    excluded = {"provider"}
    if ctl_cfg_root is None:
        return excluded
    try:
        for entry in load_ctl_state_backends_cfg(ctl_cfg_root).values():
            if isinstance(entry, dict):
                excluded |= _selector_param_axes([entry])
    except Exception:
        pass
    return excluded


def build_target_cfg_validation_report(
    selection: dict,
    *,
    implementation_key: str,
    execution_access_mode: str,
    provider_credential: str | None,
    ctl_cfg_root: Path | None = None,
) -> dict:
    """Per-target cfg resolution requires every selected identity/account binding
    to be concrete. Whole-cfg health remains non-blocking for unrelated values.
    Includes the §Phase 32 instance-axes guard: declared < consumed →
    ERROR (self-override risk); declared > consumed → WARN (unused axis)."""
    active_target_runs = selection["active_target_runs"]
    provider_adapter = selection["provider_adapter"]
    catalogs = selection["provider_catalogs"]
    execution_context = selection["execution_context"]
    by_key: dict[str, tuple[str, dict]] = {}
    for target_run_id, target_run in active_target_runs.items():
        by_key.setdefault(target_run.get("target") or target_run_id, (target_run_id, target_run))
    results: list[dict] = []
    for target_key, (target_run_id, target_run) in by_key.items():
        try:
            result = provider_adapter.resolve_target_cfg_references(
                target_run_id,
                target_run,
                catalogs,
                execution_context=execution_context,
                implementation_key=implementation_key,
                execution_access_mode=execution_access_mode,
                provider_credential=provider_credential,
            )
        except Exception as error:
            result = {
                "status": "failed",
                "rows": [],
                "failure_reason": credential_free_preflight_failure_reason(error),
            }
        result = dict(result)
        result["target_key"] = target_key
        result["instance"] = target_instance_display(target_run, execution_context)
        # §Phase 32 instance-axes guard
        inventory_target = (selection["inventory_cfg"].get("targets") or {}).get(
            target_key
        )
        if isinstance(inventory_target, dict):
            consumed = collect_target_consumed_axes(
                target_key,
                inventory_target,
                refs=selection.get("refs") or {},
                execution_identities=(catalogs or {}).get("execution_identities") or {},
                execution_context=execution_context,
            ) - instance_axis_exclusions(ctl_cfg_root)
            declared = set(target_run.get("target_instance_params") or [])
            rows = result.setdefault("rows", [])
            for axis in sorted(consumed - declared):
                rows.append(
                    {
                        "name": f"instance_axis {axis}: consumed but not declared",
                        "status": "failed",
                    }
                )
                result["status"] = "failed"
                result.setdefault(
                    "failure_reason",
                    "target varies by an undeclared axis — add it to "
                    "target_instance_params or runs will self-override",
                )
            for axis in sorted(declared - consumed):
                rows.append(
                    {
                        "name": f"instance_axis {axis}: declared but not consumed",
                        "status": "warning",
                    }
                )
        results.append(result)
    status = aggregate_execution_identity_preflight_status(
        [str(result["status"]) for result in results]
    )
    return {
        "selection": {
            "kind": selection["selection_kind"],
            "key": selection["selection_key"],
        },
        "status": status,
        "results": results,
    }


def _target_cfg_result_nodes(report: dict) -> list[dict]:
    tag = _preflight_status_tag
    nodes: list[dict] = []
    for result in report.get("results", []):
        children: list[dict] = []
        if result.get("instance"):
            children.append(_node(f"instance: {result['instance']}"))
        for row in result.get("rows", []):
            children.append(_node(f"{row['name']} {tag(row['status'])}"))
        if result.get("failure_reason"):
            children.append(_node(f"error: {result['failure_reason']}"))
        nodes.append(
            _node(f"target: {result['target_key']} {tag(result['status'])}", children)
        )
    return nodes


def _target_cfg_validation_text_lines(report: dict) -> list[str]:
    return _render_tree(_nested_report_node(report, _target_cfg_result_nodes))


def write_target_cfg_validation_artifacts(artifacts_dir: Path, report: dict) -> None:
    text_path = artifacts_dir / "target_cfg_validation.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text(
        "\n".join(_target_cfg_validation_text_lines(report)) + "\n", encoding="utf-8"
    )


def assert_target_cfg_validation_accepted(report: dict) -> None:
    if report.get("status") != "failed":
        return
    failures = [
        str(result.get("target_key", "<unknown>"))
        for result in report.get("results", [])
        if result.get("status") == "failed"
    ]
    for child in report.get("children", []):
        if child.get("status") == "failed":
            failures.append(str(child.get("selection", {}).get("key", "<unknown>")))
    raise RuntimeError(
        "❌ target cfg validation failed for: " + ", ".join(failures or ["selected run"])
    )


def write_execution_identity_preflight_artifacts(
    artifacts_dir: Path, report: dict
) -> None:
    text_path = artifacts_dir / "execution_identity_preflight.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text("\n".join(_preflight_text_lines(report)) + "\n", encoding="utf-8")


def assert_ctl_policy_preflight_accepted(report: dict) -> None:
    if report.get("status") != "failed":
        return
    failures: list[str] = []
    failures.extend(
        str(check.get("name", "<unknown>"))
        for check in report.get("checks", [])
        if check.get("status") == "failed"
    )
    for child in report.get("children", []):
        if child.get("status") == "failed":
            failures.append(
                str(child.get("selection", {}).get("key", "<unknown>"))
            )
    raise RuntimeError(
        "❌ ctl policy preflight failed for: "
        + ", ".join(failures or ["selected run"])
    )


def assert_execution_identity_preflight_accepted(report: dict) -> None:
    if report.get("status") != "failed":
        return
    failures = [
        str(result.get("target_key", "<unknown>"))
        for result in report.get("results", [])
        if result.get("status") == "failed"
    ]
    for child in report.get("children", []):
        if child.get("status") == "failed":
            failures.append(str(child.get("selection", {}).get("key", "<unknown>")))
    raise RuntimeError(
        "❌ execution identity preflight failed for: "
        + ", ".join(failures or ["selected run"])
    )


def build_selection_validation_reports(
    selection: dict,
    *,
    ctl_cfg_root: Path,
    ctl_profile: str,
    ctl_ref_policy: str,
    execution_runtime_mode: str,
    execution_access_mode: str,
    provider_credential: str | None,
    implementation_key: str,
    force_skip_execution_identity_preflight_check: bool,
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
) -> dict:
    """Build the three per-selection validation reports (ctl-policy, target-cfg,
    execution-identity) for a selection resolved with load_provider_catalogs=
    False. Catalogs are loaded LENIENTLY, so a placeholder account id becomes a
    per-target 'blocked' row rather than a crash. Shared by the single runners
    and the fan-out so all three stay in lockstep. (General cfg_validation is
    run ONCE by the caller — it is not per-selection.)"""
    # ctl-policy is provider-INDEPENDENT (each check try-wrapped) — it always
    # produces real results, even if the provider catalog load below fails.
    policy_report = build_ctl_policy_preflight_report(
        selection,
        ctl_cfg_root=ctl_cfg_root,
        ctl_profile=ctl_profile,
        ctl_ref_policy=ctl_ref_policy,
        execution_runtime_mode=execution_runtime_mode,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
    )
    selection_ref = {
        "kind": selection["selection_kind"],
        "key": selection["selection_key"],
    }
    try:
        selection = load_selection_provider_catalogs(selection, ctl_cfg_root)
        target_cfg_report = build_target_cfg_validation_report(
            selection,
            implementation_key=implementation_key,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
            ctl_cfg_root=ctl_cfg_root,
        )
        identity_report = build_execution_identity_preflight_report(
            selection,
            implementation_key=implementation_key,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
            force_skip=force_skip_execution_identity_preflight_check,
            ctl_cfg_root=ctl_cfg_root,
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        )
    except Exception as error:
        reason = credential_free_preflight_failure_reason(error)
        target_cfg_report = {
            "selection": selection_ref,
            "status": "failed",
            "results": [],
            "failure_reason": reason,
        }
        identity_report = {
            "selection": selection_ref,
            "status": "failed",
            "results": [],
            "failure_reason": reason,
        }
    return {
        "selection": selection,
        "policy": policy_report,
        "target_cfg": target_cfg_report,
        "identity": identity_report,
    }


def resolve_and_preflight_execution_identities(
    ctl_cfg_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    inventory_name: str,
    workflow_name: str | None,
    *,
    ctl_variants: list[str],
    target_repo_key: str,
    require_target_ref: bool,
    provider_implementation_key: str,
    execution_runtime_mode: str,
    provider_credential: str | None,
    execution_access_mode: str,
    artifacts_dir: Path,
    target_name: str | None = None,
    step_sequence_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    force_skip_execution_identity_preflight_check: bool = False,
) -> tuple[dict, dict]:
    """Single-runner (workflow/target/step_sequence) preflight: the same four
    validation reports the fan-out produces, for this one selection."""
    selection = resolve_pipeline_selection(
        ctl_cfg_root,
        ctl_profile,
        execution_params,
        ctl_ref_policy,
        inventory_name,
        workflow_name,
        ctl_variants=ctl_variants,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        execution_runtime_mode=execution_runtime_mode,
        provider_credential=provider_credential,
        execution_access_mode=execution_access_mode,
        target_name=target_name,
        step_sequence_run=step_sequence_run,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
        enforce_ctl_policy=False,
        load_provider_catalogs=False,
    )
    cfg_report = build_cfg_validation_report(
        collect_provider_cfg_findings(ctl_cfg_root, selection["execution_context"])
    )
    apply_full_cfg_validation_gate(
        cfg_report, force_skip=force_skip_full_cfg_validation_gate
    )
    reports = build_selection_validation_reports(
        selection,
        ctl_cfg_root=ctl_cfg_root,
        ctl_profile=ctl_profile,
        ctl_ref_policy=ctl_ref_policy,
        execution_runtime_mode=execution_runtime_mode,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        implementation_key=provider_implementation_key,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
    )
    write_cfg_validation_artifacts(artifacts_dir, cfg_report)
    write_target_cfg_validation_artifacts(artifacts_dir, reports["target_cfg"])
    write_ctl_policy_preflight_artifacts(artifacts_dir, reports["policy"])
    write_execution_identity_preflight_artifacts(artifacts_dir, reports["identity"])
    # Full cfg health is always rendered. The authorized force flag skips only
    # this aggregate gate; structural and selected-run validation still block.
    assert_full_cfg_validation_gate_accepted(cfg_report)
    assert_target_cfg_validation_accepted(reports["target_cfg"])
    assert_ctl_policy_preflight_accepted(reports["policy"])
    assert_execution_identity_preflight_accepted(reports["identity"])
    return reports["selection"], reports["identity"]


def run_pipeline(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    inventory_name: str,
    workflow_name: str | None,
    run_id: str,
    plt_overlays: list[str],
    ctl_variants: list[str],
    target_repo_key: str,
    require_target_ref: bool,
    use_local_tooling_cfg: bool,
    provider_implementation_key: str,
    run_dir: Path,
    artifacts_dir: Path,
    plt_merged_dir: Path,
    log_file: Path,
    provider_credential: str | None,
    execution_runtime_mode: str,  # required, no default — the CLI (--execution-runtime-mode) supplies it
    target_name: str | None = None,
    step_sequence_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_access_mode: str = "standard",
    force_skip_execution_identity_preflight_check: bool = False,
    skip_committed_rerun: bool = False,
    parent_graph_provisions_ctl_state_backend: bool = False,
    parent_ctl_state_backend_absence_confirmed: bool = False,
    preflight_selection: dict | None = None,
) -> None:
    """
    Run a declared workflow, declared target, or synthetic repo-local step_sequence.

    The caller passes target_run repo settings and pre-created run/log directories.
    """
    if preflight_selection is None:
        selection, _ = resolve_and_preflight_execution_identities(
            ctl_cfg_root,
            ctl_profile,
            execution_params,
            ctl_ref_policy,
            inventory_name,
            workflow_name,
            ctl_variants=ctl_variants,
            target_repo_key=target_repo_key,
            require_target_ref=require_target_ref,
            provider_implementation_key=provider_implementation_key,
            execution_runtime_mode=execution_runtime_mode,
            provider_credential=provider_credential,
            execution_access_mode=execution_access_mode,
            artifacts_dir=artifacts_dir,
            target_name=target_name,
            step_sequence_run=step_sequence_run,
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            force_skip_guardrails=force_skip_guardrails,
            force_skip_full_cfg_validation_gate=(
                force_skip_full_cfg_validation_gate
            ),
            force_skip_execution_identity_preflight_check=(
                force_skip_execution_identity_preflight_check
            ),
        )
    else:
        selection = preflight_selection
    execution_context = selection["execution_context"]
    scope_params = selection["scope_params"]
    if selection.get("selection_kind") == "workflow":
        definition_canonical = json.dumps(
            selection["workflow_cfg"], separators=(",", ":"), sort_keys=True
        )
        update_run_metadata(
            run_dir,
            {
                "workflow_definition_sha256": hashlib.sha256(
                    definition_canonical.encode("utf-8")
                ).hexdigest()
            },
        )
    require_commit_refs = selection["require_commit_refs"]
    workflow_cfg = selection["workflow_cfg"]
    inventory_cfg = selection["inventory_cfg"]
    refs = selection["refs"]
    active_target_runs = selection["active_target_runs"]
    provider_adapter = selection["provider_adapter"]
    provider_catalogs = selection["provider_catalogs"]

    # Preserve the runtime binding contract after the live gate passes.
    provider_adapter.validate_active_target_access(
        active_target_runs,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
    )

    selected_graph_provisions_backend = parent_graph_provisions_ctl_state_backend
    backend_absence_confirmed = parent_ctl_state_backend_absence_confirmed
    if agreed_defer_ctl_state_backend_sync and not selected_graph_provisions_backend:
        graph_probe = inspect_selected_graph_ctl_state_backend(
            [selection],
            ctl_cfg_root,
            implementation_key=provider_implementation_key,
            execution_access_mode=execution_access_mode,
            provider_credential=provider_credential,
        )
        selected_graph_provisions_backend = True
        backend_absence_confirmed = graph_probe["status"] == "absent"
        if not backend_absence_confirmed:
            raise RuntimeError(
                "❌ --agreed-defer-ctl-state-backend-sync is not applicable: "
                "the selected backend already exists"
            )
    update_run_metadata(
        run_dir,
        {
            "selected_graph_provisions_ctl_state_backend": selected_graph_provisions_backend,
            "ctl_state_backend_absence_confirmed_at_start": backend_absence_confirmed,
        },
    )

    # Resolve the run's namespace and arm publication only after the graph-level
    # defer gate has frozen its provider-classified readiness fact.
    ctl_state_namespace_key, _ = resolve_ctl_state_namespace(
        ctl_cfg_root, execution_context
    )
    verify_ctl_guardrails(
        ctl_cfg_root,
        guardrails_cfg_root,
        execution_context,
    )
    configure_ctl_state_sync(
        ctl_cfg_root,
        ctl_profile,
        ctl_state_namespace_key,
        execution_context,
        run_dir,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        provisions_ctl_state_backend=run_provisions_ctl_state_backend(workflow_cfg, inventory_cfg),
        selected_graph_provisions_ctl_state_backend=selected_graph_provisions_backend,
        backend_absence_confirmed=backend_absence_confirmed,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        provider_implementation_key=provider_implementation_key,
    )
    execution_context_path = write_execution_context_artifact(run_dir, execution_context)

    if use_local_tooling_cfg:
        tooling_refs = load_local_tooling_cfg(ctl_cfg_root)
    else:
        tooling_refs = refs.get("global") or {}
        validate_tooling_refs_have_commits(tooling_refs, ctl_ref_policy)

    logging.info(f"Selector policy validation passed: ctl_profile={ctl_profile}")

    # Prepare pipeline config
    active_target_runs, pipeline_run_cfg_path = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
        plt_merged_dir,
        artifacts_dir,
        ctl_profile,
        plt_overlays,
        scope_params=scope_params,
        execution_context=execution_context,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
        active_target_runs=active_target_runs,
    )
    # Single derivation chain: render the merged tree, then verify guards
    # against rendered values, then distribute target_run input views from it.
    plt_rendered_dir = render_plt_cfg(plt_merged_dir, run_dir, execution_context)
    verify_guardrails(
        ctl_cfg_root,
        plt_cfg_root,
        guardrails_cfg_root,
        plt_rendered_dir,
        execution_context,
        scope_params,
        required_target_paths=required_target_paths_for_target_runs(active_target_runs),
    )

    if step_sequence_run:
        target_keys = step_sequence_run.get("affected_target_keys") or []
        if inventory_name in MUTATING_ACTIONS and not target_keys:
            raise RuntimeError("❌ mutating step_sequence runs require affected_target_keys")
    else:
        target_keys = target_keys_from_active_target_runs(active_target_runs)
    record_run_target_keys(run_dir, target_keys)
    run_metadata = load_run_metadata(run_dir)
    ctl_state_local_root_value = run_metadata.get("ctl_state_local_root")
    if isinstance(ctl_state_local_root_value, str) and ctl_state_local_root_value:
        mark_removed_definitions_outdated(Path(ctl_state_local_root_value), ctl_cfg_root)

    write_target_flow_artifact(
        ctl_cfg_root,
        artifacts_dir,
        ctl_profile=ctl_profile,
        execution_context=execution_context,
        inventory_name=inventory_name,
        workflow_name=workflow_name,
        ctl_variants=ctl_variants,
        plt_overlays=plt_overlays,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )

    # Write git metas
    write_git_metas(ctl_cfg_root, plt_cfg_root, guardrails_cfg_root, artifacts_dir)

    # Resolved ctl cfg snapshot (self-describing run, next to cfg/plt/)
    write_ctl_cfg_snapshot(
        run_dir,
        ctl_profile=ctl_profile,
        ctl_profile_policy_cfg=ctl_profile_policy(ctl_cfg_root, ctl_profile),
        inventory_name=inventory_name,
        workflow_cfg=workflow_cfg,
        inventory_cfg=inventory_cfg,
        active_target_runs=active_target_runs,
        refs=refs,
        execution_context=execution_context,
    )

    # Distribute target_run input views from the rendered tree
    plt_targets_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path, plt_rendered_dir, run_dir
    )
    # Prepared snapshot: cfg layers + run-level metadata are immutable from here.
    ctl_state_push("preparation complete")

    # Freeze the commit facts used by the opt-in committed-rerun gate.
    cfg_source_commit, cfg_source_state = git_source_facts(plt_cfg_root)
    for target_run in active_target_runs.values():
        source_commit, target_source_state = target_run_source_facts(target_run)
        target_run["source_commit"] = source_commit
        target_run["cfg_source_commit"] = cfg_source_commit
        target_run["source_state"] = (
            "clean"
            if target_source_state == "clean" and cfg_source_state == "clean"
            else "dirty"
        )
        target_run["ref_policy"] = ctl_ref_policy
    if load_run_metadata(run_dir).get("run_type") == "target":
        only_target = next(iter(active_target_runs.values()), None)
        if only_target:
            update_run_metadata(
                run_dir,
                {
                    key: only_target[key]
                    for key in (
                        "source_commit", "cfg_source_commit", "source_state", "ref_policy"
                    )
                },
            )

    # Run target runs
    run_steps(
        active_target_runs, run_dir, plt_targets_dir_path, execution_context_path,
        inventory_name, execution_context, run_id,
        tooling_refs=tooling_refs,
        use_local_tooling_cfg=use_local_tooling_cfg,
        provider_adapter=provider_adapter,
        provider_catalogs=provider_catalogs,
        provider_implementation_key=provider_implementation_key,
        execution_access_mode=execution_access_mode,
        provider_credential=provider_credential,
        execution_runtime_mode=execution_runtime_mode,
        skip_committed_rerun=skip_committed_rerun,
    )

    print_run_summary(run_id, log_file)
