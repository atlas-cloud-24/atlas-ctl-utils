"""Shared utilities for local runner entrypoints."""

import argparse
import collections
import contextlib
import fcntl
import fnmatch
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

from utils import cfg_presets
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
MAINTENANCE_ACTIONS = ("unlock-ctl-state", "status-sweep", "history-prune", "forget")
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
RUN_ACTIONS = ("provision", "plan", "destroy", "readonly", "maintenance")
RUN_TYPES = ("workflow", "target", "procedure", "maintenance", "fan_out")
# §Phase 30: reserved local-only ctl-state root — never synced, never a locator.
# Locator segments must start alphanumeric, so "_local" cannot collide.
LOCAL_ONLY_LOCATOR = ("_local",)
# §Phase 57: the build workspace (repo checkout + provider cache) lives under the
# reserved never-synced `_local` tree, NOT inside a run prefix — a run prefix is
# published to the backend and `s3 sync` has no --delete, so anything that lands
# there is permanent.
LOCAL_WORKSPACES_DIRNAME = "workspaces"
# §Phase 57: what a published run prefix contains. An ALLOWLIST, never an exclude
# list: publication is irreversible, so anything new under a run dir must default
# to NOT published. The engine decides WHAT a record is; the adapter uploads it.
RUN_RECORD_MEMBERS = (
    "RUN.yaml",
    "source_refs.yaml",
    "gates",
    "logs",
    "artifacts",
    "execution",
    "cfg",
)
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

def parse_selector_pairs(value: str) -> list[tuple[str, str]]:
    """Parse one --execution-params value into (key, value) pairs.

    Accepts a comma-separated list (`a=1,b=2`); a single pair is the one-element
    case. Combined with ExecutionParamsAction (which EXTENDS the dest list), both
    `--execution-params a=1,b=2` and the repeated `--execution-params a=1
    --execution-params b=2` produce the same flat list of pairs.
    """
    pairs: list[tuple[str, str]] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Selector must use key=value format, got: {item!r}")
        key, selector_value = item.split("=", 1)
        key = key.strip()
        selector_value = selector_value.strip()
        if not key or not selector_value:
            raise ValueError(f"Selector must use non-empty key=value, got: {item!r}")
        pairs.append((key, selector_value))
    if not pairs:
        raise ValueError(f"Selector must use key=value format, got: {value!r}")
    return pairs


class ExecutionParamsAction(argparse.Action):
    """Collect execution params as a FLAT list of (key, value) pairs.

    `type=` is deliberately not used: an append action with a list-returning type
    would nest to list[list[tuple]], while every downstream consumer
    (selectors_to_map, the arg normalizers, the tests) expects a flat
    list[tuple[str, str]].
    """

    def __call__(self, parser, namespace, values, option_string=None):
        current = list(getattr(namespace, self.dest, None) or [])
        try:
            current.extend(parse_selector_pairs(values))
        except ValueError as exc:
            raise argparse.ArgumentError(self, str(exc)) from exc
        setattr(namespace, self.dest, current)


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
    bundles governing ctl behavior.

    TWO LEVELS, split by who DEFINES the concept: the top level holds engine
    policies (ref_policy, execution runtime, ctl-state sync skips, guardrail and
    cfg-gate skips) plus `allowed_providers`; a `<provider>:` block holds the
    policies whose vocabulary belongs to that provider. The engine never reads
    inside a provider block — it hands it to the adapter (§Phase 52)."""
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


# The engine owns NO execution-access-mode vocabulary at all: each adapter
# advertises its own via supported_execution_access_modes(), and the operator
# states one per participating provider. There is no engine-level default,
# because the engine has no mode name to default to.

# Authorization classes and the action that selects one. Reads and writes get
# different authority, so the role a target assumes depends on the ACTION, not on
# the target alone — a `plan` must not run with write authority. Both the classes
# and this mapping are engine-owned and provider-neutral: a target's
# `execution.roles` maps each class to a provider role key the adapter resolves.
# The engine owns ACTIONS; a provider owns its ROLES and the mapping between the
# two. There is no engine-level authorization class: it would be derived from
# `MUTATING_ACTIONS` and carry nothing beyond "is this action mutating", while
# imposing one provider's vocabulary on every target — against the rule that
# everything below `provider` is opaque and interpreted by that adapter.


TARGET_EXECUTION_IDENTITY_FIELDS = frozenset(
    {
        "account",
        "roles",
        "agreed_direct_credential_source_keys",
        "allowed_accounts",
    }
)


def selector_group_is_group(entry: object) -> bool:
    """§Phase 31 3c: a selector-membered group entry in a cfg collection
    (cfg_key_sets, refs.scoped) — same resolution semantics as execution
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
    - `ctl.action` / `ctl.operation` → safe: both determine the instance path.
      An operation selects the member list, and a workflow instance IS a digest
      over its members' addresses, so a different operation is a different
      instance. Where two operations resolve to the same members they still land
      in different group files;
    - any OTHER ctl.* fact (e.g. ctl.profile) → hard error: it is neither
      path-encoded nor declarable, so two runs differing only in it would
      collapse onto one instance path and self-override."""
    params_prefix = f"{EXECUTION_CONTEXT_ROOT}.params."
    safe_refs = {
        f"{EXECUTION_CONTEXT_ROOT}.ctl.action",
        f"{EXECUTION_CONTEXT_ROOT}.ctl.operation",
    }
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
            elif ref not in safe_refs:
                raise RuntimeError(
                    f"❌ {label}: dispatch on {ref!r} is not allowed — target "
                    "resolution may dispatch only on declarable params axes or "
                    "the path-determining ctl.action / ctl.operation"
                )
    return axes


def resolve_list_members(
    entry: dict,
    execution_context: dict[str, object] | None,
    *,
    value_field: str,
    label: str,
    allow_empty: bool = False,
    extra_fields: tuple[str, ...] = (),
) -> list | None:
    """Resolve a members-shaped LIST-valued declaration
    ({members: [{<value_field>: [...], selectors: {...}}, ...]}) to the ONE
    matching member's list (§Phase 32 instance schemas, §Phase 33 per-action
    target_keys). The scalar twin is resolve_selector_group_member.
    Returns None when no context is available or the dispatch axis is unbound
    (deferred — the caller decides whether that is an error)."""
    members = entry.get("members")
    if set(entry) != {"members"} or not isinstance(members, list) or not members:
        raise RuntimeError(f"❌ {label}: members-shaped declaration must be {{members: [...]}}")
    for member in members:
        allowed = {value_field, "selectors", *extra_fields}
        if not isinstance(member, dict) or not {value_field, "selectors"} <= set(member) \
                or not set(member) <= allowed:
            raise RuntimeError(
                f"❌ {label}: each member must be {{{value_field}, selectors"
                + (", " + ", ".join(extra_fields) if extra_fields else "")
                + "}}"
            )
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


def resolve_list_member(
    entry: dict,
    execution_context: dict[str, object] | None,
    *,
    value_field: str,
    label: str,
    extra_fields: tuple[str, ...] = (),
) -> dict | None:
    """The matching MEMBER itself, not just its list.

    §Phase 73: a workflow member declares `default_action` for the whole list it
    carries, so the caller needs the member and not only its keys.
    """
    resolved = resolve_list_members(
        entry, execution_context, value_field=value_field, label=label,
        extra_fields=extra_fields,
    )
    if resolved is None:
        return None
    for member in entry.get("members") or []:
        if list(member.get(value_field) or []) == resolved and selector_matches(
            member["selectors"], execution_context or {},
            label=f"{label} member", structured_only=True,
        ):
            return member
    return None


def _resolve_instance_params_members(
    entry: dict, execution_context: dict[str, object] | None, *, target_name: str
) -> list[str] | None:
    return resolve_list_members(
        entry,
        execution_context,
        value_field="params",
        label=f"target {target_name!r} target_instance_params",
    )


def validate_target_providers(declared: object, identities: dict, *, label: str) -> list[str]:
    """A target DECLARES the providers it runs against, and the declaration must
    agree with the identities it carries.

    Declared rather than inferred from `execution_identities`, for the same reason
    `--providers` is declared for a run: the provider set is a statement about what
    this target is allowed to reach, and a drifted identity block should fail
    rather than quietly widen it.
    """
    if not isinstance(declared, list) or not declared or not all(
        isinstance(item, str) and item.strip() for item in declared
    ):
        raise RuntimeError(f"❌ {label} providers must be a non-empty list of provider names")
    if sorted(set(declared)) != sorted(identities):
        raise RuntimeError(
            f"❌ {label} declares providers {sorted(set(declared))} but carries "
            f"execution_identities for {sorted(identities)}; they must agree"
        )
    return sorted(set(declared))


def validate_workflow_actions_declared(workflows: dict) -> None:
    """§Phase 73: every workflow entry must be able to resolve an ACTION.

    A list with no `default_action` whose entries carry no `action:` is not
    runnable — the engine cannot know what to do with those targets. This is a cfg
    gate rather than a run-time surprise, so a workflow that could never run is
    refused when the configuration is validated.
    """
    for name, workflow in (workflows or {}).items():
        if not isinstance(workflow, dict):
            continue
        declaration = workflow.get("target_keys")
        lists: list[tuple[list, object]] = []
        if isinstance(declaration, list):
            lists.append((declaration, workflow.get("default_action")))
        elif isinstance(declaration, dict):
            for member in declaration.get("members") or []:
                if isinstance(member, dict):
                    lists.append(
                        (member.get("target_keys") or [], member.get("default_action"))
                    )
        for entries, default_action in lists:
            if default_action:
                continue
            bare = [
                entry for entry in entries
                if not (isinstance(entry, dict) and entry.get("action"))
            ]
            if bare:
                raise RuntimeError(
                    f"❌ workflow {name!r}: {bare} have no action and the list "
                    "declares no default_action, so the engine cannot know how to "
                    "run them. Declare `default_action:` for the list — a literal "
                    "action, or ${execution_context.ctl.operation} to follow the "
                    "invocation — or `action:` beneath each key"
                )


def validate_distinct_target_signatures(definitions: dict) -> None:
    """§Phase 73: two targets with the same INPUT SIGNATURE are one target twice.

    They would run identical procedures against identical resources under two
    separate committed pointers, neither aware of the other — the same hazard the
    dependency registries exist to catch, but between declarations rather than
    between resources.

    The signature must be COMPLETE. Comparing only the procedure and the instance
    params gives false positives: two targets can share a source, a procedure and
    their params while consuming different cfg, and then legitimately produce
    different resources.

    A static cfg check. It never observes resources and never decides which
    declaration is authoritative — it answers only whether a pair may coexist.
    """
    def signature(definition: dict) -> tuple:
        def frozen(value):
            if isinstance(value, dict):
                return tuple(sorted((k, frozen(v)) for k, v in value.items()))
            if isinstance(value, list):
                return tuple(frozen(v) for v in value)
            return value

        return (
            definition.get("source_key"),
            definition.get("ref_key"),
            definition.get("procedure_key"),
            frozen(definition.get("domains")),
            frozen(definition.get("cfg_keys")),
            frozen(definition.get("cfg_key_sets")),
            frozen(definition.get("input_params")),
            frozen(definition.get("input_param_sets")),
            frozen(definition.get("target_instance_params")),
        )

    seen: dict[tuple, str] = {}
    for key, definition in sorted((definitions or {}).items()):
        if not isinstance(definition, dict):
            continue
        sig = signature(definition)
        if None in sig[:3]:
            continue
        first = seen.get(sig)
        if first is not None:
            raise RuntimeError(
                f"❌ targets {first!r} and {key!r} declare the same input signature "
                "(source, ref, procedure, domains, cfg keys, input params, instance "
                "params). They would run identical work against identical resources "
                "under two committed pointers; declare one target, or make them differ"
            )
        seen[sig] = key


def validate_target_execution_identities(entries: object, *, label: str) -> dict:
    """Validate a target's `execution_identities:` — identities KEYED BY PROVIDER.

    A run may span providers, and so may a target: a root that touches two clouds
    needs credentials for both before a single plan. The provider is the key, so
    the engine picks N adapters instead of one and everything below each key stays
    opaque.
    """
    if not isinstance(entries, dict) or not entries:
        raise RuntimeError(
            f"❌ {label} execution_identities must be a non-empty mapping keyed by provider"
        )
    resolved: dict[str, dict] = {}
    for provider, execution in entries.items():
        if not isinstance(provider, str) or not provider.strip():
            raise RuntimeError(f"❌ {label} execution_identities keys must be provider names")
        resolved[provider] = validate_target_execution_identity(
            execution, label=f"{label} [{provider}]"
        )
    return resolved


def validate_target_execution_identity(execution: object, *, label: str) -> dict:
    """Validate ONE entry of a target's `execution_identities:` block.

    The provider is the KEY, so one target may declare several. The engine reads
    the generic shape only; every value inside (`account`, the role keys,
    credential source keys) is opaque here and interpreted by that adapter.
    """
    if not isinstance(execution, dict) or not execution:
        raise RuntimeError(f"❌ {label} execution must be a non-empty mapping")

    unknown = sorted(set(execution) - TARGET_EXECUTION_IDENTITY_FIELDS)
    if unknown:
        raise RuntimeError(
            f"❌ {label} execution has unknown fields {unknown}; "
            f"allowed: {sorted(TARGET_EXECUTION_IDENTITY_FIELDS)}"
        )

    value = execution.get("account")
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} execution.account must be a non-empty string")

    roles = execution.get("roles")
    if not isinstance(roles, dict) or not roles:
        raise RuntimeError(
            f"❌ {label} execution.roles must be a non-empty mapping; its KEYS are the "
            "provider's vocabulary, not the engine's"
        )
    for role_class, role_key in roles.items():
        if not isinstance(role_key, str) or not role_key.strip():
            raise RuntimeError(
                f"❌ {label} execution.roles.{role_class} must be a non-empty string"
            )

    for field in ("agreed_direct_credential_source_keys", "allowed_accounts"):
        if field not in execution:
            continue
        values = execution.get(field)
        if not isinstance(values, list) or not values or not all(
            isinstance(item, str) and item.strip() for item in values
        ):
            raise RuntimeError(
                f"❌ {label} execution.{field} must be a non-empty list of non-empty strings"
            )

    return execution

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
    run_targets, where the repo-local target_run manifest is loaded."""
    if execution_runtime_mode not in EXECUTION_RUNTIME_MODES:
        raise RuntimeError(f"❌ unknown execution runtime {execution_runtime_mode!r} (known: {sorted(EXECUTION_RUNTIME_MODES)})")
    allowed = ctl_allowed_execution_runtime_modes(ctl_cfg_root, ctl_profile)
    if execution_runtime_mode not in allowed:
        raise RuntimeError(
            f"❌ execution runtime {execution_runtime_mode!r} is not allowed by ctl profile {ctl_profile!r} (allowed: {sorted(allowed)})"
        )


def target_consent_opt_in_fields() -> set[str]:
    """Every per-target opt-in field any registered adapter asks for."""
    from utils.providers import REGISTERED_PROVIDERS, get_adapter

    fields: set[str] = set()
    for provider in REGISTERED_PROVIDERS:
        adapter = get_adapter(provider)
        for mode in adapter.supported_execution_access_modes():
            consent = adapter.target_consent(mode)
            if consent:
                fields.add(consent["opt_in_field"])
    return fields


def ctl_allowed_providers(ctl_cfg_root: Path, ctl_profile: str) -> list[str]:
    """Providers this ctl profile may run (§Phase 52 profile split).

    Declared, never defaulted: a profile states which providers it authorizes,
    and each one must carry its own policy block. Provider IDENTITY is engine
    vocabulary; everything inside a provider's block is not.
    """
    from utils.providers import REGISTERED_PROVIDERS

    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    raw = policy.get("allowed_providers")
    if not isinstance(raw, list) or not raw or not all(isinstance(p, str) for p in raw):
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} must declare allowed_providers as a "
            "non-empty list of provider names"
        )
    unknown = sorted(set(raw) - set(REGISTERED_PROVIDERS))
    if unknown:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} allowed_providers names unregistered "
            f"providers {unknown}; registered: {list(REGISTERED_PROVIDERS)}"
        )
    missing = sorted(p for p in raw if not isinstance(policy.get(p), dict))
    if missing:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} allows providers {missing} but declares no "
            f"policy block for them (expected a `{missing[0]}:` mapping in the profile)"
        )
    return list(raw)


def validate_ctl_allowed_providers(
    ctl_cfg_root: Path, ctl_profile: str, providers: list[str] | tuple[str, ...]
) -> None:
    allowed = ctl_allowed_providers(ctl_cfg_root, ctl_profile)
    stray = sorted(set(providers) - set(allowed))
    if stray:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} does not allow providers {stray} "
            f"(allowed: {allowed})"
        )


def ctl_profile_provider_policy(
    ctl_cfg_root: Path, ctl_profile: str, provider: str
) -> dict:
    """One provider's policy block, opaque to the engine — the adapter reads it."""
    ctl_allowed_providers(ctl_cfg_root, ctl_profile)
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile).get(provider)
    if not isinstance(policy, dict):
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} declares no {provider!r} policy block"
        )
    return policy


def ctl_allows_agreed_defer_ctl_state_backend_sync(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_agreed_defer_ctl_state_backend_sync")


def ctl_allows_ctl_state_forget(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    """Erasing the only evidence that something is provisioned is a different
    power from aging out history, so it is a different grant."""
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_ctl_state_forget")


def ctl_allows_ctl_state_history_maintenance(
    ctl_cfg_root: Path, ctl_profile: str
) -> bool:
    return ctl_profile_bool(
        ctl_cfg_root, ctl_profile, "allow_ctl_state_history_maintenance"
    )


def ctl_allows_force_skip_ctl_state_backend_sync(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_force_skip_ctl_state_backend_sync")


def validate_cadence_supported(provider: str, mode: str) -> None:
    """The ADAPTER decides which cadences exist for it; the engine only asks.

    Without this the set of legal modes lived in engine strings, so adding a
    provider that refreshes differently meant editing the engine.
    """
    supported = get_provider_adapter(provider).supported_credential_refresh_modes()
    if mode not in supported:
        raise RuntimeError(
            f"❌ --credential-refresh-mode {provider}={mode} is not supported by "
            f"{provider} (supported: {', '.join(sorted(supported)) or 'none'})"
        )


def validate_credential_refresh_modes(
    ctl_cfg_root: Path,
    ctl_profile: str,
    requested: dict[str, str],
    providers,
    execution_access_modes: dict[str, str] | None = None,
) -> dict[str, str]:
    """Every participating provider states its cadence, and the profile allows it.

    Per provider because only some have expiring sessions at all — the engine
    holds an opaque provider->value map and hands each adapter its own, exactly
    as it does for execution access modes.

    There is no inherited default: an unnamed provider is a caller who skipped
    the choice, not one who wanted the cheap option.
    """
    selected = dict(requested or {})
    missing = [p for p in providers if p not in selected]
    if missing:
        raise RuntimeError(
            "❌ --credential-refresh-mode must name every provider given to "
            f"--providers; missing: {', '.join(sorted(missing))}"
        )
    unknown = [p for p in selected if p not in providers]
    if unknown:
        raise RuntimeError(
            "❌ --credential-refresh-mode names providers not in --providers: "
            + ", ".join(sorted(unknown))
        )
    for provider, mode in sorted(selected.items()):
        validate_cadence_supported(provider, mode)
        validate_cadence_against_access_mode(
            {provider: mode}, execution_access_modes
        )
        allowed = ctl_profile_provider_policy(
            ctl_cfg_root, ctl_profile, provider
        ).get("allowed_credential_refresh_modes") or []
        if mode not in allowed:
            raise RuntimeError(
                f"❌ ctl profile {ctl_profile!r} does not allow credential refresh "
                f"mode {mode!r} for provider {provider!r} "
                f"(allowed: {', '.join(allowed) or 'none'})"
            )
    return selected


def ctl_allows_force_skip_guardrails(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_force_skip_guardrails")


def ctl_allows_skip_children_precheck(ctl_cfg_root: Path, ctl_profile: str) -> bool:
    return ctl_profile_bool(ctl_cfg_root, ctl_profile, "allow_skip_children_precheck")


def validate_skip_children_precheck(
    ctl_cfg_root: Path, ctl_profile: str, requested: bool
) -> None:
    if requested and not ctl_allows_skip_children_precheck(ctl_cfg_root, ctl_profile):
        raise RuntimeError(
            "❌ --skip-children-precheck was requested, but ctl profile "
            f"{ctl_profile!r} does not grant allow_skip_children_precheck"
        )


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


def validate_skip_up_to_date_ref_policy(
    skip_up_to_date: bool | None, ref_policy: str, ctl_profile: str
) -> None:
    """Fail loud on a --skip-up-to-date=true that can never reuse.

    The reuse gate only reuses a committed result when ref_policy is
    commit_required (a clean, commit-pinned source). Under any other policy
    (e.g. local_dirty_allowed) reuse is structurally impossible, so
    --skip-up-to-date true would be a silent no-op — every run re-executes.
    Reject it instead of silently ignoring the flag."""
    if skip_up_to_date and not ref_policy_requires_commits(ref_policy):
        raise RuntimeError(
            f"❌ --skip-up-to-date true cannot reuse under ctl profile {ctl_profile!r} "
            f"(ref_policy {ref_policy!r}): reuse requires ref_policy 'commit_required' "
            "(a clean, commit-pinned source). Pass --skip-up-to-date false, or use a "
            "commit_required profile."
        )


EXECUTION_CONTEXT_CONSTRAINT_FIELDS = frozenset(
    {"when_all", "when_any", "require_present", "allowed_values"}
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
        if "when" in constraint:
            raise RuntimeError(
                f"❌ execution context constraint #{idx} uses `when`, which is removed: {path}; "
                "use `when_all` (list of match-mappings, ALL must match) — a multi-path `when` "
                "mapping becomes N single-path entries — or `when_any` (ANY matches)"
            )
        unknown = sorted(set(constraint) - set(EXECUTION_CONTEXT_CONSTRAINT_FIELDS))
        if unknown:
            raise RuntimeError(
                f"❌ execution context constraint #{idx} has unknown fields {unknown}: {path}; "
                f"allowed: {sorted(EXECUTION_CONTEXT_CONSTRAINT_FIELDS)}"
            )
        for clause in ("when_all", "when_any"):
            if clause not in constraint:
                continue
            entries = constraint.get(clause)
            if not isinstance(entries, list) or not entries:
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} {clause} must be a non-empty list "
                    f"of match-mappings: {path}"
                )
            if not all(isinstance(entry, dict) and entry for entry in entries):
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} {clause} entries must be non-empty "
                    f"mappings: {path}"
                )
        require_present = constraint.get("require_present") or []
        allowed_values = constraint.get("allowed_values") or {}
        if not isinstance(require_present, list) or not all(isinstance(item, str) and item for item in require_present):
            raise RuntimeError(f"❌ execution context constraint #{idx} require_present must be a list of non-empty strings: {path}")
        if not isinstance(allowed_values, dict):
            raise RuntimeError(f"❌ execution context constraint #{idx} allowed_values must be a mapping: {path}")
        constraints.append(constraint)
    return constraints


def execution_context_constraint_applies(
    constraint: dict, execution_context: dict[str, object], *, idx: int
) -> bool:
    """Does this constraint's gate match?

    `when_all` — every entry must match (this is what the removed `when` did across
    the paths of one mapping). `when_any` — at least one entry must match (OR, which
    the old mapping-only form could not express, forcing duplicated rules). Both
    present → AND of the two. Neither present → the constraint always applies.
    """
    entries_all = constraint.get("when_all") or []
    if entries_all and not all(
        selector_matches(entry, execution_context, label=f"execution_context_constraints[{idx}].when_all[{n}]")
        for n, entry in enumerate(entries_all)
    ):
        return False

    entries_any = constraint.get("when_any") or []
    if entries_any and not any(
        selector_matches(entry, execution_context, label=f"execution_context_constraints[{idx}].when_any[{n}]")
        for n, entry in enumerate(entries_any)
    ):
        return False

    return True


def validate_execution_context_constraints(ctl_cfg_root: Path, execution_context: dict[str, object]) -> None:
    for idx, constraint in enumerate(load_execution_context_constraints(ctl_cfg_root), start=1):
        if not execution_context_constraint_applies(constraint, execution_context, idx=idx):
            continue
        when = {
            key: constraint[key]
            for key in ("when_all", "when_any")
            if key in constraint
        }

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




def validate_cadence_against_access_mode(
    refresh_modes: dict[str, str], access_modes: dict[str, str] | None
) -> None:
    """Refuse a cadence that cannot do anything on the chosen access path.

    Checked during ARGUMENT finalization, not inside the pipeline: it needs no
    cfg, and catching it after identity preflight would mean resolving
    credentials before telling the caller the request was meaningless.

    WHICH cadence works on WHICH access path is the ADAPTER's knowledge — whether
    a fresh session can be minted depends on how that provider authenticates, not
    on the engine's step loop. The engine names no cadence and no access mode of
    its own here; it asks and enforces the answer.
    """
    for provider, mode in sorted((refresh_modes or {}).items()):
        access = (access_modes or {}).get(provider)
        if not access:
            continue
        usable = get_provider_adapter(provider).credential_refresh_mode_access_modes(
            mode
        )
        if usable and access not in usable:
            raise RuntimeError(
                f"❌ --credential-refresh-mode {provider}={mode} has no effect "
                f"under --execution-access-mode {provider}={access}: that path "
                "re-uses the credential already resolved rather than acquiring a "
                f"new one. {provider} honours {mode} only under "
                f"{', '.join(usable)}"
            )


def finalize_common_args(args: argparse.Namespace) -> None:
    """Normalize execution-params CLI args into a map and common values."""
    args.execution_params = selectors_to_map(args.execution_param, label="execution param")
    args.ctl_state_local_root = normalize_ctl_state_local_root(args.ctl_state_local_root)
    validate_provider_options(
        getattr(args, "provider_options", None), getattr(args, "providers", ()) or ()
    )
    args.execution_access_modes = normalize_execution_access_modes(args)
    args.credential_refresh_modes = selectors_to_map(
        getattr(args, "credential_refresh_modes", []) or [],
        label="credential refresh mode",
    )
    validate_cadence_against_access_mode(
        args.credential_refresh_modes, args.execution_access_modes
    )
    args.force_skip_execution_identity_preflight_check = (
        normalize_force_skip_execution_identity_preflight_check(args)
    )
    # §Phase 50: --status is gone from the run parsers (status is standalone).
    if (
        getattr(args, "execution_identity_preflight_check_only", False)
        and getattr(args, "force_skip_execution_identity_preflight_check", None)
    ):
        raise RuntimeError(
            "❌ --execution-identity-preflight-check-only and "
            "--force-skip-execution-identity-preflight-check are mutually exclusive"
        )
    # --skip-up-to-date is an explicit true/false with no default. A normal run
    # reaches the reuse-vs-rerun decision and must state intent; the exit-early
    # preflight-only mode never does, so it stays optional there.
    if hasattr(args, "skip_up_to_date"):
        exits_before_execution = getattr(
            args, "execution_identity_preflight_check_only", False
        )
        if args.skip_up_to_date is None and not exits_before_execution:
            raise RuntimeError(
                "❌ --skip-up-to-date is required (true or false) for a normal run; "
                "omit it only with --execution-identity-preflight-check-only"
            )
    args.run_id = generate_uuid7()


def normalize_execution_access_modes(args: argparse.Namespace) -> dict[str, str]:
    """Resolve the per-provider access mode map.

    `--execution-access-mode <provider>=<mode>,...` — one mode per participating
    provider.
    A provider left unnamed takes the engine default; the engine validates only
    that every named provider was declared, because the MODE NAMES belong to the
    adapters (validated against supported_execution_access_modes()).
    """
    declared = list(getattr(args, "providers", ()) or ())
    modes = dict(getattr(args, "execution_access_modes", None) or {})

    stray = sorted(set(modes) - set(declared))
    if stray:
        raise RuntimeError(
            f"❌ --execution-access-mode names providers not declared in --providers "
            f"{declared}: {stray}"
        )
    missing = sorted(set(declared) - set(modes))
    if missing:
        raise RuntimeError(
            "❌ --execution-access-mode must state the mode for every declared "
            f"provider (no default): missing {missing}"
        )

    from utils.providers import get_adapter

    options = getattr(args, "provider_options", None)
    for provider, mode in modes.items():
        adapter = get_adapter(provider)
        supported = adapter.supported_execution_access_modes()
        if mode not in supported:
            raise RuntimeError(
                f"❌ provider {provider!r} does not support execution access mode "
                f"{mode!r}; it advertises {sorted(supported)} (see `ctl.py providers`)"
            )
        # An option may only be meaningful in one mode (a substitute credential
        # means nothing unless the run stops resolving identities). Passing it in
        # another mode is a contradiction, not a silent no-op.
        implied = adapter.execution_access_mode_from_options(
            provider_options_for(options, provider)
        )
        if implied is not None and implied != mode:
            raise RuntimeError(
                f"❌ the --provider-options given for {provider!r} are only valid in "
                f"execution access mode {implied!r}, but it runs {mode!r}"
            )
    return modes


def normalize_force_skip_execution_identity_preflight_check(
    args: argparse.Namespace,
) -> list[str]:
    """Resolve the providers whose live identity check is skipped.

    A subset of --providers. Skipping is meaningless for a provider that has no
    live check to begin with, and for one already running without a resolved
    identity — both are named rather than silently ignored.
    """
    requested = list(getattr(args, "force_skip_execution_identity_preflight_check", ()) or [])
    if not requested:
        return []
    declared = list(getattr(args, "providers", ()) or ())
    stray = sorted(set(requested) - set(declared))
    if stray:
        raise RuntimeError(
            "❌ --force-skip-execution-identity-preflight-check names providers not "
            f"declared in --providers {declared}: {stray}"
        )

    from utils.providers import get_adapter

    modes = getattr(args, "execution_access_modes", None) or {}
    for provider in requested:
        adapter = get_adapter(provider)
        if not adapter.supports_identity_preflight():
            raise RuntimeError(
                f"❌ provider {provider!r} declares no live execution-identity check, "
                "so there is nothing to skip (see `ctl.py providers`)"
            )
        if not adapter.resolves_execution_identity(
            execution_access_mode_for(modes, provider)
        ):
            raise RuntimeError(
                f"❌ provider {provider!r} runs mode "
                f"{execution_access_mode_for(modes, provider)!r} without resolving an "
                "execution identity, so its preflight check does not run and cannot "
                "be skipped"
            )
    return sorted(set(requested))


def execution_access_mode_for(modes: dict[str, str] | str | None, provider: str) -> str:
    """One provider's mode from the per-provider map."""
    if isinstance(modes, str):          # already narrowed by a caller
        return modes
    try:
        return (modes or {})[provider]
    except KeyError:
        raise RuntimeError(
            f"❌ no execution access mode resolved for provider {provider!r} "
            f"(have: {sorted(modes or {})})"
        ) from None


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
    """Prefer a relative display path when the path is under a known root.

    A materialized preset is shown by the IMPORT it came from, not by its scratch
    directory: the scratch is freed at the end of the discovery pass, so its path
    names nothing a reader could go and look at.
    """
    path_obj = Path(path).expanduser()
    if not path_obj.is_absolute():
        return str(path_obj)

    for workspace, import_path in _MATERIALIZED_IMPORT_LABELS.items():
        try:
            inside = path_obj.relative_to(workspace)
        except ValueError:
            continue
        return f"{import_path.rstrip('/')}/{inside}" if str(inside) != "." else import_path

    for root in relative_roots:
        try:
            return str(path_obj.relative_to(root))
        except ValueError:
            continue

    return str(path_obj)


def strip_ansi(text: str) -> str:
    """Remove ANSI color codes from text."""
    return ANSI_ESCAPE.sub('', text)


@contextlib.contextmanager
def target_run_log(child_run_dir: Path | None):
    """§Phase 61(c): a target run writes its OWN log while it executes.

    Implemented as a SECOND file handler rather than by redirecting: the workflow
    keeps the aggregate of everything (the operator's single reading surface) and
    the target gets its own copy, which is what makes a target run independently
    inspectable. Duplication is deliberate.
    """
    if child_run_dir is None:
        yield None
        return
    logs_dir = child_run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{SERVICE_ID}_{child_run_dir.name}.log"
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(handler)
    try:
        yield log_path
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()


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
    if any(getattr(args, field, None) for field in ("source", "ref", "domain", "procedure", "execution_provider", "execution_account", "execution_role", "affected_target_keys")):
        raise RuntimeError("❌ workflow runner does not accept procedure synthetic target args")


def validate_target_args(args: argparse.Namespace) -> None:
    """Validate args for a declared single-target run."""
    if not getattr(args, "target", None):
        raise RuntimeError("❌ target runner requires --target")
    if getattr(args, "workflow", None):
        raise RuntimeError("❌ target runner does not accept --workflow")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for target runs")
    if any(getattr(args, field, None) for field in ("source", "ref", "domain", "procedure", "execution_provider", "execution_account", "execution_role", "affected_target_keys")):
        raise RuntimeError("❌ target runner does not accept procedure synthetic target args")


def validate_maintenance_args(args: argparse.Namespace) -> None:
    """Validate args for one explicit maintenance operation."""
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for maintenance")
    if any(
        getattr(args, field, None)
        for field in (
            "source", "ref", "domain", "procedure",
            "execution_provider", "execution_account", "execution_role",
            "affected_target_keys",
        )
    ):
        raise RuntimeError(
            "❌ maintenance runner does not accept synthetic target args"
        )
    action = getattr(args, "maintenance_action", None)
    if not action:
        raise RuntimeError("❌ --maintenance-action is required for maintenance")
    if action == "unlock-ctl-state":
        # Which of the two ctl-state locks. `both` is the default because a run
        # that dies holds both, and clearing one alone only moves where the next
        # run is refused. It means the remote lock and THIS machine's local one:
        # remote is namespace-wide, local is one directory, so `both` is not a
        # claim to have cleared every local lock everywhere.
        scope = getattr(args, "unlock_scope", None) or "both"
        args.unlock_scope = scope
        if scope in ("local", "both") and not getattr(args, "ctl_state_local_root", None):
            raise RuntimeError(
                f"❌ --scope {scope} releases the local lock and requires "
                "--ctl-state-local-root"
            )
        if not getattr(args, "lock_id", None):
            raise RuntimeError(
                "❌ --lock-id is required for --maintenance-action=unlock-ctl-state"
            )
        return
        if not ctl_state_lock_matches(args.ctl_state_local_root, args.lock_id):
            raise RuntimeError(
                f"❌ --lock-id {args.lock_id!r} does not hold the ctl-state lock"
            )
        return
    if getattr(args, "target", None):
        raise RuntimeError(f"❌ --target is not valid for {action}")
    if action == "forget":
        missing = [
            flag
            for flag, value in (
                ("--older-than", getattr(args, "older_than", None)),
                ("--address", getattr(args, "forget_address", None)),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                f"❌ forget requires {' and '.join(missing)}: both filters are "
                "always stated, so nothing is removed on one the caller did not write"
            )
        args.forget_scope = getattr(args, "unlock_scope", None) or "both"
        if args.forget_scope in ("local", "both") and not getattr(
            args, "ctl_state_local_root", None
        ):
            raise RuntimeError(
                f"❌ --scope {args.forget_scope} forgets local records and requires "
                "--ctl-state-local-root"
            )
        return
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


def validate_procedure_args(args: argparse.Namespace) -> None:
    """Validate args for a synthetic repo-local procedure run."""
    if getattr(args, "workflow", None) or getattr(args, "target", None):
        raise RuntimeError("❌ procedure runner does not accept --workflow or --target")
    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is not supported for procedure runs")
    missing = [f for f in ("source", "ref", "domain", "procedure") if not getattr(args, f, None)]
    if missing:
        raise RuntimeError(
            "❌ procedure needs " + ", ".join(f"--{m.replace('_', '-')}" for m in missing)
        )
    execution_fields = ("execution_provider", "execution_account", "execution_role")
    supplied = [f for f in execution_fields if getattr(args, f, None)]
    if supplied and len(supplied) != len(execution_fields):
        missing_execution = [f for f in execution_fields if f not in supplied]
        raise RuntimeError(
            "❌ a synthetic target's execution is declared in full or not at all; missing "
            + ", ".join(f"--{m.replace('_', '-')}" for m in missing_execution)
        )
    affected_target_keys = getattr(args, "affected_target_keys", None) or []
    if affected_target_keys:
        args.affected_target_keys = normalize_target_keys(affected_target_keys, label="--affected-target-key")
    if args.action in MUTATING_ACTIONS and not getattr(args, "affected_target_keys", None):
        raise RuntimeError("❌ mutating procedure runs require at least one --affected-target-key")





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

def workflow_member_actions(workflow_cfg: dict) -> set[str]:
    """Every action a workflow's member entries ask of their targets."""
    actions: set[str] = set()
    for entry in (workflow_cfg or {}).get("target_runs") or []:
        if isinstance(entry, dict) and entry.get("action"):
            actions.add(str(entry["action"]))
    return actions


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
        # fat target carries the repo-local procedure; a dict target_run entry may still override
        child_procedure = target_override.get("procedure") or target_cfg.get("procedure")

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

        # §Phase 60: a target_run carries its declared domains + per-domain key
        # contract. A domain-generic target whose axis is unbound arrives with
        # domains=None and is not materializable.
        target_domains = target_cfg.get("domains")
        target_cfg_keys = target_cfg.get("cfg_keys")
        if target_domains is not None and not isinstance(target_domains, list):
            raise RuntimeError(f"Target run target {target_key!r} domains must be a list")
        if target_cfg_keys is not None and not isinstance(target_cfg_keys, dict):
            raise RuntimeError(f"Target run target {target_key!r} cfg_keys must be a map")

        active_target_run = {
            "target": target_key,
            "source": target_source,
            "ref": target_ref,
            "branch": branch,
            "commit": commit,
            "procedure": child_procedure,
            "domains": target_domains,
            "cfg_keys": target_cfg_keys,
        }
        # §Phase 73: a member entry may declare the ACTION this target performs.
        # Carried onto the run record so the spawn hands the child its own verb
        # rather than the parent's, and validated against the target's own
        # allowlist — the workflow may choose, never widen.
        member_action = target_override.get("action")
        if member_action is not None:
            allowed = target_cfg.get("allowed_actions") or []
            if member_action not in allowed:
                raise RuntimeError(
                    f"❌ workflow member {target_key!r} declares action "
                    f"{member_action!r}, which that target does not allow "
                    f"(allowed: {', '.join(sorted(allowed)) or 'none'})"
                )
            active_target_run["action"] = member_action
        required_overlays = target_cfg.get("requires_plt_overlays")
        if required_overlays:
            active_target_run["requires_plt_overlays"] = list(required_overlays)
        for behavior_field in (
            "provisions_ctl_state_backend",
            "allow_agreed_defer_ctl_state_backend_sync",
            *sorted(target_consent_opt_in_fields()),
        ):
            if target_cfg.get(behavior_field) is not None:
                active_target_run[behavior_field] = target_cfg[behavior_field]

        target_execution_identity = target_override.get("execution_identities") or target_cfg.get("execution_identities")
        if target_execution_identity is not None:
            active_target_run["execution_identities"] = validate_target_execution_identities(
                target_execution_identity, label=f"target run {target_run_id!r}"
            )
            active_target_run["providers"] = validate_target_providers(
                target_override.get("providers") or target_cfg.get("providers"),
                active_target_run["execution_identities"],
                label=f"target run {target_run_id!r}",
            )

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
# `_inputs/` holds DATA a consumer reads through a declared input source — not
# cfg. It is ignored outright: never merged as a content-key resource, never
# validated as cfg, and reachable only through the locator its consumer declares.
# Several files in it may carry the same keys (one per landing zone, say), which
# a content-key merge would treat as a collision.
_IGNORED_CFG_DIRS = ("_inputs",)


def collect_resource(ctl_cfg_root: Path, key: str, *, entry_depth: int = 1) -> dict:
    """Merge a top-level resource map identified by `key` across every cfg file.

    A resource's type is its top-level YAML key (content-key), not its filename: a
    file with a `cfg_key_sets:` key contributes cfg-key-sets wherever it lives. The maps are
    unioned across all `*.yaml` under `ctl_cfg_root`; a duplicate entry is a load
    error (same rule as targets), order-independent. `entry_depth` is how deep the
    unique entries sit: 1 for flat catalogs (target_sources/cfg_key_sets),
    2 for action-keyed `variants`, 3 for `workflows.<action>.<scope>.<name>` and
    `providers.<name>.<section>.<entry>`.
    Intermediate levels merge; the entry level collides. Dir-routed trees (see
    `_IGNORED_CFG_DIRS`) are skipped — they have dedicated loaders.
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
        if any(part in _IGNORED_CFG_DIRS for part in rel.parts):
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


def load_ctl_sources(ctl_cfg_root: Path) -> dict:
    """Load `ctl_sources.<key>`: the data ctl SOURCES, by key.

    Engine-generic on purpose. An entry declares WHAT the payload is (`type`),
    what a collision MEANS (`conflict_resolution`), and a LIST of sources. The
    engine validates that shape only — it learns no type or policy VALUES, and no
    provider vocabulary; `provider` sits on the SOURCE, so one input may be
    assembled from sources belonging to different providers and this collection
    cannot live inside any one provider's catalog. The consumer of an input
    validates the values and the source fields it implements.
    """
    entries = collect_resource(ctl_cfg_root, "ctl_sources")
    for source_key, entry in entries.items():
        label = f"ctl_sources.{source_key}"
        if not isinstance(entry, dict):
            raise RuntimeError(f"❌ {label} must be a mapping: {ctl_cfg_root}")
        unknown = sorted(set(entry) - {"type", "conflict_resolution", "sources"})
        if unknown:
            raise RuntimeError(f"❌ {label} has unknown fields {unknown}: {ctl_cfg_root}")
        for field, why in (
            ("type", "the combine operation follows from it: a map is merged by key, "
                     "a list is concatenated"),
            ("conflict_resolution", "left implicit, source order would silently decide "
                                    "which value wins"),
        ):
            if not isinstance(entry.get(field), str) or not entry[field].strip():
                raise RuntimeError(
                    f"❌ {label}.{field} must be declared — {why}: {ctl_cfg_root}"
                )
        sources = entry.get("sources")
        if not isinstance(sources, list) or not sources:
            raise RuntimeError(f"❌ {label}.sources must be a non-empty list: {ctl_cfg_root}")
        for index, source in enumerate(sources):
            if not isinstance(source, dict):
                raise RuntimeError(f"❌ {label}.sources[{index}] must be a mapping: {ctl_cfg_root}")
            # Only these two are universal. Everything else is provider-shaped,
            # so the engine cannot know it and the consumer validates it.
            for field in ("provider", "format"):
                if not isinstance(source.get(field), str) or not source[field].strip():
                    raise RuntimeError(
                        f"❌ {label}.sources[{index}].{field} must be a non-empty string: "
                        f"{ctl_cfg_root}"
                    )
    return entries


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
    if not isinstance(import_keys, list) or not all(
        isinstance(value, str) and value for value in import_keys
    ):
        raise RuntimeError(
            f"❌ workflow {name!r} import_workflow_keys must be a list of non-empty strings"
        )
    # §Phase 73: an entry is a bare key, or a key with its OWN action. A member
    # without one inherits the invoked operation, which is what keeps every
    # existing workflow working unchanged.
    entries = normalize_target_entries(
        wf.get("target_keys") or [],
        label=f"workflow {name!r} target_keys",
        default_action=wf.get("default_action"),
    )
    target_runs: list = []
    for workflow_key in import_keys:
        target_runs.extend(expand_workflow_imports(action_workflows, workflow_key, (*_stack, name)))
    for key, action in entries:
        target_runs.append(
            {"id": key, "target": key, "action": action} if action else key
        )
    # A key MAY repeat when the actions differ — that is a composition doing two
    # things to one instance, and order decides the final state. The same key with
    # the same action twice is still a mistake.
    seen: set = set()
    for entry in target_runs:
        signature = workflow_target_run_signature(entry)
        if signature in seen:
            raise RuntimeError(
                f"❌ workflow {name!r} has duplicate target key {signature[0]!r} after "
                "import expansion"
                + (f" (action {signature[1]})" if signature[1] else "")
            )
        seen.add(signature)
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


RUNTIME_SCALAR_TOKEN_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")


def resolve_runtime_scalar(
    value, context: dict[str, object], *, label: str, tolerate_missing: bool = False
) -> str | None:
    """Resolve ${execution_context.<ns>.<key>} placeholders from the flat
    execution context (dotted keys).

    With `tolerate_missing`, an unbound reference yields None instead of raising —
    used where a domain-GENERIC declaration may simply not apply to this run.
    """
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty string")

    token_re = RUNTIME_SCALAR_TOKEN_RE
    if tolerate_missing and any(
        context.get(ref) in (None, "") for ref in token_re.findall(value)
    ):
        return None

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


def run_providers(execution_context: dict[str, object]) -> list[str]:
    """The providers this run DECLARED (--providers), in order."""
    declared = execution_context.get(f"{EXECUTION_CONTEXT_ROOT}.ctl.providers") or []
    if isinstance(declared, str):
        declared = [declared]
    providers = [str(p).strip() for p in declared if str(p).strip()]
    if not providers:
        raise RuntimeError(
            "❌ no providers declared for this run; pass --providers <name>[,<name>...]"
        )
    return providers


def run_provider_adapters(execution_context: dict[str, object]) -> list[tuple[str, object]]:
    """(name, adapter) for every participating provider."""
    return [(name, get_provider_adapter(name)) for name in run_providers(execution_context)]


def run_provider_adapter(execution_context: dict[str, object]):
    """The single participating provider's adapter.

    The run-level catalog/preflight path still assumes ONE adapter per run (it keeps
    one `provider_catalogs` bundle). Declaring several providers is accepted by the
    CLI, gating and coverage guard, but this path is not wired for it yet — so fail
    loud here rather than silently picking the first.
    """
    return get_provider_adapter(run_provider(execution_context))


def run_provider(execution_context: dict[str, object]) -> str:
    """The single participating provider's name (same single-provider fence)."""
    providers = run_providers(execution_context)
    if len(providers) > 1:
        raise RuntimeError(
            f"❌ this run declares multiple providers {providers}, but the run-level "
            "provider catalog/preflight path is single-provider today; run them as "
            "separate invocations until per-target adapter dispatch lands"
        )
    return providers[0]


def target_run_providers(target_run: dict) -> list[str]:
    """Every provider a target_run executes against, from its execution_identities.

    A target may declare more than one: a root touching two clouds needs
    credentials for both before a single plan.
    """
    identities = (target_run or {}).get("execution_identities") or {}
    if not identities:
        raise RuntimeError("❌ target_run execution_identities declares no provider")
    return sorted(identities)


def provider_inputs(
    provider: str,
    execution_access_modes: dict[str, str] | str | None,
    provider_options: dict[str, str] | None,
) -> tuple[str, dict[str, str]]:
    """Narrow the run's per-provider inputs to ONE provider, for an adapter call.

    An adapter is never handed another provider's mode or options: the mode names
    and option keys are its own vocabulary and mean nothing outside it.
    """
    return (
        execution_access_mode_for(execution_access_modes, provider),
        provider_options_for(provider_options, provider),
    )


def ctl_state_publication_access_mode(adapter, execution_access_mode: str) -> str:
    """The mode ctl-state publication runs under, given the run's mode.

    Publication always takes the provider's NORMAL path — escalated target access
    is about reaching the target, not about writing ctl-state. The one exception
    is a mode that resolves no execution identity at all: there is no normal path
    to fall back to, so it carries over. Both mode names come from the adapter.
    """
    if not adapter.resolves_execution_identity(execution_access_mode):
        return execution_access_mode
    return adapter.normal_execution_access_mode()


def collect_provider_cfg_findings(
    ctl_cfg_root: Path, execution_context: dict[str, object]
) -> list[dict]:
    """Stage-1 provider cfg well-formedness findings, once per participating provider."""
    findings: list[dict] = []
    for _name, adapter in run_provider_adapters(execution_context):
        findings.extend(
            adapter.collect_provider_cfg_findings(
                ctl_cfg_root, execution_context=execution_context
            )
        )
    return findings


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












def parse_provider_options(value: str) -> dict[str, str]:
    """Parse `--provider-options` into a flat map of provider-namespaced keys.

    ONE generic engine arg instead of per-adapter flags (which would explode the
    CLI as providers are added). The engine parses key=value and NEVER interprets
    either side; a key's leading segment routes it to that provider's adapter,
    which owns and validates its own option vocabulary.
    """
    options: dict[str, str] = {}
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                f"provider option must use <provider>.<key>=<value>, got: {item!r}"
            )
        key, raw = item.split("=", 1)
        key, raw = key.strip(), raw.strip()
        if not key or not raw:
            raise argparse.ArgumentTypeError(
                f"provider option must use non-empty <provider>.<key>=<value>, got: {item!r}"
            )
        if "." not in key:
            raise argparse.ArgumentTypeError(
                f"provider option key must be provider-namespaced (<provider>.<key>), got: {key!r}"
            )
        if key in options:
            raise argparse.ArgumentTypeError(f"duplicate provider option {key!r}")
        options[key] = raw
    if not options:
        raise argparse.ArgumentTypeError(f"expected <provider>.<key>=<value>, got: {value!r}")
    return options


class ProviderOptionsAction(argparse.Action):
    """Merge repeated/comma-separated --provider-options into one flat map."""

    def __call__(self, parser, namespace, values, option_string=None):
        merged = dict(getattr(namespace, self.dest, None) or {})
        try:
            for key, value in parse_provider_options(values).items():
                if key in merged:
                    raise argparse.ArgumentTypeError(f"duplicate provider option {key!r}")
                merged[key] = value
        except argparse.ArgumentTypeError as exc:
            raise argparse.ArgumentError(self, str(exc)) from exc
        setattr(namespace, self.dest, merged)


class ExecutionAccessModesAction(argparse.Action):
    """Merge repeated/comma-separated --execution-access-mode into one map.

    Parses shape only (`provider=mode`); the MODE NAMES are the adapters', so
    they are validated later against each adapter's advertised set.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        merged = dict(getattr(namespace, self.dest, None) or {})
        for pair in parse_comma_list(values):
            if "=" not in pair:
                raise argparse.ArgumentError(
                    self, f"expected PROVIDER=MODE, got {pair!r}"
                )
            provider, mode = (part.strip() for part in pair.split("=", 1))
            if not provider or not mode:
                raise argparse.ArgumentError(
                    self, f"expected PROVIDER=MODE, got {pair!r}"
                )
            if provider in merged and merged[provider] != mode:
                raise argparse.ArgumentError(
                    self, f"conflicting execution access modes for provider {provider!r}"
                )
            merged[provider] = mode
        setattr(namespace, self.dest, merged)


def provider_options_for(options: dict[str, str] | None, provider: str) -> dict[str, str]:
    """The subset of options addressed to one provider, with its prefix stripped."""
    prefix = f"{provider}."
    return {
        key[len(prefix):]: value
        for key, value in (options or {}).items()
        if key.startswith(prefix)
    }


def validate_provider_options(
    options: dict[str, str] | None, providers: list[str] | tuple[str, ...]
) -> None:
    """Validate provider options: addressed to a declared provider, and a key
    that provider actually offers. The engine checks the ADDRESS; each adapter
    checks its own KEYS — the engine knows none of them."""
    validate_provider_options_addressing(options, providers)
    from utils.providers import get_adapter

    for provider in providers:
        get_adapter(provider).validate_provider_options(
            provider_options_for(options, provider)
        )


def validate_provider_options_addressing(
    options: dict[str, str] | None, providers: list[str] | tuple[str, ...]
) -> None:
    """Every option must address a provider that is actually participating."""
    declared = set(providers or ())
    stray = sorted(key for key in (options or {}) if key.split(".", 1)[0] not in declared)
    if stray:
        raise RuntimeError(
            "❌ --provider-options address providers not declared in --providers "
            f"{sorted(declared)}: {stray}"
        )


def resolve_provider_implementation_key(
    provider_options: dict[str, str] | None, provider: str
) -> str:
    """The credential implementation this run wants from one provider.

    WHERE a run executes (execution_runtime_mode) and HOW it authenticates are
    INDEPENDENT axes; this reads only the latter, from the provider's own
    options. REQUIRED — the engine has no implementation to default to, and it
    does not interpret the value: the adapter does.
    """
    options = provider_options_for(provider_options, provider)
    implementation_key = options.get("credential_implementation")
    if not implementation_key:
        raise RuntimeError(
            f"❌ no credential implementation declared for provider {provider!r}; "
            f"pass --provider-options {provider}.credential_implementation=... "
            f"(see `ctl.py providers`)"
        )
    return implementation_key


def run_provider_implementation_key(args: argparse.Namespace) -> str:
    """The single declared provider's credential implementation, from CLI args."""
    providers = list(getattr(args, "providers", ()) or ())
    if len(providers) != 1:
        raise RuntimeError(
            f"❌ this run declares providers {providers}, but the run-level "
            "provider catalog/preflight path is single-provider today; run them as "
            "separate invocations until per-target adapter dispatch lands"
        )
    return resolve_provider_implementation_key(
        getattr(args, "provider_options", None), providers[0]
    )


def parse_comma_list(value: str) -> list[str]:
    """Comma-separated list arg, order-preserving and duplicate-free."""
    items: list[str] = []
    for raw in value.split(","):
        item = raw.strip()
        if not item:
            continue
        if item not in items:
            items.append(item)
    if not items:
        raise argparse.ArgumentTypeError(f"expected a comma-separated list, got: {value!r}")
    return items


def validate_target_provider_coverage(
    active_target_runs: dict, providers: list[str] | tuple[str, ...]
) -> None:
    """Every provider a selected target declares must be among the run's.

    A target may declare several identities, so its provider SET must be a subset
    of `--providers`.

    The provider set is DECLARED (--providers), never inferred from the targets, so
    a target reaching for an undeclared provider fails loud instead of silently
    widening the run.
    """
    declared = set(providers or ())
    offenders = []
    for target_run_id, target_run in active_target_runs.items():
        undeclared = sorted(
            set(target_run.get("execution_identities") or {}) - declared
        )
        if undeclared:
            offenders.append(f"{target_run_id} (providers {undeclared})")
    offenders = sorted(offenders)
    if offenders:
        raise RuntimeError(
            "❌ selected target_runs use providers not declared in --providers "
            f"{sorted(declared)}: " + ", ".join(offenders)
        )


def validate_target_execution_identity_coverage(
    active_target_runs: dict,
    *,
    execution_access_modes: dict[str, str] | None = None,
) -> None:
    """Every target_run declares its `execution_identity:` block, always.

    The one exception is a run where NO provider resolves an execution identity
    (every provider runs on a substitute credential) — then there is nothing for
    the block to feed. A mixed run still requires it everywhere: a target_run
    without a block does not say which provider it belongs to, so it cannot be
    matched to the provider that would have excused it.
    """
    modes = execution_access_modes or {}
    if modes and not any(
        get_provider_adapter(provider).resolves_execution_identity(mode)
        for provider, mode in modes.items()
    ):
        return
    stages_without_execution = sorted(
        target_run_id for target_run_id, target_run in active_target_runs.items()
        if target_run.get("execution_identities") is None
    )
    if stages_without_execution:
        raise RuntimeError(
            "❌ selected target_runs have no execution_identity block: "
            + ", ".join(stages_without_execution)
            + "; declare it, or run every provider in a mode that resolves no "
            "execution identity (see `ctl.py providers`)"
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
        "--older-than",
        default=None,
        metavar="ISO-DATE|any",
        help="forget only: age filter; `any` ignores age. Always required — a "
        "forget states both of its dimensions",
    )
    parser.add_argument(
        "--address",
        dest="forget_address",
        default=None,
        type=parse_comma_list,
        metavar="ADDRESS[,ADDRESS...]|all",
        help="forget only: what to forget; depth decides scope, so a template "
        "address takes every instance under it. `all` is the explicit wide value",
    )
    parser.add_argument(
        "--apply",
        dest="apply_forget",
        action="store_true",
        help="forget only: actually remove. Without it, a forget is a dry run "
        "that lists what would go",
    )
    parser.add_argument(
        "--accept-orphaned-resources",
        action="store_true",
        help="forget only: forget records whose state is `provisioned` or "
        "`partial`. The infrastructure outlives the record naming it, so what "
        "is accepted is orphaned resources — that is what the flag says",
    )
    parser.add_argument(
        "--accept-forget-everything",
        action="store_true",
        help="forget only: required when BOTH filters are wide, so forgetting "
        "everything cannot be reached by widening one flag at a time",
    )
    parser.add_argument(
        "--scope",
        dest="unlock_scope",
        default=None,
        choices=("local", "remote", "both"),
        help="unlock-ctl-state only: which lock to release. `local` is this "
        "machine's working tree, `remote` is the namespace, `both` (the default) "
        "is the remote lock and THIS machine's local one",
    )
    parser.add_argument(
        "--lock-id",
        help="the run ID holding the lock, as named in the error",
    )
    parser.add_argument(
        "--target",
        help="declared target to operate on",
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
        help="history-prune and forget: also take retained workflow runs that reference the selected records, which would otherwise be left naming a member that no longer exists",
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


def _add_procedure_args(parser: argparse._ActionsContainer) -> None:
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
        "--domain",
        required=True,
        dest="domain",
        help="plt domain a synthetic target reads (it takes the whole domain)",
    )
    parser.add_argument(
        "--procedure",
        required=True,
        dest="procedure",
        help="repo-local procedure to run",
    )
    # §Phase 53: a synthetic target declares the same execution axes a declared
    # target does — provider, account and the role to assume. All three or none.
    parser.add_argument(
        "--execution-provider",
        dest="execution_provider",
        default=None,
        help="synthetic target: provider whose adapter runs it",
    )
    parser.add_argument(
        "--execution-account",
        dest="execution_account",
        default=None,
        help="synthetic target: account to run in",
    )
    parser.add_argument(
        "--execution-role",
        dest="execution_role",
        default=None,
        help="synthetic target: provider role key to assume",
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
    # Execution access is a PER-PROVIDER decision (§Phase 53): a run that spans
    # providers may need normal access to one and escalated access to another,
    # and the mode NAMES belong to the adapters — the engine owns no mode
    # vocabulary of its own. Hence a provider=mode map, required and complete:
    # the operator states intent for every provider the run declares.
    execution_group.add_argument(
        "--execution-access-mode",
        required=True,
        dest="execution_access_modes",
        action=ExecutionAccessModesAction,
        metavar="PROVIDER=MODE[,PROVIDER=MODE...]",
        help="Execution access mode per provider, comma-separated and/or repeatable "
        "(required, no default). Must name every provider given to --providers, and "
        "each mode must be one the adapter advertises — run `ctl.py providers` to see "
        "what each supports, and what provider options a mode needs "
        "(passed with --provider-options).",
    )
    # execution runtime (§Phase 26): WHERE CTL produces each target_run's clean box.
    execution_group.add_argument(
        "--execution-runtime-mode",
        choices=EXECUTION_RUNTIME_MODES,
        required=True,
        help="Execution runtime (required, no default): 'local' builds a fresh Docker "
        "box per target_run on this machine; 'ci' runs each target_run on the CI "
        "runner (no Docker-in-Docker). Must be allowed by the ctl profile "
        "(allowed_execution_runtime_modes) and supported by every active target_run "
        "(step.yaml runtime.supported_execution_runtime_modes).",
    )
    # 3) execution params
    execution_group.add_argument(
        "--credential-refresh-mode",
        dest="credential_refresh_modes",
        action=ExecutionParamsAction,
        required=True,
        default=[],
        metavar="PROVIDER=MODE[,PROVIDER=MODE...]",
        help="when credentials are acquired, PER PROVIDER: `per_target` once "
        "while the target repo is prepared, or `per_step` freshly before every "
        "step. Required, no default — when a run acquires credentials is a "
        "choice, not something to inherit. Per provider because only some have "
        "expiring sessions at all, and a run may span several. Each mode must be "
        "one the provider's policy block allows",
    )
    execution_group.add_argument(
        "--execution-params",
        dest="execution_param",
        action=ExecutionParamsAction,
        default=[],
        metavar="KEY=VALUE[,KEY=VALUE...]",
        help="Execution params in key=value form; comma-separated and/or repeatable; "
        "lands in execution_context.params.*",
    )
    execution_group.add_argument(
        "--providers",
        dest="providers",
        required=True,
        type=parse_comma_list,
        metavar="NAME[,NAME...]",
        help="Providers participating in this run; every selected target's provider "
        "must be one of these. Lands in execution_context.ctl.providers",
    )
    execution_group.add_argument(
        "--provider-options",
        dest="provider_options",
        action=ProviderOptionsAction,
        default={},
        metavar="PROVIDER.KEY=VALUE[,...]",
        help="Provider-namespaced options, comma-separated and/or repeatable. The "
        "engine only routes them; each provider owns its option vocabulary",
    )
    # 4) what to do — spelled for the run type (§Phase 73)
    if run_type in ("workflow", "fan_out"):
        # A workflow is invoked with an OPERATION: it says what was asked for,
        # while each member target carries the ACTION it performs. The two differ
        # only here, because a workflow is the one level that may mix directions.
        # A fan-out shares the spelling because it expands workflows and passes the
        # operation through unchanged — it varies the address, never the verb.
        selector_group.add_argument(
            "--operation",
            dest="action",
            required=True,
            choices=list(KNOWN_ACTIONS),
            help="What to do to this thing. Each member target performs its own\n"
            "declared action, or inherits this one when it declares none",
        )
    else:
        selector_group.add_argument(
            "--action",
            required=True,
            choices=list(KNOWN_ACTIONS),
            help="Lifecycle action. `maintenance` operates on a target WITHOUT\n"
            "provisioning, planning or destroying it — releasing a tool's state\n"
            "lock is the case it exists for",
        )
    # 5) run-type selector (--workflow / --fan-out / --target / ... and its
    #    run-specific siblings)
    if run_type == "workflow":
        _add_workflow_args(selector_group)
    elif run_type == "target":
        _add_target_args(selector_group)
    elif run_type == "maintenance":
        _add_maintenance_args(selector_group)
    elif run_type == "procedure":
        _add_procedure_args(selector_group)
    elif run_type == "fan_out":
        _add_fan_out_args(selector_group)
    else:
        raise RuntimeError(f"❌ unknown runner run_type {run_type!r}")
    # --skip-up-to-date parametrizes the actual run (reuse committed children vs
    # re-run), so it lives in the run group — not with the non-executing checks.
    if run_type in {"workflow", "fan_out"}:
        selector_group.add_argument(
            "--skip-up-to-date",
            default=None,
            type=str2bool,
            metavar="{true,false}",
            help="Explicit true/false (no default; required for a normal run, "
            "omit only with --status or --execution-identity-preflight-check-only): "
            "when true, reuse a workflow child's committed result (skip re-running "
            "it) only when its committed target instance is current, commit-pinned, "
            "clean, and matches the current source/cfg commits and effective target definition/cfg view; when false, always re-run",
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
        "--skip-children-precheck",
        action="store_true",
        dest="skip_children_precheck",
        help="Do not pre-check this run's CHILDREN before starting them: a fan-out "
        "skips pre-checking its workflows, a workflow skips pre-checking its targets. "
        "Each child still renders and validates its own cfg when it runs, so this "
        "changes WHEN a bad child is found, not whether it is. A target run has no "
        "children, so it is a no-op there. Requires profile "
        "allow_skip_children_precheck",
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
            dest="force_skip_execution_identity_preflight_check",
            default=[],
            type=parse_comma_list,
            metavar="PROVIDER[,PROVIDER...]",
            help="Providers whose live execution-identity check to skip (a subset of "
            "--providers; the identity is still RESOLVED for all of them). Per provider "
            "because only some adapters have a live check at all, and a mixed run may "
            "need to skip one provider's probe while keeping another's. You accept the "
            "risk that a skipped provider's target fails MID-RUN on an execution "
            "identity the preflight would have caught up front; requires ctl-profile "
            "authorization.",
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
    # §Phase 61(d): a workflow-spawned child target run executes under the lock its
    # parent already holds. Internal wiring, not an operator flag.
    parser.add_argument(
        "--parent-workflow-run-id",
        dest="parent_workflow_run_id",
        default=None,
        help=argparse.SUPPRESS,
    )
    # §Phase 78: the parent's INSTANCE address, not just its run id. A spawned
    # child cannot derive it — it knows neither the workflow key nor the axes the
    # workflow partitions by — so the parent hands it over like every other fact
    # the child cannot compute. The in-process path already recorded it.
    parser.add_argument(
        "--parent-workflow-instance-address",
        dest="parent_workflow_instance_address",
        default=None,
        help=argparse.SUPPRESS,
    )


def redact_command_argv(argv: list[str]) -> list[str]:
    """Redact opaque credential selectors before command lines reach logs.

    Provider options are the one place a credential selector can be typed, and
    the engine cannot tell which of an adapter's keys is sensitive — so the
    VALUE of every provider option is redacted, whatever the key.
    """
    redacted: list[str] = []
    hide_next = False
    for value in argv:
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        if value == "--provider-options":
            redacted.append(value)
            hide_next = True
            continue
        if value.startswith("--provider-options="):
            redacted.append("--provider-options=<redacted>")
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


KNOWN_ACTIONS = ("provision", "plan", "destroy", "readonly", "maintenance")


def entry_actions(entry: dict, *, label: str) -> list[str]:
    """§Phase 33: the REQUIRED allowlist on a target/workflow. A missing or empty
    list is a hard error — availability is default-CLOSED and must be explicit,
    never inferred from an optional selector.

    §Phase 73: a WORKFLOW spells it `operations:`, because a workflow is invoked
    with an operation and its members carry the actions. A target keeps `actions:`.
    Exactly one spelling per entry — declaring both is a contradiction about which
    the caller supplies.
    """
    actions = entry.get("actions")
    operations = entry.get("operations")
    if actions is not None and operations is not None:
        raise RuntimeError(
            f"❌ {label} declares both 'actions' and 'operations'; a workflow "
            "declares operations, a target declares actions"
        )
    if actions is None:
        actions = operations
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
    validate_workflow_actions_declared(workflows)
    resolved_workflows: dict = {}
    for name, wf in workflows.items():
        if not isinstance(wf, dict):
            raise RuntimeError(f"❌ workflow {name!r} must be a mapping")
        # §Phase 73: a workflow declares no allowlist. Its member selectors decide
        # when it applies, and each member's declared action decides what runs —
        # an `operations:` list restated the selectors and could contradict them.
        target_keys = wf.get("target_keys")
        if isinstance(target_keys, dict):
            member = resolve_list_member(
                target_keys,
                execution_context,
                value_field="target_keys",
                label=f"workflow {name!r} target_keys",
                extra_fields=("default_action",),
            )
            if member is None:
                raise RuntimeError(
                    f"❌ workflow {name!r} members-shaped target_keys did not "
                    "resolve for this execution context"
                )
            # §Phase 73: the member's declared default reaches every entry it
            # carries, so a list going one direction states its verb once.
            wf = {
                **wf,
                "target_keys": list(member["target_keys"]),
                "default_action": resolve_default_action(
                    member.get("default_action"), execution_context,
                    label=f"workflow {name!r} member",
                ),
                "member_selectors": member.get("selectors"),
            }
        else:
            wf = {
                **wf,
                "default_action": resolve_default_action(
                    wf.get("default_action"), execution_context,
                    label=f"workflow {name!r}",
                ),
            }
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
    resolved = resolved_workflows[workflow_name]
    cfg = {
        "meta": {"name": f"{inventory_name}/{workflow_name}", "action": inventory_name},
        "target_runs": target_runs,
    }
    # §Phase 73: the matched member's declared default and its selector block travel
    # with the resolved workflow — the run record carries them, and returning only
    # meta + target_runs silently dropped both. §Phase 78 adds the instance params
    # for the same reason: a field the caller needs must survive resolution.
    for field in ("default_action", "member_selectors", "workflow_instance_params"):
        if resolved.get(field):
            cfg[field] = resolved[field]
    return cfg


def load_optional_yaml_mapping(path: Path) -> dict:
    """Load an optional YAML mapping, returning {} when the file is absent."""
    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML file must contain a mapping: {path}")
    return data


def workflow_target_run_signature(target_run_entry) -> tuple[str, str | None]:
    """Return `(target key, action)` for one workflow target_run entry.

    A bare string is a key with no declared action; a mapping carries both. The
    pair is the entry's IDENTITY: §Phase 73 lets one key repeat when the actions
    differ, so neither half alone distinguishes two entries.
    """
    if isinstance(target_run_entry, str):
        return target_run_entry, None
    if isinstance(target_run_entry, dict):
        return target_run_entry.get("target"), target_run_entry.get("action")
    raise RuntimeError(f"❌ invalid workflow target_run entry: {target_run_entry!r}")


def workflow_target_run_key(target_run_entry) -> str | None:
    """Return the TARGET key an entry addresses.

    Distinct from `get_workflow_target_run_id`: `id` is the run's display
    identity, which the target-run builder may set independently of the target it
    points at. A placement anchor names a target, so it resolves against this.
    """
    return workflow_target_run_signature(target_run_entry)[0]


def find_workflow_anchor_index(target_runs: list, anchor: str, *, label: str) -> int | None:
    """Locate the single entry an anchor target key names; None when absent.

    Ambiguity is REFUSED rather than resolved by position. Since §Phase 73 a key
    may appear twice with different actions, and picking the first would place a
    variant before or after an arbitrary one of them — a silent choice about
    ordering, which is exactly what a placement declares.
    """
    matches = [
        index
        for index, entry in enumerate(target_runs)
        if workflow_target_run_key(entry) == anchor
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise RuntimeError(
            f"❌ {label} anchor {anchor!r} matches {len(matches)} entries; a "
            "placement cannot choose between them. Qualify the anchor or make "
            "the key unique in this workflow"
        )
    return matches[0]


def get_workflow_target_run_id(target_run_entry) -> str:
    """Return the target_run id from a workflow target_run entry."""
    if isinstance(target_run_entry, str):
        return target_run_entry
    if isinstance(target_run_entry, dict):
        target_run_id = target_run_entry.get("id")
        if isinstance(target_run_id, str) and target_run_id:
            return target_run_id
    raise RuntimeError(f"❌ invalid workflow target_run entry: {target_run_entry!r}")


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


def variant_anchor(variant: dict, *, label: str) -> tuple[str, bool]:
    """Return `(anchor target key, insert_before)` for one placement.

    The two anchor fields are one choice with two spellings, so exactly one is
    required and both together is a contradiction rather than a precedence rule.
    """
    before, after = variant.get("before_target_key"), variant.get("after_target_key")
    if before and after:
        raise RuntimeError(
            f"❌ {label} cannot set both 'before_target_key' and 'after_target_key'"
        )
    if not (before or after):
        raise RuntimeError(
            f"❌ {label} must define 'after_target_key' or 'before_target_key'"
        )
    return (before, True) if before else (after, False)


def variant_target_run_entry(
    variant: dict, target_name: str, workflow_cfg: dict, *, label: str
) -> dict:
    """Build the target_run entry a placement inserts.

    §Phase 73 requires every entry to carry a DECLARED action, so a placement
    resolves one the same way a member list does: its own `action:`, else the
    workflow's resolved default. Inserting a bare key would slip past that gate,
    because normalization has already run by the time a variant applies.
    """
    action = variant.get("action") or workflow_cfg.get("default_action")
    if not action:
        raise RuntimeError(
            f"❌ {label} inserts {target_name!r} with no action: the workflow "
            "declares no `default_action`, so the placement must declare `action:`"
        )
    if action not in RUN_ACTIONS:
        raise RuntimeError(
            f"❌ {label} declares action {action!r}; expected one of {sorted(RUN_ACTIONS)}"
        )
    return {"id": target_name, "target": target_name, "action": action}


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

        anchor, before = variant_anchor(v, label=f"variant {name!r}")
        index = find_workflow_anchor_index(
            target_runs, anchor, label=f"variant {name!r}"
        )
        if index is None:
            logging.info("Variant '%s' anchor '%s' absent from '%s' — skipped", name, anchor, workflow_name)
            continue

        entry = variant_target_run_entry(
            v, target_name, workflow_cfg, label=f"variant {name!r}"
        )
        signature = workflow_target_run_signature(entry)
        if signature in {workflow_target_run_signature(e) for e in target_runs}:
            raise RuntimeError(
                f"❌ variant {name!r} inserts duplicate target {target_name!r}"
                + (f" (action {signature[1]})" if signature[1] else "")
            )
        target_runs.insert(index if before else index + 1, entry)
        logging.info(
            "Applied variant '%s': inserted '%s' (action %s) %s '%s'",
            name, target_name, signature[1], "before" if before else "after", anchor,
        )

    patched = dict(workflow_cfg)
    patched["target_runs"] = target_runs
    return patched


def resolve_input_params(
    input_params: object,
    input_param_set_keys: object,
    param_sets: dict,
    *,
    label: str,
    cfg_path: Path,
    _stack: tuple = (),
) -> list[str]:
    """§Phase 61(a): resolve a target's declared INPUT PARAMS.

    Set references and literal param names are separate fields, the same split
    `cfg_key_sets`/`cfg_keys` uses: one is a ctl catalog lookup, the other a param
    name, so they cannot collide. Nearly every target needs the same naming and
    placement axes, which is what the sets exist for.
    """
    resolved: list[str] = []
    if input_param_set_keys is not None:
        if not isinstance(input_param_set_keys, list) or not input_param_set_keys:
            raise RuntimeError(f"❌ {label} input_param_sets must be a non-empty list ({cfg_path})")
        for name in input_param_set_keys:
            if not isinstance(name, str) or not name.strip():
                raise RuntimeError(f"❌ {label} input_param_sets entries must be non-empty strings")
            name = name.strip()
            if name not in param_sets:
                available = ", ".join(sorted(param_sets)) or "none"
                raise RuntimeError(
                    f"❌ {label}: undefined param_set {name!r}; declared: {available} ({cfg_path})"
                )
            if name in _stack:
                raise RuntimeError(f"❌ param_set cycle: {' -> '.join([*_stack, name])} ({cfg_path})")
            member = param_sets[name]
            if not isinstance(member, dict):
                raise RuntimeError(f"❌ param_set {name!r} must be a mapping ({cfg_path})")
            unknown = sorted(set(member) - {"input_param_sets", "input_params"})
            if unknown:
                raise RuntimeError(f"❌ param_set {name!r} has unsupported keys {unknown} ({cfg_path})")
            resolved.extend(
                resolve_input_params(
                    member.get("input_params"), member.get("input_param_sets"), param_sets,
                    label=f"param_set {name!r}", cfg_path=cfg_path, _stack=(*_stack, name),
                )
            )
    if input_params is not None:
        if not isinstance(input_params, list) or not input_params:
            raise RuntimeError(f"❌ {label} input_params must be a non-empty list ({cfg_path})")
        for entry in input_params:
            if not isinstance(entry, str) or not CONTEXT_KEY_RE.fullmatch(entry.strip()):
                raise RuntimeError(f"❌ {label} input_params entry {entry!r} must be a param name")
            resolved.append(entry.strip())
    seen: set[str] = set()
    return [e for e in resolved if not (e in seen or seen.add(e))]


CFG_KEY_ENTRY_RE = re.compile(r"^[A-Za-z_*?\[][A-Za-z0-9_.*?\[\]-]*$")


def resolve_cfg_key_entries(
    cfg_keys: object,
    cfg_key_set_keys: object,
    cfg_key_sets: dict,
    *,
    label: str,
    cfg_path: Path,
    _stack: tuple = (),
) -> list[str]:
    """§Phase 60: resolve one consumer's contract into concrete key SELECTORS.

    Set REFERENCES and content KEYS are separate fields, exactly as the
    predecessor `cfg_file_sets` split `cfg_file_set_keys` from `cfg_files`. They
    are different things to the engine — one is a ctl catalog lookup, the other a
    plt content address — so they cannot collide, and a plt key is free to be
    named like a set.

    Referenced sets are spliced FIRST (they carry the axis/naming prefix), then
    the consumer's own keys. A key is an exact name, a dotted sub-path, or a glob
    over top-level names.
    """
    resolved: list[str] = []
    if cfg_key_set_keys is not None:
        if not isinstance(cfg_key_set_keys, list) or not cfg_key_set_keys:
            raise RuntimeError(f"❌ {label} cfg_key_sets must be a non-empty list ({cfg_path})")
        for name in cfg_key_set_keys:
            if not isinstance(name, str) or not name.strip():
                raise RuntimeError(f"❌ {label} cfg_key_sets entries must be non-empty strings ({cfg_path})")
            name = name.strip()
            if name not in cfg_key_sets:
                available = ", ".join(sorted(cfg_key_sets)) or "none"
                raise RuntimeError(
                    f"❌ {label}: undefined cfg_key_set {name!r}; declared: {available} ({cfg_path})"
                )
            if name in _stack:
                cycle = " -> ".join([*_stack, name])
                raise RuntimeError(f"❌ cfg_key_set cycle: {cycle} ({cfg_path})")
            member = cfg_key_sets[name]
            if not isinstance(member, dict):
                raise RuntimeError(
                    f"❌ cfg_key_set {name!r} must be a mapping of "
                    f"cfg_key_sets / cfg_keys ({cfg_path})"
                )
            unknown = sorted(set(member) - {"cfg_key_sets", "cfg_keys"})
            if unknown:
                raise RuntimeError(f"❌ cfg_key_set {name!r} has unsupported keys {unknown} ({cfg_path})")
            resolved.extend(
                resolve_cfg_key_entries(
                    member.get("cfg_keys"), member.get("cfg_key_sets"), cfg_key_sets,
                    label=f"cfg_key_set {name!r}", cfg_path=cfg_path, _stack=(*_stack, name),
                )
            )
    if cfg_keys is not None:
        if not isinstance(cfg_keys, list) or not cfg_keys:
            raise RuntimeError(f"❌ {label} cfg_keys must be a non-empty list ({cfg_path})")
        for entry in cfg_keys:
            if not isinstance(entry, str) or not entry.strip():
                raise RuntimeError(f"❌ {label} cfg_keys entries must be non-empty strings ({cfg_path})")
            entry = entry.strip()
            if not CFG_KEY_ENTRY_RE.fullmatch(entry):
                raise RuntimeError(
                    f"❌ {label}: {entry!r} is not a legal cfg key selector "
                    f"(key, key.path, or glob) ({cfg_path})"
                )
            resolved.append(entry)
    if not resolved:
        raise RuntimeError(f"❌ {label} declares neither cfg_key_sets nor cfg_keys ({cfg_path})")
    seen: set[str] = set()
    return [e for e in resolved if not (e in seen or seen.add(e))]


def project_cfg_keys(doc: dict, entries: list[str], *, label: str) -> dict:
    """Project the declared key selectors out of one domain's merged cfg doc.

    Assertion 1 (every declared key resolves in its domain) and assertion 5 (a
    glob matching nothing is a stale declaration) are enforced here — an entry
    that selects nothing is an ERROR, never a silent empty view.
    """
    projected: dict = {}
    for entry in entries:
        if entry == "*":
            matched = list(doc)
        elif any(ch in entry for ch in "*?["):
            matched = sorted(k for k in doc if fnmatch.fnmatchcase(k, entry))
        else:
            head, _, path = entry.partition(".")
            if head not in doc:
                raise RuntimeError(
                    f"❌ {label}: cfg key {entry!r} does not resolve in this domain; "
                    f"available: {', '.join(sorted(doc)) or 'none'}"
                )
            if not path:
                matched = [head]
            else:
                node = doc[head]
                for segment in path.split("."):
                    if not isinstance(node, dict) or segment not in node:
                        raise RuntimeError(
                            f"❌ {label}: cfg key path {entry!r} does not resolve in this domain"
                        )
                    node = node[segment]
                branch = projected.setdefault(head, {})
                if not isinstance(branch, dict):
                    raise RuntimeError(
                        f"❌ {label}: cfg key path {entry!r} conflicts with whole key {head!r}"
                    )
                cursor = branch
                segments = path.split(".")
                for segment in segments[:-1]:
                    cursor = cursor.setdefault(segment, {})
                cursor[segments[-1]] = node
                continue
        if not matched:
            raise RuntimeError(
                f"❌ {label}: cfg key selector {entry!r} matched no key in this domain "
                "(stale declaration)"
            )
        for key in matched:
            projected[key] = doc[key]
    return projected


def load_inventory_cfg(
    ctl_cfg_root: Path,
    inventory_name: str,
    execution_context: dict[str, object] | None = None,
    member_actions: set[str] | None = None,
) -> dict:
    """Compose action cfg from target_sources + cfg_key_sets + targets/<action>/*.yaml.

    `inventory_name` is the action (provision/plan/destroy/readonly). Layout:
      - target_sources.yaml  source repos: source key -> meta
      - cfg_key_sets.yaml        named CONTENT-KEY bundles: set key -> [key selectors]
      - targets/<action>/*.yaml  fat targets (the directory IS the action). Each
            file is a flat `targets:` map; all files for an action merge (duplicate
            names rejected). A target is self-contained:
              {source_key, ref_key, procedure_key, domains, cfg_keys,
               [execution], [selectors],
               [required_plt_overlay_keys]}.

    Returns the flat shape build_active_target_runs consumes ({target_sources,
    targets}), where each target carries source + domains + cfg_keys
    (its declared domains + per-domain cfg_keys) + procedure + execution identity requirement
    (+ selectors /
    requires_plt_overlays when present).
    """
    # global resources + targets are content-key (collected by top-level key)
    target_sources = collect_resource(ctl_cfg_root, "target_sources")
    cfg_key_sets = collect_resource(ctl_cfg_root, "cfg_key_sets")
    param_sets = collect_resource(ctl_cfg_root, "param_sets")
    cfg_key_sets_path = ctl_cfg_root  # label for include/error messages
    domain_registry = load_domain_registry(ctl_cfg_root)
    if not target_sources:
        raise RuntimeError(f"❌ no 'target_sources' defined under: {ctl_cfg_root}")
    if not domain_registry:
        raise RuntimeError(f"❌ no 'domains' registry defined under: {ctl_cfg_root}")

    # §Phase 33: targets are declared ONCE (no action level); each declares a
    # REQUIRED `actions:` allowlist (default-closed) and the inventory for a run
    # is the subset allowing this action.
    all_targets = collect_resource(ctl_cfg_root, "targets", entry_depth=1)
    targets = {}
    # §Phase 73: a workflow member may declare its own action, so the inventory
    # admits a target that allows the INVOKED action or any action a member asked
    # of it. Without this a member naming `destroy` on a destroy-only target could
    # not resolve inside a provision workflow, which is the whole point of letting
    # a composition mix directions.
    admitted = {inventory_name, *(member_actions or ())}
    for target_name, target_def in all_targets.items():
        if not isinstance(target_def, dict):
            raise RuntimeError(f"❌ target {target_name!r} must be a mapping")
        if admitted & set(entry_actions(target_def, label=f"target {target_name!r}")):
            targets[target_name] = target_def
    if not targets:
        raise RuntimeError(f"❌ no targets allow action {inventory_name!r}")

    validate_distinct_target_signatures(all_targets)

    resolved_targets: dict = {}
    for target_name, target_def in targets.items():
        consumed_group_axes: set[str] = set()

        source = target_def.get("source_key")
        if not isinstance(source, str) or not source:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'source_key'")

        target_ref = target_def.get("ref_key")
        if not isinstance(target_ref, str) or not target_ref.strip():
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'ref_key'")

        # §Phase 60: the target DECLARES the domains it reads and, per domain, the
        # content keys it consumes. `domains` is identity (which namespaces this
        # target subscribes to); `cfg_keys` is the contract (what it takes out of
        # them). Neither names a plt FILE, so plt owns its own layout.
        declared_domains = target_def.get("domains")
        if not isinstance(declared_domains, list) or not declared_domains:
            raise RuntimeError(
                f"❌ target {target_name!r} must define a non-empty 'domains' list"
            )
        # A domain-GENERIC target (the state-backend one) takes its domain from the
        # execution context. If that axis is not bound — a generic target in a
        # shared inventory this run does not activate — resolution is deferred
        # (None) rather than failing an unrelated run.
        domains: list[str] | None = []
        for raw_domain in declared_domains:
            if not isinstance(raw_domain, str) or not raw_domain.strip():
                raise RuntimeError(
                    f"❌ target {target_name!r} domains entries must be non-empty strings"
                )
            raw_domain = raw_domain.strip()
            if "${" not in raw_domain:
                domains.append(raw_domain)
                continue
            if execution_context is None:
                domains = None
                break
            consumed_group_axes.update(
                ref[len(f"{EXECUTION_CONTEXT_ROOT}.params."):]
                for ref in RUNTIME_SCALAR_TOKEN_RE.findall(raw_domain)
                if ref.startswith(f"{EXECUTION_CONTEXT_ROOT}.params.")
            )
            resolved_domain = resolve_runtime_scalar(
                raw_domain, execution_context,
                label=f"target {target_name!r} domains entry",
                tolerate_missing=True,
            )
            if resolved_domain is None:
                domains = None
                break
            domains.append(str(resolved_domain))
        if domains is not None:
            if len(set(domains)) != len(domains):
                raise RuntimeError(f"❌ target {target_name!r} lists a domain twice: {domains}")
            for domain in domains:
                validate_domain_value(
                    domain_registry, domain, label=f"target {target_name!r} domains"
                )

        raw_cfg_keys = target_def.get("cfg_keys") or {}
        raw_cfg_key_sets = target_def.get("cfg_key_sets") or {}
        for field, value in (("cfg_keys", raw_cfg_keys), ("cfg_key_sets", raw_cfg_key_sets)):
            if not isinstance(value, dict):
                raise RuntimeError(f"❌ target {target_name!r} {field} must be a map (domain -> list)")
        if not raw_cfg_keys and not raw_cfg_key_sets:
            raise RuntimeError(
                f"❌ target {target_name!r} must define cfg_key_sets and/or cfg_keys "
                "(domain -> what it consumes)"
            )
        cfg_keys: dict[str, list[str]] | None = None
        if domains is not None:
            def _domain_key(raw):
                key = str(raw).strip()
                if "${" in key:
                    key = str(resolve_runtime_scalar(
                        key, execution_context,
                        label=f"target {target_name!r} cfg_keys domain"))
                return key
            per_domain: dict[str, dict] = {}
            for raw_key, entries in raw_cfg_key_sets.items():
                per_domain.setdefault(_domain_key(raw_key), {})["cfg_key_sets"] = entries
            for raw_key, entries in raw_cfg_keys.items():
                per_domain.setdefault(_domain_key(raw_key), {})["cfg_keys"] = entries
            cfg_keys = {
                domain_key: resolve_cfg_key_entries(
                    spec.get("cfg_keys"), spec.get("cfg_key_sets"), cfg_key_sets,
                    label=f"target {target_name!r} [{domain_key}]",
                    cfg_path=cfg_key_sets_path,
                )
                for domain_key, spec in per_domain.items()
            }
            # assertion 2: cfg_keys for a domain this target does not read
            extra = sorted(set(cfg_keys) - set(domains))
            if extra:
                raise RuntimeError(
                    f"❌ target {target_name!r} declares cfg_keys for {extra}, "
                    f"which are not in its domains {domains}"
                )
            # assertion 3: a declared domain this target takes nothing from
            missing = sorted(set(domains) - set(cfg_keys))
            if missing:
                raise RuntimeError(
                    f"❌ target {target_name!r} declares domains {missing} with no "
                    "cfg_keys entry — a subscription that consumes nothing"
                )

        procedure = target_def.get("procedure_key")
        if not isinstance(procedure, str) or not procedure:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'procedure_key'")

        # §Phase 53: the target declares its execution axes inline. The old
        # `execution_identity_key` (a bundle of account + role + credential,
        # dispatched through named identity groups) is gone: per-action variation
        # is now `execution.roles`, keyed by authorization class.
        if "execution_identity_key" in target_def:
            raise RuntimeError(
                f"❌ target {target_name!r} uses `execution_identity_key`, which is removed; "
                "declare an `execution_identity:` block (provider, account, roles) instead"
            )
        target_execution_identity = target_def.get("execution_identities")
        target_providers = None
        if target_execution_identity is not None:
            target_execution_identity = validate_target_execution_identities(
                target_execution_identity, label=f"target {target_name!r}"
            )
            target_providers = validate_target_providers(
                target_def.get("providers"),
                target_execution_identity,
                label=f"target {target_name!r}",
            )

        # §Phase 61(a): a target declares the COORDINATES it consumes and the
        # CONSTANTS it always uses. The two must not intersect — a name is either
        # a coordinate or a constant, never both.
        declared_input_params = resolve_input_params(
            target_def.get("input_params"),
            target_def.get("input_param_sets"),
            param_sets,
            label=f"target {target_name!r}",
            cfg_path=cfg_key_sets_path,
        )
        static_vars = target_def.get("static_vars") or {}
        if not isinstance(static_vars, dict):
            raise RuntimeError(f"❌ target {target_name!r} static_vars must be a map")
        for var_name, var_value in static_vars.items():
            if not isinstance(var_name, str) or not CONTEXT_KEY_RE.fullmatch(var_name):
                raise RuntimeError(
                    f"❌ target {target_name!r} static_vars key {var_name!r} must be an identifier"
                )
            if isinstance(var_value, (dict, list)):
                raise RuntimeError(
                    f"❌ target {target_name!r} static_vars.{var_name} must be a literal scalar; "
                    "a selector-dependent constant varies per instance, which makes it a coordinate"
                )
        overlap = sorted(set(declared_input_params) & set(static_vars))
        if overlap:
            raise RuntimeError(
                f"❌ target {target_name!r} declares {overlap} as BOTH an input param and a "
                "static var; a name is either a coordinate or a constant"
            )

        resolved = {
            "source": source,
            "ref": target_ref.strip(),
            "procedure": procedure,
            "domains": domains,
            "cfg_keys": cfg_keys,
            "input_params": declared_input_params,
            "static_vars": dict(static_vars),
            # §Phase 73: the allowlist is carried onto the inventory entry, not
            # consumed by the filter that built it. A workflow member may name an
            # action, and the check that it is permitted needs the list at the
            # point the member is resolved.
            "allowed_actions": entry_actions(target_def, label=f"target {target_name!r}"),
        }
        if domains is None:
            # Domain-generic target whose domain axis is not bound in this run.
            # Record the AXIS NAMES it needs, never the raw `${...}` template:
            # the ctl cfg snapshot resolves every scalar it walks, so an
            # unresolved placeholder stored here would fail the whole run.
            resolved["domains_unresolved"] = sorted(
                {
                    ref
                    for raw in declared_domains
                    for ref in RUNTIME_SCALAR_TOKEN_RE.findall(str(raw))
                }
            )
        # §Phase 31: declared instance identity flows through to the resolved
        # target (consumed by resolve_run_instance_identity).
        # §Phase 32: a GENERIC target whose instance axes vary by another axis
        # dispatches its schema by `members` ({params: [...], selectors: {...}}),
        # the same pattern as its ref groups. Exactly one member
        # matches; an unbound dispatch axis defers (hard error only if the
        # target is actually activated in a run).
        instance_params = target_def.get("target_instance_params")
        # §Phase 61(a): every coordinate must be a declared input — checked on ALL
        # members branches, not just the one this run selects, so an unreachable
        # branch cannot hide a typo until some later execution context picks it.
        for branch in (
            [m.get("params") for m in (instance_params.get("members") or [])]
            if isinstance(instance_params, dict)
            else [instance_params]
        ):
            if not isinstance(branch, list):
                continue
            undeclared = sorted(
                {str(x).strip() for x in branch if isinstance(x, str)}
                - set(declared_input_params)
            )
            if undeclared:
                raise RuntimeError(
                    f"❌ target {target_name!r} target_instance_params {undeclared} are not "
                    f"declared input params (declared: {sorted(declared_input_params) or 'none'}); "
                    "a target cannot be identified by a coordinate it does not read"
                )
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
        if target_execution_identity is not None:
            resolved["execution_identities"] = target_execution_identity
            resolved["providers"] = target_providers
        if "provisions_ctl_state_bucket" in target_def:
            raise RuntimeError(
                f"❌ target {target_name!r} uses deprecated provisions_ctl_state_bucket; "
                "use provisions_ctl_state_backend"
            )
        if target_def.get("provisions_ctl_state_backend") is True:
            resolved["provisions_ctl_state_backend"] = True
        # per-target consent to a mode that requires it (§12); the adapter names
        # the field, the engine only carries it through. Default: not granted.
        for consent_field in target_consent_opt_in_fields():
            if consent_field in target_def:
                if not isinstance(target_def[consent_field], bool):
                    raise RuntimeError(f"❌ target {consent_field} must be a boolean")
                resolved[consent_field] = target_def[consent_field]
        for legacy_flag in (  # removed keys
            "allow_skip_ctl_entry",
            "allow_skip_ctl_state_sync",
            "skip_ctl_role_chain",  # removed
            "execution_access_mode",
        ):
            if legacy_flag in target_def:
                raise RuntimeError(
                    f"❌ target {target_name!r} uses removed {legacy_flag}; "
                    "use the execution_identity block's consent fields (§12) for access, "
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
            duplicate_overlay_keys = [
                key
                for key, count in collections.Counter(overlay_keys).items()
                if count > 1
            ]
            if duplicate_overlay_keys:
                raise RuntimeError(
                    f"❌ target {target_name!r} required_plt_overlay_keys must be unique; "
                    f"duplicates: {', '.join(sorted(duplicate_overlay_keys))}"
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
    elif run_type == "procedure":
        if getattr(args, "workflow", None) or getattr(args, "target", None):
            raise RuntimeError("❌ procedure runner does not accept --workflow or --target")
        ref = getattr(args, "ref", None)
        ref_context = resolve_ref_context(ref, args.execution_params) if ref else "procedure"
        raw_name = f"{ref_context_to_result_path(ref_context)}/{getattr(args, 'source', None) or 'unknown'}/{getattr(args, 'procedure', None) or 'unknown'}"
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
    parent_workflow_run_id: str | None = None,
    parent_workflow_instance_address: str | None = None,
    instance_segments: list[str] | None = None,
    instance_address: str | None = None,
    target_addresses: list[str] | None = None,
    identity_doc: dict | None = None,
    execution_access_modes: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Create run directories under the stable ctl result key and setup file logging.

    §Phase 31: results nest under the resolved ctl-state NAMESPACE tree
    (`_local` for stateless/synthetic runs), with the target/workflow instance
    layer between the key and `runs/`:
      <root>/<namespace>/<run_type>/<key>[/instances/<seg>...]/runs/<id>
    A parameterized instance writes its authoritative identity.yaml
    (manifest-first ordering, Q2) before any run content."""
    result_name = normalize_result_name(result_name, label="ctl result name")
    # §Phase 73: composed, never hand-assembled. Building `/ action / run_type /`
    # here is what kept every real run on the action-prefixed layout while the
    # readers had already moved — the two agreed with each other and with nothing.
    ctl_state_dir = Path(ctl_state_local_root).joinpath(
        *locator_segments
    ) / compose_state_relpath(run_type, result_name, list(instance_segments or []))
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
            # Degraded-mode audit: each provider's access mode is persisted
            # structurally (not only in the logged command) so an audit of
            # committed run records can tell which runs escalated, and where.
            **({"execution_access_modes": execution_access_modes}
               if execution_access_modes else {}),
            # §Phase 31: instance identity + namespace facts of this run.
            **({"ctl_state_namespace": locator_segments[0]}
               if locator_segments and locator_segments[0] != LOCAL_ONLY_LOCATOR[0] else {}),
            **({"instance": list(instance_segments)} if instance_segments else {}),
            **({"instance_address": instance_address} if instance_address else {}),
            **({"target_addresses": list(target_addresses)} if target_addresses else {}),
            # §Phase 31 item 8: the stateless fan-out's batch audit record —
            # "these runs were one invocation" lives only in child metadata.
            **({"fan_out_run_id": parent_fan_out_run_id} if parent_fan_out_run_id else {}),
            # §Phase 31 Q1b: a child spawned by a workflow records its parent, so the
            # namespace mutation lock can tell "my parent holds it" from contention.
            **({"parent_workflow_run_id": parent_workflow_run_id}
               if parent_workflow_run_id else {}),
            **({"parent_workflow_instance_address": parent_workflow_instance_address}
               if parent_workflow_instance_address else {}),
        },
    )

    logging.info(f"Using artifacts_dir: {artifacts_dir}")
    logging.info(f"Logging to: {log_file}")

    return run_dir, artifacts_dir, log_file


def setup_run_workspace(run_dir: Path) -> Path:
    """Materialize the target_run runtime and mutable cfg workspace after preflight."""
    step_utils_dir = materialize_step_utils(run_dir)
    logging.info("Using ctl target_run runtime: %s", step_utils_dir)

    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    cfg_dir.mkdir(parents=True)

    return cfg_dir


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
    parent_workflow_run_id: str | None = None,
    parent_workflow_instance_address: str | None = None,
    execution_access_modes: str | None = None,
) -> tuple[Path, Path, Path]:
    """Create a preflight result without target_run tooling or companion cfg."""
    result_name = normalize_result_name(result_name, label="ctl result name")
    # §Phase 73: composed, never hand-assembled. Building `/ action / run_type /`
    # here is what kept every real run on the action-prefixed layout while the
    # readers had already moved — the two agreed with each other and with nothing.
    ctl_state_dir = Path(ctl_state_local_root).joinpath(
        *locator_segments
    ) / compose_state_relpath(run_type, result_name, list(instance_segments or []))
    if instance_segments and identity_doc is not None:
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
            # Degraded-mode audit (see setup_run_dirs).
            **({"execution_access_modes": execution_access_modes}
               if execution_access_modes else {}),
            # §Phase 31: instance identity + namespace facts of this run.
            **({"ctl_state_namespace": locator_segments[0]}
               if locator_segments and locator_segments[0] != LOCAL_ONLY_LOCATOR[0] else {}),
            **({"instance": list(instance_segments)} if instance_segments else {}),
            **({"instance_address": instance_address} if instance_address else {}),
            **({"target_addresses": list(target_addresses)} if target_addresses else {}),
            **({"fan_out_run_id": parent_fan_out_run_id} if parent_fan_out_run_id else {}),
            # §Phase 31 Q1b: a child spawned by a workflow records its parent, so the
            # namespace mutation lock can tell "my parent holds it" from contention.
            **({"parent_workflow_run_id": parent_workflow_run_id}
               if parent_workflow_run_id else {}),
            **({"parent_workflow_instance_address": parent_workflow_instance_address}
               if parent_workflow_instance_address else {}),
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
    """Just the keys, in order — the shape the run inventory consumes.

    A key-only view, so it asks for no action: callers that need one go through
    `normalize_target_entries` with the list's declared default."""
    keys: list[str] = []
    for value in values if isinstance(values, list) else []:
        keys.append(normalize_result_name(
            value.get("key") if isinstance(value, dict) else value, label=label
        ))
    return keys


def resolve_default_action(
    declared: object, execution_context: dict[str, object] | None, *, label: str
) -> str | None:
    """A member's declared default action: a literal, or a context reference.

    `${execution_context.ctl.operation}` says "every member does whatever was
    invoked" — explicitly, in cfg, rather than by an unstated fallback. A literal
    says the list is one fixed direction whatever was asked.
    """
    if declared is None:
        return None
    if not isinstance(declared, str) or not declared.strip():
        raise RuntimeError(f"❌ {label} default_action must be a non-empty string")
    declared = declared.strip()
    if declared.startswith("${") and declared.endswith("}"):
        reference = declared[2:-1].strip()
        value = (execution_context or {}).get(reference)
        if value is None:
            raise RuntimeError(
                f"❌ {label} default_action references {reference!r}, which is not "
                "bound in this execution context"
            )
        return str(value)
    return declared


def normalize_target_entries(
    values: list, *, label: str, default_action: str | None = None
) -> list[tuple[str, str]]:
    """§Phase 73: a workflow member entry is a KEY or a key with its own ACTION.

        - key: env/ops/ecr
          action: provision

    The action belongs to the TARGET, never to the member list: a member is a list
    plus a selector deciding WHEN the list applies, so putting the action on the
    member would force one member per target purely to vary a verb.

    Every entry states its action; nothing inherits. A key MAY repeat with
    differing actions. Both entries address one instance, so
    its pointer is written twice in one run and the last write wins — correct,
    because after destroy-then-provision the instance is provisioned. Order is
    therefore load-bearing: with a repeated key it decides the final state, not
    merely the execution sequence.
    """
    if not isinstance(values, list):
        raise RuntimeError(f"❌ {label} must be a list")
    entries: list[tuple[str, str | None]] = []
    seen: set[tuple[str, str | None]] = set()
    for value in values:
        if isinstance(value, dict):
            unknown = sorted(set(value) - {"key", "action"})
            if unknown:
                raise RuntimeError(
                    f"❌ {label} entry has unsupported keys {unknown}; a member "
                    "entry is a key, or a key with its own action"
                )
            key = normalize_result_name(value.get("key"), label=label)
            action = value.get("action")
            if action is not None and (
                not isinstance(action, str) or action not in RUN_ACTIONS
            ):
                raise RuntimeError(
                    f"❌ {label} entry {key!r} declares action {action!r}; expected "
                    f"one of {sorted(RUN_ACTIONS)}"
                )
        else:
            key, action = normalize_result_name(value, label=label), None
        # §Phase 73: every entry ends up with a DECLARED action — its own, or the
        # one its member declares for the whole list. There is no fallback: without
        # an action the engine does not know how to run the target, and a silent
        # guess is the one thing a cfg gate exists to prevent.
        action = action or default_action
        if action is None:
            raise RuntimeError(
                f"❌ {label} entry {key!r} has no action, so it is not runnable. "
                "Declare `default_action:` for the list, or `action:` beneath the key"
            )
        if (key, action) in seen:
            raise RuntimeError(
                f"❌ duplicate target entry in {label}: {key}"
                + (f" (action {action})" if action else "")
            )
        seen.add((key, action))
        entries.append((key, action))
    return entries


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


def state_slot_dir(instance_dir: Path, state: str, group: str) -> Path:
    """`<state>/<group>/` — group-scoped for the same reason pointers are: a failed
    plan and a failed deployment on one instance must not collide."""
    return Path(instance_dir) / state / group


def remove_state_slot(run_dir: Path, state: str) -> None:
    group = action_group(str(load_run_metadata(run_dir).get("action")))
    slot_dir = state_slot_dir(ctl_state_dir_from_run_dir(run_dir), state, group)
    if slot_dir.exists():
        shutil.rmtree(slot_dir)


def run_workspace_dir(run_dir: Path) -> Path | None:
    """This run's build workspace root (§Phase 57).

    `<ctl_state_local_root>/_local/workspaces/<run_id>/` — outside every run
    prefix, so no sync can reach it whatever it publishes. Returns None when the
    run has no recorded local root yet (a failure before RUN.yaml exists).
    """
    local_root = load_run_metadata(run_dir).get("ctl_state_local_root")
    if not local_root:
        return None
    return (
        Path(local_root)
        .joinpath(*LOCAL_ONLY_LOCATOR)
        / LOCAL_WORKSPACES_DIRNAME
        / Path(run_dir).name
    )


def cleanup_run_workspace(run_dir: Path) -> None:
    """Drop the run's build workspace (the materialized repo checkout +
    provider cache). It is used only DURING the run and is fully
    reproducible from the pinned source_commit/cfg_source_commit in RUN.yaml;
    nothing reads it after the run."""
    workspace = run_workspace_dir(run_dir)
    if workspace is not None and workspace.exists():
        shutil.rmtree(workspace, ignore_errors=True)


def write_state_slot(run_dir: Path, state: str, payload: dict) -> None:
    group = action_group(str(payload.get("action") or load_run_metadata(run_dir).get("action")))
    slot_dir = state_slot_dir(ctl_state_dir_from_run_dir(run_dir), state, group)
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
    # §Phase 73: `action` is recorded because a pointer must say which direction
    # published it. Without it, reuse eligibility could not compare the action and
    # a workflow holding one target under two actions skipped both members.
    "action",
    "child_revisions", "source_commit", "cfg_source_commit",
    "source_state", "ref_policy", "workflow_definition_sha256",
    "target_definition_sha256", "target_cfg_view_sha256",
)


def committed_pointer_path(instance_dir: Path, group: str) -> Path:
    """`committed/<group>.yaml` — one published pointer per status group.

    A directory rather than one action-keyed file so publication stays atomic per
    group: a deployment run writes one file and touches nothing else."""
    if group not in RESULT_GROUPS:
        raise RuntimeError(f"❌ unknown state group {group!r} (expected {RESULT_GROUPS})")
    return Path(instance_dir) / "committed" / f"{group}.yaml"


def write_run_snapshot(run_dir: Path, payload: dict) -> str:
    """§consolidated: the run's RUN.yaml IS the snapshot — no separate
    snapshot.yaml. Write the frozen record (this run's payload) to RUN.yaml and
    return its sha256. Self-contained (does not assume write_current_status ran):
    RUN.yaml on disk always equals the hashed content, and the skip-up-to-date
    check re-reads RUN.yaml and verifies this digest. RUN.yaml must not change
    after commit or that verification breaks."""
    write_yaml_file(Path(run_dir) / RUN_METADATA_FILENAME, payload)
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def publish_committed_pointer(run_dir: Path, payload: dict) -> Path | None:
    """Publish the instance's committed.yaml pointer to this run's snapshot
    (§Phase 31). Writes the snapshot first (manifest-last), then the pointer
    with the denormalized facts + status readers need. The physical
    conditional write to the backend is the syncer's job; locally this is the
    authoritative record."""
    # §Phase 73: only a state owner publishes. A workflow owns execution, so its
    # RUN.yaml IS the record — persistent composition status would be a claim
    # nobody re-checks, because ctl never observes the cloud.
    if payload.get("run_type") == "workflow":
        write_run_snapshot(run_dir, payload)
        return None
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
    group = action_group(str(payload.get("action")))
    write_yaml_file(committed_pointer_path(instance_dir, group), pointer)
    return committed_pointer_path(instance_dir, group)


def read_committed_pointer(instance_dir: Path, group: str = "deployment") -> dict | None:
    path = committed_pointer_path(instance_dir, group)
    if not path.is_file():
        return None
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ committed.yaml must be a mapping: {path}")
    return data


def read_instance_state_slot(
    instance_dir: Path, state: str, group: str = "deployment"
) -> dict | None:
    """A run slot sitting beside committed.yaml in the SAME instance dir.

    `write_state_slot` anchors slots at `ctl_state_dir_from_run_dir(run_dir)`,
    which is the instance dir, so a status reader needs no run-log scan to learn
    what happened to this instance most recently."""
    path = state_slot_dir(instance_dir, state, group) / "STATUS.yaml"
    if not path.is_file():
        return None
    data = load_yaml(path) or {}
    return data if isinstance(data, dict) else None


def in_progress_verdict_reason(slot: dict) -> str:
    run_id = slot.get("run_id") or "unknown"
    action = slot.get("action") or "run"
    if slot.get("mutation_started") is True:
        return f"{action} mutating under run {run_id}"
    return f"{action} in progress under run {run_id} (not yet mutating)"


def failed_verdict_reason(slot: dict) -> str:
    run_id = slot.get("run_id") or "unknown"
    action = slot.get("action") or "run"
    summary = (slot.get("error") or {}).get("summary")
    mutated = " after mutation started" if slot.get("mutation_started") is True else ""
    return f"{action} failed{mutated} under run {run_id}" + (
        f": {summary}" if summary else ""
    )


def rewrite_in_progress_slot_if_present(run_dir: Path, payload: dict) -> None:
    slot_dir = state_slot_dir(
        ctl_state_dir_from_run_dir(run_dir),
        "in_progress",
        action_group(str(load_run_metadata(run_dir).get("action"))),
    )
    if slot_dir.exists():
        write_state_slot(run_dir, "in_progress", payload)


def mark_run_started(run_dir: Path) -> None:
    payload = build_status_payload(run_dir, "in_progress")
    write_current_status(run_dir, payload)
    write_state_slot(run_dir, "in_progress", payload)


def record_workflow_members(
    run_dir: Path, active_target_runs: dict, workflow_cfg: dict
) -> None:
    """§Phase 73: a workflow run records the composition it ran, as history.

    §Phase 78: it records target INSTANCES, not keys. A workflow's own instance
    params are the UNION of its members', so a member varying over fewer axes has
    a SHORTER address — which cannot be reconstructed from the workflow's segments.
    The addresses are already resolved on the run (`target_addresses`), so keying
    by name would throw away a fact the run already holds.

    Each entry mirrors the cfg shape — a bare address when it takes the member's
    `default_action`, `{instance, action}` when it differs — so the record reads
    like the declaration that produced it. `member_selectors` is the matched
    member's own block copied verbatim, which points back at that declaration and
    keeps the engine from needing to know a field called "operation".
    """
    default_action = workflow_cfg.get("default_action")
    resolved_addresses = {
        split_target_instance_address(address)[0]: address
        for address in (load_run_metadata(run_dir).get("target_addresses") or [])
        if isinstance(address, str)
    }
    target_instances: list = []
    for target_run in active_target_runs.values():
        key = target_run.get("target")
        if not key:
            continue
        # A key with no resolved address is a singleton target, whose address IS
        # its key — never a silent omission.
        address = resolved_addresses.get(key, key)
        action = target_run.get("action")
        if action and action != default_action:
            target_instances.append({"instance": address, "action": action})
        else:
            target_instances.append(address)
    facts: dict = {"target_instances": target_instances}
    if default_action:
        facts["default_action"] = default_action
    if workflow_cfg.get("member_selectors"):
        facts["member_selectors"] = workflow_cfg["member_selectors"]
    update_run_metadata(run_dir, facts)
    status = load_current_status(run_dir)
    if status:
        status.update({**facts, "updated_at": utc_timestamp()})
        write_current_status(run_dir, status)


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
    # Outdate at mutation START, not at run end. From here real resources are
    # changing, so every dependent result IS stale — deferring the marks to
    # mark_run_succeeded/failed leaves readers a `current` verdict across the
    # entire mutation window, the longest stretch of the run.
    mark_outdated_for_run(run_dir, include_current_result=False)
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
                instance_segments, _ = split_instance_segments(after)
                rest = rest[:marker]
            result_name = "/".join(rest)
            if not result_name:
                return None
            address = instance_address(result_name, instance_segments)
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
    yield from sorted(root.rglob("committed/*.yaml"))


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


CHILD_LOCK_GRANT_ENV = "ATLAS_CHILD_LOCK_GRANT"
# A grant is spent ONCE globally, but the lock decision is asked twice per run
# (prepare, then complete_prepare_after_preflight). Remember the redemption for
# this process so the second ask does not look like a replay.
_REDEEMED_CHILD_GRANT: str | None = None


def mint_child_lock_grant(
    ctl_state_local_root: Path, *, child_kind: str, child_key: str
) -> str:
    """§Phase 61(d): mint a SINGLE-USE grant letting one child run under the lock
    this process holds.

    The run id cannot serve as the credential: it is printed in logs, stored in
    run metadata and visible in `ps`, and it stays valid for the whole run — so
    anyone who reads it can start a concurrent run against the same ctl-state
    while the parent is still going. A nonce is unguessable, is BOUND to the one
    child it was minted for, and is CONSUMED on use, so a leaked value buys
    nothing.
    """
    metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
    if not metadata:
        raise RuntimeError("❌ cannot mint a child lock grant without a held ctl-state lock")
    grant = uuid.uuid4().hex
    grants = dict(metadata.get("child_lock_grants") or {})
    grants[grant] = {"kind": child_kind, "key": child_key}
    metadata["child_lock_grants"] = grants
    write_yaml_file(ctl_state_lock_metadata_path(ctl_state_local_root), metadata)
    return grant


def consume_child_lock_grant(
    ctl_state_local_root: Path, grant: str | None, *, child_kind: str, child_key: str | None
) -> bool:
    """Redeem a child grant exactly once, for the child it was minted for."""
    global _REDEEMED_CHILD_GRANT
    if not grant:
        return False
    if _REDEEMED_CHILD_GRANT == grant:
        return True
    metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
    grants = dict(metadata.get("child_lock_grants") or {})
    claim = grants.get(grant)
    if not isinstance(claim, dict):
        return False
    if claim.get("kind") != child_kind or (
        claim.get("key") is not None and claim.get("key") != child_key
    ):
        return False
    del grants[grant]                       # single use
    metadata["child_lock_grants"] = grants
    write_yaml_file(ctl_state_lock_metadata_path(ctl_state_local_root), metadata)
    _REDEEMED_CHILD_GRANT = grant
    return True


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
    details.append("If the owning ctl process is gone, run maintenance unlock-ctl-state with --lock-id " + str(lock_id))
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
    """Whether this run proceeds WITHOUT acquiring the ctl-state lock.

    Two cases, both requiring the caller to PROVE it knows the held lock's id:

    - `unlock-ctl-state` maintenance, which exists to release that very lock;
    - §Phase 61(d) a workflow-spawned child target run, which executes under the
      lock its parent already holds. The lock is `flock(LOCK_EX | LOCK_NB)`, so a
      child that tried to acquire it would fail outright — exactly one holder, and
      children run provably under it. Authorisation is a SINGLE-USE grant minted
      per child and redeemed once, so the public run id cannot be replayed into a
      concurrent run; an FD handed down instead would be unforgeable but could not
      cross a container or host boundary.
    """
    if run_type == "maintenance":
        return (
            getattr(args, "maintenance_action", None) == "unlock-ctl-state"
            and ctl_state_lock_matches(
                args.ctl_state_local_root, getattr(args, "lock_id", None)
            )
        )
    # §Phase 61(d): a child runs under its parent's lock only by redeeming a
    # SINGLE-USE grant the parent minted for it, passed by environment so it is
    # absent from `ps` and from the logged command line. The parent run id is NOT
    # a credential — it is public.
    return consume_child_lock_grant(
        args.ctl_state_local_root,
        os.environ.get(CHILD_LOCK_GRANT_ENV),
        child_kind=run_type,
        child_key=getattr(args, run_type, None) if run_type != "fan_out" else None,
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
                "summary": "ctl-state lock was cleared by maintenance unlock-ctl-state",
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


def release_remote_mutation_lock(syncer, lock_id: str) -> str:
    """Release the NAMESPACE lock, refusing to release someone else's.

    `--lock-id` names the holder from the error rather than releasing whatever is
    present: the hazard is not a stale lock, it is releasing one whose holder is
    still ALIVE. Only this lock protects the namespace from another machine, so a
    wrong release yields two concurrent mutating runs on one namespace.
    """
    existing = syncer.read_mutation_lock()
    if not existing:
        return "not present — skipped"
    holder = str(existing.get("run_id") or "")
    if holder != lock_id:
        raise RuntimeError(
            f"❌ the namespace lock is held by run {holder!r}, not {lock_id!r}; "
            "pass the id named in the error"
        )
    syncer.delete_mutation_lock()
    return "released"


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
                f"❌ ctl-state lock changed while unlock-ctl-state was starting: expected {lock_id!r}, got {metadata.get('run_id')!r}"
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
        logging.warning("Ctl-state lock released for run_id=%s", lock_id)
        lock.run_id = lock_id
        return True
    finally:
        lock.release(clear_metadata=True)


SCOPE_META_FILENAME = "__meta__.yaml"
SCOPE_COMPOSITION_FILENAME = "__scope_composition__.yaml"
# §Phase 62: declaration files configure composition and are never payload.
SCOPE_META_SKIP_FILENAMES = {
    SCOPE_META_FILENAME,
    PLT_GUARDRAILS_FILENAME,
    SCOPE_COMPOSITION_FILENAME,
    *cfg_presets.DECLARATION_FILENAMES,
}

# §Phase 62: materialized imports live for exactly one discovery pass. The next
# pass frees the previous one, so a long-lived process holds at most one run's
# worth rather than accumulating them.
_MATERIALIZED_IMPORT_DIRS: list[tempfile.TemporaryDirectory] = []
# materialized dir -> the cfg-absolute import path it was composed from, so a log
# line names the preset rather than a scratch path that will not exist afterwards
_MATERIALIZED_IMPORT_LABELS: dict[Path, str] = {}


def _new_materialization_workspace() -> Path:
    workspace = tempfile.TemporaryDirectory(prefix="atlas-cfg-preset-")
    _MATERIALIZED_IMPORT_DIRS.append(workspace)
    return Path(workspace.name)


def _release_materialized_imports() -> None:
    while _MATERIALIZED_IMPORT_DIRS:
        _MATERIALIZED_IMPORT_DIRS.pop().cleanup()
    _MATERIALIZED_IMPORT_LABELS.clear()

def selector_expected_values(expected, *, label: str) -> list[str]:
    if isinstance(expected, str) and expected:
        return [expected]
    if isinstance(expected, list) and all(isinstance(item, str) and item for item in expected):
        return expected
    raise RuntimeError(f"❌ {label} must be a non-empty string or list of non-empty strings")


EXECUTION_CONTEXT_ROOT = "execution_context"
# §Phase 61(a): three namespaces, by KIND of fact.
#   ctl     the invocation      action, profile, providers, runtime mode
#   params  COORDINATES         identify an instance; become ctl-state path segments
#   target  the target's own    what it reads (domains) + its constants (static_vars);
#                               never a coordinate, never a path segment
EXECUTION_CONTEXT_NAMESPACES = ("ctl", "params", "target", "sourced")
EXECUTION_CONTEXT_PARAMS_PREFIX = f"{EXECUTION_CONTEXT_ROOT}.params."
# The key may itself be dotted — that is how provider-specific params are
# namespaced under their provider. Provider-neutral params keep a single segment
# (execution_context.params.env.type). Kept in sync with CONTEXT_KEY_RE, which
# validates the key on the way in.
EXECUTION_CONTEXT_REF_RE = re.compile(
    rf"^{EXECUTION_CONTEXT_ROOT}\.(?:{'|'.join(EXECUTION_CONTEXT_NAMESPACES)})"
    rf"\.[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
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


SELECTOR_STRUCTURED_KEYS = ("match", "in", "contains")


def selector_contains_requirements(selectors: object, *, label: str) -> dict[str, set[str]]:
    """The `contains` block: ref -> values that must be PRESENT IN the list-valued
    fact at ref.

    `match`/`in` ask "is this scalar fact one of these values?". `contains` asks
    the inverse: "is this value among the facts?" — needed for list-valued context
    facts such as the run's declared providers. Generic: nothing here is
    provider-specific; it applies to any list-valued fact.
    """
    if not isinstance(selectors, dict):
        return {}
    requirements: dict[str, set[str]] = {}

    # structured form: {contains: {ref: values}}
    raw = selectors.get("contains") or {}
    if raw:
        if not isinstance(raw, dict):
            raise RuntimeError(f"❌ selectors.contains must be a mapping: {label}")
        for ref, expected in raw.items():
            ref = validate_execution_context_ref(ref, label=f"{label}.contains")
            requirements[ref] = set(
                selector_expected_values(expected, label=f"{label}.contains.{ref}")
            )

    # per-ref form: {ref: {contains: values}} — the shape constraint gates use,
    # where each entry is a direct match-mapping rather than a selectors block
    for ref, expected in selectors.items():
        if ref in ("match", "in", "contains") or not _is_contains_predicate(expected):
            continue
        ref = validate_execution_context_ref(ref, label=label)
        requirements[ref] = set(
            selector_expected_values(expected["contains"], label=f"{label}.{ref}.contains")
        )
    return requirements


def _is_contains_predicate(value: object) -> bool:
    return isinstance(value, dict) and set(value) == {"contains"}


def _context_fact_members(value: object) -> set[str]:
    """A context fact as a set of members (a list fact -> its items; a scalar -> itself)."""
    if isinstance(value, (list, tuple, set)):
        return {str(item) for item in value}
    return {str(value)}


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

    uses_structured = any(key in selectors for key in SELECTOR_STRUCTURED_KEYS)
    if uses_structured:
        unknown = sorted(set(selectors) - set(SELECTOR_STRUCTURED_KEYS))
        if unknown:
            raise RuntimeError(f"❌ selectors has unsupported keys {unknown}: {label}")
        requirements: dict[str, set[str]] = {}
        raw_match = selectors.get("match") or {}
        raw_in = selectors.get("in") or {}
        # `contains` is a different predicate KIND (membership in a list-valued
        # fact, not a scalar in a set) — see selector_contains_requirements.
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
        if _is_contains_predicate(expected):
            continue  # handled by selector_contains_requirements
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
    for ref, required_members in selector_contains_requirements(selectors, label=label).items():
        if ref not in execution_context:
            logging.debug("Selector %s: %s", label, execution_context_miss_message(execution_context, ref))
            return False
        if not required_members <= _context_fact_members(execution_context[ref]):
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

# An execution-context key is one or more identifier segments joined by dots.
# The dotted form is how provider-specific params are namespaced under their
# provider (`<provider>.<param>`), so one provider's vocabulary cannot collide
# with another's; provider-neutral params stay single-segment (`env_type`,
# `landing_zone`). The context is a flat dotted map, so a dotted key is simply a
# longer key — selectors already match full paths. The engine assigns no meaning
# to the leading segment; only the provider adapter interprets it.
CONTEXT_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
EXECUTION_PARAMS_KEY = "execution_params"
# The key may be DOTTED — namespaced params are the normal case, so a
# whole-value reference must be able to name one.
EXECUTION_CONTEXT_PARAM_REF_RE = re.compile(
    rf"^\$\{{({EXECUTION_CONTEXT_ROOT}\.(?:ctl|params)\."
    rf"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\}}$"
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
        # A namespaced family may be authored as a NESTED mapping and is flattened
        # to the dotted param keys the context uses — `ns: {key: x}` and
        # `ns.key: x` declare the same param. Params stay scalar-only; nesting is
        # an authoring shape only, so one namespace's params read as one block.
        # The engine names no namespace: the consumer chooses them.
        def walk(node, prefix=""):
            for key, raw in node.items():
                if not isinstance(key, str) or not CONTEXT_KEY_RE.fullmatch(key):
                    raise RuntimeError(
                        f"❌ {EXECUTION_PARAMS_KEY} key must be a valid identifier: {key!r}"
                    )
                dotted = f"{prefix}{key}"
                if isinstance(raw, dict):
                    if not raw:
                        raise RuntimeError(f"❌ {EXECUTION_PARAMS_KEY}.{dotted} must not be an empty map: {path}")
                    walk(raw, f"{dotted}.")
                    continue
                if dotted in entries:
                    raise RuntimeError(
                        f"❌ duplicate {EXECUTION_PARAMS_KEY}.{dotted}: {path} (also defined in {origins[dotted]})"
                    )
                entries[dotted] = raw
                origins[dotted] = path

        walk(section)
    return entries


def build_execution_context(
    ctl_cfg_root: Path,
    *,
    action: str | None,
    ctl_profile: str | None,
    execution_params: dict[str, str],
    execution_access_modes: dict[str, str] | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_runtime_mode: str,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
) -> dict[str, object]:
    """Build the flat dotted execution context: the closed, namespaced facts of
    this execution. Namespaces: `ctl` (promoted engine args), `params` (consumer
    values, merged from --execution-params CLI + the execution_params cfg block),
    and `sourced` (data ctl READ from a declared `ctl_sources` entry, so a
    consumer never keeps its own copy). Keys look like
    'execution_context.params.env.type'."""
    context: dict[str, object] = {}

    def put_list(namespace: str, key: str, values, *, label: str) -> None:
        """A LIST-valued promoted fact. Params remain scalar-only; this exists for
        facts that are inherently plural (the run's declared providers), matched
        with the `contains` selector predicate."""
        if not CONTEXT_KEY_RE.fullmatch(key):
            raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
        cleaned = [str(_context_scalar(v, label=label)) for v in (values or [])]
        context[f"{EXECUTION_CONTEXT_ROOT}.{namespace}.{key}"] = cleaned

    def put(namespace: str, key: str, value, *, label: str) -> None:
        if not CONTEXT_KEY_RE.fullmatch(key):
            raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
        context[f"{EXECUTION_CONTEXT_ROOT}.{namespace}.{key}"] = _context_scalar(value, label=label)

    if action is not None:
        put("ctl", "action", action, label="promoted --action")
        # §Phase 73: a WORKFLOW is invoked with an operation and its members carry
        # the actions, so member selectors gate on `ctl.operation`. It is published
        # alongside rather than instead of `ctl.action`: a target run still has one
        # action, and credential selection keys on that.
        put("ctl", "operation", action, label="promoted --operation")
    if ctl_profile is not None:
        put("ctl", "profile", ctl_profile, label="promoted --ctl-profile")
    # One fact per participating provider — the mode is a per-provider decision,
    # so cfg gates on `...ctl.execution_access_mode.<provider>`, never on a
    # single run-wide value. The engine names no provider and no mode here.
    for _provider, _mode in sorted((execution_access_modes or {}).items()):
        put(
            "ctl",
            f"execution_access_mode.{_provider}",
            _mode,
            label="promoted --execution-access-mode",
        )
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
    # §Phase 53: the run DECLARES its participating providers; cfg gates on
    # membership (`contains`), and every target's provider must be in this list.
    put_list("ctl", "providers", providers, label="promoted --providers")

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

    # Provider-DERIVED params. A fact that is a property of a declared param
    # (not an independent choice) is resolved by the adapter that owns it,
    # instead of being restated at every call site. The engine names no key: it
    # hands each participating adapter the declared params and namespaces
    # whatever it returns. Derived facts never override a declared one.
    declared = {
        ref[len(f"{EXECUTION_CONTEXT_ROOT}.params."):]: value
        for ref, value in context.items()
        if ref.startswith(f"{EXECUTION_CONTEXT_ROOT}.params.")
    }
    derived_param_keys: list[str] = []
    for provider in providers or ():
        adapter = get_provider_adapter(provider)
        derive = getattr(adapter, "derived_params", None)
        if derive is None:
            continue
        for key, value in (derive(ctl_cfg_root, dict(declared)) or {}).items():
            derived_param_keys.append(key)
            label = f"provider {provider!r} derived param {key}"
            if key in declared:
                raise RuntimeError(
                    f"❌ {label} collides with a declared execution param; "
                    "a derived fact must not be passed explicitly"
                )
            put("params", key, value, label=label)
    # §Phase 61(a): a DERIVED param is not an input — no caller can supply it, so a
    # target never declares it. Recording which keys were derived lets the per-target
    # filter pass them through without treating them as declarable inputs.
    put_list("ctl", "derived_params", sorted(derived_param_keys), label="derived params")

    # Declared SOURCES. ctl reads each `ctl_sources` entry once, for the
    # frozen context, and publishes the payload here — so a consumer references
    # the fact instead of keeping a second copy of it. The engine names no input
    # and no key: it asks each participating adapter what it resolved and
    # flattens whatever comes back. Values are scalars because that is what a
    # cfg reference can carry.
    for provider in providers or ():
        adapter = get_provider_adapter(provider)
        resolve_sources = getattr(adapter, "resolved_sources", None)
        if resolve_sources is None:
            continue
        for source_key, payload in (resolve_sources(ctl_cfg_root, dict(context)) or {}).items():
            for leaf_key, leaf_value in _flatten_sourced_payload(
                payload, prefix=source_key, label=f"provider {provider!r} source {source_key!r}"
            ).items():
                put("sourced", leaf_key, leaf_value, label=f"source {source_key!r}")
    return context




def _flatten_sourced_payload(payload, *, prefix: str, label: str) -> dict[str, object]:
    """Flatten a sourced payload into dotted scalar leaves.

    A cfg reference resolves to a SCALAR, so a nested payload is published leaf by
    leaf: `accounts_registry.dev.account_id`, never one map under one key.
    """
    if isinstance(payload, dict):
        flat: dict[str, object] = {}
        for key, value in payload.items():
            if not isinstance(key, str) or not CONTEXT_KEY_RE.fullmatch(key):
                raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
            flat.update(_flatten_sourced_payload(value, prefix=f"{prefix}.{key}", label=label))
        return flat
    if isinstance(payload, (str, int, float, bool)):
        return {prefix: payload}
    raise RuntimeError(
        f"❌ {label}: {prefix} must resolve to nested mappings of scalars, "
        f"got {type(payload).__name__}"
    )


def execution_context_nested(execution_context: dict[str, object]) -> dict[str, dict[str, object]]:
    """Nested {execution_context: {ctl: {...}, params: {...}}} view.

    Dotted keys nest fully (params.<provider>.x -> params: {<provider>: {x: ...}}),
    so the rendered artifact reads as structure, not as flat dotted strings. A
    scalar and a subtree cannot share a path — that collision fails loud.
    """
    nested: dict[str, dict[str, object]] = {ns: {} for ns in EXECUTION_CONTEXT_NAMESPACES}
    for ref, value in execution_context.items():
        _, namespace, key = ref.split(".", 2)
        node = nested[namespace]
        segments = key.split(".")
        for segment in segments[:-1]:
            child = node.setdefault(segment, {})
            if not isinstance(child, dict):
                raise RuntimeError(
                    f"❌ execution context key {ref!r} nests under a scalar fact"
                )
            node = child
        if isinstance(node.get(segments[-1]), dict):
            raise RuntimeError(
                f"❌ execution context key {ref!r} collides with a nested subtree"
            )
        node[segments[-1]] = value
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
) -> None:
    from utils import guardrails

    guardrails.verify_plt_guardrails(
        plt_cfg_root,
        plt_cfg_root,
        guardrails_cfg_root,
        plt_rendered_dir,
        execution_context,
        scope_params,
    )


def verify_guardrails(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
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

    # §Phase 62: imports are declared in their own file, so a preset can import
    # without being a scope. A scope declaring them in __meta__.yaml is stale.
    if "imports" in meta_cfg:
        raise RuntimeError(
            f"❌ imports belong in {cfg_presets.IMPORTS_FILENAME}, not {SCOPE_META_FILENAME}: {meta_path}"
        )
    if (scope_root / cfg_presets.PARAMS_FILENAME).exists():
        raise RuntimeError(
            f"❌ a scope is selected by selectors, not instantiated by an importer, so it cannot "
            f"declare {cfg_presets.PARAMS_FILENAME}: {scope_id}"
        )

    source_dirs: list[str] = []
    seen_imports: set[Path] = set()
    entries = cfg_presets.declared_imports(scope_root)
    workspace = _new_materialization_workspace() if entries else None
    # one composition per scope: a preset reached by several paths must be
    # configured the same way each time
    composition: dict[str, tuple] = {}
    cfg_presets.assert_no_redundant_imports(cfg_root, scope_root, scope_id)
    for index, entry in enumerate(entries):
        import_path = normalize_cfg_absolute_path(
            entry["from"],
            label=f"import path in {scope_id}",
            allow_root=False,
        )
        src = cfg_abs_path_to_dir(cfg_root, import_path, label=f"import path in {scope_id}")
        if not src.exists():
            raise RuntimeError(f"Import path not found: {src}")
        if not src.is_dir():
            raise RuntimeError(f"Import path must be a directory: {src}")
        if not any(p.is_file() and ".git" not in p.relative_to(src).parts for p in src.rglob("*.yaml")):
            raise RuntimeError(f"Import path must contain at least one yaml cfg file: {src} ({scope_id})")
        validate_no_cfg_meta_inside_data_dir(src, import_path=import_path, meta_path=meta_path)
        if src == scope_root:
            raise RuntimeError(f"Scope imports itself in {scope_id}: {import_path}")
        seen_imports.add(src)

        materialized = workspace / f"{index:03d}"
        _MATERIALIZED_IMPORT_LABELS[materialized] = import_path
        cfg_presets.materialize(
            cfg_root,
            import_path,
            dest=materialized,
            bindings=entry["with"],
            composition=composition,
        )
        source_dirs.append(str(materialized))

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


def validate_all_cfg_payload_is_reachable(cfg_root: Path) -> None:
    """§Phase 62: every payload file sits inside a scope or an imported preset.

    A file outside both is never merged and never renders — it is silently dead
    cfg, which reads as configuration and behaves as nothing. Composition is the
    only way payload reaches a target, so a file that no unit contains is an
    authoring error rather than an inert extra.
    """
    units: list[str] = []
    for meta_path in discover_cfg_meta_paths(cfg_root):
        if load_cfg_meta(meta_path)["type"] == "overlay":
            continue
        units.append("/" + meta_path.parent.relative_to(cfg_root).as_posix())
    for imports_path in sorted(cfg_root.rglob(cfg_presets.IMPORTS_FILENAME)):
        for entry in cfg_presets.declared_imports(imports_path.parent):
            units.append(entry["from"])
    prefixes = tuple(unit.rstrip("/") + "/" for unit in units)

    orphans: list[str] = []
    for path in sorted(cfg_root.rglob("*.yaml")):
        parts = path.relative_to(cfg_root).parts
        if parts[0] in (".git", "_overlays", "diagrams") or PLT_GUARDRAILS_DIRNAME in parts:
            continue
        if path.name in SCOPE_META_SKIP_FILENAMES:
            continue
        rel = "/" + path.relative_to(cfg_root).as_posix()
        if not rel.startswith(prefixes):
            orphans.append(rel)
    if orphans:
        raise RuntimeError(
            "❌ cfg payload outside every scope and preset, so it can never be merged: "
            + ", ".join(orphans)
        )


def discover_active_cfg_scopes(
    plt_cfg_root: Path,
    *,
    scope_params: dict[str, str],
    execution_context: dict[str, object] | None = None,
) -> list[dict]:
    """Discover active cfg merge scopes from type: scope metadata."""
    cfg_root = plt_cfg_root.resolve()
    runtime_context = execution_context or execution_context_from_scope_params(scope_params)
    validate_all_cfg_payload_is_reachable(cfg_root)
    _release_materialized_imports()
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


def canonical_sha256(value: object) -> str:
    """Hash a JSON-compatible value with stable mapping-key ordering."""
    canonical = json.dumps(
        value, separators=(",", ":"), sort_keys=True, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def target_definition_document(target_run: dict) -> dict:
    """Return the stable, resolved target definition used for execution."""
    stable_keys = (
        "target",
        "source",
        "ref",
        "branch",
        "commit",
        "procedure",
        "domains",
        "cfg_keys",
        "target_instance_params",
        "requires_plt_overlays",
        "execution_identities",
        "provisions_ctl_state_backend",
        "allow_agreed_defer_ctl_state_backend_sync",
        *sorted(target_consent_opt_in_fields()),
    )
    definition = {
        key: target_run[key]
        for key in stable_keys
        if target_run.get(key) is not None
    }
    modules = {}
    for module_name, module in (target_run.get("modules") or {}).items():
        stable_module = {
            key: module[key]
            for key in ("dest", "branch", "commit")
            if module.get(key) is not None
        }
        modules[module_name] = stable_module
    if modules:
        definition["modules"] = modules
    return definition


def attach_target_definition_facts(active_target_runs: dict) -> None:
    for target_run in active_target_runs.values():
        definition = target_definition_document(target_run)
        target_run["target_definition"] = definition
        target_run["target_definition_sha256"] = canonical_sha256(definition)


def directory_content_sha256(path: Path) -> str:
    """Hash a directory view from sorted relative paths and exact file bytes."""
    digest = hashlib.sha256()
    files = (
        sorted(item for item in Path(path).rglob("*") if item.is_file())
        if Path(path).is_dir()
        else []
    )
    for item in files:
        relative = item.relative_to(path).as_posix().encode("utf-8")
        content = item.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def attach_target_cfg_view_facts(
    active_target_runs: dict, plt_targets_dir: Path
) -> None:
    for target_run_id, target_run in active_target_runs.items():
        target_run["target_cfg_view_sha256"] = directory_content_sha256(
            (Path(plt_targets_dir) if (Path(plt_targets_dir) / "input").is_dir()
             else Path(plt_targets_dir) / target_run_id) / "input"
        )


def finalize_target_cfg_view_facts(
    active_target_runs: dict,
    plt_targets_dir: Path,
    pipeline_run_cfg_path: Path,
) -> None:
    """Attach cfg-view hashes and refresh the resolved target-run artifact."""
    attach_target_cfg_view_facts(active_target_runs, plt_targets_dir)
    pipeline_cfg = load_yaml(pipeline_run_cfg_path) or {}
    pipeline_cfg["target_runs"] = active_target_runs
    write_yaml_file(pipeline_run_cfg_path, pipeline_cfg)


def _overlay_leaf_values(overlay: dict) -> dict:
    return _scope_final_yaml_leaves(
        {"source_dirs": [str(overlay["root"])]},
        skip_filenames=SCOPE_META_SKIP_FILENAMES,
    )


def resolve_target_plt_overlays(
    plt_cfg_root: Path,
    explicit_overlays: list[str],
    target_run: dict,
    *,
    execution_context: dict[str, object],
) -> list[str]:
    """§Phase 61(b): the overlays ONE target_run is merged with.

    A target's `requires_plt_overlays` applies to that target and no other. The
    run-wide union it replaces meant an overlay declared by one target reshaped
    the cfg every other target received — `db_artificial_populator` alone touches
    `foundation`, `ecr_images_cfg`, `ecr_repos_cfg` and `workload_identity`, all
    of which other targets consume.
    """
    return resolve_run_plt_overlays(
        plt_cfg_root,
        explicit_overlays,
        {"target": target_run},
        execution_context=execution_context,
    )


def whole_tree_execution_context(
    ctl_cfg_root: Path, execution_context: dict[str, object]
) -> dict[str, object]:
    """§Phase 60/61: a context that activates EVERY declared domain's scopes.

    Scope activation is now the scope's own condition (`contains` over
    `target.domains`), which a real run supplies per target. Whole-tree tooling —
    `validate_cfg`, `regenerate_guardrails` — has no target, so it declares the
    full domain registry: it is validating the tree, not running one target.
    """
    context = dict(execution_context)
    context[f"{EXECUTION_CONTEXT_ROOT}.target.domains"] = sorted(
        load_domain_registry(ctl_cfg_root)
    )
    return context


def build_target_execution_context(
    target_run_id: str,
    target_run: dict,
    run_execution_context: dict[str, object],
) -> dict[str, object]:
    """§Phase 61(a): the execution context AS ONE TARGET SEES IT.

    - `ctl.*` passes through unchanged — it is a property of the INVOCATION.
    - `params.*` is FILTERED to the params this target declared. A target cannot
      read a coordinate it did not declare, so a param that is irrelevant to it is
      structurally unreachable rather than merely unused.
    - `target.*` carries what the target declares about itself: the domains it
      reads and its static vars. Never a coordinate, never a ctl-state segment.
    """
    declared = target_run.get("input_params")
    derived = set(
        run_execution_context.get(f"{EXECUTION_CONTEXT_ROOT}.ctl.derived_params") or []
    )
    context: dict[str, object] = {}
    for ref, value in run_execution_context.items():
        _, namespace, key = ref.split(".", 2)
        if namespace != "params":
            context[ref] = value
            continue
        if declared is None or key in declared or key in derived:
            context[ref] = value

    if declared:
        missing = sorted(
            k for k in declared
            if f"{EXECUTION_CONTEXT_ROOT}.params.{k}" not in context
        )
        if missing:
            raise RuntimeError(
                f"❌ target_run {target_run_id!r} declares input params {missing} that this "
                "run does not supply"
            )

    domains = target_run.get("domains")
    if domains:
        context[f"{EXECUTION_CONTEXT_ROOT}.target.domains"] = [str(d) for d in domains]
    for name, value in (target_run.get("static_vars") or {}).items():
        context[f"{EXECUTION_CONTEXT_ROOT}.target.static_vars.{name}"] = _context_scalar(
            value, label=f"target_run {target_run_id!r} static_vars.{name}"
        )
    return context


def target_cfg_views_root(run_dir: Path, run_type: str) -> Path:
    """Where a run keeps its per-target cfg derivations.

    A TARGET run has exactly one target, so it writes straight to `cfg/plt` —
    nesting it under `targets/<key>/` would repeat, in a path, what the whole run
    already is. A WORKFLOW pre-checks many, so it keeps them apart under
    `cfg/plt/targets/<key>/` (and drops the tree once each child owns its copy).
    """
    base = run_dir / "cfg" / "plt"
    return base if run_type == "target" else base / "targets"


def target_cfg_view_dir(run_dir: Path, run_type: str, target_run_id: str) -> Path:
    root = target_cfg_views_root(run_dir, run_type)
    return root if run_type == "target" else root / target_run_id


def prepare_target_cfg_view(
    target_run_id: str,
    target_run: dict,
    *,
    plt_cfg_root: Path,
    target_cfg_dir: Path,
    ctl_profile: str,
    scope_params: dict[str, str] | None,
    execution_context: dict[str, object],
) -> Path:
    """§Phase 61(b): merge, render and project ONE target's cfg, under its own dir.

    The workflow authors ordering; a target derives its own cfg. Nothing is shared
    between targets, so a target runs standalone exactly as it runs in a workflow,
    and its provenance is its own rather than a slice of a run-wide tree.
    """
    target_dir = target_cfg_dir
    merged_dir = target_dir / "merged"
    merge_plt_cfg_dirs(
        plt_cfg_root=plt_cfg_root,
        plt_merged_dir=merged_dir,
        ctl_profile=ctl_profile,
        plt_overlays=list(target_run.get("plt_overlays") or []),
        scope_params=scope_params,
        execution_context=execution_context,
        source_log_roots=(plt_cfg_root.resolve(),),
        dest_log_roots=(target_dir.resolve(),),
    )
    return render_plt_cfg(merged_dir, target_dir, execution_context)


def resolve_run_plt_overlays(
    plt_cfg_root: Path,
    explicit_overlays: list[str],
    active_target_runs: dict,
    *,
    execution_context: dict[str, object],
) -> list[str]:
    """Append target-required overlays in target order and validate conflicts."""
    duplicates = [
        item
        for item, count in collections.Counter(explicit_overlays).items()
        if count > 1
    ]
    if duplicates:
        raise RuntimeError(
            "plt overlays must be unique; duplicates: " + ", ".join(sorted(duplicates))
        )

    final_overlays = list(explicit_overlays)
    automatically_appended: list[str] = []
    for target_run in active_target_runs.values():
        for overlay_name in target_run.get("requires_plt_overlays") or []:
            if overlay_name not in final_overlays:
                final_overlays.append(overlay_name)
                automatically_appended.append(overlay_name)

    if not final_overlays:
        return []

    candidates = discover_overlay_candidates(
        plt_cfg_root, execution_context=execution_context
    )
    for overlay_name in final_overlays:
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

    leaf_cache: dict[str, dict] = {}
    for overlay_name in automatically_appended:
        overlay_index = final_overlays.index(overlay_name)
        current_leaves = leaf_cache.setdefault(
            overlay_name, _overlay_leaf_values(candidates[overlay_name])
        )
        for previous_name in final_overlays[:overlay_index]:
            previous_leaves = leaf_cache.setdefault(
                previous_name, _overlay_leaf_values(candidates[previous_name])
            )
            conflicts = sorted(
                leaf_key
                for leaf_key in current_leaves.keys() & previous_leaves.keys()
                if current_leaves[leaf_key] != previous_leaves[leaf_key]
            )
            if conflicts:
                rel_path, yaml_path = conflicts[0]
                rendered_path = ".".join(str(part) for part in yaml_path) or "<root>"
                raise RuntimeError(
                    f"Automatically required plt overlay {overlay_name!r} conflicts with "
                    f"selected overlay {previous_name!r} at {rel_path}:{rendered_path}; "
                    "supply the complete ordered overlay list explicitly with --plt-overlays "
                    "to acknowledge precedence"
                )

    return final_overlays


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
) -> dict[str, list[str]]:
    """Build scoped merged cfg trees from typed __meta__.yaml metadata.

    Scope and overlay activation both use the uniform selectors.match/selectors.in
    execution-context selector model.
    §Phase 60/61: a scope declares its own CONDITION
    (`selectors.contains: {execution_context.target.domains: <domain>}`), so it
    activates iff the run reads its domain. `target_path` stays purely the
    DESTINATION. The former `required_target_paths` filter did the same job from
    the other side and was removed — two mechanisms deciding one thing can
    disagree."""
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
        scopes_by_target: dict[str, list[dict]] = collections.defaultdict(list)
        for scope in active_scopes:
            scopes_by_target[scope["target_path"]].append(scope)
        for target_path, scopes in scopes_by_target.items():
            validate_cross_scope_leaf_conflicts(
                scopes,
                target_path=target_path,
                skip_filenames=composition_files,
            )

        merged_target_paths: set[str] = set()

        for scope in active_scopes:
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
) -> tuple[dict, Path, list[str]]:
    """
    Build active target_runs, resolve per-target overlays, and write pipeline_run_cfg.

    §Phase 61(b): this no longer merges anything. Each target derives its own cfg
    (`prepare_target_cfg_view`), so there is no run-wide merged tree to build here.

    Returns:
        tuple: (active_target_runs, pipeline_run_cfg_path, final_plt_overlays)
    """
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

    # §Phase 61(b): overlays are a PER-TARGET declaration, so each target_run gets
    # exactly the overlays it asked for plus the run's explicit ones. The former
    # run-wide union meant a target that never declared an overlay still had its cfg
    # merged with it — `requires_plt_overlays` now means what it says.
    final_plt_overlays = resolve_run_plt_overlays(
        plt_cfg_root,
        plt_overlays,
        active_target_runs,
        execution_context=execution_context or {},
    )
    for target_run in active_target_runs.values():
        target_run["plt_overlays"] = resolve_target_plt_overlays(
            plt_cfg_root,
            plt_overlays,
            target_run,
            execution_context=execution_context or {},
        )
    attach_target_definition_facts(active_target_runs)

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

    return active_target_runs, pipeline_run_cfg_path, final_plt_overlays


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
                "execution_identities": target_run.get("execution_identities"),
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

CTL_STATE_OPERATIONS = ("read", "sync", "maintenance")


def validate_ctl_state_backend_execution(execution: object, *, label: str, path: Path) -> dict:
    """Validate a ctl-state backend's `execution_identity:` block (§Phase 53).

    Same shape as a target's, with one structural difference: a backend has a
    role per ctl-state OPERATION (read / sync / maintenance — least privilege),
    where a target has one per authorization class. The account is fixed, so the
    direct credential source is SINGULAR per operation rather than a list.
    """
    if not isinstance(execution, dict) or not execution:
        raise RuntimeError(f"❌ {label}.execution must be a non-empty mapping: {path}")
    unknown = sorted(set(execution) - {"account", "operations"})
    if unknown:
        raise RuntimeError(
            f"❌ {label}.execution has unknown fields {unknown}; allowed: ['account', 'operations']: {path}"
        )
    account = execution.get("account")
    if not isinstance(account, str) or not account.strip():
        raise RuntimeError(f"❌ {label}.execution.account must be a non-empty string: {path}")

    operations = execution.get("operations")
    if not isinstance(operations, dict) or not operations:
        raise RuntimeError(
            f"❌ {label}.execution_identity.operations must be a non-empty mapping keyed by "
            f"{list(CTL_STATE_OPERATIONS)}: {path}"
        )
    unknown_ops = sorted(set(operations) - set(CTL_STATE_OPERATIONS))
    if unknown_ops:
        raise RuntimeError(
            f"❌ {label}.execution_identity.operations has unknown operations {unknown_ops}; "
            f"allowed: {list(CTL_STATE_OPERATIONS)}: {path}"
        )

    cleaned_ops: dict[str, dict] = {}
    for operation, spec in operations.items():
        op_label = f"{label}.execution_identity.operations.{operation}"
        if not isinstance(spec, dict) or not spec:
            raise RuntimeError(f"❌ {op_label} must be a non-empty mapping: {path}")
        unknown_fields = sorted(set(spec) - {"role", "agreed_direct_credential_source_key"})
        if unknown_fields:
            raise RuntimeError(f"❌ {op_label} has unknown fields {unknown_fields}: {path}")
        role = spec.get("role")
        if not isinstance(role, str) or not role.strip():
            raise RuntimeError(f"❌ {op_label}.role must be a non-empty string: {path}")
        cleaned = {"role": role.strip()}
        direct_key = spec.get("agreed_direct_credential_source_key")
        if direct_key is not None:
            if not isinstance(direct_key, str) or not direct_key.strip():
                raise RuntimeError(
                    f"❌ {op_label}.agreed_direct_credential_source_key must be a non-empty string: {path}"
                )
            cleaned["agreed_direct_credential_source_key"] = direct_key.strip()
        cleaned_ops[operation] = cleaned

    return {"account": account.strip(), "operations": cleaned_ops}


def describe_target_execution_identity(execution: object) -> str | None:
    """Compact report label for execution identities: provider:account:role.

    Accepts either the keyed block (`{provider: {...}}`, one entry per provider)
    or one already-selected entry. Several providers render one line each, joined
    by `, ` — a target with two identities reads as two, not as a merged one.

    Provider-neutral: the engine only joins the declared fields; it does not
    interpret them.
    """
    if not isinstance(execution, dict) or not execution:
        return None

    def _entry(provider: str | None, entry: dict) -> str:
        parts = [str(provider or entry.get("provider") or "?"), str(entry.get("account") or "?")]
        roles = entry.get("roles")
        if isinstance(roles, dict) and roles:
            parts.append("/".join(f"{cls}={key}" for cls, key in sorted(roles.items())))
        elif entry.get("role"):
            parts.append(str(entry["role"]))
        return ":".join(parts)

    if all(isinstance(value, dict) for value in execution.values()):
        return ", ".join(_entry(provider, entry) for provider, entry in sorted(execution.items()))
    return _entry(None, execution)


def ctl_state_backend_operation_execution(
    entry: dict, operation: str, *, namespace_key: str, required: bool = True
) -> dict | None:
    """A ctl-state backend's execution narrowed to ONE operation (§Phase 53).

    Returns the account plus that operation's role and optional direct credential
    source; the adapter turns those into a credential. Replaces the old
    execution_identity_keys.<operation> indirection.
    """
    execution = entry.get("execution_identity") or {}
    spec = (execution.get("operations") or {}).get(operation)
    if spec is None:
        if required:
            raise RuntimeError(
                f"❌ ctl_state_backends.{namespace_key} declares no "
                f"execution_identity.operations.{operation}"
            )
        return None
    return {"account": execution.get("account"), "operation": operation, **spec}


def load_ctl_state_backends_cfg(ctl_cfg_root: Path) -> dict | None:
    """Load the optional ctl-state backend registry.

    Schema is ``ctl_state_backends``:
    {namespace: {selectors, provider, backend_type, bucket_name,
    bucket_region, execution}}. The backend declares its own `execution_identity:` block
    (§Phase 53) — one account, and a role per ctl-state OPERATION.
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
            allowed = {"provider", "backend_type", "bucket_name", "bucket_region", "execution_identity", "selectors"}
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
            execution = entry.get("execution_identity")
            if execution is not None:
                resolved["execution_identity"] = validate_ctl_state_backend_execution(
                    execution, label=f"{section_name}.{namespace_key}", path=path
                )
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
    providers: list[str] | tuple[str, ...] = (),
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
            providers=providers,
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
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
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
    probe_access_mode, probe_options = provider_inputs(
        str(entry["provider"]), execution_access_modes, provider_options
    )
    credential = adapter.resolve_state_backend_probe_credential(
        target_run,
        selection["provider_catalogs"],
        execution_context=selection["execution_context"],
        implementation_key=implementation_key,
        execution_access_mode=probe_access_mode,
        provider_options=probe_options,
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
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
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
        "execution_access_modes": execution_access_modes,
        "provider_options": provider_options,
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
    run_access_mode, adapter_options = provider_inputs(
        entry["provider"], config["execution_access_modes"], config["provider_options"]
    )
    operation_execution = ctl_state_backend_operation_execution(
        entry,
        "sync",
        namespace_key=config["namespace_key"],
        required=adapter.resolves_execution_identity(run_access_mode),
    )
    # Escalated target access may be needed to reach the target, but ctl-state
    # publication takes the provider's normal path as soon as it can.
    sync_access_mode = ctl_state_publication_access_mode(adapter, run_access_mode)
    try:
        object_keys, object_prefixes = _ctl_state_run_access_scope(config)
        credential = adapter.resolve_ctl_state_credential(
            operation_execution,
            config["ctl_cfg_root"],
            execution_context=config["execution_context"],
            implementation_key=config["provider_implementation_key"],
            operation="sync",
            bucket_name=config["bucket_name"],
            object_keys=object_keys,
            object_prefixes=object_prefixes,
            execution_access_mode=sync_access_mode,
            provider_options=adapter_options,
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
    execution_access_modes: dict[str, str] | None = None,
    provider_options: dict[str, str] | None = None,
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
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
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
        parent_run_id=metadata.get("parent_workflow_run_id"),
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
    components containing `=` are instance segments; key components never contain
    one (Q1j parse boundary). A workflow composition segment is `sha256=<digest>`,
    so it needs no special case."""
    if not isinstance(address, str) or not address:
        raise RuntimeError("❌ target instance address must be a non-empty string")
    parts = address.split("/")
    idx = len(parts)
    while idx > 0 and "=" in parts[idx - 1]:
        idx -= 1
    segments = parts[idx:]
    # The marker separates key from segments and belongs to NEITHER.
    if idx > 0 and parts[idx - 1] == INSTANCES_MARKER:
        idx -= 1
    if idx == 0:
        raise RuntimeError(f"❌ malformed target instance address: {address!r}")
    target_key = "/".join(parts[:idx])
    return normalize_result_name(target_key, label="target instance address"), segments


def ctl_state_target_address_prefix(action: str, address: str) -> str:
    target_key, segments = split_target_instance_address(address)
    return compose_state_relpath("target", target_key, segments).as_posix()


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
                "target_definition_sha256": canonical_sha256(
                    target_definition_document(target_run)
                ),
                **(
                    {"target_cfg_view_sha256": target_run["target_cfg_view_sha256"]}
                    if target_run.get("target_cfg_view_sha256") is not None
                    else {}
                ),
                "segments": segments,
                "address": target_instance_address(target_key, segments),
                "prefix": compose_state_relpath("target", target_key, segments
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
    digest = workflow_composition_sha256(
        addresses, [item.get("action") for item in target_specs]
    )
    key = normalize_result_name(selection["selection_key"], label="status workflow key")
    segments = [f"sha256={digest}"]
    definition_canonical = json.dumps(
        selection["workflow_cfg"], separators=(",", ":"), sort_keys=True
    )
    return {
        "kind": "workflow",
        "key": key,
        "segments": segments,
        "address": instance_address(key, segments),
        "prefix": compose_state_relpath("workflow", key, segments
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




def _freshness(pointer: dict | None, spec: dict) -> tuple[str, list[str]]:
    """Does the committed record still match what is declared?

    Meaningful only where a record exists; the caller supplies `none` otherwise.
    """
    if pointer is None:
        return "outdated", []
    reasons: list[str] = []
    if pointer.get("status") == "outdated" or pointer.get("outdated"):
        outdated = pointer.get("outdated") or {}
        reasons.append(str(outdated.get("reason") or "target marker"))
    for fact_key, reason in (
        ("target_definition_sha256", "target definition changed"),
        ("target_cfg_view_sha256", "target cfg view changed"),
    ):
        expected = spec.get(fact_key)
        if expected is not None and pointer.get(fact_key) != expected:
            reasons.append(reason)
    return ("outdated" if reasons else "up_to_date"), reasons


def _run_status(instance_dir: Path, group: str = "deployment") -> dict:
    """What the last (or current) run on this instance did.

    Returns `status`, its `reasons`, whether a mutation had begun, and — the part
    a row must not take from anywhere else — the RUN THAT PRODUCED THAT STATUS.

    `run_id`/`at` travel with `status` because they describe one event. Reading
    the status from a slot and the timestamp from the committed pointer mixes two
    different runs: while the newest run succeeds they agree, so it looks correct;
    the moment a run FAILS after a success the row reports the failure's status
    beside the success's time and id, and a reader chasing the failure opens the
    wrong run (observed 2026-08-03).
    """
    for state, verdict, reason in (
        ("in_progress", "running", in_progress_verdict_reason),
        ("failed", "failed", failed_verdict_reason),
    ):
        slot = read_instance_state_slot(instance_dir, state, group)
        if slot is not None:
            return {
                "status": verdict,
                "reasons": [reason(slot)],
                "mutation_started": slot.get("mutation_started") is True,
                "run_id": slot.get("run_id"),
                "at": slot.get("updated_at"),
                "action": slot.get("action"),
                "parent_workflow_instance": slot.get("parent_workflow_instance_address"),
                "parent_workflow_run_id": slot.get("parent_workflow_run_id"),
            }
    # No slot AND no committed pointer means nothing ever ran here. `passed`
    # would be a claim of success nobody made, so the caller gets None and omits
    # the group entirely — absence stays absence.
    pointer = read_committed_pointer(instance_dir, group)
    if pointer is None:
        return {"status": None, "reasons": [], "mutation_started": False,
                "run_id": None, "at": None, "action": None,
                "parent_workflow_instance": None, "parent_workflow_run_id": None}
    return {
        "status": "passed",
        "reasons": [],
        "mutation_started": False,
        "run_id": pointer.get("run_id"),
        "at": pointer.get("committed_at"),
        "action": pointer.get("action"),
        "parent_workflow_instance": pointer.get("parent_workflow_instance_address"),
        "parent_workflow_run_id": pointer.get("parent_workflow_run_id"),
    }


def _mutating_run_status(
    namespace_root: Path, kind: str, key: str, segments: list[str]
) -> tuple[str | None, list[str], bool, str | None]:
    """The live run axes of a deployment instance.

    §Phase 73: provision and destroy now share one instance directory and one
    `committed/deployment.yaml`, so there are no longer two direction prefixes to
    merge — the slot and the pointer are read from one place, and the action comes
    from whichever record is there.
    """
    instance_dir = namespace_root / compose_state_relpath(kind, key, segments)
    for state, status, describe in (
        ("in_progress", "running", in_progress_verdict_reason),
        ("failed", "failed", failed_verdict_reason),
    ):
        slot = read_instance_state_slot(instance_dir, state, "deployment")
        if slot is not None:
            return (
                status,
                [describe(slot)],
                slot.get("mutation_started") is True,
                slot.get("action"),
            )
    pointer = read_committed_pointer(instance_dir, "deployment")
    if pointer is None:
        return None, [], False, None
    return "passed", [], False, pointer.get("action")

def compute_target_instance_status(
    namespace_root: Path, action: str, spec: dict
) -> dict:
    """Status of one target instance, on the two axes a row carries.

    §Phase 73: `state` and `action` are gone. `provisioned`/`destroyed` asserted
    what exists in the cloud, which ctl never observes — a destroy run directly in
    the repo empties the tool's own state while ctl kept reporting `provisioned`.
    A row reports what ctl's own runs did: whether one is live or broken, and
    whether the published result still matches its inputs.
    """
    group = action_group(action)
    instance_dir = namespace_root / compose_state_relpath(
        "target", spec["key"], spec["segments"]
    )
    pointer = read_committed_pointer(instance_dir, group)
    run = _run_status(instance_dir, group)
    status, reasons, mutation_started = run["status"], run["reasons"], run["mutation_started"]

    result: dict = {
        "kind": "target",
        "key": spec["key"],
        "address": spec["address"],
    }
    if status is not None:
        result["status"] = status
    # Freshness applies to a published result only. An interrupted run changed
    # resources and committed nothing, so its pointer describes nothing for inputs
    # to have moved away from; `up_to_date` would be false and `outdated`
    # understates it.
    # §Phase 73 removed `state` (`provisioned`/`destroyed`) because that asserts
    # what exists in the cloud, which ctl never observes. The ACTION is a
    # different fact and one ctl owns outright. Without it the direction is
    # inferred from `freshness` being ABSENT, which is an implicit signal.
    #
    # It comes from the run that produced `status`, NOT from the committed
    # pointer: a failed destroy after a successful provision reported
    # `status: failed` beside `last_action: provision`, which reads as "the
    # provision failed" when the provision succeeded and the DESTROY failed.
    # Same rule as `at` and `run_id` — one row, one run.
    if run["action"]:
        result["last_action"] = run["action"]
    if group == "deployment" and pointer and not mutation_started:
        if str(pointer.get("action")) != "destroy":
            freshness, freshness_reasons = _freshness(pointer, spec)
            result["freshness"] = freshness
            reasons = reasons + freshness_reasons

    # The workflow INSTANCE this run belonged to, closing the loop the workflow
    # row opens: a workflow instance names the target instances it drove, and a
    # target instance names the workflow instance that drove it. Absent for a
    # target run invoked directly, which is a real distinction and not a gap.
    if run["parent_workflow_instance"]:
        result["parent_workflow"] = qualified_address(
            "workflow", run["parent_workflow_instance"]
        )
    elif run["parent_workflow_run_id"]:
        # An older run recorded only the id. It goes under its own key: one field
        # meaning "an address, or else an id" makes every reader branch on shape.
        result["parent_workflow_run_id"] = run["parent_workflow_run_id"]
    # `run_id`/`at` come from whichever run produced `status`, never from the
    # committed pointer when the two are different runs.
    if run["run_id"]:
        result["run_id"] = run["run_id"]
    if run["at"]:
        result["at"] = run["at"]
    # The last PUBLISHED result is a separate fact, and only worth stating when
    # it is not the run above — otherwise it would repeat `at` on every row.
    if pointer and pointer.get("run_id") != run["run_id"]:
        result["committed_at"] = pointer.get("committed_at")
        result["committed_run_id"] = pointer.get("run_id")
    if reasons:
        result["reasons"] = reasons
    return result


def compute_workflow_instance_status(
    namespace_root: Path, action: str, spec: dict
) -> dict:
    """Status of one workflow instance, rolled up from its members.

    §Phase 73: a composition reports `status` and `freshness` and nothing else. It
    holds no `state`, because once members carry their own action a composition can
    hold a destroy member and a provision member at once and no single word is true
    of it; and no `action`, for the same reason. Both surviving axes roll up without
    reference to direction — running or failed when ANY member is, outdated as soon
    as ANY member is — which is why one row shape serves both kinds.
    """
    group = action_group(action)
    workflow_dir = namespace_root / compose_state_relpath(
        "workflow", spec["key"], spec["segments"]
    )
    pointer = read_committed_pointer(workflow_dir, group)
    run = _run_status(workflow_dir, group)
    status, reasons, mutation_started = run["status"], run["reasons"], run["mutation_started"]

    children: list[dict] = []
    recorded = {
        item.get("address"): item
        for item in ((pointer or {}).get("child_revisions") or [])
        if isinstance(item, dict)
    }
    drift: list[str] = []
    for target_spec in spec["target_specs"]:
        child = compute_target_instance_status(namespace_root, action, target_spec)
        child_pointer = read_committed_pointer(
            namespace_root
            / compose_state_relpath("target", target_spec["key"], target_spec["segments"]),
            group,
        )
        expected = recorded.get(target_spec["address"])
        if child.get("freshness") == "outdated":
            drift.append(f"{target_spec['address']}: outdated")
        elif expected is None:
            drift.append(f"{target_spec['address']}: not recorded by workflow")
        elif (
            expected.get("run_id") != (child_pointer or {}).get("run_id")
            or expected.get("snapshot_sha256")
            != (child_pointer or {}).get("snapshot_sha256")
        ):
            drift.append(f"{target_spec['address']}: committed revision changed")
        children.append(child)

    if pointer is not None:
        if pointer.get("workflow_definition_sha256") != spec[
            "workflow_definition_sha256"
        ]:
            drift.append("workflow definition changed")
        pointer_addresses = [
            str(item.get("address"))
            for item in (pointer.get("child_revisions") or [])
            if isinstance(item, dict)
        ]
        if pointer_addresses != [item["address"] for item in spec["target_specs"]]:
            drift.append("workflow target order or set changed")

    # status: a live or broken child makes the composition live or broken.
    if status in (None, "passed"):
        running = [c for c in children if c.get("status") == "running"]
        failed = [c for c in children if c.get("status") == "failed"]
        if running:
            status = "running"
            reasons = [f"{c['address']}: running" for c in running]
        elif failed:
            status = "failed"
            reasons = [f"{c['address']}: failed" for c in failed]

    result: dict = {
        "kind": "workflow",
        "key": spec["key"],
        "address": spec["address"],
    }
    if status is not None:
        result["status"] = status
    if group == "deployment" and pointer is not None and not mutation_started:
        own_freshness, own_reasons = _freshness(pointer, spec)
        result["freshness"] = (
            "outdated" if (drift or own_freshness == "outdated") else own_freshness
        )
        reasons = reasons + own_reasons + drift

    if run["run_id"]:
        result["run_id"] = run["run_id"]
    if run["at"]:
        result["at"] = run["at"]
    if pointer and pointer.get("run_id") != run["run_id"]:
        result["committed_at"] = pointer.get("committed_at")
        result["committed_run_id"] = pointer.get("run_id")
    if reasons:
        result["reasons"] = list(dict.fromkeys(reasons))
    result["children"] = children
    return result


def _arm_ctl_state_operation(
    ctl_cfg_root: Path,
    execution_context: dict[str, object],
    ctl_state_local_root: Path,
    *,
    operation: str,
    provider_implementation_key: str,
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
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
    run_access_mode, adapter_options = provider_inputs(
        entry["provider"], execution_access_modes, provider_options
    )
    operation_execution = ctl_state_backend_operation_execution(
        entry,
        operation,
        namespace_key=namespace_key,
        required=adapter.resolves_execution_identity(run_access_mode),
    )
    operation_access_mode = ctl_state_publication_access_mode(adapter, run_access_mode)
    credential = adapter.resolve_ctl_state_credential(
        operation_execution,
        ctl_cfg_root,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        operation=operation,
        bucket_name=bucket_name,
        object_keys=object_keys,
        object_prefixes=object_prefixes,
        execution_access_mode=operation_access_mode,
        provider_options=adapter_options,
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
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
):
    return _arm_ctl_state_operation(
        ctl_cfg_root,
        selection["execution_context"],
        ctl_state_local_root,
        operation="read",
        provider_implementation_key=provider_implementation_key,
        credential_refresh_modes=credential_refresh_modes,
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
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
        providers=getattr(args, "providers", ()),
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
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
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
        **instances,
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
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
    )
    writer.put_object(cache_key, cache_path)
    report = {
        "operation": "status-sweep",
        "namespace": namespace_key,
        **instances,
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


def forget_selection(
    namespace_root: Path,
    older_than: str,
    addresses: list[str],
) -> list[dict]:
    """Resolve the two filters to instance directories.

    Both filters are always supplied — neither defaults — so a forget always
    states both dimensions and nothing is removed on a filter the caller did not
    write down. `any` and `all` are the explicit wide values.

    An ADDRESS may name a template or an instance: depth decides scope, so
    `.../env/core/baseline` selects every instance under it and
    `.../instances/env.type=dev/...` selects one.
    """
    cutoff = None
    if older_than != "any":
        try:
            cutoff = datetime.fromisoformat(older_than)
        except ValueError as error:
            raise RuntimeError("❌ --older-than must be `any` or ISO-8601") from error
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)

    wanted = None if addresses == ["all"] else [a.strip("/") for a in addresses]
    selected: list[dict] = []
    for pointer_path in sorted(Path(namespace_root).rglob("committed/*.yaml")):
        instance_dir = pointer_path.parent
        rel = instance_dir.relative_to(namespace_root).as_posix()
        if wanted is not None and not any(
            rel == a or rel.startswith(a + "/") for a in wanted
        ):
            continue
        pointer = read_committed_pointer(instance_dir) or {}
        when = pointer.get("committed_at")
        if cutoff is not None:
            if not when:
                continue
            try:
                stamp = datetime.fromisoformat(str(when).replace("Z", "+00:00"))
            except ValueError:
                continue
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            if stamp >= cutoff:
                continue
        selected.append({"address": rel, "dir": instance_dir, "at": when})
    return selected


def forget_guard(
    namespace_root: Path,
    rel: str,
    *,
    accept_orphans: bool,
    cascade: bool,
    referenced_by: dict[str, set[str]],
) -> str | None:
    """Why this instance may not be forgotten, or None.

    Read straight off the status axes rather than joined from a collapsed
    summary — which is the whole reason `state` and `status` are separate fields.
    """
    instance_dir = namespace_root / rel
    if read_instance_state_slot(instance_dir, "in_progress") is not None:
        # No override: the run republishes the record moments later, so forgetting
        # it now would look like it worked and would not have.
        return "a run is in progress on it"
    parsed = parse_state_relpath(namespace_root, instance_dir)
    if parsed and parsed["kind"] == "target":
        computed = compute_target_instance_status(
            namespace_root,
            "provision",
            {
                "kind": "target",
                "key": parsed["key"],
                "segments": list(parsed["instance_segments"]),
                "address": rel,
                "prefix": compose_state_relpath("target", parsed["key"], list(parsed["instance_segments"])
                ).as_posix(),
            },
        )
        state = computed.get("state")
        if state in ("provisioned", "partial") and not accept_orphans:
            return f"state is {state}; pass --accept-orphaned-resources"
    referrers = referenced_by.get(rel, set())
    if referrers and not cascade:
        return (
            "referenced by retained workflow runs "
            f"({', '.join(sorted(referrers))}); pass --cascade"
        )
    return None


def workflow_references(namespace_root: Path) -> dict[str, set[str]]:
    """Which retained workflow runs point at each instance address.

    Forgetting a record a workflow still names leaves that workflow describing a
    member that no longer exists, so it is refused unless the caller says
    `--cascade`.
    """
    references: dict[str, set[str]] = {}
    for pointer_path in Path(namespace_root).rglob("committed/*.yaml"):
        pointer = read_committed_pointer(pointer_path.parent) or {}
        for child in pointer.get("child_revisions") or []:
            if not isinstance(child, dict) or not child.get("address"):
                continue
            key, segments = split_target_instance_address(str(child["address"]))
            rel = compose_state_relpath("target", key, segments).as_posix()
            references.setdefault(rel, set()).add(
                pointer_path.parent.relative_to(namespace_root).as_posix()
            )
    return references


def run_ctl_state_forget(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    *,
    provider_implementation_key: str = "local",
) -> dict:
    """Remove ctl-state records selected by AGE and ADDRESS.

    A dry run by default, so the safe form is also the discovery form: it lists
    what would go and removes nothing. `--apply` is the only flag that means
    "do it".

    A record missing in one scope is a SKIP, never a failure and never silent.
    The scopes diverge legitimately — a force-skipped run is local-only and
    permanently absent from remote, a record made on another machine is
    remote-only from here — so erroring on absence would make `--scope both`
    unusable in exactly the case it exists for.
    """
    if not ctl_allows_ctl_state_forget(ctl_cfg_root, args.ctl_profile):
        raise RuntimeError(
            f"❌ ctl profile {args.ctl_profile!r} does not grant allow_ctl_state_forget"
        )
    wide = args.older_than == "any" and args.forget_address == ["all"]
    if wide and not getattr(args, "accept_forget_everything", False):
        raise RuntimeError(
            "❌ --older-than any with --address all forgets EVERY record; "
            "pass --accept-forget-everything"
        )

    context = build_execution_context(
        ctl_cfg_root,
        action=args.action,
        ctl_profile=args.ctl_profile,
        execution_params=args.execution_params,
        providers=getattr(args, "providers", ()),
        execution_runtime_mode=args.execution_runtime_mode,
    )
    namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, context)
    scope = getattr(args, "forget_scope", None) or "both"
    apply = getattr(args, "apply_forget", False)

    results: dict[str, dict[str, str]] = {}
    roots: dict[str, Path] = {}
    if scope in ("local", "both"):
        roots["local"] = Path(args.ctl_state_local_root) / namespace_key
    scratch = None
    if scope in ("remote", "both"):
        scratch = tempfile.TemporaryDirectory(prefix="atlas-ctl-state-forget-")
        _, remote_root, syncer = _arm_ctl_state_operation(
            ctl_cfg_root,
            context,
            Path(scratch.name),
            operation="maintenance",
            provider_implementation_key=provider_implementation_key,
            execution_access_modes=args.execution_access_modes,
            provider_options=args.provider_options,
        )
        hydrate_ctl_state_index(syncer)
        roots["remote"] = remote_root

    try:
        agree_active = getattr(args, "accept_orphaned_resources", False)
        cascade = getattr(args, "cascade", False)
        refused = 0
        for where, root in roots.items():
            if not root.is_dir():
                continue
            referenced_by = workflow_references(root)
            for item in forget_selection(root, args.older_than, args.forget_address):
                row = results.setdefault(item["address"], {})
                refusal = forget_guard(
                    root,
                    item["address"],
                    accept_orphans=agree_active,
                    cascade=cascade,
                    referenced_by=referenced_by,
                )
                if refusal:
                    row[where] = f"refused — {refusal}"
                    refused += 1
                    continue
                row[where] = "removed" if apply else "would remove"
                if apply:
                    shutil.rmtree(item["dir"], ignore_errors=True)
        # An address the caller named that no scope held is stated, not inferred
        # from a count: "removed nothing" and "was not there" must not look alike.
        if args.forget_address != ["all"]:
            for named in args.forget_address:
                row = results.setdefault(named.strip("/"), {})
                for where in roots:
                    row.setdefault(where, "not present — skipped")
    finally:
        if scratch is not None:
            scratch.cleanup()

    report = {
        "operation": "forget",
        "namespace": namespace_key,
        "scope": scope,
        "older_than": args.older_than,
        "address": args.forget_address,
        "applied": apply,
        **({"refused": refused} if refused else {}),
        "instances": dict(sorted(results.items())),
    }
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


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
        providers=getattr(args, "providers", ()),
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
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
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
    for pointer_path in namespace_root.rglob("committed/*.yaml"):
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
        execution_access_modes=args.execution_access_modes,
        provider_options=args.provider_options,
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
        providers=getattr(args, "providers", ()),
        execution_access_modes=args.execution_access_modes,
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
    if args.maintenance_action == "forget":
        return run_ctl_state_forget(
            ctl_cfg_root, args, provider_implementation_key=provider_implementation_key
        )
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



def _targeted_workflow_status(namespace_root: Path, spec: dict) -> dict:
    """§Phase 73: a workflow owns execution, so a targeted query reads its LAST RUN.

    Reading a committed pointer here returned an empty row once workflows stopped
    publishing one — the query answered nothing rather than failing.
    """
    result = {"kind": "workflow", "key": spec["key"], "address": spec["address"]}
    last_run = workflow_last_run(
        namespace_root, spec["key"], spec.get("segments") or []
    )
    if last_run:
        result["last_run"] = last_run
    return result


def _compute_status_results(
    namespace_root: Path, action: str, labels: list[str], specs: list[dict]
) -> list[dict]:
    results = []
    for label, spec in zip(labels, specs):
        computed = (
            compute_target_instance_status(namespace_root, action, spec)
            if spec["kind"] == "target"
            else _targeted_workflow_status(namespace_root, spec)
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
        providers=getattr(args, "providers", ()),
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
        providers=getattr(args, "providers", ()),
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
                    provider_options=args.provider_options,
                    execution_access_modes=args.execution_access_modes,
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
            provider_options=args.provider_options,
            execution_access_modes=args.execution_access_modes,
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
                execution_access_modes=args.execution_access_modes,
                provider_options=args.provider_options,
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
                            compose_state_relpath("target",
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
        # One roll-up PER AXIS. A single summary would reintroduce exactly what
        # the axes exist to remove: a live child hiding a stale one.
        "status": (
            "running"
            if any(item["status"] == "running" for item in results)
            else "failed"
            if any(item["status"] == "failed" for item in results)
            else "passed"
        ),
        **(
            {
                "state": (
                    "partial"
                    if any(item.get("state") == "partial" for item in results)
                    else "destroyed"
                    if all(
                        item.get("state") == "destroyed"
                        for item in results
                        if item.get("state")
                    )
                    else "provisioned"
                ),
                "freshness": (
                    "outdated"
                    if any(item.get("freshness") == "outdated" for item in results)
                    else "current"
                ),
            }
            if any(item.get("state") for item in results)
            else {}
        ),
        "results": results,
    }
    print(yaml.safe_dump(report, sort_keys=False).rstrip())
    return report


# The action classes a status row is grouped by. `provision` and `destroy` share
# one group because they are two directions of the SAME state — a destroy is not
# a separate thing that happened to the instance, it is the instance ending.
# One representative action per group, for the compute functions that still take
# an action and derive the group from it.
STATUS_GROUP_ACTION = {
    "plan": "plan",
    "readonly": "readonly",
    "deployment": "provision",
    "maintenance": "maintenance",
}
STATUS_GROUPS: dict[str, tuple[str, ...]] = {
    "plan": ("plan",),
    "readonly": ("readonly",),
    "deployment": MUTATING_ACTIONS,
}


# §Phase 73: one shape for every kind. `status` first because it is the fact a
# reader acts on, `freshness` second because it qualifies a published result, `at`
# last because it dates the record rather than describing it.
# `committed_at`/`committed_run_id` stay OUT of the flat row: a field that
# appears only when a run failed after a success makes the common row harder to
# scan, and two timestamps invite misreading which is which. They remain on the
# detailed row, where a reader is already investigating.
AXIS_ORDER = ("status", "last_action", "freshness", "at",
              "parent_workflow", "parent_workflow_run_id")


def order_axes(axes: dict[str, str]) -> dict[str, str]:
    """One canonical order for every emitted group: status, freshness, at."""
    return {k: axes[k] for k in AXIS_ORDER if k in axes}


def _axis_row(computed: dict) -> dict[str, str]:
    """The axes only, for the flat namespace map — no reasons, no children.

    A row carrying no `status` describes nothing that happened, so it collapses to
    empty and its group is omitted. Freshness alone is not a fact about a run.
    """
    row = {
        axis: computed[axis]
        for axis in ("status", "last_action", "freshness")
        if computed.get(axis)
    }
    if not row.get("status"):
        return {}
    for field in ("at", "parent_workflow", "parent_workflow_run_id"):
        if computed.get(field):
            row[field] = computed[field]
    return order_axes(row)


def workflow_last_run(
    namespace_root: Path, key: str, segments: list[str] | None = None
) -> dict | None:
    """The most recent run of one workflow INSTANCE — its record, not its state.

    §Phase 73: a workflow owns execution, so there is no pointer to read. The row
    is the last run: what it did, what selected its members, and which members it
    ran with.

    §Phase 78: per INSTANCE. One key fanned across environments used to report a
    single row — whichever child finished last — so "did this succeed in test?"
    was unanswerable from the workflow.
    """
    runs_dir = (
        namespace_root / compose_state_relpath("workflow", key, segments) / "runs"
    )
    if not runs_dir.is_dir():
        return None
    records = []
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        metadata = load_run_metadata(run_dir)
        if not metadata.get("updated_at"):
            continue
        records.append(metadata)
    if not records:
        return None
    latest = max(records, key=lambda m: str(m.get("updated_at") or ""))
    row: dict = {"status": _run_conclusion(latest), "at": latest.get("updated_at")}
    # The matched member's own selector block, copied verbatim: it points back at
    # the cfg that produced this run, and nothing depends on the engine knowing a
    # field called "operation". A member that matched with none omits it.
    if latest.get("member_selectors"):
        row["selectors"] = latest["member_selectors"]
    if latest.get("default_action"):
        row["default_action"] = latest["default_action"]
    target_instances = latest.get("target_instances")
    if target_instances:
        # Qualified for the same reason the parent link is: these point at the
        # TARGET rows in this map, and an address a reader can paste back into a
        # query beats one they have to prefix by hand.
        row["target_instances"] = [
            qualified_address("target", entry)
            if isinstance(entry, str)
            else {**entry, "instance": qualified_address("target", entry["instance"])}
            for entry in target_instances
        ]
    return row


def _run_conclusion(metadata: dict) -> str:
    """A run's outcome, in the vocabulary a target row already uses."""
    status = str(metadata.get("status") or "")
    if status == "ok":
        return "passed"
    if status == "in_progress":
        return "running"
    return "failed" if status else "passed"


def _recorded_target_specs(pointer: dict | None) -> list[dict]:
    """The members a workflow pointer recorded, as target specs."""
    specs: list[dict] = []
    for item in (pointer or {}).get("child_revisions") or []:
        if not isinstance(item, dict):
            continue
        child_key, child_segments = split_target_instance_address(
            str(item.get("address"))
        )
        specs.append(
            {
                "kind": "target",
                "key": child_key,
                "segments": child_segments,
                "address": str(item.get("address")),
            }
        )
    return specs


def _place_instance(
    rows: dict, kind: str, key: str, segments: list[str], groups: dict
) -> None:
    """File one instance under its TEMPLATE, mirroring the state dir layout.

    `kind -> template -> instances -> <segments> -> groups`, so every
    materialization of one declared key reads as a single block instead of
    scattered rows sorted by address.

    A singleton has no segments and therefore no instance to name: its groups sit
    directly under the template, exactly as `compose_state_relpath` omits the
    `instances/` layer on disk.
    """
    template = rows[kind].setdefault(key, {})
    if not segments:
        template.update(groups)
        return
    template.setdefault(INSTANCES_MARKER, {})["/".join(segments)] = groups


def compute_namespace_status_map(
    namespace_root: Path,
) -> dict[str, dict[str, dict[str, dict[str, str]]]]:
    """Every target and workflow instance under the namespace root, one row per
    published GROUP.

    §Phase 73: the tree is partitioned by group rather than by action, so an
    instance appears once and each of its groups reports independently:

        status      running | passed | failed
        freshness   up_to_date | outdated     (a published deployment only)
        at          when the record was published

    `status` comes from the live run slots — a success clears the failed slot, so
    `failed` cannot outlive the run that caused it — and `freshness` from the
    published pointer. Fan-outs own no state and never appear."""
    namespace_root = Path(namespace_root)
    if not namespace_root.is_dir():
        return {}
    targets: set[tuple[str, tuple[str, ...]]] = set()
    workflows: set[tuple[str, tuple[str, ...]]] = set()
    # An instance is discovered by anything it has PUBLISHED or is DOING. Scanning
    # only for committed pointers hid a first-ever run entirely: it has written a
    # slot and no pointer, so `--all` reported an empty namespace while a run was
    # in flight.
    discovered: list[Path] = [
        pointer.parent.parent for pointer in namespace_root.rglob("committed/*.yaml")
    ]
    for state in ("in_progress", "failed"):
        discovered += [
            slot.parent.parent.parent
            for slot in namespace_root.rglob(f"{state}/*/STATUS.yaml")
        ]
    for instance_dir in discovered:
        parsed = parse_state_relpath(namespace_root, instance_dir)
        if parsed is None or parsed["kind"] != "target":
            continue
        targets.add((parsed["key"], tuple(parsed["instance_segments"])))
    # §Phase 73: a workflow is discovered by its RUNS. It publishes no pointer, so
    # there is nothing else to find it by. §Phase 78: the parsed segments are KEPT,
    # so a key fanned across environments yields one row per environment instead of
    # collapsing to whichever child ran last.
    workflow_root = namespace_root / "workflow"
    if workflow_root.is_dir():
        for runs_dir in workflow_root.rglob("runs"):
            parsed = parse_state_relpath(namespace_root, runs_dir.parent)
            if parsed is not None and parsed["kind"] == "workflow":
                workflows.add((parsed["key"], tuple(parsed["instance_segments"])))
    rows: dict[str, dict[str, dict[str, dict[str, str]]]] = {
        kind: {} for kind in RESULT_KINDS
    }
    for key, seg in targets:
        segments = list(seg)
        address = target_instance_address(key, segments)
        groups: dict[str, dict[str, str]] = {}
        spec = {
            "kind": "target",
            "key": key,
            "segments": segments,
            "address": address,
        }
        for group in RESULT_GROUPS:
            axes = _axis_row(
                compute_target_instance_status(
                    namespace_root, STATUS_GROUP_ACTION[group], spec
                )
            )
            # An empty group means no run of that class ever touched this
            # instance — it is omitted rather than reported as anything.
            if axes:
                groups[group] = axes
        if groups:
            _place_instance(rows, "target", key, segments, groups)
    for key, seg in workflows:
        segments = list(seg)
        last_run = workflow_last_run(namespace_root, key, segments)
        if not last_run:
            continue
        if segments:
            _place_instance(rows, "workflow", key, segments, {"last_run": last_run})
        else:
            rows["workflow"][key] = {"last_run": last_run}
    # Kind is the OUTER key, so a reader sees where the workflows are and where
    # the targets are without parsing a prefix off every address.
    return {
        kind: dict(sorted(instances.items()))
        for kind, instances in rows.items()
        if instances
    }


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
        action=ExecutionParamsAction,
        default=[],
        metavar="KEY=VALUE[,KEY=VALUE...]",
        help="Execution params key=value; comma-separated and/or repeatable. Namespace selectors "
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
        choices=list(KNOWN_ACTIONS),
        help="which status GROUP a targeted target query reports (an action names "
        "its group: provision/destroy -> deployment); ignored by --all",
    )
    # Filters, not selectors: they narrow what --all PRINTS. A breadth argument
    # names ONE instance, so a filter needs its own word rather than a valueless
    # --target/--workflow.
    query_group.add_argument(
        "--kind",
        dest="kinds",
        default=None,
        type=parse_comma_list,
        metavar="KIND[,KIND...]",
        help="--all only: show these row kinds (target, workflow); default both. "
        "Same vocabulary as the row prefix and as --prune-kind",
    )
    query_group.add_argument(
        "--structure",
        default=None,
        choices=("nested", "flat"),
        help="--all only: `nested` keeps the kind/template/instance tree; `flat` "
        "emits one row per group carrying its own address, the only shape a "
        "strictly chronological order can take. Required with --all — the shape "
        "is a choice, not a default to inherit",
    )
    query_group.add_argument(
        "--sort",
        default="address",
        metavar="FIELD[:asc|desc]",
        help="--all only: order by `time` or `address`; direction defaults to asc. "
        "Nested sorts on two levels — templates by their newest instance, "
        "instances by their newest group",
    )
    query_group.add_argument(
        "--group",
        dest="groups",
        default=None,
        type=parse_comma_list,
        metavar="GROUP[,GROUP...]",
        help="--all only: show these status groups (plan, readonly, deployment); "
        "default all. A row whose every group is filtered out is dropped",
    )
    # 3) scope — where to read, and the local dir / dev substitute
    scope_group.add_argument(
        "--hydrate-to",
        dest="hydrate_to",
        default=None,
        metavar="DIR",
        help="--scope remote only: keep the hydrated namespace here instead of "
        "discarding it. The result is directly usable as --ctl-state-local-root "
        "— a full copy of the namespace IS a valid local root",
    )
    scope_group.add_argument(
        "--scope",
        required=True,
        choices=("local", "remote"),
        help="'local' reads the dir offline (no remote ctl-state backend, no "
        "credentials) — the only way to see force-skipped runs; 'remote' is the "
        "authoritative ctl-state backend view (hydrated into an auto temp, discarded)",
    )
    scope_group.add_argument(
        "--ctl-state-local-root",
        default=None,
        metavar="DIR",
        help="required for --scope local (the ctl-state tree to read); not "
        "valid for --scope remote (remote uses an auto temp)",
    )
    scope_group.add_argument(
        "--provider-options",
        dest="provider_options",
        action=ProviderOptionsAction,
        default={},
        metavar="PROVIDER.KEY=VALUE[,...]",
        help="Provider-namespaced options for --scope remote, comma-separated "
        "and/or repeatable — including a substitute credential when no ctl-state "
        "read chain exists yet, which makes the read skip identity resolution. "
        "Run `ctl.py providers` for each provider's option keys.",
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
    if getattr(args, "hydrate_to", None) and args.scope != "remote":
        raise RuntimeError("❌ --hydrate-to keeps a REMOTE hydration; use --scope remote")
    if args.all and not getattr(args, "structure", None):
        raise RuntimeError("❌ --all requires --structure nested|flat")
    if getattr(args, "structure", None) and not args.all:
        raise RuntimeError("❌ --structure shapes --all; a targeted query names one instance")
    if args.all:
        parse_sort(args.sort)
    for flag, dest, allowed in (
        ("--kind", "kinds", RESULT_KINDS),
        ("--group", "groups", tuple(STATUS_GROUPS)),
    ):
        selected = getattr(args, dest, None)
        if selected is None:
            continue
        if not args.all:
            raise RuntimeError(f"❌ {flag} filters --all; a targeted query names one instance")
        unknown = sorted(set(selected) - set(allowed))
        if unknown:
            raise RuntimeError(
                f"❌ {flag} got unknown {', '.join(unknown)}; expected one of {', '.join(allowed)}"
            )
    if args.scope == "local":
        if not args.ctl_state_local_root:
            raise RuntimeError("❌ --scope local requires --ctl-state-local-root")
        if args.provider_options:
            raise RuntimeError(
                "❌ --provider-options is not valid with --scope local "
                "(local reads the dir — no remote ctl-state backend, no credentials)"
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
    # A read has ONE operation against ONE provider — the namespace selector
    # names it — so the run-level per-provider inputs collapse to that provider.
    # The mode is the provider's normal path, unless its options imply another
    # (a substitute credential IS the request to skip identity resolution). The
    # engine picks no mode name here; the adapter does.
    read_provider = args.execution_params.get("provider")
    if args.scope == "remote" and not read_provider:
        raise RuntimeError(
            "❌ --scope remote requires --execution-params provider=<name>: the read "
            "acquires a credential, and only the provider knows how"
        )
    # scope local reads the dir offline: no credential is acquired, so the
    # provider (if given as a query selector) implies no options and no mode.
    args.providers = [read_provider] if read_provider and args.scope == "remote" else []
    args.execution_access_modes = {}
    if args.providers:
        validate_provider_options(args.provider_options, args.providers)
        adapter = get_provider_adapter(read_provider)
        adapter_options = provider_options_for(args.provider_options, read_provider)
        args.execution_access_modes = {
            read_provider: (
                adapter.execution_access_mode_from_options(adapter_options)
                or adapter.normal_execution_access_mode()
            )
        }
    # inert for a read (no box is built) but required by the shared context
    # builders / selection resolvers.
    args.execution_runtime_mode = "local"
    args.ctl_variants = []
    if not args.all and args.action is None:
        raise RuntimeError(
            "❌ a targeted status (--target/--workflow/--fan-out) requires "
            "--action provision|destroy"
        )


SORT_FIELDS = ("time", "address")


def parse_sort(raw: str) -> tuple[str, bool]:
    """`<field>[:asc|desc]` -> (field, descending). Direction defaults to asc."""
    field, _, direction = raw.partition(":")
    if field not in SORT_FIELDS:
        raise RuntimeError(
            f"❌ --sort field {field!r} unknown; expected one of {', '.join(SORT_FIELDS)}"
        )
    if direction not in ("", "asc", "desc"):
        raise RuntimeError(f"❌ --sort direction {direction!r} must be asc or desc")
    return field, direction == "desc"


def _newest(groups: dict) -> str:
    """The latest `at` across a row's groups; '' when none carries one."""
    return max((g.get("at") or "" for g in groups.values()), default="")


def structure_status_map(instances: dict, structure: str, sort: str) -> dict:
    """Order and shape a status map.

    `nested` keeps the kind -> template -> instances tree and sorts on TWO levels:
    templates by their newest instance, instances by their newest group. A tree
    cannot express a globally chronological order — grouping and global ordering
    are in conflict — so `flat` exists for that: a LIST of one row per group,
    each carrying its own address, which can be ordered strictly by time.
    """
    field, descending = parse_sort(sort)

    if structure == "flat":
        # ONE list. Splitting by kind would break global chronological order for
        # the same reason template nesting does, and the kind is already a path
        # segment of the address, so it needs no field of its own.
        rows = []
        for kind, templates in instances.items():
            for template, body in templates.items():
                bodies = (
                    list(body[INSTANCES_MARKER].items())
                    if INSTANCES_MARKER in body
                    else [(None, body)]
                )
                for segs, row in bodies:
                    address = "/".join(
                        [kind, instance_address(template, segs.split("/") if segs else [])]
                    )
                    for group, axes in row.items():
                        rows.append({"address": address, "group": group, **axes})
        key = (
            (lambda r: (r.get("at") or ""))
            if field == "time"
            else (lambda r: (r["address"], r["group"]))
        )
        return {"instances": sorted(rows, key=key, reverse=descending)}

    ordered: dict = {}
    for kind, templates in instances.items():
        def template_key(item):
            template, body = item
            if field == "address":
                return template
            rows = (
                body[INSTANCES_MARKER].values()
                if INSTANCES_MARKER in body
                else [body]
            )
            return max((_newest(r) for r in rows), default="")

        kind_out: dict = {}
        for template, body in sorted(templates.items(), key=template_key, reverse=descending):
            if INSTANCES_MARKER in body:
                rows = body[INSTANCES_MARKER]
                instance_key = (
                    (lambda kv: _newest(kv[1])) if field == "time" else (lambda kv: kv[0])
                )
                kind_out[template] = {
                    INSTANCES_MARKER: dict(
                        sorted(rows.items(), key=instance_key, reverse=descending)
                    )
                }
            else:
                kind_out[template] = body
        ordered[kind] = kind_out
    return ordered


def filter_status_map(
    instances: dict,
    kinds: list[str] | None,
    groups: list[str] | None,
) -> dict:
    """Narrow a namespace map by row kind and status group.

    A row whose every group is filtered out is DROPPED rather than shown empty —
    an empty row would read as "nothing happened here", which is a different
    claim from "you asked not to see it".

    §Phase 73: groups are a TARGET concept — a workflow publishes history, which
    has none. Asking for workflows AND a group is therefore a contradiction that
    can only ever return nothing, so it is refused rather than answered emptily.
    """
    if groups and kinds and set(kinds) <= GROUPLESS_KINDS:
        raise RuntimeError(
            f"❌ --kind {', '.join(sorted(kinds))} cannot be combined with --group: "
            f"{'a workflow publishes history, which has no status groups'}. "
            "Drop --group, or ask for --kind target"
        )

    def _keep(row: dict) -> dict:
        return {g: axes for g, axes in row.items() if not groups or g in groups}

    selected: dict = {}
    for kind, templates in instances.items():
        if kinds and kind not in kinds:
            continue
        kept_templates: dict = {}
        for template, body in templates.items():
            # A template holds either an `instances` map or, for a singleton, the
            # groups directly.
            if INSTANCES_MARKER in body:
                kept = {
                    segs: g
                    for segs, row in body[INSTANCES_MARKER].items()
                    if (g := _keep(row))
                }
                if kept:
                    kept_templates[template] = {INSTANCES_MARKER: kept}
            else:
                kept = _keep(body)
                if kept:
                    kept_templates[template] = kept
        if kept_templates:
            selected[kind] = kept_templates
    return selected


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
        providers=getattr(args, "providers", ()),
        execution_access_modes=args.execution_access_modes,
        execution_runtime_mode=args.execution_runtime_mode,
    )
    namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, execution_context)
    if args.status == "local":
        namespace_root = Path(args.ctl_state_local_root) / namespace_key
        instances = compute_namespace_status_map(namespace_root)
    else:
        keep = getattr(args, "hydrate_to", None)
        scratch = (
            contextlib.nullcontext(str(Path(keep).expanduser()))
            if keep
            else tempfile.TemporaryDirectory(prefix="atlas-ctl-state-remote-all-")
        )
        with scratch as scratch_root:
            _, namespace_root, syncer = _arm_ctl_state_operation(
                ctl_cfg_root,
                execution_context,
                Path(scratch_root),
                operation="read",
                provider_implementation_key=provider_implementation_key,
                execution_access_modes=args.execution_access_modes,
                provider_options=args.provider_options,
            )
            hydrate_ctl_state_index(syncer)
            instances = compute_namespace_status_map(namespace_root)
            if keep:
                # Provenance, not a guard: ctl refuses nothing on account of it.
                write_yaml_file(
                    Path(scratch_root) / "hydrated_from.yaml",
                    {
                        "namespace": namespace_key,
                        "hydrated_at": utc_timestamp(),
                        "source": "remote ctl-state backend",
                    },
                )
    kinds = getattr(args, "kinds", None)
    groups = getattr(args, "groups", None)
    instances = filter_status_map(instances, kinds, groups)
    instances = structure_status_map(instances, args.structure, args.sort)
    # Kinds sit at the TOP level, not under an `instances:` wrapper: the wrapper
    # said nothing the kind keys do not, and cost a level of nesting on every read.
    report = {
        "namespace": namespace_key,
        "scope": args.status,
        "computed_at": utc_timestamp(),
        "structure": args.structure,
        "sort": args.sort,
        **({"kinds": kinds} if kinds else {}),
        **({"groups": groups} if groups else {}),
        **instances,
    }
    if getattr(args, "write_cache", False):
        # `report` already carries `kinds`/`groups` when a filter was applied, so
        # a filtered cache states which view produced it and cannot be mistaken
        # for a whole-namespace map.
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


def enforce_mutation_lock(
    syncer, *, action: str, run_id: str, parent_run_id: str | None = None
) -> None:
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
    outcome = evaluate_mutation_lock(
        existing, action=action, run_id=run_id, parent_run_id=parent_run_id
    )
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


def render_plt_cfg(
    plt_merged_dir: Path, dest_dir: Path, execution_context: dict[str, object]
) -> Path:
    """Render merged/ into <dest_dir>/rendered (whole-scope). In-process engine
    step — no subprocess, no target_run costume. §Phase 61(b): `dest_dir` is the
    TARGET's own cfg dir, so nothing is rendered once and shared."""
    plt_rendered_dir = dest_dir / "rendered"
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


def run_cfg_distribution(
    pipeline_run_cfg_path: Path, plt_targets_dir_path: Path, run_type: str = "workflow"
) -> Path:
    """Project each target_run's declared cfg keys out of ITS OWN rendered tree.

    §Phase 61(b): the rendered tree lives under the target
    (`plt/targets/<target>/rendered`), not once per run, so this reads only what
    that target derived for itself.
    """
    cfg = load_yaml(pipeline_run_cfg_path) or {}
    target_runs = cfg.get("target_runs") or {}
    if not isinstance(target_runs, dict):
        raise RuntimeError("pipeline_run_cfg.yaml target_runs must be a mapping")
    plt_targets_dir_path.mkdir(parents=True, exist_ok=True)

    brc = _step_utils_module("build_runtime_cfg")

    for target_run_name, target_run_cfg in target_runs.items():
        if not isinstance(target_run_cfg, dict):
            raise RuntimeError(f"Target run {target_run_name!r} config must be a mapping")
        domains = target_run_cfg.get("domains") or []
        cfg_keys = target_run_cfg.get("cfg_keys") or {}
        if not domains:
            continue
        if not isinstance(cfg_keys, dict):
            raise RuntimeError(f"Target run {target_run_name!r} cfg_keys must be a mapping")

        view_dir = (
            plt_targets_dir_path if run_type == "target"
            else plt_targets_dir_path / target_run_name
        )
        rendered_root = view_dir / "rendered"
        target_input_dir = view_dir / "input"
        target_input_dir.mkdir(parents=True, exist_ok=True)

        for domain in domains:
            domain = str(domain).strip().strip("/")
            scope_root = rendered_root / domain
            # assertion 4: a declared domain that matches NO active scope would
            # otherwise deliver a silently empty view.
            if not scope_root.is_dir():
                raise RuntimeError(
                    f"❌ target_run {target_run_name!r} declares domain {domain!r}, but no active "
                    f"cfg scope publishes into /{domain} for this execution context"
                )
            merged: dict = {}
            for path in sorted(scope_root.rglob("*.yaml")):
                merged = brc.merge_values(merged, brc.load_yaml_mapping(path))
            entries = cfg_keys.get(domain)
            if not entries:
                raise RuntimeError(
                    f"❌ target_run {target_run_name!r} declares domain {domain!r} with no cfg_keys"
                )
            projected = project_cfg_keys(
                merged, list(entries),
                label=f"target_run {target_run_name!r} domain {domain!r}",
            )
            write_yaml_file(target_input_dir / f"{domain}.yaml", projected)

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


def rebind_step_credentials(
    step_providers: list[str],
    *,
    target_run_id: str,
    target_run: dict,
    step_env: dict[str, str],
    provider_adapter,
    provider_catalogs: dict,
    execution_context: dict,
    provider_implementation_key: str,
    execution_access_modes: dict[str, str] | None,
    provider_options: dict[str, str] | None,
) -> None:
    """Acquire a fresh session for ONE step (§Phase 70 `--credential-refresh per_step`).

    The CADENCE is the engine's: it owns the step loop and is the only thing that
    knows a sequence is long. The ACQUISITION stays the provider's — this calls
    the same binding the target repo was prepared with, so the engine never
    learns what a session is.

    Scoped by the step's declared `providers`, so a step re-acquires only what it
    said it uses. A step declaring none is left alone.

    The step contract is untouched: a step still RECEIVES environment values and
    never obtains them. Each step is a new container, so a fresh binding is simply
    a different environment — nothing reaches into a running step.
    """
    if provider_adapter is None or not step_providers:
        return
    if provider_adapter.PROVIDER_NAME not in step_providers:
        return
    adapter_access_mode, adapter_options = provider_inputs(
        provider_adapter.PROVIDER_NAME, execution_access_modes, provider_options
    )
    provider_adapter.materialize_target_binding(
        target_run_id,
        target_run,
        step_env,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=adapter_access_mode,
        provider_options=adapter_options,
    )


def prepare_target_repo(
    target_run_id: str,
    target_run: dict,
    run_dir: Path,
    tooling_env: dict[str, str],
    provider_adapter=None,
    provider_catalogs: dict | None = None,
    execution_context: dict[str, object] | None = None,
    provider_implementation_key: str | None = None,
    execution_access_modes: dict[str, str] | None = None,
    provider_options: dict[str, str] | None = None,
) -> tuple[Path, dict[str, str]]:
    """Clone/copy a target_run repo, materialize child modules, and prepare its execution env."""
    workspace = run_workspace_dir(run_dir)
    if workspace is None:
        raise RuntimeError(
            "❌ cannot materialize a target_run workspace: the run records no "
            "ctl_state_local_root"
        )
    repo_path = workspace / target_run_id
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
        adapter_access_mode, adapter_options = provider_inputs(
            provider_adapter.PROVIDER_NAME, execution_access_modes, provider_options
        )
        provider_adapter.materialize_target_binding(
            target_run_id,
            target_run,
            target_env,
            provider_catalogs,
            execution_context=execution_context,
            implementation_key=provider_implementation_key,
            execution_access_mode=adapter_access_mode,
            provider_options=adapter_options,
        )
    return repo_path, target_env


def _repo_local_active_steps(
    action_manifest: dict, active_ids: list[str], repo_root: Path, action: str | None = None
) -> list[dict]:
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
        # §Phase 73: the manifest's action grouping is the single source for what a
        # step does, so nothing re-declares it. What the grouping cannot state is
        # that the PATH agrees with it — an entry filed under one action may point
        # at another action's directory — so the path is checked against the
        # action that reached it.
        if action is not None:
            expected_prefix = f"steps/{action}/"
            if expected_prefix not in f"{step_path}/":
                raise RuntimeError(
                    f"❌ step {step_id!r} is declared under action {action!r} but its "
                    f"path is {step_path!r}; a step's path must sit under "
                    f"{expected_prefix!r}"
                )
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
        # §Phase 60: the STEP is the true consumer (its root declares the
        # variables), so it declares content keys, not files.
        # §Phase 60: a step declares PURE CONTENT KEYS. `cfg_key_sets` is a
        # CTL-cfg authoring convenience; a source repo cannot resolve a ctl
        # catalog entry, and depending on one would recreate the cross-repo
        # coupling this phase removed. The step's contract is its own root's
        # variables, spelled out.
        if step_meta.get("cfg_key_sets"):
            raise RuntimeError(
                f"Step metadata must not use cfg_key_sets — a step declares content keys, "
                f"and cfg_key_sets is a CTL-cfg catalog the source repo cannot resolve: {step_meta_path}"
            )
        # A step declares the providers it uses. The target is the CEILING, the
        # step is the SIGNATURE: without this, every step in a multi-provider
        # target receives every credential set — a silent over-grant on the one
        # path that must not widen by default.
        step_providers = step_meta.get("providers")
        if not isinstance(step_providers, list) or not step_providers or not all(
            isinstance(item, str) and item.strip() for item in step_providers
        ):
            raise RuntimeError(
                f"❌ Step metadata providers must be a non-empty list of provider names: "
                f"{step_meta_path}"
            )

        step_contract = step_meta.get("cfg_keys") or {}
        if not isinstance(step_contract, dict):
            raise RuntimeError(f"Step metadata cfg_keys must be a mapping: {step_meta_path}")
        for domain, bindings in step_contract.items():
            # A step is a SIGNATURE: `<local name>: <cfg key>`. It binds by name,
            # so a glob — which can expand to a key the step never named — is not
            # expressible here by construction.
            if not isinstance(bindings, dict) or not bindings:
                raise RuntimeError(
                    f"Step metadata cfg_keys[{domain!r}] must be a non-empty mapping of "
                    f"local name -> cfg key: {step_meta_path}"
                )
            for local_name, cfg_key in bindings.items():
                if not isinstance(local_name, str) or not local_name:
                    raise RuntimeError(
                        f"Step metadata cfg_keys[{domain!r}] local names must be "
                        f"non-empty strings: {step_meta_path}"
                    )
                if not isinstance(cfg_key, str) or not cfg_key:
                    raise RuntimeError(
                        f"Step metadata cfg_keys[{domain!r}][{local_name!r}] must bind a "
                        f"non-empty cfg key: {step_meta_path}"
                    )

        active.append(
            {
                "id": step_id,
                "path": step_path,
                "providers": sorted(set(step_providers)),
                "cfg_keys": step_contract,
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


def get_repo_local_steps(
    repo_path: Path, action: str, procedure_key: str
) -> tuple[list[str], list[dict]]:
    manifest_file = repo_path / ADAPTER_DIR / "manifest.yaml"
    if not manifest_file.is_file():
        raise RuntimeError(f"❌ manifest file not found: {manifest_file}")
    procedures_file = repo_path / ADAPTER_DIR / "procedures.yaml"
    if not procedures_file.is_file():
        raise RuntimeError(f"❌ procedures file not found: {procedures_file}")

    manifest = (load_yaml(manifest_file) or {}).get("manifest", {})
    procedures = (load_yaml(procedures_file) or {}).get("procedures", {})

    action_manifest = manifest.get(action)
    if not isinstance(action_manifest, dict) or not action_manifest:
        raise RuntimeError(f"manifest {manifest_file} declares no steps for action {action!r}")

    action_procedures = procedures.get(action)
    if not isinstance(action_procedures, dict) or procedure_key not in action_procedures:
        raise RuntimeError(f"procedure {action}/{procedure_key} not found in {procedures_file}")
    procedure = action_procedures[procedure_key]
    if not isinstance(procedure, dict) or "steps" not in procedure:
        raise RuntimeError(f"procedure {action}/{procedure_key} must define steps")

    active_ids: list[str] = []
    for step_id in procedure.get("steps", []):
        if step_id not in action_manifest:
            raise RuntimeError(f"Step {step_id!r} not declared in manifest for action {action!r}")
        active_ids.append(step_id)

    return active_ids, _repo_local_active_steps(action_manifest, active_ids, repo_path, action)


def ensure_repo_execution_context(repo_path: Path, execution_context_path: Path) -> bool:
    repo_execution_context_path = repo_path / EXECUTION_CONTEXT_FILENAME
    if execution_context_path.resolve() == repo_execution_context_path.resolve():
        return False
    shutil.copy2(execution_context_path, repo_execution_context_path)
    return True


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
    action: str | None = None,
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
        / compose_state_relpath("target", target_key, segments),
        target_instance_address(target_key, segments),
    )


def latest_child_revision(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
    action: str | None = None,
) -> dict | None:
    """§Phase 61(d): the revision a SPAWNED child just committed.

    The child publishes its own committed pointer, so the parent reads it back
    rather than being told — the same record any later run would consult.
    """
    instance_dir, address = target_instance_dir_for_run(
        parent_run_dir, target_run, execution_context, action
    )
    # §Phase 73: read the GROUP this child published into. Defaulting to
    # `deployment` made a plan child invisible to its workflow, which then
    # committed with no child_revisions at all — a composition recording nothing.
    resolved_action = action or load_run_metadata(parent_run_dir).get("action")
    pointer = read_committed_pointer(instance_dir, action_group(str(resolved_action)))
    if not pointer:
        return None
    return {
        "address": address,
        "run_id": pointer.get("run_id"),
        "snapshot_sha256": pointer.get("snapshot_sha256"),
        "status": pointer.get("status"),
    }


def up_to_date_child_revision(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
    action: str | None = None,
) -> dict | None:
    """The published revision, only when reusing it is still correct.

    §Phase 73: the ACTION is compared like every other identity field. Without it
    a workflow holding one target under two actions skips BOTH members — the six
    content fields match either way, because source and cfg are identical — so a
    run that destroyed and then failed to re-provision reports success while the
    instance is still destroyed."""
    if target_run.get("ref_policy") != "commit_required":
        return None
    if target_run.get("source_state") != "clean":
        return None
    source_commit = target_run.get("source_commit")
    cfg_source_commit = target_run.get("cfg_source_commit")
    target_definition_sha256 = target_run.get("target_definition_sha256")
    target_cfg_view_sha256 = target_run.get("target_cfg_view_sha256")
    if not all((
        source_commit,
        cfg_source_commit,
        target_definition_sha256,
        target_cfg_view_sha256,
    )):
        return None
    instance_dir, address = target_instance_dir_for_run(
        parent_run_dir, target_run, execution_context, action
    )
    resolved_action = action or load_run_metadata(parent_run_dir).get("action")
    pointer = read_committed_pointer(instance_dir, action_group(str(resolved_action)))
    if not pointer or pointer.get("status") == "outdated" or pointer.get("outdated"):
        return None
    expected = {
        "action": action or load_run_metadata(parent_run_dir).get("action"),
        "source_commit": source_commit,
        "cfg_source_commit": cfg_source_commit,
        "source_state": "clean",
        "ref_policy": "commit_required",
        "target_definition_sha256": target_definition_sha256,
        "target_cfg_view_sha256": target_cfg_view_sha256,
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
        "target", target_key, segments
    )
    # a target instance's identity is fully encoded in its path
    # (<key>/instances/<seg>/…) — no identity.yaml is written (§minimal files).
    child_run_id = generate_uuid7()
    child_run_dir = instance_dir / "runs" / child_run_id
    # §Phase 61(c): the target owns its own log; the workflow keeps the aggregate.
    child_logs_dir = child_run_dir / "logs"
    child_logs_dir.mkdir(parents=True, exist_ok=True)
    child_log_path = child_logs_dir / f"{SERVICE_ID}_{child_run_id}.log"
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
            "log_path": str(child_log_path),
            "parent_log_path": parent_metadata.get("log_path"),
            "target_keys": [target_key],
            "instance": segments,
            "instance_address": address,
            "parent_workflow_run_id": parent_metadata.get("run_id"),
            # §Phase 61(b4): name the parent by INSTANCE too, so a target record
            # says which workflow instance it belongs to without loading the parent.
            "parent_workflow_instance_address": parent_metadata.get("instance_address"),
            "fan_out_run_id": parent_metadata.get("fan_out_run_id"),
            "mutation_started": False,
            **{
                key: target_run[key]
                for key in (
                    "source_commit", "cfg_source_commit", "source_state", "ref_policy",
                    "plt_overlays", "target_definition_sha256", "target_cfg_view_sha256",
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
    # §Phase 61(b3): the child owns its WHOLE cfg derivation, not two views of a
    # tree the workflow keeps. The workflow builds it up front (fail-fast for every
    # target before any runs — §b2) and hands the complete tree to the target it
    # describes; `run_pipeline` then drops the workflow-side copy.
    cfg_dst = child_run_dir / "cfg"
    src_root = plt_targets_dir_path / target_run_id
    for view in ("merged", "rendered", "input", "resolved"):
        src = src_root / view
        if src.is_dir():
            shutil.copytree(src, cfg_dst / "plt" / view, dirs_exist_ok=True)
    # the target's OWN execution context — params filtered to what it declared,
    # plus target.* — not the run-wide one (§Phase 61(a))
    target_context_path = src_root / "execution" / EXECUTION_CONTEXT_FILENAME
    if target_context_path.is_file():
        (child_run_dir / "execution").mkdir(parents=True, exist_ok=True)
        shutil.copy2(target_context_path, child_run_dir / "execution" / EXECUTION_CONTEXT_FILENAME)
    else:
        write_execution_context_artifact(child_run_dir, execution_context)
    if target_run.get("target_definition") is not None:
        write_yaml_file(
            cfg_dst / "ctl" / "target_definition.yaml", target_run["target_definition"]
        )
    source_refs = {
        key: target_run[key]
        for key in ("source", "ref", "branch", "commit", "procedure")
        if target_run.get(key) is not None
    }
    if source_refs:
        # §Phase 57: a RECORD (what this run ran), not workspace scratch — it no
        # longer shares a name with the build workspace.
        write_yaml_file(child_run_dir / "source_refs.yaml", source_refs)


def build_child_target_command(
    spec: dict,
    target_key: str,
    *,
    parent_run_dir: Path,
    parent_run_id: str,
    action: str | None = None,
) -> list[str]:
    """§Phase 61(d): the argv for one workflow child, derived from ONE frozen spec.

    A child must run with exactly its parent's settings. Dropping a flag would not
    fail — the child would silently run DIFFERENTLY — so the argv is built from a
    single object captured in `run_pipeline`, never assembled from scattered
    locals.

    §Phase 73: the ACTION is the exception, and deliberately so. It comes from the
    member entry when that entry declares one, because a workflow is the one level
    that may hold members going different directions. Everything else still comes
    from the frozen spec.
    """
    argv = [
        sys.executable, str(spec["ctl_entrypoint"]), "target",
        "--ctl-cfg", str(spec["ctl_cfg_root"]),
        "--ctl-profile", spec["ctl_profile"],
        "--ctl-state-local-root", str(spec["ctl_state_local_root"]),
        "--execution-runtime-mode", spec["execution_runtime_mode"],
        "--action", action or spec["action"],
        "--target", target_key,
        # the child runs UNDER the parent's ctl-state lock. Authorisation is a
        # single-use grant passed by ENVIRONMENT (see CHILD_LOCK_GRANT_ENV); the
        # run id below is provenance only, and is deliberately not a credential.
        "--parent-workflow-run-id", parent_run_id,
    ]
    # Read from the parent's own record rather than the frozen spec: the spec is
    # about how to INVOKE a child, the instance address is a fact about the parent,
    # and taking it from the record it is written in leaves nothing to drift.
    parent_instance_address = load_run_metadata(parent_run_dir).get("instance_address")
    if parent_instance_address:
        argv += ["--parent-workflow-instance-address", str(parent_instance_address)]
    if spec.get("providers"):
        argv += ["--providers", ",".join(spec["providers"])]
    # The cadence has no default, so a child cannot inherit one: the parent's
    # choice travels in the frozen spec like every other run-shaping argument.
    if spec.get("credential_refresh_modes"):
        argv += [
            "--credential-refresh-mode",
            ",".join(f"{k}={v}" for k, v in sorted(spec["credential_refresh_modes"].items())),
        ]
    for key, value in (spec.get("execution_params") or {}).items():
        argv += ["--execution-params", f"{key}={value}"]
    for key, value in (spec.get("provider_options") or {}).items():
        argv += ["--provider-options", f"{key}={value}"]
    for provider, mode in (spec.get("execution_access_modes") or {}).items():
        argv += ["--execution-access-mode", f"{provider}={mode}"]
    for overlay in (spec.get("plt_overlays") or []):
        argv += ["--plt-overlays", overlay]
    for provider in (spec.get("force_skip_execution_identity_preflight_check") or []):
        argv += ["--force-skip-execution-identity-preflight-check", provider]
    for flag, enabled in (
        ("--agreed-defer-ctl-state-backend-sync", spec.get("agreed_defer_ctl_state_backend_sync")),
        ("--force-skip-ctl-state-backend-sync", spec.get("force_skip_ctl_state_backend_sync")),
        ("--force-skip-guardrails", spec.get("force_skip_guardrails")),
        ("--force-skip-full-cfg-validation-gate", spec.get("force_skip_full_cfg_validation_gate")),
        ("--skip-children-precheck", spec.get("skip_children_precheck")),
    ):
        if enabled:
            argv.append(flag)
    return argv


def run_targets(
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
    execution_access_modes: dict[str, str] | None = None,
    provider_options: dict[str, str] | None = None,
    skip_up_to_date: bool = False,
    child_command_spec: dict | None = None,
    credential_refresh_modes: dict | None = None,
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
        if skip_up_to_date:
            revision = up_to_date_child_revision(
                run_dir, target_run, execution_context, inventory_name
            )
            if revision is not None:
                logging.info(
                    "Skipping committed target instance %s (commits unchanged)",
                    revision["address"],
                )
                child_revisions.append(revision)
                continue

        # §Phase 61(d): a WORKFLOW spawns `ctl.py target` per child, so a target
        # runs by exactly the same path standalone and inside a workflow. The child
        # builds its own cfg, context and log; `run_and_log` streams its output into
        # this run's log, so the workflow keeps the aggregate. It executes under this
        # run's ctl-state lock — flock is exclusive and non-blocking, so acquiring it
        # again would fail outright.
        if child_command_spec is not None and load_run_metadata(run_dir).get(
            "run_type"
        ) == "workflow":
            target_key = target_run.get("target")
            argv = build_child_target_command(
                child_command_spec, target_key,
                parent_run_dir=run_dir, parent_run_id=run_id,
                action=target_run.get("action"),
            )
            logging.info("Spawning child target run: %s", target_key)
            child_env = dict(os.environ)
            child_env[CHILD_LOCK_GRANT_ENV] = mint_child_lock_grant(
                Path(child_command_spec["ctl_state_local_root"]),
                child_kind="target", child_key=target_key,
            )
            # The workflow's OWN slot has to record the mutation. This branch
            # spawns and `continue`s, so it never reaches the inline mark below
            # — a workflow run therefore reported `mutation_started: false` no
            # matter how much its children changed, and `partial` could not
            # surface on the composition row at all. Marked BEFORE the child
            # runs, on the same conservative rule as the inline path: from here
            # resources may change, and claiming possible damage beats denying it.
            if inventory_name in MUTATING_ACTIONS and not mutation_marked:
                mark_mutation_started(run_dir, target_run_id)
                mutation_marked = True
            run_and_log(argv, cwd=str(run_dir), env=child_env)
            revision = latest_child_revision(
                run_dir, target_run, execution_context, inventory_name
            )
            if revision is not None:
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
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            )

        procedure_key = target_run.get("procedure")
        if not isinstance(procedure_key, str) or not procedure_key:
            raise RuntimeError(f"❌ target run {target_run_id!r} must define a non-empty procedure")
        target_view_dir = (
            plt_targets_dir_path
            if (plt_targets_dir_path / "input").is_dir()
            else plt_targets_dir_path / target_run_id
        )
        origin_cfg_path = target_view_dir / "input"
        if not origin_cfg_path.is_dir():
            raise RuntimeError(f"❌ target_run input cfg dir not found for target_run {target_run_id!r}: {origin_cfg_path}")
        target_cfg_dir = target_view_dir / "resolved"
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
        # §Phase 61(c): everything this target emits also lands in its own log.
        target_log = target_run_log(
            target_state_run_dir if target_instance_address is not None else None
        )
        target_log.__enter__()
        try:
            repo_step_ids, repo_steps = get_repo_local_steps(repo_path, inventory_name, procedure_key)
            run_manifest = {
                "run_id": run_id,
                "branch": target_run.get("branch"),
                "commit": target_run.get("commit"),
                "action": inventory_name,
                "procedure": procedure_key,
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
                repo_step_env["cfg_keys"] = json.dumps(repo_step.get("cfg_keys") or {})
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
                if (credential_refresh_modes or {}).get(
                    getattr(provider_adapter, "PROVIDER_NAME", "")
                ) == "per_step":
                    rebind_step_credentials(
                        list(repo_step.get("providers") or []),
                        target_run_id=target_run_id,
                        target_run=target_run,
                        step_env=repo_step_env,
                        provider_adapter=provider_adapter,
                        provider_catalogs=provider_catalogs,
                        execution_context=execution_context,
                        provider_implementation_key=provider_implementation_key,
                        execution_access_modes=execution_access_modes,
                        provider_options=provider_options,
                    )

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
            target_log.__exit__(None, None, None)
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
    log_file: Path,
    provider_options: dict[str, str] | None,
    execution_runtime_mode: str,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_access_modes: dict[str, str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
    unlock_scope: str | None = None,
) -> None:
    """Run a maintenance action against a single target_run target."""
    if maintenance_action == "unlock-ctl-state":
        # Two locks with different reach. `both` clears the remote lock and THIS
        # machine's local one; remote is namespace-wide, local is one directory,
        # so `both` is not a claim to have cleared every local lock everywhere.
        # A lock missing in one scope is a SKIP: the scopes diverge legitimately.
        scope = unlock_scope or "both"
        outcome: dict[str, str] = {}
        if scope in ("local", "both"):
            outcome["local"] = (
                "released"
                if force_unlock_ctl_state_lock(ctl_state_local_root, lock_id, run_dir)
                else "not present — skipped"
            )
        if scope in ("remote", "both"):
            with tempfile.TemporaryDirectory(prefix="atlas-ctl-state-unlock-") as scratch:
                context = build_execution_context(
                    ctl_cfg_root,
                    action=inventory_name,
                    ctl_profile=ctl_profile,
                    execution_params=execution_params,
                    providers=providers,
                    execution_runtime_mode=execution_runtime_mode,
                )
                _, _, syncer = _arm_ctl_state_operation(
                    ctl_cfg_root,
                    context,
                    Path(scratch),
                    operation="maintenance",
                    provider_implementation_key=provider_implementation_key,
                    execution_access_modes=execution_access_modes,
                    provider_options=provider_options,
                )
                outcome["remote"] = release_remote_mutation_lock(syncer, lock_id)
        print(yaml.safe_dump(
            {"operation": "unlock-ctl-state", "scope": scope,
             "lock_id": lock_id, "locks": outcome},
            sort_keys=False).rstrip())
        print_run_summary(run_id, log_file)
        return

    execution_context = build_execution_context(
        ctl_cfg_root,
        action=inventory_name,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        providers=providers,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        execution_access_modes=execution_access_modes,
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
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
    )
    cfg_report = build_cfg_validation_report(
        collect_provider_cfg_findings(ctl_cfg_root, execution_context)
    )
    apply_full_cfg_validation_gate(
        cfg_report, force_skip=force_skip_full_cfg_validation_gate
    )
    write_cfg_validation_artifacts(run_gates_dir(run_dir), cfg_report)
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
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
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

    active_target_runs, pipeline_run_cfg_path, final_plt_overlays = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
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
    update_run_metadata(run_dir, {"plt_overlays": final_plt_overlays})
    record_run_target_keys(run_dir, target_keys_from_active_target_runs(active_target_runs))
    # §Phase 61(b): per-target derivation, same as run_pipeline.
    run_type_now = str(load_run_metadata(run_dir).get("run_type"))
    plt_targets_dir_path = target_cfg_views_root(run_dir, run_type_now)
    for target_run_id, target_run in active_target_runs.items():
        if not target_run.get("domains"):
            continue
        target_context = build_target_execution_context(
            target_run_id, target_run, execution_context
        )
        target_rendered_dir = prepare_target_cfg_view(
            target_run_id, target_run,
            plt_cfg_root=plt_cfg_root,
            target_cfg_dir=target_cfg_view_dir(run_dir, run_type_now, target_run_id),
            ctl_profile=ctl_profile,
            scope_params=scope_params_from_context(target_context),
            execution_context=target_context,
        )
        verify_guardrails(
            ctl_cfg_root,
            plt_cfg_root,
            guardrails_cfg_root,
            target_rendered_dir,
            target_context,
            scope_params_from_context(target_context),
        )

    validate_target_runs_have_commits(active_target_runs, ctl_ref_policy)
    provider_adapter = run_provider_adapter(execution_context)
    provider_catalogs = provider_adapter.load_runtime_catalogs(
        ctl_cfg_root, execution_context=execution_context
    )
    adapter_access_mode, adapter_options = provider_inputs(
        run_provider(execution_context), execution_access_modes, provider_options
    )
    provider_adapter.validate_active_target_access(
        active_target_runs,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=adapter_access_mode,
        provider_options=adapter_options,
    )
    write_git_metas(ctl_cfg_root, plt_cfg_root, guardrails_cfg_root, artifacts_dir)
    plt_targets_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path, plt_targets_dir_path, run_type_now
    )
    finalize_target_cfg_view_facts(
        active_target_runs, plt_targets_dir_path, pipeline_run_cfg_path
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
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
    )
    assertion_argv = provider_adapter.target_assertion_argv(materialize_step_utils(run_dir))
    if assertion_argv:
        run_and_log(assertion_argv, cwd=repo_path, env=target_env)

    target_cfg_dir = (
        plt_targets_dir_path
        if (plt_targets_dir_path / "input").is_dir()
        else plt_targets_dir_path / target_run_id
    ) / "input"
    if not target_cfg_dir.is_dir():
        raise RuntimeError(f"❌ target_run input cfg dir not found for target_run '{target_run_id}': {target_cfg_dir}")

    # A tool's own state lock is released by the target repo's declared `unlock`
    # procedure, not here: only the repo knows which tool it runs and where
    # that tool keeps its state. The engine used to author the script itself,
    # which meant reading step SOURCE to find a project path — the one place it
    # reached inside a step instead of going through the step contract.
    raise RuntimeError(
        f"❌ maintenance action {maintenance_action!r} does not operate on a target.\n"
        "To release a tool's state lock, run the target repo's declared `unlock` "
        "procedure:\n"
        "  ./ctl.py procedure --procedure unlock --action destroy "
        "--target <key> --lock-id <id>"
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
    existing_lock: dict | None, *, action: str, run_id: str, parent_run_id: str | None = None
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
    # A workflow holds the namespace for the whole run and spawns its targets as
    # child processes. A child meeting its OWN parent's lock is not contention —
    # the namespace is already held on its behalf, and blocking it would make
    # every sync-armed workflow fail on its first child. Mirrors the single-use
    # local-lock grant, which covers the flock but not this backend lock.
    if parent_run_id and holder == str(parent_run_id):
        logging.info("mutation lock held by parent run %s; proceeding as its child", holder)
        return {"decision": "proceed"}
    return {"decision": "blocked", "holder": holder}


INSTANCES_MARKER = "instances"


def qualified_address(kind: str, address: str) -> str:
    """`<kind>/<instance address>` — the form `--structure flat` already emits.

    A cross-reference between kinds must say WHICH kind it points at: without it
    `env/baseline` is ambiguous between a workflow and a target of the same name,
    and a reader cannot paste it back into a query.
    """
    return address if address.startswith(f"{kind}/") else f"{kind}/{address}"


def instance_address(key: str, instance_segments: list[str]) -> str:
    """The canonical instance address: `<key>` for a singleton, otherwise
    `<key>/instances/<seg>/<seg>` — the SAME path form as the state dir layout,
    marker included, so an address can be read straight off a path and back.

    Segments contain `=`, key components never do, and the `instances` marker
    separates them explicitly rather than by inference.
    """
    if not instance_segments:
        return key
    return "/".join([key, INSTANCES_MARKER, *instance_segments])


def target_instance_address(target_key: str, instance_segments: list[str]) -> str:
    return instance_address(target_key, instance_segments)


def workflow_composition_sha256(
    target_instance_addresses: list[str], actions: list[str] | None = None
) -> str:
    """Workflow instance identity: SHA-256 over a whitespace-free canonical JSON
    array of the ORDERED members, truncated to 8 hex chars.

    §Phase 73: a member is `(address, action)`, not an address alone. Once members
    carry their own action, hashing addresses only makes two compositions doing
    OPPOSITE things to one target hash identically — a teardown of A and a deploy
    of A would share one instance and overwrite each other's pointer.

    The digest indexes a tiny per-namespace set, so 32 bits is ample; identity.yaml
    records the full members and stays the authoritative identity source.
    """
    if not isinstance(target_instance_addresses, list) or not target_instance_addresses:
        raise RuntimeError("❌ workflow composition needs a non-empty ordered address list")
    for address in target_instance_addresses:
        if not isinstance(address, str) or not address.strip():
            raise RuntimeError("❌ workflow composition addresses must be non-empty strings")
    if actions is None:
        members: list = list(target_instance_addresses)
    else:
        if len(actions) != len(target_instance_addresses):
            raise RuntimeError(
                "❌ workflow composition needs one action per address, got "
                f"{len(actions)} for {len(target_instance_addresses)}"
            )
        members = [[a, act] for a, act in zip(target_instance_addresses, actions)]
    canonical = json.dumps(members, separators=(",", ":"), ensure_ascii=False)
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
# §Phase 73: state is partitioned by status GROUP. Three groups are three
# independent facts, so they never overwrite one another; provision and destroy
# are two directions of ONE fact and share the `deployment` file.
GROUP_BY_ACTION = {
    "plan": "plan",
    "readonly": "readonly",
    "provision": "deployment",
    "destroy": "deployment",
    "maintenance": "maintenance",
}
RESULT_GROUPS = ("plan", "readonly", "deployment", "maintenance")
# §Phase 73: kinds that publish history rather than grouped state.
GROUPLESS_KINDS = frozenset({"workflow"})


def action_group(action: str) -> str:
    """The group an action publishes into; unknown actions fail loud."""
    group = GROUP_BY_ACTION.get(action)
    if group is None:
        raise RuntimeError(
            f"❌ unknown action {action!r}; expected one of {sorted(GROUP_BY_ACTION)}"
        )
    return group
STATE_STRUCTURAL_NAMES = frozenset({"runs", "committed.yaml", "identity.yaml", "locks"})


def compose_state_relpath(
    kind: str, key: str, instance_segments: list[str]
) -> Path:
    """The namespace-relative instance directory for a state owner (§Phase 73):
    `<kind>/<key...>/instances/<seg>/<seg>` — or, for a singleton, `<kind>/<key...>`
    with no instances/ layer. The namespace root is prepended by the caller.

    There is no action segment. A key names a THING, and provision and destroy are
    two directions of one state, so they share an instance and differ only in the
    group file they publish."""
    if kind not in RESULT_KINDS:
        raise RuntimeError(f"❌ unknown state kind {kind!r} (expected one of {RESULT_KINDS})")
    key_parts = [p for p in key.split("/") if p]
    if not key_parts:
        raise RuntimeError("❌ state key must be non-empty")
    parts = [kind, *key_parts]
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
    if len(parts) < 2 or parts[0] not in RESULT_KINDS:
        return None
    kind, rest = parts[0], parts[1:]
    if "instances" in rest:
        idx = rest.index("instances")
        key_parts = rest[:idx]
        after = rest[idx + 1:]
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
        "kind": kind,
        "key": key,
        "instance_segments": instance_segments,
        "instance": "/".join(instance_segments),
        "address": "/".join([kind, instance_address(key, instance_segments)]),
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
    providers: list[str] | tuple[str, ...] = (),
) -> list[str]:
    """Resolve a run's local ctl-state locator BEFORE its dirs exist (§Phase 30).

    Pure cfg resolution: the run's single ctl-state namespace maps through the
    provider adapter to the backend mirror tree the run lives in. The same
    namespace is re-resolved when the syncer is armed and must agree. Fan-out
    and namespace-less runs land under the reserved `_local` tree."""
    if run_type in ("fan_out", "procedure"):
        # §Phase 31: fan-outs are stateless (local artifacts only) and
        # procedure runs are synthetic dev-loop records — neither has a
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
        providers=providers,
        execution_runtime_mode=execution_runtime_mode,
    )
    namespace_key, _ = resolve_ctl_state_namespace(ctl_cfg_root, execution_context)
    return [namespace_key]


def workflow_instance_address(workflow_name: str, segments: list[str]) -> str:
    """`env/baseline` when a workflow varies by nothing; otherwise the Hive path.

    Same shape as a target instance address, so one addressing scheme covers both
    kinds and a reader can tell which environment a run belongs to by reading it.
    """
    return instance_address(workflow_name, segments)


def workflow_target_key_entries(
    workflow: dict, workflows: dict, *, label: str, _seen: tuple = ()
) -> list[str]:
    """Every target key a workflow can run, across ALL its member branches.

    Static: no execution context, so every branch counts rather than the one a
    run would select — a misdeclaration in an unselected branch is still a
    misdeclaration. Imports are followed, since an imported workflow's members
    are this workflow's members too.
    """
    name = str(workflow.get("__name__", ""))
    keys: list[str] = []
    for imported in workflow.get("import_workflow_keys") or []:
        if imported in _seen or imported not in workflows:
            continue
        keys += workflow_target_key_entries(
            {**workflows[imported], "__name__": imported},
            workflows,
            label=label,
            _seen=(*_seen, name, imported),
        )
    target_keys = workflow.get("target_keys")
    branches = (
        [member.get("target_keys") or [] for member in (target_keys.get("members") or [])]
        if isinstance(target_keys, dict)
        else [target_keys or []]
    )
    for branch in branches:
        for entry in branch:
            key = entry.get("key") if isinstance(entry, dict) else entry
            if isinstance(key, str) and key and key not in keys:
                keys.append(key)
    return keys


def _selector_branches(declared) -> list[tuple[list[str], dict | None]]:
    """A params declaration as `(params, selectors)` branches.

    A plain list is ONE branch with no condition — it applies always. A
    members-shaped declaration is one branch per member.
    """
    if declared is None:
        return [([], None)]
    if isinstance(declared, dict):
        return [
            (list(member.get("params") or []), member.get("selectors"))
            for member in (declared.get("members") or [])
        ]
    if isinstance(declared, list):
        return [(list(declared), None)]
    return []


def _selectors_can_both_hold(left: dict | None, right: dict | None, *, label: str) -> bool:
    """Whether one execution context could satisfy both selector blocks.

    Not subset: neither condition contains the other, they merely have to be
    SATISFIABLE together. Two blocks conflict only where they constrain the same
    reference to disjoint value sets; a reference only one of them names is free.
    """
    left_req = selector_requirements(left, label=label)
    right_req = selector_requirements(right, label=label)
    for ref, values in left_req.items():
        other = right_req.get(ref)
        if other is not None and not (values & other):
            return False
    return True


def validate_all_workflow_instance_params(workflows: dict, targets: dict) -> None:
    """Every workflow's declared instance params, checked STATICALLY.

    The per-run guard is exact but only ever sees the workflow being run, so a
    misdeclaration elsewhere stays silent until someone runs it. This is the
    whole-cfg pass: it compares each workflow BRANCH against the target params
    that apply under that branch's condition, with no execution context and no
    resolution.

    A target's plain list applies to every branch; a members-shaped target
    contributes only the branches whose selectors could hold together with the
    workflow branch's. If SEVERAL branches of one target could hold and they
    declare different params, the workflow branch does not pin the axis that
    target dispatches on, and no single declaration can be correct — that is an
    error rather than a guess.
    """
    for name, workflow in sorted(workflows.items()):
        if not isinstance(workflow, dict):
            continue
        label = f"workflow {name!r}"
        member_keys = workflow_target_key_entries(
            {**workflow, "__name__": name}, workflows, label=label
        )
        for params, selectors in _selector_branches(
            workflow.get("workflow_instance_params")
        ):
            union: list[str] = []
            for key in member_keys:
                target = targets.get(key)
                if not isinstance(target, dict):
                    continue  # not in this cfg's inventory; the per-run guard sees it
                applicable = [
                    branch
                    for branch in _selector_branches(target.get("target_instance_params"))
                    if _selectors_can_both_hold(selectors, branch[1], label=label)
                ]
                distinct = {tuple(branch[0]) for branch in applicable}
                if len(distinct) > 1:
                    raise RuntimeError(
                        f"❌ {label}: a branch selected by {selectors!r} matches "
                        f"{len(distinct)} different instance-param branches of target "
                        f"{key!r} ({sorted(distinct)}). The workflow branch does not pin "
                        "the axis that target dispatches on, so no single declaration is "
                        "correct — split the workflow branch the way the target is split"
                    )
                for branch_params in applicable:
                    for param in branch_params[0]:
                        if param not in union:
                            union.append(param)
            missing = sorted(set(union) - set(params))
            extra = sorted(set(params) - set(union))
            if missing:
                raise RuntimeError(
                    f"❌ {label}: workflow_instance_params branch {params} is missing "
                    f"{missing}, which its members instance over. Two target instances "
                    "would share one workflow address and their histories would merge"
                )
            if extra:
                raise RuntimeError(
                    f"❌ {label}: workflow_instance_params branch {params} declares "
                    f"{extra}, which no member instances over. That address can never "
                    "differ, and it tells a reader the workflow varies by an axis it "
                    "does not"
                )


def workflow_member_instance_params(
    workflow_cfg: dict, targets: dict, *, label: str
) -> tuple[list[str], bool]:
    """The union of the instance params of every target this workflow runs.

    UNION, not intersection: members may instance on different axes — one over
    two params, another over just one of them — and a workflow must partition by
    everything ANY member varies over. Otherwise two target instances collapse
    into one workflow address and their histories merge.

    Returns the union and whether it is COMPLETE — a member missing from this
    action's inventory contributes unknown axes, so the caller must not conclude
    that a declared param is spare.
    """
    union: list[str] = []
    complete = True
    for entry in workflow_cfg.get("target_runs") or []:
        name = entry if isinstance(entry, str) else entry.get("target")
        if not name:
            continue
        # A member absent from THIS action's inventory contributes no axes. The
        # sibling resolver is tolerant the same way (`targets.get(name) or {}`);
        # raising here would turn "this workflow is not runnable for this action"
        # into a hard failure at identity resolution, which is not this guard's
        # job — the action allowlist already answers that.
        target_def = targets.get(name)
        if target_def is None:
            # Absent from THIS action's inventory, so its axes are unknown here
            # and the union is incomplete. Raising would turn "not runnable for
            # this action" into a hard failure, which the action allowlist
            # already answers.
            complete = False
            continue
        for param in target_def.get("target_instance_params") or []:
            if param not in union:
                union.append(param)
    return union, complete


def validate_workflow_instance_params(
    declared, workflow_cfg: dict, targets: dict, *, label: str,
    execution_context: dict[str, object] | None = None,
) -> list[str]:
    """A workflow's declared instance params must EQUAL its members' union.

    Declared rather than derived because declared params ARE identity (§Phase 32),
    and guarded because the value has exactly one correct answer:

        declared < union   two target instances collapse into one workflow
                           address, so their histories merge and `last_run`
                           answers for the wrong one
        declared > union   an address that can never differ, and one that LIES:
                           a reader concludes the workflow varies by an axis
                           whose value is the same in every instance

    Both are errors. This is stricter than the target rule (§Phase 32:
    over-declaration warns) on purpose: a target's params describe a thing that
    exists, so a spare axis is only slack; a workflow's params are DERIVED from
    its members, so a spare axis contradicts them.
    """
    union, union_is_complete = workflow_member_instance_params(
        workflow_cfg, targets, label=label
    )
    if declared is None:
        declared_list: list[str] = []
    elif isinstance(declared, dict):
        # Members-shaped, exactly as a target's own instance params may be: a
        # member whose instance axes DISPATCH on a param (a domain, a profile)
        # has a different union per context, so the workflow above it needs the
        # same dispatch rather than one list that can only ever be right once.
        resolved = resolve_list_members(
            declared, execution_context, value_field="params", label=label
        )
        declared_list = list(resolved or [])
    elif isinstance(declared, list):
        declared_list = list(declared)
    else:
        raise RuntimeError(
            f"❌ {label}: workflow_instance_params must be a list, or members-shaped"
        )

    missing = [p for p in union if p not in declared_list]
    extra = [p for p in declared_list if p not in union]
    if missing:
        raise RuntimeError(
            f"❌ {label}: workflow_instance_params is missing {sorted(missing)}, "
            f"which its members instance over. Two target instances would share one "
            f"workflow address and their histories would merge"
        )
    if extra and union_is_complete:
        raise RuntimeError(
            f"❌ {label}: workflow_instance_params declares {sorted(extra)}, which no "
            f"member instances over. That address can never differ, and it tells a "
            f"reader the workflow varies by an axis it does not"
        )
    # Declaration order is the ADDRESS order, so it is preserved as written.
    return declared_list


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
    providers: list[str] | tuple[str, ...] = (),
) -> dict | None:
    """Resolve a run's target-instance identity BEFORE its dirs exist (§Phase 31 6b).

    target run: the target's declared target_instance_params -> Hive segments;
    workflow run: the ordered child target-instance addresses -> the sha256
    composition segment + the authoritative identity doc. Returns
    {instance_segments, address, target_addresses, identity_doc?} or None for
    run types without instance identity (fan_out/procedure/maintenance)."""
    if run_type not in ("target", "workflow"):
        return None
    execution_context = build_execution_context(
        ctl_cfg_root,
        action=action,
        ctl_profile=ctl_profile,
        execution_params=execution_params,
        providers=providers,
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
    member_actions: list = []
    for entry in workflow_cfg.get("target_runs", []):
        name = entry if isinstance(entry, str) else entry.get("target")
        if not name:
            continue
        addresses.append(target_instance_address(name, target_segments(name)))
        # §Phase 73: the member's ACTION is part of its identity, so a teardown of
        # a target and a deploy of the same target are different compositions.
        member_actions.append(entry.get("action") if isinstance(entry, dict) else None)
    if not addresses:
        raise RuntimeError(f"❌ workflow {workflow_name!r} resolves no target addresses")
    # §Phase 73: a workflow publishes HISTORY, not state — no composition digest,
    # no committed pointer. §Phase 78: that history is PARTITIONED by the axes its
    # members vary over, so one key fanned across environments keeps a separate
    # `last_run` per environment instead of one row for whichever finished last.
    # Params, not a hash: params ADDRESS (readable, predictable from cfg, stable
    # across cfg edits) where a hash IDENTIFIES, and identity is the question
    # Phase 73 decided ctl must not answer.
    instance_params = validate_workflow_instance_params(
        workflow_cfg.get("workflow_instance_params"),
        workflow_cfg,
        targets,
        label=f"workflow {workflow_name!r}",
        execution_context=execution_context,
    )
    segments = resolve_target_instance_segments(
        instance_params, execution_context, label=f"workflow {workflow_name!r}"
    )
    return {
        "instance_segments": segments,
        "address": workflow_instance_address(workflow_name, segments),
        "target_addresses": addresses,
        "member_actions": member_actions,
        "identity_doc": None,
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


def validate_execution_access(
    ctl_cfg_root: Path,
    ctl_profile: str,
    workflow_cfg: dict,
    inventory_cfg: dict,
    *,
    execution_context: dict[str, object],
    execution_access_modes: dict[str, str],
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
    provider_options: dict[str, str] | None,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
) -> None:
    """Validate the PROVIDER-NEUTRAL execution access policy: per-target mode
    consent, and the ctl-state sync / guardrail / cfg-gate skip permissions.

    Provider gating and each provider's own policy (mode, credential
    implementation, option grants) are validated separately — the
    `allowed_providers` and `provider_access_policy` checks — so this function
    names no provider vocabulary."""
    # consent: a mode may require each active target of that provider to have
    # declared, up front, that it accepts being run this way and with what
    # (§Phase 53). The ADAPTER says which of its modes need consent and which
    # target fields carry it; declaring the sources is not the same as opting in.
    targets = inventory_cfg.get("targets", {})
    for target_name in active_target_names(workflow_cfg):
        target_cfg = targets.get(target_name) or {}
        identities = target_cfg.get("execution_identities")
        if identities is None:
            continue  # coverage is validated separately
        # A target may declare several identities; consent is asked of EACH
        # provider that this run actually activates.
        for provider, execution in sorted((identities or {}).items()):
            if provider not in (execution_access_modes or {}):
                continue  # provider coverage is validated separately
            mode = execution_access_modes[str(provider)]
            consent = get_provider_adapter(str(provider)).target_consent(mode)
            if not consent:
                continue
            opt_in_field = consent["opt_in_field"]
            opt_in = target_cfg.get(opt_in_field, False)
            if not isinstance(opt_in, bool):
                raise RuntimeError(f"❌ target {opt_in_field} must be a boolean")
            if not opt_in:
                raise RuntimeError(
                    f"❌ execution access mode {mode!r} was requested, but target "
                    f"{target_name!r} does not set {opt_in_field}: true"
                )
            execution_field = consent["execution_field"]
            if not ((execution or {}).get(execution_field) or []):
                raise RuntimeError(
                    f"❌ execution access mode {mode!r} was requested, but target "
                    f"{target_name!r} declares no execution_identities.{provider}.{execution_field}"
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

def build_procedure_cfg(
    ctl_cfg_root: Path,
    action: str,
    *,
    source: str,
    ref: str,
    domain_name: str,
    procedure: str,
    execution_provider: str | None = None,
    execution_account: str | None = None,
    execution_role: str | None = None,
    action_role_class: str | None = None,
) -> tuple[dict, dict]:
    """Build a one-target cfg for a synthetic repo-local procedure run.

    The synthetic target is composed directly from CLI args and need not exist
    in targets/<action>/. Synthetic runs are local-only and do not publish ctl state.
    """
    target_sources = collect_resource(ctl_cfg_root, "target_sources")
    cfg_key_sets = collect_resource(ctl_cfg_root, "cfg_key_sets")
    # §Phase 60: a synthetic procedure target names a DOMAIN and takes the
    # whole of it — the operator is debugging one step, not authoring a contract.
    domain = str(domain_name).strip().strip("/")
    validate_domain_value(
        load_domain_registry(ctl_cfg_root), domain, label="synthetic procedure target domain"
    )
    resolved = {
        "source": source,
        "ref": ref,
        "procedure": procedure,
        "domains": [domain],
        "cfg_keys": {domain: ["*"]},
    }
    if execution_provider:
        # The synthetic target gets the same execution_identity block a declared
        # target has; a single --execution-role is bound under the key that
        # provider uses for this action — the engine asks, it does not decide.
        role_class = action_role_class or get_provider_adapter(
            execution_provider
        )._action_role_key(action, label="synthetic procedure target")
        resolved["execution_identities"] = validate_target_execution_identities(
            {
                "provider": execution_provider,
                "account": execution_account,
                "roles": {role_class: execution_role},
            },
            label="synthetic procedure target",
        )
    name = "procedure"
    inventory_cfg = {"target_sources": target_sources, "targets": {name: resolved}}
    workflow_cfg = {
        "meta": {"name": f"procedure/{source}/{procedure}", "action": action},
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
        # §Phase 73: a fan-out expands WORKFLOWS. To fan a target, wrap it in a
        # workflow, exactly as a step is only reachable through a procedure. One
        # child kind leaves ONE place in cfg where a target's action is declared
        # — the workflow member — so the two mechanisms cannot disagree about
        # what a target does.
        workflow_key = run.get("workflow_key")
        if not workflow_key:
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} run[{i}] must set workflow_key"
            )
        if run.get("target_key"):
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} run[{i}] sets target_key; a fan-out "
                "expands workflows only. Wrap the target in a workflow and name that"
            )
        kind = "workflow"
        key = workflow_key
        param_set_key = run.get("fan_out_param_set_key")
        # §Phase 59: `extra_params` adds the SAME param to every member of the
        # referenced set, so one account list can serve several domains instead
        # of being copied per domain. Additive only — a key already declared by
        # a member is a hard error, never a silent override.
        extra_params = run.get("extra_params")
        if extra_params is not None:
            run_label = f"fan-out {fan_out_key!r} run[{i}] extra_params"
            if not isinstance(extra_params, dict) or not extra_params:
                raise RuntimeError(f"❌ {run_label} must be a non-empty map")
            if param_set_key is None:
                raise RuntimeError(
                    f"❌ {run_label} requires fan_out_param_set_key "
                    "(there are no members to add the params to)"
                )
            for extra_key, extra_value in extra_params.items():
                if not isinstance(extra_key, str) or not CONTEXT_KEY_RE.fullmatch(extra_key):
                    raise RuntimeError(f"❌ {run_label}: key {extra_key!r} must be a valid identifier")
                if isinstance(extra_value, (dict, list)):
                    raise RuntimeError(f"❌ {run_label}.{extra_key} must be a scalar")
            if "domain" in extra_params:
                validate_domain_value(domains, extra_params["domain"], label=run_label)
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
            if extra_params:
                collisions = sorted(set(params) & set(extra_params))
                if collisions:
                    raise RuntimeError(
                        f"❌ {member_label} already declares {collisions} also set by "
                        f"fan-out {fan_out_key!r} run[{i}] extra_params; define each param "
                        "in one place"
                    )
            if not selector_matches(
                member.get("selectors"), execution_context,
                label=member_label, structured_only=True,
            ):
                continue
            children.append(
                {
                    "kind": kind,
                    "key": key,
                    "params": {**params, **(extra_params or {})},
                    # One param set may serve several runs of the same
                    # workflow (each pinned by different extra_params), so the
                    # member name alone no longer identifies a child.
                    "label": (
                        f"{key}[{'+'.join(str(v) for v in extra_params.values())}:{entry_name}]"
                        if extra_params
                        else f"{key}[{entry_name}]"
                    ),
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
    # Fan-out children run SEQUENTIALLY. Each child acquires the ctl-state lock,
    # which is exclusive and non-blocking over the whole local root, so a second
    # concurrent child fails outright. `max_parallel` therefore described a knob
    # that could not be turned; it is removed rather than left as a trap. Running
    # disjoint children in parallel needs a finer-grained lock (per namespace or
    # per instance) — recorded as tech debt, not a cfg setting.
    if "max_parallel" in fan_out:
        raise RuntimeError(
            f"❌ fan-out {fan_out_key!r} declares max_parallel, which is removed: "
            "children run sequentially because each acquires the exclusive "
            "ctl-state lock. Delete the key."
        )
    failure_mode = fan_out.get("failure_mode", "stop")
    if failure_mode not in ("stop", "continue"):
        raise RuntimeError(f"❌ fan-out {fan_out_key!r} failure_mode must be 'stop' or 'continue'")
    return {"failure_mode": failure_mode, "children": children}



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
    provider_options: dict[str, str] | None,
    execution_access_modes: dict[str, str],
    target_name: str | None = None,
    procedure_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    enforce_ctl_policy: bool = True,
    load_provider_catalogs: bool = True,
    providers: list[str] | tuple[str, ...] = (),
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
        providers=providers,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        execution_access_modes=execution_access_modes,
        execution_runtime_mode=execution_runtime_mode,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
    )
    if enforce_ctl_policy:
        validate_execution_context_constraints(ctl_cfg_root, execution_context)
    require_commit_refs = ref_policy_requires_commits(ctl_ref_policy)

    if procedure_run:
        workflow_cfg, inventory_cfg = build_procedure_cfg(
            ctl_cfg_root,
            inventory_name,
            source=procedure_run["source"],
            ref=procedure_run["ref"],
            domain_name=procedure_run["domain"],
            procedure=procedure_run["procedure"],
            execution_provider=procedure_run.get("execution_provider"),
            execution_account=procedure_run.get("execution_account"),
            execution_role=procedure_run.get("execution_role"),
        )
        selection_kind = "procedure"
        selection_key = procedure_run["procedure"]
    elif target_name:
        # A standalone target run has no members, so the inventory is filtered by
        # the invoked action alone.
        inventory_cfg = load_inventory_cfg(
            ctl_cfg_root, inventory_name, execution_context
        )
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
        inventory_cfg = load_inventory_cfg(
            ctl_cfg_root, inventory_name, execution_context,
            member_actions=workflow_member_actions(workflow_cfg),
        )
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

    if not procedure_run:
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
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
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
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
    force_skip_execution_identity_preflight_check: list[str],
) -> dict:
    """Evaluate run policy independently from provider identity reachability."""
    checks: list[dict] = []

    def check(name: str, validator) -> None:
        try:
            detail = validator()
            entry = {"name": name, "status": "passed"}
            # A validator MAY return render lines (provider-authored strings the
            # engine prints verbatim); it never composes provider vocabulary itself.
            if detail:
                entry["detail"] = list(detail) if not isinstance(detail, dict) else detail
            checks.append(entry)
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

    def _providers_check() -> list[str]:
        declared = sorted(execution_access_modes or {})
        validate_ctl_allowed_providers(ctl_cfg_root, ctl_profile, declared)
        return [f"declared: {', '.join(declared) or '(none)'}"]

    check("allowed_providers", _providers_check)

    # per-provider access authorization, each provider's own policy block. The
    # ADAPTER returns the render lines; the engine nests them, naming no mode.
    def _provider_access_detail() -> dict[str, list[str]]:
        rows: dict[str, list[str]] = {}
        for provider, mode in sorted((execution_access_modes or {}).items()):
            rows[provider] = get_provider_adapter(provider).authorize_run(
                ctl_profile_provider_policy(ctl_cfg_root, ctl_profile, provider),
                execution_access_mode=mode,
                provider_options=provider_options_for(provider_options, provider),
                label=f"ctl profile {ctl_profile!r} policy for {provider!r}",
            )
        return rows

    check("provider_access_policy", _provider_access_detail)

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
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
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
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    force_skip_providers: list[str],
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
        "execution_identities": None,
        "provider": None,
        "access_mode": None,
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
    # The backend's own provider decides the mode and options here — it is not
    # necessarily the provider the targets run against.
    namespace_provider = str(entry["provider"])
    namespace_adapter = get_provider_adapter(namespace_provider)
    namespace_access_mode, namespace_adapter_options = provider_inputs(
        namespace_provider, execution_access_modes, provider_options
    )
    operation_execution = ctl_state_backend_operation_execution(
        entry, "sync", namespace_key=namespace_key, required=False
    )
    if operation_execution is None:
        if not namespace_adapter.resolves_execution_identity(namespace_access_mode):
            result["reason"] = "identity bypass: synchronizer uses the substitute credential"
            return result
        result["status"] = "failed"
        result["failure_reason"] = (
            f"ctl_state_backends.{namespace_key} declares no execution_identity.operations.sync"
        )
        return result
    # Ctl-state publication always uses its normal operation role path.
    sync_access_mode = ctl_state_publication_access_mode(
        namespace_adapter, namespace_access_mode
    )
    provider_adapter = namespace_adapter
    try:
        checked = provider_adapter.preflight_execution_identity(
            f"ctl_state_backend/{namespace_key}",
            {"execution_identities": operation_execution},
            selection["provider_catalogs"],
            execution_context=selection["execution_context"],
            implementation_key=implementation_key,
            execution_access_mode=sync_access_mode,
            provider_options=namespace_adapter_options,
            live_check=namespace_provider not in force_skip_providers,
        )
        if not isinstance(checked, dict) or checked.get("status") not in PREFLIGHT_RESULT_STATUSES:
            raise RuntimeError("provider preflight returned an invalid result")
    except Exception as error:
        result["status"] = "failed"
        result["execution_identities"] = operation_execution
        result["failure_reason"] = credential_free_preflight_failure_reason(error)
        return result
    checked = dict(checked)
    checked["ctl_state_backend"] = namespace_key
    return checked


def build_execution_identity_preflight_report(
    selection: dict,
    *,
    implementation_key: str,
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    force_skip_providers: list[str],
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
            adapter_access_mode, adapter_options = provider_inputs(
                provider_adapter.PROVIDER_NAME,
                execution_access_modes,
                provider_options,
            )
            result = provider_adapter.preflight_execution_identity(
                target_run_id,
                target_run,
                catalogs,
                execution_context=execution_context,
                implementation_key=implementation_key,
                execution_access_mode=adapter_access_mode,
                provider_options=adapter_options,
                live_check=provider_adapter.PROVIDER_NAME not in force_skip_providers,
            )
            if not isinstance(result, dict):
                raise RuntimeError("provider preflight returned a non-mapping result")
        except Exception as error:
            result = {
                "execution_identities": target_run.get("execution_identities"),
                # this adapter's own view: it preflights the identity it owns
                "provider": provider_adapter.PROVIDER_NAME,
                "access_mode": execution_access_mode_for(
                    execution_access_modes, provider_adapter.PROVIDER_NAME
                ) if (target_run.get("execution_identities") or {}) else None,
                "status": "failed",
                "provider_path": [],
                "failure_reason": credential_free_preflight_failure_reason(error),
            }
        status = result.get("status")
        if status not in PREFLIGHT_RESULT_STATUSES:
            result = {
                "execution_identities": result.get("execution_identities"),
                "provider": result.get("provider"),
                "access_mode": result.get("access_mode"),
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
                execution_access_modes=execution_access_modes,
                provider_options=provider_options,
                force_skip_providers=force_skip_providers,
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
            backend_execution = describe_target_execution_identity(result.get("execution_identities"))
            if backend_execution:
                children.append(
                    _node(f"execution_identity: {backend_execution} {tag(status)}")
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
        identity_key = describe_target_execution_identity(result.get("execution_identities")) or "<unresolved>"
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
        children: list[dict] = []
        if check.get("failure_reason"):
            children.append(_node(f"error: {check['failure_reason']}"))
        # Detail is provider-authored render text (§Phase 52): a flat list of
        # lines, or a mapping of provider -> lines for the per-provider check.
        detail = check.get("detail")
        if isinstance(detail, dict):
            for name in sorted(detail):
                children.append(
                    _node(f"provider: {name}", [_node(line) for line in detail[name]])
                )
        elif detail:
            children.extend(_node(line) for line in detail)
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


def run_gates_dir(run_dir: Path) -> Path:
    """The run's GATES dir — a top-level sibling of artifacts/ and logs/.

    Holds the verdicts that decide whether the run may proceed (cfg validation,
    target-cfg validation, ctl-policy, execution-identity preflight), kept apart
    from artifacts/ (what the run PRODUCES) and logs/ (what it SAID). Published
    with the run record (RUN_RECORD_MEMBERS).
    """
    return Path(run_dir) / "gates"


def write_ctl_policy_preflight_artifacts(
    gates_dir: Path, report: dict
) -> None:
    text_path = Path(gates_dir) / "ctl_policy_validation.txt"
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


def write_cfg_validation_artifacts(gates_dir: Path, report: dict) -> None:
    text_path = Path(gates_dir) / "cfg_validation.txt"
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
    domain + instance-schema group selectors (recorded at inventory load),
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
    # §Phase 53: the target declares its execution inline, so the only axis it can
    # consume is the `account` template. The old walk over nested identity groups
    # (one dispatch axis per level) is gone with the groups themselves; per-action
    # variation is now `execution.roles`, keyed by authorization class, which
    # consumes no params axis.
    execution = inventory_target.get("execution_identities")
    if isinstance(execution, dict):
        consumed |= _template_param_axes(execution.get("account"))
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
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
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
            adapter_access_mode, adapter_options = provider_inputs(
                provider_adapter.PROVIDER_NAME,
                execution_access_modes,
                provider_options,
            )
            result = provider_adapter.resolve_target_cfg_references(
                target_run_id,
                target_run,
                catalogs,
                execution_context=execution_context,
                implementation_key=implementation_key,
                execution_access_mode=adapter_access_mode,
                provider_options=adapter_options,
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


def write_target_cfg_validation_artifacts(gates_dir: Path, report: dict) -> None:
    text_path = Path(gates_dir) / "target_cfg_validation.txt"
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
    gates_dir: Path, report: dict
) -> None:
    text_path = Path(gates_dir) / "execution_identity_preflight.txt"
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
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    implementation_key: str,
    force_skip_execution_identity_preflight_check: list[str],
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
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
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
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            ctl_cfg_root=ctl_cfg_root,
        )
        identity_report = build_execution_identity_preflight_report(
            selection,
            implementation_key=implementation_key,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            force_skip_providers=force_skip_execution_identity_preflight_check,
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
    provider_options: dict[str, str] | None,
    execution_access_modes: dict[str, str],
    artifacts_dir: Path,
    gates_dir: Path,
    target_name: str | None = None,
    procedure_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
) -> tuple[dict, dict]:
    """Single-runner (workflow/target/procedure) preflight: the same four
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
        provider_options=provider_options,
        execution_access_modes=execution_access_modes,
        target_name=target_name,
        procedure_run=procedure_run,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        force_skip_guardrails=force_skip_guardrails,
        force_skip_full_cfg_validation_gate=force_skip_full_cfg_validation_gate,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
        enforce_ctl_policy=False,
        load_provider_catalogs=False,
        providers=providers,
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
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
        implementation_key=provider_implementation_key,
        force_skip_execution_identity_preflight_check=(
            force_skip_execution_identity_preflight_check
        ),
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
    )
    write_cfg_validation_artifacts(gates_dir, cfg_report)
    write_target_cfg_validation_artifacts(gates_dir, reports["target_cfg"])
    write_ctl_policy_preflight_artifacts(gates_dir, reports["policy"])
    write_execution_identity_preflight_artifacts(gates_dir, reports["identity"])
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
    log_file: Path,
    provider_options: dict[str, str] | None,
    execution_runtime_mode: str,  # required, no default — the CLI (--execution-runtime-mode) supplies it
    target_name: str | None = None,
    procedure_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_access_modes: dict[str, str] | None = None,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
    skip_up_to_date: bool = False,
    credential_refresh_modes: dict | None = None,
    skip_children_precheck: bool = False,
    parent_graph_provisions_ctl_state_backend: bool = False,
    parent_ctl_state_backend_absence_confirmed: bool = False,
    preflight_selection: dict | None = None,
) -> None:
    """
    Run a declared workflow, declared target, or synthetic repo-local procedure.

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
            # a run DECLARES its providers; without this the selection resolves
            # with none and every provider lookup fails ("no providers declared")
            providers=providers,
            target_repo_key=target_repo_key,
            require_target_ref=require_target_ref,
            provider_implementation_key=provider_implementation_key,
            execution_runtime_mode=execution_runtime_mode,
            provider_options=provider_options,
            execution_access_modes=execution_access_modes,
            artifacts_dir=artifacts_dir,
            gates_dir=run_gates_dir(run_dir),
            target_name=target_name,
            procedure_run=procedure_run,
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
    # §Phase 73: recorded as soon as the composition is RESOLVED, not after the
    # cfg and guardrail phases. Those take tens of seconds, and a status read
    # during them showed a running workflow with no members — the composition
    # was known the whole time and simply had not been written down.
    if load_run_metadata(run_dir).get("run_type") == "workflow":
        record_workflow_members(run_dir, active_target_runs, workflow_cfg)
    provider_adapter = selection["provider_adapter"]
    provider_catalogs = selection["provider_catalogs"]

    # Preserve the runtime binding contract after the live gate passes.
    adapter_access_mode, adapter_options = provider_inputs(
        run_provider(execution_context), execution_access_modes, provider_options
    )
    provider_adapter.validate_active_target_access(
        active_target_runs,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=adapter_access_mode,
        provider_options=adapter_options,
    )

    selected_graph_provisions_backend = parent_graph_provisions_ctl_state_backend
    backend_absence_confirmed = parent_ctl_state_backend_absence_confirmed
    if agreed_defer_ctl_state_backend_sync and not selected_graph_provisions_backend:
        graph_probe = inspect_selected_graph_ctl_state_backend(
            [selection],
            ctl_cfg_root,
            implementation_key=provider_implementation_key,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
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
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
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
    active_target_runs, pipeline_run_cfg_path, final_plt_overlays = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
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
    update_run_metadata(run_dir, {"plt_overlays": final_plt_overlays})
    # §Phase 61(b) derivation chain, PER TARGET: each target_run merges its own
    # scopes with its own overlays, renders them, is guard-verified against its own
    # rendered values, and receives its projected key view. Nothing is shared, so a
    # target's cfg cannot be reshaped by another target's declarations.
    run_type_now = str(load_run_metadata(run_dir).get("run_type"))
    plt_targets_dir_path = target_cfg_views_root(run_dir, run_type_now)
    # §Phase 61(d): for a WORKFLOW this loop is a PRE-CHECK of its CHILDREN — each
    # spawned target re-derives and re-validates its own cfg when it runs. Skipping
    # trades fail-fast (catch a bad target before target #1 mutates anything) for not
    # doing the work twice. A target run has no children, so the flag is a no-op.
    precheck_runs = active_target_runs
    if skip_children_precheck and load_run_metadata(run_dir).get("run_type") == "workflow":
        logging.info(
            "Skipping the child pre-check (--skip-children-precheck); each target "
            "renders and validates its own cfg when it runs"
        )
        precheck_runs = {}
        update_run_metadata(run_dir, {"skipped_children_precheck": True})
    for target_run_id, target_run in precheck_runs.items():
        if not target_run.get("domains"):
            continue
        target_context = build_target_execution_context(
            target_run_id, target_run, execution_context
        )
        target_rendered_dir = prepare_target_cfg_view(
            target_run_id, target_run,
            plt_cfg_root=plt_cfg_root,
            target_cfg_dir=target_cfg_view_dir(run_dir, run_type_now, target_run_id),
            ctl_profile=ctl_profile,
            scope_params=scope_params_from_context(target_context),
            execution_context=target_context,
        )
        verify_guardrails(
            ctl_cfg_root,
            plt_cfg_root,
            guardrails_cfg_root,
            target_rendered_dir,
            target_context,
            scope_params_from_context(target_context),
        )

    if procedure_run:
        target_keys = procedure_run.get("affected_target_keys") or []
        if inventory_name in MUTATING_ACTIONS and not target_keys:
            raise RuntimeError("❌ mutating procedure runs require affected_target_keys")
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
        plt_overlays=final_plt_overlays,
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
        pipeline_run_cfg_path, plt_targets_dir_path, run_type_now
    )
    finalize_target_cfg_view_facts(
        active_target_runs, plt_targets_dir_path, pipeline_run_cfg_path
    )
    if load_run_metadata(run_dir).get("run_type") == "target":
        only_target = next(iter(active_target_runs.values()), None)
        if only_target and only_target.get("target_definition") is not None:
            write_yaml_file(
                run_dir / "cfg" / "ctl" / "target_definition.yaml",
                only_target["target_definition"],
            )
        if only_target:
            update_run_metadata(
                run_dir,
                {
                    key: only_target[key]
                    for key in (
                        "target_definition_sha256", "target_cfg_view_sha256"
                    )
                },
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

    # §Phase 61(d): ONE frozen spec describing this invocation, from which every
    # child target's argv is derived. Built here because run_pipeline is the only
    # place that holds all of it; passing scattered locals into run_targets is how
    # a flag gets forgotten and a child silently runs differently.
    run_metadata_now = load_run_metadata(run_dir)
    child_command_spec = {
        "ctl_entrypoint": Path(__file__).resolve().parents[3]
            / "atlas-ctl-orchestrator" / "ctl.py",
        "ctl_cfg_root": ctl_cfg_root,
        "ctl_profile": ctl_profile,
        "ctl_state_local_root": run_metadata_now.get("ctl_state_local_root"),
        "execution_runtime_mode": execution_runtime_mode,
        "action": inventory_name,
        "providers": list(run_providers(execution_context)),
        "execution_params": dict(execution_params),
        "provider_options": dict(provider_options or {}),
        "execution_access_modes": dict(execution_access_modes or {}),
        "plt_overlays": list(final_plt_overlays or []),
        "force_skip_execution_identity_preflight_check":
            list(force_skip_execution_identity_preflight_check or []),
        "agreed_defer_ctl_state_backend_sync": agreed_defer_ctl_state_backend_sync,
        "force_skip_ctl_state_backend_sync": force_skip_ctl_state_backend_sync,
        "force_skip_guardrails": force_skip_guardrails,
        "force_skip_full_cfg_validation_gate": force_skip_full_cfg_validation_gate,
        "skip_children_precheck": skip_children_precheck,
        "credential_refresh_modes": credential_refresh_modes,
    }
    write_yaml_file(
        artifacts_dir / "child_command_spec.yaml",
        {k: (str(v) if isinstance(v, Path) else v) for k, v in child_command_spec.items()},
    )

    # Run target runs
    credential_refresh_modes = validate_credential_refresh_modes(
        ctl_cfg_root, ctl_profile, credential_refresh_modes, providers,
        execution_access_modes,
    )
    run_targets(
        active_target_runs, run_dir, plt_targets_dir_path, execution_context_path,
        inventory_name, execution_context, run_id,
        child_command_spec=child_command_spec,
        tooling_refs=tooling_refs,
        use_local_tooling_cfg=use_local_tooling_cfg,
        provider_adapter=provider_adapter,
        provider_catalogs=provider_catalogs,
        provider_implementation_key=provider_implementation_key,
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
        execution_runtime_mode=execution_runtime_mode,
        skip_up_to_date=skip_up_to_date,
    )

    # §Phase 61(b3): a WORKFLOW owns ordering, policy and the run verdict — not cfg.
    # Each child has received its complete derivation, so the workflow-side copy is
    # dropped rather than published twice.
    if load_run_metadata(run_dir).get("run_type") == "workflow":
        workflow_plt_dir = run_dir / "cfg" / "plt"
        if workflow_plt_dir.exists():
            shutil.rmtree(workflow_plt_dir)

    print_run_summary(run_id, log_file)
