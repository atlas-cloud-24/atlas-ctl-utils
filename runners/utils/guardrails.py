"""Unified typed guardrail policies and exact-value baselines."""

import collections
import json
import math
import os
import re
import tempfile
from pathlib import Path

import yaml


POLICIES_KEY = "guardrail_policies"
BASELINES_KEY = "guardrail_baselines"
LEGACY_KEYS = {
    "plt_guardrail_policies",
    "plt_guardrail_baselines",
    "ctl_guardrail_declarations",
    "ctl_guardrail_baselines",
}
SUBJECT_KINDS = {
    "ctl": {"ctl_cfg", "execution_context"},
    "plt": {"plt_rendered_target"},
}
TOKEN_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")
EXACT_TOKEN_RE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_.]*)\}$")


def _common():
    from utils import common

    return common


def _schema_path(name: str) -> Path:
    return Path(__file__).resolve().parents[2] / "schemas" / name


def _validate_schema(data, schema_name: str, *, origin: Path) -> None:
    try:
        from jsonschema import Draft202012Validator
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "❌ guardrail schema validation requires the jsonschema Python package"
        ) from exc
    schema_path = _schema_path(schema_name)
    if not schema_path.is_file():
        raise RuntimeError(f"❌ guardrail schema is missing: {schema_path}")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(schema).iter_errors(data),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.absolute_path) or "<root>"
        raise RuntimeError(
            f"❌ guardrail schema validation failed at {location}: "
            f"{error.message}: {origin}"
        )


def _validate_native(value, *, label: str):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RuntimeError(f"❌ {label} must not contain NaN or infinity")
        return value
    if isinstance(value, list):
        return [
            _validate_native(item, label=f"{label}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise RuntimeError(f"❌ {label} mapping keys must be strings")
            result[key] = _validate_native(item, label=f"{label}.{key}")
        return result
    raise RuntimeError(
        f"❌ {label} must be a YAML/JSON-compatible native value, got "
        f"{type(value).__name__}"
    )


def _canonical(value, *, label: str) -> str:
    value = _validate_native(value, label=label)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def json_pointer_tokens(pointer: str, *, label: str) -> tuple[str, ...]:
    if not isinstance(pointer, str) or not pointer:
        raise RuntimeError(f"❌ {label} must be a non-empty JSON Pointer")
    if not pointer.startswith("/"):
        raise RuntimeError(f"❌ {label} must start with /: {pointer!r}")
    tokens = []
    for raw in pointer[1:].split("/"):
        if re.search(r"~(?:[^01]|$)", raw):
            raise RuntimeError(f"❌ {label} has an invalid JSON Pointer escape: {pointer!r}")
        tokens.append(raw.replace("~1", "/").replace("~0", "~"))
    return tuple(tokens)


def json_pointer_get(document, pointer: str, *, label: str):
    current = document
    for token in json_pointer_tokens(pointer, label=label):
        if isinstance(current, dict):
            if token not in current:
                raise RuntimeError(
                    f"❌ {label} does not exist; missing mapping key {token!r}"
                )
            current = current[token]
            continue
        if isinstance(current, list):
            if not token.isdigit() or (len(token) > 1 and token.startswith("0")):
                raise RuntimeError(
                    f"❌ {label} uses invalid list index {token!r}"
                )
            index = int(token)
            if index >= len(current):
                raise RuntimeError(
                    f"❌ {label} list index {index} is out of range"
                )
            current = current[index]
            continue
        raise RuntimeError(
            f"❌ {label} traverses through {type(current).__name__} at {token!r}"
        )
    return current


def _reject_overlapping_paths(paths: list[str], *, label: str) -> None:
    token_paths = [
        (path, json_pointer_tokens(path, label=f"{label}.{path}"))
        for path in paths
    ]
    for index, (left, left_tokens) in enumerate(token_paths):
        for right, right_tokens in token_paths[index + 1 :]:
            prefix_len = min(len(left_tokens), len(right_tokens))
            if left_tokens[:prefix_len] == right_tokens[:prefix_len]:
                raise RuntimeError(
                    f"❌ {label} contains overlapping protected paths "
                    f"{left!r} and {right!r}"
                )


def _normalize_subject(raw, *, label: str, allow_instance: bool) -> dict:
    if not isinstance(raw, dict):
        raise RuntimeError(f"❌ {label} must be a mapping")
    allowed = {"kind", "target_path"}
    if allow_instance:
        allowed.add("instance")
    unknown = set(raw) - allowed
    if unknown or "kind" not in raw:
        raise RuntimeError(
            f"❌ {label} must contain kind and only supported subject fields; "
            f"unsupported: {sorted(unknown)}"
        )
    kind = raw["kind"]
    if kind not in {"ctl_cfg", "execution_context", "plt_rendered_target"}:
        raise RuntimeError(f"❌ {label}.kind is unsupported: {kind!r}")
    subject = {"kind": kind}
    if kind == "plt_rendered_target":
        if "target_path" not in raw:
            raise RuntimeError(
                f"❌ {label}.target_path is required for plt_rendered_target"
            )
        subject["target_path"] = _common().normalize_cfg_absolute_path(
            raw["target_path"],
            label=f"{label}.target_path",
        )
    elif "target_path" in raw:
        raise RuntimeError(
            f"❌ {label}.target_path is valid only for plt_rendered_target"
        )
    if "instance" in raw:
        instance = raw["instance"]
        if not isinstance(instance, dict) or set(instance) != {"params"}:
            raise RuntimeError(
                f"❌ {label}.instance must contain exactly non-empty params"
            )
        raw_params = instance["params"]
        if not isinstance(raw_params, dict) or not raw_params:
            raise RuntimeError(f"❌ {label}.instance.params must be a non-empty mapping")
        params = {}
        for ref, value in raw_params.items():
            ref = _common().validate_execution_context_ref(
                ref,
                label=f"{label}.instance.params",
            )
            if not ref.startswith(_common().EXECUTION_CONTEXT_PARAMS_PREFIX):
                raise RuntimeError(
                    f"❌ {label}.instance.params keys must be params refs: {ref!r}"
                )
            params[ref] = _validate_native(
                value,
                label=f"{label}.instance.params.{ref}",
            )
        subject["instance"] = {"params": dict(sorted(params.items()))}
    return subject


def subject_identity(subject: dict) -> str:
    normalized = _normalize_subject(
        subject,
        label="guardrail subject",
        allow_instance=True,
    )
    return _canonical(normalized, label="guardrail subject")


def subject_without_instance(subject: dict) -> dict:
    return {key: value for key, value in subject.items() if key != "instance"}


def guardrail_policy_sources(cfg_root: Path, *, owner: str) -> list[Path]:
    if owner == "plt":
        sources = []
        file_path = cfg_root / _common().PLT_GUARDRAILS_FILENAME
        dir_path = cfg_root / _common().PLT_GUARDRAILS_DIRNAME
        if file_path.is_file():
            sources.append(file_path)
        if dir_path.is_dir():
            sources.extend(
                sorted(path for path in dir_path.rglob("*.yaml") if path.is_file())
            )
        return sources
    if owner == "ctl":
        return sorted(path for path in cfg_root.rglob("*.yaml") if path.is_file())
    raise RuntimeError(f"❌ unknown guardrail policy owner: {owner!r}")


def load_guardrail_policies(cfg_root: Path, *, owner: str) -> dict[str, dict]:
    if owner not in SUBJECT_KINDS:
        raise RuntimeError(f"❌ unknown guardrail policy owner: {owner!r}")
    policies = {}
    origins = {}
    for path in guardrail_policy_sources(cfg_root, owner=owner):
        data = _common().load_yaml(path) or {}
        if not isinstance(data, dict):
            raise RuntimeError(f"❌ guardrail cfg must contain a mapping: {path}")
        legacy = sorted(set(data) & LEGACY_KEYS)
        if legacy:
            raise RuntimeError(
                f"❌ legacy guardrail collections {legacy} are unsupported; "
                f"use {POLICIES_KEY}: {path}"
            )
        if POLICIES_KEY not in data:
            continue
        if owner == "plt" and set(data) != {POLICIES_KEY}:
            raise RuntimeError(
                f"❌ PLT guardrail files must contain only {POLICIES_KEY}: {path}"
            )
        _validate_schema(
            {POLICIES_KEY: data[POLICIES_KEY]},
            "guardrail-policies.schema.json",
            origin=path,
        )
        raw_policies = data[POLICIES_KEY]
        if not isinstance(raw_policies, dict) or not raw_policies:
            raise RuntimeError(f"❌ {POLICIES_KEY} must be a non-empty mapping: {path}")
        for raw_name, raw_policy in raw_policies.items():
            if not isinstance(raw_name, str) or not raw_name.strip():
                raise RuntimeError(f"❌ guardrail policy names must be non-empty: {path}")
            name = raw_name.strip()
            if name in policies:
                raise RuntimeError(
                    f"❌ duplicate guardrail policy {name!r}: {path} "
                    f"(also in {origins[name]})"
                )
            label = f"{POLICIES_KEY}.{name} in {path}"
            if not isinstance(raw_policy, dict):
                raise RuntimeError(f"❌ {label} must be a mapping")
            unknown = set(raw_policy) - {
                "subject",
                "selectors",
                "instance_params",
                "protected_paths",
            }
            if unknown:
                raise RuntimeError(f"❌ {label} has unsupported keys {sorted(unknown)}")
            subject = _normalize_subject(
                raw_policy.get("subject"),
                label=f"{label}.subject",
                allow_instance=False,
            )
            if subject["kind"] not in SUBJECT_KINDS[owner]:
                raise RuntimeError(
                    f"❌ {label}.subject.kind {subject['kind']!r} is invalid "
                    f"for {owner} policies"
                )
            selectors = raw_policy.get("selectors") or {}
            selector_refs = _common().selector_requirements(
                selectors,
                label=f"{label}.selectors",
                structured_only=True,
            )
            raw_params = raw_policy.get("instance_params") or []
            if not isinstance(raw_params, list):
                raise RuntimeError(f"❌ {label}.instance_params must be a list")
            instance_params = []
            for raw_ref in raw_params:
                ref = _common().validate_execution_context_ref(
                    raw_ref,
                    label=f"{label}.instance_params",
                )
                if not ref.startswith(_common().EXECUTION_CONTEXT_PARAMS_PREFIX):
                    raise RuntimeError(
                        f"❌ {label}.instance_params entries must be params refs"
                    )
                if ref in instance_params:
                    raise RuntimeError(
                        f"❌ duplicate instance param {ref!r}: {label}"
                    )
                instance_params.append(ref)
            missing_selector_params = sorted(
                set(selector_refs) - set(instance_params)
            )
            if missing_selector_params:
                raise RuntimeError(
                    f"❌ {label}.instance_params must include every selector ref; "
                    f"missing {missing_selector_params}"
                )
            raw_paths = raw_policy.get("protected_paths")
            if (
                not isinstance(raw_paths, list)
                or not raw_paths
                or any(not isinstance(item, str) for item in raw_paths)
            ):
                raise RuntimeError(
                    f"❌ {label}.protected_paths must be a non-empty list"
                )
            protected_paths = []
            for pointer in raw_paths:
                json_pointer_tokens(pointer, label=f"{label}.protected_paths")
                if pointer in protected_paths:
                    raise RuntimeError(
                        f"❌ duplicate protected path {pointer!r}: {label}"
                    )
                protected_paths.append(pointer)
            _reject_overlapping_paths(protected_paths, label=f"{label}.protected_paths")
            policies[name] = {
                "name": name,
                "subject": subject,
                "selectors": selectors,
                "instance_params": tuple(sorted(instance_params)),
                "protected_paths": tuple(protected_paths),
                "origin": path,
            }
            origins[name] = path
    return policies


def active_guardrail_policies(
    policies: dict[str, dict],
    execution_context: dict[str, object],
    *,
    target_paths: set[str] | None = None,
) -> list[dict]:
    active = []
    for policy in policies.values():
        target_path = policy["subject"].get("target_path")
        if target_paths is not None and target_path not in target_paths:
            continue
        if _common().selector_matches(
            policy["selectors"],
            execution_context,
            label=f"guardrail policy {policy['name']!r}",
            structured_only=True,
        ):
            active.append(policy)
    return sorted(active, key=lambda policy: policy["name"])


def _group_active_policies(
    policies: list[dict],
    execution_context: dict[str, object],
) -> list[tuple[dict, list[dict]]]:
    by_base = collections.defaultdict(list)
    for policy in policies:
        by_base[subject_identity(policy["subject"])].append(policy)
    groups = []
    for base_policies in by_base.values():
        declarations = {
            policy["instance_params"] for policy in base_policies
        }
        if len(declarations) != 1:
            detail = ", ".join(
                f"{policy['name']}={list(policy['instance_params'])}"
                for policy in base_policies
            )
            raise RuntimeError(
                "❌ active policies for one guardrail subject must declare the "
                f"same instance_params: {detail}"
            )
        params = next(iter(declarations))
        subject = dict(base_policies[0]["subject"])
        if params:
            values = {}
            for ref in params:
                if ref not in execution_context:
                    raise RuntimeError(
                        f"❌ guardrail instance param {ref!r} has no value in this run"
                    )
                values[ref] = _validate_native(
                    execution_context[ref],
                    label=f"guardrail instance param {ref}",
                )
            subject["instance"] = {"params": dict(sorted(values.items()))}
        paths = [
            pointer
            for policy in base_policies
            for pointer in policy["protected_paths"]
        ]
        if len(paths) != len(set(paths)):
            raise RuntimeError(
                f"❌ active policies for subject {subject} protect a path more than once"
            )
        _reject_overlapping_paths(paths, label=f"active policies for {subject}")
        groups.append((subject, base_policies))
    groups.sort(key=lambda item: subject_identity(item[0]))
    return groups


def _scope_selector_refs(scopes: list[dict], *, target_path: str) -> set[str]:
    refs = set()
    for scope in scopes:
        refs.update(
            _common().selector_requirements(
                scope.get("selectors") or {},
                label=f"scope {scope['scope_path']} selectors for {target_path}",
                structured_only=True,
            )
        )
    return refs


def _validate_plt_instance_completeness(
    groups: list[tuple[dict, list[dict]]],
    scopes_by_target: dict[str, list[dict]],
) -> None:
    for subject, policies in groups:
        target_path = subject["target_path"]
        required = _scope_selector_refs(
            scopes_by_target[target_path],
            target_path=target_path,
        )
        declared = set(policies[0]["instance_params"])
        missing = sorted(required - declared)
        if missing:
            raise RuntimeError(
                f"❌ guardrail policies for {target_path} must explicitly include "
                f"every active scope selector ref in instance_params; missing {missing}"
            )


def _materialize_runtime_value(value, execution_context, *, label: str):
    if isinstance(value, str):
        exact = EXACT_TOKEN_RE.fullmatch(value)
        if exact:
            ref = exact.group(1)
            if ref not in execution_context:
                raise RuntimeError(
                    f"❌ {label}: {_common().execution_context_miss_message(execution_context, ref)}"
                )
            return _validate_native(
                execution_context[ref],
                label=f"{label} resolved from {ref}",
            )

        def replace(match: re.Match[str]) -> str:
            ref = match.group(1)
            if ref not in execution_context or execution_context[ref] is None:
                raise RuntimeError(
                    f"❌ {label}: {_common().execution_context_miss_message(execution_context, ref)}"
                )
            return str(execution_context[ref])

        resolved = TOKEN_RE.sub(replace, value)
        if "${" in resolved:
            raise RuntimeError(
                f"❌ {label} contains an unsupported or unresolved placeholder: {value!r}"
            )
        return resolved
    if isinstance(value, list):
        return [
            _materialize_runtime_value(
                item,
                execution_context,
                label=f"{label}[{index}]",
            )
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        return {
            key: _materialize_runtime_value(
                item,
                execution_context,
                label=f"{label}.{key}",
            )
            for key, item in value.items()
        }
    return _validate_native(value, label=label)


def _load_ctl_cfg_document(ctl_cfg_root: Path) -> dict:
    document = {}
    for path in sorted(ctl_cfg_root.rglob("*.yaml")):
        if ".git" in path.relative_to(ctl_cfg_root).parts:
            continue
        data = _common().load_yaml(path) or {}
        if not isinstance(data, dict):
            raise RuntimeError(f"❌ ctl cfg file must contain a mapping: {path}")
        data = {
            key: value
            for key, value in data.items()
            if key not in {POLICIES_KEY, *LEGACY_KEYS}
        }
        document = _common().merge_cfg_values(document, data)
    return document


def _load_rendered_target_document(rendered_dir: Path, target_path: str) -> dict:
    target_dir = _common().rendered_scope_target_dir(rendered_dir, target_path)
    if not target_dir.is_dir():
        raise RuntimeError(
            f"❌ rendered target directory does not exist for {target_path}: {target_dir}"
        )
    document = {}
    for path in sorted(target_dir.rglob("*.yaml")):
        if path.name in _common().SCOPE_META_SKIP_FILENAMES:
            continue
        data = _common().load_yaml(path) or {}
        if not isinstance(data, dict):
            raise RuntimeError(f"❌ rendered cfg file must contain a mapping: {path}")
        document = _common().merge_cfg_values(document, data)
    return document


def resolve_guardrail_values(
    subject: dict,
    protected_paths: list[str] | tuple[str, ...],
    *,
    ctl_cfg_root: Path,
    plt_rendered_dir: Path | None,
    execution_context: dict[str, object],
) -> dict[str, object]:
    kind = subject["kind"]
    if kind == "ctl_cfg":
        document = _load_ctl_cfg_document(ctl_cfg_root)
    elif kind == "execution_context":
        document = _common().execution_context_nested(execution_context)[
            _common().EXECUTION_CONTEXT_ROOT
        ]
    elif kind == "plt_rendered_target":
        if plt_rendered_dir is None:
            raise RuntimeError("❌ plt rendered cfg is required for PLT guardrails")
        document = _load_rendered_target_document(
            plt_rendered_dir,
            subject["target_path"],
        )
    else:
        raise RuntimeError(f"❌ unsupported guardrail subject kind: {kind!r}")
    values = {}
    for pointer in sorted(protected_paths):
        raw_value = json_pointer_get(
            document,
            pointer,
            label=f"guardrail subject {subject} path {pointer}",
        )
        values[pointer] = _materialize_runtime_value(
            raw_value,
            execution_context,
            label=f"guardrail subject {subject} path {pointer}",
        )
    return values


def materialize_ctl_guardrails(
    ctl_cfg_root: Path,
    execution_context: dict[str, object],
) -> list[dict]:
    policies = load_guardrail_policies(ctl_cfg_root, owner="ctl")
    active = active_guardrail_policies(policies, execution_context)
    result = []
    for subject, group in _group_active_policies(active, execution_context):
        paths = sorted(
            pointer for policy in group for pointer in policy["protected_paths"]
        )
        result.append(
            {
                "subject": subject,
                "values": resolve_guardrail_values(
                    subject,
                    paths,
                    ctl_cfg_root=ctl_cfg_root,
                    plt_rendered_dir=None,
                    execution_context=execution_context,
                ),
                "policies": tuple(policy["name"] for policy in group),
            }
        )
    return result


def materialize_plt_guardrails(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
    required_target_paths: set[str] | None = None,
) -> list[dict]:
    policies = load_guardrail_policies(plt_cfg_root, owner="plt")
    if not policies:
        return []
    if not _common().discover_cfg_meta_paths(plt_cfg_root):
        raise RuntimeError(
            f"❌ PLT guardrail policies exist but no cfg scopes were found: {plt_cfg_root}"
        )
    active_scopes = _common().discover_active_cfg_scopes(
        plt_cfg_root,
        scope_params=scope_params,
        execution_context=execution_context,
    )
    scopes_by_target = collections.defaultdict(list)
    for scope in active_scopes:
        target_path = scope["target_path"]
        if required_target_paths is not None and target_path not in required_target_paths:
            continue
        scopes_by_target[target_path].append(scope)
    target_paths = set(scopes_by_target)
    active = active_guardrail_policies(
        policies,
        execution_context,
        target_paths=target_paths,
    )
    groups = _group_active_policies(active, execution_context)
    _validate_plt_instance_completeness(groups, scopes_by_target)
    result = []
    for subject, group in groups:
        paths = sorted(
            pointer for policy in group for pointer in policy["protected_paths"]
        )
        result.append(
            {
                "subject": subject,
                "values": resolve_guardrail_values(
                    subject,
                    paths,
                    ctl_cfg_root=ctl_cfg_root,
                    plt_rendered_dir=plt_rendered_dir,
                    execution_context=execution_context,
                ),
                "policies": tuple(policy["name"] for policy in group),
            }
        )
    return result


def load_guardrail_baselines(guardrails_cfg_root: Path) -> dict[str, dict]:
    baselines = {}
    for path in sorted(guardrails_cfg_root.rglob("*.yaml")):
        data = _common().load_yaml(path) or {}
        if not isinstance(data, dict):
            raise RuntimeError(f"❌ guardrail baseline file must be a mapping: {path}")
        legacy = sorted(set(data) & LEGACY_KEYS)
        if legacy:
            raise RuntimeError(
                f"❌ legacy guardrail baseline collections {legacy} are unsupported: {path}"
            )
        if BASELINES_KEY not in data:
            continue
        if set(data) != {BASELINES_KEY}:
            raise RuntimeError(
                f"❌ generated guardrail files must contain only {BASELINES_KEY}: {path}"
            )
        _validate_schema(
            data,
            "guardrail-baselines.schema.json",
            origin=path,
        )
        entries = data[BASELINES_KEY]
        if not isinstance(entries, list):
            raise RuntimeError(f"❌ {BASELINES_KEY} must be a list: {path}")
        for index, raw in enumerate(entries):
            label = f"{BASELINES_KEY}[{index}] in {path}"
            if not isinstance(raw, dict) or set(raw) != {"subject", "values"}:
                raise RuntimeError(
                    f"❌ {label} must contain exactly subject and values"
                )
            subject = _normalize_subject(
                raw["subject"],
                label=f"{label}.subject",
                allow_instance=True,
            )
            raw_values = raw["values"]
            if not isinstance(raw_values, dict) or not raw_values:
                raise RuntimeError(f"❌ {label}.values must be a non-empty mapping")
            values = {}
            for pointer, value in raw_values.items():
                json_pointer_tokens(pointer, label=f"{label}.values")
                values[pointer] = _validate_native(
                    value,
                    label=f"{label}.values.{pointer}",
                )
            _reject_overlapping_paths(
                list(values),
                label=f"{label}.values",
            )
            identity = subject_identity(subject)
            if identity in baselines:
                raise RuntimeError(
                    f"❌ duplicate guardrail baseline subject {subject}: {path} "
                    f"(also in {baselines[identity]['origin']})"
                )
            baselines[identity] = {
                "subject": subject,
                "values": dict(sorted(values.items())),
                "origin": path,
            }
    return baselines


def guardrail_baseline_file(root: Path, subject: dict) -> Path:
    subject = _normalize_subject(
        subject,
        label="guardrail subject",
        allow_instance=True,
    )
    if subject["kind"] == "plt_rendered_target":
        rel = subject["target_path"].lstrip("/") or "_root"
        path = root / "invariants" / "plt" / f"{rel}.yaml"
    else:
        path = root / "invariants" / "ctl" / "guardrails.yaml"
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise RuntimeError(
            f"❌ guardrail baseline output escapes guardrail root: {resolved}"
        ) from exc
    return resolved


def write_guardrail_baseline(
    root: Path,
    *,
    subject: dict,
    values: dict[str, object],
) -> Path:
    subject = _normalize_subject(
        subject,
        label="guardrail subject",
        allow_instance=True,
    )
    if not isinstance(values, dict) or not values:
        raise RuntimeError("❌ guardrail baseline values must be a non-empty mapping")
    normalized_values = {}
    for pointer, value in values.items():
        json_pointer_tokens(pointer, label="guardrail baseline values")
        normalized_values[pointer] = _validate_native(
            value,
            label=f"guardrail baseline value {pointer}",
        )
    _reject_overlapping_paths(
        list(normalized_values),
        label="guardrail baseline values",
    )
    path = guardrail_baseline_file(root, subject)
    entries = []
    if path.is_file():
        data = _common().load_yaml(path) or {}
        if not isinstance(data, dict) or set(data) != {BASELINES_KEY}:
            raise RuntimeError(
                f"❌ generated guardrail file must contain only {BASELINES_KEY}: {path}"
            )
        else:
            entries = data[BASELINES_KEY]
            if not isinstance(entries, list):
                raise RuntimeError(f"❌ {BASELINES_KEY} must be a list: {path}")
    identity = subject_identity(subject)
    replacement = {
        "subject": subject,
        "values": dict(sorted(normalized_values.items())),
    }
    kept = []
    replaced = False
    for index, raw in enumerate(entries):
        if not isinstance(raw, dict) or set(raw) != {"subject", "values"}:
            raise RuntimeError(
                f"❌ invalid guardrail baseline entry [{index}]: {path}"
            )
        raw_subject = _normalize_subject(
            raw["subject"],
            label=f"guardrail baseline entry [{index}].subject",
            allow_instance=True,
        )
        if subject_identity(raw_subject) == identity:
            if replaced:
                raise RuntimeError(
                    f"❌ duplicate guardrail baseline subject in {path}"
                )
            kept.append(replacement)
            replaced = True
        else:
            kept.append(raw)
    if not replaced:
        kept.append(replacement)
    kept.sort(key=lambda entry: subject_identity(entry["subject"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = yaml.safe_dump({BASELINES_KEY: kept}, sort_keys=False)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        handle.write(rendered)
        temporary_path = Path(handle.name)
    os.replace(temporary_path, path)
    return path


def _validate_baseline_contract(
    policies: dict[str, dict],
    baselines: dict[str, dict],
) -> None:
    allowed_paths = collections.defaultdict(set)
    allowed_instance_params = collections.defaultdict(set)
    for policy in policies.values():
        base_identity = subject_identity(policy["subject"])
        allowed_paths[base_identity].update(policy["protected_paths"])
        allowed_instance_params[base_identity].add(policy["instance_params"])
    for entry in baselines.values():
        base = subject_without_instance(entry["subject"])
        base_identity = subject_identity(base)
        if base_identity not in allowed_paths:
            raise RuntimeError(
                f"❌ baseline subject {entry['subject']} in {entry['origin']} "
                "has no authored guardrail policy"
            )
        extras = sorted(set(entry["values"]) - allowed_paths[base_identity])
        if extras:
            raise RuntimeError(
                f"❌ baseline paths {extras} in {entry['origin']} have no "
                f"authored policy for subject {base}"
            )
        actual_params = tuple(
            entry["subject"].get("instance", {}).get("params", {}).keys()
        )
        if actual_params not in allowed_instance_params[base_identity]:
            expected = sorted(
                list(params)
                for params in allowed_instance_params[base_identity]
            )
            raise RuntimeError(
                f"❌ baseline subject {entry['subject']} in {entry['origin']} "
                f"uses instance params {list(actual_params)}; expected one of {expected}"
            )


def _verify_materialized(
    materialized: list[dict],
    baselines: dict[str, dict],
) -> None:
    for entry in materialized:
        identity = subject_identity(entry["subject"])
        baseline = baselines.get(identity)
        if baseline is None:
            raise RuntimeError(
                f"❌ guardrail subject {entry['subject']} has no baseline; "
                "run regenerate_guardrails.py for this instance"
            )
        expected_paths = set(entry["values"])
        actual_paths = set(baseline["values"])
        if expected_paths != actual_paths:
            raise RuntimeError(
                f"❌ guardrail baseline paths for {entry['subject']} differ: "
                f"required={sorted(expected_paths)}, baseline={sorted(actual_paths)}"
            )
        for pointer, actual in entry["values"].items():
            expected = baseline["values"][pointer]
            if _canonical(actual, label=f"actual {pointer}") != _canonical(
                expected,
                label=f"baseline {pointer}",
            ):
                raise RuntimeError(
                    f"❌ guardrail mismatch for {entry['subject']} {pointer}: "
                    f"expected {expected!r}, got {actual!r}"
                )


def verify_materialized_guardrails(
    materialized: list[dict],
    guardrails_cfg_root: Path,
    policies: dict[str, dict],
    *,
    owner: str,
) -> None:
    if owner not in SUBJECT_KINDS:
        raise RuntimeError(f"❌ unknown guardrail policy owner: {owner!r}")
    baselines = {
        identity: entry
        for identity, entry in load_guardrail_baselines(guardrails_cfg_root).items()
        if entry["subject"]["kind"] in SUBJECT_KINDS[owner]
    }
    _validate_baseline_contract(policies, baselines)
    _verify_materialized(materialized, baselines)


def verify_ctl_guardrails(
    ctl_cfg_root: Path,
    guardrails_cfg_root: Path,
    execution_context: dict[str, object],
) -> None:
    policies = load_guardrail_policies(ctl_cfg_root, owner="ctl")
    if not policies:
        return
    baselines = {
        identity: entry
        for identity, entry in load_guardrail_baselines(guardrails_cfg_root).items()
        if entry["subject"]["kind"] in SUBJECT_KINDS["ctl"]
    }
    _validate_baseline_contract(policies, baselines)
    _verify_materialized(
        materialize_ctl_guardrails(ctl_cfg_root, execution_context),
        baselines,
    )


def verify_plt_guardrails(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
    required_target_paths: set[str] | None = None,
) -> None:
    policies = load_guardrail_policies(plt_cfg_root, owner="plt")
    if not policies:
        return
    baselines = {
        identity: entry
        for identity, entry in load_guardrail_baselines(guardrails_cfg_root).items()
        if entry["subject"]["kind"] in SUBJECT_KINDS["plt"]
    }
    _validate_baseline_contract(policies, baselines)
    _verify_materialized(
        materialize_plt_guardrails(
            ctl_cfg_root,
            plt_cfg_root,
            plt_rendered_dir,
            execution_context,
            scope_params,
            required_target_paths,
        ),
        baselines,
    )
