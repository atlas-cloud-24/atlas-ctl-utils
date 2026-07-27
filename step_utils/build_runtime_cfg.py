#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
from copy import deepcopy
from pathlib import Path

import yaml


VAR_NAME_RE = r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*"
PLACEHOLDER_RE = re.compile(rf"\$\{{({VAR_NAME_RE})(?::-(.*?))?\}}")
EXACT_PLACEHOLDER_RE = re.compile(rf"^\$\{{({VAR_NAME_RE})(?::-(.*?))?\}}$")
# One key per nesting level: namespaced facts (e.g. a provider-namespaced
# param) arrive as nested mappings, never as dotted keys, so every key is a
# single identifier segment.
RUNTIME_CONTEXT_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CFG_ENTRY_REF_PREFIX = "cfg-entry-ref:"
CFG_ENTRY_REF_KEY = "cfg_entry_ref"
CFG_ENTRY_REF_COLLECTION_RE = re.compile(rf"^{VAR_NAME_RE}$")
OMIT = object()


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


def load_yaml_mapping(path: Path) -> dict:
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        return {}
    data = yaml.load(raw, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"cfg file must contain a mapping: {path}")
    return data



EXECUTION_CONTEXT_ROOT = "execution_context"


def load_execution_context(path: Path) -> tuple[dict[str, object], dict]:
    """Load the nested execution_context file; return (flat dotted map, nested)."""
    data = load_yaml_mapping(path)
    nested = data.get(EXECUTION_CONTEXT_ROOT)
    if not isinstance(nested, dict):
        raise RuntimeError(f"execution context file must contain a top-level {EXECUTION_CONTEXT_ROOT} mapping: {path}")
    flat: dict[str, object] = {}

    def walk(prefix: str, entries: dict) -> None:
        for key, value in entries.items():
            if not isinstance(key, str) or not RUNTIME_CONTEXT_KEY_RE.fullmatch(key):
                raise RuntimeError(f"execution context key {key!r} is not a valid identifier")
            ref = f"{prefix}.{key}"
            # A fact is a scalar, a list of scalars (e.g. the run's declared
            # providers), or a nested mapping of facts — the same shapes the
            # engine's context builder writes.
            if isinstance(value, dict):
                walk(ref, value)
            elif isinstance(value, list):
                if not all(isinstance(item, (str, int, float, bool)) for item in value):
                    raise RuntimeError(
                        f"execution context key {ref!r} must be a list of scalars"
                    )
                flat[ref] = value
            elif isinstance(value, (str, int, float, bool)):
                flat[ref] = value
            else:
                raise RuntimeError(f"execution context key {ref!r} must be scalar, got {type(value).__name__}")

    for namespace, entries in nested.items():
        if not isinstance(entries, dict):
            raise RuntimeError(f"{EXECUTION_CONTEXT_ROOT}.{namespace} must be a mapping: {path}")
        walk(f"{EXECUTION_CONTEXT_ROOT}.{namespace}", entries)
    return flat, nested


def merge_values(base, overlay):
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            if key in merged:
                merged[key] = merge_values(merged[key], value)
            else:
                merged[key] = value
        return merged
    return deepcopy(overlay)



def format_cfg_path(path: tuple[str, ...]) -> str:
    return ".".join(path) if path else "<root>"


def parse_cfg_entry_ref(value: str, path: tuple[str, ...]) -> tuple[str, str]:
    body = value[len(CFG_ENTRY_REF_PREFIX):]
    if ":" not in body:
        raise RuntimeError(
            f"cfg entry reference at {format_cfg_path(path)} must use "
            f"{CFG_ENTRY_REF_PREFIX}<collection>:<key>"
        )
    collection, key = body.rsplit(":", 1)
    if not collection:
        raise RuntimeError(f"cfg entry reference at {format_cfg_path(path)} has an empty collection path")
    if not key or key != key.strip():
        raise RuntimeError(f"cfg entry reference at {format_cfg_path(path)} has an empty or padded item key")
    if not CFG_ENTRY_REF_COLLECTION_RE.fullmatch(collection):
        raise RuntimeError(
            f"cfg entry reference at {format_cfg_path(path)} has invalid collection path {collection!r}"
        )
    return collection, key


def lookup_cfg_collection(root: dict, collection: str, path: tuple[str, ...]):
    current = root
    for part in collection.split("."):
        if not isinstance(current, dict) or part not in current:
            raise RuntimeError(
                f"cfg entry reference at {format_cfg_path(path)} points to missing collection {collection!r}"
            )
        current = current[part]
    if not isinstance(current, dict):
        raise RuntimeError(
            f"cfg entry reference at {format_cfg_path(path)} points to non-mapping collection {collection!r}"
        )
    return current


def resolve_cfg_entry_refs(root: dict, lookup_root: dict | None = None):
    ref_lookup_root = root if lookup_root is None else lookup_root

    def resolve_value(value, path: tuple[str, ...]):
        if isinstance(value, str):
            if value.startswith(CFG_ENTRY_REF_PREFIX):
                collection, key = parse_cfg_entry_ref(value, path)
                collection_value = lookup_cfg_collection(ref_lookup_root, collection, path)
                if key not in collection_value:
                    raise RuntimeError(
                        f"cfg entry reference at {format_cfg_path(path)} points to missing item "
                        f"{key!r} in collection {collection!r}"
                    )
                return {
                    CFG_ENTRY_REF_KEY: {
                        "collection": collection,
                        "key": key,
                    }
                }
            if CFG_ENTRY_REF_PREFIX in value:
                raise RuntimeError(
                    f"cfg entry reference at {format_cfg_path(path)} must be a whole scalar, not part of a string"
                )
            return value

        if isinstance(value, list):
            return [resolve_value(item, (*path, str(index))) for index, item in enumerate(value)]

        if isinstance(value, dict):
            return {key: resolve_value(item, (*path, str(key))) for key, item in value.items()}

        return value

    return resolve_value(root, ())


def resolve_cfg_path(origin_cfg_dir: Path, key: str) -> Path:
    key_path = Path(key)
    if key_path.is_absolute():
        raise RuntimeError(f"cfg path must be relative to the step cfg root: {key}")

    resolved = (origin_cfg_dir / key_path).resolve()
    try:
        resolved.relative_to(origin_cfg_dir)
    except ValueError as exc:
        raise RuntimeError(f"cfg path escapes the step cfg root: {key}") from exc
    return resolved


def load_domain_docs(origin_cfg_dir: Path) -> dict[str, dict]:
    """Load the per-domain views the engine projected into this step's cfg dir.

    §Phase 60: the target delivers `<domain>.yaml` holding exactly the keys it
    declared; the step then narrows further to its own `cfg_keys`.
    """
    origin_cfg_dir = origin_cfg_dir.resolve()
    docs: dict[str, dict] = {}
    for path in sorted(origin_cfg_dir.glob("*.yaml")):
        docs[path.stem] = load_yaml_mapping(path)
    return docs


def project_step_keys(doc: dict, entries: list[str], *, label: str) -> dict:
    """Narrow one domain view to the step's declared key selectors.

    Assertion 1 + 5 at the step boundary: a key that does not resolve, or a glob
    that matches nothing, is a stale declaration and an ERROR — never a silently
    missing Terraform input.
    """
    import fnmatch

    projected: dict = {}
    for entry in entries:
        if entry == "*":
            matched = list(doc)
        elif any(ch in entry for ch in "*?["):
            matched = sorted(k for k in doc if fnmatch.fnmatchcase(k, entry))
        else:
            head = entry.split(".", 1)[0]
            if head not in doc:
                raise RuntimeError(
                    f"{label}: cfg key {entry!r} was not delivered by the target "
                    f"(delivered: {', '.join(sorted(doc)) or 'none'})"
                )
            matched = [head]
        if not matched:
            raise RuntimeError(
                f"{label}: cfg key selector {entry!r} matched nothing (stale declaration)"
            )
        for key in matched:
            projected[key] = doc[key]
    return projected


class Resolver:
    def __init__(self, raw: dict, env_ctx: dict[str, str]):
        self.raw = raw
        self.env_ctx = env_ctx
        self.cache: dict[str, object] = {}
        self.resolving: set[str] = set()

    def lookup(self, name: str):
        if name in self.cache:
            return deepcopy(self.cache[name])

        value = self._lookup_from_raw(name)
        if value is not OMIT:
            return deepcopy(value)

        if name in self.env_ctx:
            return self.env_ctx[name]

        return OMIT

    def _lookup_from_raw(self, name: str):
        if name in self.raw:
            return self._resolve_named_value(name, self.raw[name])

        if "." not in name:
            return OMIT

        path = name.split(".")
        root_name = path[0]
        if root_name not in self.raw:
            return OMIT

        current = self.raw[root_name]
        for part in path[1:]:
            if not isinstance(current, dict) or part not in current:
                return OMIT
            current = current[part]

        return self._resolve_named_value(name, current)

    def _resolve_named_value(self, name: str, raw_value):
        if name in self.resolving:
            raise RuntimeError(f"cyclic cfg interpolation reference: {name}")
        self.resolving.add(name)
        try:
            value = self.resolve_value(raw_value)
        finally:
            self.resolving.remove(name)
        self.cache[name] = value
        return value

    @staticmethod
    def parse_default(raw: str):
        if raw.strip() == "":
            raise RuntimeError("empty cfg interpolation fallback is not allowed")
        if raw == "null":
            return None
        if raw == "true":
            return True
        if raw == "false":
            return False
        if raw.startswith("{") or raw.startswith("["):
            return json.loads(raw)
        if re.fullmatch(r"-?\d+", raw):
            return int(raw)
        if re.fullmatch(r"-?\d+\.\d+", raw):
            return float(raw)
        return raw

    def resolve_string(self, value: str):
        exact_match = EXACT_PLACEHOLDER_RE.fullmatch(value)
        if exact_match:
            var_name = exact_match.group(1)
            default_raw = exact_match.group(2)
            looked_up = self.lookup(var_name)
            if looked_up is OMIT and default_raw is not None:
                return self.parse_default(default_raw)
            if looked_up is OMIT:
                raise RuntimeError(f"missing cfg interpolation reference: {var_name}")
            return looked_up

        def replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default_raw = match.group(2)
            looked_up = self.lookup(var_name)
            if looked_up is OMIT:
                if default_raw is not None:
                    looked_up = self.parse_default(default_raw)
                else:
                    raise RuntimeError(f"missing cfg interpolation reference: {var_name}")
            if isinstance(looked_up, (dict, list)):
                raise RuntimeError(
                    f"cfg interpolation reference '{var_name}' is non-scalar and cannot be embedded in a string"
                )
            if looked_up is None:
                return "null"
            if isinstance(looked_up, bool):
                return "true" if looked_up else "false"
            return str(looked_up)

        return PLACEHOLDER_RE.sub(replace, value)

    def resolve_value(self, value):
        if isinstance(value, str):
            return self.resolve_string(value)

        if isinstance(value, list):
            resolved = []
            for item in value:
                item_value = self.resolve_value(item)
                if item_value is OMIT:
                    raise RuntimeError("unexpected unresolved cfg list item")
                resolved.append(item_value)
            return resolved

        if isinstance(value, dict):
            resolved = {}
            for key, item in value.items():
                item_value = self.resolve_value(item)
                if item_value is OMIT:
                    raise RuntimeError(f"unexpected unresolved cfg value for key: {key}")
                resolved_key = self.resolve_string(key) if isinstance(key, str) else key
                if resolved_key is OMIT:
                    raise RuntimeError(f"unexpected unresolved cfg key: {key}")
                resolved[resolved_key] = item_value
            return resolved

        return value


def build_step_values(origin_cfg_dir: Path, cfg_keys: dict, env_ctx: dict[str, str]) -> tuple[dict, list[str]]:
    docs = load_domain_docs(origin_cfg_dir)
    merged: dict = {}
    merged_domains: list[str] = []
    for raw_domain, entries in cfg_keys.items():
        # A domain-GENERIC step (one that serves every domain, e.g. the tfstate
        # backend) names its domain by execution-context reference rather than
        # enumerating every domain it could ever serve.
        domain = raw_domain
        if "${" in raw_domain:
            resolved = EXACT_PLACEHOLDER_RE.fullmatch(raw_domain.strip())
            if not resolved:
                raise RuntimeError(
                    f"step cfg_keys domain {raw_domain!r} must be a whole-value "
                    "${execution_context...} reference"
                )
            domain = env_ctx.get(resolved.group(1))
            if not domain:
                raise RuntimeError(
                    f"step cfg_keys domain {raw_domain!r} is unbound in the execution context"
                )
        doc = docs.get(domain)
        if doc is None:
            raise RuntimeError(
                f"step declares cfg_keys for domain {domain!r}, which the target did not "
                f"deliver (delivered: {', '.join(sorted(docs)) or 'none'})"
            )
        if EXECUTION_CONTEXT_ROOT in doc:
            raise RuntimeError(
                f"plt payload must not define reserved top-level key {EXECUTION_CONTEXT_ROOT!r}: {domain}"
            )
        merged = merge_values(
            merged,
            project_step_keys(doc, list(entries), label=f"step cfg_keys[{domain}]"),
        )
        merged_domains.append(domain)

    resolver = Resolver(merged, env_ctx)
    resolved = {}
    for key in merged:
        value = resolver.lookup(key)
        if value is OMIT:
            continue
        resolved[key] = value

    resolved = resolve_cfg_entry_refs(resolved)
    return resolved, merged_domains


def shell_value(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def write_step_env(path: Path, values: dict, values_json_path: Path | None) -> None:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
    ]
    if values_json_path is not None:
        lines.append(f"export STEP_VALUES_JSON={shlex.quote(str(values_json_path.resolve()))}")
    for key in sorted(values):
        lines.append(f"export {key}={shlex.quote(shell_value(values[key]))}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_cfg_keys(raw: str) -> dict:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"--cfg-keys must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not all(
        isinstance(domain, str)
        and isinstance(entries, list)
        and entries
        and all(isinstance(e, str) for e in entries)
        for domain, entries in parsed.items()
    ):
        raise argparse.ArgumentTypeError(
            "--cfg-keys must be a JSON map of domain -> non-empty list of key selectors"
        )
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-cfg-dir", required=True)
    parser.add_argument("--cfg-keys", required=True, type=parse_cfg_keys)
    parser.add_argument("--values-json-out", required=True)
    parser.add_argument("--step-env-out", required=True)
    parser.add_argument("--execution-context-file", required=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    origin_cfg_dir = Path(args.origin_cfg_dir).resolve()
    cfg_keys = args.cfg_keys
    values_json_out_arg = args.values_json_out
    step_env_out_arg = args.step_env_out
    values_json_out = None if values_json_out_arg == "-" else Path(values_json_out_arg).resolve()
    step_env_out = None if step_env_out_arg == "-" else Path(step_env_out_arg).resolve()

    execution_context_file = Path(args.execution_context_file).resolve()
    env_ctx, execution_context_nested = load_execution_context(execution_context_file)

    step_values, merged_domains = build_step_values(origin_cfg_dir, cfg_keys, env_ctx)
    # Nested merge + aliases: the whole context under ONE reserved key; the
    # engine never invents flat leaves (flat names exist only via payload aliases).
    step_env_values = dict(step_values)
    step_env_values[EXECUTION_CONTEXT_ROOT] = execution_context_nested

    if values_json_out is not None:
        values_json_out.parent.mkdir(parents=True, exist_ok=True)
        values_json_out.write_text(
            json.dumps(
                {
                    "_meta": {
                        "origin_cfg_dir": str(origin_cfg_dir),
                        "execution_context_file": str(execution_context_file),
                        "execution_context_keys": sorted(env_ctx),
                        "merged_domains": merged_domains,
                    },
                    "values": step_env_values,
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )

    if step_env_out is not None:
        step_env_out.parent.mkdir(parents=True, exist_ok=True)
        write_step_env(step_env_out, step_env_values, values_json_out)
        step_env_out.chmod(0o755)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
