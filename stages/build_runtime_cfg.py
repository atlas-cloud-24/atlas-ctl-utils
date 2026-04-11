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


def resolve_cfg_path(origin_cfg_dir: Path, key: str) -> Path:
    key_path = Path(key)
    if key_path.is_absolute():
        raise RuntimeError(f"cfg path must be relative to the stage cfg root: {key}")

    resolved = (origin_cfg_dir / key_path).resolve()
    try:
        resolved.relative_to(origin_cfg_dir)
    except ValueError as exc:
        raise RuntimeError(f"cfg path escapes the stage cfg root: {key}") from exc
    return resolved


def iter_cfg_files(origin_cfg_dir: Path, cfg_files: list[str]) -> list[Path]:
    origin_cfg_dir = origin_cfg_dir.resolve()
    files: list[Path] = []
    for key in cfg_files:
        if key == "*":
            files.extend(sorted(p for p in origin_cfg_dir.rglob("*") if p.is_file()))
            continue

        if key.endswith("/*"):
            dir_path = resolve_cfg_path(origin_cfg_dir, key[:-2])
            if not dir_path.is_dir():
                raise RuntimeError(f"cfg directory not found for wildcard slice '{key}': {dir_path}")
            files.extend(sorted(p for p in dir_path.rglob("*") if p.is_file()))
            continue

        file_path = resolve_cfg_path(origin_cfg_dir, key)
        if not file_path.is_file():
            raise RuntimeError(f"cfg file not found: {file_path}")
        files.append(file_path)

    seen: set[Path] = set()
    unique: list[Path] = []
    for path in files:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


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


def build_stage_values(origin_cfg_dir: Path, cfg_files: list[str], env_ctx: dict[str, str]) -> tuple[dict, list[str]]:
    merged: dict = {}
    merged_files: list[str] = []
    for cfg_file in iter_cfg_files(origin_cfg_dir, cfg_files):
        merged = merge_values(merged, load_yaml_mapping(cfg_file))
        merged_files.append(str(cfg_file))

    resolver = Resolver(merged, env_ctx)
    resolved = {}
    for key in merged:
        value = resolver.lookup(key)
        if value is OMIT:
            continue
        resolved[key] = value

    return resolved, merged_files


def shell_value(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def write_stage_env(path: Path, values: dict, values_json_path: Path | None) -> None:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
    ]
    if values_json_path is not None:
        lines.append(f"export STAGE_VALUES_JSON={shlex.quote(str(values_json_path.resolve()))}")
    for key in sorted(values):
        lines.append(f"export {key}={shlex.quote(shell_value(values[key]))}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_cfg_files(raw: str) -> list[str]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"--cfg-files must be valid JSON: {exc}") from exc
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise argparse.ArgumentTypeError("--cfg-files must be a JSON list of strings")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-cfg-dir", required=True)
    parser.add_argument("--cfg-files", required=True, type=parse_cfg_files)
    parser.add_argument("--values-json-out", required=True)
    parser.add_argument("--stage-env-out", required=True)
    parser.add_argument("--env-type", required=True)
    parser.add_argument("--main-tag", required=True)
    parser.add_argument("--run-id", required=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    origin_cfg_dir = Path(args.origin_cfg_dir).resolve()
    cfg_files = args.cfg_files
    values_json_out_arg = args.values_json_out
    stage_env_out_arg = args.stage_env_out
    values_json_out = None if values_json_out_arg == "-" else Path(values_json_out_arg).resolve()
    stage_env_out = None if stage_env_out_arg == "-" else Path(stage_env_out_arg).resolve()

    env_ctx = {
        "env_type": args.env_type,
        "main_tag": args.main_tag,
        "run_id": args.run_id,
    }

    stage_values, merged_files = build_stage_values(origin_cfg_dir, cfg_files, env_ctx)

    if values_json_out is not None:
        values_json_out.parent.mkdir(parents=True, exist_ok=True)
        values_json_out.write_text(
            json.dumps(
                {
                    "_meta": {
                        "origin_cfg_dir": str(origin_cfg_dir),
                        "merged_files": merged_files,
                    },
                    "values": stage_values,
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )

    if stage_env_out is not None:
        stage_env_out.parent.mkdir(parents=True, exist_ok=True)
        write_stage_env(stage_env_out, stage_values, values_json_out)
        stage_env_out.chmod(0o755)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
