#!/usr/bin/env python3
"""Add or refresh guarded var hashes for ctl or plt cfg."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def key_value(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(f"Expected key=value, got {value!r}")
    key, raw = value.split("=", 1)
    key = key.strip()
    raw = raw.strip()
    if not key or not raw:
        raise argparse.ArgumentTypeError(f"Expected non-empty key=value, got {value!r}")
    return key, raw


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add or refresh guardrail sha256 values while preserving existing entries."
    )
    parser.add_argument("--owner", required=True, choices=("ctl", "plt"))
    parser.add_argument("--cfg-dir", required=True, help="Owner cfg root.")
    parser.add_argument(
        "--merged-cfg-dir",
        help="Required for --owner plt; prepared/merged plt cfg tree used by a run.",
    )
    parser.add_argument(
        "--runtime-context-file",
        help="YAML runtime_context artifact. Recommended for plt values containing ${...} placeholders.",
    )
    parser.add_argument(
        "--runtime-context",
        action="append",
        type=key_value,
        default=[],
        help="Extra runtime context key=value. May be repeated; overrides --runtime-context-file values.",
    )
    parser.add_argument(
        "--plt-selector",
        action="append",
        type=key_value,
        default=[],
        help="Plt selector key=value used to identify the active scope for --scope-target.",
    )
    parser.add_argument(
        "--scope-target",
        help="For --owner plt, update the active scope whose __meta__.yaml target_path matches this path, e.g. /env. Omit to update root __guardrails__.yaml.",
    )
    parser.add_argument(
        "--var",
        action="append",
        dest="vars",
        help="Guarded var name to add/regenerate. Defaults to vars already declared at the selected guardrails level.",
    )
    return parser.parse_args()


def runtime_context_from_file(path: str | None) -> dict[str, object]:
    if not path:
        return {}
    runtime_path = Path(path).expanduser().resolve()
    data = common.load_yaml(runtime_path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"runtime context file must contain a mapping: {runtime_path}")
    return data


def runtime_context_from_args(args: argparse.Namespace) -> dict[str, object]:
    context = runtime_context_from_file(args.runtime_context_file)
    for key, value in args.runtime_context:
        context[key] = value
    return context


def ctl_runtime_context(cfg_dir: Path, args: argparse.Namespace) -> dict[str, object]:
    context = common.build_runtime_context(
        cfg_dir,
        {"ctl": {}, "plt": dict(args.plt_selector)},
        base_values={},
    )
    context.update(runtime_context_from_args(args))
    return context


def plt_value(merged_cfg_dir: Path, var_name: str, runtime_context: dict[str, object]):
    values = common.load_merged_cfg_top_level_values(merged_cfg_dir, var_name)
    if not values:
        raise RuntimeError(f"plt guarded var {var_name!r} was not found in {merged_cfg_dir}")
    first_path, first_value = values[0]
    first_effective = common.guard_effective_value(
        first_value,
        runtime_context,
        label=f"plt.{var_name} at {first_path}",
    )
    first_text = common.guard_value_text(first_effective, label=f"plt.{var_name} at {first_path}")
    for path, value in values[1:]:
        effective = common.guard_effective_value(value, runtime_context, label=f"plt.{var_name} at {path}")
        value_text = common.guard_value_text(effective, label=f"plt.{var_name} at {path}")
        if value_text != first_text:
            raise RuntimeError(
                f"plt guarded var {var_name!r} has multiple active values: "
                f"{first_path}={first_text!r}, {path}={value_text!r}"
            )
    return first_effective


def write_ctl_guardrails(cfg_dir: Path, hashes: dict[str, str]) -> Path:
    path = cfg_dir / "guardrails.yaml"
    data = {"guardrails": {"guarded_vars": hashes}}
    path.write_text(yaml.safe_dump(data, sort_keys=True), encoding="utf-8")
    return path


def write_guardrails_file(path: Path, hashes: dict[str, str]) -> Path:
    data = {"guarded_vars": hashes}
    path.write_text(yaml.safe_dump(data, sort_keys=True), encoding="utf-8")
    return path


def selected_plt_guardrails_path(cfg_dir: Path, args: argparse.Namespace) -> tuple[Path, str]:
    if not args.scope_target:
        return cfg_dir / common.PLT_GUARDRAILS_FILENAME, "."

    target_path = common.normalize_cfg_absolute_path(args.scope_target, label="--scope-target")
    selectors = dict(args.plt_selector)
    matches = [
        scope
        for scope in common.discover_active_cfg_scopes(cfg_dir, plt_runtime_selectors=selectors)
        if scope["target_path"] == target_path
    ]
    if not matches:
        raise RuntimeError(f"no active plt scope with target_path {target_path!r} for selectors {selectors}")
    if len(matches) > 1:
        raise RuntimeError(f"multiple active plt scopes with target_path {target_path!r} for selectors {selectors}")
    scope = matches[0]
    return scope["scope_root"] / common.PLT_GUARDRAILS_FILENAME, scope["target_path"]


def selected_merged_dir(merged_cfg_dir: Path, scope_target: str | None) -> Path:
    if not scope_target:
        return merged_cfg_dir
    target_path = common.normalize_cfg_absolute_path(scope_target, label="--scope-target")
    target_dir = (merged_cfg_dir / target_path.lstrip("/")).resolve()
    try:
        target_dir.relative_to(merged_cfg_dir.resolve())
    except ValueError as exc:
        raise RuntimeError(f"--scope-target escapes merged cfg dir: {target_path}") from exc
    if not target_dir.is_dir():
        raise RuntimeError(f"merged scope dir not found for {target_path}: {target_dir}")
    return target_dir


def main() -> int:
    args = parse_args()
    cfg_dir = Path(args.cfg_dir).expanduser().resolve()
    if not cfg_dir.is_dir():
        raise RuntimeError(f"cfg dir not found: {cfg_dir}")

    if args.owner == "ctl":
        existing = common.load_ctl_guarded_vars(cfg_dir)
        var_names = args.vars or sorted(existing)
        if not var_names:
            raise RuntimeError("no ctl guarded vars declared; pass --var to add one")
        runtime_context = ctl_runtime_context(cfg_dir, args)
        hashes: dict[str, str] = dict(existing)
        for var_name in var_names:
            if var_name not in runtime_context:
                raise RuntimeError(f"ctl guarded var {var_name!r} is not available in runtime_context")
            hashes[var_name] = common.guard_value_hash(runtime_context[var_name], label=f"ctl.{var_name}")
        path = write_ctl_guardrails(cfg_dir, hashes)
    else:
        if not args.merged_cfg_dir:
            raise RuntimeError("--merged-cfg-dir is required for --owner plt")
        merged_cfg_dir = Path(args.merged_cfg_dir).expanduser().resolve()
        if not merged_cfg_dir.is_dir():
            raise RuntimeError(f"merged cfg dir not found: {merged_cfg_dir}")
        guardrails_path, guardrails_scope = selected_plt_guardrails_path(cfg_dir, args)
        existing = common.load_guarded_vars_file(guardrails_path, allow_missing=True)
        var_names = args.vars or sorted(existing)
        if not var_names:
            raise RuntimeError("no plt guarded vars declared at selected level; pass --var to add one")
        runtime_context = runtime_context_from_args(args)
        value_dir = selected_merged_dir(merged_cfg_dir, args.scope_target)
        hashes = dict(existing)
        for var_name in var_names:
            hashes[var_name] = common.guard_value_hash(
                plt_value(value_dir, var_name, runtime_context),
                label=f"plt.{var_name}",
            )
        path = write_guardrails_file(guardrails_path, hashes)
        print(f"scope: {guardrails_scope}")

    print(f"wrote {path}")
    for var_name in sorted(hashes):
        print(f"{var_name}: {hashes[var_name]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
