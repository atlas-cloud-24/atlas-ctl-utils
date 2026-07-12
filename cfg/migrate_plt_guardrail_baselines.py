#!/usr/bin/env python3
"""Migrate scope-local plt baselines to explicit guardrail-repo identities."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plt-cfg-root", required=True)
    parser.add_argument("--guardrails-cfg-root", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    return parser.parse_args()


def legacy_entries(plt_root: Path) -> tuple[dict, list[Path]]:
    declarations = common.load_plt_guard_declarations(plt_root)
    expected = {}
    old_paths = []
    for meta_path in common.discover_cfg_meta_paths(plt_root):
        meta = common.load_cfg_meta(meta_path)
        if meta["type"] != "scope":
            continue
        scope_root = meta_path.parent
        rel = scope_root.relative_to(plt_root).as_posix()
        scope_path = "/" + rel if rel != "." else "/"
        scope = {
            "scope_path": scope_path,
            "target_path": common.normalize_cfg_absolute_path(
                meta["target_path"],
                label=f"target_path in {meta_path}",
            ),
            "selectors": meta.get("selectors") or {},
        }
        matching = [d for d in declarations if common.guard_declaration_matches_scope(d, scope)]
        if not matching:
            continue
        axis_refs = sorted(
            {axis for declaration in matching for axis in declaration.get("baseline_axes", ())}
        )
        if axis_refs:
            baseline_paths = sorted((scope_root / common.PLT_GUARDRAILS_DIRNAME).glob("*.yaml"))
        else:
            flat = scope_root / common.PLT_GUARDRAILS_FILENAME
            baseline_paths = [flat] if flat.is_file() else []
        for baseline_path in baseline_paths:
            if axis_refs:
                values = baseline_path.stem.split("__")
                if len(values) != len(axis_refs):
                    raise RuntimeError(
                        f"legacy axis filename {baseline_path} has {len(values)} values; "
                        f"expected {len(axis_refs)} for {axis_refs}"
                    )
                axes = dict(zip(axis_refs, values))
            else:
                axes = {}
            data = common.load_yaml(baseline_path) or {}
            if not isinstance(data, dict) or set(data) != {"guarded_vars"}:
                raise RuntimeError(f"invalid legacy baseline: {baseline_path}")
            guarded = {}
            common.merge_guarded_vars(guarded, data["guarded_vars"], origin=baseline_path)
            identity = common.guard_baseline_identity(scope_path, axes)
            if identity in expected:
                raise RuntimeError(f"duplicate legacy baseline identity: {identity!r}")
            expected[identity] = guarded
            old_paths.append(baseline_path)
    return expected, old_paths


def comparable_new(root: Path) -> dict:
    loaded = common.load_plt_guardrail_baselines(root)
    return {identity: entry["guarded_vars"] for identity, entry in loaded.items()}


def main() -> int:
    args = parse_args()
    plt_root = Path(args.plt_cfg_root).expanduser().resolve()
    guardrails_root = Path(args.guardrails_cfg_root).expanduser().resolve()
    if not plt_root.is_dir():
        raise RuntimeError(f"plt cfg root not found: {plt_root}")
    guardrails_root.mkdir(parents=True, exist_ok=True)

    expected, old_paths = legacy_entries(plt_root)
    if args.apply:
        for (scope_path, axis_items), guarded in sorted(expected.items()):
            common.write_plt_guardrail_baseline(
                guardrails_root,
                scope_path=scope_path,
                axes=dict(axis_items),
                guarded_vars=guarded,
            )

    actual = comparable_new(guardrails_root)
    if actual != expected:
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        changed = sorted(
            identity for identity in set(actual) & set(expected)
            if actual[identity] != expected[identity]
        )
        raise RuntimeError(
            f"migration parity failed: missing={missing}, extra={extra}, changed={changed}"
        )

    print(f"OK: {len(expected)} baseline identities and all value/hash pairs match")
    print("deletion manifest:")
    for path in old_paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
