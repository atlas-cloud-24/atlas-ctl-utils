#!/usr/bin/env python3
"""Render the merged plt cfg tree into the rendered/ layer.

Whole-scope resolution: for each top-level scope dir under the merged tree,
all *.yaml files are merged into one lookup document, `${...}` placeholders
are interpolated (engine-volatile context keys are kept verbatim and resolve
later at the per-stage step), and `cfg-entry-ref:` scalars are normalized to
their `cfg_entry_ref` object form with whole-scope validation. Each source
file is written back to the rendered tree with its own structure preserved;
non-YAML files are copied verbatim.
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

import yaml

from build_runtime_cfg import (
    Resolver,
    OMIT,
    load_runtime_context,
    load_yaml_mapping,
    merge_values,
    resolve_cfg_entry_refs,
)


def render_scope(scope_dir: Path, dest_dir: Path, env_ctx: dict, keep_unresolved: frozenset[str]) -> None:
    yaml_files = sorted(p for p in scope_dir.rglob("*.yaml") if p.is_file())
    scope_merged: dict = {}
    for path in yaml_files:
        scope_merged = merge_values(scope_merged, load_yaml_mapping(path))

    resolver = Resolver(scope_merged, env_ctx, keep_unresolved=keep_unresolved)
    scope_resolved: dict = {}
    for key in scope_merged:
        value = resolver.lookup(key)
        if value is OMIT:
            continue
        scope_resolved[key] = value
    scope_resolved = resolve_cfg_entry_refs(scope_resolved)

    for path in sorted(p for p in scope_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(scope_dir)
        dest = dest_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix != ".yaml":
            shutil.copy2(path, dest)
            continue
        doc = load_yaml_mapping(path)
        rendered = resolver.resolve_value(doc)
        rendered = resolve_cfg_entry_refs(rendered, lookup_root=scope_resolved)
        dest.write_text(
            yaml.safe_dump(rendered, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )


def render_cfg_tree(merged_dir: Path, rendered_dir: Path, env_ctx: dict, keep_unresolved: frozenset[str]) -> None:
    if rendered_dir.exists():
        shutil.rmtree(rendered_dir)
    rendered_dir.mkdir(parents=True)
    for entry in sorted(merged_dir.iterdir()):
        if entry.is_dir():
            render_scope(entry, rendered_dir / entry.name, env_ctx, keep_unresolved)
        else:
            shutil.copy2(entry, rendered_dir / entry.name)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--merged-dir", required=True)
    parser.add_argument("--rendered-dir", required=True)
    parser.add_argument("--runtime-context-file", required=True)
    parser.add_argument(
        "--volatile-keys",
        required=True,
        help="JSON list of engine-volatile context keys kept verbatim at render time",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    merged_dir = Path(args.merged_dir).resolve()
    if not merged_dir.is_dir():
        raise RuntimeError(f"merged cfg dir not found: {merged_dir}")
    rendered_dir = Path(args.rendered_dir).resolve()

    volatile_keys = json.loads(args.volatile_keys)
    if not isinstance(volatile_keys, list) or not all(isinstance(k, str) for k in volatile_keys):
        raise RuntimeError("--volatile-keys must be a JSON list of strings")

    runtime_context = load_runtime_context(Path(args.runtime_context_file).resolve())
    env_ctx = {k: v for k, v in runtime_context.items() if k not in volatile_keys}

    render_cfg_tree(merged_dir, rendered_dir, env_ctx, frozenset(volatile_keys))
    print(f"Rendered cfg written: {rendered_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
