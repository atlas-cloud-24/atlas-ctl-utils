#!/usr/bin/env python3
"""Regenerate guardrail baselines.

--mode plt: compose + render one cfg variation (one execution-input assignment)
via the same engine code path the pipeline uses, then rewrite the scope-local
`__guardrails__.yaml` (`hashes:` only) of every active scope with matching root
declarations. The temp merged/rendered tree is discarded unless
--keep-artifacts is passed; the only durable output is the committed baseline
files.

--mode ctl: refresh the combined ctl `guardrails.guarded_vars` hashes from the
execution context built for the given input assignment (no rendering).
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("plt", "ctl"))
    parser.add_argument("--cfg-root", required=True, help="Owner cfg root (plt or ctl per --mode).")
    parser.add_argument(
        "--execution-params",
        dest="execution_param",
        action="append",
        type=key_value,
        default=[],
        help="Execution param key=value defining the cfg variation. May be repeated.",
    )
    parser.add_argument(
        "--ctl-cfg-root",
        help="Ctl cfg root used to build the execution context for rendering (plt mode).",
    )
    parser.add_argument(
        "--execution-context",
        action="append",
        type=key_value,
        default=[],
        help="Extra execution-context entry as full dotted key=value; overrides built values.",
    )
    parser.add_argument(
        "--var",
        action="append",
        dest="vars",
        help="Ctl mode: guarded var name to add/regenerate. Defaults to already-declared vars.",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Plt mode: keep the temp merged/rendered tree for inspection.",
    )
    args = parser.parse_args()
    if args.mode == "ctl" and args.keep_artifacts:
        parser.error("--keep-artifacts is only valid with --mode plt")
    if args.mode == "plt" and args.vars:
        parser.error("--var is only valid with --mode ctl; plt vars come from root declarations")
    return args


def build_context(args: argparse.Namespace, ctl_cfg_root: Path | None) -> dict[str, object]:
    context: dict[str, object] = {}
    if ctl_cfg_root is not None:
        context = common.build_execution_context(
            ctl_cfg_root,
            action=None,
            ctl_profile=None,
            execution_params=dict(args.execution_param),
        )
    else:
        for key, value in dict(args.execution_param).items():
            context[f"{common.EXECUTION_CONTEXT_ROOT}.params.{key}"] = value
    for key, value in args.execution_context:
        context[key] = value
    return context


def run_plt(args: argparse.Namespace) -> int:
    plt_cfg_root = Path(args.cfg_root).expanduser().resolve()
    if not plt_cfg_root.is_dir():
        raise RuntimeError(f"plt cfg root not found: {plt_cfg_root}")
    declarations = common.load_plt_guard_declarations(plt_cfg_root)
    if not declarations:
        raise RuntimeError(f"no guard declarations at the plt cfg root: {plt_cfg_root}")

    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve() if args.ctl_cfg_root else None
    execution_context = build_context(args, ctl_cfg_root)
    scope_params = dict(args.execution_param)

    tmp_dir = Path(tempfile.mkdtemp(prefix="atlas-regenerate-guardrails-"))
    try:
        merged_dir = tmp_dir / "merged"
        rendered_dir = tmp_dir / "rendered"
        # Same engine code path as the pipeline: compose scopes, then render.
        common.merge_plt_cfg_dirs(
            plt_cfg_root=plt_cfg_root,
            plt_merged_dir=merged_dir,
            ctl_profile="regenerate-guardrails",
            plt_overlays=[],
            scope_params=scope_params,
        )
        rendered_dir.mkdir(parents=True)
        env_ctx = {k: v for k, v in execution_context.items() if k not in common.ENGINE_VOLATILE_CONTEXT_KEYS}
        volatile = frozenset(common.ENGINE_VOLATILE_CONTEXT_KEYS)
        for entry in sorted(merged_dir.iterdir()):
            if entry.is_dir():
                common.render_scope_tree(entry, rendered_dir / entry.name, env_ctx, volatile)

        wrote_any = False
        for scope in common.discover_active_cfg_scopes(plt_cfg_root, scope_params=scope_params):
            matching = [d for d in declarations if common.guard_declaration_matches_scope(d, scope)]
            if not matching:
                continue
            target_dir = common.rendered_scope_target_dir(rendered_dir, scope["target_path"])
            label = f"plt scope {scope['scope_path']}->{scope['target_path']}"
            hashes: dict[str, str] = {}
            for declaration in matching:
                var_name = declaration["path"]
                value = common.read_rendered_guard_value(target_dir, var_name, label=label)
                hashes[var_name] = common.guard_value_hash(value, label=f"plt.{var_name}")
            baseline_path = scope["scope_root"] / common.PLT_GUARDRAILS_FILENAME
            baseline_path.write_text(
                yaml.safe_dump({"hashes": hashes}, sort_keys=True),
                encoding="utf-8",
            )
            wrote_any = True
            print(f"wrote {baseline_path}")
            for var_name in sorted(hashes):
                print(f"  {var_name}: {hashes[var_name]}")

        if not wrote_any:
            raise RuntimeError(
                f"no active scope matched any declaration for params {scope_params}; nothing written"
            )
    finally:
        if args.keep_artifacts:
            print(f"kept artifacts: {tmp_dir}")
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)
    return 0


def run_ctl(args: argparse.Namespace) -> int:
    ctl_cfg_root = Path(args.cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"ctl cfg root not found: {ctl_cfg_root}")
    existing = common.load_ctl_guarded_vars(ctl_cfg_root)
    var_names = args.vars or sorted(existing)
    if not var_names:
        raise RuntimeError("no ctl guarded vars declared; pass --var to add one")

    execution_context = build_context(args, ctl_cfg_root)
    hashes: dict[str, str] = dict(existing)
    for var_name in var_names:
        ref = f"{common.EXECUTION_CONTEXT_ROOT}.params.{var_name}"
        if ref not in execution_context:
            raise RuntimeError(f"ctl guarded var {var_name!r} is not available: {ref} missing")
        hashes[var_name] = common.guard_value_hash(execution_context[ref], label=f"ctl.{var_name}")

    path = ctl_cfg_root / "guardrails.yaml"
    path.write_text(
        yaml.safe_dump({"guardrails": {"guarded_vars": hashes}}, sort_keys=True),
        encoding="utf-8",
    )
    print(f"wrote {path}")
    for var_name in sorted(hashes):
        print(f"  {var_name}: {hashes[var_name]}")
    return 0


def main() -> int:
    args = parse_args()
    if args.mode == "plt":
        return run_plt(args)
    return run_ctl(args)


if __name__ == "__main__":
    raise SystemExit(main())
