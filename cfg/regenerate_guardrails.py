#!/usr/bin/env python3
"""Regenerate ctl or plt guardrail baselines.

Plt mode resolves writable local plt and guardrail repositories from the dev ctl
cfg binding. Ctl mode reads declarations from ctl cfg and writes resolved baselines to the bound guardrail repository.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def key_value(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(f"Expected key=value, got {value!r}")
    key, raw = value.split("=", 1)
    key, raw = key.strip(), raw.strip()
    if not key or not raw:
        raise argparse.ArgumentTypeError(f"Expected non-empty key=value, got {value!r}")
    return key, raw


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("plt", "ctl"))
    parser.add_argument("--ctl-cfg-root", required=True, help="Dev ctl cfg root with writable local cfg sources.")
    parser.add_argument("--execution-runtime-mode", required=True, choices=common.EXECUTION_RUNTIME_MODES)
    parser.add_argument("--execution-params", dest="execution_param", action="append", type=key_value, default=[])
    parser.add_argument("--execution-context", action="append", type=key_value, default=[])
    parser.add_argument("--var", action="append", dest="vars", help="Ctl mode declaration ref; repeatable.")
    parser.add_argument("--keep-artifacts", action="store_true")
    args = parser.parse_args()
    if args.mode == "plt":
        if args.vars:
            parser.error("--var is not valid with --mode plt")
    elif args.keep_artifacts:
        parser.error("--keep-artifacts is only valid with --mode plt")
    return args


def build_context(args: argparse.Namespace, ctl_cfg_root: Path) -> dict[str, object]:
    context = common.build_execution_context(
        ctl_cfg_root,
        action=None,
        ctl_profile=None,
        execution_params=dict(args.execution_param),
        execution_runtime_mode=args.execution_runtime_mode,
    )
    for key, value in args.execution_context:
        context[key] = value
    return context


def bound_local_roots(ctl_cfg_root: Path, temp_root: Path) -> tuple[Path, Path]:
    sources = common.load_cfg_sources(ctl_cfg_root)
    remote = [key for key, entry in sources.items() if "repo_path" not in entry]
    if remote:
        raise RuntimeError(
            "guardrail regeneration requires writable local cfg sources; "
            f"use generated dev ctl cfg (remote entries: {remote})"
        )
    roots = common.materialize_cfg_sources(
        ctl_cfg_root,
        ref_policy="local_dirty_allowed",
        run_cfg_dir=temp_root / "cfg",
    )
    return roots["plt"], roots["guardrails"]


def run_plt(args: argparse.Namespace) -> int:
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"ctl cfg root not found: {ctl_cfg_root}")
    execution_context = build_context(args, ctl_cfg_root)
    common.validate_execution_context_constraints(ctl_cfg_root, execution_context)
    scope_params = common.scope_params_from_context(execution_context)

    temp_root = Path(tempfile.mkdtemp(prefix="atlas-regenerate-guardrails-"))
    try:
        plt_cfg_root, guardrails_cfg_root = bound_local_roots(ctl_cfg_root, temp_root)
        policies = common.load_plt_guardrail_policies(plt_cfg_root)
        if not policies:
            raise RuntimeError(f"no plt guardrail policies at the plt cfg root: {plt_cfg_root}")

        merged_dir = temp_root / "merged"
        common.merge_plt_cfg_dirs(
            plt_cfg_root=plt_cfg_root,
            plt_merged_dir=merged_dir,
            ctl_profile="regenerate-guardrails",
            plt_overlays=[],
            scope_params=scope_params,
            execution_context=execution_context,
        )
        rendered_dir = common.render_plt_cfg(merged_dir, temp_root, execution_context)

        active_scopes = common.discover_active_cfg_scopes(
            plt_cfg_root,
            scope_params=scope_params,
            execution_context=execution_context,
        )
        scopes_by_target = {}
        for scope in active_scopes:
            scopes_by_target.setdefault(scope["target_path"], []).append(scope)
        composition = common.load_scope_composition(plt_cfg_root)

        wrote = []
        for target_path, scopes in sorted(scopes_by_target.items()):
            matching = common.active_plt_guardrail_policies(
                policies,
                target_path,
                execution_context,
            )
            names = common.protected_vars_for_policies(matching, target_path=target_path)
            if not names:
                continue
            target_dir = common.rendered_scope_target_dir(rendered_dir, target_path)
            label = f"plt target {target_path}"
            protected_values = {}
            for name in names:
                value = common.read_rendered_guard_value(target_dir, name, label=label)
                protected_values[name] = common.guard_value_text(value, label=f"plt.{name}")
            instance = common.build_plt_guardrail_instance(
                target_path,
                scopes,
                composition,
                execution_context,
            )
            path = common.write_plt_guardrail_baseline(
                guardrails_cfg_root,
                instance=instance,
                protected_values=protected_values,
            )
            wrote.append(path)
            print(f"wrote {path} instance={instance}")
        if not wrote:
            raise RuntimeError(f"no active rendered target matched a plt guardrail policy for params {scope_params}")
    finally:
        if args.keep_artifacts:
            print(f"kept artifacts: {temp_root}")
        else:
            shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def run_ctl(args: argparse.Namespace) -> int:
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"ctl cfg root not found: {ctl_cfg_root}")
    execution_context = build_context(args, ctl_cfg_root)
    common.validate_execution_context_constraints(ctl_cfg_root, execution_context)

    declarations = common.load_ctl_guard_declarations(ctl_cfg_root)
    if not declarations:
        raise RuntimeError("no ctl guard declarations found")
    requested_refs = args.vars or sorted(declarations)
    unknown = sorted(set(requested_refs) - set(declarations))
    if unknown:
        raise RuntimeError(f"unknown ctl guard declarations: {unknown}")

    selected_namespace, _ = common.resolve_ctl_state_namespace(
        ctl_cfg_root, execution_context
    )
    requested_namespaces = {
        common.ctl_guard_namespace_key(ref)
        for ref in requested_refs
        if common.ctl_guard_namespace_key(ref) is not None
    }
    mismatched = sorted(requested_namespaces - {selected_namespace}) if args.vars else []
    if mismatched:
        raise RuntimeError(
            f"--var refs select namespaces {mismatched}, but execution context "
            f"selects {selected_namespace!r}"
        )

    active = []
    for ref in requested_refs:
        declaration_namespace = common.ctl_guard_namespace_key(ref)
        if declaration_namespace is None or declaration_namespace == selected_namespace:
            active.append(declarations[ref])
    if not active:
        raise RuntimeError("no ctl guard declarations match the selected namespace")

    temp_root = Path(tempfile.mkdtemp(prefix="atlas-regenerate-ctl-guardrails-"))
    try:
        _, guardrails_cfg_root = bound_local_roots(ctl_cfg_root, temp_root)
        for declaration in active:
            ref = declaration["ref"]
            axes = common.resolve_guard_axes(
                [declaration],
                execution_context,
                scope_path=f"ctl guard {ref}",
            )
            value = common.resolve_ctl_guard_value(ref, ctl_cfg_root, execution_context)
            path = common.write_ctl_guardrail_baseline(
                guardrails_cfg_root,
                ref=ref,
                axes=axes,
                value=value,
            )
            print(f"wrote {path} ref={ref} axes={axes}")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def main() -> int:
    args = parse_args()
    return run_plt(args) if args.mode == "plt" else run_ctl(args)


if __name__ == "__main__":
    raise SystemExit(main())
