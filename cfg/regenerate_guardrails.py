#!/usr/bin/env python3
"""Generate or check typed CTL and PLT guardrail baselines."""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common, guardrails  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("plt", "ctl"))
    parser.add_argument(
        "--ctl-cfg-root",
        required=True,
        help="Dev CTL cfg root with writable local cfg sources.",
    )
    parser.add_argument(
        "--execution-runtime-mode",
        required=True,
        choices=common.EXECUTION_RUNTIME_MODES,
    )
    parser.add_argument(
        "--execution-params",
        dest="execution_param",
        action=common.ExecutionParamsAction,
        default=[],
        metavar="KEY=VALUE[,KEY=VALUE...]",
    )
    parser.add_argument(
        "--providers",
        dest="providers",
        required=True,
        type=common.parse_comma_list,
        metavar="NAME[,NAME...]",
    )
    parser.add_argument(
        "--execution-context",
        action=common.ExecutionParamsAction,
        default=[],
        metavar="KEY=VALUE[,KEY=VALUE...]",
    )
    parser.add_argument(
        "--policy",
        action="append",
        dest="policies",
        help="Generate/check only subjects containing this policy; repeatable.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify selected baselines without writing files.",
    )
    parser.add_argument("--keep-artifacts", action="store_true")
    args = parser.parse_args()
    if args.mode == "ctl" and args.keep_artifacts:
        parser.error("--keep-artifacts is only valid with --mode plt")
    return args


def build_context(args: argparse.Namespace, ctl_cfg_root: Path) -> dict[str, object]:
    context = common.build_execution_context(
        ctl_cfg_root,
        action=None,
        ctl_profile=None,
        execution_params=dict(args.execution_param),
        execution_runtime_mode=args.execution_runtime_mode,
        providers=args.providers,
    )
    for key, value in args.execution_context:
        context[key] = value
    return context


def bound_local_roots(ctl_cfg_root: Path, temp_root: Path) -> tuple[Path, Path]:
    sources = common.load_cfg_sources(ctl_cfg_root)
    remote = [key for key, entry in sources.items() if "repo_path" not in entry]
    if remote:
        raise RuntimeError(
            "guardrail generation requires writable local cfg sources; "
            f"use generated dev CTL cfg (remote entries: {remote})"
        )
    roots = common.materialize_cfg_sources(
        ctl_cfg_root,
        ref_policy="local_dirty_allowed",
        run_cfg_dir=temp_root / "cfg",
    )
    return roots["plt"], roots["guardrails"]


def select_entries(
    entries: list[dict],
    policies: list[str] | None,
    known_policies: set[str],
) -> list[dict]:
    if not policies:
        return entries
    unknown = sorted(set(policies) - known_policies)
    if unknown:
        raise RuntimeError(f"unknown guardrail policies: {unknown}")
    selected = [
        entry for entry in entries if set(entry["policies"]) & set(policies)
    ]
    if not selected:
        raise RuntimeError(
            f"no active guardrail subject contains requested policies {policies}"
        )
    return selected


def emit_coverage(entries: list[dict], *, status: str) -> None:
    print("coverage_scope: selected_context")
    print("global_coverage: not-enumerable; supply contexts explicitly")
    for entry in entries:
        print(
            f"{status}: subject={entry['subject']} "
            f"policies={list(entry['policies'])} "
            f"paths={sorted(entry['values'])}"
        )


def write_entries(entries: list[dict], guardrails_cfg_root: Path) -> None:
    for entry in entries:
        path = guardrails.write_guardrail_baseline(
            guardrails_cfg_root,
            subject=entry["subject"],
            values=entry["values"],
        )
        print(f"wrote {path} subject={entry['subject']}")


def run_plt(args: argparse.Namespace) -> int:
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"CTL cfg root not found: {ctl_cfg_root}")
    execution_context = build_context(args, ctl_cfg_root)
    common.validate_execution_context_constraints(
        ctl_cfg_root,
        execution_context,
    )
    scope_params = common.scope_params_from_context(execution_context)
    temp_root = Path(tempfile.mkdtemp(prefix="atlas-guardrails-plt-"))
    try:
        plt_cfg_root, guardrails_cfg_root = bound_local_roots(
            ctl_cfg_root,
            temp_root,
        )
        policies = guardrails.load_guardrail_policies(plt_cfg_root, owner="plt")
        if not policies:
            raise RuntimeError(
                f"no PLT guardrail policies found under {plt_cfg_root}"
            )
        merged_dir = temp_root / "merged"
        common.merge_plt_cfg_dirs(
            plt_cfg_root=plt_cfg_root,
            plt_merged_dir=merged_dir,
            ctl_profile="regenerate-guardrails",
            plt_overlays=[],
            scope_params=scope_params,
            execution_context=execution_context,
        )
        rendered_dir = common.render_plt_cfg(
            merged_dir,
            temp_root,
            execution_context,
        )
        entries = guardrails.materialize_plt_guardrails(
            ctl_cfg_root,
            plt_cfg_root,
            rendered_dir,
            execution_context,
            scope_params,
        )
        entries = select_entries(entries, args.policies, set(policies))
        if not entries:
            raise RuntimeError(
                f"no active PLT guardrail policy matched params {scope_params}"
            )
        if args.check:
            guardrails.verify_materialized_guardrails(
                entries,
                guardrails_cfg_root,
                policies,
                owner="plt",
            )
            emit_coverage(entries, status="covered")
        else:
            write_entries(entries, guardrails_cfg_root)
            emit_coverage(entries, status="generated")
    finally:
        if args.keep_artifacts:
            print(f"kept artifacts: {temp_root}")
        else:
            shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def run_ctl(args: argparse.Namespace) -> int:
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"CTL cfg root not found: {ctl_cfg_root}")
    execution_context = build_context(args, ctl_cfg_root)
    common.validate_execution_context_constraints(
        ctl_cfg_root,
        execution_context,
    )
    policies = guardrails.load_guardrail_policies(ctl_cfg_root, owner="ctl")
    if not policies:
        raise RuntimeError("no CTL guardrail policies found")
    entries = guardrails.materialize_ctl_guardrails(
        ctl_cfg_root,
        execution_context,
    )
    entries = select_entries(entries, args.policies, set(policies))
    if not entries:
        raise RuntimeError("no active CTL guardrail policy matched this context")
    temp_root = Path(tempfile.mkdtemp(prefix="atlas-guardrails-ctl-"))
    try:
        _, guardrails_cfg_root = bound_local_roots(ctl_cfg_root, temp_root)
        if args.check:
            guardrails.verify_materialized_guardrails(
                entries,
                guardrails_cfg_root,
                policies,
                owner="ctl",
            )
            emit_coverage(entries, status="covered")
        else:
            write_entries(entries, guardrails_cfg_root)
            emit_coverage(entries, status="generated")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def main() -> int:
    args = parse_args()
    return run_plt(args) if args.mode == "plt" else run_ctl(args)


if __name__ == "__main__":
    raise SystemExit(main())
