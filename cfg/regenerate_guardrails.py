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

from engine.cfg import materialize as cfg_materialize
from engine.cfg import tree as cfg_tree
from engine.execution import run_context as execution_run_context
from engine.kernel import scalars as kernel_scalars
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import policy as run_policy
from engine.cli import args as cli_args
from engine.guardrails import policies as guardrails


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
        choices=run_policy.EXECUTION_RUNTIME_MODES,
    )
    parser.add_argument(
        "--execution-params",
        dest="execution_param",
        action=cli_args.ExecutionParamsAction,
        default=[],
        metavar="KEY=VALUE[,KEY=VALUE...]",
    )
    parser.add_argument(
        "--providers",
        dest="providers",
        required=True,
        type=kernel_scalars.parse_comma_list,
        metavar="NAME[,NAME...]",
    )
    parser.add_argument(
        "--execution-context",
        action=cli_args.ExecutionParamsAction,
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
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_assignments",
        help="Cover every assignment declared in the guardrail repository's "
        "coverage.yaml for this --mode, instead of the single one given. Each "
        "assignment is merged over that mode's `common` block and run through "
        "the same path as a single invocation.",
    )
    parser.add_argument("--keep-artifacts", action="store_true")
    args = parser.parse_args()
    if args.mode == "ctl" and args.keep_artifacts:
        parser.error("--keep-artifacts is only valid with --mode plt")
    if args.all_assignments and args.keep_artifacts:
        parser.error("--keep-artifacts is not supported with --all")
    return args


def build_context(
    args: argparse.Namespace,
    ctl_cfg_root: Path,
    extra_params: dict[str, str] | None = None,
) -> dict[str, object]:
    context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=None,
        ctl_profile=None,
        execution_params={**dict(args.execution_param), **(extra_params or {})},
        execution_runtime_mode=args.execution_runtime_mode,
        providers=args.providers,
    )
    for key, value in args.execution_context:
        context[key] = value
    return context


COVERAGE_FILENAME = "coverage.yaml"


def load_coverage_assignments(
    guardrails_cfg_root: Path, mode: str
) -> list[dict[str, str]]:
    """The DECLARED assignments needing a baseline, for one mode.

    Read from `coverage.yaml` in the guardrails repository, beside the baselines
    it governs, so adding an assignment and the baseline it produces land in one
    commit and one review.

    Deliberately NOT derived from `fan_out_param_sets`: those declare which
    combinations are DEPLOYED together, which is a different question from which
    must be GUARDED. Coupling them would widen or narrow coverage silently
    whenever a fan-out changed for an unrelated reason — and a fan-out list is
    also not something the guardrail owner controls.

    `common` is merged under each assignment, so an assignment may override a
    shared axis where it genuinely differs.
    """

    path = Path(guardrails_cfg_root) / COVERAGE_FILENAME
    if not path.is_file():
        raise RuntimeError(f"--all requires a coverage declaration: {path}")
    doc = kernel_yaml_io.load_yaml(path) or {}
    coverage = doc.get("guardrail_coverage")
    if not isinstance(coverage, dict):
        raise RuntimeError(f"{path}: guardrail_coverage must be a mapping")
    block = coverage.get(mode)
    if not isinstance(block, dict):
        raise RuntimeError(f"{path}: no guardrail_coverage.{mode} block")
    shared = block.get("common") or {}
    if not isinstance(shared, dict):
        raise RuntimeError(f"{path}: guardrail_coverage.{mode}.common must be a mapping")
    declared = block.get("assignments")
    if not isinstance(declared, list) or not declared:
        raise RuntimeError(
            f"{path}: guardrail_coverage.{mode}.assignments must be a non-empty list"
        )
    assignments: list[dict[str, str]] = []
    seen: set[tuple] = set()
    for index, entry in enumerate(declared):
        if not isinstance(entry, dict) or not entry:
            raise RuntimeError(
                f"{path}: guardrail_coverage.{mode}.assignments[{index}] "
                "must be a non-empty mapping of params"
            )
        merged = {
            str(k): str(v) for k, v in {**shared, **entry}.items()
        }
        fingerprint = tuple(sorted(merged.items()))
        if fingerprint in seen:
            raise RuntimeError(
                f"{path}: guardrail_coverage.{mode}.assignments[{index}] duplicates "
                "an earlier assignment; each must be distinct"
            )
        seen.add(fingerprint)
        assignments.append(merged)
    return assignments


def format_assignment(params: dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(params.items())) or "<base>"


def bound_local_roots(ctl_cfg_root: Path, temp_root: Path) -> tuple[Path, Path]:
    sources = cfg_materialize.load_cfg_sources(ctl_cfg_root)
    remote = [key for key, entry in sources.items() if "repo_path" not in entry]
    if remote:
        raise RuntimeError(
            "guardrail generation requires writable local cfg sources; "
            f"use generated dev CTL cfg (remote entries: {remote})"
        )
    roots = cfg_materialize.materialize_cfg_sources(
        ctl_cfg_root,
        ref_policy="local_dirty_allowed",
        run_cfg_dir=temp_root / "cfg",
    )
    return roots["plt"], roots["guardrails"]


class NoMatchingPolicy(RuntimeError):
    """This assignment activates no guardrail policy.

    A hard error for a single invocation — the caller asked for a baseline that
    cannot exist. Under `--all` it is an ordinary skip: the declared coverage is
    broader than any one domain's policies, and `--policy` narrows it further.
    """

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
        # Same meaning as "no policy matched": under --all with --policy, most
        # assignments legitimately contain none of the requested policies.
        raise NoMatchingPolicy(
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


def run_plt(args: argparse.Namespace, extra_params: dict[str, str] | None = None) -> int:
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"CTL cfg root not found: {ctl_cfg_root}")
    execution_context = build_context(args, ctl_cfg_root, extra_params)
    execution_run_context.validate_execution_context_constraints(
        ctl_cfg_root,
        execution_context,
    )
    # whole-tree tooling activates every declared domain (see helper)
    execution_context = execution_run_context.whole_tree_execution_context(ctl_cfg_root, execution_context)
    scope_params = execution_run_context.scope_params_from_context(execution_context)
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
        cfg_tree.merge_plt_cfg_dirs(
            plt_cfg_root=plt_cfg_root,
            plt_merged_dir=merged_dir,
            ctl_profile="regenerate-guardrails",
            plt_overlays=[],
            scope_params=scope_params,
            execution_context=execution_context,
        )
        rendered_dir = cfg_tree.render_plt_cfg(
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
            raise NoMatchingPolicy(
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


def run_ctl(args: argparse.Namespace, extra_params: dict[str, str] | None = None) -> int:
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"CTL cfg root not found: {ctl_cfg_root}")
    execution_context = build_context(args, ctl_cfg_root, extra_params)
    execution_run_context.validate_execution_context_constraints(
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
        raise NoMatchingPolicy("no active CTL guardrail policy matched this context")
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


def run_all(args: argparse.Namespace) -> int:
    """Cover every DECLARED assignment for this --mode in one pass.

    The list comes from the guardrail repository's coverage.yaml, so what must be
    guarded is a decision recorded next to the baselines rather than inferred
    from run dispatch.

    Each assignment is independent: one that activates no policy is skipped, one
    that the constraints reject is skipped, and under --check a stale or missing
    baseline is COLLECTED rather than raised, so the report names every problem
    instead of dying on the first.
    """
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"CTL cfg root not found: {ctl_cfg_root}")
    runner = run_plt if args.mode == "plt" else run_ctl
    temp_root = Path(tempfile.mkdtemp(prefix="atlas-guardrails-coverage-"))
    try:
        _, guardrails_cfg_root = bound_local_roots(ctl_cfg_root, temp_root)
        assignments = load_coverage_assignments(guardrails_cfg_root, args.mode)
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    covered, skipped, failed = [], [], []
    for params in assignments:
        label = format_assignment(params)
        print(f"\n=== {args.mode} :: {label}")
        try:
            runner(args, params)
        except NoMatchingPolicy as exc:
            skipped.append((label, str(exc)))
            print(f"skipped: {exc}")
        except RuntimeError as exc:
            # A constraint rejecting the combination is also a skip: the union of
            # every fan-out's members necessarily contains combinations that are
            # not valid for this mode.
            if "execution_context_constraints" in str(exc) or "constraint" in str(exc):
                skipped.append((label, str(exc)))
                print(f"skipped (constraint): {exc}")
            else:
                failed.append((label, str(exc)))
                print(f"FAILED: {exc}")
        else:
            covered.append(label)

    verb = "checked" if args.check else "generated"
    print(
        f"\n=== summary ({args.mode}): {len(covered)} {verb}, "
        f"{len(skipped)} skipped, {len(failed)} failed, "
        f"{len(assignments)} assignments enumerated"
    )
    for label, reason in failed:
        print(f"  failed: {label}: {reason}")
    return 1 if failed else 0


def main() -> int:
    args = parse_args()
    if args.all_assignments:
        return run_all(args)
    return run_plt(args) if args.mode == "plt" else run_ctl(args)


if __name__ == "__main__":
    raise SystemExit(main())
