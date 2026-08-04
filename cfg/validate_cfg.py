#!/usr/bin/env python3
"""Validate the full bound plt cfg tree and ctl/plt guardrails.

One variation by default. `--overlay-sweep` validates the base tree, then each
overlay this execution context permits on its own, then all of them together —
linear in the number of overlays rather than 2^N, because overlay application is
a last-wins merge by file path: the failures that exist are an overlay broken on
its own and two overlays writing one path, and those two passes cover both.
"""


from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.catalog import workflow as catalog_workflow
from engine.guardrails import verify as guardrails_verify
from engine.cfg import materialize as cfg_materialize
from engine.cfg import overlays as cfg_overlays
from engine.cfg import resources as cfg_resources
from engine.cfg import secrets as cfg_secrets
from engine.cfg import tree as cfg_tree
from engine.execution import providers as execution_providers
from engine.execution import run_context as execution_run_context
from engine.kernel import scalars as kernel_scalars
from engine.run import policy as run_policy
from engine.cli import args as cli_args


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ctl-cfg-root", required=True)
    parser.add_argument("--ctl-profile", required=True)
    parser.add_argument("--execution-runtime-mode", required=True, choices=run_policy.EXECUTION_RUNTIME_MODES)
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
        "--plt-overlays",
        dest="plt_overlays",
        type=cli_args.parse_overlays_arg,
        default=[],
        metavar="NAME[,NAME...]",
        help="Validate the tree with these plt overlays applied.",
    )
    parser.add_argument(
        "--overlay-sweep",
        action="store_true",
        help=(
            "Validate the base tree, then EACH overlay this execution context "
            "permits on its own, then all of them together. Linear in the number "
            "of overlays; see the module docstring for why not every subset."
        ),
    )
    parser.add_argument("--keep-artifacts", action="store_true")
    args = parser.parse_args()
    if args.plt_overlays and args.overlay_sweep:
        parser.error("--plt-overlays selects one variation; --overlay-sweep enumerates them")
    return args


def _validate_variation(
    *,
    ctl_cfg_root: Path,
    ctl_profile: str,
    execution_context: dict,
    scope_params: dict,
    plt_overlays: list[str],
    keep_artifacts: bool,
) -> None:
    """One (params, overlays) variation, end to end.

    Guardrails are verified for the BASE tree only. `coverage.yaml` enumerates
    parameter assignments and knows nothing about overlays, so every baseline
    describes rendered cfg with none applied; verifying an overlay variation
    against them would report every intended override as a violation. The overlay
    runs still exercise what they exist to exercise — that each overlay merges,
    resolves its references, and does not fight another over one file path.
    """
    temp_root = Path(tempfile.mkdtemp(prefix="atlas-validate-cfg-"))
    label = ",".join(plt_overlays) if plt_overlays else "base (no overlays)"
    try:
        roots = cfg_materialize.materialize_cfg_sources(
            ctl_cfg_root,
            ref_policy=run_policy.ctl_ref_policy(ctl_cfg_root, ctl_profile),
            run_cfg_dir=temp_root / "cfg",
            token=os.getenv("cfg_source_token"),
        )
        plt_cfg_root = roots["plt"]
        guardrails_cfg_root = roots["guardrails"]
        merged_dir = temp_root / "merged"
        cfg_tree.merge_plt_cfg_dirs(
            plt_cfg_root=plt_cfg_root,
            plt_merged_dir=merged_dir,
            ctl_profile="validate-cfg",
            plt_overlays=plt_overlays,
            scope_params=scope_params,
            execution_context=execution_context,
        )
        rendered_dir = cfg_tree.render_plt_cfg(merged_dir, temp_root, execution_context)
        if plt_overlays:
            print(f"OK: cfg valid with overlays {label} (guardrails skipped — baselines are base-only)")
        else:
            guardrails_verify.verify_guardrails(
                ctl_cfg_root,
                plt_cfg_root,
                guardrails_cfg_root,
                rendered_dir,
                execution_context,
                scope_params,
            )
            print(f"OK: full bound plt cfg valid for params {scope_params or '{}'}")
    finally:
        if keep_artifacts:
            print(f"kept artifacts: {temp_root}")
        else:
            shutil.rmtree(temp_root, ignore_errors=True)


def _sweep_variations(plt_cfg_root: Path, execution_context: dict) -> list[list[str]]:
    """Base, then each permitted overlay alone, then all of them together.

    NOT every subset. Overlay application is a last-wins merge by file path, so
    the failures that exist are an overlay broken on its OWN — a stale path, a
    reference the base no longer defines — and two overlays writing one path.
    Each-alone catches the first, all-together catches the second, and a subset in
    between adds nothing while costing 2^N runs.
    """

    permitted = sorted(
        name for name, candidate in cfg_overlays.discover_overlay_candidates(
            plt_cfg_root, execution_context=execution_context
        ).items()
        if candidate.get("matches")
    )
    variations: list[list[str]] = [[]]
    variations += [[name] for name in permitted]
    if len(permitted) > 1:
        variations.append(permitted)
    return variations


def main() -> int:
    args = parse_args()
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"ctl cfg root not found: {ctl_cfg_root}")

    execution_context = execution_run_context.build_execution_context(
        ctl_cfg_root,
        action=None,
        ctl_profile=args.ctl_profile,
        execution_params=dict(args.execution_param),
        execution_runtime_mode=args.execution_runtime_mode,
        providers=args.providers,
    )
    execution_run_context.validate_execution_context_constraints(ctl_cfg_root, execution_context)
    # A registration accepted but not backed is the silent acceptance the
    # registry exists to prevent, so both halves are checked before any render:
    # what each provider promises, and that every secret is declared and
    # resolvable without a cycle.
    execution_providers.validate_declared_contracts(ctl_cfg_root)
    cfg_secrets.validate_declared_secrets(ctl_cfg_root)
    # Every workflow's instance params, not just the one a run selects.
    # The per-run guard is exact but sees one workflow, so a misdeclaration
    # elsewhere stays silent until someone runs it. This pass is static — no
    # execution context, every branch — and it belongs to the SKIPPABLE gate: a
    # workflow that is not in the run cannot corrupt it, while the per-run guard
    # still fires unskippably for the one that is.
    catalog_workflow.validate_all_workflow_instance_params(
        cfg_resources.collect_resource(ctl_cfg_root, "workflows", entry_depth=1),
        cfg_resources.collect_resource(ctl_cfg_root, "targets"),
    )
    # whole-tree tooling activates every declared domain (see helper)
    execution_context = execution_run_context.whole_tree_execution_context(ctl_cfg_root, execution_context)
    scope_params = execution_run_context.scope_params_from_context(execution_context)

    variations = [args.plt_overlays]
    if args.overlay_sweep:
        probe = Path(tempfile.mkdtemp(prefix="atlas-validate-overlays-"))
        try:
            roots = cfg_materialize.materialize_cfg_sources(
                ctl_cfg_root,
                ref_policy=run_policy.ctl_ref_policy(ctl_cfg_root, args.ctl_profile),
                run_cfg_dir=probe / "cfg",
                token=os.getenv("cfg_source_token"),
            )
            variations = _sweep_variations(roots["plt"], execution_context)
        finally:
            shutil.rmtree(probe, ignore_errors=True)
        print(f"overlay sweep: {len(variations)} variations")

    for plt_overlays in variations:
        _validate_variation(
            ctl_cfg_root=ctl_cfg_root,
            ctl_profile=args.ctl_profile,
            execution_context=execution_context,
            scope_params=scope_params,
            plt_overlays=plt_overlays,
            keep_artifacts=args.keep_artifacts,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
