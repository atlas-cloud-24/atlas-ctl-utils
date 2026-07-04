#!/usr/bin/env python3
"""Validate the FULL plt cfg tree for one cfg variation (lint-style, no stages).

Runs no longer validate scopes they don't consume (selective merge: a run
merges/renders/guard-verifies only the scopes serving its stages' cfg_roots).
This action is the full-tree companion: merge + render + guard-verify EVERY
active scope for the given execution-param assignment. Run it per variation
(e.g. once per env_type, once with none for deployments/org) on demand or in CI.
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
    key = key.strip()
    raw = raw.strip()
    if not key or not raw:
        raise argparse.ArgumentTypeError(f"Expected non-empty key=value, got {value!r}")
    return key, raw


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cfg-root", required=True, help="Plt cfg root to validate.")
    parser.add_argument(
        "--ctl-cfg-root",
        required=True,
        help="Ctl cfg root used to build the execution context (execution_params, guards).",
    )
    parser.add_argument(
        "--execution-params",
        dest="execution_param",
        action="append",
        type=key_value,
        default=[],
        help="Execution param key=value defining the cfg variation. May be repeated.",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Keep the temp merged/rendered tree for inspection.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plt_cfg_root = Path(args.cfg_root).expanduser().resolve()
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    for root, label in ((plt_cfg_root, "plt cfg root"), (ctl_cfg_root, "ctl cfg root")):
        if not root.is_dir():
            raise RuntimeError(f"{label} not found: {root}")

    execution_context = common.build_execution_context(
        ctl_cfg_root,
        action=None,
        ctl_profile=None,
        execution_params=dict(args.execution_param),
    )
    scope_params = common.scope_params_from_context(execution_context)

    tmp_dir = Path(tempfile.mkdtemp(prefix="atlas-validate-cfg-"))
    try:
        merged_dir = tmp_dir / "merged"
        # Full tree: no required_target_paths filter — every active scope.
        common.merge_plt_cfg_dirs(
            plt_cfg_root=plt_cfg_root,
            plt_merged_dir=merged_dir,
            ctl_profile="validate-cfg",
            plt_overlays=[],
            scope_params=scope_params,
        )
        rendered_dir = common.render_plt_cfg(merged_dir, tmp_dir, execution_context)
        common.verify_guardrails(ctl_cfg_root, plt_cfg_root, rendered_dir, execution_context, scope_params)
        print(f"OK: full plt cfg tree valid for params {scope_params or '{}'} (render + ctl/plt guards)")
    finally:
        if args.keep_artifacts:
            print(f"kept artifacts: {tmp_dir}")
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
