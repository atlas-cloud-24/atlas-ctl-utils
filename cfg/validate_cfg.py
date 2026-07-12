#!/usr/bin/env python3
"""Validate the full bound plt cfg tree and ctl/plt guardrails for one variation."""

from __future__ import annotations

import argparse
import os
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
    parser.add_argument("--ctl-cfg-root", required=True)
    parser.add_argument("--ctl-profile", required=True)
    parser.add_argument("--execution-runtime", required=True, choices=common.EXECUTION_RUNTIMES)
    parser.add_argument("--ctl-state-backend-key", required=True)
    parser.add_argument("--execution-params", dest="execution_param", action="append", type=key_value, default=[])
    parser.add_argument("--keep-artifacts", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"ctl cfg root not found: {ctl_cfg_root}")

    execution_context = common.build_execution_context(
        ctl_cfg_root,
        action=None,
        ctl_profile=args.ctl_profile,
        execution_params=dict(args.execution_param),
        execution_runtime=args.execution_runtime,
    )
    common.validate_execution_context_constraints(ctl_cfg_root, execution_context)
    provider = execution_context.get("execution_context.params.provider")
    if isinstance(provider, str) and provider.strip():
        common.get_provider_adapter(provider.strip()).synthesize_validation_provider_facts(execution_context, ctl_cfg_root)
    scope_params = common.scope_params_from_context(execution_context)

    temp_root = Path(tempfile.mkdtemp(prefix="atlas-validate-cfg-"))
    try:
        roots = common.materialize_cfg_sources(
            ctl_cfg_root,
            ref_policy=common.ctl_ref_policy(ctl_cfg_root, args.ctl_profile),
            run_cfg_dir=temp_root / "cfg",
            token=os.getenv("cfg_source_token"),
        )
        plt_cfg_root = roots["plt"]
        guardrails_cfg_root = roots["guardrails"]
        merged_dir = temp_root / "merged"
        common.merge_plt_cfg_dirs(
            plt_cfg_root=plt_cfg_root,
            plt_merged_dir=merged_dir,
            ctl_profile="validate-cfg",
            plt_overlays=[],
            scope_params=scope_params,
            execution_context=execution_context,
        )
        rendered_dir = common.render_plt_cfg(merged_dir, temp_root, execution_context)
        common.verify_guardrails(
            ctl_cfg_root,
            plt_cfg_root,
            guardrails_cfg_root,
            rendered_dir,
            execution_context,
            scope_params,
            args.ctl_state_backend_key,
        )
        print(f"OK: full bound plt cfg valid for params {scope_params or '{}'}")
    finally:
        if args.keep_artifacts:
            print(f"kept artifacts: {temp_root}")
        else:
            shutil.rmtree(temp_root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
