#!/usr/bin/env python3
import argparse
import contextlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from utils.common import (
    get_plt_cfg_source_dirs,
    load_yaml,
    merge_plt_cfg_dirs,
    parse_overlays_arg,
    str2bool,
    validate_uuid7,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _build_active_stages(inventory, active_ids, repo_root: Path):
    inventory_env = inventory.get("env_vars", {})
    inventory_stage_ids = inventory.get("stages", [])

    active = []
    for sid in active_ids:
        if sid not in inventory_stage_ids:
            raise RuntimeError(f"Stage '{sid}' not listed in inventory")

        stage_meta_path = repo_root / "pipeline" / "stages" / sid / "stage.yaml"
        if not stage_meta_path.is_file():
            raise RuntimeError(f"Stage metadata not found: {stage_meta_path}")
        st = load_yaml(stage_meta_path)
        runtime_cfg = st.get("runtime") or {}
        if not isinstance(runtime_cfg, dict):
            raise RuntimeError(f"Stage metadata 'runtime' must be a mapping: {stage_meta_path}")
        values_json = runtime_cfg.get("values_json", True)
        env_sh = runtime_cfg.get("env_sh", True)
        if not isinstance(values_json, bool) or not isinstance(env_sh, bool):
            raise RuntimeError(f"Stage metadata 'runtime' flags must be booleans: {stage_meta_path}")

        active.append(
            {
                "id": sid,
                "cfg_files": st.get("cfg_files", []),
                "runtime": {
                    "values_json": values_json,
                    "env_sh": env_sh,
                },
                "env_vars": {
                    "inventory": inventory_env,
                    "stage": st.get("env_vars", {}),
                },
            }
        )
    return active


def get_stages_from_workflow(inventory_file: Path, workflow_file: Path, repo_root: Path):
    inventory = load_yaml(inventory_file)
    workflow = load_yaml(workflow_file)

    inventory_stage_ids = inventory.get("stages", [])
    if not isinstance(inventory_stage_ids, list):
        raise RuntimeError(f"inventory {inventory_file} 'stages' must be a list of ids")

    if "stages" in workflow:
        active_ids = []
        for sid in workflow.get("stages", []):
            if sid not in inventory_stage_ids:
                raise RuntimeError(f"Stage '{sid}' not found in inventory {inventory_file}")
            active_ids.append(sid)
    else:
        raise RuntimeError(f"workflow {workflow_file} must have 'stages'")

    active_stages = _build_active_stages(inventory, active_ids, repo_root)
    return active_ids, active_stages


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-tag", required=True)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--env-type", required=True)
    parser.add_argument(
        "--overlays",
        required=False,
        default=[],
        dest="overlays",
        type=parse_overlays_arg,
        help="Optional comma-separated cfg overlays under overlays/.",
    )
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--origin-cfg", required=True)
    parser.add_argument("--ephemeral", required=True, type=str2bool)
    parser.add_argument(
        "--skip-cfg-merge",
        action="store_true",
        help="Use cfg that was already merged by an upstream pipeline step.",
    )
    parser.add_argument("--run-id", required=True, type=validate_uuid7)
    return parser


def _resolve_workflow_file(repo_root: Path, env_type: str, inventory: str, workflow: str) -> Path:
    workflow_file = repo_root / f"pipeline/workflows/{env_type}/{inventory}/{workflow}.yaml"
    if workflow_file.is_file():
        logging.info(f"📋 Using environment-specific workflow: {workflow_file.relative_to(repo_root)}")
        return workflow_file

    base_workflow_file = repo_root / f"pipeline/workflows/base/{inventory}/{workflow}.yaml"
    if base_workflow_file.is_file():
        logging.info(f"📋 Using base workflow: {base_workflow_file.relative_to(repo_root)}")
        return base_workflow_file

    logging.error(f"❌ workflow file not found in {env_type} or base: {workflow}.yaml")
    sys.exit(1)


@contextlib.contextmanager
def _prepare_merged(origin_cfg: str, env_type: str, overlays: list[str] | None, skip_cfg_merge: bool):
    if skip_cfg_merge:
        yield origin_cfg
        return

    with tempfile.TemporaryDirectory() as tmp_cfg_dir:
        source_dirs = get_plt_cfg_source_dirs(Path(origin_cfg), env_type)
        merge_plt_cfg_dirs(
            plt_cfg_root=Path(origin_cfg),
            plt_merged_dir=Path(tmp_cfg_dir),
            plt_cfg_source_dirs=source_dirs,
            plt_overlays=overlays,
        )
        yield tmp_cfg_dir


def run_local_runner(stage_runner_script: str) -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.env_type == "prod" and args.ephemeral:
        raise RuntimeError("❌ For env-type 'prod', only --ephemeral=false is allowed")

    repo_root = Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )

    inventory_file = repo_root / f"pipeline/inventory/{args.inventory}.yaml"
    if not inventory_file.is_file():
        logging.error(f"❌ inventory file not found: {inventory_file}")
        sys.exit(1)

    workflow_file = _resolve_workflow_file(repo_root, args.env_type, args.inventory, args.workflow)
    active_stage_ids, active_stages = get_stages_from_workflow(inventory_file, workflow_file, repo_root)

    manifest = {
        "run_id": args.run_id,
        "branch": None,
        "commit": None,
        "inventory": args.inventory,
        "env_type": args.env_type,
        "overlays": args.overlays,
        "workflow": args.workflow,
        "active_stages": active_stage_ids,
        "origin_cfg": args.origin_cfg,
    }
    logging.info(json.dumps(manifest, indent=4))

    with _prepare_merged(args.origin_cfg, args.env_type, args.overlays, args.skip_cfg_merge) as source_for_resolve:
        merged_dir = "merged"
        if os.path.exists(merged_dir):
            shutil.rmtree(merged_dir)
        os.makedirs(merged_dir)
        subprocess.run(
            ["cp", "-aL", source_for_resolve + "/.", merged_dir],
            check=True,
        )

        try:
            for stage in active_stages:
                stage_id = stage.get("id")
                logging.info(f"===================== {stage_id} =====================")
                env = os.environ.copy()
                env["main_tag"] = args.main_tag
                env["env_type"] = args.env_type
                env["run_id"] = args.run_id
                env["cfg_files"] = json.dumps(stage.get("cfg_files"))
                env["STAGE_WRITE_VALUES_JSON"] = "true" if stage.get("runtime", {}).get("values_json", True) else "false"
                env["STAGE_WRITE_ENV_SH"] = "true" if stage.get("runtime", {}).get("env_sh", True) else "false"
                env["origin_cfg_base_dir_path"] = merged_dir
                subprocess.run(
                    args=[f"./pipeline/stages/{stage_id}/run/{stage_runner_script}"],
                    check=True,
                    env=env,
                )
        finally:
            if os.path.exists(merged_dir):
                shutil.rmtree(merged_dir)

    logging.info(f"✅ All stages completed, run_id: {args.run_id}")
    print(f"export run_id={args.run_id}")
