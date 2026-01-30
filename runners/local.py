#!/usr/bin/env python3
import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
import tempfile

import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def validate_uuid7(v: str) -> str:
    """Validate that a string is a valid UUID version 7."""
    try:
        parsed = uuid.UUID(v)
        if parsed.version != 7:
            raise argparse.ArgumentTypeError(f"UUID must be version 7, got version {parsed.version}: {v}")
        return v
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid UUID format: {v}")


def str2bool(v):
    """Convert string to boolean for argparse."""
    if isinstance(v, bool):
        return v
    if v == 'true':
        return True
    elif v == 'false':
        return False
    else:
        raise argparse.ArgumentTypeError(f"Only 'true' or 'false' allowed, got: {v}")


def load_yaml(path: Path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


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

        active.append(
            {
                "id": sid,
                "cfg_keys": st.get("cfg_keys", []),
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
        raise RuntimeError(
            f"workflow {workflow_file} must have 'stages'"
        )

    active_stages = _build_active_stages(inventory, active_ids, repo_root)
    return active_ids, active_stages


def merge_config_dirs(source_dirs: list[str], dest_dir: str) -> None:
    """Merge config directories in sequence. Files at same path are concatenated."""
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    
    merged_files: dict[str, list[str]] = {}  # dest_path -> list of source file paths
    
    for source_dir in source_dirs:
        for root, _, files in os.walk(source_dir):
            rel_root = os.path.relpath(root, source_dir)
            dest_root = os.path.join(dest_dir, rel_root) if rel_root != "." else dest_dir
            
            os.makedirs(dest_root, exist_ok=True)
            
            for file in files:
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_root, file)
                
                if os.path.exists(dest_file):
                    with open(src_file, 'r') as f:
                        content = f.read()
                    with open(dest_file, 'a') as f:
                        f.write('\n' + content)
                    merged_files.setdefault(dest_file, []).append(src_file)
                else:
                    shutil.copy2(src_file, dest_file)
                    merged_files[dest_file] = [src_file]
    
    for dest_path, sources in merged_files.items():
        if len(sources) > 1:
            logging.info(f"Merged {' + '.join(sources)} -> {dest_path}")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--main-tag", required=True)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--env-type", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--origin-cfg", required=True)
    parser.add_argument("--ephemeral", required=True, type=str2bool)
    parser.add_argument("--skip-cfg-merge", action="store_true")
    parser.add_argument("--run-id", required=True, type=validate_uuid7)

    args = parser.parse_args()

    main_tag = args.main_tag
    inventory = args.inventory
    env_type = args.env_type
    workflow = args.workflow
    origin_cfg = args.origin_cfg
    ephemeral = args.ephemeral
    skip_cfg_merge = args.skip_cfg_merge
    run_id = args.run_id
    base_all_plt_cfg_dir = str(Path(f"{origin_cfg}/common__all").resolve())
    base_test_staging_prod_plt_cfg_dir = str(Path(f"{origin_cfg}/common__test_staging_prod").resolve())
    plt_cfg_dir = str(Path(f"{origin_cfg}/{env_type}").resolve())

    # Validate ephemeral against env_type
    if env_type == "prod" and ephemeral:
        raise RuntimeError(
            "❌ For env-type 'prod', only --ephemeral=false is allowed"
        )

    # --- PREPARATION ---------------------------------------------------------
    repo_root = Path(
        subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
    )

    inventory_file = repo_root / f"pipeline/inventory/{inventory}.yaml"
    if not inventory_file.is_file():
        logging.error(f"❌ inventory file not found: {inventory_file}")
        sys.exit(1)

    workflow_file = None
    if workflow:
        # Try environment-specific workflow first
        workflow_file = repo_root / f"pipeline/workflows/{env_type}/{inventory}/{workflow}.yaml"
        if not workflow_file.is_file():
            # Fallback to base workflow
            base_workflow_file = repo_root / f"pipeline/workflows/base/{inventory}/{workflow}.yaml"
            if base_workflow_file.is_file():
                workflow_file = base_workflow_file
                logging.info(f"📋 Using base workflow: {base_workflow_file.relative_to(repo_root)}")
            else:
                logging.error(f"❌ workflow file not found in {env_type} or base: {workflow}.yaml")
                sys.exit(1)
        else:
            logging.info(f"📋 Using environment-specific workflow: {workflow_file.relative_to(repo_root)}")

    # --- PARSE STAGES DATA --------------------------------
    active_stage_ids, active_stages = get_stages_from_workflow(
        inventory_file, workflow_file, repo_root
    )

    manifest = {
        "run_id": run_id,
        "branch": None,
        "commit": None,
        "inventory": inventory,
        "env_type": env_type,
        "workflow": workflow,
        "active_stages": active_stage_ids,
        "origin_cfg": origin_cfg,
    }
    logging.info(json.dumps(manifest, indent=4))
    # TODO: save manifest if needed

    # --- CFG PREPARATION -----------------------------------------------------
    # Step 1: Merge (if needed)
    if skip_cfg_merge:
        source_for_resolve = origin_cfg
    else:
        tmp_cfg_dir =  tempfile.mkdtemp()
        logging.info(f"Merging cfg dirs to {tmp_cfg_dir}")
        source_dirs = (
            [plt_cfg_dir, base_test_staging_prod_plt_cfg_dir, base_all_plt_cfg_dir]
            if env_type in ("test", "staging", "prod")
            else [ plt_cfg_dir, base_all_plt_cfg_dir]
        )
        merge_config_dirs(
            source_dirs=source_dirs,
            dest_dir=tmp_cfg_dir
        )
        source_for_resolve = tmp_cfg_dir

    # Step 2: Resolve symlinks
    effective_cfg_dir = "effective_cfg"
    if os.path.exists(effective_cfg_dir):
        shutil.rmtree(effective_cfg_dir)
    os.makedirs(effective_cfg_dir)
    subprocess.run(
        ["cp", "-aL", source_for_resolve + "/.", effective_cfg_dir],
        check=True,
    )

    # --- STAGES EXECUTION ----------------------------------------------------
    try:
        for stage in active_stages:
            stage_id = stage.get("id")
            logging.info(
                f"===================== {stage_id} ====================="
            )
            env = os.environ.copy()
            env["main_tag"] = main_tag
            env["env_type"] = env_type
            env["run_id"] = run_id
            env["cfg_keys"] = json.dumps(stage.get("cfg_keys"))
            env["origin_cfg_base_dir_path"] = effective_cfg_dir

            subprocess.run(
                args=[f"./pipeline/stages/{stage_id}/run/local.sh"],
                check=True,
                env=env,
            )
    finally:
        pass
        if os.path.exists(effective_cfg_dir):
            shutil.rmtree(effective_cfg_dir)

    logging.info(f"✅ All stages completed, run_id: {run_id}")
    print(f"export run_id={run_id}")


if __name__ == "__main__":
    main()
