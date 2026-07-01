#!/usr/bin/env python3
import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from utils.common import load_yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ADAPTER_DIR = "atlas_ctl_adapter"
RUNTIME_CONTEXT_FILENAME = "runtime_context.yaml"
RUNTIME_CONTEXT_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _runtime_context_env_value(value, *, key: str) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    raise RuntimeError(f"runtime context key {key!r} must be scalar, got {type(value).__name__}")


def load_runtime_context(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise RuntimeError(f"runtime context file not found: {path}")
    data = load_yaml(path) or {}
    runtime_context: dict[str, object] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not RUNTIME_CONTEXT_ENV_KEY_RE.fullmatch(key):
            raise RuntimeError(f"runtime context key {key!r} is not a valid env var name")
        runtime_context[key] = value
        _runtime_context_env_value(value, key=key)
    return runtime_context


def runtime_context_to_env(runtime_context: dict[str, object]) -> dict[str, str]:
    return {
        key: _runtime_context_env_value(value, key=key)
        for key, value in runtime_context.items()
    }


def _build_active_stages(action_manifest, active_ids, repo_root: Path):
    active = []
    for sid in active_ids:
        entry = action_manifest.get(sid)
        if not isinstance(entry, dict):
            raise RuntimeError(f"Stage '{sid}' not declared in manifest")
        stage_path = entry.get("path")
        if not isinstance(stage_path, str) or not stage_path:
            raise RuntimeError(f"Stage '{sid}' manifest entry must define a non-empty 'path'")

        stage_meta_path = repo_root / stage_path / "stage.yaml"
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
                "path": stage_path,
                "cfg_files": st.get("cfg_files", []),
                "runtime": {
                    "values_json": values_json,
                    "env_sh": env_sh,
                },
                "env_vars": {
                    "inventory": {},
                    "stage": st.get("env_vars", {}),
                },
            }
        )
    return active


def get_stages_from_workflow(manifest_file: Path, workflows_file: Path, action: str, workflow_name: str, repo_root: Path):
    manifest = (load_yaml(manifest_file) or {}).get("manifest", {})
    workflows = (load_yaml(workflows_file) or {}).get("workflows", {})

    action_manifest = manifest.get(action)
    if not isinstance(action_manifest, dict) or not action_manifest:
        raise RuntimeError(f"manifest {manifest_file} declares no stages for action '{action}'")

    action_workflows = workflows.get(action)
    if not isinstance(action_workflows, dict) or workflow_name not in action_workflows:
        raise RuntimeError(f"workflow '{action}/{workflow_name}' not found in {workflows_file}")
    wf = action_workflows[workflow_name]
    if not isinstance(wf, dict) or "stages" not in wf:
        raise RuntimeError(f"workflow '{action}/{workflow_name}' must have 'stages'")

    active_ids = []
    for sid in wf.get("stages", []):
        if sid not in action_manifest:
            raise RuntimeError(f"Stage '{sid}' not declared in manifest for action '{action}'")
        active_ids.append(sid)

    active_stages = _build_active_stages(action_manifest, active_ids, repo_root)
    return active_ids, active_stages


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--origin-cfg", required=True)
    parser.add_argument("--runtime-context-file", required=True)
    return parser


def run_local_runner(stage_runner_script: str) -> None:
    parser = _build_parser()
    args = parser.parse_args()

    repo_root = Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )

    runtime_context_file = Path(args.runtime_context_file).expanduser().resolve()
    runtime_context = load_runtime_context(runtime_context_file)
    runtime_env = runtime_context_to_env(runtime_context)
    run_id = runtime_env.get("run_id", "unknown")

    adapter_runtime_context_file = repo_root / RUNTIME_CONTEXT_FILENAME
    copied_runtime_context = False
    if runtime_context_file != adapter_runtime_context_file.resolve():
        shutil.copy2(runtime_context_file, adapter_runtime_context_file)
        copied_runtime_context = True

    origin_cfg_path = Path(args.origin_cfg).expanduser().resolve()
    if not origin_cfg_path.is_dir():
        raise RuntimeError(f"origin cfg dir not found: {origin_cfg_path}")

    manifest_file = repo_root / ADAPTER_DIR / "manifest.yaml"
    if not manifest_file.is_file():
        logging.error(f"❌ manifest file not found: {manifest_file}")
        sys.exit(1)
    workflows_file = repo_root / ADAPTER_DIR / "workflows.yaml"
    if not workflows_file.is_file():
        logging.error(f"❌ workflows file not found: {workflows_file}")
        sys.exit(1)

    active_stage_ids, active_stages = get_stages_from_workflow(
        manifest_file, workflows_file, args.action, args.workflow, repo_root
    )

    run_manifest = {
        "run_id": run_id,
        "branch": None,
        "commit": None,
        "action": args.action,
        "workflow": args.workflow,
        "active_stages": active_stage_ids,
        "origin_cfg": str(origin_cfg_path),
        "runtime_context_file": str(runtime_context_file),
        "runtime_context_keys": sorted(runtime_context),
    }
    logging.info(json.dumps(run_manifest, indent=4))

    try:
        for stage in active_stages:
            stage_id = stage.get("id")
            stage_path = stage.get("path")
            logging.info(f"===================== {stage_id} =====================")
            env = os.environ.copy()
            env.update(runtime_env)
            env["ATLAS_RUNTIME_CONTEXT_FILE"] = RUNTIME_CONTEXT_FILENAME
            env["cfg_files"] = json.dumps(stage.get("cfg_files"))
            env["STAGE_WRITE_VALUES_JSON"] = "true" if stage.get("runtime", {}).get("values_json", True) else "false"
            env["STAGE_WRITE_ENV_SH"] = "true" if stage.get("runtime", {}).get("env_sh", True) else "false"
            env["origin_cfg_base_dir_path"] = str(origin_cfg_path)
            subprocess.run(
                args=[f"./{stage_path}/run/{stage_runner_script}"],
                check=True,
                env=env,
            )
    finally:
        if copied_runtime_context and adapter_runtime_context_file.is_file():
            adapter_runtime_context_file.unlink()

    logging.info(f"✅ All stages completed, run_id: {run_id}")
    print(f"export run_id={run_id}")
