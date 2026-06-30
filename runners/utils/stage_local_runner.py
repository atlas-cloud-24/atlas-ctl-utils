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
    load_yaml,
    merge_plt_cfg_dirs,
    parse_overlays_arg,
    str2bool,
    validate_uuid7,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ADAPTER_DIR = "atlas_ctl_adapter"


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
    parser.add_argument("--main-tag", required=True)
    parser.add_argument("--action", required=True)
    parser.add_argument("--env-type", required=True)
    parser.add_argument(
        "--overlays",
        required=False,
        default=[],
        dest="overlays",
        type=parse_overlays_arg,
        help="Optional comma-separated plt overlay names.",
    )
    parser.add_argument("--workflow", required=True)
    parser.add_argument(
        "--cfg-root",
        default="/env",
        help="Scoped cfg root to use when merging origin cfg locally. Use /env, /org, /deployments, or /.",
    )
    parser.add_argument("--origin-cfg", required=True)
    parser.add_argument("--ephemeral", required=True, type=str2bool)
    parser.add_argument(
        "--skip-cfg-merge",
        action="store_true",
        help="Use cfg that was already merged by an upstream step.",
    )
    parser.add_argument("--run-id", required=True, type=validate_uuid7)
    return parser


def _normalize_cfg_root(raw_value: str) -> str:
    value = raw_value if raw_value is not None else "/env"
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("cfg-root must be a non-empty string")

    value = value.strip()
    if "\\" in value:
        raise RuntimeError(f"cfg-root must use forward slashes: {value}")
    if not value.startswith("/"):
        raise RuntimeError(f"cfg-root must start with /: {value}")

    parts = [part for part in value.split("/") if part]
    if any(part in (".", "..") for part in parts):
        raise RuntimeError(f"cfg-root must not contain . or ..: {value}")

    return "/" + "/".join(parts) if parts else "/"


def _resolve_cfg_root(merged_root: Path, cfg_root: str) -> Path:
    normalized = _normalize_cfg_root(cfg_root)
    rel = normalized.lstrip("/")
    scoped = (merged_root / rel).resolve() if rel else merged_root.resolve()
    try:
        scoped.relative_to(merged_root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"cfg-root escapes merged cfg: {normalized}") from exc
    if not scoped.is_dir():
        raise RuntimeError(f"cfg-root not found in merged cfg: {normalized} ({scoped})")
    return scoped


@contextlib.contextmanager
def _prepare_merged(origin_cfg: str, env_type: str, overlays: list[str] | None, skip_cfg_merge: bool, cfg_root: str):
    if skip_cfg_merge:
        yield origin_cfg
        return

    with tempfile.TemporaryDirectory() as tmp_cfg_dir:
        merged_root = Path(tmp_cfg_dir)
        merge_plt_cfg_dirs(
            plt_cfg_root=Path(origin_cfg),
            plt_merged_dir=merged_root,
            ctl_context=env_type,
            plt_env=env_type,
            plt_overlays=overlays,
        )
        yield str(_resolve_cfg_root(merged_root, cfg_root))


def run_local_runner(stage_runner_script: str) -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.env_type == "prod" and args.ephemeral:
        raise RuntimeError("❌ For env-type 'prod', only --ephemeral=false is allowed")

    repo_root = Path(
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    )

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
        "run_id": args.run_id,
        "branch": None,
        "commit": None,
        "action": args.action,
        "env_type": args.env_type,
        "overlays": args.overlays,
        "workflow": args.workflow,
        "active_stages": active_stage_ids,
        "origin_cfg": args.origin_cfg,
        "cfg_root": args.cfg_root,
    }
    logging.info(json.dumps(run_manifest, indent=4))

    with _prepare_merged(args.origin_cfg, args.env_type, args.overlays, args.skip_cfg_merge, args.cfg_root) as source_for_resolve:
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
                stage_path = stage.get("path")
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
                    args=[f"./{stage_path}/run/{stage_runner_script}"],
                    check=True,
                    env=env,
                )
        finally:
            if os.path.exists(merged_dir):
                shutil.rmtree(merged_dir)

    logging.info(f"✅ All stages completed, run_id: {args.run_id}")
    print(f"export run_id={args.run_id}")
