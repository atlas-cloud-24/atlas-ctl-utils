"""Shared utilities for local and local_dev runners."""

import argparse
import logging
import logging.handlers
import os
import re
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from utils.git_meta import write_git_meta_to_file

# Environment trust hierarchy: higher value = higher trust/security requirements
# Control plane must have equal or higher trust than target platform
ENV_TRUST = {"dev": 0, "test": 1, "staging": 2, "prod": 3}

# Environment groupings
ENVS_ALL = tuple(ENV_TRUST.keys())
ENVS_DEV_TEST = ("dev", "test")
ENVS_STAGING_PROD = ("staging", "prod")

SERVICE_ID = "atlas-ctl-orchestartor-local"

# ANSI escape code pattern
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')


@dataclass
class RunContext:
    """Context object holding all paths and config for a pipeline run."""
    run_id: str
    inventory_name: str
    workflow_name: str
    ctl_env: str
    plt_env: str
    ephemeral: bool
    run_dir: Path
    artifacts_dir: Path
    plt_merged_cfg_dir: Path
    log_file: Path
    ctl_cfg_root: Path
    plt_cfg_root: Path
    workflow_cfg: dict
    inventory_cfg: dict
    active_stages: dict
    pipeline_run_cfg_path: Path
    plt_destination_cfg_dir_path: Path


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


def bool2str(v: bool) -> str:
    """Convert boolean to 'true'/'false' string."""
    if isinstance(v, bool):
        return "true" if v else "false"
    raise argparse.ArgumentTypeError(f"Expected bool, got: {type(v).__name__} ({v!r})")


def validate_uuid7(v: str) -> str:
    """Validate that a string is a valid UUID version 7."""
    try:
        parsed = uuid.UUID(v)
        if parsed.version != 7:
            raise argparse.ArgumentTypeError(f"UUID must be version 7, got version {parsed.version}: {v}")
        return v
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid UUID format: {v}")


def load_yaml(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def format_path_for_log(path: str | Path, relative_roots: tuple[Path, ...] = ()) -> str:
    """Prefer a relative display path when the path is under a known root."""
    path_obj = Path(path).expanduser()
    if not path_obj.is_absolute():
        return str(path_obj)

    for root in relative_roots:
        try:
            return str(path_obj.relative_to(root))
        except ValueError:
            continue

    return str(path_obj)


def strip_ansi(text: str) -> str:
    """Remove ANSI color codes from text."""
    return ANSI_ESCAPE.sub('', text)


def log_stage_banner(stage_id: str, *, ch: str = "#", min_width: int = 100) -> None:
    title = f" {stage_id} "
    width = max(min_width, len(title) + 2)  # ensure it always fits
    line = ch * width
    mid  = title.center(width, ch)
    logging.info(line)
    logging.info(mid)
    logging.info(line)


def run_and_log(cmd, shell=False, cwd=None, env=None, check=True):
    """Run subprocess and log all output in real-time."""
    process = subprocess.Popen(
        cmd,
        shell=shell,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1  # Line buffered
    )

    # Stream output in real-time
    for line in process.stdout:
        line_stripped = line.rstrip()
        # Print colored output to terminal
        print(f"  {line_stripped}", flush=True)
        # Log clean output to file (strip ANSI codes)
        clean_line = strip_ansi(line_stripped)
        # Only log to file handlers, not console
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                handler.emit(logging.LogRecord(
                    name=logging.getLogger().name,
                    level=logging.INFO,
                    pathname="",
                    lineno=0,
                    msg=f"  {clean_line}",
                    args=(),
                    exc_info=None
                ))

    # Wait for process to complete
    returncode = process.wait()

    if check and returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)

    return returncode


def validate_env_compatibility(ctl_env: str, plt_env: str) -> None:
    """
    Validate that control plane environment can deploy to target platform environment.

    Security principle: A lower-trust environment should never have the credentials
    or capability to modify a higher-trust environment.

    Raises:
        RuntimeError: If ctl_env has lower trust than plt_env
    """
    if ctl_env not in ENV_TRUST:
        raise RuntimeError(
            f"❌ Unknown ctl_env: '{ctl_env}'. Must be one of: {list(ENV_TRUST.keys())}"
        )
    if plt_env not in ENV_TRUST:
        raise RuntimeError(
            f"❌ Unknown plt_env: '{plt_env}'. Must be one of: {list(ENV_TRUST.keys())}"
        )

    ctl_trust = ENV_TRUST[ctl_env]
    plt_trust = ENV_TRUST[plt_env]

    if ctl_trust < plt_trust:
        allowed_targets = [env for env, trust in ENV_TRUST.items() if trust <= ctl_trust]
        raise RuntimeError(
            f"❌ Security violation: Cannot deploy to '{plt_env}' from '{ctl_env}' control plane.\n"
            f"   Control environment (trust={ctl_trust}) must have equal or higher "
            f"trust level than target platform (trust={plt_trust}).\n"
            f"   Allowed plt_env values for ctl_env='{ctl_env}': {allowed_targets}"
        )


def validate_ephemeral(ctl_env: str, ephemeral: bool) -> None:
    """Validate ephemeral flag against ctl_env."""
    if ctl_env in ENVS_STAGING_PROD and ephemeral:
        raise RuntimeError(
            f"❌ For env-type in {ENVS_STAGING_PROD}, only --ephemeral=false is allowed"
        )


def validate_stages_have_commits(workflow_cfg: dict, ctl_env: str) -> None:
    """
    Validate that all stages have explicit commits for prod environments.

    For prod environments, using branch references is not allowed - all stages must
    be pinned to specific commits for reproducibility and auditability.
    """
    if ctl_env not in ENVS_STAGING_PROD:
        return

    stages_without_commit = []
    for st in workflow_cfg.get("stages", []):
        if isinstance(st, str):
            stages_without_commit.append(st)
        else:
            stage_id = st.get("id", "unknown")
            if not st.get("commit"):
                stages_without_commit.append(stage_id)

    if stages_without_commit:
        raise RuntimeError(
            f"❌ For {ENVS_STAGING_PROD} environments, all stages must have explicit 'commit' specified.\n"
            f"   Stages missing 'commit': {stages_without_commit}\n"
            f"   Using branch references is not allowed in {ENVS_STAGING_PROD} for reproducibility."
        )


def validate_cfg_refs_have_commits(
    ctl_env: str,
    ctl_cfg_branch: str | None,
    ctl_cfg_commit: str | None,
    plt_cfg_branch: str | None,
    plt_cfg_commit: str | None,
) -> None:
    """
    Validate that cfg repos use commits (not branches) for staging/prod environments.
    """
    if ctl_env not in ENVS_STAGING_PROD:
        return

    errors = []
    if ctl_cfg_branch and not ctl_cfg_commit:
        errors.append(f"--ctl-cfg uses branch='{ctl_cfg_branch}' but commit is required")
    if plt_cfg_branch and not plt_cfg_commit:
        errors.append(f"--plt-cfg uses branch='{plt_cfg_branch}' but commit is required")

    if errors:
        raise RuntimeError(
            f"❌ For {ENVS_STAGING_PROD} environments, cfg repos must use @commit=sha (not @branch=name).\n"
            f"   {'; '.join(errors)}"
        )


def git_clone(repo_url: str, branch: str | None, commit: str | None, dest: Path, token: str | None = None):
    env = os.environ.copy()
    url = repo_url
    if token:
        env["GIT_ASKPASS"] = "echo"
        env["github_token"] = token
        url = url.replace(
            "https://github.com/",
            "https://x-access-token:${github_token}@github.com/",
        )

    # commit pinned → checkout exact commit
    if commit:
        cmd = f"git clone {url} {dest}"
        logging.info(f"Running command: {cmd}")
        run_and_log(cmd, shell=True, env=env)

        cmd = f"git checkout {commit}"
        logging.info(f"Running command: {cmd}")
        run_and_log(cmd.split(), cwd=dest, env=env)
        return

    # no commit → use branch HEAD
    if not branch:
        raise RuntimeError(f"❌ Either branch or commit must be provided for repo {repo_url}")

    cmd = f"git clone --branch {branch} --depth 1 {url} {dest}"
    logging.info(f"Running command: {cmd}")
    run_and_log(cmd, shell=True, env=env)


def parse_repo_url_ref(value: str) -> tuple[str, str | None, str | None]:
    """
    Parse URL@branch=name or URL@commit=sha format into (url, branch, commit).

    Examples:
        https://github.com/org/repo@branch=main -> (url, "main", None)
        https://github.com/org/repo@commit=abc123 -> (url, None, "abc123")

    Returns:
        tuple: (repo_url, branch, commit) where one of branch/commit is None
    """
    if '@' not in value:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Expected URL@branch=name or URL@commit=sha"
        )

    # Split on last @ to handle URLs that might contain @
    idx = value.rfind('@')
    repo_url = value[:idx]
    ref_part = value[idx + 1:]

    if not repo_url or not ref_part:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Both URL and ref are required."
        )

    if ref_part.startswith("branch="):
        branch = ref_part[7:]  # len("branch=") = 7
        if not branch:
            raise argparse.ArgumentTypeError(f"Invalid format: '{value}'. Branch name cannot be empty.")
        return repo_url, branch, None
    elif ref_part.startswith("commit="):
        commit = ref_part[7:]  # len("commit=") = 7
        if not commit:
            raise argparse.ArgumentTypeError(f"Invalid format: '{value}'. Commit sha cannot be empty.")
        return repo_url, None, commit
    else:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Expected @branch=name or @commit=sha"
        )


def parse_variants_arg(value: str) -> list[str]:
    """Parse comma-separated variant paths under variants/."""
    if value is None:
        return []

    raw = [v.strip() for v in value.split(",") if v.strip()]
    if not raw:
        return []
    if len(raw) == 1 and raw[0].lower() in ("none", "null", "-"):
        return []

    for item in raw:
        path = Path(item)
        if path.is_absolute():
            raise argparse.ArgumentTypeError(
                f"Variant path must be relative to variants/: {item}"
            )
        if ".." in path.parts:
            raise argparse.ArgumentTypeError(
                f"Variant path must not contain '..': {item}"
            )

    return raw


def build_active_stages(
    workflow_cfg: dict,
    inventory_cfg: dict,
    repo_key: str = "repo_url",
    require_branch_or_commit: bool = True,
) -> dict:
    inventory_stages = inventory_cfg.get("stages", {})
    if not isinstance(inventory_stages, dict):
        raise RuntimeError("'stages' in inventory must be a mapping: source -> meta")

    active = {}

    for st in workflow_cfg.get("stages", []):
        if isinstance(st, str):
            stage_id = st
            stage_source = st
            stage_over = {}
        else:
            stage_id = st.get("id")
            if not stage_id:
                raise RuntimeError("Stage entry missing required field 'id'")
            stage_source = st.get("source")
            if not stage_source:
                raise RuntimeError(f"Stage '{stage_id}' has empty 'source'")
            stage_over = st

        if stage_source not in inventory_stages:
            raise RuntimeError(
                f"Stage source '{stage_source}' (stage id='{stage_id}') not found in inventory '{workflow_cfg.get('inventory')}'"
            )

        cat = inventory_stages[stage_source]

        branch = stage_over.get("branch")
        commit = stage_over.get("commit")
        child_workflow = stage_over.get("workflow")

        if require_branch_or_commit and not branch and not commit:
            raise RuntimeError(f"Stage '{stage_id}' has neither branch nor commit configured")

        repo_value = cat.get(repo_key)
        if not repo_value:
            raise RuntimeError(
                f"Stage '{stage_id}' (source='{stage_source}') missing '{repo_key}' in inventory '{workflow_cfg.get('inventory')}'"
            )

        active[stage_id] = {
            "source": stage_source,
            "branch": branch,
            "commit": commit,
            "workflow": child_workflow,
            "cfg_keys": cat.get("cfg_keys", []),
        }

        if repo_key == "repo_path":
            repo_path = Path(repo_value).expanduser()
            if not repo_path.is_absolute():
                raise RuntimeError(
                    f"Stage '{stage_id}' repo_path must be absolute, got: {repo_value}"
                )
            active[stage_id]["repo_path"] = str(repo_path.resolve())
        else:
            active[stage_id]["repo_url"] = repo_value
            active[stage_id]["token_type"] = cat.get("token_type")

    return active


def merge_config_dirs(
    source_dirs: list[str],
    dest_dir: str,
    clear_dest: bool = True,
    *,
    source_log_roots: tuple[Path, ...] = (),
    dest_log_roots: tuple[Path, ...] = (),
) -> None:
    """Merge config directories in sequence. Files at same path are concatenated."""
    if clear_dest and os.path.exists(dest_dir):
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
            rendered_sources = [format_path_for_log(src, source_log_roots) for src in sources]
            rendered_dest = format_path_for_log(dest_path, dest_log_roots)
            logging.info("Merged:")
            logging.info("  %s", rendered_sources[0])
            for src in rendered_sources[1:]:
                logging.info("  + %s", src)
            logging.info("  = %s", rendered_dest)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add arguments common to both local and local_dev runners."""
    parser.add_argument(
        "--main-tag",
        required=True,
        help="Main tag passed to stage runners",
    )
    parser.add_argument(
        "--plt-variants",
        required=True,
        type=parse_variants_arg,
        help="Comma-separated variant paths under variants/",
    )
    parser.add_argument(
        "--ctl-env",
        required=True,
        choices=list(ENV_TRUST.keys()),
        help="Ctl environment type (e.g. dev|test|staging|prod)",
    )
    parser.add_argument(
        "--inventory",
        required=True,
        help="inventory name (e.g. create|destroy)",
    )
    parser.add_argument(
        "--workflow",
        required=True,
        help="workflow name (e.g. default|core|test)",
    )
    parser.add_argument(
        "--ephemeral",
        required=True,
        type=str2bool
    )
    parser.add_argument(
        "--plt-env",
        required=True,
        choices=list(ENV_TRUST.keys()),
        help="Plt environment type (e.g. dev|test|staging|prod)",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        type=validate_uuid7,
        help="Run id (must be a valid UUID format)",
    )


def setup_logging() -> logging.handlers.MemoryHandler:
    """Setup logging with memory handler to capture early logs."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    memory_handler = logging.handlers.MemoryHandler(capacity=1000, flushLevel=logging.CRITICAL)
    logging.getLogger().addHandler(memory_handler)
    logging.info(f"Command: {' '.join(sys.argv)}")
    return memory_handler


def build_target_stage_workflow_cfg(ctl_env: str, inventory_name: str, workflow_name: str) -> dict | None:
    """
    Build an in-memory single-stage workflow config from <stage-source>/<stage-workflow>.

    This is used by local_dev.py, which supports targeting one local stage directly
    without requiring a dedicated workflow yaml file in ctl-cfg.
    """
    if "/" not in workflow_name:
        return None

    stage_source, child_workflow = workflow_name.rsplit("/", 1)
    if not stage_source or not child_workflow:
        return None

    return {
        "meta": {
            "name": f"{ctl_env}/{inventory_name}/{workflow_name}",
            "env_type": ctl_env,
            "inventory": inventory_name,
        },
        "stages": [
            {
                "id": workflow_name,
                "source": stage_source,
                "workflow": child_workflow,
            }
        ],
    }


def load_workflow_cfg(
    ctl_cfg_root: Path,
    ctl_env: str,
    inventory_name: str,
    workflow_name: str,
    allow_target_stage_workflow: bool = False,
) -> dict:
    """Load workflow configuration from yaml file or synthesize a local target-stage workflow."""
    workflow_path = (
        ctl_cfg_root
        / "workflows"
        / ctl_env
        / inventory_name
        / f"{workflow_name}.yaml"
    )
    if workflow_path.is_file():
        return load_yaml(workflow_path)

    if allow_target_stage_workflow:
        workflow_cfg = build_target_stage_workflow_cfg(ctl_env, inventory_name, workflow_name)
        if workflow_cfg is not None:
            logging.info(
                "Workflow file not found, using synthesized single-stage workflow for local dev: %s",
                workflow_name,
            )
            return workflow_cfg

    raise RuntimeError(f"❌ workflow file not found: {workflow_path}")


def load_inventory_cfg(ctl_cfg_root: Path, inventory_name: str) -> dict:
    """Load inventory configuration from yaml file."""
    inventory_path = (
        ctl_cfg_root
        / "inventory"
        / f"{inventory_name}.yaml"
    )
    if not inventory_path.is_file():
        raise RuntimeError(f"❌ inventory file not found: {inventory_path}")
    return load_yaml(inventory_path)


def setup_run_dirs(run_id: str, inventory_name: str, memory_handler: logging.handlers.MemoryHandler) -> tuple[Path, Path, Path, Path]:
    """
    Create run directories and setup file logging.

    Returns:
        tuple: (run_dir, artifacts_dir, plt_merged_cfg_dir, log_file)
    """
    # create run_dir
    run_dir = Path("/tmp") / run_id / inventory_name
    run_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Using run_dir: {run_dir}")

    # create artifacts_dir
    artifacts_dir = run_dir / "artifacts"
    os.makedirs(artifacts_dir, exist_ok=True)

    # create cfg root
    cfg_dir = run_dir / "cfg"
    if cfg_dir.exists():
        shutil.rmtree(cfg_dir)
    os.makedirs(cfg_dir)

    # clear stages source
    stages_source_dir = run_dir / "stages_source"
    if stages_source_dir.exists():
        shutil.rmtree(stages_source_dir)

    # create merged cfg dir
    plt_merged_cfg_dir = cfg_dir / "plt_merged_cfg"
    os.makedirs(plt_merged_cfg_dir)

    # Setup file logging and flush buffered early logs
    logs_dir = artifacts_dir / "logs"
    os.makedirs(logs_dir, exist_ok=True)
    logs_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "_" + uuid.uuid4().hex[:6]
    log_file = logs_dir / f"{SERVICE_ID}_{logs_run_id}.log"
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(file_handler)

    # Flush early logs from memory to file
    memory_handler.setTarget(file_handler)
    memory_handler.flush()
    logging.getLogger().removeHandler(memory_handler)

    logging.info(f"Using artifacts_dir: {artifacts_dir}")
    logging.info(f"Logging to: {log_file}")

    return run_dir, artifacts_dir, plt_merged_cfg_dir, log_file


def get_plt_cfg_source_dirs(plt_cfg_root: Path, plt_env: str) -> list[str]:
    """Get list of plt config source directories based on environment."""
    env_root = (plt_cfg_root / "env").resolve()
    env_specific = (env_root / plt_env).resolve()
    layers_cfg_path = env_specific / "layers.yaml"

    if not env_specific.is_dir():
        raise RuntimeError(f"Platform env dir not found: {env_specific}")

    if not layers_cfg_path.is_file():
        raise RuntimeError(f"Missing required layers.yaml for plt env '{plt_env}': {layers_cfg_path}")

    cfg = load_yaml(layers_cfg_path) or {}
    if not isinstance(cfg, dict):
        raise RuntimeError(f"layers.yaml must contain a mapping: {layers_cfg_path}")

    layers = cfg.get("layers")
    if not isinstance(layers, list):
        raise RuntimeError(f"layers.yaml must contain a 'layers' list: {layers_cfg_path}")

    source_dirs = [str(env_specific)]
    seen: set[Path] = {env_specific}
    for layer in layers:
        if not isinstance(layer, str) or not layer.strip():
            raise RuntimeError(f"layers.yaml entries must be non-empty strings: {layers_cfg_path}")

        src = (env_root / layer).resolve()
        try:
            src.relative_to(env_root)
        except ValueError as exc:
            raise RuntimeError(f"Layer path escapes env/: {layer}") from exc

        if src in seen:
            raise RuntimeError(f"Duplicate layer path in {layers_cfg_path}: {layer}")
        if not src.exists():
            raise RuntimeError(f"Layer path not found: {src}")
        if not src.is_dir():
            raise RuntimeError(f"Layer path must be a directory: {src}")

        seen.add(src)
        source_dirs.append(str(src))

    logging.info(f"Using layers.yaml for plt env '{plt_env}': {source_dirs}")
    return source_dirs


def get_variant_source_dirs(plt_cfg_root: Path, plt_variants: list[str]) -> list[str]:
    """Return absolute variant directories selected by --plt-variants."""
    if not plt_variants:
        return []

    variants_root = plt_cfg_root / "variants"
    if not variants_root.is_dir():
        raise RuntimeError(f"Variants dir not found: {variants_root}")

    variants_root = variants_root.resolve()
    source_dirs: list[str] = []
    seen: set[str] = set()
    for rel in plt_variants:
        if rel in seen:
            continue
        seen.add(rel)

        rel_path = Path(rel)
        src = (variants_root / rel_path).resolve()
        try:
            src.relative_to(variants_root)
        except ValueError as exc:
            raise RuntimeError(f"Variant path escapes variants/: {rel}") from exc
        if not src.exists():
            raise RuntimeError(f"Variant path not found: {src}")
        if not src.is_dir():
            raise RuntimeError(f"Variant path must be a directory: {src}")

        source_dirs.append(str(src))

    return source_dirs


def prepare_pipeline_cfg(
    plt_cfg_root: Path,
    workflow_cfg: dict,
    inventory_cfg: dict,
    plt_merged_cfg_dir: Path,
    plt_cfg_source_dirs: list[str],
    artifacts_dir: Path,
    plt_variants: list[str],
    stage_repo_key: str = "repo_url",
    require_stage_ref: bool = True,
) -> tuple[dict, Path]:
    """
    Merge config dirs, build active stages, and write pipeline_run_cfg.

    Returns:
        tuple: (active_stages, pipeline_run_cfg_path)
    """
    os.makedirs(plt_merged_cfg_dir, exist_ok=True)
    source_log_roots = (plt_cfg_root.resolve(),)
    dest_log_roots = (plt_merged_cfg_dir.parent.parent.resolve(),)

    # merge selected variants into merged cfg root (lowest precedence)
    variant_dirs = get_variant_source_dirs(plt_cfg_root, plt_variants)
    if variant_dirs:
        logging.info(f"Merging variant dirs to {plt_merged_cfg_dir}: {variant_dirs}")
        merge_config_dirs(
            source_dirs=variant_dirs,
            dest_dir=str(plt_merged_cfg_dir),
            clear_dest=True,
            source_log_roots=source_log_roots,
            dest_log_roots=dest_log_roots,
        )

    # merge env cfg into merged cfg root (higher precedence)
    logging.info(f"Merging env cfg dirs to {plt_merged_cfg_dir}")
    merge_config_dirs(
        source_dirs=plt_cfg_source_dirs,
        dest_dir=str(plt_merged_cfg_dir),
        clear_dest=not variant_dirs,
        source_log_roots=source_log_roots,
        dest_log_roots=dest_log_roots,
    )

    # get active stages
    active_stages = build_active_stages(
        workflow_cfg,
        inventory_cfg,
        repo_key=stage_repo_key,
        require_branch_or_commit=require_stage_ref,
    )

    # create and write pipeline_run_cfg
    pipeline_run_cfg = {
        "meta": workflow_cfg.get("meta"),
        "stages": active_stages
    }
    pipeline_run_cfg_path = artifacts_dir / "pipeline_run_cfg.yaml"
    with pipeline_run_cfg_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(pipeline_run_cfg, f, sort_keys=False)

    return active_stages, pipeline_run_cfg_path


def write_git_metas(ctl_cfg_root: Path, plt_cfg_root: Path, artifacts_dir: Path) -> None:
    """Write all git meta files to artifacts directory."""
    # ctl_cfg_git_meta
    write_git_meta_to_file(
        git_dir=ctl_cfg_root,
        dest_dir=artifacts_dir,
        filename="piepeline_orchestartor_cfg_git_meta.yaml",
        generator=SERVICE_ID
    )

    # orchestartor_git_meta
    write_git_meta_to_file(
        git_dir=os.getcwd(),
        dest_dir=artifacts_dir,
        filename="piepeline_orchestartor_git_meta.yaml",
        generator=SERVICE_ID
    )

    # plt_cfg_git_meta
    write_git_meta_to_file(
        git_dir=plt_cfg_root,
        dest_dir=artifacts_dir,
        filename="plt_cfg_git_meta.yaml",
        generator=SERVICE_ID
    )


def run_cfg_distribution(pipeline_run_cfg_path: Path, plt_merged_cfg_dir: Path, run_dir: Path) -> Path:
    """Run cfg distribution script and return destination cfg dir path."""
    plt_destination_cfg_dir_path = run_dir / "cfg" / "plt_distributed_cfg"
    env = os.environ.copy()
    env["pipeline_run_cfg_path"] = str(pipeline_run_cfg_path)
    env["plt_origin_cfg_dir_path"] = str(plt_merged_cfg_dir)
    env["plt_destination_cfg_dir_path"] = str(plt_destination_cfg_dir_path)
    logging.info(f"Running: {os.getcwd()}/stages/prepare/cfg/run/local.sh")
    run_and_log(
        [f"{os.getcwd()}/stages/prepare/cfg/run/local.sh"],
        env=env,
    )
    return plt_destination_cfg_dir_path


def run_stages(
    active_stages: dict,
    run_dir: Path,
    plt_destination_cfg_dir_path: Path,
    inventory_name: str,
    plt_env: str,
    ephemeral: bool,
    run_id: str,
    main_tag: str,
) -> None:
    """Clone and run all active stages."""
    os.chdir(run_dir)
    for stage_id, stage in active_stages.items():
        log_stage_banner(stage_id)
        repo_path = run_dir / "stages_source" / stage_id
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path)
        if "repo_path" in stage:
            repo_path_value = stage["repo_path"]
            if not repo_path_value:
                raise RuntimeError(f"Stage '{stage_id}' has empty repo_path")
            repo_src = Path(repo_path_value).expanduser()
            if not repo_src.is_dir():
                raise RuntimeError(f"Stage '{stage_id}' repo_path not found: {repo_src}")
            shutil.copytree(repo_src, repo_path, symlinks=True)
        else:
            git_clone(
                repo_url=stage["repo_url"],
                branch=stage["branch"],
                commit=stage["commit"],
                dest=repo_path,
                token=os.getenv(stage["token_type"])
            )
        # setup stage
        stage_setup_cmd = [
            "./pipeline/setup.sh"
        ]
        logging.info(" ".join(stage_setup_cmd))
        run_and_log(
            stage_setup_cmd,
            cwd=repo_path,
        )

        # run stage
        stage_run_cmd = [
            "./pipeline/runners/local.py",
            "--main-tag", main_tag,
            "--inventory", inventory_name,
            "--env-type", plt_env,
            "--workflow", stage["workflow"],
            "--origin-cfg", f"{plt_destination_cfg_dir_path}/{stage_id}",
            "--ephemeral", bool2str(ephemeral),
            "--skip-cfg-merge",
            "--run-id", run_id,
        ]
        logging.info(" ".join(stage_run_cmd))
        run_and_log(
            stage_run_cmd,
            cwd=repo_path,
        )


def print_run_summary(run_id: str, log_file: Path) -> None:
    """Print run summary at the end."""
    print(f"export run_id={run_id}")
    print(f"Log file: {log_file}")


def run_pipeline(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    ctl_env: str,
    plt_env: str,
    inventory_name: str,
    workflow_name: str,
    ephemeral: bool,
    run_id: str,
    main_tag: str,
    plt_variants: list[str],
    stage_repo_key: str,
    require_stage_ref: bool,
    allow_target_stage_workflow: bool,
    run_dir: Path,
    artifacts_dir: Path,
    plt_merged_cfg_dir: Path,
    log_file: Path,
) -> None:
    """
    Run the full pipeline with given cfg roots.

    This is the main entry point that both local.py and local_dev.py call
    after obtaining cfg roots (via cloning or local paths). The caller must
    pass stage repo settings and pre-created run/log directories.
    """
    # Validation
    validate_ephemeral(ctl_env, ephemeral)
    validate_env_compatibility(ctl_env, plt_env)

    # Load workflow and inventory (validate before creating dirs)
    workflow_cfg = load_workflow_cfg(
        ctl_cfg_root,
        ctl_env,
        inventory_name,
        workflow_name,
        allow_target_stage_workflow=allow_target_stage_workflow,
    )
    inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name)

    # Validate stages have commits for staging/prod
    validate_stages_have_commits(workflow_cfg, ctl_env)

    logging.info(f"Environment validation passed: ctl_env={ctl_env} → plt_env={plt_env}")

    # Prepare pipeline config
    plt_cfg_source_dirs = get_plt_cfg_source_dirs(plt_cfg_root, plt_env)
    active_stages, pipeline_run_cfg_path = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
        plt_merged_cfg_dir,
        plt_cfg_source_dirs,
        artifacts_dir,
        plt_variants,
        stage_repo_key=stage_repo_key,
        require_stage_ref=require_stage_ref,
    )

    # Write git metas
    write_git_metas(ctl_cfg_root, plt_cfg_root, artifacts_dir)

    # Run cfg distribution
    plt_destination_cfg_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path, plt_merged_cfg_dir, run_dir
    )

    # Run stages
    run_stages(
        active_stages, run_dir, plt_destination_cfg_dir_path,
        inventory_name, plt_env, ephemeral, run_id,
        main_tag=main_tag,
    )

    print_run_summary(run_id, log_file)
