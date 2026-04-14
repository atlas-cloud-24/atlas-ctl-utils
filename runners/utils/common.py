"""Shared utilities for local and local_dev runners."""

import argparse
import collections
import logging
import logging.handlers
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml

from utils.git_meta import write_git_meta_to_file

# Environment trust hierarchy: higher value = higher trust/security requirements
# Control plane must have equal or higher trust than target platform
ENV_TRUST = {"dev": 0, "test": 1, "staging": 2, "prod": 3}

# Environment groupings
ENVS_ALL = tuple(ENV_TRUST.keys())
ENVS_DEV_TEST = ("dev", "test")
ENVS_STAGING_PROD = ("staging", "prod")
REQUIRED_TOOLING_REFS = ("atlas-ctl-utils", "atlas-plt-utils")
LOCAL_TOOLING_CFG_NAME = "local_repos.yaml"
TOOLING_ENV_PREFIXES = {
    "atlas-ctl-utils": "ATLAS_CTL_UTILS",
    "atlas-plt-utils": "ATLAS_PLT_UTILS",
}
TOOLING_DEFAULT_REPO_URLS = {
    "atlas-ctl-utils": "https://github.com/atlas-cloud-24/atlas-ctl-utils.git",
    "atlas-plt-utils": "https://github.com/atlas-cloud-24/atlas-plt-utils.git",
}
RUN_ACTIONS = ("pipeline", "maintenance")
MAINTENANCE_ACTIONS = ("force-unlock",)
FORCE_UNLOCK_STAGE_SCRIPT_CANDIDATES = (
    Path("pipeline/stages/plan/infra/src/stage.sh"),
    Path("pipeline/stages/provision/infra/src/stage.sh"),
    Path("pipeline/stages/destroy/infra/src/stage.sh"),
)
FORCE_UNLOCK_KEY_RE = re.compile(r"\./bin/tf\.sh\s+infra\s+init\s+\$?([A-Za-z_][A-Za-z0-9_]*)")
FORCE_UNLOCK_URI_RE = re.compile(r'echo\s+"Using\s+\$([A-Za-z_][A-Za-z0-9_]*)"')

SERVICE_ID = "atlas-ctl-orchestrator-local"

# ANSI escape code pattern
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')


class UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


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
    plt_merged_dir: Path
    log_file: Path
    ctl_cfg_root: Path
    plt_cfg_root: Path
    workflow_cfg: dict
    inventory_cfg: dict
    active_stages: dict
    pipeline_run_cfg_path: Path
    plt_distributed_dir_path: Path


def merge_cfg_values(base, overlay):
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            if key in merged:
                merged[key] = merge_cfg_values(merged[key], value)
            else:
                merged[key] = value
        return merged
    return overlay


def load_cfg_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    if not raw.strip():
        return {}

    data = yaml.load(raw, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"cfg file must contain a mapping: {path}")
    return data


def render_merged_cfg_header(
    dest_path: str | Path,
    sources: list[str],
    source_log_roots: tuple[Path, ...] = (),
    dest_log_roots: tuple[Path, ...] = (),
) -> str:
    rendered_dest = format_path_for_log(dest_path, dest_log_roots)
    rendered_sources = [format_path_for_log(src, source_log_roots) for src in sources]

    dest_rel = Path(rendered_dest)
    section_name = dest_rel.parent.name if dest_rel.parent.name else dest_rel.stem
    section_name = section_name.replace("_", " ").upper()

    lines = [
        "###################################",
        f"# {section_name}",
        "###################################",
        "# =================================",
        f"# {dest_rel.stem} ({rendered_dest})",
        "# =================================",
        "# merged from:",
    ]
    lines.extend(f"# - {src}" for src in rendered_sources)
    return "\n".join(lines) + "\n\n"


def write_cfg_yaml(path: str, data: dict, *, header_comment: str | None = None) -> None:
    rendered = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    with open(path, "w", encoding="utf-8") as f:
        if header_comment:
            f.write(header_comment)
        f.write(rendered)


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
        raw = f.read()

    if not raw.strip():
        return {}

    data = yaml.load(raw, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML file must contain a mapping: {path}")
    return data


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


def validate_action_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments for pipeline vs maintenance mode."""
    if args.action == "pipeline":
        if not args.workflow:
            raise RuntimeError("❌ --workflow is required for --action=pipeline")
        return

    if getattr(args, "ctl_variants", None):
        raise RuntimeError("❌ --ctl-variants is supported only for --action=pipeline")
    if not args.maintenance_action:
        raise RuntimeError("❌ --maintenance-action is required for --action=maintenance")
    if not args.stage_source:
        raise RuntimeError("❌ --stage-source is required for --action=maintenance")
    if args.maintenance_action == "force-unlock" and not args.lock_id:
        raise RuntimeError("❌ --lock-id is required for --maintenance-action=force-unlock")





def validate_stages_have_commits(active_stages: dict, ctl_env: str) -> None:
    """
    Validate that all resolved stages and modules have explicit commits for prod environments.

    For prod environments, using branch references is not allowed. Validation runs
    after workflow overrides and env refs have been resolved into active stages.
    """
    if ctl_env not in ENVS_STAGING_PROD:
        return

    stages_without_commit = []
    modules_without_commit = []
    for stage_id, stage_cfg in active_stages.items():
        if not stage_cfg.get("commit"):
            stages_without_commit.append(stage_id)

        raw_modules = stage_cfg.get("modules") or {}
        if not isinstance(raw_modules, dict):
            modules_without_commit.append(f"{stage_id}:<invalid-modules>")
            continue

        for module_name, module_cfg in raw_modules.items():
            if not module_cfg.get("commit"):
                modules_without_commit.append(f"{stage_id}:{module_name}")

    if stages_without_commit or modules_without_commit:
        details = []
        if stages_without_commit:
            details.append(f"Stages missing 'commit': {stages_without_commit}")
        if modules_without_commit:
            details.append(f"Modules missing 'commit': {modules_without_commit}")
        raise RuntimeError(
            f"❌ For {ENVS_STAGING_PROD} environments, all stages and modules must have explicit 'commit' specified.\n"
            f"   {'; '.join(details)}\n"
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


def validate_tooling_refs_have_commits(tooling_refs: dict, ctl_env: str) -> None:
    """Validate that tooling refs use commits (not branches) for staging/prod environments."""
    if ctl_env not in ENVS_STAGING_PROD:
        return

    errors = []
    for tooling_name in REQUIRED_TOOLING_REFS:
        tooling_ref = tooling_refs.get(tooling_name) or {}
        if not isinstance(tooling_ref, dict):
            errors.append(f"tooling '{tooling_name}' ref must be a mapping")
            continue

        if tooling_ref.get("commit"):
            continue

        if tooling_ref.get("branch"):
            errors.append(f"tooling '{tooling_name}' uses branch='{tooling_ref['branch']}' but commit is required")
        else:
            errors.append(f"tooling '{tooling_name}' is missing commit")

    if errors:
        raise RuntimeError(
            f"❌ For {ENVS_STAGING_PROD} environments, tooling refs must use explicit commits.\n"
            f"   {'; '.join(errors)}"
        )


def git_clone(repo_url: str, branch: str | None, commit: str | None, dest: Path, token: str | None = None):
    env = os.environ.copy()
    askpass_path: str | None = None
    if token:
        fd, askpass_path = tempfile.mkstemp(suffix=".sh")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(
                    "#!/bin/sh\n"
                    "case \"$1\" in\n"
                    "  *Username*) printf '%s\\n' \"${GIT_HTTP_USERNAME:-x-access-token}\" ;;\n"
                    "  *Password*) printf '%s\\n' \"${GIT_HTTP_PASSWORD:-}\" ;;\n"
                    "  *) printf '\\n' ;;\n"
                    "esac\n"
                )
            os.chmod(askpass_path, 0o700)
        except Exception:
            os.unlink(askpass_path)
            raise

        env["GIT_ASKPASS"] = askpass_path
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_HTTP_USERNAME"] = "x-access-token"
        env["GIT_HTTP_PASSWORD"] = token

    try:
        # commit pinned → checkout exact commit
        if commit:
            cmd = ["git", "clone", repo_url, str(dest)]
            logging.info(f"Running command: git clone {repo_url} {dest}")
            run_and_log(cmd, env=env)

            cmd = f"git checkout {commit}"
            logging.info(f"Running command: {cmd}")
            run_and_log(cmd.split(), cwd=dest, env=env)
            return

        # no commit → use branch HEAD
        if not branch:
            raise RuntimeError(f"❌ Either branch or commit must be provided for repo {repo_url}")

        cmd = ["git", "clone", "--branch", branch, "--depth", "1", repo_url, str(dest)]
        logging.info(f"Running command: git clone --branch {branch} --depth 1 {repo_url} {dest}")
        run_and_log(cmd, env=env)
    finally:
        if askpass_path:
            os.unlink(askpass_path)


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

    parsed = urlparse(repo_url)
    if not parsed.scheme or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. local.py accepts only URL@branch=name or URL@commit=sha; use local_dev.py for local paths"
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


def parse_relative_paths_arg(value: str, *, root_dir_name: str, item_label: str) -> list[str]:
    """Parse comma-separated relative paths under a cfg root directory."""
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
                f"{item_label} path must be relative to {root_dir_name}/: {item}"
            )
        if ".." in path.parts:
            raise argparse.ArgumentTypeError(
                f"{item_label} path must not contain '..': {item}"
            )

    duplicates = [item for item, count in collections.Counter(raw).items() if count > 1]
    if duplicates:
        raise argparse.ArgumentTypeError(
            f"{item_label} paths must be unique under {root_dir_name}/; duplicates: {', '.join(sorted(duplicates))}"
        )

    return raw


def parse_overlays_arg(value: str) -> list[str]:
    """Parse comma-separated overlay paths under overlays/."""
    return parse_relative_paths_arg(
        value,
        root_dir_name="overlays",
        item_label="Overlay",
    )


def parse_ctl_variants_arg(value: str) -> list[str]:
    """Parse comma-separated ctl variant paths under variants/."""
    return parse_relative_paths_arg(
        value,
        root_dir_name="variants",
        item_label="Ctl variant",
    )

def build_active_stages(
    workflow_cfg: dict,
    inventory_cfg: dict,
    repo_key: str = "repo_url",
    require_branch_or_commit: bool = True,
    stage_source_refs: dict | None = None,
    module_refs: dict | None = None,
) -> dict:
    inventory_stage_sources = inventory_cfg.get("stage_sources", {})
    if not isinstance(inventory_stage_sources, dict):
        raise RuntimeError("'stage_sources' in inventory must be a mapping: source -> meta")

    stage_source_refs = stage_source_refs or {}
    module_refs = module_refs or {}
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

        if stage_source not in inventory_stage_sources:
            raise RuntimeError(
                f"Stage source '{stage_source}' (stage id='{stage_id}') not found in inventory '{workflow_cfg.get('inventory')}'"
            )

        cat = inventory_stage_sources[stage_source]
        if not isinstance(cat, dict):
            raise RuntimeError(
                f"Stage source '{stage_source}' metadata must be a mapping, got: {type(cat).__name__}"
            )

        stage_ref = stage_source_refs.get(stage_source) or {}
        if not isinstance(stage_ref, dict):
            raise RuntimeError(
                f"Stage source refs for '{stage_source}' must be a mapping, got: {type(stage_ref).__name__}"
            )

        branch = stage_over.get("branch") or stage_ref.get("branch")
        commit = stage_over.get("commit") or stage_ref.get("commit")
        child_workflow = stage_over.get("workflow")

        if branch and commit:
            raise RuntimeError(
                f"Stage '{stage_id}' resolved both branch='{branch}' and commit='{commit}'. "
                "Only one ref type may be set."
            )

        if require_branch_or_commit and not branch and not commit:
            raise RuntimeError(f"Stage '{stage_id}' has neither branch nor commit configured")

        repo_value = cat.get(repo_key)
        if not repo_value:
            raise RuntimeError(
                f"Stage '{stage_id}' (source='{stage_source}') missing '{repo_key}' in inventory '{workflow_cfg.get('inventory')}'"
            )

        active_stage = {
            "source": stage_source,
            "branch": branch,
            "commit": commit,
            "workflow": child_workflow,
            "cfg_files": cat.get("cfg_files", []),
        }

        if repo_key == "repo_path":
            repo_path = Path(repo_value).expanduser()
            if not repo_path.is_absolute():
                raise RuntimeError(
                    f"Stage '{stage_id}' repo_path must be absolute, got: {repo_value}"
                )
            active_stage["repo_path"] = str(repo_path.resolve())
        else:
            active_stage["repo_url"] = repo_value
            active_stage["token_type"] = cat.get("token_type")

        raw_modules = cat.get("modules") or {}
        if raw_modules and not isinstance(raw_modules, dict):
            raise RuntimeError(
                f"Stage '{stage_id}' modules must be a mapping, got: {type(raw_modules).__name__}"
            )

        resolved_modules = {}
        for module_name, module_meta in raw_modules.items():
            if not isinstance(module_name, str):
                raise RuntimeError(
                    f"Stage '{stage_id}' module names must be strings, got: {type(module_name).__name__}"
                )
            if module_meta is None:
                module_meta = {}
            if not isinstance(module_meta, dict):
                raise RuntimeError(
                    f"Stage '{stage_id}' module '{module_name}' metadata must be a mapping, got: {type(module_meta).__name__}"
                )

            module_ref = module_refs.get(module_name) or {}
            if not isinstance(module_ref, dict):
                raise RuntimeError(
                    f"Module refs for '{module_name}' must be a mapping, got: {type(module_ref).__name__}"
                )

            module_branch = module_ref.get("branch")
            module_commit = module_ref.get("commit")
            if module_branch and module_commit:
                raise RuntimeError(
                    f"Module '{module_name}' resolved both branch='{module_branch}' and commit='{module_commit}'. "
                    "Only one ref type may be set."
                )
            if require_branch_or_commit and not module_branch and not module_commit:
                raise RuntimeError(
                    f"Stage '{stage_id}' module '{module_name}' has neither branch nor commit configured"
                )

            dest = module_meta.get("dest")
            if not isinstance(dest, str) or not dest.strip():
                raise RuntimeError(
                    f"Stage '{stage_id}' module '{module_name}' must define non-empty 'dest'"
                )
            dest_path = Path(dest)
            if dest_path.is_absolute() or ".." in dest_path.parts:
                raise RuntimeError(
                    f"Stage '{stage_id}' module '{module_name}' dest must stay within the stage repo: {dest}"
                )

            module_repo_value = module_meta.get(repo_key)
            if not module_repo_value:
                raise RuntimeError(
                    f"Stage '{stage_id}' module '{module_name}' missing '{repo_key}' in inventory '{workflow_cfg.get('inventory')}'"
                )

            resolved_module = {
                "dest": dest,
                "branch": module_branch,
                "commit": module_commit,
            }
            if repo_key == "repo_path":
                module_repo_path = Path(module_repo_value).expanduser()
                if not module_repo_path.is_absolute():
                    raise RuntimeError(
                        f"Stage '{stage_id}' module '{module_name}' repo_path must be absolute, got: {module_repo_value}"
                    )
                resolved_module["repo_path"] = str(module_repo_path.resolve())
            else:
                resolved_module["repo_url"] = module_repo_value
                resolved_module["token_type"] = module_meta.get("token_type")

            resolved_modules[module_name] = resolved_module

        if resolved_modules:
            active_stage["modules"] = resolved_modules

        active[stage_id] = active_stage

    return active

def merge_config_dirs(
    source_dirs: list[str],
    dest_dir: str,
    clear_dest: bool = True,
    *,
    source_log_roots: tuple[Path, ...] = (),
    dest_log_roots: tuple[Path, ...] = (),
    merged_files: dict[str, list[str]] | None = None,
    skip_filenames: set[str] | None = None,
) -> dict[str, list[str]]:
    """Merge config directories in sequence using YAML-aware overlay semantics."""
    if clear_dest and os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)

    if merged_files is None:
        merged_files = {}

    for source_dir in source_dirs:
        for root, _, files in os.walk(source_dir):
            rel_root = os.path.relpath(root, source_dir)
            dest_root = os.path.join(dest_dir, rel_root) if rel_root != "." else dest_dir

            os.makedirs(dest_root, exist_ok=True)

            for file in files:
                if skip_filenames and file in skip_filenames:
                    continue
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_root, file)

                if os.path.exists(dest_file):
                    merged_data = merge_cfg_values(load_cfg_yaml(dest_file), load_cfg_yaml(src_file))
                    source_list = merged_files.setdefault(dest_file, [])
                    source_list.append(src_file)
                    header_comment = None
                    if len(source_list) > 1 and (source_log_roots or dest_log_roots):
                        header_comment = render_merged_cfg_header(
                            dest_file,
                            source_list,
                            source_log_roots=source_log_roots,
                            dest_log_roots=dest_log_roots,
                        )
                    write_cfg_yaml(dest_file, merged_data, header_comment=header_comment)
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

    return merged_files


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add arguments common to both local and local_dev runners."""
    parser.add_argument(
        "--main-tag",
        required=True,
        help="Main tag passed to stage runners",
    )
    parser.add_argument(
        "--plt-overlays",
        required=False,
        default=[],
        dest="plt_overlays",
        type=parse_overlays_arg,
        help="Optional comma-separated overlay paths under overlays/",
    )
    parser.add_argument(
        "--ctl-variants",
        required=False,
        default=[],
        dest="ctl_variants",
        type=parse_ctl_variants_arg,
        help="Optional comma-separated ctl variant paths under variants/",
    )
    parser.add_argument(
        "--ctl-env",
        required=True,
        choices=list(ENV_TRUST.keys()),
        help="Ctl environment type (e.g. dev|test|staging|prod)",
    )
    parser.add_argument(
        "--action",
        default="pipeline",
        choices=list(RUN_ACTIONS),
        help="Top-level runner action",
    )
    parser.add_argument(
        "--inventory",
        required=True,
        help="inventory name (e.g. create|destroy|plan)",
    )
    parser.add_argument(
        "--workflow",
        help="workflow name (required for --action pipeline)",
    )
    parser.add_argument(
        "--maintenance-action",
        choices=list(MAINTENANCE_ACTIONS),
        help="maintenance action name (required for --action maintenance)",
    )
    parser.add_argument(
        "--stage-source",
        help="target stage source from inventory (required for --action maintenance)",
    )
    parser.add_argument(
        "--lock-id",
        help="state lock ID to force-unlock (required for force-unlock)",
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
    """Load env workflow config, then base fallback, then optional local target-stage workflow."""
    env_workflow_path = (
        ctl_cfg_root
        / "workflows"
        / ctl_env
        / inventory_name
        / f"{workflow_name}.yaml"
    )
    if env_workflow_path.is_file():
        return load_yaml(env_workflow_path)

    base_workflow_path = (
        ctl_cfg_root
        / "workflows"
        / "base"
        / inventory_name
        / f"{workflow_name}.yaml"
    )
    if base_workflow_path.is_file():
        logging.info(
            "Workflow file not found for ctl env '%s', using base workflow: %s",
            ctl_env,
            base_workflow_path,
        )
        return load_yaml(base_workflow_path)

    if allow_target_stage_workflow:
        workflow_cfg = build_target_stage_workflow_cfg(ctl_env, inventory_name, workflow_name)
        if workflow_cfg is not None:
            logging.info(
                "Workflow file not found, using synthesized single-stage workflow for local dev: %s",
                workflow_name,
            )
            return workflow_cfg

    raise RuntimeError(
        f"❌ workflow file not found in ctl env '{ctl_env}' or base: "
        f"{inventory_name}/{workflow_name}.yaml"
    )


def get_ctl_variants_root(ctl_cfg_root: Path) -> Path:
    """Return ctl variant root dir under variants/."""
    ctl_variants_root = (ctl_cfg_root / "variants").resolve()
    if ctl_variants_root.is_dir():
        return ctl_variants_root
    raise RuntimeError(f"Ctl variants dir not found under: {ctl_cfg_root}")


def get_ctl_variant_root(ctl_cfg_root: Path, ctl_variant: str) -> Path:
    """Resolve one selected ctl variant path under variants/."""
    ctl_variants_root = get_ctl_variants_root(ctl_cfg_root)
    variant_root = (ctl_variants_root / ctl_variant).resolve()
    try:
        variant_root.relative_to(ctl_variants_root)
    except ValueError as exc:
        raise RuntimeError(f"Ctl variant path escapes variants/: {ctl_variant}") from exc
    if not variant_root.exists():
        raise RuntimeError(f"Ctl variant path not found: {variant_root}")
    if not variant_root.is_dir():
        raise RuntimeError(f"Ctl variant path must be a directory: {variant_root}")
    return variant_root


def load_optional_yaml_mapping(path: Path) -> dict:
    """Load an optional YAML mapping, returning {} when the file is absent."""
    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML file must contain a mapping: {path}")
    return data


def _load_meta_string_list(meta: dict, key: str, item_kind: str, item_label: str) -> list[str]:
    raw = meta.get(key) or []
    if not isinstance(raw, list) or not all(isinstance(item, str) and item for item in raw):
        raise RuntimeError(
            f"❌ {item_kind} '{item_label}' meta key '{key}' must be a list of non-empty strings"
        )
    return raw


def validate_ctl_variant_meta(
    meta: dict,
    *,
    variant_label: str,
    ctl_env: str,
    plt_env: str,
    plt_overlays: list[str],
) -> None:
    """Validate ctl variant metadata against selected plt overlays."""
    if not isinstance(meta, dict):
        raise RuntimeError(f"❌ ctl variant '{variant_label}' meta.yaml must contain a mapping")

    allowed_envs = _load_meta_string_list(meta, "allowed_envs", "ctl variant", variant_label)
    if allowed_envs:
        if ctl_env not in allowed_envs:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' is not allowed for ctl env '{ctl_env}'; "
                f"allowed_envs={allowed_envs}"
            )
        if plt_env not in allowed_envs:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' is not allowed for plt env '{plt_env}'; "
                f"allowed_envs={allowed_envs}"
            )

    required_plt_overlays = _load_meta_string_list(meta, "requires_plt_overlays", "ctl variant", variant_label)
    if required_plt_overlays:
        missing = [overlay for overlay in required_plt_overlays if overlay not in plt_overlays]
        if missing:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' requires plt overlays {missing}, "
                f"but selected plt overlays are {plt_overlays}"
            )


def get_overlay_dir(plt_cfg_root: Path, plt_overlay: str) -> Path:
    """Resolve one selected overlay path under overlays/."""
    overlays_root = get_overlay_root(plt_cfg_root)
    overlay_root = (overlays_root / plt_overlay).resolve()
    try:
        overlay_root.relative_to(overlays_root)
    except ValueError as exc:
        raise RuntimeError(f"Overlay path escapes overlays/: {plt_overlay}") from exc
    if not overlay_root.exists():
        raise RuntimeError(f"Overlay path not found: {overlay_root}")
    if not overlay_root.is_dir():
        raise RuntimeError(f"Overlay path must be a directory: {overlay_root}")
    return overlay_root


def validate_overlay_meta(
    meta: dict,
    *,
    overlay_label: str,
    ctl_env: str,
    plt_env: str,
) -> None:
    """Validate plt overlay metadata against selected envs."""
    if not isinstance(meta, dict):
        raise RuntimeError(f"❌ plt overlay '{overlay_label}' meta.yaml must contain a mapping")

    allowed_envs = _load_meta_string_list(meta, "allowed_envs", "plt overlay", overlay_label)
    if allowed_envs:
        if ctl_env not in allowed_envs:
            raise RuntimeError(
                f"❌ plt overlay '{overlay_label}' is not allowed for ctl env '{ctl_env}'; "
                f"allowed_envs={allowed_envs}"
            )
        if plt_env not in allowed_envs:
            raise RuntimeError(
                f"❌ plt overlay '{overlay_label}' is not allowed for plt env '{plt_env}'; "
                f"allowed_envs={allowed_envs}"
            )


def get_workflow_stage_id(stage_entry) -> str:
    """Return the stage id from a workflow stage entry."""
    if isinstance(stage_entry, str):
        return stage_entry
    if isinstance(stage_entry, dict):
        stage_id = stage_entry.get("id")
        if isinstance(stage_id, str) and stage_id:
            return stage_id
    raise RuntimeError(f"❌ invalid workflow stage entry: {stage_entry!r}")


def validate_ctl_variant_stage_patch_entry(raw_stage: dict, variant_label: str) -> tuple[str, dict]:
    """Validate one ctl variant stage patch entry and return its op plus stage payload."""
    if not isinstance(raw_stage, dict):
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' workflow patch entries must be mappings"
        )

    add_before = raw_stage.get("add_before")
    add_after = raw_stage.get("add_after")
    op_keys = [key for key, value in (("add_before", add_before), ("add_after", add_after)) if value is not None]
    if len(op_keys) != 1:
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' stage patch entry must define exactly one of "
            f"'add_before' or 'add_after': {raw_stage}"
        )

    anchor_stage_id = raw_stage[op_keys[0]]
    if not isinstance(anchor_stage_id, str) or not anchor_stage_id:
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' {op_keys[0]} value must be a non-empty stage id"
        )

    stage_entry = {k: v for k, v in raw_stage.items() if k not in ("add_before", "add_after")}
    stage_id = stage_entry.get("id")
    stage_source = stage_entry.get("source")
    stage_workflow = stage_entry.get("workflow")
    if not isinstance(stage_id, str) or not stage_id:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' inserted stage must define non-empty 'id'")
    if not isinstance(stage_source, str) or not stage_source:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' stage '{stage_id}' must define non-empty 'source'")
    if not isinstance(stage_workflow, str) or not stage_workflow:
        raise RuntimeError(f"❌ ctl variant '{variant_label}' stage '{stage_id}' must define non-empty 'workflow'")
    if stage_entry.get("branch") and stage_entry.get("commit"):
        raise RuntimeError(
            f"❌ ctl variant '{variant_label}' stage '{stage_id}' cannot define both 'branch' and 'commit'"
        )

    return op_keys[0], stage_entry


def apply_ctl_variant_workflow_patch(
    workflow_cfg: dict,
    patch_cfg: dict,
    *,
    variant_label: str,
    patch_label: str,
) -> dict:
    """Apply add_before/add_after workflow patch entries from one ctl variant patch file."""
    stages = workflow_cfg.get("stages")
    if not isinstance(stages, list):
        raise RuntimeError(f"❌ workflow cfg must contain a 'stages' list before applying ctl variants")

    patch_stages = patch_cfg.get("stages") or []
    if not isinstance(patch_stages, list):
        raise RuntimeError(
            f"❌ ctl variant patch '{patch_label}' must contain a 'stages' list"
        )

    resolved_stages = list(stages)
    for raw_stage in patch_stages:
        op, stage_entry = validate_ctl_variant_stage_patch_entry(raw_stage, variant_label)
        anchor_stage_id = raw_stage[op]
        stage_id = stage_entry["id"]

        stage_ids = [get_workflow_stage_id(stage) for stage in resolved_stages]
        if anchor_stage_id not in stage_ids:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' patch '{patch_label}' references missing anchor "
                f"stage id '{anchor_stage_id}'"
            )
        if stage_id in stage_ids:
            raise RuntimeError(
                f"❌ ctl variant '{variant_label}' patch '{patch_label}' inserts duplicate stage id '{stage_id}'"
            )

        anchor_index = stage_ids.index(anchor_stage_id)
        insert_index = anchor_index if op == "add_before" else anchor_index + 1
        resolved_stages.insert(insert_index, stage_entry)
        logging.info(
            "Applied ctl variant '%s': %s stage '%s' %s '%s'",
            variant_label,
            op,
            stage_id,
            "before" if op == "add_before" else "after",
            anchor_stage_id,
        )

    patched_workflow_cfg = dict(workflow_cfg)
    patched_workflow_cfg["stages"] = resolved_stages
    return patched_workflow_cfg


def apply_ctl_variants_to_workflow_cfg(
    ctl_cfg_root: Path,
    workflow_cfg: dict,
    *,
    ctl_env: str,
    plt_env: str,
    inventory_name: str,
    workflow_name: str,
    ctl_variants: list[str],
    plt_overlays: list[str],
) -> dict:
    """Apply selected ctl variants to a loaded workflow cfg."""
    if not ctl_variants:
        return workflow_cfg

    patched_workflow_cfg = dict(workflow_cfg)
    patched_workflow_cfg["stages"] = list(workflow_cfg.get("stages") or [])

    for ctl_variant in ctl_variants:
        variant_root = get_ctl_variant_root(ctl_cfg_root, ctl_variant)
        meta = load_optional_yaml_mapping(variant_root / "meta.yaml")
        validate_ctl_variant_meta(
            meta,
            variant_label=ctl_variant,
            ctl_env=ctl_env,
            plt_env=plt_env,
            plt_overlays=plt_overlays,
        )

        patch_path = variant_root / "workflows" / inventory_name / f"{workflow_name}.yaml"
        patch_cfg = load_optional_yaml_mapping(patch_path)
        if not patch_cfg:
            logging.info(
                "Ctl variant '%s' has no workflow patch for %s/%s",
                ctl_variant,
                inventory_name,
                workflow_name,
            )
            continue

        patched_workflow_cfg = apply_ctl_variant_workflow_patch(
            patched_workflow_cfg,
            patch_cfg,
            variant_label=ctl_variant,
            patch_label=str(patch_path),
        )

    return patched_workflow_cfg


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


def load_env_refs_cfg(ctl_cfg_root: Path, ctl_env: str) -> tuple[dict, Path]:
    """Load env-scoped refs from refs/<ctl_env>.yaml if present."""
    refs_path = (
        ctl_cfg_root
        / "refs"
        / f"{ctl_env}.yaml"
    )
    if not refs_path.is_file():
        logging.info(f"No refs file found for ctl env '{ctl_env}': {refs_path}")
        return {}, refs_path

    refs_cfg = load_yaml(refs_path) or {}
    if not isinstance(refs_cfg, dict):
        raise RuntimeError(f"❌ refs file must contain a mapping: {refs_path}")

    logging.info(f"Using refs file for ctl env '{ctl_env}': {refs_path}")
    return refs_cfg, refs_path



def load_ref_section_cfg(ctl_cfg_root: Path, ctl_env: str, section_name: str, entry_label: str) -> dict:
    """Load a generic ref section from refs/<ctl_env>.yaml if present."""
    refs_cfg, refs_path = load_env_refs_cfg(ctl_cfg_root, ctl_env)

    raw_refs = refs_cfg.get(section_name) or {}
    if not isinstance(raw_refs, dict):
        raise RuntimeError(
            f"❌ refs file must contain a '{section_name}' mapping: {refs_path}"
        )

    resolved_refs = {}
    for entry_name, entry_ref in raw_refs.items():
        if not isinstance(entry_name, str):
            raise RuntimeError(
                f"❌ {entry_label} refs keys must be strings: {refs_path}"
            )
        if entry_ref is None:
            entry_ref = {}
        if not isinstance(entry_ref, dict):
            raise RuntimeError(
                f"❌ {entry_label} refs for '{entry_name}' must be a mapping: {refs_path}"
            )

        branch = entry_ref.get("branch")
        commit = entry_ref.get("commit")
        if branch and commit:
            raise RuntimeError(
                f"❌ {entry_label} refs for '{entry_name}' define both branch and commit: {refs_path}"
            )

        resolved_refs[entry_name] = entry_ref

    return resolved_refs


def load_stage_source_refs_cfg(ctl_cfg_root: Path, ctl_env: str) -> dict:
    """Load env-scoped stage source refs from refs/<ctl_env>.yaml if present."""
    return load_ref_section_cfg(ctl_cfg_root, ctl_env, "stage_sources", "stage source")


def load_module_refs_cfg(ctl_cfg_root: Path, ctl_env: str) -> dict:
    """Load env-scoped module refs from refs/<ctl_env>.yaml if present."""
    return load_ref_section_cfg(ctl_cfg_root, ctl_env, "modules", "module")


def load_tooling_refs_cfg(ctl_cfg_root: Path, ctl_env: str) -> dict:
    """Load env-scoped tooling refs from refs/<ctl_env>.yaml if present."""
    refs_cfg, refs_path = load_env_refs_cfg(ctl_cfg_root, ctl_env)

    raw_tooling_refs = refs_cfg.get("tooling") or {}
    if not isinstance(raw_tooling_refs, dict):
        raise RuntimeError(f"❌ refs file must contain a 'tooling' mapping: {refs_path}")

    tooling_refs = {}
    for tooling_name, tooling_ref in raw_tooling_refs.items():
        if not isinstance(tooling_name, str):
            raise RuntimeError(f"❌ tooling refs keys must be strings: {refs_path}")
        if tooling_ref is None:
            tooling_ref = {}
        if not isinstance(tooling_ref, dict):
            raise RuntimeError(
                f"❌ tooling refs for '{tooling_name}' must be a mapping: {refs_path}"
            )

        repo_url = tooling_ref.get("repo_url")
        if repo_url is not None and not isinstance(repo_url, str):
            raise RuntimeError(
                f"❌ tooling repo_url for '{tooling_name}' must be a string: {refs_path}"
            )

        branch = tooling_ref.get("branch")
        commit = tooling_ref.get("commit")
        if branch and commit:
            raise RuntimeError(
                f"❌ tooling refs for '{tooling_name}' define both branch and commit: {refs_path}"
            )

        tooling_refs[tooling_name] = tooling_ref

    return tooling_refs


def load_local_tooling_cfg(ctl_cfg_root: Path) -> dict:
    """Load local tooling repo paths from local_repos.yaml for local_dev runs."""
    tooling_path = ctl_cfg_root / LOCAL_TOOLING_CFG_NAME
    if not tooling_path.is_file():
        logging.info(f"No local tooling file found: {tooling_path}")
        return {}

    tooling_cfg_root = load_yaml(tooling_path) or {}
    if not isinstance(tooling_cfg_root, dict):
        raise RuntimeError(f"❌ local tooling file must contain a mapping: {tooling_path}")

    raw_tooling_cfg = tooling_cfg_root.get("tooling") or {}
    if not isinstance(raw_tooling_cfg, dict):
        raise RuntimeError(
            f"❌ local tooling file must contain a 'tooling' mapping: {tooling_path}"
        )

    tooling_refs = {}
    for tooling_name, tooling_entry in raw_tooling_cfg.items():
        if not isinstance(tooling_name, str):
            raise RuntimeError(f"❌ local tooling keys must be strings: {tooling_path}")
        if tooling_entry is None:
            tooling_entry = {}
        if not isinstance(tooling_entry, dict):
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must be a mapping: {tooling_path}"
            )

        if tooling_entry.get("repo_url"):
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must use repo_path, not repo_url: {tooling_path}"
            )

        repo_path = tooling_entry.get("repo_path")
        if not repo_path:
            continue
        if not isinstance(repo_path, str):
            raise RuntimeError(
                f"❌ local tooling repo path for '{tooling_name}' must be a string: {tooling_path}"
            )

        branch = tooling_entry.get("branch")
        commit = tooling_entry.get("commit")
        if branch or commit:
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must not define branch or commit: {tooling_path}"
            )

        repo_path_obj = Path(repo_path).expanduser()
        if not repo_path_obj.is_absolute():
            repo_path_obj = (ctl_cfg_root / repo_path_obj).resolve()

        tooling_refs[tooling_name] = {"repo_path": str(repo_path_obj)}

    logging.info(f"Using local tooling file: {tooling_path}")
    return tooling_refs


def build_tooling_env(tooling_refs: dict) -> dict[str, str]:
    """Translate tooling refs into environment variables for setup scripts."""
    env_updates: dict[str, str] = {}

    for tooling_name, env_prefix in TOOLING_ENV_PREFIXES.items():
        tooling_ref = tooling_refs.get(tooling_name) or {}
        if not isinstance(tooling_ref, dict):
            continue

        repo_path = tooling_ref.get("repo_path")
        repo_url = tooling_ref.get("repo_url") or (
            None if repo_path else TOOLING_DEFAULT_REPO_URLS.get(tooling_name)
        )
        branch = tooling_ref.get("branch")
        commit = tooling_ref.get("commit")

        if repo_path:
            env_updates[f"{env_prefix}_REPO_PATH"] = repo_path
        if repo_url:
            env_updates[f"{env_prefix}_REPO_URL"] = repo_url
        if branch:
            env_updates[f"{env_prefix}_BRANCH"] = branch
        if commit:
            env_updates[f"{env_prefix}_COMMIT"] = commit

    return env_updates


def setup_run_dirs(run_id: str, inventory_name: str, memory_handler: logging.handlers.MemoryHandler) -> tuple[Path, Path, Path, Path]:
    """
    Create run directories and setup file logging.

    Returns:
        tuple: (run_dir, artifacts_dir, plt_merged_dir, log_file)
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
    plt_merged_dir = cfg_dir / "plt" / "merged"
    os.makedirs(plt_merged_dir)

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

    return run_dir, artifacts_dir, plt_merged_dir, log_file


def get_plt_cfg_source_dirs(plt_cfg_root: Path, plt_env: str) -> list[str]:
    """Get list of plt config source directories based on environment."""
    env_root = (plt_cfg_root / "env").resolve()
    env_specific = (env_root / plt_env).resolve()
    layers_cfg_path = env_specific / "import.yaml"

    if not env_specific.is_dir():
        raise RuntimeError(f"Platform env dir not found: {env_specific}")

    if not layers_cfg_path.is_file():
        raise RuntimeError(f"Missing required import.yaml for plt env '{plt_env}': {layers_cfg_path}")

    cfg = load_yaml(layers_cfg_path) or {}
    if not isinstance(cfg, dict):
        raise RuntimeError(f"import.yaml must contain a mapping: {layers_cfg_path}")

    layers = cfg.get("imports")
    if not isinstance(layers, list):
        raise RuntimeError(f"import.yaml must contain an 'imports' list: {layers_cfg_path}")

    source_dirs: list[str] = []
    seen: set[Path] = set()
    for layer in layers:
        if not isinstance(layer, str) or not layer.strip():
            raise RuntimeError(f"import.yaml entries must be non-empty strings: {layers_cfg_path}")

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

    source_dirs.append(str(env_specific))

    logging.info(f"Using import.yaml for plt env '{plt_env}': {source_dirs}")
    return source_dirs


def get_overlay_root(plt_cfg_root: Path) -> Path:
    """Return overlay root dir under overlays/."""
    overlays_root = (plt_cfg_root / "overlays").resolve()
    if overlays_root.is_dir():
        return overlays_root

    raise RuntimeError(f"Overlays dir not found under: {plt_cfg_root}")


def get_overlay_source_dirs(
    plt_cfg_root: Path,
    plt_cfg_source_dirs: list[str],
    plt_overlays: list[str],
    *,
    ctl_env: str,
    plt_env: str,
) -> list[str]:
    """Return absolute overlay source dirs selected by --plt-overlays.

    An overlay may be either:
    - a legacy direct overlay directory merged as-is
    - an env-layered directory that mirrors env/ source dirs such as
      common/all, common/non_prod, common/prod_shared, and dev/test/...
    """
    if not plt_overlays:
        return []

    env_root = (plt_cfg_root / "env").resolve()
    layer_rels: list[Path] = []
    seen_layer_rels: set[Path] = set()
    for src in plt_cfg_source_dirs:
        src_path = Path(src).resolve()
        try:
            rel = src_path.relative_to(env_root)
        except ValueError as exc:
            raise RuntimeError(f"Platform cfg source dir is outside env/: {src_path}") from exc
        if rel in seen_layer_rels:
            continue
        seen_layer_rels.add(rel)
        layer_rels.append(rel)

    source_dirs: list[str] = []
    seen: set[str] = set()
    for rel in plt_overlays:
        if rel in seen:
            continue
        seen.add(rel)

        src = get_overlay_dir(plt_cfg_root, rel)
        meta = load_optional_yaml_mapping(src / "meta.yaml")
        if meta:
            validate_overlay_meta(
                meta,
                overlay_label=rel,
                ctl_env=ctl_env,
                plt_env=plt_env,
            )

        layered_dirs: list[str] = []
        for layer_rel in layer_rels:
            candidate = (src / layer_rel).resolve()
            if not candidate.is_dir():
                continue
            layered_dirs.append(str(candidate))

        if layered_dirs:
            source_dirs.extend(layered_dirs)
        else:
            source_dirs.append(str(src))

    return source_dirs


def merge_plt_cfg_dirs(
    plt_cfg_root: Path,
    plt_merged_dir: Path,
    plt_cfg_source_dirs: list[str],
    ctl_env: str,
    plt_env: str,
    plt_overlays: list[str] | None = None,
    *,
    source_log_roots: tuple[Path, ...] | None = None,
    dest_log_roots: tuple[Path, ...] | None = None,
    merged_files: dict[str, list[str]] | None = None,
) -> dict[str, list[str]]:
    """Merge env layers first and selected overlays after them."""
    os.makedirs(plt_merged_dir, exist_ok=True)

    if source_log_roots is None:
        source_log_roots = (plt_cfg_root.resolve(),)
    if dest_log_roots is None:
        dest_log_roots = (plt_merged_dir.resolve(),)
    if merged_files is None:
        merged_files = {}

    logging.info(f"Merging env cfg dirs to {plt_merged_dir}")
    merged_files = merge_config_dirs(
        source_dirs=plt_cfg_source_dirs,
        dest_dir=str(plt_merged_dir),
        clear_dest=True,
        source_log_roots=source_log_roots,
        dest_log_roots=dest_log_roots,
        merged_files=merged_files,
    )

    overlay_dirs = get_overlay_source_dirs(
        plt_cfg_root,
        plt_cfg_source_dirs,
        plt_overlays or [],
        ctl_env=ctl_env,
        plt_env=plt_env,
    )
    if overlay_dirs:
        logging.info(f"Merging overlay dirs to {plt_merged_dir}: {overlay_dirs}")
        merge_config_dirs(
            source_dirs=overlay_dirs,
            dest_dir=str(plt_merged_dir),
            clear_dest=False,
            source_log_roots=source_log_roots,
            dest_log_roots=dest_log_roots,
            merged_files=merged_files,
            skip_filenames={"meta.yaml"},
        )

    return merged_files


def prepare_pipeline_cfg(
    plt_cfg_root: Path,
    workflow_cfg: dict,
    inventory_cfg: dict,
    plt_merged_dir: Path,
    plt_cfg_source_dirs: list[str],
    artifacts_dir: Path,
    ctl_env: str,
    plt_env: str,
    plt_overlays: list[str],
    stage_repo_key: str = "repo_url",
    require_stage_ref: bool = True,
    stage_source_refs: dict | None = None,
    module_refs: dict | None = None,
) -> tuple[dict, Path]:
    """
    Merge config dirs, build active stages, and write pipeline_run_cfg.

    Returns:
        tuple: (active_stages, pipeline_run_cfg_path)
    """
    source_log_roots = (plt_cfg_root.resolve(),)
    dest_log_roots = (plt_merged_dir.parent.parent.resolve(),)

    merged_files = merge_plt_cfg_dirs(
        plt_cfg_root=plt_cfg_root,
        plt_merged_dir=plt_merged_dir,
        plt_cfg_source_dirs=plt_cfg_source_dirs,
        ctl_env=ctl_env,
        plt_env=plt_env,
        plt_overlays=plt_overlays,
        source_log_roots=source_log_roots,
        dest_log_roots=dest_log_roots,
    )

    # get active stages
    active_stages = build_active_stages(
        workflow_cfg,
        inventory_cfg,
        repo_key=stage_repo_key,
        require_branch_or_commit=require_stage_ref,
        stage_source_refs=stage_source_refs,
        module_refs=module_refs,
    )

    write_stage_flow_artifact(
        artifacts_dir / "resolved_stages_flow.yaml",
        workflow_cfg.get("meta"),
        active_stages,
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


def write_stage_flow_artifact(path: Path, workflow_meta: dict | None, active_stages: dict) -> None:
    """Write a compact ordered stage-flow artifact."""
    stage_flow = {
        "meta": workflow_meta,
        "stages": [
            {
                "id": stage_id,
                "source": stage["source"],
                "workflow": stage["workflow"],
                "branch": stage.get("branch"),
                "commit": stage.get("commit"),
            }
            for stage_id, stage in active_stages.items()
        ],
    }
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(stage_flow, f, sort_keys=False)


def write_target_stage_flow_artifact(
    ctl_cfg_root: Path,
    artifacts_dir: Path,
    *,
    ctl_env: str,
    plt_env: str,
    inventory_name: str,
    workflow_name: str,
    ctl_variants: list[str],
    plt_overlays: list[str],
    stage_repo_key: str,
    require_stage_ref: bool,
    allow_target_stage_workflow: bool,
    stage_source_refs: dict | None,
    module_refs: dict | None,
) -> None:
    """For plan runs, write the matching create-flow preview artifact."""
    if inventory_name != "plan":
        return

    target_inventory_name = "create"
    try:
        target_workflow_cfg = load_workflow_cfg(
            ctl_cfg_root,
            ctl_env,
            target_inventory_name,
            workflow_name,
            allow_target_stage_workflow=allow_target_stage_workflow,
        )
        target_workflow_cfg = apply_ctl_variants_to_workflow_cfg(
            ctl_cfg_root,
            target_workflow_cfg,
            ctl_env=ctl_env,
            plt_env=plt_env,
            inventory_name=target_inventory_name,
            workflow_name=workflow_name,
            ctl_variants=ctl_variants,
            plt_overlays=plt_overlays,
        )
        target_inventory_cfg = load_inventory_cfg(ctl_cfg_root, target_inventory_name)
        target_active_stages = build_active_stages(
            target_workflow_cfg,
            target_inventory_cfg,
            repo_key=stage_repo_key,
            require_branch_or_commit=require_stage_ref,
            stage_source_refs=stage_source_refs,
            module_refs=module_refs,
        )
    except Exception as exc:
        logging.warning(
            "Skipping target_stages_flow.yaml generation for plan/%s: %s",
            workflow_name,
            exc,
        )
        return

    write_stage_flow_artifact(
        artifacts_dir / "target_stages_flow.yaml",
        target_workflow_cfg.get("meta"),
        target_active_stages,
    )


def write_git_metas(ctl_cfg_root: Path, plt_cfg_root: Path, artifacts_dir: Path) -> None:
    """Write all git meta files to artifacts directory."""
    # ctl_cfg_git_meta
    write_git_meta_to_file(
        git_dir=ctl_cfg_root,
        dest_dir=artifacts_dir,
        filename="piepeline_orchestrator_cfg_git_meta.yaml",
        generator=SERVICE_ID
    )

    # orchestrator_git_meta
    write_git_meta_to_file(
        git_dir=os.getcwd(),
        dest_dir=artifacts_dir,
        filename="piepeline_orchestrator_git_meta.yaml",
        generator=SERVICE_ID
    )

    # plt_cfg_git_meta
    write_git_meta_to_file(
        git_dir=plt_cfg_root,
        dest_dir=artifacts_dir,
        filename="plt_cfg_git_meta.yaml",
        generator=SERVICE_ID
    )


def run_cfg_distribution(pipeline_run_cfg_path: Path, plt_merged_dir: Path, run_dir: Path) -> Path:
    """Run cfg distribution script and return destination cfg dir path."""
    plt_distributed_dir_path = run_dir / "cfg" / "plt" / "distributed"
    env = os.environ.copy()
    env["pipeline_run_cfg_path"] = str(pipeline_run_cfg_path)
    env["plt_merged_dir_path"] = str(plt_merged_dir)
    env["plt_distributed_dir_path"] = str(plt_distributed_dir_path)
    logging.info(f"Running: {os.getcwd()}/stages/prepare/cfg/run/local.sh")
    run_and_log(
        [f"{os.getcwd()}/stages/prepare/cfg/run/local.sh"],
        env=env,
    )
    return plt_distributed_dir_path



def _remove_path(path: Path) -> None:
    """Remove an existing file, directory, or symlink."""
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def materialize_stage_modules(stage_id: str, stage: dict, repo_path: Path) -> None:
    """Populate stage-local child modules before setup runs."""
    modules = stage.get("modules") or {}
    if not modules:
        return

    repo_root = repo_path.resolve()
    for module_name, module_cfg in modules.items():
        dest_path = repo_path / module_cfg["dest"]
        try:
            dest_path.relative_to(repo_path)
        except ValueError as exc:
            raise RuntimeError(
                f"Stage '{stage_id}' module '{module_name}' dest escapes the stage repo: {module_cfg['dest']}"
            ) from exc

        if dest_path.exists() or dest_path.is_symlink():
            _remove_path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if "repo_path" in module_cfg:
            module_src = Path(module_cfg["repo_path"]).expanduser()
            if not module_src.is_dir():
                raise RuntimeError(
                    f"Stage '{stage_id}' module '{module_name}' repo_path not found: {module_src}"
                )
            # Copy the local working tree snapshot so Dockerized stage runners can read it.
            shutil.copytree(module_src, dest_path, symlinks=True)
        else:
            git_clone(
                repo_url=module_cfg["repo_url"],
                branch=module_cfg["branch"],
                commit=module_cfg["commit"],
                dest=dest_path,
                token=os.getenv(module_cfg["token_type"]) if module_cfg.get("token_type") else None,
            )


def prepare_stage_repo(
    stage_id: str,
    stage: dict,
    run_dir: Path,
    tooling_env: dict[str, str],
) -> tuple[Path, dict[str, str]]:
    """Clone/copy a stage repo, materialize child modules, and run its setup script."""
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
            token=os.getenv(stage["token_type"]),
        )

    materialize_stage_modules(stage_id, stage, repo_path)

    stage_setup_cmd = ["./pipeline/setup.sh"]
    stage_env = os.environ.copy()
    stage_env.update(tooling_env)
    logging.info(" ".join(stage_setup_cmd))
    run_and_log(
        stage_setup_cmd,
        cwd=repo_path,
        env=stage_env,
    )
    return repo_path, stage_env


def resolve_force_unlock_tfstate_vars(repo_path: Path) -> tuple[str, str | None]:
    """Extract tfstate key/uri variable names from standard infra stage scripts."""
    key_var = None
    uri_var = None

    for rel_path in FORCE_UNLOCK_STAGE_SCRIPT_CANDIDATES:
        script_path = repo_path / rel_path
        if not script_path.is_file():
            continue

        content = script_path.read_text(encoding="utf-8")
        key_match = FORCE_UNLOCK_KEY_RE.search(content)
        uri_match = FORCE_UNLOCK_URI_RE.search(content)

        if key_match:
            candidate = key_match.group(1)
            if key_var and candidate != key_var:
                raise RuntimeError(
                    f"❌ Conflicting tfstate key variables found for force-unlock in '{repo_path}': "
                    f"'{key_var}' vs '{candidate}'"
                )
            key_var = candidate

        if uri_match:
            candidate = uri_match.group(1)
            if uri_var and candidate != uri_var:
                raise RuntimeError(
                    f"❌ Conflicting tfstate uri variables found for force-unlock in '{repo_path}': "
                    f"'{uri_var}' vs '{candidate}'"
                )
            uri_var = candidate

    if not key_var:
        raise RuntimeError(
            f"❌ force-unlock is not supported for stage repo '{repo_path}'. "
            "Expected one of pipeline/stages/{plan,provision,destroy}/infra/src/stage.sh to call './bin/tf.sh infra init $..._tfstate_key'."
        )

    if uri_var is None and key_var.endswith("_key"):
        uri_var = f"{key_var[:-4]}_uri"

    return key_var, uri_var


def run_stages(
    active_stages: dict,
    run_dir: Path,
    plt_distributed_dir_path: Path,
    inventory_name: str,
    plt_env: str,
    ephemeral: bool,
    run_id: str,
    main_tag: str,
    tooling_refs: dict,
    use_local_tooling_cfg: bool,
) -> None:
    """Clone and run all active stages."""
    os.chdir(run_dir)
    tooling_env = build_tooling_env(tooling_refs)
    for stage_id, stage in active_stages.items():
        log_stage_banner(stage_id)
        repo_path, stage_env = prepare_stage_repo(stage_id, stage, run_dir, tooling_env)

        stage_runner_path = "./pipeline/runners/local_dev.py" if use_local_tooling_cfg else "./pipeline/runners/local.py"
        stage_run_cmd = [
            stage_runner_path,
            "--main-tag", main_tag,
            "--inventory", inventory_name,
            "--env-type", plt_env,
            "--workflow", stage["workflow"],
            "--origin-cfg", f"{plt_distributed_dir_path}/{stage_id}",
            "--ephemeral", bool2str(ephemeral),
            "--skip-cfg-merge",
            "--run-id", run_id,
        ]
        stage_cfg_dir = run_dir / "cfg" / "plt" / "per_stage" / stage_id
        os.makedirs(stage_cfg_dir, exist_ok=True)
        stage_env["STAGE_CFG_DIR"] = str(stage_cfg_dir)

        logging.info(" ".join(stage_run_cmd))
        run_and_log(
            stage_run_cmd,
            cwd=repo_path,
            env=stage_env,
        )


def print_run_summary(run_id: str, log_file: Path) -> None:
    """Print run summary at the end."""
    print(f"export run_id={run_id}")
    print(f"Log file: {log_file}")


def run_maintenance(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    ctl_env: str,
    plt_env: str,
    inventory_name: str,
    maintenance_action: str,
    stage_source: str,
    lock_id: str,
    ephemeral: bool,
    run_id: str,
    main_tag: str,
    plt_overlays: list[str],
    stage_repo_key: str,
    require_stage_ref: bool,
    use_local_tooling_cfg: bool,
    run_dir: Path,
    artifacts_dir: Path,
    plt_merged_dir: Path,
    log_file: Path,
) -> None:
    """Run a maintenance action against a single stage source."""
    validate_ephemeral(ctl_env, ephemeral)
    validate_env_compatibility(ctl_env, plt_env)

    inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name)
    stage_source_refs = load_stage_source_refs_cfg(ctl_cfg_root, ctl_env)
    module_refs = load_module_refs_cfg(ctl_cfg_root, ctl_env)
    if use_local_tooling_cfg:
        tooling_refs = load_local_tooling_cfg(ctl_cfg_root)
    else:
        tooling_refs = load_tooling_refs_cfg(ctl_cfg_root, ctl_env)
        validate_tooling_refs_have_commits(tooling_refs, ctl_env)

    logging.info(f"Environment validation passed: ctl_env={ctl_env} → plt_env={plt_env}")

    workflow_cfg = {
        "meta": {
            "name": f"{ctl_env}/{inventory_name}/maintenance/{maintenance_action}/{stage_source}",
            "inventory": inventory_name,
        },
        "stages": [
            {
                "id": stage_source,
                "source": stage_source,
            }
        ],
    }

    plt_cfg_source_dirs = get_plt_cfg_source_dirs(plt_cfg_root, plt_env)
    active_stages, pipeline_run_cfg_path = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
        plt_merged_dir,
        plt_cfg_source_dirs,
        artifacts_dir,
        ctl_env,
        plt_env,
        plt_overlays,
        stage_repo_key=stage_repo_key,
        require_stage_ref=require_stage_ref,
        stage_source_refs=stage_source_refs,
        module_refs=module_refs,
    )

    validate_stages_have_commits(active_stages, ctl_env)
    write_git_metas(ctl_cfg_root, plt_cfg_root, artifacts_dir)
    plt_distributed_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path,
        plt_merged_dir,
        run_dir,
    )

    os.chdir(run_dir)
    tooling_env = build_tooling_env(tooling_refs)
    if len(active_stages) != 1:
        raise RuntimeError(
            f"❌ maintenance action '{maintenance_action}' expected exactly one active stage, got: {list(active_stages)}"
        )

    stage_id, stage = next(iter(active_stages.items()))
    log_stage_banner(f"maintenance/{maintenance_action}/{stage_id}")
    repo_path, stage_env = prepare_stage_repo(stage_id, stage, run_dir, tooling_env)
    stage_cfg_dir = plt_distributed_dir_path / stage_id
    if not stage_cfg_dir.is_dir():
        raise RuntimeError(f"❌ distributed cfg dir not found for stage '{stage_id}': {stage_cfg_dir}")

    if maintenance_action != "force-unlock":
        raise RuntimeError(f"❌ Unsupported maintenance action: {maintenance_action}")

    tfstate_key_var, tfstate_uri_var = resolve_force_unlock_tfstate_vars(repo_path)
    stage_env["GITHUB_WORKSPACE"] = str(repo_path)
    stage_env["MAINTENANCE_STAGE_CFG_DIR"] = str(stage_cfg_dir)
    stage_env["TFSTATE_KEY_VAR"] = tfstate_key_var
    stage_env["LOCK_ID"] = lock_id
    stage_env["env_type"] = plt_env
    stage_env["main_tag"] = main_tag
    stage_env["run_id"] = run_id

    maintenance_cmd = [
        "bash",
        "-lc",
        """
set -euo pipefail
source ./pipeline/stages/_common/prepare_stage_runtime.sh
prepare_stage_runtime "${MAINTENANCE_STAGE_CFG_DIR}"
./bin/tf.sh infra init "$TFSTATE_KEY_VAR"
./bin/tf.sh infra force-unlock "$TFSTATE_KEY_VAR" "$LOCK_ID"
""",
    ]
    logging.info("bash -lc <force-unlock-script>")
    run_and_log(
        maintenance_cmd,
        cwd=repo_path,
        env=stage_env,
    )

    print_run_summary(run_id, log_file)


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
    plt_overlays: list[str],
    ctl_variants: list[str],
    stage_repo_key: str,
    require_stage_ref: bool,
    use_local_tooling_cfg: bool,
    allow_target_stage_workflow: bool,
    run_dir: Path,
    artifacts_dir: Path,
    plt_merged_dir: Path,
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
    workflow_cfg = apply_ctl_variants_to_workflow_cfg(
        ctl_cfg_root,
        workflow_cfg,
        ctl_env=ctl_env,
        plt_env=plt_env,
        inventory_name=inventory_name,
        workflow_name=workflow_name,
        ctl_variants=ctl_variants,
        plt_overlays=plt_overlays,
    )
    inventory_cfg = load_inventory_cfg(ctl_cfg_root, inventory_name)

    stage_source_refs = load_stage_source_refs_cfg(ctl_cfg_root, ctl_env)
    module_refs = load_module_refs_cfg(ctl_cfg_root, ctl_env)
    if use_local_tooling_cfg:
        tooling_refs = load_local_tooling_cfg(ctl_cfg_root)
    else:
        tooling_refs = load_tooling_refs_cfg(ctl_cfg_root, ctl_env)
        validate_tooling_refs_have_commits(tooling_refs, ctl_env)

    logging.info(f"Environment validation passed: ctl_env={ctl_env} → plt_env={plt_env}")

    # Prepare pipeline config
    plt_cfg_source_dirs = get_plt_cfg_source_dirs(plt_cfg_root, plt_env)
    active_stages, pipeline_run_cfg_path = prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        inventory_cfg,
        plt_merged_dir,
        plt_cfg_source_dirs,
        artifacts_dir,
        ctl_env,
        plt_env,
        plt_overlays,
        stage_repo_key=stage_repo_key,
        require_stage_ref=require_stage_ref,
        stage_source_refs=stage_source_refs,
        module_refs=module_refs,
    )

    # Validate stages have commits for staging/prod
    validate_stages_have_commits(active_stages, ctl_env)

    write_target_stage_flow_artifact(
        ctl_cfg_root,
        artifacts_dir,
        ctl_env=ctl_env,
        plt_env=plt_env,
        inventory_name=inventory_name,
        workflow_name=workflow_name,
        ctl_variants=ctl_variants,
        plt_overlays=plt_overlays,
        stage_repo_key=stage_repo_key,
        require_stage_ref=require_stage_ref,
        allow_target_stage_workflow=allow_target_stage_workflow,
        stage_source_refs=stage_source_refs,
        module_refs=module_refs,
    )

    # Write git metas
    write_git_metas(ctl_cfg_root, plt_cfg_root, artifacts_dir)

    # Run cfg distribution
    plt_distributed_dir_path = run_cfg_distribution(
        pipeline_run_cfg_path, plt_merged_dir, run_dir
    )

    # Run stages
    run_stages(
        active_stages, run_dir, plt_distributed_dir_path,
        inventory_name, plt_env, ephemeral, run_id,
        main_tag=main_tag,
        tooling_refs=tooling_refs,
        use_local_tooling_cfg=use_local_tooling_cfg,
    )

    print_run_summary(run_id, log_file)
