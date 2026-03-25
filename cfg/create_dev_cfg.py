#!/usr/bin/env python3
"""Create a local dev copy of a ctl cfg directory.

The source cfg can be either:
- a local directory
- an HTTP/HTTPS git URL, optionally with `@branch=...`, `@tag=...`, or `@commit=...`

The script copies the source cfg into a target directory, rewrites
`inventory/*.yaml` so each stage uses `repo_path` instead of `repo_url`,
removes ineffective `branch` keys from workflow YAMLs, writes local tooling
repo paths to `local_repos.yaml`, and removes `refs/` from the generated dev cfg.

Examples:
    ./cfg/create_dev_cfg.py \
        --input-cfg ~/programs/atlas/cfg/oxygen/oxygen-ctl-cfg \
        --output-dev-cfg ~/programs/atlas/cfg/oxygen/oxygen-ctl-cfg-dev \
        --repo-map-file ~/programs/atlas/cfg/oxygen/repo-map.json \
        --force

    ./cfg/create_dev_cfg.py \
        --input-cfg 'https://github.com/atlas-cloud-24/oxygen-ctl-cfg.git@branch=main' \
        --output-dev-cfg ~/programs/atlas/cfg/oxygen/oxygen-ctl-cfg-dev \
        --repo-map-file ~/programs/atlas/cfg/oxygen/repo-map.json \
        --force
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml


REF_SPEC_RE = re.compile(r"^(?P<source>.+)@(?P<kind>branch|tag|commit)=(?P<value>.+)$")
STAGE_LINE_RE = re.compile(r"^  (?P<stage>[^:\n]+):\s*$")
REPO_URL_LINE_RE = re.compile(r"^    repo_url:\s*.+$")
WORKFLOW_BRANCH_LINE_RE = re.compile(r"^\s+branch:\s*.+$")
LOCAL_TOOLING_CFG_NAME = "local_repos.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy a ctl cfg directory into a target directory and rewrite "
            "inventory repo_url values to repo_path using a JSON file with "
            "stage-to-path mappings and optional tooling repo-to-path mappings."
        )
    )
    parser.add_argument(
        "--input-cfg",
        required=True,
        dest="source_cfg",
        help=(
            "Source cfg directory or HTTP/HTTPS git URL. Git URLs may optionally end "
            "with @branch=..., @tag=..., or @commit=...."
        ),
    )
    parser.add_argument(
        "--output-dev-cfg",
        required=True,
        dest="target_dir",
        help="Directory where the dev copy should be created.",
    )
    parser.add_argument(
        "--repo-map-file",
        required=True,
        dest="repo_map_file",
        help=(
            "Path to a JSON file with stage-to-path mappings and optional tooling "
            "repo-to-path mappings. Matching tooling entries from source refs are "
            "written to local_repos.yaml as local repo paths."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remove the target directory first if it already exists.",
    )
    return parser.parse_args()


def load_path_map(path_map_file: str, *, map_label: str) -> dict[str, str]:
    repo_map_path = Path(path_map_file).expanduser()
    raw_json = repo_map_path.read_text(encoding="utf-8")
    data = json.loads(raw_json)
    if not isinstance(data, dict):
        raise ValueError(f"{map_label} must decode to a JSON object")

    repo_map: dict[str, str] = {}
    for stage, repo_path in data.items():
        if not isinstance(stage, str) or not isinstance(repo_path, str):
            raise ValueError(f"{map_label} must contain only string keys and values")

        normalized_path = Path(repo_path).expanduser()
        if not normalized_path.is_absolute():
            normalized_path = (Path.cwd() / normalized_path).resolve()

        repo_map[stage] = str(normalized_path)

    return repo_map


def load_repo_map(repo_map_file: str) -> dict[str, str]:
    return load_path_map(repo_map_file, map_label="repo_map_file")


def inventory_files(root_dir: Path) -> list[Path]:
    return sorted((root_dir / "inventory").glob("*.yaml"))


def workflow_files(root_dir: Path) -> list[Path]:
    return sorted((root_dir / "workflows").rglob("*.yaml"))


def refs_files(root_dir: Path) -> list[Path]:
    return sorted((root_dir / "refs").glob("*.yaml"))


def required_repo_entries(paths: list[Path]) -> set[str]:
    repo_entries: set[str] = set()

    for path in paths:
        inventory_cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(inventory_cfg, dict):
            continue

        stages_cfg = inventory_cfg.get("stages") or {}
        if not isinstance(stages_cfg, dict):
            continue

        for stage_name, stage_cfg in stages_cfg.items():
            if not isinstance(stage_name, str) or not isinstance(stage_cfg, dict):
                continue

            if isinstance(stage_cfg.get("repo_url"), str):
                repo_entries.add(stage_name)

            modules_cfg = stage_cfg.get("modules") or {}
            if not isinstance(modules_cfg, dict):
                continue

            for module_name, module_cfg in modules_cfg.items():
                if (
                    isinstance(module_name, str)
                    and isinstance(module_cfg, dict)
                    and isinstance(module_cfg.get("repo_url"), str)
                ):
                    repo_entries.add(module_name)

    return repo_entries


def required_tooling(paths: list[Path]) -> set[str]:
    tooling_names: set[str] = set()

    for path in paths:
        refs_cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(refs_cfg, dict):
            continue

        tooling_cfg = refs_cfg.get("tooling") or {}
        if not isinstance(tooling_cfg, dict):
            continue

        for tooling_name in tooling_cfg:
            if isinstance(tooling_name, str):
                tooling_names.add(tooling_name)

    return tooling_names


def parse_source_ref(source_cfg: str) -> tuple[str, str | None, str | None]:
    match = REF_SPEC_RE.match(source_cfg)
    if not match:
        return source_cfg, None, None

    return (
        match.group("source"),
        match.group("kind"),
        match.group("value"),
    )


def looks_like_http_git_source(source_cfg: str) -> bool:
    return source_cfg.startswith(("http://", "https://"))


def run_git(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(
        cmd,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def clone_source_cfg(
    source_cfg: str,
    ref_kind: str | None,
    ref_value: str | None,
    temp_root: Path,
) -> Path:
    clone_dir = temp_root / "source_cfg"

    if ref_kind in (None, "branch", "tag"):
        clone_cmd = ["git", "clone", "--depth", "1"]
        if ref_value is not None:
            clone_cmd.extend(["--branch", ref_value])
        clone_cmd.extend([source_cfg, str(clone_dir)])
        run_git(clone_cmd)
        return clone_dir

    run_git(["git", "clone", source_cfg, str(clone_dir)])
    run_git(["git", "checkout", ref_value], cwd=clone_dir)
    return clone_dir


def resolve_source_dir(source_cfg: str, stack: contextlib.ExitStack) -> Path:
    source_ref, ref_kind, ref_value = parse_source_ref(source_cfg)
    source_path = Path(source_ref).expanduser()

    if ref_kind is None and source_path.is_dir():
        return source_path.resolve()

    if ref_kind is None and source_path.exists() and not source_path.is_dir():
        raise ValueError(f"source_cfg exists but is not a directory: {source_path}")

    if source_path.exists() or looks_like_http_git_source(source_ref):
        temp_root = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="create-dev-cfg-")))
        return clone_source_cfg(source_ref, ref_kind, ref_value, temp_root)

    raise ValueError(
        "source_cfg must be an existing directory or an HTTP/HTTPS git URL "
        "(optionally with @branch=..., @tag=..., or @commit=...)"
    )


def validate_source_dir(source_dir: Path) -> None:
    if not source_dir.is_dir():
        raise ValueError(f"source cfg directory not found: {source_dir}")

    if not inventory_files(source_dir):
        raise ValueError(f"source cfg has no inventory YAML files: {source_dir}")


def validate_target_dir(source_dir: Path, target_dir: Path) -> None:
    if source_dir == target_dir:
        raise ValueError("target_dir must be different from the source cfg directory")

    if source_dir in target_dir.parents:
        raise ValueError("target_dir must not be inside the source cfg directory")


def copy_source_tree(source_dir: Path, target_dir: Path, force: bool) -> None:
    if target_dir.exists():
        if not force:
            raise FileExistsError(
                f"target directory already exists: {target_dir}. Use --force to replace it."
            )

        if not target_dir.is_dir():
            raise FileExistsError(f"target path exists and is not a directory: {target_dir}")

        shutil.rmtree(target_dir)

    shutil.copytree(
        source_dir,
        target_dir,
        ignore=shutil.ignore_patterns(".git", "__pycache__", "*.pyc"),
    )


def line_ending_for(line: str) -> str:
    if line.endswith("\r\n"):
        return "\r\n"
    if line.endswith("\n"):
        return "\n"
    return ""


def rewrite_inventory_file(path: Path, repo_map: dict[str, str]) -> int:
    inventory_cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(inventory_cfg, dict):
        raise ValueError(f"inventory YAML must contain a mapping: {path}")

    stages_cfg = inventory_cfg.get("stages") or {}
    if not isinstance(stages_cfg, dict):
        raise ValueError(f"inventory YAML must contain a 'stages' mapping: {path}")

    replacements = 0
    for stage_name, stage_cfg in stages_cfg.items():
        if not isinstance(stage_cfg, dict):
            continue

        if isinstance(stage_cfg.get("repo_url"), str):
            stage_cfg["repo_path"] = repo_map[stage_name]
            del stage_cfg["repo_url"]
            replacements += 1

        modules_cfg = stage_cfg.get("modules") or {}
        if not isinstance(modules_cfg, dict):
            continue

        for module_name, module_cfg in modules_cfg.items():
            if not isinstance(module_cfg, dict):
                continue
            if isinstance(module_cfg.get("repo_url"), str):
                module_cfg["repo_path"] = repo_map[module_name]
                del module_cfg["repo_url"]
                replacements += 1

    path.write_text(yaml.safe_dump(inventory_cfg, sort_keys=False), encoding="utf-8")
    return replacements


def rewrite_inventory(root_dir: Path, repo_map: dict[str, str]) -> list[Path]:
    rewritten_paths: list[Path] = []

    for inventory_path in inventory_files(root_dir):
        if rewrite_inventory_file(inventory_path, repo_map):
            rewritten_paths.append(inventory_path)

    return rewritten_paths


def strip_branch_from_workflow_file(path: Path) -> int:
    rewritten_lines: list[str] = []
    removals = 0

    for line in path.read_text(encoding="utf-8").splitlines(keepends=True):
        if WORKFLOW_BRANCH_LINE_RE.match(line):
            removals += 1
            continue

        rewritten_lines.append(line)

    path.write_text("".join(rewritten_lines), encoding="utf-8")
    return removals


def rewrite_workflows(root_dir: Path) -> list[Path]:
    rewritten_paths: list[Path] = []

    for workflow_path in workflow_files(root_dir):
        if strip_branch_from_workflow_file(workflow_path):
            rewritten_paths.append(workflow_path)

    return rewritten_paths


def write_local_tooling_file(root_dir: Path, tooling_map: dict[str, str]) -> Path | None:
    if not tooling_map:
        return None

    local_tooling_cfg = {
        "tooling": {
            tooling_name: {"repo_path": repo_path}
            for tooling_name, repo_path in tooling_map.items()
        }
    }
    local_tooling_path = root_dir / LOCAL_TOOLING_CFG_NAME
    local_tooling_path.write_text(
        yaml.safe_dump(local_tooling_cfg, sort_keys=False),
        encoding="utf-8",
    )
    return local_tooling_path


def remove_refs_dir(root_dir: Path) -> bool:
    refs_dir = root_dir / "refs"
    if not refs_dir.exists():
        return False

    shutil.rmtree(refs_dir)
    return True


def warn_about_extra_mappings(path_map: dict[str, str], required: set[str], label: str) -> None:
    extra_keys = sorted(set(path_map) - required)
    if not extra_keys:
        return

    print(
        f"warning: unused {label} mappings: " + ", ".join(extra_keys),
        file=sys.stderr,
    )


def main() -> int:
    args = parse_args()
    target_dir = Path(args.target_dir).expanduser().resolve()

    try:
        path_map = load_repo_map(args.repo_map_file)
        with contextlib.ExitStack() as stack:
            source_dir = resolve_source_dir(args.source_cfg, stack)
            validate_source_dir(source_dir)
            validate_target_dir(source_dir, target_dir)

            source_inventory = inventory_files(source_dir)
            source_refs = refs_files(source_dir)
            required_repo_names = required_repo_entries(source_inventory)
            required_tooling_names = required_tooling(source_refs)

            repo_map = {
                name: path
                for name, path in path_map.items()
                if name in required_repo_names
            }
            tooling_map = {
                name: path
                for name, path in path_map.items()
                if name in required_tooling_names
            }

            missing = sorted(required_repo_names - set(repo_map))
            if missing:
                raise ValueError(
                    "missing repo paths for inventory repos: " + ", ".join(missing)
                )

            warn_about_extra_mappings(repo_map, required_repo_names, "repo")
            warn_about_extra_mappings(tooling_map, required_tooling_names, "tooling")
            copy_source_tree(source_dir, target_dir, args.force)
            rewritten_paths = rewrite_inventory(target_dir, repo_map)
            rewritten_workflows = rewrite_workflows(target_dir)
            local_tooling_path = write_local_tooling_file(target_dir, tooling_map)
            removed_refs_dir = remove_refs_dir(target_dir)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else str(exc)
        print(f"error: git command failed: {stderr}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"source cfg: {source_dir}")
    print(f"created dev config: {target_dir}")
    for path in rewritten_paths:
        print(f"rewrote inventory: {path}")
    for path in rewritten_workflows:
        print(f"rewrote workflow: {path}")
    if local_tooling_path is not None:
        print(f"wrote local tooling config: {local_tooling_path}")
    if removed_refs_dir:
        print(f"removed refs dir: {target_dir / 'refs'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
