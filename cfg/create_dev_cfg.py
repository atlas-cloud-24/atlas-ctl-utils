#!/usr/bin/env python3
"""Create a local dev copy of a ctl cfg directory.

The source cfg can be either:
- a local directory
- an HTTP/HTTPS git URL, optionally with `@branch=...`, `@tag=...`, or `@commit=...`

The script copies the source cfg into a target directory, rewrites
`inventory/*.yaml` so each stage uses `repo_path` instead of `repo_url`,
and removes ineffective `branch` keys from workflow YAMLs.

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


REF_SPEC_RE = re.compile(r"^(?P<source>.+)@(?P<kind>branch|tag|commit)=(?P<value>.+)$")
STAGE_LINE_RE = re.compile(r"^  (?P<stage>[^:\n]+):\s*$")
REPO_URL_LINE_RE = re.compile(r"^    repo_url:\s*.+$")
WORKFLOW_BRANCH_LINE_RE = re.compile(r"^\s+branch:\s*.+$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy a ctl cfg directory into a target directory and rewrite "
            "inventory repo_url values to repo_path using a JSON file with "
            "stage-to-path mappings."
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
        help="Path to a JSON file with stage-to-path mappings.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remove the target directory first if it already exists.",
    )
    return parser.parse_args()


def load_repo_map(repo_map_file: str) -> dict[str, str]:
    repo_map_path = Path(repo_map_file).expanduser()
    raw_json = repo_map_path.read_text(encoding="utf-8")
    data = json.loads(raw_json)
    if not isinstance(data, dict):
        raise ValueError("repo_map_file must decode to a JSON object")

    repo_map: dict[str, str] = {}
    for stage, repo_path in data.items():
        if not isinstance(stage, str) or not isinstance(repo_path, str):
            raise ValueError("repo_map_file must contain only string keys and values")

        normalized_path = Path(repo_path).expanduser()
        if not normalized_path.is_absolute():
            normalized_path = (Path.cwd() / normalized_path).resolve()

        repo_map[stage] = str(normalized_path)

    return repo_map


def inventory_files(root_dir: Path) -> list[Path]:
    return sorted((root_dir / "inventory").glob("*.yaml"))


def workflow_files(root_dir: Path) -> list[Path]:
    return sorted((root_dir / "workflows").rglob("*.yaml"))


def required_stages(paths: list[Path]) -> set[str]:
    stages: set[str] = set()

    for path in paths:
        current_stage: str | None = None
        for line in path.read_text(encoding="utf-8").splitlines():
            stage_match = STAGE_LINE_RE.match(line)
            if stage_match:
                current_stage = stage_match.group("stage")
                continue

            if current_stage and REPO_URL_LINE_RE.match(line):
                stages.add(current_stage)

    return stages


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
    rewritten_lines: list[str] = []
    current_stage: str | None = None
    replacements = 0

    for line in path.read_text(encoding="utf-8").splitlines(keepends=True):
        stage_match = STAGE_LINE_RE.match(line)
        if stage_match:
            current_stage = stage_match.group("stage")
            rewritten_lines.append(line)
            continue

        if current_stage and REPO_URL_LINE_RE.match(line):
            rewritten_lines.append(
                f"    repo_path: {repo_map[current_stage]}{line_ending_for(line)}"
            )
            replacements += 1
            continue

        rewritten_lines.append(line)

    path.write_text("".join(rewritten_lines), encoding="utf-8")
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


def warn_about_extra_stages(repo_map: dict[str, str], required: set[str]) -> None:
    extra_stages = sorted(set(repo_map) - required)
    if not extra_stages:
        return

    print(
        "warning: unused stage mappings: " + ", ".join(extra_stages),
        file=sys.stderr,
    )


def main() -> int:
    args = parse_args()
    target_dir = Path(args.target_dir).expanduser().resolve()

    try:
        repo_map = load_repo_map(args.repo_map_file)
        with contextlib.ExitStack() as stack:
            source_dir = resolve_source_dir(args.source_cfg, stack)
            validate_source_dir(source_dir)
            validate_target_dir(source_dir, target_dir)

            source_inventory = inventory_files(source_dir)
            required = required_stages(source_inventory)
            missing = sorted(required - set(repo_map))
            if missing:
                raise ValueError(
                    "missing repo paths for stages: " + ", ".join(missing)
                )

            warn_about_extra_stages(repo_map, required)
            copy_source_tree(source_dir, target_dir, args.force)
            rewritten_paths = rewrite_inventory(target_dir, repo_map)
            rewritten_workflows = rewrite_workflows(target_dir)
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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
