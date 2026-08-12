"""The per-target slice of cfg a step actually sees.

A step is given what its signature declares and nothing else, so a step cannot
quietly start depending on a key it never asked for."""

from pathlib import Path

from engine.cfg import tree as cfg_tree
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io


def attach_target_cfg_view_facts(active_target_runs: dict, plt_targets_dir: Path) -> None:
    for target_run_id, target_run in active_target_runs.items():
        target_view = (
            Path(plt_targets_dir)
            if (Path(plt_targets_dir) / "input").is_dir()
            or (Path(plt_targets_dir) / "selection.yaml").is_file()
            else Path(plt_targets_dir) / target_run_id
        )
        hashed_view = target_view / "input"
        target_run["target_cfg_view_sha256"] = kernel_paths.directory_content_sha256(hashed_view)


def finalize_target_cfg_view_facts(
    active_target_runs: dict,
    plt_targets_dir: Path,
    pipeline_run_cfg_path: Path,
) -> None:
    """Attach cfg-view hashes and refresh the resolved target-run artifact."""
    attach_target_cfg_view_facts(active_target_runs, plt_targets_dir)
    pipeline_cfg = kernel_yaml_io.load_yaml(pipeline_run_cfg_path) or {}
    pipeline_cfg["target_runs"] = active_target_runs
    kernel_yaml_io.write_yaml_file(pipeline_run_cfg_path, pipeline_cfg)


def target_cfg_views_root(run_dir: Path, run_type: str) -> Path:
    """Where a run keeps its per-target cfg derivations.

    A TARGET run has exactly one target, so it writes straight to `cfg/plt` —
    nesting it under `targets/<key>/` would repeat, in a path, what the whole run
    already is. A WORKFLOW pre-checks many, so it keeps them apart under
    `cfg/plt/targets/<key>/` (and drops the tree once each child owns its copy).
    """

    base = run_dir / "cfg" / "plt"
    return base if run_type == "target" else base / "targets"


def target_cfg_view_dir(run_dir: Path, run_type: str, target_run_id: str) -> Path:
    root = target_cfg_views_root(run_dir, run_type)
    return root if run_type == "target" else root / target_run_id


def prepare_target_cfg_view(
    target_run_id: str,
    target_run: dict,
    *,
    plt_cfg_root: Path,
    target_cfg_dir: Path,
    ctl_profile: str,
    scope_params: dict[str, str] | None,
    execution_context: dict[str, object],
) -> Path:
    """Merge, render and project ONE target's cfg, under its own dir.

    The workflow authors ordering; a target derives its own cfg. Nothing is shared
    between targets, so a target runs standalone exactly as it runs in a workflow,
    and its provenance is its own rather than a slice of a run-wide tree.
    """
    target_dir = target_cfg_dir
    merged_dir = target_dir / "merged"
    cfg_tree.merge_plt_cfg_dirs(
        plt_cfg_root=plt_cfg_root,
        plt_merged_dir=merged_dir,
        ctl_profile=ctl_profile,
        plt_overlays=list(target_run.get("plt_overlays") or []),
        scope_params=scope_params,
        execution_context=execution_context,
        source_log_roots=(plt_cfg_root.resolve(),),
        dest_log_roots=(target_dir.resolve(),),
    )
    return cfg_tree.render_plt_cfg(merged_dir, target_dir, execution_context)
