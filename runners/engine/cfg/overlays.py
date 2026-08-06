"""Selecting overlays and applying them to a cfg root.

An overlay merges without clearing, so a payload aimed at a path the base no
longer has lands as a new file that nothing reads. That failure is silent by
construction, which is why the checks here are static rather than per-run."""

import collections
import logging

from pathlib import Path

from engine.cfg import layout as cfg_layout
from engine.cfg import merge as cfg_merge
from engine.cfg import resources as cfg_resources
from engine.run import selectors as run_selectors

def normalize_overlay_name(raw_value, *, label: str) -> str:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise RuntimeError(f"{label} must be a non-empty string")
    value = raw_value.strip()
    if "/" in value or "\\" in value:
        raise RuntimeError(f"{label} must be a metadata name, not a path: {value}")
    if value in (".", ".."):
        raise RuntimeError(f"{label} is invalid: {value}")
    return value


def validate_overlay_data_tree(overlay_root: Path, *, meta_path: Path) -> None:
    """Reject overlay payloads that can change cfg topology or escape by symlink."""
    root_resolved = overlay_root.resolve()
    for path in sorted(root_resolved.rglob("*")):
        rel = path.relative_to(root_resolved)
        if ".git" in rel.parts:
            continue
        if path.is_symlink():
            raise RuntimeError(f"Overlay data must not contain symlinks: {path}")
        if path.name == cfg_layout.SCOPE_META_FILENAME and path.resolve() != meta_path.resolve():
            raise RuntimeError(
                f"Overlay data must not contain nested {cfg_layout.SCOPE_META_FILENAME}: {path}"
            )
        if path.name == cfg_layout.PLT_GUARDRAILS_FILENAME:
            raise RuntimeError(f"Overlay data must not contain {cfg_layout.PLT_GUARDRAILS_FILENAME}: {path}")


def load_overlay_candidate(
    plt_cfg_root: Path,
    meta_path: Path,
    meta_cfg: dict,
    execution_context: dict[str, object],
) -> dict:
    """Load one overlay metadata file."""
    cfg_root = plt_cfg_root.resolve()
    overlay_root = meta_path.parent.resolve()
    try:
        overlay_root.relative_to(cfg_root)
    except ValueError as exc:
        raise RuntimeError(f"Overlay metadata escapes plt cfg root: {meta_path}") from exc

    overlay_name = normalize_overlay_name(
        meta_cfg.get("name"),
        label=f"overlay name in {meta_path}",
    )
    selectors = meta_cfg.get("selectors") or {}
    matches = run_selectors.selector_matches(selectors, execution_context, label=str(meta_path))
    validate_overlay_data_tree(overlay_root, meta_path=meta_path)

    return {
        "name": overlay_name,
        "root": overlay_root,
        "meta_path": meta_path,
        "selectors": selectors,
        "matches": matches,
    }


def discover_overlay_candidates(
    plt_cfg_root: Path,
    *,
    execution_context: dict[str, object],
) -> dict[str, dict]:
    """Discover all type: overlay metadata entries by unique overlay name."""
    cfg_root = plt_cfg_root.resolve()
    candidates: dict[str, dict] = {}

    for meta_path in cfg_resources.discover_cfg_meta_paths(cfg_root):
        meta_cfg = cfg_resources.load_cfg_meta(meta_path)
        if meta_cfg["type"] == "scope":
            continue

        overlay = load_overlay_candidate(cfg_root, meta_path, meta_cfg, execution_context)
        previous = candidates.get(overlay["name"])
        if previous is not None:
            raise RuntimeError(
                f"Duplicate plt overlay name {overlay['name']!r}: {previous['meta_path']} and {meta_path}"
            )
        candidates[overlay["name"]] = overlay

    return candidates


def _overlay_leaf_values(overlay: dict) -> dict:
    return cfg_merge._scope_final_yaml_leaves(
        {"source_dirs": [str(overlay["root"])]},
        skip_filenames=cfg_layout.SCOPE_META_SKIP_FILENAMES,
    )


def resolve_target_plt_overlays(
    plt_cfg_root: Path,
    explicit_overlays: list[str],
    target_run: dict,
    *,
    execution_context: dict[str, object],
) -> list[str]:
    """The overlays ONE target_run is merged with.

    A target's `requires_plt_overlays` applies to that target and no other. The
    run-wide union it replaces meant an overlay declared by one target reshaped
    the cfg every other target received — `db_artificial_populator` alone touches
    `foundation`, `ecr_images_cfg`, `ecr_repos_cfg` and `workload_identity`, all
    of which other targets consume.
    """

    return resolve_run_plt_overlays(
        plt_cfg_root,
        explicit_overlays,
        {"target": target_run},
        execution_context=execution_context,
    )


def resolve_run_plt_overlays(
    plt_cfg_root: Path,
    explicit_overlays: list[str],
    active_target_runs: dict,
    *,
    execution_context: dict[str, object],
) -> list[str]:
    """

    append target-required overlays in target order and validate conflicts."""

    duplicates = [
        item
        for item, count in collections.Counter(explicit_overlays).items()
        if count > 1
    ]
    if duplicates:
        raise RuntimeError(
            "plt overlays must be unique; duplicates: " + ", ".join(sorted(duplicates))
        )

    final_overlays = list(explicit_overlays)
    automatically_appended: list[str] = []
    for target_run in active_target_runs.values():
        for overlay_name in target_run.get("requires_plt_overlays") or []:
            if overlay_name not in final_overlays:
                final_overlays.append(overlay_name)
                automatically_appended.append(overlay_name)

    if not final_overlays:
        return []

    candidates = discover_overlay_candidates(
        plt_cfg_root, execution_context=execution_context
    )
    for overlay_name in final_overlays:
        overlay = candidates.get(overlay_name)
        if overlay is None:
            available = ", ".join(sorted(candidates)) or "none"
            raise RuntimeError(
                f"Unknown plt overlay {overlay_name!r}; available overlays: {available}"
            )
        if not overlay["matches"]:
            raise RuntimeError(
                f"plt overlay {overlay_name!r} is not allowed for this execution context; "
                f"selectors={overlay['selectors']}"
            )

    leaf_cache: dict[str, dict] = {}
    for overlay_name in automatically_appended:
        overlay_index = final_overlays.index(overlay_name)
        current_leaves = leaf_cache.setdefault(
            overlay_name, _overlay_leaf_values(candidates[overlay_name])
        )
        for previous_name in final_overlays[:overlay_index]:
            previous_leaves = leaf_cache.setdefault(
                previous_name, _overlay_leaf_values(candidates[previous_name])
            )
            conflicts = sorted(
                leaf_key
                for leaf_key in current_leaves.keys() & previous_leaves.keys()
                if current_leaves[leaf_key] != previous_leaves[leaf_key]
            )
            if conflicts:
                rel_path, yaml_path = conflicts[0]
                rendered_path = ".".join(str(part) for part in yaml_path) or "<root>"
                raise RuntimeError(
                    f"plt overlay {overlay_name!r} conflicts with overlay "
                    f"{previous_name!r} at {rel_path}:{rendered_path}; two overlays a "
                    "target requires cannot set one leaf differently, because the "
                    "later one would win silently"
                )

    return final_overlays


def apply_selected_overlays_to_merged_cfg(
    plt_cfg_root: Path,
    plt_merged_dir: Path,
    plt_overlays: list[str],
    *,
    active_scopes: list[dict],
    execution_context: dict[str, object],
    merged_files: dict,
    source_log_roots: tuple[Path, ...],
    dest_log_roots: tuple[Path, ...],
    skip_filenames: set[str],
) -> None:
    """Merge each selected overlay over the COMPOSED cfg, in the order given.

    An overlay overwrites a leaf another declaration already set, which is what
    distinguishes it from a scope — scopes are refused when they conflict. That
    only means "switched on = applied" if the overlay lands LAST; merged into the
    source tree it would patch one preset level and could still lose to a
    narrower one downstream.

    An overlay mirrors SOURCE paths, so each of its files belongs to the scope
    whose root it sits under, and lands where that scope landed. That is what
    keeps an overlay able to carry env-specific values: `tech_jobs` pins a branch
    for `dev` and a commit for `prod`, and only the scope this run activated
    applies.
    """

    if not plt_overlays:
        return

    duplicates = [item for item, count in collections.Counter(plt_overlays).items() if count > 1]
    if duplicates:
        raise RuntimeError(f"plt overlays must be unique; duplicates: {', '.join(sorted(duplicates))}")

    cfg_root = plt_cfg_root.resolve()
    candidates = discover_overlay_candidates(plt_cfg_root, execution_context=execution_context)
    merged_root = Path(plt_merged_dir).resolve()

    for overlay_name in plt_overlays:
        overlay = candidates.get(overlay_name)
        if overlay is None:
            available = ", ".join(sorted(candidates)) or "none"
            raise RuntimeError(
                f"Unknown plt overlay {overlay_name!r}; available overlays: {available}"
            )
        if not overlay["matches"]:
            raise RuntimeError(
                f"plt overlay {overlay_name!r} is not allowed for this execution context; "
                f"selectors={overlay['selectors']}"
            )

        overlay_root = Path(overlay["root"]).resolve()
        applied_any = False
        for scope in active_scopes:
            scope_rel = Path(scope["scope_root"]).resolve().relative_to(cfg_root)
            overlay_scope_dir = overlay_root / scope_rel
            if not overlay_scope_dir.is_dir():
                continue
            target_dest = (merged_root / scope["target_path"].lstrip("/")).resolve()
            logging.info(
                "Applying plt overlay %s from %s to %s",
                overlay_name,
                overlay_scope_dir,
                target_dest,
            )
            cfg_merge.merge_config_dirs(
                source_dirs=[str(overlay_scope_dir)],
                dest_dir=str(target_dest),
                clear_dest=False,
                source_log_roots=source_log_roots,
                dest_log_roots=dest_log_roots,
                merged_files=merged_files,
                skip_filenames=skip_filenames,
            )
            applied_any = True

        if not applied_any:
            raise RuntimeError(
                f"plt overlay {overlay_name!r} matched no active cfg scope; its payload "
                "mirrors scope paths, so a payload under a scope this run does not "
                "activate would apply to nothing"
            )
