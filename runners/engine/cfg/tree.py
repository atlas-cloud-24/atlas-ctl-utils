"""Composing the plt cfg tree a run renders against.

The whole tree is never merged when a run needs part of it: scopes are
discovered from what the active targets consume, and only those are merged and
rendered."""

import collections
import logging
import os
import shutil
import tempfile

from pathlib import Path
import yaml
from engine.cfg import presets as cfg_presets

from engine.cfg import layout as cfg_layout
from engine.cfg import materialize as cfg_materialize
from engine.cfg import merge as cfg_merge
from engine.cfg import overlays as cfg_overlays
from engine.cfg import resources as cfg_resources
from engine.cfg import validate as cfg_validate
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import selectors as run_selectors

def validate_cross_scope_leaf_conflicts(scopes: list[dict], *, target_path: str, skip_filenames: set[str]) -> None:
    """

    reject shared-target producers that define different final values for one YAML leaf."""

    if len(scopes) < 2:
        return
    owners: dict[tuple[str, tuple[object, ...]], tuple[object, dict]] = {}
    for scope in scopes:
        for leaf_key, leaf_value in cfg_merge._scope_final_yaml_leaves(scope, skip_filenames=skip_filenames).items():
            previous = owners.get(leaf_key)
            if previous is None:
                owners[leaf_key] = (leaf_value, scope)
                continue
            previous_value, previous_scope = previous
            if previous_value != leaf_value:
                rel_path, yaml_path = leaf_key
                rendered_path = ".".join(str(part) for part in yaml_path) or "<root>"
                raise RuntimeError(
                    f"❌ cross-scope cfg conflict for target_path {target_path!r} at "
                    f"{rel_path}:{rendered_path}: {previous_scope['scope_id']} and {scope['scope_id']} "
                    "produce different final values"
                )


def load_scope_composition(plt_cfg_root: Path) -> dict[str, dict]:
    path = plt_cfg_root / cfg_layout.SCOPE_COMPOSITION_FILENAME
    if not path.is_file():
        return {}
    data = kernel_yaml_io.load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ {cfg_layout.SCOPE_COMPOSITION_FILENAME} must contain a mapping: {path}")
    unknown = set(data) - {"scope_composition"}
    if unknown:
        raise RuntimeError(f"❌ {cfg_layout.SCOPE_COMPOSITION_FILENAME} has unsupported keys {sorted(unknown)}: {path}")
    raw_rules = data.get("scope_composition") or []
    if not isinstance(raw_rules, list):
        raise RuntimeError(f"❌ scope_composition must be a list: {path}")

    rules: dict[str, dict] = {}
    for index, raw_rule in enumerate(raw_rules):
        label = f"scope_composition[{index}] in {path}"
        if not isinstance(raw_rule, dict):
            raise RuntimeError(f"❌ {label} must be a mapping")
        unknown = set(raw_rule) - {"target_path", "scopes"}
        if unknown:
            raise RuntimeError(f"❌ {label} has unsupported keys {sorted(unknown)}")
        target_path = kernel_paths.normalize_cfg_absolute_path(raw_rule.get("target_path"), label=f"{label}.target_path")
        if target_path in rules:
            raise RuntimeError(f"❌ duplicate scope composition target_path {target_path!r}: {path}")
        raw_scopes = raw_rule.get("scopes") or []
        if not isinstance(raw_scopes, list) or not raw_scopes:
            raise RuntimeError(f"❌ {label}.scopes must be a non-empty list")
        prefixes = []
        for raw_scope in raw_scopes:
            prefix = kernel_paths.normalize_cfg_absolute_path(raw_scope, label=f"{label}.scopes")
            if prefix in prefixes:
                raise RuntimeError(f"❌ duplicate scope composition prefix {prefix!r}: {label}")
            prefixes.append(prefix)
        rules[target_path] = {
            "scopes": tuple(prefixes),
        }
    return rules


def load_scope_candidate(
    plt_cfg_root: Path,
    meta_path: Path,
    meta_cfg: dict,
    execution_context: dict[str, object],
) -> dict | None:
    """Load one scope __meta__.yaml and return an active merge scope, or None."""
    cfg_root = plt_cfg_root.resolve()
    scope_root = meta_path.parent.resolve()
    try:
        scope_root.relative_to(cfg_root)
    except ValueError as exc:
        raise RuntimeError(f"Scope metadata escapes plt cfg root: {meta_path}") from exc

    scope_rel = scope_root.relative_to(cfg_root).as_posix()
    scope_id = "/" + scope_rel if scope_rel != "." else "/"

    for legacy in ("scope_identity", "identity_selectors"):
        if legacy in meta_cfg:
            raise RuntimeError(
                f"scope {cfg_layout.SCOPE_META_FILENAME} must use selectors.match/selectors.in, not {legacy}: {meta_path}"
            )

    selectors = meta_cfg.get("selectors") or {}
    if not run_selectors.selector_matches(selectors, execution_context, label=str(meta_path), structured_only=True):
        return None

    nested = cfg_resources.find_nested_cfg_meta(scope_root, exclude=meta_path)
    if nested is not None:
        raise RuntimeError(f"❌ nested cfg metadata is not allowed under scope {scope_id}: {nested}")

    if "target_path" not in meta_cfg:
        raise RuntimeError(f"target_path is required in scope {cfg_layout.SCOPE_META_FILENAME}: {meta_path}")
    target_path = kernel_paths.normalize_cfg_absolute_path(
        meta_cfg["target_path"],
        label=f"target_path in {meta_path}",
        allow_root=False,
    )

    # Imports are declared in their own file, so a preset can import
    # without being a scope. A scope declaring them in __meta__.yaml is stale.
    if "imports" in meta_cfg:
        raise RuntimeError(
            f"❌ imports belong in {cfg_presets.IMPORTS_FILENAME}, not {cfg_layout.SCOPE_META_FILENAME}: {meta_path}"
        )
    if (scope_root / cfg_presets.PARAMS_FILENAME).exists():
        raise RuntimeError(
            f"❌ a scope is selected by selectors, not instantiated by an importer, so it cannot "
            f"declare {cfg_presets.PARAMS_FILENAME}: {scope_id}"
        )

    source_dirs: list[str] = []
    seen_imports: set[Path] = set()
    entries = cfg_presets.declared_imports(scope_root)
    workspace = cfg_materialize._fresh_materialization_workspace() if entries else None
    # One composition per scope: a preset reached by several paths must be
    # configured the same way each time
    composition: dict[str, tuple] = {}
    cfg_presets.assert_no_redundant_imports(cfg_root, scope_root, scope_id)
    for index, entry in enumerate(entries):
        import_path = kernel_paths.normalize_cfg_absolute_path(
            entry["from"],
            label=f"import path in {scope_id}",
            allow_root=False,
        )
        src = kernel_paths.cfg_abs_path_to_dir(cfg_root, import_path, label=f"import path in {scope_id}")
        if not src.exists():
            raise RuntimeError(f"Import path not found: {src}")
        if not src.is_dir():
            raise RuntimeError(f"Import path must be a directory: {src}")
        if not any(p.is_file() and ".git" not in p.relative_to(src).parts for p in src.rglob("*.yaml")):
            raise RuntimeError(f"Import path must contain at least one yaml cfg file: {src} ({scope_id})")
        cfg_validate.CfgTreeShape.reject_meta_inside_data_dir(src, import_path=import_path, meta_path=meta_path)
        if src == scope_root:
            raise RuntimeError(f"Scope imports itself in {scope_id}: {import_path}")
        seen_imports.add(src)

        materialized = workspace / f"{index:03d}"
        kernel_paths._MATERIALIZED_IMPORT_LABELS[materialized] = import_path
        cfg_presets.materialize(
            cfg_root,
            import_path,
            dest=materialized,
            bindings=entry["with"],
            composition=composition,
        )
        source_dirs.append(str(materialized))

    source_dirs.append(str(scope_root))
    return {
        "meta_path": meta_path,
        "scope_root": scope_root,
        "scope_path": scope_id,
        "scope_id": scope_id,
        "target_path": target_path,
        "selectors": selectors,
        "source_dirs": source_dirs,
    }


def discover_active_cfg_scopes(
    plt_cfg_root: Path,
    *,
    scope_params: dict[str, str],
    execution_context: dict[str, object] | None = None,
) -> list[dict]:
    """Discover active cfg merge scopes from type: scope metadata."""
    cfg_root = plt_cfg_root.resolve()
    runtime_context = execution_context or execution_run_context.execution_context_from_scope_params(scope_params)
    cfg_validate.CfgTreeShape.require_all_payload_reachable(cfg_root)
    cfg_materialize._release_materialized_imports()
    active_scopes: list[dict] = []

    for meta_path in cfg_resources.discover_cfg_meta_paths(cfg_root):
        meta_cfg = cfg_resources.load_cfg_meta(meta_path)
        if meta_cfg["type"] == "overlay":
            continue

        scope = load_scope_candidate(cfg_root, meta_path, meta_cfg, runtime_context)
        if scope is None:
            continue
        active_scopes.append(scope)

    if not active_scopes:
        raise RuntimeError(f"No active cfg scopes found under: {cfg_root}")

    run_selectors.validate_scope_composition(active_scopes, load_scope_composition(cfg_root))

    logging.info(
        "Active cfg scopes: %s",
        [f"{scope['scope_id']} -> {scope['target_path']}" for scope in active_scopes],
    )
    return active_scopes


def merge_plt_cfg_dirs(
    plt_cfg_root: Path,
    plt_merged_dir: Path,
    ctl_profile: str,
    plt_overlays: list[str] | None = None,
    scope_params: dict[str, str] | None = None,
    *,
    execution_context: dict[str, object] | None = None,
    source_log_roots: tuple[Path, ...] | None = None,
    dest_log_roots: tuple[Path, ...] | None = None,
    merged_files: dict[str, list[str]] | None = None,
) -> dict[str, list[str]]:
    """Build scoped merged cfg trees from typed __meta__.yaml metadata.

    Scope and overlay activation both use the uniform selectors.match/selectors.in
    execution-context selector model.
/61: a scope declares its own CONDITION
    (`selectors.contains: {execution_context.target.domains: <domain>}`), so it
    activates iff the run reads its domain. `target_path` stays purely the
    DESTINATION. The former `required_target_paths` filter did the same job from
    the other side and was removed — two mechanisms deciding one thing can
    disagree."""

    if plt_merged_dir.exists():
        shutil.rmtree(plt_merged_dir)
    os.makedirs(plt_merged_dir, exist_ok=True)

    if dest_log_roots is None:
        dest_log_roots = (plt_merged_dir.resolve(),)
    if merged_files is None:
        merged_files = {}

    selected_overlays = plt_overlays or []
    runtime_selectors = scope_params or {}
    composition_files = set(cfg_layout.SCOPE_META_SKIP_FILENAMES)

    def merge_scopes(effective_cfg_root: Path, effective_source_log_roots: tuple[Path, ...]) -> None:
        active_scopes = discover_active_cfg_scopes(
            effective_cfg_root,
            scope_params=runtime_selectors,
            execution_context=execution_context,
        )
        scopes_by_target: dict[str, list[dict]] = collections.defaultdict(list)
        for scope in active_scopes:
            scopes_by_target[scope["target_path"]].append(scope)
        for target_path, scopes in scopes_by_target.items():
            validate_cross_scope_leaf_conflicts(
                scopes,
                target_path=target_path,
                skip_filenames=composition_files,
            )

        merged_target_paths: set[str] = set()

        for scope in active_scopes:
            target_path = scope["target_path"]
            target_rel = target_path.lstrip("/")
            target_dest = (plt_merged_dir / target_rel).resolve()
            try:
                target_dest.relative_to(plt_merged_dir.resolve())
            except ValueError as exc:
                raise RuntimeError(f"Scope target_path escapes merged cfg dir: {target_path}") from exc

            logging.info(
                "Merging cfg scope %s to %s",
                scope["scope_path"],
                target_dest,
            )
            cfg_merge.merge_config_dirs(
                source_dirs=scope["source_dirs"],
                dest_dir=str(target_dest),
                clear_dest=target_path not in merged_target_paths,
                source_log_roots=effective_source_log_roots,
                dest_log_roots=dest_log_roots,
                merged_files=merged_files,
                skip_filenames=composition_files,
            )
            merged_target_paths.add(target_path)

    if selected_overlays:
        with tempfile.TemporaryDirectory(prefix="atlas-plt-cfg-") as tmp_dir:
            effective_cfg_root = Path(tmp_dir) / "source"
            cfg_overlays.copy_cfg_root_without_overlay_catalog(plt_cfg_root, effective_cfg_root)
            if execution_context is None:
                raise RuntimeError("❌ plt overlays require the execution context for selector gating")
            cfg_overlays.apply_selected_overlays_to_cfg_root(
                plt_cfg_root,
                effective_cfg_root,
                selected_overlays,
                execution_context=execution_context,
            )
            effective_source_log_roots = source_log_roots or (
                effective_cfg_root.resolve(),
                plt_cfg_root.resolve(),
            )
            merge_scopes(effective_cfg_root, effective_source_log_roots)
    else:
        effective_source_log_roots = source_log_roots or (plt_cfg_root.resolve(),)
        merge_scopes(plt_cfg_root, effective_source_log_roots)

    return merged_files


def render_scope_tree(scope_dir: Path, dest_dir: Path, env_ctx: dict) -> None:
    """

    render one scope: merge all scope YAML for lookups, interpolate,
    normalize cfg-entry refs whole-scope, write back per-file YAML, copy
    non-YAML verbatim. Engine logic (folded from the former target_run-side
    render_cfg.py)."""

    brc = cfg_materialize._step_utils_module("build_runtime_cfg")
    yaml_files = sorted(p for p in scope_dir.rglob("*.yaml") if p.is_file())
    scope_merged: dict = {}
    for path in yaml_files:
        doc = brc.load_yaml_mapping(path)
        if execution_references.EXECUTION_CONTEXT_ROOT in doc:
            raise RuntimeError(
                f"❌ plt payload must not define reserved top-level key "
                f"{execution_references.EXECUTION_CONTEXT_ROOT!r}: {path}"
            )
        scope_merged = brc.merge_values(scope_merged, doc)

    resolver = brc.Resolver(scope_merged, env_ctx)
    scope_resolved: dict = {}
    for key in scope_merged:
        value = resolver.lookup(key)
        if value is brc.OMIT:
            continue
        scope_resolved[key] = value
    scope_resolved = brc.resolve_cfg_entry_refs(scope_resolved)

    for path in sorted(p for p in scope_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(scope_dir)
        dest = dest_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix != ".yaml":
            shutil.copy2(path, dest)
            continue
        doc = brc.load_yaml_mapping(path)
        rendered = resolver.resolve_value(doc)
        rendered = brc.resolve_cfg_entry_refs(rendered, lookup_root=scope_resolved)
        dest.write_text(
            yaml.safe_dump(rendered, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )


def render_plt_cfg(
    plt_merged_dir: Path, dest_dir: Path, execution_context: dict[str, object]
) -> Path:
    """Render merged/ into <dest_dir>/rendered (whole-scope). In-process engine
    step — no subprocess, no target_run costume. `dest_dir` is the
    TARGET's own cfg dir, so nothing is rendered once and shared."""
    plt_rendered_dir = dest_dir / "rendered"
    if plt_rendered_dir.exists():
        shutil.rmtree(plt_rendered_dir)
    plt_rendered_dir.mkdir(parents=True)
    env_ctx = dict(execution_context)
    for entry in sorted(plt_merged_dir.iterdir()):
        if entry.is_dir():
            render_scope_tree(entry, plt_rendered_dir / entry.name, env_ctx)
        else:
            shutil.copy2(entry, plt_rendered_dir / entry.name)
    logging.info("Rendered plt cfg: %s", plt_rendered_dir)
    return plt_rendered_dir
