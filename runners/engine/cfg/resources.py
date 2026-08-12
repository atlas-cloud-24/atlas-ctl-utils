"""Reading a named collection out of a cfg tree.

The engine finds a collection by its content key and the tree's metadata, never
by looking for a filename it was told to expect. That is what lets a consumer
lay its cfg out however it likes."""

from pathlib import Path

from engine.cfg import layout as cfg_layout
from engine.cfg import plt_provider as cfg_plt_provider
from engine.execution import references as execution_references
from engine.kernel import yaml_io as kernel_yaml_io

# After the Phase 2d cutover, targets / workflows / refs are all content-key
# resources (identified by their top-level key, not by a dir), so nothing is skipped.
# `_inputs/` holds DATA a consumer reads through a declared input source — not
# cfg. It is ignored outright: never merged as a content-key resource, never
# validated as cfg, and reachable only through the locator its consumer declares.
# Several files in it may carry the same keys (one per landing zone, say), which
# A content-key merge would treat as a collision.
_IGNORED_CFG_DIRS = ("_inputs",)


def collect_resource(ctl_cfg_root: Path, key: str, *, entry_depth: int = 1) -> dict:
    """Merge a top-level resource map identified by `key` across every cfg file.

    A resource's type is its top-level YAML key (content-key), not its filename: a
    file with a `cfg_key_sets:` key contributes cfg-key-sets wherever it lives. The maps are
    unioned across all `*.yaml` under `ctl_cfg_root`; a duplicate entry is a load
    error (same rule as targets), order-independent. `entry_depth` is how deep the
    unique entries sit: 1 for flat catalogs (target_sources/cfg_key_sets),
    2 for action-keyed collections, 3 for `workflows.<action>.<scope>.<name>` and
    `providers.<name>.<section>.<entry>`.
    Intermediate levels merge; the entry level collides. Dir-routed trees (see
    `_IGNORED_CFG_DIRS`) are skipped — they have dedicated loaders.
    """
    merged: dict = {}
    origin: dict = {}

    def _merge(dst: dict, src: dict, prefix: str, yf: Path) -> None:
        for name, val in src.items():
            path = f"{prefix}.{name}" if prefix else str(name)
            depth = path.count(".") + 1
            if depth < entry_depth:
                if not isinstance(val, dict):
                    raise RuntimeError(f"❌ {key} {path!r} must be a mapping: {yf}")
                node = dst.setdefault(name, {})
                if not isinstance(node, dict):
                    raise RuntimeError(f"❌ {key} {path!r} must be a mapping: {yf}")
                _merge(node, val, path, yf)
            else:
                if name in dst:
                    raise RuntimeError(
                        f"❌ duplicate {key} entry {path!r}: {yf} (also defined in {origin[path]})"
                    )
                dst[name] = val
                origin[path] = yf

    for yf in sorted(ctl_cfg_root.rglob("*.yaml")):
        rel = yf.relative_to(ctl_cfg_root)
        if any(part in _IGNORED_CFG_DIRS for part in rel.parts):
            continue
        data = kernel_yaml_io.load_yaml(yf) or {}
        if not isinstance(data, dict):
            continue
        section = data.get(key)
        if section is None:
            continue
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ '{key}' must be a mapping: {yf}")
        _merge(merged, section, "", yf)

    return merged


def _flatten_sourced_payload(payload, *, prefix: str, label: str) -> dict[str, object]:
    """Flatten a sourced payload into dotted scalar leaves.

    A cfg reference resolves to a SCALAR, so a nested payload is published leaf by
    leaf: `accounts_registry.dev.account_id`, never one map under one key.
    """

    if isinstance(payload, dict):
        flat: dict[str, object] = {}
        for key, value in payload.items():
            if not isinstance(key, str) or not execution_references.CONTEXT_KEY_RE.fullmatch(key):
                raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
            flat.update(_flatten_sourced_payload(value, prefix=f"{prefix}.{key}", label=label))
        return flat
    if isinstance(payload, (str, int, float, bool)):
        return {prefix: payload}
    raise RuntimeError(
        f"❌ {label}: {prefix} must resolve to nested mappings of scalars, "
        f"got {type(payload).__name__}"
    )


def discover_cfg_meta_paths(plt_cfg_root: Path) -> list[Path]:
    """Find cfg metadata files, excluding git internals."""
    cfg_root = plt_cfg_root.resolve()
    meta_paths: list[Path] = []
    for meta_path in sorted(cfg_root.rglob(cfg_layout.SCOPE_META_FILENAME)):
        rel = meta_path.relative_to(cfg_root)
        if ".git" in rel.parts:
            continue
        meta_paths.append(meta_path)
    return meta_paths


def load_cfg_meta(meta_path: Path) -> dict:
    """Load typed cfg metadata from __meta__.yaml."""
    meta_cfg = kernel_yaml_io.load_yaml(meta_path) or {}
    if not isinstance(meta_cfg, dict):
        raise RuntimeError(f"{cfg_layout.SCOPE_META_FILENAME} must contain a mapping: {meta_path}")

    meta_type = meta_cfg.get("type")
    if meta_type not in ("scope", "shared_scope", "overlay"):
        raise RuntimeError(
            f"{cfg_layout.SCOPE_META_FILENAME} type must be 'scope', "
            f"'shared_scope', or 'overlay': {meta_path}"
        )
    binding = cfg_plt_provider.ProviderBinding.selectable_unit(meta_cfg, label=str(meta_path))
    if binding:
        meta_cfg["plt"] = binding
    else:
        meta_cfg.pop("plt", None)
    return meta_cfg


def find_nested_cfg_meta(root: Path, *, exclude: Path | None = None) -> Path | None:
    """Return a nested metadata file under root, ignoring an optional root meta."""
    root_resolved = root.resolve()
    exclude_resolved = exclude.resolve() if exclude is not None else None
    for meta_path in sorted(root_resolved.rglob(cfg_layout.SCOPE_META_FILENAME)):
        if exclude_resolved is not None and meta_path.resolve() == exclude_resolved:
            continue
        rel = meta_path.relative_to(root_resolved)
        if ".git" in rel.parts:
            continue
        return meta_path
    return None
