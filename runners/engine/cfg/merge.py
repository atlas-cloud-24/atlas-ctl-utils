"""Combining cfg trees, and the leaf values that result.

Last-wins by path, which is the rule the whole overlay mechanism depends on:
it is what makes an overlay a merge rather than a rewrite, and what lets
validation cover N overlays in N+2 passes instead of 2^N."""

import logging
import os
import shutil
import tempfile

from pathlib import Path

from engine.cfg import layout as cfg_layout
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io

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


def render_merged_cfg_header(
    dest_path: str | Path,
    sources: list[str],
    source_log_roots: tuple[Path, ...] = (),
    dest_log_roots: tuple[Path, ...] = (),
) -> str:
    rendered_dest = kernel_paths.format_path_for_log(dest_path, dest_log_roots)
    rendered_sources = [kernel_paths.format_path_for_log(src, source_log_roots) for src in sources]

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


def _deep_merge_refs(dst: dict, src: dict, yf: Path, path: str = "") -> None:
    """

    deep-merge a `refs` subtree across files; a duplicate leaf is a load error."""

    for k, v in src.items():
        cur = f"{path}.{k}" if path else str(k)
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge_refs(dst[k], v, yf, cur)
        elif k in dst:
            raise RuntimeError(f"❌ duplicate refs entry {cur!r}: {yf}")
        else:
            dst[k] = v


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
    """

    merge config directories in sequence using YAML-aware overlay semantics."""

    if clear_dest and os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)

    if merged_files is None:
        merged_files = {}

    for source_dir in source_dirs:
        for root, dirs, files in os.walk(source_dir):
            # scope-local baseline dirs are guard artifacts, never cfg payload
            dirs[:] = [d for d in dirs if d != cfg_layout.PLT_GUARDRAILS_DIRNAME]
            rel_root = os.path.relpath(root, source_dir)
            dest_root = os.path.join(dest_dir, rel_root) if rel_root != "." else dest_dir

            os.makedirs(dest_root, exist_ok=True)

            for file in files:
                if skip_filenames and file in skip_filenames:
                    continue
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_root, file)

                if os.path.exists(dest_file):
                    merged_data = merge_cfg_values(kernel_yaml_io.load_cfg_yaml(dest_file), kernel_yaml_io.load_cfg_yaml(src_file))
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
                    kernel_yaml_io.write_cfg_yaml(dest_file, merged_data, header_comment=header_comment)
                else:
                    shutil.copy2(src_file, dest_file)
                    merged_files[dest_file] = [src_file]

    for dest_path, sources in merged_files.items():
        if len(sources) > 1:
            rendered_sources = [kernel_paths.format_path_for_log(src, source_log_roots) for src in sources]
            rendered_dest = kernel_paths.format_path_for_log(dest_path, dest_log_roots)
            logging.info("Merged:")
            logging.info("  %s", rendered_sources[0])
            for src in rendered_sources[1:]:
                logging.info("  + %s", src)
            logging.info("  = %s", rendered_dest)

    return merged_files


def _flatten_yaml_leaf_values(value, path: tuple[object, ...] = ()) -> dict[tuple[object, ...], object]:
    if isinstance(value, dict):
        leaves: dict[tuple[object, ...], object] = {}
        for key, child in value.items():
            leaves.update(_flatten_yaml_leaf_values(child, path + (key,)))
        return leaves
    return {path: value}


def _scope_final_yaml_leaves(scope: dict, *, skip_filenames: set[str]) -> dict[tuple[str, tuple[object, ...]], object]:
    with tempfile.TemporaryDirectory(prefix="atlas-scope-leaves-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        merge_config_dirs(
            source_dirs=scope["source_dirs"],
            dest_dir=str(tmp_path),
            clear_dest=True,
            skip_filenames=skip_filenames,
        )
        leaves: dict[tuple[str, tuple[object, ...]], object] = {}
        for yaml_path in sorted(tmp_path.rglob("*.yaml")):
            rel_path = yaml_path.relative_to(tmp_path).as_posix()
            data = kernel_yaml_io.load_cfg_yaml(str(yaml_path))
            for leaf_path, leaf_value in _flatten_yaml_leaf_values(data).items():
                leaves[(rel_path, leaf_path)] = leaf_value
        return leaves
