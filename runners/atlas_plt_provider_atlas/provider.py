"""Materialize one Atlas-native scope into universal values."""

from __future__ import annotations

import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path

import yaml
from engine.cfg import presets as cfg_presets
from engine.cfg import tree as cfg_tree


class AtlasProvider:
    """Own Atlas-native merge and reference resolution for one selected scope."""

    name = "atlas"

    @staticmethod
    def _unit(selected_units: Sequence[Mapping[str, object]]) -> dict[str, object]:
        if (
            not isinstance(selected_units, Sequence)
            or isinstance(selected_units, (str, bytes))
            or len(selected_units) != 1
        ):
            raise RuntimeError("❌ Atlas provider requires exactly one selected scope")
        unit = selected_units[0]
        if not isinstance(unit, Mapping):
            raise RuntimeError("❌ selected Atlas scope must be a mapping")
        expected = {"relative_path", "target_path", "type"}
        if set(unit) != expected or unit.get("type") not in {
            "scope",
            "shared_scope",
        }:
            raise RuntimeError(
                f"❌ selected Atlas unit must define exactly {sorted(expected)} "
                "and type scope or shared_scope"
            )
        return dict(unit)

    @staticmethod
    def _merge(destination: dict, incoming: Mapping) -> dict:
        merged = dict(destination)
        for key, value in incoming.items():
            if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
                merged[key] = AtlasProvider._merge(dict(merged[key]), value)
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _payload_files(scope_root: Path) -> list[Path]:
        declarations = set(cfg_presets.DECLARATION_FILENAMES) | {"__meta__.yaml"}
        return [
            path
            for path in sorted(scope_root.rglob("*.yaml"))
            if path.name not in declarations and ".git" not in path.relative_to(scope_root).parts
        ]

    def materialize(
        self,
        *,
        plt_cfg_root: Path,
        selected_units: Sequence[Mapping[str, object]],
        execution_context: Mapping[str, object],
        workspace: Path,
        imported_values: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        unit = self._unit(selected_units)
        plt_cfg_root = Path(plt_cfg_root).resolve()
        scope_root = (plt_cfg_root / str(unit["relative_path"])).resolve()
        try:
            scope_root.relative_to(plt_cfg_root)
        except ValueError as exc:
            raise RuntimeError("❌ selected Atlas scope escapes the PLT cfg root") from exc
        if not scope_root.is_dir():
            raise RuntimeError(f"❌ selected Atlas scope does not exist: {scope_root}")

        workspace = Path(workspace).resolve()
        if workspace.exists():
            raise RuntimeError(f"❌ Atlas workspace already exists: {workspace}")
        merged_dir = workspace / "merged"
        rendered_dir = workspace / "rendered"
        merged_dir.mkdir(parents=True)

        imported = dict(imported_values or {})
        if imported:
            (merged_dir / "000-imported.yaml").write_text(
                yaml.safe_dump(imported, sort_keys=False), encoding="utf-8"
            )
        for source in self._payload_files(scope_root):
            destination = merged_dir / source.relative_to(scope_root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

        if not any(merged_dir.rglob("*.yaml")):
            raise RuntimeError(
                f"❌ selected Atlas scope contains no universal values: {scope_root}"
            )
        cfg_tree.render_scope_tree(merged_dir, rendered_dir, dict(execution_context))

        values: dict = {}
        for path in sorted(rendered_dir.rglob("*.yaml")):
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if not isinstance(document, Mapping):
                raise RuntimeError(f"❌ Atlas scope payload must be a mapping: {path}")
            values = self._merge(values, document)
        if not values:
            raise RuntimeError("❌ Atlas provider resolved an empty scope")
        return {
            "resolved_cfg": [{"target_path": unit["target_path"], "values": values}],
            "workspace": str(workspace),
        }


PROVIDER = AtlasProvider()
