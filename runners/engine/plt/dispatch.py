"""Resolve PLT-provider contracts for selected cfg scopes."""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from pathlib import Path

from engine.cfg import materialize as cfg_materialize
from engine.cfg import overlays as cfg_overlays
from engine.cfg import presets as cfg_presets
from engine.cfg import resources as cfg_resources
from engine.cfg import tree as cfg_tree
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.plt import providers as plt_providers


class ProviderDispatch:
    def __init__(self, ctl_cfg_root: Path, plt_cfg_root: Path):
        self.plt_cfg_root = Path(plt_cfg_root).resolve()
        self.registry = plt_providers.ProviderRegistry(ctl_cfg_root)
        provider = cfg_materialize.load_cfg_sources(ctl_cfg_root)["plt"].get("provider")
        self.provider = str(provider) if provider is not None else None
        if self.registry.entries:
            self.registry.activate_local_adapters()
            self.registry.validate_declared_contracts()

    @property
    def enabled(self) -> bool:
        return self.provider is None or self.provider in self.registry.entries

    def _unit(self, scope):
        binding = dict(scope.get("plt") or {})
        unit = {
            "type": scope["type"],
            "relative_path": Path(scope["scope_root"]).relative_to(self.plt_cfg_root).as_posix(),
            "target_path": scope["target_path"],
        }
        for key in ("provider", "provider_cfg"):
            if key in binding:
                unit[key] = binding[key]
        return unit

    def _scope_imports(self, unit):
        dependencies = []
        root = self.plt_cfg_root / unit["relative_path"]
        for entry in cfg_presets.declared_imports(root):
            source = entry["from"].lstrip("/")
            if not (self.plt_cfg_root / source / "__meta__.yaml").is_file():
                continue
            if entry["with"]:
                raise RuntimeError(f"❌ scope import {entry['from']!r} must not declare with")
            dependencies.append({"from": source, "import": entry["import"], "as": entry["as"]})
        return dependencies

    def _imported_unit(self, relative_path, execution_context):
        meta_path = self.plt_cfg_root / relative_path / "__meta__.yaml"
        meta_cfg = cfg_resources.load_cfg_meta(meta_path)
        if meta_cfg["type"] != "shared_scope":
            raise RuntimeError(
                f"❌ cross-scope import must reference type shared_scope: /{relative_path}"
            )
        scope = cfg_tree.load_scope_selection(
            self.plt_cfg_root, meta_path, meta_cfg, execution_context
        )
        if scope is None:
            raise RuntimeError(f"❌ imported shared scope is unavailable: /{relative_path}")
        if scope["selectors"]:
            raise RuntimeError(f"❌ shared scope must omit selectors: /{relative_path}")
        return self._unit(scope)

    def select(self, target_run, execution_context, *, scope_params):
        scopes = cfg_tree.discover_active_scope_selections(
            self.plt_cfg_root,
            scope_params=scope_params,
            execution_context=execution_context,
        )
        units = [self._unit(scope) for scope in scopes]
        overlay_names = list(target_run.get("plt_overlays") or [])
        if self.provider is None and overlay_names:
            raise RuntimeError("❌ per-scope PLT providers require provider-owned overlays")
        if overlay_names:
            candidates = cfg_overlays.discover_overlay_candidates(
                self.plt_cfg_root, execution_context=execution_context
            )
            for name in overlay_names:
                overlay = candidates.get(str(name))
                if overlay is None or not overlay["matches"]:
                    raise RuntimeError(f"❌ selected PLT overlay {name!r} is unavailable")
                binding = dict(overlay.get("plt") or {})
                unit = {
                    "type": "overlay",
                    "name": overlay["name"],
                    "relative_path": Path(overlay["root"])
                    .relative_to(self.plt_cfg_root)
                    .as_posix(),
                }
                if "provider_cfg" in binding:
                    unit["provider_cfg"] = binding["provider_cfg"]
                units.append(unit)
        if self.provider is not None:
            self.registry.adapter(self.provider)
            return {"provider": self.provider, "selected_units": units}

        providers = set()
        units_by_path = {unit["relative_path"]: unit for unit in units}
        cursor = 0
        while cursor < len(units):
            unit = units[cursor]
            cursor += 1
            provider = unit.get("provider")
            if not isinstance(provider, str) or not provider:
                raise RuntimeError(f"❌ scope {unit['relative_path']!r} must declare plt.provider")
            self.registry.adapter(provider)
            providers.add(provider)
            unit["imports"] = self._scope_imports(unit)
            for dependency in unit["imports"]:
                source = dependency["from"]
                if source in units_by_path:
                    continue
                imported = self._imported_unit(source, execution_context)
                units_by_path[source] = imported
                units.append(imported)
        return {"providers": sorted(providers), "selected_units": units}

    @staticmethod
    def _project(values, selection, source):
        if selection == "*":
            return dict(values)
        projected = {}
        for key in selection:
            if key not in values:
                raise RuntimeError(f"❌ imported key {key!r} is not produced by scope {source!r}")
            projected[key] = values[key]
        return projected

    @classmethod
    def _merge(cls, destination, incoming, *, source, prefix=()):
        for key, value in incoming.items():
            path = (*prefix, str(key))
            if key not in destination:
                destination[key] = value
            elif isinstance(destination[key], Mapping) and isinstance(value, Mapping):
                child = dict(destination[key])
                destination[key] = child
                cls._merge(child, value, source=source, prefix=path)
            elif destination[key] != value:
                dotted = ".".join(path)
                raise RuntimeError(
                    f"❌ conflicting imported scope value {dotted!r} from {source!r}"
                )

    @staticmethod
    def _validate(provider, value):
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("resolved_cfg"), list)
            or not value["resolved_cfg"]
        ):
            raise RuntimeError(f"❌ PLT provider {provider!r} must return non-empty resolved_cfg")
        return value

    @classmethod
    def _values(cls, unit, materialized):
        values = {}
        for entry in materialized["resolved_cfg"]:
            if (
                not isinstance(entry, Mapping)
                or entry.get("target_path") != unit["target_path"]
                or not isinstance(entry.get("values"), Mapping)
            ):
                raise RuntimeError(
                    f"❌ provider output for scope {unit['relative_path']!r} "
                    "must resolve its declared target_path"
                )
            cls._merge(values, entry["values"], source=unit["relative_path"])
        return values

    def _materialize_graph(self, selection, *, execution_context, workspace):
        units = {unit["relative_path"]: dict(unit) for unit in selection["selected_units"]}
        resolved = {}
        output = []
        visiting = []

        def run(path):
            if path in resolved:
                return
            if path in visiting:
                chain = " -> ".join((*visiting, path))
                raise RuntimeError(f"❌ PLT scope import cycle: {chain}")
            visiting.append(path)
            unit = units[path]
            imported = {}
            for dependency in unit.get("imports") or []:
                source = dependency["from"]
                run(source)
                projected = self._project(resolved[source], dependency["import"], source)
                incoming = {dependency["as"]: projected} if dependency.get("as") else projected
                self._merge(imported, incoming, source=source)
            provider = unit["provider"]
            provider_unit = {
                key: value for key, value in unit.items() if key not in {"provider", "imports"}
            }
            plt_adapter = self.registry.adapter(provider)
            materialized = self._validate(
                provider,
                plt_adapter.materialize(
                    plt_cfg_root=self.plt_cfg_root,
                    selected_units=[provider_unit],
                    execution_context=execution_context,
                    workspace=workspace / path,
                    imported_values=imported,
                ),
            )
            resolved[path] = self._values(unit, materialized)
            output.extend(materialized["resolved_cfg"])
            visiting.pop()

        for path in units:
            run(path)
        return {"resolved_cfg": output}

    def materialize_scope(self, selection, *, execution_context, workspace):
        provider = selection.get("provider")
        if provider is None:
            return self._materialize_graph(
                selection, execution_context=execution_context, workspace=workspace
            )
        plt_adapter = self.registry.adapter(provider)
        return self._validate(
            provider,
            plt_adapter.materialize(
                plt_cfg_root=self.plt_cfg_root,
                selected_units=selection["selected_units"],
                execution_context=execution_context,
                workspace=workspace,
            ),
        )

    @staticmethod
    def _write_resolved_cfg(target_cfg_dir, materialization_key, materialized):
        resolved_cfg = materialized.get("resolved_cfg")
        if not isinstance(resolved_cfg, list) or not resolved_cfg:
            raise RuntimeError("❌ PLT provider must return non-empty resolved_cfg")
        for index, entry in enumerate(resolved_cfg):
            if not isinstance(entry, Mapping) or set(entry) != {"target_path", "values"}:
                raise RuntimeError(
                    "❌ each PLT provider resolved_cfg entry must define exactly "
                    "target_path and values"
                )
            target_path = kernel_paths.normalize_cfg_absolute_path(
                entry["target_path"], label="PLT provider resolved_cfg target_path"
            )
            if not isinstance(entry["values"], Mapping):
                raise RuntimeError("❌ PLT provider resolved_cfg values must be a mapping")
            destination = target_cfg_dir / "rendered" / target_path.lstrip("/")
            destination.mkdir(parents=True, exist_ok=True)
            digest = kernel_paths.canonical_sha256(materialization_key)[:12]
            kernel_yaml_io.write_yaml_file(
                destination / f"{index:03d}-{digest}.yaml", dict(entry["values"])
            )

    def prepare_target_view(
        self,
        target_run_id,
        target_run,
        *,
        execution_context,
        target_cfg_dir,
        scope_params,
    ):
        selection = self.select(target_run, execution_context, scope_params=scope_params)
        if target_cfg_dir.exists():
            shutil.rmtree(target_cfg_dir)
        target_cfg_dir.mkdir(parents=True)
        kernel_yaml_io.write_yaml_file(target_cfg_dir / "selection.yaml", selection)
        materialized = self.materialize_scope(
            selection,
            execution_context=execution_context,
            workspace=target_cfg_dir / "provider",
        )
        self._write_resolved_cfg(target_cfg_dir, target_run_id, materialized)
        return target_cfg_dir / "rendered", selection
