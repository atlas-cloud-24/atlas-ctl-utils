"""The CTL-owned registry for PLT-provider implementations."""

from __future__ import annotations

import importlib
import re
import sys
from enum import StrEnum
from pathlib import Path

from atlas_plt_provider_atlas import provider as atlas_provider

from engine.cfg import resources as cfg_resources
from engine.cfg import tooling as cfg_tooling

_PROVIDER_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]*$")
_PACKAGE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")


class ProviderContract(StrEnum):
    MATERIALIZE = "materialize"


class ProviderRegistry:
    """Load built-in and CTL-declared PLT provider implementations."""

    resource_key = "plt_providers"
    builtins = {atlas_provider.PROVIDER.name: atlas_provider.PROVIDER}
    tooling_name_template = "plt-provider-{provider}"

    def __init__(self, ctl_cfg_root: Path):
        self.ctl_cfg_root = Path(ctl_cfg_root)
        self._entries: dict[str, dict[str, object]] | None = None

    @property
    def entries(self) -> dict[str, dict[str, object]]:
        if self._entries is None:
            raw = cfg_resources.collect_resource(self.ctl_cfg_root, self.resource_key) or {}
            self._entries = {
                provider: self._entry(provider, entry) for provider, entry in raw.items()
            }
        return self._entries

    @staticmethod
    def tooling_name(provider: str) -> str:
        return ProviderRegistry.tooling_name_template.format(provider=provider)

    @staticmethod
    def _entry(provider: object, raw: object) -> dict[str, object]:
        label = f"plt_providers.{provider}"
        if not isinstance(provider, str) or not _PROVIDER_KEY_RE.fullmatch(provider):
            raise RuntimeError(f"❌ PLT provider key {provider!r} has an invalid shape")
        if not isinstance(raw, dict) or not raw:
            raise RuntimeError(f"❌ {label} must be a non-empty mapping")
        expected = {"implements", "package", "source"}
        actual = set(raw)
        if actual != expected:
            raise RuntimeError(
                f"❌ {label} must define exactly {sorted(expected)}; "
                f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
            )

        implements = raw["implements"]
        if (
            not isinstance(implements, list)
            or not implements
            or not all(isinstance(value, str) and value for value in implements)
        ):
            raise RuntimeError(f"❌ {label}.implements must be a non-empty string list")
        if len(set(implements)) != len(implements):
            raise RuntimeError(f"❌ {label}.implements must not contain duplicates")
        known = {contract.value for contract in ProviderContract}
        unknown = sorted(set(implements) - known)
        if unknown:
            raise RuntimeError(
                f"❌ {label}.implements declares unknown contracts {unknown}; "
                f"known: {sorted(known)}"
            )

        package = raw["package"]
        if not isinstance(package, str) or not _PACKAGE_RE.fullmatch(package):
            raise RuntimeError(f"❌ {label}.package must be an importable dotted name")

        source = raw["source"]
        if not isinstance(source, dict) or set(source) != {"repo_url", "secret_key"}:
            raise RuntimeError(f"❌ {label}.source must define exactly ['repo_url', 'secret_key']")
        for key, value in source.items():
            if not isinstance(value, str) or not value.strip():
                raise RuntimeError(f"❌ {label}.source.{key} must be a non-empty string")

        return {
            "implements": list(implements),
            "package": package,
            "source": dict(source),
        }

    def activate_local_adapters(self) -> list[str]:
        """Add only locally declared provider repositories to the import path."""

        tooling = cfg_tooling.load_local_tooling_cfg(self.ctl_cfg_root)
        added: list[str] = []
        for provider in self.entries:
            tooling_name = self.tooling_name(provider)
            tooling_entry = tooling.get(tooling_name) or {}
            repo_path = tooling_entry.get("repo_path") if isinstance(tooling_entry, dict) else None
            if not repo_path:
                continue
            path = Path(repo_path)
            if not path.is_dir():
                raise RuntimeError(
                    f"❌ local tooling path for {tooling_name!r} does not exist: {path}"
                )
            path_string = str(path)
            if path_string not in sys.path:
                sys.path.insert(0, path_string)
                added.append(path_string)
        return added

    def adapter(self, provider: str):
        builtin = self.builtins.get(provider)
        if builtin is not None:
            return builtin
        entry = self.entries.get(provider)
        if entry is None:
            raise RuntimeError(
                f"❌ PLT provider {provider!r} is not declared in {self.resource_key}; "
                f"available: {sorted(set(self.builtins) | set(self.entries))}"
            )
        package = str(entry["package"])
        try:
            module = importlib.import_module(package)
        except ImportError as exc:
            raise RuntimeError(
                f"❌ PLT provider {provider!r} is registered but package {package!r} "
                f"is not importable: {exc}. Check tooling ref "
                f"{self.tooling_name(provider)!r}."
            ) from exc
        adapter = getattr(module, "PROVIDER", None)
        if adapter is None:
            raise RuntimeError(f"❌ PLT provider package {package!r} must expose PROVIDER")
        if getattr(adapter, "name", None) != provider:
            raise RuntimeError(
                f"❌ PLT provider package {package!r} identifies as "
                f"{getattr(adapter, 'name', None)!r}, expected {provider!r}"
            )
        return adapter

    def validate_declared_contracts(self) -> None:
        for provider, entry in self.entries.items():
            adapter = self.adapter(provider)
            missing = [
                contract
                for contract in entry["implements"]
                if not callable(getattr(adapter, str(contract), None))
            ]
            if missing:
                raise RuntimeError(
                    f"❌ plt_providers.{provider} declares contracts {missing} but its "
                    "adapter does not implement them"
                )
