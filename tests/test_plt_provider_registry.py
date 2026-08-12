"""PLT-provider registration is cfg-owned and package-backed."""

import sys
import tempfile
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.plt import providers as plt_providers


class PltProviderRegistryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.adapter_root = self.root / "adapter"
        self.adapter_root.mkdir()

    def declare(self, *, package: str = "sample_plt_adapter") -> None:
        (self.root / "plt_providers.yaml").write_text(
            yaml.safe_dump(
                {
                    "plt_providers": {
                        "runtime": {
                            "implements": ["materialize"],
                            "source": {
                                "repo_url": "https://example.test/provider.git",
                                "secret_key": "source_token",
                            },
                            "package": package,
                        }
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (self.root / "local_repos.yaml").write_text(
            yaml.safe_dump(
                {
                    "tooling": {
                        "plt-provider-runtime": {
                            "repo_path": str(self.adapter_root),
                        }
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )

    def write_adapter(self, body: str) -> None:
        (self.adapter_root / "sample_plt_adapter.py").write_text(body, encoding="utf-8")

    def cleanup_import(self) -> None:
        sys.modules.pop("sample_plt_adapter", None)
        path = str(self.adapter_root)
        if path in sys.path:
            sys.path.remove(path)

    def test_activates_and_validates_declared_contracts(self):
        self.addCleanup(self.cleanup_import)
        self.declare()
        self.write_adapter(
            "class Adapter:\n"
            "    name = 'runtime'\n"
            "    def materialize(self): pass\n"
            "PROVIDER = Adapter()\n"
        )
        registry = plt_providers.ProviderRegistry(self.root)

        self.assertEqual([str(self.adapter_root)], registry.activate_local_adapters())
        registry.validate_declared_contracts()
        self.assertEqual("runtime", registry.adapter("runtime").name)

    def test_builtin_atlas_provider_is_always_available(self):
        registry = plt_providers.ProviderRegistry(self.root)
        self.assertEqual("atlas", registry.adapter("atlas").name)

    def test_refuses_an_undeclared_provider(self):
        self.declare()
        registry = plt_providers.ProviderRegistry(self.root)
        with self.assertRaisesRegex(RuntimeError, "is not declared"):
            registry.adapter("another")

    def test_refuses_a_declared_contract_without_a_callable(self):
        self.addCleanup(self.cleanup_import)
        self.declare()
        self.write_adapter("class Adapter:\n    name = 'runtime'\nPROVIDER = Adapter()\n")
        registry = plt_providers.ProviderRegistry(self.root)
        registry.activate_local_adapters()
        with self.assertRaisesRegex(RuntimeError, "does not implement"):
            registry.validate_declared_contracts()

    def test_refuses_unknown_contract_names(self):
        self.declare()
        path = self.root / "plt_providers.yaml"
        document = yaml.safe_load(path.read_text())
        document["plt_providers"]["runtime"]["implements"].append("merge")
        path.write_text(yaml.safe_dump(document), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "unknown contracts"):
            _ = plt_providers.ProviderRegistry(self.root).entries


if __name__ == "__main__":
    unittest.main()
