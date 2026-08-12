"""Scopes and steps carry only optional provider-owned configuration."""

import sys
import tempfile
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cfg import plt_provider as cfg_plt_provider
from engine.cfg import resources as cfg_resources


class PltProviderBindingTest(unittest.TestCase):
    def test_absent_binding_is_allowed(self):
        self.assertEqual({}, cfg_plt_provider.ProviderBinding.selectable_unit({}, label="unit"))

    def test_selectable_unit_carries_provider_identity(self):
        self.assertEqual(
            {"provider": "runtime"},
            cfg_plt_provider.ProviderBinding.selectable_unit(
                {"plt": {"provider": "runtime"}}, label="unit"
            ),
        )

    def test_selectable_unit_carries_opaque_provider_cfg(self):
        provider_cfg = {"entrypoint": "stack.yaml.tmpl"}
        self.assertEqual(
            {"provider_cfg": provider_cfg},
            cfg_plt_provider.ProviderBinding.selectable_unit(
                {
                    "plt": {
                        "provider_cfg": provider_cfg,
                    }
                },
                label="unit",
            ),
        )

    def test_binding_fields_are_not_interchangeable(self):
        cases = (
            (
                cfg_plt_provider.ProviderBinding.selectable_unit,
                {"plt": {"request": {"component": "seed"}}},
            ),
        )
        for validator, value in cases:
            with (
                self.subTest(validator=validator.__name__),
                self.assertRaisesRegex(RuntimeError, "unsupported keys"),
            ):
                validator(value, label="unit")

    def test_opaque_mapping_must_be_non_empty(self):
        for value in (None, "stack.yaml", {}):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(RuntimeError, "non-empty mapping"),
            ):
                cfg_plt_provider.ProviderBinding.selectable_unit(
                    {
                        "plt": {
                            "provider_cfg": value,
                        }
                    },
                    label="unit",
                )

    def test_unknown_binding_fields_are_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unsupported keys"):
            cfg_plt_provider.ProviderBinding.selectable_unit(
                {"plt": {"mode": "other"}},
                label="unit",
            )


class PltMetadataProviderTest(unittest.TestCase):
    def test_scope_metadata_allows_no_provider_cfg(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "__meta__.yaml"
            path.write_text(yaml.safe_dump({"type": "scope", "target_path": "/env"}))
            self.assertNotIn("plt", cfg_resources.load_cfg_meta(path))

    def test_overlay_metadata_carries_the_binding(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "__meta__.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "type": "overlay",
                        "name": "variation",
                        "plt": {
                            "provider_cfg": {"entrypoint": "stack.yaml.tmpl"},
                        },
                    }
                )
            )
            self.assertEqual(
                {
                    "provider_cfg": {"entrypoint": "stack.yaml.tmpl"},
                },
                cfg_resources.load_cfg_meta(path)["plt"],
            )


if __name__ == "__main__":
    unittest.main()
