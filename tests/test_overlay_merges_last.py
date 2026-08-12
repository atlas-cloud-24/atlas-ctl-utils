"""An overlay merges over the COMPOSED cfg, so switching it on always applies it.

The merge point is the whole point. Merged into the SOURCE tree an overlay
patches one preset level, and the scope chain continues over it:

    _common/all  (patched)  ->  _common/non_prod  ->  dev

so an overlay aimed at a broad level silently loses to any narrower declaration
of the same leaf, and "switched on" stops meaning "applied". `tech_jobs` worked
only because `origin_path` happened to be declared nowhere else.

These go through `merge_plt_cfg_dirs` rather than the applier directly: the
applier can be correct while nothing calls it, which is exactly what an earlier
version of this test failed to catch.

Last means last BEFORE render — an overlay's values interpolate like any other
cfg, and `alarms_disabled` sets `alarms_cfg: ${alarms_disabled_empty_map}`.
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.cfg import tree as cfg_tree  # noqa: E402
from engine.kernel import yaml_io as kernel_yaml_io  # noqa: E402

CONTEXT = {
    "execution_context.params.env.type": "dev",
    "execution_context.target.domains": ["env"],
}


def _write_scope(root: Path, rel: str, *, target_path: str, selectors: dict, payload: dict) -> None:
    scope_root = root / rel
    kernel_yaml_io.write_yaml_file(
        scope_root / "__meta__.yaml",
        {
            "type": "scope",
            "target_path": target_path,
            "selectors": selectors,
        },
    )
    kernel_yaml_io.write_yaml_file(scope_root / "service.yaml", payload)


def _write_overlay(root: Path, name: str, *, scope_rel: str, payload: dict) -> None:
    """An overlay mirrors SOURCE paths, so its payload sits under the scope root
    it patches — that is what lets one overlay carry env-specific values."""

    overlay_root = root / "_overlays" / name
    kernel_yaml_io.write_yaml_file(
        overlay_root / "__meta__.yaml",
        {
            "type": "overlay",
            "name": name,
            "selectors": {"in": {"execution_context.params.env.type": ["dev"]}},
        },
    )
    kernel_yaml_io.write_yaml_file(overlay_root / scope_rel / "service.yaml", payload)


class OverlayMergesLastTest(unittest.TestCase):
    def _merge(self, overlays: list[str]) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "cfg"
            _write_scope(
                root,
                "domains/env/dev",
                target_path="/env",
                selectors={
                    "contains": {"execution_context.target.domains": "env"},
                    "match": {"execution_context.params.env.type": "dev"},
                },
                payload={"service_cfg": {"a-api": {"mode": "composed", "port": 8080}}},
            )
            _write_overlay(
                root,
                "maintenance",
                scope_rel="domains/env/dev",
                payload={"service_cfg": {"a-api": {"mode": "maintenance"}}},
            )
            merged = Path(tmp) / "merged"
            cfg_tree.merge_plt_cfg_dirs(
                plt_cfg_root=root,
                plt_merged_dir=merged,
                ctl_profile="local_dev",
                plt_overlays=overlays,
                scope_params={"env.type": "dev"},
                execution_context=CONTEXT,
            )
            return kernel_yaml_io.load_yaml(merged / "env" / "service.yaml")

    def test_without_the_overlay_the_composed_value_stands(self):
        """Guards the test below: identical results either way would prove nothing."""

        self.assertEqual(self._merge([])["service_cfg"]["a-api"]["mode"], "composed")

    def test_the_overlay_overrides_the_composed_value(self):
        self.assertEqual(
            self._merge(["maintenance"])["service_cfg"]["a-api"]["mode"], "maintenance"
        )

    def test_the_overlay_leaves_untouched_leaves_alone(self):
        """It overwrites the leaf it declares and merges into the rest — a
        replaced FILE would silently drop every sibling value."""

        self.assertEqual(self._merge(["maintenance"])["service_cfg"]["a-api"]["port"], 8080)


if __name__ == "__main__":
    unittest.main()
