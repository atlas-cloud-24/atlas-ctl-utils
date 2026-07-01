import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


class SelectorRegistryTests(unittest.TestCase):
    def test_ctl_selector_registry_keeps_policy_map_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_policy.any.yaml").write_text(
                """selectors:\n  ctl_context:\n    local_dev:\n      ref_policy: local_dirty_allowed\n      allow_aws_profile_only: true\n""",
                encoding="utf-8",
            )

            registry = common.load_selector_registry(root, "ctl")

        self.assertEqual(
            registry,
            {
                "ctl_context": {
                    "local_dev": {
                        "ref_policy": "local_dirty_allowed",
                        "allow_aws_profile_only": True,
                    }
                }
            },
        )

    def test_plt_selector_registry_loads_root_cfg_list_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / common.CFG_ROOT_META_FILENAME).write_text(
                """selector_registry:\n  env_type:\n    - dev\n    - prod\n""",
                encoding="utf-8",
            )

            registry = common.load_selector_registry(root, "plt")

        self.assertEqual(registry, {"env_type": {"dev": {}, "prod": {}}})

    def test_plt_selector_registry_rejects_duplicate_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / common.CFG_ROOT_META_FILENAME).write_text(
                """selector_registry:\n  env_type:\n    - dev\n    - dev\n""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "duplicate selector_registry.env_type value"):
                common.load_selector_registry(root, "plt")


    def test_runtime_context_loads_by_top_level_key_not_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "context.any.yaml").write_text(
                """runtime_context:\n  main_tag: oxygen\n  env_type: ${selectors.plt.env_type}\n""",
                encoding="utf-8",
            )

            runtime_context = common.load_runtime_context(root)

        self.assertEqual(
            runtime_context,
            {
                "main_tag": {"value": "oxygen"},
                "env_type": {"selector": "plt.env_type"},
            },
        )


if __name__ == "__main__":
    unittest.main()
