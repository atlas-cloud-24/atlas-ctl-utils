import importlib.util
import tempfile
import unittest
from pathlib import Path


CTL_UTILS_ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


build_runtime_cfg = load_module(
    "build_runtime_cfg",
    CTL_UTILS_ROOT / "step_utils" / "build_runtime_cfg.py",
)


class CfgEntryRefTests(unittest.TestCase):
    def test_build_runtime_cfg_normalizes_cfg_entry_ref_after_interpolation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "cfg.yaml").write_text(
                """
foundation:
  computing:
    launch_templates_cfg:
      app:
        name: app-lt
    asg_cfg:
      app:
        name: app-asg
        launch_template: cfg-entry-ref:foundation.computing.launch_templates_cfg:app
""",
                encoding="utf-8",
            )

            values, _ = build_runtime_cfg.build_step_values(root, ["cfg.yaml"], {})

        self.assertEqual(
            values["foundation"]["computing"]["asg_cfg"]["app"]["launch_template"],
            {
                "cfg_entry_ref": {
                    "collection": "foundation.computing.launch_templates_cfg",
                    "key": "app",
                }
            },
        )

    def test_build_runtime_cfg_rejects_missing_cfg_entry_ref_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "cfg.yaml").write_text(
                """
foundation:
  computing:
    launch_templates_cfg: {}
    asg_cfg:
      app:
        launch_template: cfg-entry-ref:foundation.computing.launch_templates_cfg:missing
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "missing item 'missing'"):
                build_runtime_cfg.build_step_values(root, ["cfg.yaml"], {})


if __name__ == "__main__":
    unittest.main()
