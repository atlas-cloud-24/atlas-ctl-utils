import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


CTL_UTILS_ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# render_cfg imports build_runtime_cfg by module name from its own directory.
load_module("build_runtime_cfg", CTL_UTILS_ROOT / "stages" / "build_runtime_cfg.py")
render_cfg = load_module("render_cfg", CTL_UTILS_ROOT / "stages" / "render_cfg.py")


def write_tree(root: Path, files: dict[str, str]) -> None:
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


class RenderCfgTests(unittest.TestCase):
    def render(self, files: dict[str, str], env_ctx: dict, volatile: set[str]) -> Path:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        merged = root / "merged"
        write_tree(merged, files)
        rendered = root / "rendered"
        render_cfg.render_cfg_tree(merged, rendered, env_ctx, frozenset(volatile))
        return rendered

    def test_renders_stable_context_keeps_volatile_and_normalizes_refs(self):
        rendered = self.render(
            {
                "env/computing/asg.yaml": (
                    "foundation:\n"
                    "  computing:\n"
                    "    asg_cfg:\n"
                    "      app:\n"
                    "        name: ${main_tag}-asg\n"
                    "        run_marker: ${run_id}\n"
                    "        launch_template_key: cfg-entry-ref:foundation.computing.launch_templates_cfg:app\n"
                ),
                "env/computing/lt.yaml": (
                    "foundation:\n"
                    "  computing:\n"
                    "    launch_templates_cfg:\n"
                    "      app:\n"
                    "        instance_type: t3.small\n"
                ),
                "env/notes.md": "not yaml\n",
            },
            {"main_tag": "oxygen"},
            {"run_id"},
        )

        asg = yaml.safe_load((rendered / "env/computing/asg.yaml").read_text())
        app = asg["foundation"]["computing"]["asg_cfg"]["app"]
        self.assertEqual(app["name"], "oxygen-asg")
        self.assertEqual(app["run_marker"], "${run_id}")
        self.assertEqual(
            app["launch_template_key"],
            {
                "cfg_entry_ref": {
                    "collection": "foundation.computing.launch_templates_cfg",
                    "key": "app",
                }
            },
        )
        self.assertEqual((rendered / "env/notes.md").read_text(), "not yaml\n")

    def test_cross_file_refs_validate_whole_scope(self):
        with self.assertRaisesRegex(RuntimeError, "missing item 'missing-lt'"):
            self.render(
                {
                    "env/asg.yaml": (
                        "foundation:\n"
                        "  computing:\n"
                        "    launch_templates_cfg: {}\n"
                        "    asg_cfg:\n"
                        "      app:\n"
                        "        launch_template_key: cfg-entry-ref:foundation.computing.launch_templates_cfg:missing-lt\n"
                    ),
                },
                {},
                {"run_id"},
            )

    def test_missing_stable_reference_fails_at_render(self):
        with self.assertRaisesRegex(RuntimeError, "missing cfg interpolation reference: absent_key"):
            self.render(
                {"env/a.yaml": "top:\n  value: ${absent_key}\n"},
                {},
                {"run_id"},
            )

    def test_scopes_render_independently(self):
        rendered = self.render(
            {
                "env/a.yaml": "shared:\n  value: env-value\nuses:\n  ref: ${shared.value}\n",
                "org/b.yaml": "shared:\n  value: org-value\nuses:\n  ref: ${shared.value}\n",
            },
            {},
            {"run_id"},
        )
        env_doc = yaml.safe_load((rendered / "env/a.yaml").read_text())
        org_doc = yaml.safe_load((rendered / "org/b.yaml").read_text())
        self.assertEqual(env_doc["uses"]["ref"], "env-value")
        self.assertEqual(org_doc["uses"]["ref"], "org-value")


if __name__ == "__main__":
    unittest.main()
