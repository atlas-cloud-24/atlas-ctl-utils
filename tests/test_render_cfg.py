import sys
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def write_tree(root: Path, files: dict[str, str]) -> None:
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def render(files: dict[str, str], env_ctx: dict, volatile: set[str]) -> Path:
    tmp = tempfile.mkdtemp()
    merged = Path(tmp) / "merged"
    write_tree(merged, files)
    rendered = Path(tmp) / "rendered"
    rendered.mkdir()
    for entry in sorted(merged.iterdir()):
        if entry.is_dir():
            common.render_scope_tree(entry, rendered / entry.name, env_ctx, frozenset(volatile))
    return rendered


VOLATILE = {"execution_context.run.id"}
CTX = {
    "execution_context.params.main_tag": "oxygen",
    "execution_context.params.env_type": "dev",
}


class RenderScopeTreeTests(unittest.TestCase):
    def test_renders_context_keeps_volatile_and_normalizes_refs(self):
        rendered = render(
            {
                "env/computing/asg.yaml": (
                    "foundation:\n"
                    "  computing:\n"
                    "    asg_cfg:\n"
                    "      app:\n"
                    "        name: ${execution_context.params.main_tag}-asg\n"
                    "        run_marker: ${execution_context.run.id}\n"
                    "        launch_template_key: cfg-entry-ref:foundation.computing.launch_templates_cfg:app\n"
                ),
                "env/computing/lt.yaml": (
                    "foundation:\n  computing:\n    launch_templates_cfg:\n      app:\n        instance_type: t3.small\n"
                ),
                "env/notes.md": "not yaml\n",
            },
            CTX,
            VOLATILE,
        )
        app = yaml.safe_load((rendered / "env/computing/asg.yaml").read_text())["foundation"]["computing"]["asg_cfg"]["app"]
        self.assertEqual(app["name"], "oxygen-asg")
        self.assertEqual(app["run_marker"], "${execution_context.run.id}")
        self.assertEqual(
            app["launch_template_key"],
            {"cfg_entry_ref": {"collection": "foundation.computing.launch_templates_cfg", "key": "app"}},
        )
        self.assertEqual((rendered / "env/notes.md").read_text(), "not yaml\n")

    def test_reserved_top_level_key_rejected_in_payload(self):
        with self.assertRaisesRegex(RuntimeError, "reserved top-level key"):
            render({"env/a.yaml": "execution_context:\n  var:\n    main_tag: hacked\n"}, CTX, VOLATILE)

    def test_missing_stable_reference_fails_at_render(self):
        with self.assertRaisesRegex(RuntimeError, "missing cfg interpolation reference"):
            render({"env/a.yaml": "top:\n  value: ${execution_context.params.absent}\n"}, CTX, VOLATILE)

    def test_scopes_render_independently(self):
        rendered = render(
            {
                "env/a.yaml": "shared:\n  value: env-value\nuses:\n  ref: ${shared.value}\n",
                "org/b.yaml": "shared:\n  value: org-value\nuses:\n  ref: ${shared.value}\n",
            },
            CTX,
            VOLATILE,
        )
        self.assertEqual(yaml.safe_load((rendered / "env/a.yaml").read_text())["uses"]["ref"], "env-value")
        self.assertEqual(yaml.safe_load((rendered / "org/b.yaml").read_text())["uses"]["ref"], "org-value")


if __name__ == "__main__":
    unittest.main()
