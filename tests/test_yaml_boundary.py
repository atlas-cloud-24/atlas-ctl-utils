"""Every cfg file the engine reads goes through one loader, with one guard.

The duplicate-key guard existed as a named class and an unregistered
constructor, so it never ran: a cfg declaring `targets:` twice silently lost the
first block. Nothing failed, which is why it survived — these tests are what say
it is armed.
"""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import yaml
from engine.kernel import yaml_io


class DuplicateKeyTests(unittest.TestCase):
    """A repeated key is a hard error, wherever it sits."""

    def load(self, text: str):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cfg.yaml"
            path.write_text(text, encoding="utf-8")
            return yaml_io.load_yaml(path)

    def assert_rejected(self, text: str, key: str):
        with self.assertRaises(yaml.constructor.ConstructorError) as caught:
            self.load(text)
        self.assertIn(f"duplicate key {key!r}", str(caught.exception))

    def test_a_repeated_top_level_key_is_rejected(self):
        self.assert_rejected("targets:\n  a: 1\ntargets:\n  b: 2\n", "targets")

    def test_a_repeated_nested_key_is_rejected(self):
        self.assert_rejected("targets:\n  a: 1\n  a: 2\n", "a")

    def test_a_repeated_key_inside_a_list_item_is_rejected(self):
        self.assert_rejected("targets:\n  - a: 1\n    a: 2\n", "a")

    def test_the_text_loader_is_guarded_too(self):
        # every path into the engine's YAML goes through load_yaml_text
        with self.assertRaises(yaml.constructor.ConstructorError):
            yaml_io.load_yaml_text("a: 1\na: 2\n", label="in memory")


class LoaderCompatibilityTests(unittest.TestCase):
    """The guard must not cost the loader anything it could already do."""

    def load(self, text: str):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cfg.yaml"
            path.write_text(text, encoding="utf-8")
            return yaml_io.load_yaml(path)

    def test_a_merge_key_still_merges_and_the_explicit_key_still_wins(self):
        # `<<` has no constructor until the base class flattens it away, so a
        # naive duplicate scan turns every merge into an unreadable file
        self.assertEqual(
            self.load("base: &b\n  a: 1\n  b: 1\nuse:\n  <<: *b\n  a: 2\n"),
            {"base": {"a": 1, "b": 1}, "use": {"a": 2, "b": 1}},
        )

    def test_one_anchor_may_be_used_twice(self):
        self.assertEqual(
            self.load("base: &b\n  a: 1\none: *b\ntwo: *b\n"),
            {"base": {"a": 1}, "one": {"a": 1}, "two": {"a": 1}},
        )

    def test_an_ordinary_mapping_still_loads(self):
        self.assertEqual(self.load("targets:\n  a: 1\n  b: 2\n"), {"targets": {"a": 1, "b": 2}})


class OneBoundaryTests(unittest.TestCase):
    """No engine module PARSES yaml behind the kernel's back.

    Loading is what the guard covers, so loading is what this asserts. Dumping is
    not in scope: a report rendered to stdout parses nothing and can lose no key.
    """

    ENGINE = REPO_ROOT / "runners" / "engine"
    ALLOWED = {"kernel/yaml_io.py"}
    LOADERS = ("yaml.safe_load", "yaml.load(", "yaml.full_load", "yaml.unsafe_load")

    def test_only_the_kernel_parses_yaml(self):
        callers = set()
        for path in sorted(self.ENGINE.rglob("*.py")):
            relative = str(path.relative_to(self.ENGINE))
            if relative in self.ALLOWED:
                continue
            text = path.read_text()
            if any(loader in text for loader in self.LOADERS):
                callers.add(relative)
        self.assertEqual(
            callers,
            set(),
            "these modules parse yaml directly and so do not get the duplicate-key guard",
        )


if __name__ == "__main__":
    unittest.main()
