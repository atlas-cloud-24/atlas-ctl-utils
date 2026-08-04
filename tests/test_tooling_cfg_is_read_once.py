"""Local tooling cfg is read once per cfg root, and says so once.

A run resolves it three times — `build_execution_context` runs for the preflight
and again for the pipeline, and the pipeline reads it once more for the step env.
A materialized cfg root cannot change while a run reads it, so the second and
third answers can only agree with the first. Reading it three times walked the
whole cfg tree three times and printed one INFO line three times, which reads to
an operator as three separate decisions rather than one fact.
"""

import sys
import tempfile
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.cfg import resources as cfg_resources
from engine.cfg import tooling as cfg_tooling


def tooling_root(tmp: str, **entries) -> Path:
    root = Path(tmp)
    (root / "__meta__.yaml").write_text(yaml.safe_dump({"cfg_root": {"kind": "ctl"}}))
    (root / "tooling.yaml").write_text(yaml.safe_dump({"tooling": entries}))
    return root


class ToolingCfgIsCachedPerRootTest(unittest.TestCase):
    def setUp(self):
        cfg_tooling._local_tooling_cfg.cache_clear()
        self.addCleanup(cfg_tooling._local_tooling_cfg.cache_clear)

    def test_the_cfg_tree_is_walked_once_however_often_it_is_asked(self):
        walks = []
        real = cfg_resources.collect_resource

        def counting(root, key, **kwargs):
            walks.append(key)
            return real(root, key, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            root = tooling_root(tmp, **{"ctl-utils": {"repo_path": tmp}})
            cfg_resources.collect_resource = counting
            try:
                first = cfg_tooling.load_local_tooling_cfg(root)
                cfg_tooling.load_local_tooling_cfg(root)
                cfg_tooling.load_local_tooling_cfg(root)
            finally:
                cfg_resources.collect_resource = real

        self.assertEqual(["tooling"], walks)
        self.assertIn("ctl-utils", first)

    def test_every_caller_gets_the_same_answer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = tooling_root(tmp, **{"ctl-utils": {"repo_path": tmp}})
            self.assertEqual(
                cfg_tooling.load_local_tooling_cfg(root),
                cfg_tooling.load_local_tooling_cfg(root),
            )

    def test_a_caller_cannot_corrupt_the_cache(self):
        """The cached mapping is never handed out — a caller that mutates its
        result would otherwise silently rewrite what every later caller reads."""
        with tempfile.TemporaryDirectory() as tmp:
            root = tooling_root(tmp, **{"ctl-utils": {"repo_path": tmp}})
            cfg_tooling.load_local_tooling_cfg(root)["ctl-utils"] = "corrupted"
            self.assertNotEqual(
                "corrupted", cfg_tooling.load_local_tooling_cfg(root)["ctl-utils"]
            )

    def test_two_cfg_roots_are_cached_apart(self):
        """Keyed by root, not global: a fan-out child may run against another."""
        with tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as two:
            first = tooling_root(one, **{"ctl-utils": {"repo_path": one}})
            second = tooling_root(two, **{"plt-utils": {"repo_path": two}})
            self.assertIn("ctl-utils", cfg_tooling.load_local_tooling_cfg(first))
            self.assertIn("plt-utils", cfg_tooling.load_local_tooling_cfg(second))

    def test_the_same_root_spelled_differently_is_one_entry(self):
        """Resolved before it is keyed, so `a/../a` is not a second read."""
        with tempfile.TemporaryDirectory() as tmp:
            root = tooling_root(tmp, **{"ctl-utils": {"repo_path": tmp}})
            cfg_tooling.load_local_tooling_cfg(root)
            cfg_tooling.load_local_tooling_cfg(root / ".." / root.name)
            self.assertEqual(1, cfg_tooling._local_tooling_cfg.cache_info().misses)


if __name__ == "__main__":
    unittest.main()
