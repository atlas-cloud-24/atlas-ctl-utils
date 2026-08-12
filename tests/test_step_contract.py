"""The pipeline may only read what a Step declares."""

import ast
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.units import step as units_step

PIPELINE = REPO_ROOT / "runners" / "engine" / "commands" / "pipeline.py"
HOLDERS = {"repo_step": units_step.Step, "repo_step_runtime": units_step.StepRuntime}


class StepContractTests(unittest.TestCase):
    """A step is an object, so the pipeline reaches it by attribute."""

    def setUp(self):
        self.tree = ast.parse(PIPELINE.read_text())

    def test_every_attribute_the_pipeline_reads_exists(self):
        # a renamed or dropped field fails here rather than mid-run, where the
        # step loop is reached only by a real provisioning run
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Attribute) or not isinstance(node.value, ast.Name):
                continue
            holder = HOLDERS.get(node.value.id)
            if holder is None:
                continue
            with self.subTest(read=f"{node.value.id}.{node.attr}", line=node.lineno):
                self.assertTrue(
                    hasattr(holder, node.attr)
                    or node.attr in getattr(holder, "__annotations__", {}),
                    f"{holder.__name__} declares no {node.attr!r}",
                )

    def test_a_step_is_never_subscripted(self):
        # `step["image"]` raises only when that line runs; the object is frozen,
        # so the mistake is always a bug and never a style choice
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
                self.assertNotIn(
                    node.value.id,
                    HOLDERS,
                    f"line {node.lineno}: {node.value.id} is an object, not a mapping",
                )


if __name__ == "__main__":
    unittest.main()


class RunTargetsBindingTests(unittest.TestCase):
    """A unit must be built on the path that runs it."""

    def test_every_unit_run_is_preceded_by_its_construction_in_the_same_block(self):
        # `run_targets` has two branches — spawn a child, or run the steps here —
        # and only a real run reaches either. Building the unit in one branch and
        # running it in the other raises UnboundLocalError at provision time,
        # which no suite reaches. So the construction is required in the SAME
        # block as the call.
        fn = next(
            node
            for node in ast.parse(PIPELINE.read_text()).body
            if isinstance(node, ast.FunctionDef) and node.name == "run_targets"
        )

        def built_in(block) -> set[str]:
            names = set()
            for statement in block:
                for node in ast.walk(statement):
                    if (
                        isinstance(node, ast.Assign)
                        and isinstance(node.value, ast.Call)
                        and isinstance(node.value.func, ast.Attribute)
                        and node.value.func.attr.startswith("from_")
                    ):
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                names.add(target.id)
            return names

        def check(block):
            available = built_in(block)
            for statement in block:
                for node in ast.walk(statement):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "run"
                        and isinstance(node.func.value, ast.Name)
                    ):
                        with self.subTest(unit=node.func.value.id, line=node.lineno):
                            self.assertIn(
                                node.func.value.id,
                                available,
                                f"line {node.lineno}: {node.func.value.id}.run() is called "
                                "in a block that never builds it",
                            )
                for inner in ("body", "orelse", "finalbody"):
                    nested = getattr(statement, inner, None)
                    if nested:
                        check(nested)

        check(fn.body)
