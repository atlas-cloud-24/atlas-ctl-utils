"""A unit is reached through the ports it declares, and by attribute."""

import ast
import inspect
import sys
import typing
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.commands import target_runner as commands_target_runner
from engine.units import procedure as units_procedure
from engine.units import step as units_step
from engine.units import target as units_target
from engine.units import workflow as units_workflow

ENGINE = REPO_ROOT / "runners" / "engine"
WORKFLOW_UNIT = ENGINE / "units" / "workflow.py"


class StepContractTests(unittest.TestCase):
    """A step is an object, so the engine reaches it by attribute."""

    def engine_modules(self):
        return sorted(path for path in ENGINE.rglob("*.py") if path.name != "__init__.py")

    def test_every_attribute_read_off_a_step_exists(self):
        # a renamed or dropped field fails here rather than mid-run, where the
        # step loop is reached only by a real provisioning run
        reads = 0
        for path in self.engine_modules():
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, ast.Attribute):
                    continue
                holder, attribute = self._step_read(node)
                if holder is None:
                    continue
                reads += 1
                with self.subTest(read=attribute, file=path.name, line=node.lineno):
                    self.assertTrue(
                        hasattr(holder, attribute)
                        or attribute in getattr(holder, "__annotations__", {}),
                        f"{holder.__name__} declares no {attribute!r}",
                    )
        self.assertGreater(reads, 0, "the scan found no step attribute reads at all")

    @staticmethod
    def _step_read(node: ast.Attribute):
        """The class an attribute read resolves against, for `step.x` and `step.runtime.x`."""

        value = node.value
        if isinstance(value, ast.Name) and value.id == "step":
            return units_step.Step, node.attr
        if (
            isinstance(value, ast.Attribute)
            and value.attr == "runtime"
            and isinstance(value.value, ast.Name)
            and value.value.id == "step"
        ):
            return units_step.StepRuntime, node.attr
        return None, ""

    def test_a_step_is_never_subscripted(self):
        # `step["image"]` raises only when that line runs; the object is frozen,
        # so the mistake is always a bug and never a style choice
        for path in self.engine_modules():
            for node in ast.walk(ast.parse(path.read_text())):
                if (
                    isinstance(node, ast.Subscript)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "step"
                ):
                    self.fail(f"{path.name}:{node.lineno}: step is an object, not a mapping")


def protocol_members(protocol) -> set[str]:
    """Every name a protocol requires of whoever implements it."""

    members = set(getattr(protocol, "__annotations__", {}))
    members |= {
        name
        for name, value in vars(protocol).items()
        if inspect.isfunction(value)
        and (not name.startswith("_") or name in ("__enter__", "__exit__"))
    }
    return members


class PortImplementationTests(unittest.TestCase):
    """Every port a unit declares must be answered by whoever runs it.

    Protocols here are structural and unchecked at runtime, so a missing member
    is an AttributeError reached only by a real run — the same class of failure
    as a renamed step field, and just as invisible to a green suite.
    """

    PAIRS = (
        (units_target.TargetContext, commands_target_runner.ReusedTargetRun),
        (units_target.TargetContext, commands_target_runner.SpawnedTargetRun),
        (units_target.TargetContext, commands_target_runner.InProcessTargetRun),
        (units_procedure.StepProgress, commands_target_runner.InProcessTargetRun),
        (units_step.StepContext, commands_target_runner.TargetStepContext),
        (units_workflow.ChildSpawnContext, commands_target_runner.SpawnedTargetRun),
        (units_workflow.WorkflowContext, commands_target_runner.TargetRunner),
    )

    def test_every_declared_port_is_implemented(self):
        for protocol, implementation in self.PAIRS:
            required = protocol_members(protocol)
            self.assertTrue(required, f"{protocol.__name__} declares no members")
            for name in sorted(required):
                with self.subTest(protocol=protocol.__name__, port=name):
                    self.assertTrue(
                        hasattr(implementation, name)
                        or name in getattr(implementation, "__annotations__", {}),
                        f"{implementation.__name__} answers no {name!r}",
                    )

    def test_each_protocol_is_a_protocol(self):
        # a port that stops being a Protocol stops being a contract; the pairs
        # above would still pass while checking nothing anyone must satisfy
        for protocol, _ in self.PAIRS:
            with self.subTest(protocol=protocol.__name__):
                self.assertIn(typing.Protocol, protocol.__mro__)


class StepReachabilityTests(unittest.TestCase):
    """A step is only reachable through a procedure.

    The rule the engine already states in `catalog/fan_out.py`, asserted rather
    than assumed: a second step loop is how the two disagree about the mutation
    mark or the order, and only a real run would show it.
    """

    def test_only_the_procedure_unit_runs_a_step(self):
        callers = set()
        for path in sorted(ENGINE.rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "run"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "step"
                ):
                    callers.add(str(path.relative_to(ENGINE)))
        self.assertEqual(callers, {"units/procedure.py"})


class TellDontAskTests(unittest.TestCase):
    """A workflow tells its targets to run; it never runs them itself."""

    def test_workflow_run_calls_run_on_its_loop_variable(self):
        # `context.run(target)` hands the unit to the composition and leaves the
        # target with no behaviour of its own. Only a real run reaches this loop,
        # so the shape is asserted here rather than discovered in production.
        method = next(
            node
            for cls in ast.parse(WORKFLOW_UNIT.read_text()).body
            if isinstance(cls, ast.ClassDef) and cls.name == "Workflow"
            for node in cls.body
            if isinstance(node, ast.FunctionDef) and node.name == "run"
        )
        loop = next(node for node in ast.walk(method) if isinstance(node, ast.For))
        self.assertIsInstance(loop.target, ast.Name)
        called_on = {
            node.func.value.id
            for node in ast.walk(loop)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "run"
            and isinstance(node.func.value, ast.Name)
        }
        self.assertIn(
            loop.target.id,
            called_on,
            "Workflow.run must call run() on the member it is iterating",
        )
        self.assertNotIn(
            "context",
            called_on,
            "Workflow.run must not ask its context to run a member for it",
        )


if __name__ == "__main__":
    unittest.main()
