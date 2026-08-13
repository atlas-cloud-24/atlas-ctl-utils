"""Every engine name a runner reaches for must exist.

The orchestrator holds the engine as ONE object and reaches through it by
attribute, resolved at call time. A name that is renamed or deleted therefore
fails at the moment the runner runs it, not at import — and the runners are
reached only by a real run, so a green suite says nothing about them.

That is not hypothetical: `setup_run_workspace` was deleted by a refactor whose
span happened to cover it, 941 tests passed, and the next `ctl.py workflow` died
with `module 'engine.commands.selection' has no attribute 'setup_run_workspace'`.
"""

import ast
import importlib
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

ENGINE = REPO_ROOT / "runners" / "engine"
RUNNERS = REPO_ROOT.parent / "atlas-ctl-orchestrator" / "runners"


def engine_modules() -> dict[str, str]:
    """The alias -> dotted path map the orchestrator's Engine facade builds."""

    modules = {}
    for path in sorted(ENGINE.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        dotted = ".".join(path.relative_to(ENGINE).with_suffix("").parts)
        modules[dotted.replace(".", "_")] = f"engine.{dotted}"
    return modules


class RunnerEngineSurfaceTests(unittest.TestCase):
    """Resolve every `engine.<alias>.<name>` a runner writes."""

    def test_every_engine_attribute_a_runner_reaches_for_exists(self):
        modules = engine_modules()
        checked = 0
        for path in sorted(RUNNERS.rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                # engine.<alias>.<name>
                if not (
                    isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Attribute)
                    and isinstance(node.value.value, ast.Name)
                    and node.value.value.id == "engine"
                ):
                    continue
                alias, attribute = node.value.attr, node.attr
                with self.subTest(reference=f"engine.{alias}.{attribute}", file=path.name):
                    self.assertIn(alias, modules, f"engine has no module {alias!r}")
                    module = importlib.import_module(modules[alias])
                    self.assertTrue(
                        hasattr(module, attribute),
                        f"{modules[alias]} has no {attribute!r}, "
                        f"reached from {path.name}:{node.lineno}",
                    )
                    checked += 1
        self.assertGreater(checked, 50, "the scan found almost no engine references")


class RunContextAndRequestAgreeTests(unittest.TestCase):
    """A field on the context and the same field on the request it carries.

    `prepare()` freezes the request before a workflow's plt and guardrails cfg
    have been fetched — that happens after the preflight gate. Assigning the
    resolved root to the CONTEXT and not to the request leaves the run verifying
    guardrails against `None`, which is reached only by a real run.
    """

    COMMON = Path(__file__).resolve().parents[2] / "atlas-ctl-orchestrator" / "runners"

    def test_every_shared_field_assigned_on_the_context_refreshes_the_request(self):
        from engine.run import request as run_request

        shared = set(run_request.RunRequest.__dataclass_fields__)
        tree = ast.parse((self.COMMON / "_runner_common.py").read_text())
        for function in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
            assigned, refreshed = set(), False
            for node in ast.walk(function):
                if not isinstance(node, ast.Assign):
                    continue
                for target in node.targets:
                    if not (
                        isinstance(target, ast.Attribute)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == "ctx"
                    ):
                        continue
                    if target.attr == "request":
                        refreshed = True
                    elif target.attr in shared:
                        assigned.add(target.attr)
            if assigned:
                with self.subTest(function=function.name, fields=sorted(assigned)):
                    self.assertTrue(
                        refreshed,
                        f"{function.name} assigns {sorted(assigned)} on the context but "
                        "never refreshes ctx.request, so the run keeps the stale value",
                    )


if __name__ == "__main__":
    unittest.main()
