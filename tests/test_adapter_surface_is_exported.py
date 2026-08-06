"""Every adapter attribute the engine reaches for is exported by the adapter.

An adapter is a separate PACKAGE reached by name, so the engine's calls into it
are resolved at run time and nothing earlier notices a missing one. A function
can exist in `atlas_ctl_adapter_aws/execution.py`, be called by the engine, and
still fail — because the package `__init__` never re-exported it, and
`get_adapter()` returns the package.

That failed a real `ctl.py workflow` while every suite was green, so the check is
static and derived: parse the ENGINE for `adapter.<name>` and require the adapter
package to expose each one. Deriving beats listing — a hardcoded list rots into a
false pass the day someone adds a call.
"""
import ast
import importlib
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE = REPO_ROOT / "runners" / "engine"

sys.path.insert(0, str(REPO_ROOT / "runners"))
sys.path.insert(0, str(REPO_ROOT.parent / "atlas-ctl-adapter-aws"))

# Locals that hold an adapter. Naming them is the narrow part: `adapter.foo` is
# unambiguous, while every other attribute access in the engine is not.
ADAPTER_NAMES = frozenset({"adapter"})


def _adapter_attributes() -> set[str]:
    names: set[str] = set()
    for path in ENGINE.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id in ADAPTER_NAMES
                and isinstance(node.ctx, ast.Load)
            ):
                names.add(node.attr)
            # `get_adapter(...).name(...)` — the call result used directly
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and node.value.func.id == "get_adapter"
            ):
                names.add(node.attr)
    return names


class AdapterSurfaceTest(unittest.TestCase):
    def setUp(self):
        self.adapter = importlib.import_module("atlas_ctl_adapter_aws")

    def test_the_scan_finds_real_calls(self):
        """Guards the check below: an empty scan would pass for the wrong reason."""

        found = _adapter_attributes()
        self.assertGreater(len(found), 5, found)
        self.assertIn("target_consent", found)

    def test_every_attribute_the_engine_uses_is_exported(self):
        missing = sorted(
            name for name in _adapter_attributes()
            if not hasattr(self.adapter, name)
        )
        self.assertEqual(
            [], missing,
            "the engine reaches for these on the adapter PACKAGE, and a function "
            "that exists in a submodule but is not re-exported fails only at run "
            "time, after a run has already started:\n" + "\n".join(missing),
        )

    def test_a_submodule_function_alone_would_not_satisfy_it(self):
        """The failure mode, asserted directly: defined but not exported."""

        from atlas_ctl_adapter_aws import execution

        self.assertTrue(hasattr(execution, "resolve_execution_identity_references"))
        self.assertTrue(hasattr(self.adapter, "resolve_execution_identity_references"))


if __name__ == "__main__":
    unittest.main()
