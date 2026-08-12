"""Every entry point still imports.

This is the third time a rename broke an entry point while every suite stayed
green. The pattern is always the same: a module-level `from engine.x import y`
in a file that no test imports, so nothing exercises the import until someone
runs the tool.

The suite cannot cover these by running them — they materialize cfg, take locks
and reach a provider — so the check is what it can honestly be: each entry point
must IMPORT, in a subprocess that only knows what a real invocation knows. An
import failure is the whole failure class here, because these files do their work
in `main()` behind `if __name__ == "__main__"`.

`ctl.py` is deliberately included: it is the entry point that broke first, and it
dispatches to runners in a sibling repository.
"""

import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
STACK = REPO_ROOT.parent
ADAPTER = STACK / "atlas-ctl-adapter-aws"
ORCHESTRATOR = STACK / "atlas-ctl-orchestrator"


def engine_entry_points() -> list[Path]:
    """Discovered, not listed: a new tool is covered the day it lands."""
    found = sorted(
        path
        for path in (REPO_ROOT / "cfg").glob("*.py")
        if "__pycache__" not in path.parts and "if __name__" in path.read_text(encoding="utf-8")
    )
    if not found:
        raise RuntimeError(
            f"no entry points found under {REPO_ROOT / 'cfg'} — this check would pass vacuously"
        )
    return found


def import_in_subprocess(path: Path, *, extra_paths: list[Path]) -> subprocess.CompletedProcess:
    """Import a file the way a real invocation would, and nothing more.

    A subprocess because `conftest.py` has already fixed up `sys.path` for the
    suite, so an in-process import proves something no real invocation enjoys —
    a green test for a reason that does not apply to the entry point.
    """
    joined = ":".join(str(p) for p in extra_paths)
    return subprocess.run(
        [
            sys.executable,
            "-c",
            # registered in sys.modules BEFORE execution: `@dataclass` resolves
            # its annotations through `sys.modules[cls.__module__]`, so a module
            # executed without being registered fails for a reason the real
            # invocation never hits
            "import importlib.util, sys;"
            # NOT under `__main__`, which would run the tool instead of importing it
            f"spec = importlib.util.spec_from_file_location('entry', {str(path)!r});"
            "mod = importlib.util.module_from_spec(spec);"
            "sys.modules['entry'] = mod;"
            "spec.loader.exec_module(mod)",
        ],
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": joined, "HOME": str(Path.home())},
        capture_output=True,
        text=True,
        timeout=120,
    )


class EntryPointsImportTest(unittest.TestCase):
    def test_entry_points_are_discovered(self):
        """Guards the check: an empty list passes every assertion below."""
        self.assertTrue(engine_entry_points())

    def test_every_engine_tool_imports(self):
        for path in engine_entry_points():
            with self.subTest(entry_point=path.name):
                result = import_in_subprocess(path, extra_paths=[REPO_ROOT / "runners", ADAPTER])
                self.assertEqual(
                    0,
                    result.returncode,
                    f"{path.name} does not import:\n{result.stderr}",
                )

    def test_the_ctl_entry_point_imports(self):
        ctl = ORCHESTRATOR / "ctl.py"
        self.assertTrue(ctl.is_file(), f"ctl entry point not found: {ctl}")
        result = import_in_subprocess(
            ctl, extra_paths=[ORCHESTRATOR / "runners", REPO_ROOT / "runners", ADAPTER]
        )
        self.assertEqual(0, result.returncode, f"ctl.py does not import:\n{result.stderr}")


if __name__ == "__main__":
    unittest.main()
