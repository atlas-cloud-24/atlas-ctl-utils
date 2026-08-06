"""Every `args.<name>` a runner reads is an argument its parser declares.

An import check is not enough. Removing `--plt-overlays` left four runners
reading `args.plt_overlays`, every suite stayed green, and a real
`ctl.py workflow` died at `AttributeError: 'Namespace' object has no attribute
'plt_overlays'` — after taking a mutation lock and writing a run directory.

The gap is structural: a runner's argument reads live in `main()`, which the
suite cannot call (it materializes cfg, takes locks, reaches a provider). So the
check is static and exhaustive — parse the runner FILE, collect every
`args.<name>` it reads, and require the parser to declare it.

`getattr(args, "name", default)` is deliberately NOT flagged: the default is the
author saying the attribute may be absent, which is a different contract from a
bare read.
"""
import ast
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATOR = REPO_ROOT.parent / "atlas-ctl-orchestrator" / "runners"

sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cli import args as cli_args  # noqa: E402

# A runner file maps to the run_type its parser is built for.
RUNNERS = {
    "workflow.py": "workflow",
    "target.py": "target",
    "fan_out.py": "fan_out",
    "procedure.py": "procedure",
    "maintenance.py": "maintenance",
}

def _attribute_assignments(paths: list[Path]) -> set[str]:
    """`args.<name> = ...` anywhere in the shared setup.

    Derived rather than listed: several attributes are legitimately attached
    AFTER parsing — `execution_params` by `finalize_common_args`, `ctl_ref_policy`
    by the shared runner context — and a hardcoded allowlist would rot into
    either a false failure or a hole.
    """

    assigned: set[str] = set()
    for path in paths:
        if not path.is_file():
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "args"
                and isinstance(node.ctx, ast.Store)
            ):
                assigned.add(node.attr)
    return assigned


def _attribute_reads(path: Path) -> set[str]:
    """`args.<name>` reads in the file, excluding `getattr(args, ...)`."""

    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    assigned: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "args":
            if isinstance(node.ctx, ast.Store):
                assigned.add(node.attr)
            else:
                names.add(node.attr)
    # A `getattr(args, "x", default)` read is a STRING, not an Attribute node, so
    # it never enters `names` — the author's default already says the attribute
    # may be absent, which is a different contract from a bare read.
    return names - assigned


def _declared(run_type: str) -> set[str]:
    import argparse

    parser = argparse.ArgumentParser()
    cli_args.add_common_args(parser, run_type=run_type)
    return {action.dest for action in parser._actions}


class RunnerArgsAreDeclaredTest(unittest.TestCase):
    def test_runner_files_exist(self):
        """Guards the suite: a moved runner would make every check below vacuous."""

        missing = [name for name in RUNNERS if not (ORCHESTRATOR / name).is_file()]
        self.assertEqual([], missing, f"runner files not found under {ORCHESTRATOR}")

    def test_every_read_argument_is_declared_by_the_parser(self):
        attached = _attribute_assignments(
            [REPO_ROOT / "runners/engine/cli/args.py"]
            + sorted(ORCHESTRATOR.glob("*.py"))
            + sorted((ORCHESTRATOR / "bootstrap").glob("*.py"))
        )
        offenders: list[str] = []
        for name, run_type in RUNNERS.items():
            path = ORCHESTRATOR / name
            if not path.is_file():
                continue
            declared = _declared(run_type) | attached
            for attribute in sorted(_attribute_reads(path)):
                if attribute not in declared:
                    offenders.append(f"{name}: args.{attribute}")
        self.assertEqual(
            [], offenders,
            "a runner reads an argument its parser does not declare, which fails "
            "at runtime with AttributeError after the run has already started:\n"
            + "\n".join(offenders),
        )

    def test_the_check_sees_real_reads(self):
        """The parser is not empty and the AST walk finds reads — without this,
        an empty result would pass for the wrong reason."""

        reads = _attribute_reads(ORCHESTRATOR / "workflow.py")
        self.assertIn("ctl_profile", reads)
        self.assertIn("ctl_profile", _declared("workflow"))

    def test_a_removed_argument_would_be_caught(self):
        """The failure mode this exists for, asserted directly."""

        declared = _declared("workflow")
        self.assertNotIn("plt_overlays", declared)
        self.assertNotIn("ctl_variants", declared)


if __name__ == "__main__":
    unittest.main()
