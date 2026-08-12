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
import re
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATOR = REPO_ROOT.parent / "atlas-ctl-orchestrator" / "runners"

sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cli import args as cli_args  # noqa: E402

# A runner file maps to the run_type its parser is built for. `status` builds a
# different parser and was left out, which made the read check skip the one
# runner an operator invokes most.
RUNNERS = {
    "workflow.py": "workflow",
    "target.py": "target",
    "fan_out.py": "fan_out",
    "procedure.py": "procedure",
    "maintenance.py": "maintenance",
    "status.py": "status",
}

# What each internal `--parent-*` argument is for, and therefore which run types
# may declare it. Exactly two places build a child argv:
# `build_child_target_command`, which always spawns a `target`, and the fan-out,
# whose children are a `workflow` or a `target`. A procedure, a maintenance run
# and a fan-out are always top-level, so anything else accepting these would be
# taking a flag nothing can send and then ignoring it.
PARENT_ARGS_BY_SENDER = {
    "parent_graph_provisions_ctl_state_backend": {"workflow", "target"},
    "parent_ctl_state_backend_absence_confirmed": {"workflow", "target"},
    "parent_fan_out_run_id": {"workflow", "target"},
    "parent_workflow_run_id": {"target"},
    "parent_workflow_instance_address": {"target"},
}


def _reads_itself(assignment: ast.Assign, attribute: str) -> bool:
    """Whether `args.x = ...` computes its new value FROM `args.x`.

    That is normalisation of a parsed argument, not an attachment: the attribute
    has to already exist for the line to run. Counting it as an attachment is a
    hole — `args.label = normalize_run_label(args.label)` made every reader of
    `args.label` pass the check below even with `--label` deleted from the
    parser, which is the exact runtime AttributeError this file exists to
    prevent.
    """

    for node in ast.walk(assignment.value):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "args"
            and node.attr == attribute
        ):
            return True
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("getattr", "hasattr")
            and len(node.args) >= 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "args"
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value == attribute
        ):
            return True
    return False


def _attribute_assignments(paths: list[Path]) -> set[str]:
    """`args.<name> = ...` anywhere in the shared setup, ATTACHMENTS only.

    Derived rather than listed: several attributes are legitimately attached
    AFTER parsing — `execution_params` by `finalize_common_args`, `ctl_ref_policy`
    by the shared runner context — and a hardcoded allowlist would rot into
    either a false failure or a hole.

    An assignment that reads the same attribute back is excluded, because it
    normalises something the parser already produced rather than creating it.
    """

    assigned: set[str] = set()
    for path in paths:
        if not path.is_file():
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "args"
                    and not _reads_itself(node, target.attr)
                ):
                    assigned.add(target.attr)
    return assigned


def _attribute_reads(path: Path) -> set[str]:
    """`args.<name>` reads in the file, excluding `getattr(args, ...)`."""

    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    assigned: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "args"
        ):
            if isinstance(node.ctx, ast.Store):
                assigned.add(node.attr)
            else:
                names.add(node.attr)
    # A `getattr(args, "x", default)` read is a STRING, not an Attribute node, so
    # it never enters `names` — the author's default already says the attribute
    # may be absent, which is a different contract from a bare read.
    return names - assigned


def _consumer_sources() -> list[Path]:
    """Every file that may legitimately consume a parsed argument.

    Includes `args.py` itself: several arguments are normalised there and never
    touched again (`--execution-params` becomes `execution_params`), so a scan
    that skipped it would call them dead.
    """

    return (
        sorted(ORCHESTRATOR.rglob("*.py"))
        + [ORCHESTRATOR.parent / "ctl.py"]
        + sorted((REPO_ROOT / "runners/engine").rglob("*.py"))
    )


def _is_consumed(dest: str, sources: dict[Path, str]) -> bool:
    """Whether anything reads this argument.

    Whitespace is collapsed first, because a `getattr(args, "x", None)` wrapped
    across two lines is the normal shape in these files and a line-wise scan
    reports it as dead.

    A bare quoted name counts: several consumers read a LIST of field names
    (`any(getattr(args, field, None) for field in ("source", "ref", ...))`), and
    that is a real read even though the attribute never appears as `args.source`.

    That last pattern is deliberately loose, and biased toward NOT failing: a
    dest whose name coincides with an unrelated dict key reads as consumed. The
    bias is the right way round — a false failure here blocks work over a name
    collision, while a miss leaves one dead argument for the next audit.
    """

    patterns = (
        rf"args\.{dest}\b",
        rf'getattr\( args , "{dest}"',
        rf'hasattr\( args , "{dest}"',
        rf'"{dest}"',
    )
    return any(re.search(pattern, text) for text in sources.values() for pattern in patterns)


def _declared(run_type: str) -> set[str]:
    import argparse

    parser = argparse.ArgumentParser()
    if run_type == "status":
        cli_args.add_status_args(parser)
    else:
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
            [],
            offenders,
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

    def test_no_declared_argument_is_unread(self):
        """The mirror image of the check above, and the half nothing covered.

        A missing argument fails loudly at runtime; a REDUNDANT one never fails
        at all. It is advertised in `--help`, accepted on the command line, and
        silently ignored — which is worse than rejecting it, because the operator
        believes it took effect. `--parent-graph-provisions-ctl-state-backend` was
        offered by three runners nothing could ever send it to.
        """

        sources = {
            path: re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))
            for path in _consumer_sources()
            if path.is_file()
        }
        offenders: list[str] = []
        for run_type in sorted(set(RUNNERS.values())):
            for dest in sorted(_declared(run_type)):
                if dest == "help":
                    continue
                if not _is_consumed(dest, sources):
                    offenders.append(f"{run_type}: {dest}")
        self.assertEqual(
            [],
            offenders,
            "an argument is declared but nothing reads it, so a run accepts it "
            "and ignores it:\n" + "\n".join(offenders),
        )

    def test_the_unread_check_would_catch_a_dead_argument(self):
        """Guards the guard: a matcher that said yes to everything would pass it.

        The multi-line case is pinned explicitly, because it is the one that
        silently breaks the check — `parent_workflow_instance_address` is only
        ever read through a `getattr(args, ...)` wrapped across two lines, and a
        line-wise scan reports it dead.
        """

        sources = {
            path: re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))
            for path in _consumer_sources()
            if path.is_file()
        }
        self.assertTrue(_is_consumed("ctl_profile", sources))
        self.assertTrue(_is_consumed("parent_workflow_instance_address", sources))
        self.assertTrue(_is_consumed("label", sources))
        self.assertFalse(_is_consumed("an_argument_that_was_never_declared", sources))

    def test_an_argument_is_declared_only_where_something_can_send_it(self):
        """The other half: an argument nothing reads fails no test, so a runner
        can accept a flag that does nothing. These are the ones with a knowable
        sender, so they are the ones that can be checked.

        A procedure once accepted `--parent-graph-provisions-ctl-state-backend`
        and passed it to the pipeline, from a parent it can never have.
        """

        offenders: list[str] = []
        for run_type in set(RUNNERS.values()) - {"status"}:
            declared = _declared(run_type)
            for argument, senders in PARENT_ARGS_BY_SENDER.items():
                if (argument in declared) != (run_type in senders):
                    offenders.append(
                        f"{run_type}: {argument} "
                        f"{'declared but' if argument in declared else 'missing though'} "
                        f"senders are {sorted(senders)}"
                    )
        self.assertEqual([], offenders, "\n".join(offenders))

    def test_a_removed_argument_would_be_caught(self):
        """The failure mode this exists for, asserted directly."""

        declared = _declared("workflow")
        self.assertNotIn("plt_overlays", declared)
        self.assertNotIn("ctl_variants", declared)


if __name__ == "__main__":
    unittest.main()
