"""A run label groups one deployment, and must never become part of its identity.

A deployment spans several source repositories — proflow's `env` domain is
eleven targets across five unrelated commit histories — so no commit says which
target revisions were one release. The label does. That makes it useful only if
it travels the whole way down (a fan-out's workflows, a workflow's targets, the
pointer each one publishes), and safe only if nothing compares it: were it part
of an instance address, a composition digest, or the reuse comparison, running
the same deployment under a new label would materialize a fresh instance, reset
staleness, and make `--skip-up-to-date` miss every child.

Both halves are tested here, because either one alone is a trap: propagation
without the exclusion is a correctness bug that only shows up on the second
release, and the exclusion without propagation is a column that is always empty.
"""

import argparse
import inspect
import logging.handlers
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.catalog import workflow as catalog_workflow
from engine.cli import args as cli_args
from engine.commands import selection as commands_selection
from engine.execution import run_context as execution_run_context
from engine.run import addressing as run_addressing
from engine.state import run_store as state_run_store
from engine.state import status as state_status

LABEL = "release-2026.08"

# The run types that publish a revision, or spawn runs that do. A maintenance
# run is out-of-band ctl-state work holding no run state, so it has nothing to
# group and is deliberately offered no label.
LABELLED_RUN_TYPES = ("workflow", "target", "fan_out", "procedure")


def _memory_handler() -> logging.handlers.MemoryHandler:
    return logging.handlers.MemoryHandler(capacity=1024)


class LabelIsOfferedWhereItGroupsSomethingTest(unittest.TestCase):
    def test_a_run_that_publishes_a_revision_accepts_a_label(self):
        for run_type in LABELLED_RUN_TYPES:
            with self.subTest(run_type=run_type):
                parser = argparse.ArgumentParser()
                cli_args.add_common_args(parser, run_type=run_type)
                flags = {
                    option
                    for action in parser._actions
                    for option in action.option_strings
                }
                self.assertIn("--label", flags)

    def test_maintenance_is_offered_no_label(self):
        """Not an oversight: a maintenance run publishes nothing to group."""

        parser = argparse.ArgumentParser()
        cli_args.add_common_args(parser, run_type="maintenance")
        flags = {
            option for action in parser._actions for option in action.option_strings
        }
        self.assertNotIn("--label", flags)


class LabelNormalizationTest(unittest.TestCase):
    """A label is written into records and printed in a status column, so the
    only thing it has to be is one readable line."""

    def test_absent_stays_absent(self):
        self.assertIsNone(cli_args.normalize_run_label(None))

    def test_surrounding_space_is_dropped(self):
        self.assertEqual(cli_args.normalize_run_label(f"  {LABEL} "), LABEL)

    def test_an_empty_label_is_refused(self):
        """An empty string would be a label every unlabelled run shares."""

        for value in ("", "   "):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    cli_args.normalize_run_label(value)

    def test_a_label_that_breaks_a_line_or_a_column_is_refused(self):
        for value in ("two\nlines", "a" * (cli_args.RUN_LABEL_MAX_LENGTH + 1)):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    cli_args.normalize_run_label(value)

    def test_a_label_argparse_would_read_as_a_flag_is_refused(self):
        """`--label=-x` parses fine HERE and dies in the child, which receives
        it as two argv elements — after the parent has taken the lock and written
        a run directory. Refused up front instead."""

        with self.assertRaises(RuntimeError):
            cli_args.normalize_run_label("-x")


class LabelReachesTheRunRecordTest(unittest.TestCase):
    def test_both_run_dir_builders_accept_and_record_it(self):
        """Preflight and full runs write their record in different functions;
        a label recorded by only one is absent from exactly the runs that exit
        early, which is the hardest absence to notice."""

        builders = (
            commands_selection.setup_run_dirs,
            commands_selection.setup_preflight_run_dirs,
        )
        for builder in builders:
            with self.subTest(builder=builder.__name__):
                self.assertIn("label", inspect.signature(builder).parameters)
                with tempfile.TemporaryDirectory(prefix="atlas-run-label-") as tmp:
                    run_dir, *_ = builder(
                        "labelled-run", "provision", "target", "env/seed/baseline",
                        Path(tmp), _memory_handler(),
                        locator_segments=["live"],
                        label=LABEL,
                    )
                    self.assertEqual(
                        state_run_store.load_run_metadata(run_dir).get("label"), LABEL
                    )

    def test_an_unlabelled_run_leaves_no_key(self):
        """Absence stays absence: an empty label would read as a release name
        nobody chose."""

        with tempfile.TemporaryDirectory(prefix="atlas-run-label-") as tmp:
            run_dir, *_ = commands_selection.setup_run_dirs(
                "plain-run", "provision", "target", "env/seed/baseline",
                Path(tmp), _memory_handler(),
                locator_segments=["live"],
            )
            self.assertNotIn("label", state_run_store.load_run_metadata(run_dir))


class LabelPropagatesToChildrenTest(unittest.TestCase):
    def test_a_workflow_child_inherits_its_parents_label(self):
        """One invocation, one label. A child is never given one of its own —
        that is what makes the label group a deployment rather than restate the
        run it sits on."""

        with tempfile.TemporaryDirectory(prefix="atlas-run-label-") as tmp:
            parent_run_dir, *_ = commands_selection.setup_run_dirs(
                "parent-run", "provision", "workflow", "env/seed",
                Path(tmp), _memory_handler(),
                locator_segments=["live"],
                label=LABEL,
            )
            argv = catalog_workflow.build_child_target_command(
                {
                    "ctl_entrypoint": "ctl.py",
                    "ctl_cfg_root": tmp,
                    "ctl_profile": "operator",
                    "ctl_state_local_root": tmp,
                    "execution_runtime_mode": "local",
                    "action": "provision",
                },
                "env/seed/baseline",
                parent_run_dir=parent_run_dir,
                parent_run_id="parent-run",
            )
            self.assertIn("--label", argv)
            self.assertEqual(argv[argv.index("--label") + 1], LABEL)

    def test_an_unlabelled_parent_passes_no_label_flag(self):
        with tempfile.TemporaryDirectory(prefix="atlas-run-label-") as tmp:
            parent_run_dir, *_ = commands_selection.setup_run_dirs(
                "parent-run", "provision", "workflow", "env/seed",
                Path(tmp), _memory_handler(),
                locator_segments=["live"],
            )
            argv = catalog_workflow.build_child_target_command(
                {
                    "ctl_entrypoint": "ctl.py",
                    "ctl_cfg_root": tmp,
                    "ctl_profile": "operator",
                    "ctl_state_local_root": tmp,
                    "execution_runtime_mode": "local",
                    "action": "provision",
                },
                "env/seed/baseline",
                parent_run_dir=parent_run_dir,
                parent_run_id="parent-run",
            )
            self.assertNotIn("--label", argv)


class LabelIsDenormalizedOntoThePointerTest(unittest.TestCase):
    def test_a_published_pointer_carries_the_label(self):
        """Denormalized for the same reason the other facts are: a status read
        opens the pointer and nothing else."""

        self.assertIn("label", state_run_store._COMMITTED_FACT_KEYS)
        with tempfile.TemporaryDirectory(prefix="atlas-run-label-") as tmp:
            run_dir, *_ = commands_selection.setup_run_dirs(
                "published-run", "provision", "target", "env/seed/baseline",
                Path(tmp), _memory_handler(),
                locator_segments=["live"],
                label=LABEL,
            )
            payload = state_status.build_status_payload(
                run_dir, state_run_store.RunStatus.OK
            )
            state_run_store.publish_committed_pointer(run_dir, payload)
            pointer = state_run_store.read_committed_pointer(
                state_run_store.ctl_state_dir_from_run_dir(run_dir), "mutative"
            )
            self.assertEqual(pointer.get("label"), LABEL)


class LabelIsMetadataNeverIdentityTest(unittest.TestCase):
    """The constraint the whole design rests on.

    Each of these is a way a label could silently become identity, and each one
    would show the same symptom: the second release of the same code re-runs
    everything and reports it as new."""

    def test_the_reuse_comparison_never_reads_it(self):
        source = inspect.getsource(state_status.up_to_date_child_revision)
        expected_block = source.split("expected = {", 1)[1].split("}", 1)[0]
        self.assertNotIn("label", expected_block)

    def test_the_execution_context_never_carries_it(self):
        """A label is a CTL argument: it changes nothing a step does and never
        reaches the source repository, so it is absent from the context a target
        runs under."""

        parameters = inspect.signature(
            execution_run_context.build_execution_context
        ).parameters
        self.assertNotIn("label", parameters)

    def test_no_address_is_built_from_it(self):
        """An address is composed, never hand-assembled; a label in one would
        mint a new instance on every rename."""

        for builder in (
            run_addressing.target_instance_address,
            run_addressing.workflow_instance_address,
            run_addressing.compose_state_relpath,
        ):
            with self.subTest(builder=builder.__name__):
                self.assertNotIn("label", inspect.signature(builder).parameters)

    def test_the_workflow_identity_manifest_never_carries_it(self):
        """A workflow instance is addressed by the composition it names. A label
        inside the manifest would change the digest and therefore the instance."""

        doc = catalog_workflow.build_workflow_identity_doc(
            "env/seed",
            ["target/env/seed/baseline/instances/env.type=dev"],
            {"env_type": "dev"},
        )
        self.assertNotIn("label", doc["workflow_instance"])


class LabelIsReadableFromStatusTest(unittest.TestCase):
    def test_the_axis_allowlist_carries_it(self):
        """A namespace row shows only what `AXIS_ORDER` names, so a label that
        reaches the pointer but not this tuple is recorded and invisible."""

        self.assertIn("label", run_addressing.AXIS_ORDER)

    def test_a_row_reports_the_label_of_the_run_that_produced_its_status(self):
        with tempfile.TemporaryDirectory(prefix="atlas-run-label-") as tmp:
            run_dir, *_ = commands_selection.setup_run_dirs(
                "published-run", "provision", "target", "env/seed/baseline",
                Path(tmp), _memory_handler(),
                locator_segments=["live"],
                label=LABEL,
            )
            state_run_store.publish_committed_pointer(
                run_dir,
                state_status.build_status_payload(
                    run_dir, state_run_store.RunStatus.OK
                ),
            )
            namespace_root = Path(tmp) / "live"
            computed = state_status.compute_target_instance_status(
                namespace_root,
                "provision",
                {
                    "kind": "target",
                    "key": "env/seed/baseline",
                    "segments": [],
                    "address": run_addressing.target_instance_address(
                        "env/seed/baseline", []
                    ),
                },
            )
            self.assertEqual(computed.get("label"), LABEL)
            self.assertEqual(run_addressing._axis_row(computed).get("label"), LABEL)


if __name__ == "__main__":
    unittest.main()
