"""Maintenance actions named for their object.

`force-unlock` covered two unrelated jobs chosen by whether `--target` happened
to be present, and the target-shaped one made the engine read step SOURCE to find
where a tool kept its state. The actions now name what they act on, and the tool
lock left the engine entirely.
"""

import argparse
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import contextlib

from engine.commands import selection as commands_selection
from engine.kernel import yaml_io as kernel_yaml_io
from engine.state import status as state_status
from engine.state import sync as state_sync
from engine_surface import engine_source


def _pointer(root: Path, rel: str, when: str) -> Path:
    d = root / rel
    kernel_yaml_io.write_yaml_file(
        d / "committed.yaml", {"run_id": "r1", "status": "ok", "committed_at": when}
    )
    return d


class UnlockScopeTest(unittest.TestCase):
    """Two locks with different reach, named rather than inferred."""

    def _args(self, **kw):
        base = dict(
            maintenance_action="unlock-ctl-state",
            lock_id="r1",
            ctl_state_local_root="/tmp/x",
            unlock_scope=None,
            target=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_scope_defaults_to_both(self):
        """A run that dies holds both; clearing one only moves where the next is refused."""

        args = self._args()
        with contextlib.suppress(RuntimeError):
            commands_selection.validate_run_args("maintenance", args)
        self.assertEqual("both", args.unlock_scope)

    def test_local_and_both_require_a_local_root(self):
        for scope in ("local", "both"):
            with self.assertRaisesRegex(RuntimeError, "ctl-state-local-root"):
                commands_selection.validate_run_args(
                    "maintenance", self._args(unlock_scope=scope, ctl_state_local_root=None)
                )

    def test_remote_needs_no_local_root(self):
        """It touches nothing local."""

        args = self._args(unlock_scope="remote", ctl_state_local_root=None)
        commands_selection.validate_run_args("maintenance", args)
        self.assertEqual("remote", args.unlock_scope)


class ForgetSelectionTest(unittest.TestCase):
    """Two orthogonal filters, both always stated."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        _pointer(self.root, "target/env/core/instances/account=dev", "2026-01-01T00:00:00Z")
        _pointer(self.root, "target/env/core/instances/account=stg", "2026-06-01T00:00:00Z")
        _pointer(self.root, "target/env/acm/instances/account=dev", "2026-06-01T00:00:00Z")

    def tearDown(self):
        self._tmp.cleanup()

    def _addrs(self, older_than, addresses):
        return sorted(
            i["address"] for i in state_status.forget_selection(self.root, older_than, addresses)
        )

    def test_both_filters_compose(self):
        self.assertEqual([], self._addrs("2026-03-01", ["target/env/acm"]))

    def test_a_bad_date_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "ISO-8601"):
            state_status.forget_selection(self.root, "yesterday", ["all"])


class ForgetValidationTest(unittest.TestCase):
    def _args(self, **kw):
        base = dict(
            maintenance_action="forget",
            older_than="any",
            forget_address=["all"],
            ctl_state_local_root="/tmp/x",
            unlock_scope=None,
            lock_id=None,
            target=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_both_filters_are_required(self):
        for missing in ("older_than", "forget_address"):
            with self.assertRaisesRegex(RuntimeError, "both filters are"):
                commands_selection.validate_run_args("maintenance", self._args(**{missing: None}))

    def test_scope_defaults_to_both(self):
        args = self._args()
        commands_selection.validate_run_args("maintenance", args)
        self.assertEqual("both", args.forget_scope)


class ToolLockLeftTheEngineTest(unittest.TestCase):
    def test_the_engine_authors_no_tool_script(self):
        source = engine_source()
        for gone in ("TF_STACK_DIR", "TFSTATE_KEY_VAR", "bin/tf.sh"):
            self.assertNotIn(gone, source, f"{gone} still in engine core")


if __name__ == "__main__":
    unittest.main()


class ForgetGuardTest(unittest.TestCase):
    """Guards read the axes directly — that is why `state` and `status` are separate."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.rel = "target/env/core/instances/account=dev"
        _pointer(self.root, self.rel, "2026-01-01T00:00:00Z")

    def tearDown(self):
        self._tmp.cleanup()

    def _guard(self, **kw):
        base = dict(accept_orphans=False, cascade=False, referenced_by={})
        base.update(kw)
        return state_status.forget_guard(self.root, self.rel, **base)

    def test_a_referenced_instance_needs_cascade(self):
        refs = {self.rel: {"workflow/env/seed/instances/sha256=abc"}}
        self.assertIn("cascade", self._guard(accept_orphans=True, referenced_by=refs))
        self.assertIsNone(self._guard(accept_orphans=True, cascade=True, referenced_by=refs))


class UnlockRemoteTest(unittest.TestCase):
    """The namespace lock is released by ID, never blindly."""

    class _Syncer:
        def __init__(self, doc):
            self.doc, self.deleted = doc, False

        def read_mutation_lock(self):
            return self.doc

        def delete_mutation_lock(self):
            self.deleted = True

    def test_absent_lock_is_a_skip_not_a_failure(self):
        s = self._Syncer(None)
        self.assertIn("skipped", state_sync.release_remote_mutation_lock(s, "r1"))
        self.assertFalse(s.deleted)

    def test_a_different_holder_is_refused(self):
        """Releasing a live holder's lock gives two concurrent mutating runs."""

        s = self._Syncer({"run_id": "someone-else"})
        with self.assertRaisesRegex(RuntimeError, "held by run"):
            state_sync.release_remote_mutation_lock(s, "r1")
        self.assertFalse(s.deleted)

    def test_the_named_holder_is_released(self):
        s = self._Syncer({"run_id": "r1"})
        self.assertEqual("released", state_sync.release_remote_mutation_lock(s, "r1"))
        self.assertTrue(s.deleted)
