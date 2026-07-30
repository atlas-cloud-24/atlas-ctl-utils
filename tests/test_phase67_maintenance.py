"""§Phase 67: maintenance actions named for their object.

`force-unlock` covered two unrelated jobs chosen by whether `--target` happened
to be present, and the target-shaped one made the engine read step SOURCE to find
where a tool kept its state. The actions now name what they act on, and the tool
lock left the engine entirely.
"""

import argparse
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def _pointer(root: Path, rel: str, when: str) -> Path:
    d = root / rel
    common.write_yaml_file(d / "committed.yaml", {"run_id": "r1", "status": "ok", "committed_at": when})
    return d


class UnlockScopeTest(unittest.TestCase):
    """Two locks with different reach, named rather than inferred."""

    def _args(self, **kw):
        base = dict(
            maintenance_action="unlock-ctl-state", lock_id="r1",
            ctl_state_local_root="/tmp/x", unlock_scope=None, target=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_scope_defaults_to_both(self):
        """A run that dies holds both; clearing one only moves where the next is refused."""
        args = self._args()
        try:
            common.validate_maintenance_args(args)
        except RuntimeError:
            pass
        self.assertEqual("both", args.unlock_scope)

    def test_local_and_both_require_a_local_root(self):
        for scope in ("local", "both"):
            with self.assertRaisesRegex(RuntimeError, "ctl-state-local-root"):
                common.validate_maintenance_args(
                    self._args(unlock_scope=scope, ctl_state_local_root=None)
                )

    def test_remote_needs_no_local_root(self):
        """It touches nothing local."""
        args = self._args(unlock_scope="remote", ctl_state_local_root=None)
        common.validate_maintenance_args(args)
        self.assertEqual("remote", args.unlock_scope)


class ForgetSelectionTest(unittest.TestCase):
    """Two orthogonal filters, both always stated."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        _pointer(self.root, "provision/target/env/core/instances/account=dev", "2026-01-01T00:00:00Z")
        _pointer(self.root, "provision/target/env/core/instances/account=stg", "2026-06-01T00:00:00Z")
        _pointer(self.root, "provision/target/env/acm/instances/account=dev", "2026-06-01T00:00:00Z")

    def tearDown(self):
        self._tmp.cleanup()

    def _addrs(self, older_than, addresses):
        return sorted(i["address"] for i in common.forget_selection(self.root, older_than, addresses))

    def test_a_template_address_takes_every_instance_under_it(self):
        got = self._addrs("any", ["provision/target/env/core"])
        self.assertEqual(
            ["provision/target/env/core/instances/account=dev",
             "provision/target/env/core/instances/account=stg"], got)

    def test_an_instance_address_takes_exactly_one(self):
        got = self._addrs("any", ["provision/target/env/core/instances/account=dev"])
        self.assertEqual(["provision/target/env/core/instances/account=dev"], got)

    def test_age_filters_independently_of_address(self):
        got = self._addrs("2026-03-01", ["all"])
        self.assertEqual(["provision/target/env/core/instances/account=dev"], got)

    def test_both_filters_compose(self):
        self.assertEqual([], self._addrs("2026-03-01", ["provision/target/env/acm"]))

    def test_any_and_all_is_everything(self):
        self.assertEqual(3, len(self._addrs("any", ["all"])))

    def test_a_bad_date_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "ISO-8601"):
            common.forget_selection(self.root, "yesterday", ["all"])


class ForgetValidationTest(unittest.TestCase):
    def _args(self, **kw):
        base = dict(
            maintenance_action="forget", older_than="any", forget_address=["all"],
            ctl_state_local_root="/tmp/x", unlock_scope=None, lock_id=None, target=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_both_filters_are_required(self):
        for missing in ("older_than", "forget_address"):
            with self.assertRaisesRegex(RuntimeError, "both filters are"):
                common.validate_maintenance_args(self._args(**{missing: None}))

    def test_scope_defaults_to_both(self):
        args = self._args()
        common.validate_maintenance_args(args)
        self.assertEqual("both", args.forget_scope)


class ToolLockLeftTheEngineTest(unittest.TestCase):
    def test_the_engine_authors_no_tool_script(self):
        source = Path(common.__file__).read_text()
        for gone in ("TF_STACK_DIR", "TFSTATE_KEY_VAR", "bin/tf.sh"):
            self.assertNotIn(gone, source, f"{gone} still in engine core")


if __name__ == "__main__":
    unittest.main()


class TearsDownDeclarationTest(unittest.TestCase):
    """A teardown declares what it undoes — nothing is inferred from names."""

    BASE = {
        "env/core/baseline": {"target_instance_params": ["env.type", "aws.account"]},
        "env/core/prepare_destroy": {
            "target_instance_params": ["env.type", "aws.account"],
            "tears_down": "env/core/baseline",
        },
    }

    def test_a_valid_declaration_resolves(self):
        links = common.validate_tears_down(self.BASE, "target")
        self.assertEqual({"env/core/prepare_destroy": "env/core/baseline"}, links)

    def test_an_undeclared_target_is_rejected(self):
        broken = {**self.BASE}
        broken["env/core/prepare_destroy"] = {
            **broken["env/core/prepare_destroy"], "tears_down": "env/nope"
        }
        with self.assertRaisesRegex(RuntimeError, "not a declared target"):
            common.validate_tears_down(broken, "target")

    def test_mismatched_axes_are_a_cfg_error(self):
        """Otherwise a teardown appears to succeed while stamping nothing."""
        broken = {**self.BASE}
        broken["env/core/prepare_destroy"] = {
            **broken["env/core/prepare_destroy"], "target_instance_params": ["aws.account"]
        }
        with self.assertRaisesRegex(RuntimeError, "could never name an instance"):
            common.validate_tears_down(broken, "target")

    def test_self_reference_is_rejected(self):
        broken = {**self.BASE}
        broken["env/core/prepare_destroy"] = {
            **broken["env/core/prepare_destroy"], "tears_down": "env/core/prepare_destroy"
        }
        with self.assertRaisesRegex(RuntimeError, "cannot name itself"):
            common.validate_tears_down(broken, "target")

    def test_absent_declaration_is_fine(self):
        self.assertEqual({}, common.validate_tears_down(
            {"env/core/baseline": {"target_instance_params": []}}, "target"))


class ForgetGuardTest(unittest.TestCase):
    """Guards read the axes directly — that is why `state` and `status` are separate."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.rel = "provision/target/env/core/instances/account=dev"
        _pointer(self.root, self.rel, "2026-01-01T00:00:00Z")

    def tearDown(self):
        self._tmp.cleanup()

    def _guard(self, **kw):
        base = dict(accept_orphans=False, cascade=False, referenced_by={})
        base.update(kw)
        return common.forget_guard(self.root, self.rel, **base)

    def test_a_running_instance_is_refused_with_no_override(self):
        """A live run republishes the record, so forgetting it would look like it
        worked and would not have."""
        common.write_yaml_file(
            self.root / self.rel / "in_progress" / "STATUS.yaml",
            {"run_id": "r2", "action": "provision"},
        )
        self.assertIn("in progress", self._guard(accept_orphans=True, cascade=True))

    def test_a_provisioned_instance_needs_the_orphan_acceptance(self):
        self.assertIn("accept-orphaned-resources", self._guard())
        self.assertIsNone(self._guard(accept_orphans=True))

    def test_a_referenced_instance_needs_cascade(self):
        refs = {self.rel: {"provision/workflow/env/seed/instances/sha256=abc"}}
        self.assertIn("cascade", self._guard(accept_orphans=True, referenced_by=refs))
        self.assertIsNone(
            self._guard(accept_orphans=True, cascade=True, referenced_by=refs)
        )

    def test_workflow_references_are_discovered_from_child_revisions(self):
        common.write_yaml_file(
            self.root / "provision/workflow/env/seed/instances/sha256=abc/committed.yaml",
            {"run_id": "w1", "status": "ok",
             "child_revisions": [{"address": "env/core/instances/account=dev", "run_id": "r1"}]},
        )
        refs = common.workflow_references(self.root)
        self.assertIn(self.rel, refs)


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
        self.assertIn("skipped", common.release_remote_mutation_lock(s, "r1"))
        self.assertFalse(s.deleted)

    def test_a_different_holder_is_refused(self):
        """Releasing a live holder's lock gives two concurrent mutating runs."""
        s = self._Syncer({"run_id": "someone-else"})
        with self.assertRaisesRegex(RuntimeError, "held by run"):
            common.release_remote_mutation_lock(s, "r1")
        self.assertFalse(s.deleted)

    def test_the_named_holder_is_released(self):
        s = self._Syncer({"run_id": "r1"})
        self.assertEqual("released", common.release_remote_mutation_lock(s, "r1"))
        self.assertTrue(s.deleted)


class TearsDownStampTest(unittest.TestCase):
    """A teardown run marks the instance it declared it tears down.

    Without the stamp the relationship lives only in cfg: the teardown completes
    and the target it undid still reads `provisioned`, because nothing wrote a
    destroy pointer at that address.
    """

    def _payload(self, root: Path, **kw):
        base = dict(
            run_id="d1", action="destroy", result_name="env/core/prepare_destroy",
            tears_down="env/core/baseline", instance=["account=dev"],
            ctl_state_local_root=str(root), ctl_state_locator=[],
            updated_at="2026-07-29T12:00:00Z",
        )
        base.update(kw)
        return base

    def test_a_destroy_stamps_the_named_instance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            address = common.stamp_torn_down_instance(root, self._payload(root))
            self.assertEqual("env/core/baseline/instances/account=dev", address)
            stamped = common.read_committed_pointer(
                root / "destroy/target/env/core/baseline/instances/account=dev"
            )
            self.assertEqual("ok", stamped["status"])
            self.assertEqual("env/core/prepare_destroy", stamped["torn_down_by"])

    def test_the_stamp_carries_this_run_instance_not_just_the_key(self):
        """The `uid` lesson: a re-provisioned instance must not inherit a stale claim."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            common.stamp_torn_down_instance(root, self._payload(root, instance=["account=stg"]))
            self.assertTrue(
                (root / "destroy/target/env/core/baseline/instances/account=stg").is_dir()
            )
            self.assertFalse(
                (root / "destroy/target/env/core/baseline/instances/account=dev").is_dir()
            )

    def test_a_plan_stamps_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertIsNone(
                common.stamp_torn_down_instance(root, self._payload(root, action="plan"))
            )

    def test_a_target_without_the_declaration_stamps_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertIsNone(
                common.stamp_torn_down_instance(root, self._payload(root, tears_down=None))
            )

    def test_the_stamped_instance_then_reads_destroyed(self):
        """The point of the whole mechanism."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            common.write_yaml_file(
                root / "provision/target/env/core/baseline/instances/account=dev/committed.yaml",
                {"run_id": "p1", "status": "ok", "committed_at": "2026-07-01T00:00:00Z"},
            )
            common.stamp_torn_down_instance(root, self._payload(root))
            rows = common.compute_namespace_status_map(root)
            block = rows["target"]["env/core/baseline"]["instances"]["account=dev"]
            self.assertEqual("destroyed", block["deployment"]["state"])
