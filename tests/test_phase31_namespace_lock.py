import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


BACKENDS = (
    "ctl_state_backends:\n"
    "  live:\n"
    "    selectors:\n"
    "      match:\n"
    "        execution_context.params.landing_zone: live\n"
    "    provider: aws\n"
    "    backend_type: s3\n"
    "    bucket_name: oxygen-live-ctl-state\n"
    "    bucket_region: eu-west-2\n"
    "    execution_identity_keys:\n"
    "      read: ctl_state_reader\n"
    "      sync: ctl_state_synchronizer\n"
    "      maintenance: ctl_state_maintainer\n"
    "  canary:\n"
    "    selectors:\n"
    "      match:\n"
    "        execution_context.params.landing_zone: canary\n"
    "    provider: aws\n"
    "    backend_type: s3\n"
    "    bucket_name: oxygen-canary-ctl-state\n"
    "    bucket_region: eu-west-2\n"
)

def ctx(**params):
    return {f"execution_context.params.{k}": v for k, v in params.items()}


class NamespaceResolverTests(unittest.TestCase):
    """§Phase 31 item 3 — exact-one namespace resolution."""

    def _root(self, tmp: str, backends: str = BACKENDS) -> Path:
        root = Path(tmp)
        (root / "backends.yaml").write_text(backends, encoding="utf-8")
        return root

    def test_resolves_exactly_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            key, backend = common.resolve_ctl_state_namespace(root, ctx(landing_zone="live"))
            self.assertEqual(key, "live")
            self.assertEqual(backend["bucket_name"], "oxygen-live-ctl-state")
            self.assertEqual(backend["execution_identity_keys"]["sync"], "ctl_state_synchronizer")

    def test_zero_matches_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            with self.assertRaisesRegex(RuntimeError, "matched 0"):
                common.resolve_ctl_state_namespace(root, ctx(landing_zone="qa"))

    def test_selectorless_backend_never_resolves(self):
        # a backend without selectors is not a namespace (item 13c): it can't
        # be auto-selected, so a context matching nothing is a hard error.
        no_sel = BACKENDS.replace(
            "  canary:\n"
            "    selectors:\n"
            "      match:\n"
            "        execution_context.params.landing_zone: canary\n",
            "  canary:\n",
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, no_sel)
            with self.assertRaisesRegex(RuntimeError, "matched 0"):
                common.resolve_ctl_state_namespace(root, ctx(landing_zone="canary"))


class FanOutNamespaceGateTests(unittest.TestCase):
    """§Phase 31 item 3 — cross-namespace fan-out rejection."""

    def _root(self, tmp: str) -> Path:
        root = Path(tmp)
        (root / "backends.yaml").write_text(BACKENDS, encoding="utf-8")
        (root / "params.yaml").write_text(
            "execution_params:\n  main_tag: oxygen\n", encoding="utf-8"
        )
        return root

    def _children(self, *zones):
        return [
            {"label": f"wf[{z}]", "params": {"landing_zone": z}} for z in zones
        ]

    def test_same_namespace_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            ns = common.require_unique_fan_out_namespace(
                root, self._children("live", "live"),
                action="provision", ctl_profile=None,
                execution_params={}, execution_runtime_mode="local",
            )
            self.assertEqual(ns, "live")

    def test_cross_namespace_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            with self.assertRaisesRegex(RuntimeError, "must not cross namespaces"):
                common.require_unique_fan_out_namespace(
                    root, self._children("live", "canary"),
                    action="provision", ctl_profile=None,
                    execution_params={}, execution_runtime_mode="local",
                )


class WorkflowCompositionTests(unittest.TestCase):
    """§Phase 31 item 7 — composition sha + identity doc."""

    ADDRS = ["target/env/tfstate_backend/account=stg/env_type=stg"]

    def test_sha_is_deterministic_and_order_sensitive(self):
        a = common.workflow_composition_sha256(["x", "y"])
        b = common.workflow_composition_sha256(["x", "y"])
        c = common.workflow_composition_sha256(["y", "x"])
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertEqual(len(a), 64)

    def test_identity_doc_facts_only(self):
        doc = common.build_workflow_identity_doc(
            "env/bootstrap", self.ADDRS, {"account": "stg", "env_type": "stg"}
        )
        wf = doc["workflow_instance"]
        self.assertEqual(wf["workflow"], "env/bootstrap")
        self.assertEqual(wf["targets"], self.ADDRS)
        # the composition sha is the instance DIR NAME — never duplicated in the doc
        self.assertNotIn("composition_sha256", wf)
        self.assertNotIn("status", wf)

    def test_target_instance_address_forms(self):
        self.assertEqual(common.target_instance_address("env/core", []), "env/core")
        self.assertEqual(
            common.target_instance_address("env/core", ["account=dev", "env_type=dev"]),
            "env/core/account=dev/env_type=dev",
        )


class MutationLockTests(unittest.TestCase):
    """§Phase 31 Q1b — interim global mutation lock decision logic."""

    def test_mutating_acquires_free_lock(self):
        out = common.evaluate_mutation_lock(None, action="provision", run_id="r1")
        self.assertEqual(out["decision"], "acquire")
        self.assertEqual(out["lock_doc"]["run_id"], "r1")

    def test_non_mutating_proceeds_when_free(self):
        out = common.evaluate_mutation_lock(None, action="plan", run_id="r1")
        self.assertEqual(out["decision"], "proceed")

    def test_live_lock_blocks_everyone(self):
        live = common.build_mutation_lock_doc("holder", "provision")
        self.assertEqual(
            common.evaluate_mutation_lock(live, action="provision", run_id="r2"),
            {"decision": "blocked", "holder": "holder"},
        )
        self.assertEqual(
            common.evaluate_mutation_lock(live, action="plan", run_id="r2")["decision"],
            "blocked",
        )

    def test_stale_lock_broken_by_mutating_only(self):
        stale = common.build_mutation_lock_doc("dead", "provision")
        stale["expires_at"] = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat()
        out = common.evaluate_mutation_lock(stale, action="destroy", run_id="r2")
        self.assertEqual(out["decision"], "break_and_acquire")
        self.assertEqual(out["lock_doc"]["broke_lock_of"], "dead")
        self.assertEqual(
            common.evaluate_mutation_lock(stale, action="plan", run_id="r2")["decision"],
            "proceed",
        )


if __name__ == "__main__":
    unittest.main()


class _FakeSyncer:
    def __init__(self, existing=None, create_wins=True):
        self.lock = existing
        self.create_wins = create_wins
        self.deleted = 0

    def read_mutation_lock(self):
        return self.lock

    def write_mutation_lock(self, doc):
        if self.lock is not None and self.create_wins is False:
            return False
        if self.create_wins:
            self.lock = doc
            return True
        return False

    def delete_mutation_lock(self):
        self.deleted += 1
        self.lock = None


class MutationLockGateTests(unittest.TestCase):
    """§Phase 31 Q1b — engine gate + release around the adapter."""

    def tearDown(self):
        common._MUTATION_LOCK_HELD = None

    def test_mutating_run_acquires_and_releases(self):
        syncer = _FakeSyncer()
        common.enforce_mutation_lock(syncer, action="provision", run_id="r1")
        self.assertEqual(syncer.lock["run_id"], "r1")
        common.release_mutation_lock_if_held()
        self.assertIsNone(syncer.lock)

    def test_plan_checks_but_never_writes(self):
        syncer = _FakeSyncer()
        common.enforce_mutation_lock(syncer, action="plan", run_id="r1")
        self.assertIsNone(syncer.lock)

    def test_blocked_by_live_holder(self):
        live = common.build_mutation_lock_doc("holder", "provision")
        syncer = _FakeSyncer(existing=live)
        with self.assertRaisesRegex(RuntimeError, "locked by run 'holder'"):
            common.enforce_mutation_lock(syncer, action="provision", run_id="r2")
        with self.assertRaisesRegex(RuntimeError, "locked by run 'holder'"):
            common.enforce_mutation_lock(syncer, action="plan", run_id="r2")

    def test_stale_lock_broken_and_recorded(self):
        stale = common.build_mutation_lock_doc("dead", "provision")
        stale["expires_at"] = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat()
        syncer = _FakeSyncer(existing=stale)
        common.enforce_mutation_lock(syncer, action="provision", run_id="r2")
        self.assertEqual(syncer.deleted, 1)
        self.assertEqual(syncer.lock["broke_lock_of"], "dead")

    def test_lost_conditional_create_raises(self):
        syncer = _FakeSyncer(create_wins=False)
        with self.assertRaisesRegex(RuntimeError, "lock lost"):
            common.enforce_mutation_lock(syncer, action="provision", run_id="r2")

    def test_no_syncer_skips(self):
        common.enforce_mutation_lock(None, action="provision", run_id="r1")  # no raise


class DuplicateSelectorGuardTests(unittest.TestCase):
    """§Phase 31 — reject byte-identical selectors at load (exactly-one structures)."""

    def test_identical_backend_selectors_rejected_at_load(self):
        dup = BACKENDS.replace(
            "        execution_context.params.landing_zone: canary\n",
            "        execution_context.params.landing_zone: live\n",
        )
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "b.yaml").write_text(dup, encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "identical selectors"):
                common.load_ctl_state_backends_cfg(Path(tmp))

    def test_distinct_selectors_load_fine(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "b.yaml").write_text(BACKENDS, encoding="utf-8")
            self.assertEqual(
                sorted(common.load_ctl_state_backends_cfg(Path(tmp))), ["canary", "live"]
            )

    def test_helper_direct(self):
        with self.assertRaisesRegex(RuntimeError, "identical selectors"):
            common.reject_duplicate_selectors(
                {
                    "a": {"match": {"execution_context.params.x": "1"}},
                    "b": {"match": {"execution_context.params.x": "1"}},
                },
                label="grp",
            )
