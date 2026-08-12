import logging.handlers
import pathlib
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.catalog import workflow as catalog_workflow
from engine.commands import selection as commands_selection
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import addressing as run_addressing
from engine.run import selectors as run_selectors
from engine.state import run_store as state_run_store
from engine.state import sync as state_sync

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
    "    execution_identity:\n"
    "      account: ctl_plane\n"
    "      operations:\n"
    "        read:\n          role: reader\n"
    "        sync:\n          role: synchronizer\n"
    "        maintenance:\n          role: maintainer\n"
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
    """exact-one namespace resolution."""

    def _root(self, tmp: str, backends: str = BACKENDS) -> Path:
        root = Path(tmp)
        (root / "backends.yaml").write_text(backends, encoding="utf-8")
        return root

    def test_resolves_exactly_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            key, backend = state_sync.CtlStateBackends.resolve_namespace(
                root, ctx(landing_zone="live")
            )
            self.assertEqual(key, "live")
            self.assertEqual(backend["bucket_name"], "oxygen-live-ctl-state")
            self.assertEqual(
                backend["execution_identity"]["operations"]["sync"]["role"], "synchronizer"
            )

    def test_zero_matches_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            with self.assertRaisesRegex(RuntimeError, "matched 0"):
                state_sync.CtlStateBackends.resolve_namespace(root, ctx(landing_zone="qa"))

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
                state_sync.CtlStateBackends.resolve_namespace(root, ctx(landing_zone="canary"))


class FanOutNamespaceGateTests(unittest.TestCase):
    """cross-namespace fan-out rejection."""

    def _root(self, tmp: str) -> Path:
        root = Path(tmp)
        (root / "backends.yaml").write_text(BACKENDS, encoding="utf-8")
        (root / "params.yaml").write_text(
            "execution_params:\n  main_tag: oxygen\n", encoding="utf-8"
        )
        return root

    def _children(self, *zones):
        return [{"label": f"wf[{z}]", "params": {"landing_zone": z}} for z in zones]

    def test_same_namespace_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            ns = commands_selection.require_unique_fan_out_namespace(
                root,
                self._children("live", "live"),
                action="provision",
                ctl_profile=None,
                execution_params={},
                execution_runtime_mode="local",
            )
            self.assertEqual(ns, "live")

    def test_cross_namespace_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            with self.assertRaisesRegex(RuntimeError, "must not cross namespaces"):
                commands_selection.require_unique_fan_out_namespace(
                    root,
                    self._children("live", "canary"),
                    action="provision",
                    ctl_profile=None,
                    execution_params={},
                    execution_runtime_mode="local",
                )


class WorkflowCompositionTests(unittest.TestCase):
    """The workflow identity doc.

    The composition sha it also covered is gone: a workflow instance
    is addressed by declared params, not by a digest over its members.
    """

    ADDRS = ["target/env/tfstate_backend/instances/account=stg/env_type=stg"]

    def test_identity_doc_facts_only(self):
        doc = catalog_workflow.WorkflowArtifacts.identity_doc(
            "env/bootstrap", self.ADDRS, {"account": "stg", "env_type": "stg"}
        )
        wf = doc["workflow_instance"]
        self.assertEqual(wf["workflow"], "env/bootstrap")
        self.assertEqual(wf["targets"], self.ADDRS)
        # the composition sha is the instance DIR NAME — never duplicated in the doc
        self.assertNotIn("composition_sha256", wf)
        self.assertNotIn("status", wf)

    def test_target_instance_address_forms(self):
        self.assertEqual(run_addressing.target_instance_address("env/core", []), "env/core")
        self.assertEqual(
            run_addressing.target_instance_address("env/core", ["account=dev", "env_type=dev"]),
            "env/core/instances/account=dev/env_type=dev",
        )


class MutationLockTests(unittest.TestCase):
    """

    interim global mutation lock decision logic."""

    def test_mutating_acquires_free_lock(self):
        out = state_run_store.evaluate_mutation_lock(None, action="provision", run_id="r1")
        self.assertEqual(out["decision"], "acquire")
        self.assertEqual(out["lock_doc"]["run_id"], "r1")

    def test_non_mutating_proceeds_when_free(self):
        out = state_run_store.evaluate_mutation_lock(None, action="plan", run_id="r1")
        self.assertEqual(out["decision"], "proceed")

    def test_a_live_lock_blocks_another_mutation(self):
        live = state_run_store.build_mutation_lock_doc("holder", "provision")
        self.assertEqual(
            state_run_store.evaluate_mutation_lock(live, action="provision", run_id="r2"),
            {"decision": "blocked", "holder": "holder"},
        )

    def test_a_live_lock_does_not_block_a_reader(self):
        """A non-mutating action never acquires this lock, so
        blocking it only ever denied a read — and denied exactly the read worth
        having, since `status` reports on the run holding the lock."""

        live = state_run_store.build_mutation_lock_doc("holder", "provision")
        for action in ("plan", "readonly", "maintenance"):
            with self.subTest(action=action):
                self.assertEqual(
                    "proceed",
                    state_run_store.evaluate_mutation_lock(live, action=action, run_id="r2")[
                        "decision"
                    ],
                )

    def test_stale_lock_broken_by_mutating_only(self):
        stale = state_run_store.build_mutation_lock_doc("dead", "provision")
        stale["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        out = state_run_store.evaluate_mutation_lock(stale, action="destroy", run_id="r2")
        self.assertEqual(out["decision"], "break_and_acquire")
        self.assertEqual(out["lock_doc"]["broke_lock_of"], "dead")
        self.assertEqual(
            state_run_store.evaluate_mutation_lock(stale, action="plan", run_id="r2")["decision"],
            "proceed",
        )


class ChildOfHolderTest(unittest.TestCase):
    """A workflow child must not be blocked by its own parent's namespace lock."""

    @staticmethod
    def _held_by(run_id: str) -> dict:
        return state_run_store.build_mutation_lock_doc(run_id, "provision")

    def test_child_proceeds_past_its_parents_lock(self):
        outcome = state_run_store.evaluate_mutation_lock(
            self._held_by("parent-run"),
            action="provision",
            run_id="child-run",
            parent_run_id="parent-run",
        )
        self.assertEqual(outcome["decision"], "proceed")

    def test_unrelated_run_is_still_blocked(self):
        outcome = state_run_store.evaluate_mutation_lock(
            self._held_by("someone-else"),
            action="provision",
            run_id="child-run",
            parent_run_id="parent-run",
        )
        self.assertEqual(outcome["decision"], "blocked")
        self.assertEqual(outcome["holder"], "someone-else")

    def test_no_parent_means_no_exemption(self):
        outcome = state_run_store.evaluate_mutation_lock(
            self._held_by("parent-run"),
            action="provision",
            run_id="child-run",
        )
        self.assertEqual(outcome["decision"], "blocked")


class ParentRunIdIsRecordedTest(unittest.TestCase):
    """The exemption is only reachable if the child RECORDS its parent.

    The decision logic and the metadata that feeds it were added in separate
    places; testing only the logic left the whole path dead.
    """

    def test_setup_run_dirs_records_parent_workflow_run_id(self):
        import inspect

        signature = inspect.signature(commands_selection.setup_run_dirs)
        self.assertIn("parent_workflow_run_id", signature.parameters)

        with tempfile.TemporaryDirectory(prefix="atlas-parent-meta-") as tmp:
            memory_handler = logging.handlers.MemoryHandler(capacity=1024)
            run_dir, _, _ = commands_selection.setup_run_dirs(
                "child-run",
                "destroy",
                "target",
                "env/seed/baseline",
                pathlib.Path(tmp),
                memory_handler,
                locator_segments=["live"],
                parent_workflow_run_id="parent-run",
            )
            metadata = state_run_store.load_run_metadata(run_dir)
            self.assertEqual(metadata.get("parent_workflow_run_id"), "parent-run")

    def test_absent_parent_leaves_no_key(self):
        with tempfile.TemporaryDirectory(prefix="atlas-parent-meta-") as tmp:
            memory_handler = logging.handlers.MemoryHandler(capacity=1024)
            run_dir, _, _ = commands_selection.setup_run_dirs(
                "solo-run",
                "destroy",
                "target",
                "env/seed/baseline",
                pathlib.Path(tmp),
                memory_handler,
                locator_segments=["live"],
            )
            self.assertIsNone(
                state_run_store.load_run_metadata(run_dir).get("parent_workflow_run_id")
            )


if __name__ == "__main__":
    unittest.main()


class _FakeSyncer:
    def __init__(self, existing=None, create_wins=True):
        self.lock = existing
        self.create_wins = create_wins
        self.deleted = 0
        self.reads = 0

    def read_mutation_lock(self):
        self.reads += 1
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
    """

    engine gate + release around the adapter."""

    def tearDown(self):
        state_run_store._MUTATION_LOCK_HELD = None

    def test_mutating_run_acquires_and_releases(self):
        syncer = _FakeSyncer()
        state_run_store.enforce_mutation_lock(syncer, action="provision", run_id="r1")
        self.assertEqual(syncer.lock["run_id"], "r1")
        state_run_store.release_mutation_lock_if_held()
        self.assertIsNone(syncer.lock)

    def test_plan_checks_but_never_writes(self):
        syncer = _FakeSyncer()
        state_run_store.enforce_mutation_lock(syncer, action="plan", run_id="r1")
        self.assertIsNone(syncer.lock)

    def test_blocked_by_live_holder(self):
        live = state_run_store.build_mutation_lock_doc("holder", "provision")
        syncer = _FakeSyncer(existing=live)
        with self.assertRaisesRegex(RuntimeError, "locked by run 'holder'"):
            state_run_store.enforce_mutation_lock(syncer, action="provision", run_id="r2")

    def test_a_reader_is_not_blocked_and_does_not_even_look(self):
        """

        reading the lock object cost a GET per query to answer a
        question whose only possible outcome was to refuse the caller."""

        live = state_run_store.build_mutation_lock_doc("holder", "provision")
        syncer = _FakeSyncer(existing=live)
        before = syncer.reads
        state_run_store.enforce_mutation_lock(syncer, action="readonly", run_id="r2")
        self.assertEqual(before, syncer.reads, "a reader still fetched the lock object")

    def test_stale_lock_broken_and_recorded(self):
        stale = state_run_store.build_mutation_lock_doc("dead", "provision")
        stale["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        syncer = _FakeSyncer(existing=stale)
        state_run_store.enforce_mutation_lock(syncer, action="provision", run_id="r2")
        self.assertEqual(syncer.deleted, 1)
        self.assertEqual(syncer.lock["broke_lock_of"], "dead")

    def test_lost_conditional_create_raises(self):
        syncer = _FakeSyncer(create_wins=False)
        with self.assertRaisesRegex(RuntimeError, "lock lost"):
            state_run_store.enforce_mutation_lock(syncer, action="provision", run_id="r2")

    def test_no_syncer_skips(self):
        state_run_store.enforce_mutation_lock(None, action="provision", run_id="r1")  # no raise


class DuplicateSelectorGuardTests(unittest.TestCase):
    """

    reject byte-identical selectors at load (exactly-one structures)."""

    def test_identical_backend_selectors_rejected_at_load(self):
        dup = BACKENDS.replace(
            "        execution_context.params.landing_zone: canary\n",
            "        execution_context.params.landing_zone: live\n",
        )
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "b.yaml").write_text(dup, encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "identical selectors"):
                state_sync.CtlStateBackends.load(Path(tmp))

    def test_distinct_selectors_load_fine(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "b.yaml").write_text(BACKENDS, encoding="utf-8")
            self.assertEqual(
                sorted(state_sync.CtlStateBackends.load(Path(tmp))), ["canary", "live"]
            )

    def test_helper_direct(self):
        with self.assertRaisesRegex(RuntimeError, "identical selectors"):
            run_selectors.reject_duplicate_selectors(
                {
                    "a": {"match": {"execution_context.params.x": "1"}},
                    "b": {"match": {"execution_context.params.x": "1"}},
                },
                label="grp",
            )


class MutationLockTtlTest(unittest.TestCase):
    """The TTL is what makes a lock BREAKABLE.

    At one hour a long apply outlived its own lock, the next mutating run broke
    it, and the result was the two concurrent mutators the lock exists to prevent.
    The default is now past any single apply, and a namespace holding a slower
    estate may declare its own.
    """

    def test_the_default_outlasts_a_long_apply(self):
        """The number is the point: an hour is inside the range of a real apply,
        so the old default broke live locks as a matter of course."""
        self.assertGreaterEqual(state_run_store.MUTATION_LOCK_TTL_SECONDS, 4 * 3600)

    def test_a_namespace_may_declare_its_own(self):
        self.assertEqual(
            900, state_sync.CtlStateBackends.mutation_lock_ttl({"mutation_lock_ttl_seconds": 900})
        )

    def test_a_namespace_that_declares_none_takes_the_default(self):
        for entry in ({}, {"bucket_name": "b"}, None):
            with self.subTest(entry=entry):
                self.assertEqual(
                    state_run_store.MUTATION_LOCK_TTL_SECONDS,
                    state_sync.CtlStateBackends.mutation_lock_ttl(entry),
                )

    def test_a_nonsense_ttl_is_refused(self):
        """

        silently falling back to the default would give a namespace a lock
        window its author did not choose and does not know about."""

        for bad in ("3600", 0, -1, 1.5, True, [3600]):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                state_sync.CtlStateBackends.mutation_lock_ttl({"mutation_lock_ttl_seconds": bad})

    def test_the_declared_ttl_reaches_the_lock_document(self):
        doc = state_run_store.build_mutation_lock_doc("r1", "provision", ttl_seconds=60)
        acquired = datetime.fromisoformat(doc["acquired_at"])
        expires = datetime.fromisoformat(doc["expires_at"])
        self.assertEqual(60, doc["ttl_seconds"])
        self.assertAlmostEqual(60, (expires - acquired).total_seconds(), delta=2)

    def test_a_lock_taken_under_a_long_ttl_is_not_stale_after_an_hour(self):
        """

        the regression itself: this is the run that used to have its own lock
        broken out from under it."""

        doc = state_run_store.build_mutation_lock_doc("slow-apply", "provision")
        an_hour_in = datetime.now(UTC) + timedelta(seconds=3601)
        self.assertFalse(state_run_store.mutation_lock_is_stale(doc, now=an_hour_in))

    def test_a_lock_is_still_breakable_once_its_own_ttl_passes(self):
        """

        longer, not infinite — an abandoned lock must still clear."""

        doc = state_run_store.build_mutation_lock_doc("abandoned", "provision", ttl_seconds=60)
        later = datetime.now(UTC) + timedelta(seconds=61)
        self.assertTrue(state_run_store.mutation_lock_is_stale(doc, now=later))

    def _namespace_root(self, tmp, ttl):
        root = Path(tmp)
        entry = {
            "provider": "aws",
            "backend_type": "s3",
            "bucket_name": "b",
            "bucket_region": "eu-west-2",
            "selectors": {"match": {"execution_context.params.lz": "live"}},
        }
        if ttl is not None:
            entry["mutation_lock_ttl_seconds"] = ttl
        kernel_yaml_io.write_yaml_file(
            root / "ctl_state_backends.yaml", {"ctl_state_backends": {"live": entry}}
        )
        return root

    def test_a_namespace_is_allowed_to_declare_the_field_at_all(self):
        """

        backend entries are key-ALLOWLISTED, so a new field is refused until it
        is listed. Without this the whole option is dead cfg: declaring it fails
        at load with `unsupported keys`, and every other test here works on dicts
        that never went through the loader."""

        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._namespace_root(tmp, 900)
            _, entry = state_sync.CtlStateBackends.resolve_namespace(
                root, {"execution_context.params.lz": "live"}
            )
            self.assertEqual(900, state_sync.CtlStateBackends.mutation_lock_ttl(entry))

    def test_a_malformed_ttl_fails_at_cfg_load(self):
        """Not at the moment a mutating run reaches for the lock: by then the run
        has done its setup and the failure reads as a lock problem. At LOAD,
        where every other field of the entry is checked and where the value is
        carried onto the resolved entry.

        Matched on the VALUE complaint, not on the field name: the name alone also
        appears in the loader's `unsupported keys` error, and this test passed
        against that unrelated failure until mutation exposed it.
        """

        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._namespace_root(tmp, "not-a-number")
            with self.assertRaisesRegex(RuntimeError, "must be a positive integer"):
                state_sync.CtlStateBackends.resolve_namespace(
                    root, {"execution_context.params.lz": "live"}
                )
