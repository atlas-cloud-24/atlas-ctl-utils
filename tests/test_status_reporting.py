"""What `ctl.py status` reports, over the lifetime of a run.

Written after a real namespace read as EMPTY and looked broken. It was not: the
status had been computed 0.5s before the first pointer was published, and
`status_cache.yaml` is only rewritten on `--write-cache`, so the empty snapshot
sat there afterwards looking like a defect.

So these tests walk the timeline — nothing, running, committed — and assert what
each moment reports, including that an empty namespace is a legitimate answer
rather than a failure.
"""

import sys
import tempfile
import argparse
import inspect
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


TARGET_KEY = "env/workload_identity/baseline"
TARGET_SEGMENTS = ["env.type=dev", "aws.account=dev"]
WORKFLOW_KEY = "env/workload_identity"
WORKFLOW_SEGMENTS = ["sha256=a3af8057"]


def _instance(ns: Path, kind: str, key: str, segments: list[str]) -> Path:
    return ns / common.compose_state_relpath(kind, key, segments)


def _publish(ns: Path, kind: str, key: str, segments: list[str], *, group="mutative",
             action="provision", at="2026-07-30T15:05:34Z", run_id="r1", **facts) -> None:
    common.write_yaml_file(
        common.committed_pointer_path(_instance(ns, kind, key, segments), group),
        {"run_id": run_id, "status": "ok", "committed_at": at, "action": action, **facts},
    )


def _workflow_run(ns: Path, key: str, *, run_id: str, status: str = "ok",
                  at: str = "2026-07-30T15:05:39Z") -> None:
    """A workflow publishes history: its RUN.yaml is the record."""
    run_dir = ns / common.compose_state_relpath("workflow", key, []) / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    common.write_yaml_file(
        common.run_metadata_path(run_dir),
        {"run_id": run_id, "run_type": "workflow", "action": "provision",
         "status": status, "updated_at": at},
    )


def _slot(ns: Path, kind: str, key: str, segments: list[str], state: str, *,
          group="mutative", **facts) -> None:
    common.write_yaml_file(
        common.state_slot_dir(_instance(ns, kind, key, segments), state, group)
        / "STATUS.yaml",
        {"run_id": "r2", "action": "provision", "status": state, **facts},
    )


class TimelineTest(unittest.TestCase):
    """Nothing -> running -> committed, read at each moment."""

    def test_an_empty_namespace_reports_nothing_and_does_not_fail(self):
        """The case that looked broken: a read before anything is published."""
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual({}, common.compute_namespace_status_map(Path(tmp)))

    def test_a_missing_namespace_reports_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                {}, common.compute_namespace_status_map(Path(tmp) / "never-created")
            )

    def test_a_run_in_flight_reports_running_before_it_commits(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _slot(ns, "target", TARGET_KEY, TARGET_SEGMENTS, "in_progress")
            rows = common.compute_namespace_status_map(ns)
            row = rows["target"][TARGET_KEY]["instances"]["/".join(TARGET_SEGMENTS)]
            self.assertEqual("running", row["mutative"]["status"])
            self.assertNotIn("freshness", row["mutative"])

    def test_a_committed_run_reports_passed_and_up_to_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS)
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["mutative"]
            self.assertEqual("passed", row["status"])
            self.assertEqual("up_to_date", row["freshness"])
            self.assertEqual("2026-07-30T15:05:34Z", row["at"])

    def test_a_target_and_its_workflow_both_appear(self):
        """The target reports state; the workflow reports its last run."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS)
            _workflow_run(ns, WORKFLOW_KEY, run_id="w1")
            rows = common.compute_namespace_status_map(ns)
            self.assertEqual({"target", "workflow"}, set(rows))
            self.assertIn("mutative", rows["target"][TARGET_KEY]["instances"][
                "/".join(TARGET_SEGMENTS)])
            self.assertEqual("passed", rows["workflow"][WORKFLOW_KEY]["last_run"]["status"])

    def test_a_failed_run_reports_failed_and_survives_the_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS)
            _slot(ns, "target", TARGET_KEY, TARGET_SEGMENTS, "failed",
                  error={"summary": "boom"})
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["mutative"]
            self.assertEqual("failed", row["status"])


class GroupIndependenceTest(unittest.TestCase):
    """A plan read must not disturb what the deployment row says."""

    def test_a_plan_run_does_not_change_the_deployment_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS,
                     at="2026-07-30T10:00:00Z")
            before = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["mutative"]
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS,
                     group="plan", action="plan", at="2026-07-30T11:00:00Z")
            after = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]
            self.assertEqual(before, after["mutative"])
            self.assertEqual("passed", after["plan"]["status"])

    def test_a_destroyed_instance_reports_no_freshness(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS, action="destroy")
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["mutative"]
            self.assertEqual("passed", row["status"])
            self.assertNotIn("freshness", row)


class ShapingTest(unittest.TestCase):
    """Filtering and structuring the map a reader actually sees."""

    def _map(self, ns: Path) -> dict:
        _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS, at="2026-07-30T15:05:34Z")
        _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS, group="plan",
                 action="plan", at="2026-07-30T15:00:00Z")
        _workflow_run(ns, WORKFLOW_KEY, run_id="w1")
        return common.compute_namespace_status_map(ns)

    def test_kind_filter_keeps_only_that_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = common.filter_status_map(self._map(Path(tmp)), ["workflow"], None)
            self.assertEqual(["workflow"], list(rows))

    def test_group_filter_keeps_only_that_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = common.filter_status_map(self._map(Path(tmp)), None, ["plan"])
            instance = rows["target"][TARGET_KEY]["instances"][
                "/".join(TARGET_SEGMENTS)
            ]
            self.assertEqual(["plan"], list(instance))

    def test_a_row_left_with_no_group_is_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = common.filter_status_map(self._map(Path(tmp)), None, ["readonly"])
            self.assertEqual({}, rows)

    def test_flat_structure_sorts_chronologically(self):
        with tempfile.TemporaryDirectory() as tmp:
            flat = common.structure_status_map(self._map(Path(tmp)), "flat", "time:asc")
            times = [row["at"] for row in flat["instances"]]
            self.assertEqual(sorted(times), times)

    def test_an_unknown_sort_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unknown"):
            common.parse_sort("size:desc")


if __name__ == "__main__":
    unittest.main()


class StatusArgumentsTest(unittest.TestCase):
    """Every status argument still means something under the history model."""

    def _tree(self, ns: Path) -> None:
        _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS)
        _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS, group="plan",
                 action="plan", at="2026-07-30T15:00:00Z")
        _workflow_run(ns, WORKFLOW_KEY, run_id="w1")

    def test_a_targeted_workflow_query_reports_its_last_run(self):
        """It read a committed pointer, which a workflow no longer publishes —
        so the query answered nothing rather than failing."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _workflow_run(ns, WORKFLOW_KEY, run_id="w1")
            result = common._targeted_workflow_status(
                ns, {"kind": "workflow", "key": WORKFLOW_KEY, "segments": [],
                     "address": WORKFLOW_KEY},
            )
            self.assertEqual("passed", result["last_run"]["status"])

    def test_a_targeted_workflow_that_never_ran_reports_no_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = common._targeted_workflow_status(
                Path(tmp), {"kind": "workflow", "key": WORKFLOW_KEY, "segments": [],
                            "address": WORKFLOW_KEY},
            )
            self.assertNotIn("last_run", result)

    def test_kind_workflow_keeps_the_history_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            self._tree(ns)
            rows = common.filter_status_map(
                common.compute_namespace_status_map(ns), ["workflow"], None
            )
            self.assertEqual(["workflow"], list(rows))

    def test_naming_a_group_excludes_workflows(self):
        """A workflow has no groups, so a group filter is a target filter. This is
        deliberate — `--kind` is how you choose kinds."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            self._tree(ns)
            rows = common.filter_status_map(
                common.compute_namespace_status_map(ns), None, ["mutative"]
            )
            self.assertEqual(["target"], list(rows))

    def test_flat_structure_carries_both_kinds(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            self._tree(ns)
            flat = common.structure_status_map(
                common.compute_namespace_status_map(ns), "flat", "time:asc"
            )
            addresses = [row["address"] for row in flat["instances"]]
            self.assertIn("workflow/env/workload_identity", addresses)
            self.assertTrue(any(a.startswith("target/") for a in addresses))

    def test_asking_for_workflows_and_a_group_is_answered_not_refused(self):
        """§Phase 82 replaced the refusal with an answer.

        Phase 73 refused the pair because groups were a target concept and a
        workflow had none. A workflow's group is now DERIVED from its members'
        actions, so the question has an answer — and a workflow that provisions
        is exactly what someone filtering for `mutative` is looking for.
        """
        instances = {
            "workflow": {
                "env/baseline": {"last_run": {"group": "mutative", "status": "succeeded"}},
                "env/readonly": {"last_run": {"group": "readonly", "status": "succeeded"}},
            }
        }
        kept = common.filter_status_map(instances, ["workflow"], ["mutative"])
        self.assertEqual(["env/baseline"], list(kept["workflow"]))

    def test_a_workflow_row_is_kept_whole_or_dropped(self):
        """A history row has no group partitions to narrow — it records ONE group,
        so filtering selects the row rather than trimming inside it."""
        instances = {"workflow": {"env/baseline": {"last_run": {"group": "mutative", "status": "succeeded"}}}}
        kept = common.filter_status_map(instances, ["workflow"], ["mutative"])
        self.assertEqual({"last_run": {"group": "mutative", "status": "succeeded"}},
                         kept["workflow"]["env/baseline"])
        self.assertEqual({}, common.filter_status_map(instances, ["workflow"], ["plan"]))

    def test_workflows_alone_are_fine(self):
        common.filter_status_map({}, ["workflow"], None)

    def test_a_group_with_targets_included_is_fine(self):
        """The group narrows the target rows; the workflow rows are unaffected."""
        common.filter_status_map({}, ["target", "workflow"], ["mutative"])
        common.filter_status_map({}, None, ["mutative"])


class RunIdentityTest(unittest.TestCase):
    """`status` and `at`/`run_id` must describe ONE run.

    Observed 2026-08-03 on a real namespace: a destroy failed after an earlier
    provision had committed, and the row reported the failure's status beside the
    SUCCESS's timestamp and run id — so anyone chasing the failure opened the
    wrong run directory. While the newest run succeeds the two agree, which is
    why it went unnoticed; it only diverges once a run fails after a success.
    """

    def _instance_with_failure_after_success(self, ns: Path) -> dict:
        _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS,
                 at="2026-08-03T11:12:17Z", run_id="r-provision")
        common.write_yaml_file(
            common.state_slot_dir(
                _instance(ns, "target", TARGET_KEY, TARGET_SEGMENTS), "failed", "mutative"
            ) / "STATUS.yaml",
            {"run_id": "r-destroy", "action": "destroy", "status": "failed",
             "updated_at": "2026-08-03T11:22:26Z", "mutation_started": True},
        )
        return common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
            "instances"
        ]["/".join(TARGET_SEGMENTS)]["mutative"]

    def test_at_is_the_failed_runs_time_not_the_last_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = self._instance_with_failure_after_success(Path(tmp))
            self.assertEqual("failed", row["status"])
            self.assertEqual("2026-08-03T11:22:26Z", row["at"])

    def test_run_id_is_the_failed_run(self):
        """The worse half: the detailed row carried the SUCCESSFUL run's id, so
        anyone opening it landed in the wrong run directory."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            self._instance_with_failure_after_success(ns)
            detail = common.compute_target_instance_status(
                ns, "provision",
                {"key": TARGET_KEY, "segments": TARGET_SEGMENTS,
                 "address": f"{TARGET_KEY}/instances/" + "/".join(TARGET_SEGMENTS)},
            )
            self.assertEqual("failed", detail["status"])
            self.assertEqual("r-destroy", detail["run_id"])
            self.assertEqual("r-provision", detail["committed_run_id"])

    def test_the_flat_row_does_not_carry_a_second_timestamp(self):
        """`committed_at` lives on the DETAILED row only — a field that appears
        just when a run failed after a success makes the common row harder to
        scan, and two timestamps invite misreading which is which."""
        with tempfile.TemporaryDirectory() as tmp:
            row = self._instance_with_failure_after_success(Path(tmp))
            self.assertEqual(["status", "last_action", "at"], list(row))

    def test_a_clean_row_does_not_repeat_itself(self):
        """When one run is both newest and committed, there is nothing to add."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS)
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["mutative"]
            self.assertNotIn("committed_at", row)
            self.assertNotIn("committed_run_id", row)


class LastActionTest(unittest.TestCase):
    """The direction of the last published run is stated, not inferred.

    Before this, a destroyed instance and a provisioned one differed only by
    `freshness` being ABSENT — a reader had to know that absence meant "the last
    thing that succeeded was a destroy". `state` stays gone (ctl never observes
    the cloud); this is what ctl's OWN run did.
    """

    def _row(self, ns: Path, action: str) -> dict:
        _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS, action=action)
        return common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
            "instances"
        ]["/".join(TARGET_SEGMENTS)]["mutative"]

    def test_a_provisioned_instance_says_so(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = self._row(Path(tmp), "provision")
            self.assertEqual("provision", row["last_action"])
            self.assertEqual("up_to_date", row["freshness"])

    def test_a_destroyed_instance_says_so_instead_of_omitting_freshness(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = self._row(Path(tmp), "destroy")
            self.assertEqual("destroy", row["last_action"])
            self.assertNotIn("freshness", row)

    def test_a_running_instance_reports_the_action_in_flight(self):
        """A run in progress already knows its direction — that is the moment a
        reader most wants it."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _slot(ns, "target", TARGET_KEY, TARGET_SEGMENTS, "in_progress")
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["mutative"]
            self.assertEqual("running", row["status"])
            self.assertEqual("provision", row["last_action"])

    def test_a_failed_destroy_after_a_good_provision_reports_destroy(self):
        """The confusing case from a real namespace: `status: failed` beside
        `last_action: provision` reads as "the provision failed", when the
        provision succeeded and the DESTROY failed."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS, action="provision",
                     run_id="r-ok")
            common.write_yaml_file(
                common.state_slot_dir(
                    _instance(ns, "target", TARGET_KEY, TARGET_SEGMENTS),
                    "failed", "mutative") / "STATUS.yaml",
                {"run_id": "r-bad", "action": "destroy", "status": "failed",
                 "updated_at": "2026-08-03T12:52:50Z", "mutation_started": True},
            )
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["mutative"]
            self.assertEqual("failed", row["status"])
            self.assertEqual("destroy", row["last_action"])

    def test_last_action_reads_before_freshness(self):
        """It qualifies `status`, so it sits next to it."""
        with tempfile.TemporaryDirectory() as tmp:
            row = self._row(Path(tmp), "provision")
            self.assertEqual(["status", "last_action", "freshness", "at"], list(row))


class WorkflowInstanceTest(unittest.TestCase):
    """A workflow's history is partitioned by the axes its members vary over.

    Before this, one key fanned across environments reported a single row —
    whichever child finished last — so "did baseline succeed in test?" could not
    be answered from the workflow at all.
    """

    def _run(self, ns: Path, segments: list[str], *, run_id: str, status: str,
             at: str) -> None:
        run_dir = (
            ns / common.compose_state_relpath("workflow", WORKFLOW_KEY, segments)
            / "runs" / run_id
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        common.write_yaml_file(
            common.run_metadata_path(run_dir),
            {"run_id": run_id, "run_type": "workflow", "action": "provision",
             "status": status, "updated_at": at},
        )

    def test_each_instance_keeps_its_own_last_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            self._run(ns, ["env.type=dev"], run_id="w-dev", status="ok",
                      at="2026-08-03T10:00:00Z")
            self._run(ns, ["env.type=test"], run_id="w-test", status="failed",
                      at="2026-08-03T11:00:00Z")
            rows = common.compute_namespace_status_map(ns)["workflow"][WORKFLOW_KEY]
            self.assertEqual(
                {"env.type=dev", "env.type=test"}, set(rows["instances"])
            )
            self.assertEqual("passed", rows["instances"]["env.type=dev"]["last_run"]["status"])
            self.assertEqual("failed", rows["instances"]["env.type=test"]["last_run"]["status"])

    def test_a_later_run_elsewhere_does_not_answer_for_this_instance(self):
        """The exact failure: dev succeeded, then test failed later, and the one
        row reported `failed` for the whole key."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            self._run(ns, ["env.type=dev"], run_id="w-dev", status="ok",
                      at="2026-08-03T10:00:00Z")
            self._run(ns, ["env.type=test"], run_id="w-test", status="failed",
                      at="2026-08-03T23:00:00Z")
            dev = common.compute_namespace_status_map(ns)["workflow"][WORKFLOW_KEY][
                "instances"]["env.type=dev"]["last_run"]
            self.assertEqual("passed", dev["status"])
            self.assertEqual("2026-08-03T10:00:00Z", dev["at"])

    def test_a_workflow_varying_by_nothing_stays_unnested(self):
        """A singleton keeps the flat shape — no empty instances/ layer."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            self._run(ns, [], run_id="w1", status="ok", at="2026-08-03T10:00:00Z")
            row = common.compute_namespace_status_map(ns)["workflow"][WORKFLOW_KEY]
            self.assertIn("last_run", row)
            self.assertNotIn("instances", row)


class ParentWorkflowLinkTest(unittest.TestCase):
    """A target instance names the workflow instance that drove it.

    The workflow row already lists the target instances it ran; without the
    reverse link, "this target failed — which workflow run was that?" meant
    opening the run directory by hand.
    """

    PARENT = "env/workload_permissions_boundary/instances/env.type=dev/aws.account=dev"

    def _row(self, ns: Path, **slot_facts) -> dict:
        common.write_yaml_file(
            common.state_slot_dir(
                _instance(ns, "target", TARGET_KEY, TARGET_SEGMENTS), "failed", "mutative"
            ) / "STATUS.yaml",
            {"run_id": "r1", "action": "destroy", "status": "failed",
             "updated_at": "2026-08-03T12:52:50Z", **slot_facts},
        )
        return common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
            "instances"
        ]["/".join(TARGET_SEGMENTS)]["mutative"]

    def test_a_spawned_target_names_its_workflow_instance(self):
        with tempfile.TemporaryDirectory() as tmp:
            row = self._row(Path(tmp), parent_workflow_instance_address=self.PARENT)
            self.assertEqual(f"workflow/{self.PARENT}", row["parent_workflow"])

    def test_a_directly_invoked_target_names_none(self):
        """Absent is a real distinction — the run had no parent — not a gap."""
        with tempfile.TemporaryDirectory() as tmp:
            row = self._row(Path(tmp))
            self.assertNotIn("parent_workflow", row)

    def test_the_run_id_goes_under_its_own_key_when_no_address_was_recorded(self):
        """Older runs recorded only the id. It gets its OWN field: one key meaning
        "an address, or else an id" makes every reader branch on shape."""
        with tempfile.TemporaryDirectory() as tmp:
            row = self._row(Path(tmp), parent_workflow_run_id="019fc756")
            self.assertNotIn("parent_workflow", row)
            self.assertEqual("019fc756", row["parent_workflow_run_id"])


class SpawnedChildLearnsItsParentTest(unittest.TestCase):
    """A spawned child must be TOLD its parent's instance address.

    The in-process path recorded it from the parent's metadata directly, so the
    field looked wired — but a workflow spawns its children as separate
    processes, and that path passed only `--parent-workflow-run-id`. Every real
    run therefore recorded the id and no address, and the status row fell back to
    the id (observed 2026-08-03).
    """

    def test_the_spawn_argv_carries_the_parents_instance_address(self):
        import tempfile as _tempfile

        with _tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp) / "runs" / "p1"
            parent.mkdir(parents=True)
            common.write_run_metadata(parent, {
                "run_id": "p1", "run_type": "workflow", "action": "provision",
                "instance_address": "env/baseline/instances/env.type=dev",
                "ctl_state_local_root": tmp, "ctl_state_locator": ["live"],
            })
            argv = common.build_child_target_command(
                {"ctl_entrypoint": "ctl.py", "ctl_cfg_root": Path(tmp),
                 "ctl_profile": "local_dev", "ctl_state_local_root": tmp,
                 "execution_runtime_mode": "local", "action": "provision",
                 "providers": ["aws"], "execution_params": {}},
                "env/core/baseline", parent_run_dir=parent, parent_run_id="p1",
            )
            self.assertIn("--parent-workflow-instance-address", argv)
            self.assertEqual(
                "env/baseline/instances/env.type=dev",
                argv[argv.index("--parent-workflow-instance-address") + 1],
            )

    def test_a_parent_without_an_instance_address_passes_nothing(self):
        """A workflow varying over nothing has no instance layer; the child then
        carries only the run id rather than an empty flag."""
        import tempfile as _tempfile

        with _tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp) / "runs" / "p1"
            parent.mkdir(parents=True)
            common.write_run_metadata(parent, {
                "run_id": "p1", "run_type": "workflow", "action": "provision",
                "ctl_state_local_root": tmp, "ctl_state_locator": ["live"],
            })
            argv = common.build_child_target_command(
                {"ctl_entrypoint": "ctl.py", "ctl_cfg_root": Path(tmp),
                 "ctl_profile": "local_dev", "ctl_state_local_root": tmp,
                 "execution_runtime_mode": "local", "action": "provision",
                 "providers": ["aws"], "execution_params": {}},
                "env/core/baseline", parent_run_dir=parent, parent_run_id="p1",
            )
            self.assertNotIn("--parent-workflow-instance-address", argv)


class StatusQueryHistoryTest(unittest.TestCase):
    """§Phase 82: every `--write-cache` query is kept, dated.

    The point of the split is WHERE. `status` is read-only against ctl-state, so
    a query record must never land in the synced tree — that would make a read
    mutate, fail under read-only credentials, and add sync churn for something no
    run consumes. The local root is already an advisory mirror that is never
    truth, which is what a query log is.
    """

    def _run(self, local_root: Path, namespace: str = "live"):
        args = argparse.Namespace(
            all=True, scope="local", status="local", write_cache=True,
            ctl_state_local_root=str(local_root), execution_param=[],
            execution_params={}, action="readonly", ctl_profile="local_dev",
            providers=(), execution_access_modes={}, execution_runtime_mode="local",
            kind=None, group=None, view="flat", sort="time:asc", hydrate_to=None,
        )
        return args

    def test_the_latest_stays_at_the_namespace_root_and_history_accumulates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "local"
            ns = root / "live"
            (ns / "status_history").mkdir(parents=True)
            # two queries, written as the runner writes them
            for stamp in ("2026-08-04T00:00:00Z", "2026-08-04T00:05:00Z"):
                payload = {"advisory": True, "queried_at": stamp}
                common.write_yaml_file(ns / "status_cache.yaml", payload)
                common.write_yaml_file(
                    ns / "status_history" / f"{stamp.replace(':', '-')}.yaml", payload
                )

            history = sorted(p.name for p in (ns / "status_history").glob("*.yaml"))
            self.assertEqual(2, len(history), "each query is kept, not overwritten")
            latest = common.load_yaml(ns / "status_cache.yaml")
            self.assertEqual("2026-08-04T00:05:00Z", latest["queried_at"],
                             "the root file is the LATEST, so a tool reading one "
                             "stable path does not have to scan history")

    def test_history_lives_under_the_local_root_only(self):
        """The synced ctl-state must contain no query record at all."""
        source = inspect.getsource(common.run_status_all_command)
        self.assertIn("ctl_state_local_root", source)
        self.assertIn("status_history", source)
        # the only paths written are built from the local root
        for line in source.splitlines():
            if "status_history" in line and "=" in line:
                self.assertIn("namespace_dir", line)


class WorkflowGroupIsDerivedTest(unittest.TestCase):
    """§Phase 82: a workflow's group comes from what its members DO.

    Derived, never declared: a declared flag can disagree with the composition and
    nothing would catch it, while the members are already recorded on the run.
    """

    def test_a_mixed_composition_is_mutative(self):
        """A workflow that provisions one target and plans another IS a mutation —
        the plan does not soften it. This is the precedence that matters."""
        facts = {
            "default_action": "plan",
            "target_instances": ["env/static/acm", {"instance": "env/core", "action": "provision"}],
        }
        self.assertEqual("mutative", common.workflow_group(facts))

    def test_destroy_is_mutative_too(self):
        """`deployment` was renamed BECAUSE destroy is not a deployment."""
        facts = {"default_action": "destroy", "target_instances": ["env/core"]}
        self.assertEqual("mutative", common.workflow_group(facts))

    def test_a_uniform_composition_takes_its_own_group(self):
        facts = {"default_action": "plan", "target_instances": ["env/core", "env/seed"]}
        self.assertEqual("plan", common.workflow_group(facts))

    def test_members_inherit_the_default_action(self):
        """A bare address took the workflow's default_action (§Phase 73)."""
        facts = {"default_action": "provision", "target_instances": ["a", "b"]}
        self.assertEqual(["provision", "provision"], common.recorded_member_actions(facts))

    def test_a_dict_member_without_an_action_also_inherits_it(self):
        """Both member forms appear in ONE list, so both must fall back the same
        way — a dict entry that carries only an instance is still a member that
        took the default."""
        facts = {
            "default_action": "provision",
            "target_instances": [{"instance": "env/core"}, {"instance": "env/seed", "action": "plan"}],
        }
        self.assertEqual(["provision", "plan"], common.recorded_member_actions(facts))
        self.assertEqual("mutative", common.workflow_group(facts))

    def test_an_empty_composition_answers_nothing(self):
        """None, not a default — a malformed record must not claim a group."""
        self.assertIsNone(common.workflow_group({"target_instances": []}))

    def test_the_group_is_recorded_with_the_composition(self):
        """Derived where the members are known, so no reader re-derives it and
        none can derive it differently."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs" / "w1"
            run_dir.mkdir(parents=True)
            common.write_yaml_file(run_dir / "metadata.yaml", {"target_addresses": []})
            common.record_workflow_members(
                run_dir,
                {"t1": {"target": "env/core", "action": "provision"}},
                {"default_action": "provision"},
            )
            self.assertEqual("mutative", common.load_run_metadata(run_dir).get("group"))


class WorkflowGroupFilterAgainstRealRowsTest(unittest.TestCase):
    """Filter the map `compute_namespace_status_map` ACTUALLY produces.

    This exists because of a bug I shipped and the user found: `--kind workflow
    --group mutative` silently returned nothing. The group is recorded on the RUN,
    and a workflow row wraps it as `{"last_run": {...}}`, so a filter reading
    `row["group"]` finds nothing and drops every workflow — which reads as "no
    workflows ran", not as "your filter was wrong".

    The tests I wrote first passed anyway, because they hand-built the row shape
    and I built it wrong in the test the same way I built it wrong in the code.
    So this one goes through the real producer: state on disk -> the real map ->
    the real filter.
    """

    def _namespace(self, tmp: Path, *, default_action: str) -> Path:
        ns = tmp / "live"
        run_dir = ns / common.compose_state_relpath("workflow", "env/baseline", []) / "runs" / "r1"
        run_dir.mkdir(parents=True)
        common.write_yaml_file(
            common.run_metadata_path(run_dir),
            {"run_id": "r1", "run_type": "workflow", "action": default_action,
             "status": "ok", "updated_at": "2026-08-04T00:00:00Z",
             "default_action": default_action,
             "target_instances": ["env/core"],
             "group": common.action_group(default_action)},
        )
        return ns

    def test_a_mutative_workflow_survives_its_own_group_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = self._namespace(Path(tmp), default_action="provision")
            rows = common.compute_namespace_status_map(ns)
            self.assertIn("env/baseline", rows["workflow"], "precondition: the row exists unfiltered")

            kept = common.filter_status_map(rows, ["workflow"], ["mutative"])
            self.assertIn(
                "workflow", kept,
                "a provisioning workflow IS mutative — dropping it here is the bug "
                "that made `--kind workflow --group mutative` return nothing",
            )
            self.assertIn("env/baseline", kept["workflow"])

    def test_a_plan_workflow_is_excluded_by_that_same_filter(self):
        """The filter must still filter — surviving everything would be the
        opposite bug and just as invisible."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = self._namespace(Path(tmp), default_action="plan")
            rows = common.compute_namespace_status_map(ns)
            self.assertEqual({}, common.filter_status_map(rows, ["workflow"], ["mutative"]))
            self.assertIn("workflow", common.filter_status_map(rows, ["workflow"], ["plan"]))

    def test_a_run_recorded_before_the_group_existed_still_answers(self):
        """Runs written before §Phase 82 have no `group` field. Deriving it from
        the members they DID record is what stops them filtering out as though
        they had no group at all."""
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp) / "live"
            run_dir = ns / common.compose_state_relpath("workflow", "env/legacy", []) / "runs" / "r1"
            run_dir.mkdir(parents=True)
            common.write_yaml_file(
                common.run_metadata_path(run_dir),
                {"run_id": "r1", "run_type": "workflow", "action": "provision",
                 "status": "ok", "updated_at": "2026-08-04T00:00:00Z",
                 "default_action": "provision", "target_instances": ["env/core"]},
            )
            rows = common.compute_namespace_status_map(ns)
            self.assertEqual("mutative", rows["workflow"]["env/legacy"]["last_run"]["group"])
            self.assertIn("workflow", common.filter_status_map(rows, ["workflow"], ["mutative"]))


class StatusHistoryLivesOutsideTheNamespaceTest(unittest.TestCase):
    """`_local` is a SIBLING of the namespaces, never a child of one.

    My first attempt put history at `<root>/<namespace>/_local/`, which buries a
    never-synced directory inside a tree that IS synced — the exact thing the
    reserved segment exists to prevent.
    """

    def test_the_history_path_is_built_from_the_local_root_not_the_namespace(self):
        source = inspect.getsource(common.run_status_all_command)
        start = source.index("history_path")
        window = source[start:start + 400]
        self.assertIn("ctl_state_local_root", window)
        self.assertIn("LOCAL_ONLY_LOCATOR", window)
        self.assertNotIn(
            "namespace_dir.joinpath", window,
            "history under the namespace puts a never-synced dir inside a synced tree",
        )
