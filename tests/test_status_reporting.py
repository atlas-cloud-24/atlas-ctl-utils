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


def _publish(ns: Path, kind: str, key: str, segments: list[str], *, group="deployment",
             action="provision", at="2026-07-30T15:05:34Z", **facts) -> None:
    common.write_yaml_file(
        common.committed_pointer_path(_instance(ns, kind, key, segments), group),
        {"run_id": "r1", "status": "ok", "committed_at": at, "action": action, **facts},
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
          group="deployment", **facts) -> None:
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
            self.assertEqual("running", row["deployment"]["status"])
            self.assertNotIn("freshness", row["deployment"])

    def test_a_committed_run_reports_passed_and_up_to_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS)
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["deployment"]
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
            self.assertIn("deployment", rows["target"][TARGET_KEY]["instances"][
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
            ]["/".join(TARGET_SEGMENTS)]["deployment"]
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
            ]["/".join(TARGET_SEGMENTS)]["deployment"]
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS,
                     group="plan", action="plan", at="2026-07-30T11:00:00Z")
            after = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]
            self.assertEqual(before, after["deployment"])
            self.assertEqual("passed", after["plan"]["status"])

    def test_a_destroyed_instance_reports_no_freshness(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns = Path(tmp)
            _publish(ns, "target", TARGET_KEY, TARGET_SEGMENTS, action="destroy")
            row = common.compute_namespace_status_map(ns)["target"][TARGET_KEY][
                "instances"
            ]["/".join(TARGET_SEGMENTS)]["deployment"]
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
                common.compute_namespace_status_map(ns), None, ["deployment"]
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

    def test_asking_for_workflows_and_a_group_is_refused(self):
        """A contradiction: groups are a target concept, so the pair can only ever
        return nothing. Answering it emptily would read as 'nothing happened'."""
        with self.assertRaisesRegex(RuntimeError, "cannot be combined with --group"):
            common.filter_status_map({}, ["workflow"], ["deployment"])

    def test_workflows_alone_are_fine(self):
        common.filter_status_map({}, ["workflow"], None)

    def test_a_group_with_targets_included_is_fine(self):
        """The group narrows the target rows; the workflow rows are unaffected."""
        common.filter_status_map({}, ["target", "workflow"], ["deployment"])
        common.filter_status_map({}, None, ["deployment"])
