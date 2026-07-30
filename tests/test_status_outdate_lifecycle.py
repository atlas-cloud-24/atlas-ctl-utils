"""The outdate lifecycle, end to end, over a namespace nobody configured.

Every tree here is built by the test. Nothing reads oxygen cfg, so this says the
same thing on a machine that has never seen this project, and a cfg change can
never quietly make it pass or fail.

The lifecycle it walks:

    publish            -> up_to_date
    a dependency runs  -> outdated
    republish          -> up_to_date again
    a member runs      -> the composition is running
    a member fails     -> the composition is failed

and the COUNTS across the namespace at each step, because a status view is read
for "how many things need attention", not one row at a time.
"""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


class Namespace:
    """A synthetic ctl-state namespace, built entirely by the test."""

    def __init__(self, root: Path):
        self.root = root

    # ── construction ────────────────────────────────────────────────────────
    def instance(self, kind: str, key: str, segments: list[str]) -> Path:
        return self.root / common.compose_state_relpath(kind, key, segments)

    def publish(self, kind: str, key: str, segments: list[str], *, run_id: str,
                group: str = "deployment", action: str = "provision",
                at: str = "2026-07-30T10:00:00Z", children: list[dict] | None = None,
                **facts) -> Path:
        pointer = {
            "run_id": run_id,
            "snapshot_sha256": f"snap-{run_id}",
            "status": "ok",
            "committed_at": at,
            "action": action,
            **facts,
        }
        if children is not None:
            pointer["child_revisions"] = children
        path = common.committed_pointer_path(self.instance(kind, key, segments), group)
        common.write_yaml_file(path, pointer)
        return path

    def slot(self, kind: str, key: str, segments: list[str], state: str, *,
             group: str = "deployment", **facts) -> None:
        common.write_yaml_file(
            common.state_slot_dir(self.instance(kind, key, segments), state, group)
            / "STATUS.yaml",
            {"run_id": "live", "action": "provision", "status": state, **facts},
        )

    def workflow_run(self, key: str, *, run_id: str, status: str = "ok",
                     at: str = "2026-07-30T10:00:05Z", targets: list | None = None,
                     default_action: str | None = None,
                     selectors: dict | None = None) -> None:
        """A workflow publishes HISTORY: its RUN.yaml is the record."""
        run_dir = (
            self.root / common.compose_state_relpath("workflow", key, [])
            / "runs" / run_id
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        record = {"run_id": run_id, "run_type": "workflow", "action": "provision",
                  "status": status, "updated_at": at}
        if targets is not None:
            record["targets"] = targets
        if default_action:
            record["default_action"] = default_action
        if selectors:
            record["member_selectors"] = selectors
        common.write_yaml_file(common.run_metadata_path(run_dir), record)

    def clear_slots(self, kind: str, key: str, segments: list[str]) -> None:
        import shutil

        for state in ("in_progress", "failed"):
            directory = common.state_slot_dir(
                self.instance(kind, key, segments), state, "deployment"
            )
            if directory.exists():
                shutil.rmtree(directory)

    def outdate(self, kind: str, key: str, segments: list[str], *,
                group: str = "deployment", reason: str = "a dependency changed") -> None:
        path = common.committed_pointer_path(self.instance(kind, key, segments), group)
        common.mark_committed_status_outdated(
            path, common.load_yaml(path) or {}, reason=reason
        )

    # ── reading ─────────────────────────────────────────────────────────────
    def rows(self) -> dict:
        return common.compute_namespace_status_map(self.root)

    def row(self, kind: str, key: str, segments: list[str],
            group: str = "deployment") -> dict:
        instances = self.rows()[kind][key]
        block = instances["instances"]["/".join(segments)] if segments else instances
        return block.get(group, {})

    def counts(self) -> dict[str, int]:
        """How many rows sit in each state, across every kind and group."""
        tally: dict[str, int] = {}
        for kind_rows in self.rows().values():
            for template in kind_rows.values():
                blocks = (
                    template["instances"].values()
                    if "instances" in template else [template]
                )  # a workflow row is one block: {last_run: {...}}
                for groups in blocks:
                    for axes in groups.values():
                        for axis in ("status", "freshness"):
                            if axes.get(axis):
                                tally[axes[axis]] = tally.get(axes[axis], 0) + 1
        return tally


TARGET, TSEG = "env/core/baseline", ["env.type=dev", "aws.account=dev"]
OTHER, OSEG = "env/ops/app", ["env.type=dev", "aws.account=dev"]
WORKFLOW, WSEG = "env/baseline", ["sha256=abc123"]


class OutdateLifecycleTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.ns = Namespace(Path(self._tmp.name))

    def test_a_freshly_published_result_is_up_to_date(self):
        self.ns.publish("target", TARGET, TSEG, run_id="r1")
        row = self.ns.row("target", TARGET, TSEG)
        self.assertEqual({"status": "passed", "freshness": "up_to_date",
                          "at": "2026-07-30T10:00:00Z"}, row)

    def test_outdating_moves_only_that_axis(self):
        """`status` describes the RUN and must not change when inputs move."""
        self.ns.publish("target", TARGET, TSEG, run_id="r1")
        self.ns.outdate("target", TARGET, TSEG)
        row = self.ns.row("target", TARGET, TSEG)
        self.assertEqual("outdated", row["freshness"])
        self.assertEqual("passed", row["status"])

    def test_republishing_clears_outdated(self):
        self.ns.publish("target", TARGET, TSEG, run_id="r1")
        self.ns.outdate("target", TARGET, TSEG)
        self.assertEqual("outdated", self.ns.row("target", TARGET, TSEG)["freshness"])
        self.ns.publish("target", TARGET, TSEG, run_id="r2",
                        at="2026-07-30T12:00:00Z")
        row = self.ns.row("target", TARGET, TSEG)
        self.assertEqual("up_to_date", row["freshness"])
        self.assertEqual("2026-07-30T12:00:00Z", row["at"])

    def test_outdating_one_instance_leaves_its_siblings_alone(self):
        self.ns.publish("target", TARGET, TSEG, run_id="r1")
        self.ns.publish("target", OTHER, OSEG, run_id="r2")
        self.ns.outdate("target", TARGET, TSEG)
        self.assertEqual("outdated", self.ns.row("target", TARGET, TSEG)["freshness"])
        self.assertEqual("up_to_date", self.ns.row("target", OTHER, OSEG)["freshness"])

    def test_outdating_the_deployment_leaves_the_plan_alone(self):
        """Groups are independent facts; one going stale says nothing of another."""
        self.ns.publish("target", TARGET, TSEG, run_id="r1")
        self.ns.publish("target", TARGET, TSEG, run_id="p1", group="plan",
                        action="plan")
        self.ns.outdate("target", TARGET, TSEG)
        self.assertEqual("outdated", self.ns.row("target", TARGET, TSEG)["freshness"])
        self.assertEqual("passed", self.ns.row("target", TARGET, TSEG, "plan")["status"])


class WorkflowHistoryTest(unittest.TestCase):
    """§Phase 73: a workflow publishes history. Its row is the LAST RUN — what it
    did, what selected its members, and which members it ran with. It holds no
    state, so nothing about it is rolled up from members or goes stale."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.ns = Namespace(Path(self._tmp.name))

    def test_a_workflow_row_is_its_last_run(self):
        self.ns.workflow_run(WORKFLOW, run_id="w1")
        self.assertEqual(
            {"status": "passed", "at": "2026-07-30T10:00:05Z"},
            self.ns.rows()["workflow"][WORKFLOW]["last_run"],
        )

    def test_the_newest_run_wins(self):
        self.ns.workflow_run(WORKFLOW, run_id="w1", at="2026-07-30T10:00:00Z")
        self.ns.workflow_run(WORKFLOW, run_id="w2", at="2026-07-30T12:00:00Z",
                             status="failed")
        row = self.ns.rows()["workflow"][WORKFLOW]["last_run"]
        self.assertEqual("failed", row["status"])
        self.assertEqual("2026-07-30T12:00:00Z", row["at"])

    def test_a_run_in_flight_reads_running(self):
        self.ns.workflow_run(WORKFLOW, run_id="w1", status="in_progress")
        self.assertEqual(
            "running", self.ns.rows()["workflow"][WORKFLOW]["last_run"]["status"]
        )

    def test_the_record_carries_the_targets_it_ran_with(self):
        targets = ["env/core/baseline", {"key": "env/ops/app", "action": "destroy"}]
        self.ns.workflow_run(WORKFLOW, run_id="w1", targets=targets,
                             default_action="provision")
        row = self.ns.rows()["workflow"][WORKFLOW]["last_run"]
        self.assertEqual(targets, row["targets"])
        self.assertEqual("provision", row["default_action"])

    def test_the_record_carries_the_selectors_that_matched(self):
        self.ns.workflow_run(
            WORKFLOW, run_id="w1",
            selectors={"match": {"execution_context.params.intent": "rebuild"}},
        )
        row = self.ns.rows()["workflow"][WORKFLOW]["last_run"]
        self.assertEqual(
            {"match": {"execution_context.params.intent": "rebuild"}}, row["selectors"]
        )

    def test_a_member_matched_without_selectors_omits_the_field(self):
        self.ns.workflow_run(WORKFLOW, run_id="w1")
        self.assertNotIn("selectors", self.ns.rows()["workflow"][WORKFLOW]["last_run"])

    def test_a_workflow_carries_no_freshness(self):
        """It owns no state, so nothing about it can go stale."""
        self.ns.workflow_run(WORKFLOW, run_id="w1")
        self.assertNotIn("freshness", self.ns.rows()["workflow"][WORKFLOW]["last_run"])

    def test_a_member_going_stale_does_not_touch_the_workflow_row(self):
        self.ns.publish("target", TARGET, TSEG, run_id="r1")
        self.ns.workflow_run(WORKFLOW, run_id="w1")
        before = self.ns.rows()["workflow"][WORKFLOW]
        self.ns.outdate("target", TARGET, TSEG)
        self.assertEqual(before, self.ns.rows()["workflow"][WORKFLOW])
        self.assertEqual("outdated", self.ns.row("target", TARGET, TSEG)["freshness"])

    def test_a_workflow_has_no_instance_layer(self):
        self.ns.workflow_run(WORKFLOW, run_id="w1")
        self.assertNotIn("instances", self.ns.rows()["workflow"][WORKFLOW])


class NamespaceCountsTest(unittest.TestCase):
    """A status view is read for 'how many need attention', not row by row."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.ns = Namespace(Path(self._tmp.name))

    def _three_targets(self) -> None:
        for index, (key, segments) in enumerate(
            ((TARGET, TSEG), (OTHER, OSEG), ("env/ops/ecr", OSEG)), start=1
        ):
            self.ns.publish("target", key, segments, run_id=f"r{index}")

    def test_a_settled_namespace_counts_all_up_to_date(self):
        self._three_targets()
        counts = self.ns.counts()
        self.assertEqual(3, counts["passed"])
        self.assertEqual(3, counts["up_to_date"])
        self.assertNotIn("outdated", counts)
        self.assertNotIn("running", counts)

    def test_outdating_two_moves_exactly_two(self):
        self._three_targets()
        self.ns.outdate("target", TARGET, TSEG)
        self.ns.outdate("target", OTHER, OSEG)
        counts = self.ns.counts()
        self.assertEqual(2, counts["outdated"])
        self.assertEqual(1, counts["up_to_date"])
        self.assertEqual(3, counts["passed"], "staleness must not disturb run status")

    def test_rerunning_one_returns_it_to_up_to_date(self):
        self._three_targets()
        self.ns.outdate("target", TARGET, TSEG)
        self.ns.outdate("target", OTHER, OSEG)
        self.ns.publish("target", TARGET, TSEG, run_id="r1b",
                        at="2026-07-30T12:00:00Z")
        counts = self.ns.counts()
        self.assertEqual(1, counts["outdated"])
        self.assertEqual(2, counts["up_to_date"])

    def test_a_running_run_is_counted_as_running_not_passed(self):
        self._three_targets()
        self.ns.slot("target", TARGET, TSEG, "in_progress")
        counts = self.ns.counts()
        self.assertEqual(1, counts["running"])
        self.assertEqual(2, counts["passed"])

    def test_a_first_ever_run_is_counted_before_it_publishes(self):
        """No pointer yet — discovery must still find it."""
        self.ns.slot("target", "env/new/thing", TSEG, "in_progress")
        self.assertEqual({"running": 1}, self.ns.counts())

    def test_counts_span_kinds_and_groups(self):
        self._three_targets()
        self.ns.publish("target", TARGET, TSEG, run_id="p1", group="plan",
                        action="plan")
        self.ns.workflow_run(WORKFLOW, run_id="w1")
        counts = self.ns.counts()
        self.assertEqual(5, counts["passed"], "3 deployments + 1 plan + 1 workflow run")
        self.assertEqual(3, counts["up_to_date"],
                         "only completed deployments carry freshness")


if __name__ == "__main__":
    unittest.main()


class SkipUpToDateUnderHistoryTest(unittest.TestCase):
    """§Phase 73: skipping reads the CHILD's pointer, so removing the workflow's
    changes nothing. A workflow always runs; skipping happens per member."""

    FACTS = {
        "source_commit": "a" * 40, "cfg_source_commit": "b" * 40,
        "source_state": "clean", "ref_policy": "commit_required",
        "target_definition_sha256": "c" * 64, "target_cfg_view_sha256": "d" * 64,
    }

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.parent = (
            root / "live" / common.compose_state_relpath("workflow", WORKFLOW, [])
            / "runs" / "w1"
        )
        self.parent.mkdir(parents=True)
        common.write_run_metadata(self.parent, {
            "run_id": "w1", "action": "provision", "run_type": "workflow",
            "result_name": WORKFLOW, "ctl_state_local_root": str(root),
            "ctl_state_locator": ["live"],
        })
        child = (
            root / "live"
            / common.compose_state_relpath("target", TARGET, ["account=dev"])
            / "runs" / "r1"
        )
        child.mkdir(parents=True)
        common.write_run_metadata(child, {
            "run_id": "r1", "action": "provision", "run_type": "target",
            "result_name": TARGET, "ctl_state_local_root": str(root),
            "ctl_state_locator": ["live"], "instance": ["account=dev"], **self.FACTS,
        })
        common.publish_committed_pointer(
            child, common.build_status_payload(child, "ok")
        )
        self.target_run = {
            "target": TARGET, "target_instance_params": ["account"], **self.FACTS
        }
        self.context = {"execution_context.params.account": "dev"}

    def _revision(self, action):
        return common.up_to_date_child_revision(
            self.parent, self.target_run, self.context, action
        )

    def test_a_matching_member_is_still_skippable(self):
        self.assertIsNotNone(self._revision("provision"))

    def test_the_action_check_still_holds(self):
        self.assertIsNone(self._revision("destroy"))

    def test_the_workflow_itself_publishes_no_pointer_to_skip_on(self):
        self.assertFalse((self.parent.parent.parent / "committed").exists())


class WorkflowRecordsItsCompositionTest(unittest.TestCase):
    """`record_workflow_members` writes what the row reads.

    The call was first placed in the wrong function, so nothing recorded the
    composition and every workflow row showed only status and time. These pin the
    writer to the reader.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.ns = Namespace(self.root)
        self.run_dir = (
            self.root / common.compose_state_relpath("workflow", WORKFLOW, [])
            / "runs" / "w1"
        )
        self.run_dir.mkdir(parents=True)
        common.write_run_metadata(self.run_dir, {
            "run_id": "w1", "run_type": "workflow", "action": "provision",
            "status": "ok", "updated_at": "2026-07-30T10:00:05Z",
        })

    def _record(self, runs: dict, cfg: dict) -> dict:
        common.record_workflow_members(self.run_dir, runs, cfg)
        return self.ns.rows()["workflow"][WORKFLOW]["last_run"]

    def test_a_member_taking_the_default_is_a_bare_key(self):
        row = self._record(
            {"a": {"target": TARGET, "action": "provision"}},
            {"default_action": "provision"},
        )
        self.assertEqual([TARGET], row["targets"])
        self.assertEqual("provision", row["default_action"])

    def test_a_member_that_differs_carries_its_action(self):
        row = self._record(
            {"a": {"target": TARGET, "action": "provision"},
             "b": {"target": OTHER, "action": "destroy"}},
            {"default_action": "provision"},
        )
        self.assertEqual([TARGET, {"key": OTHER, "action": "destroy"}], row["targets"])

    def test_the_matched_selectors_are_recorded_verbatim(self):
        selectors = {"match": {"execution_context.params.intent": "rebuild"}}
        row = self._record(
            {"a": {"target": TARGET, "action": "provision"}},
            {"default_action": "provision", "member_selectors": selectors},
        )
        self.assertEqual(selectors, row["selectors"])

    def test_a_member_matched_without_selectors_records_none(self):
        row = self._record(
            {"a": {"target": TARGET, "action": "provision"}},
            {"default_action": "provision"},
        )
        self.assertNotIn("selectors", row)

    def test_the_resolved_workflow_carries_its_member_fields(self):
        """`load_workflow_cfg` returned only meta + target_runs, dropping both —
        so the recorder had nothing to record and every row looked bare."""
        import tempfile as _tempfile

        with _tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "workflows").mkdir()
            common.write_yaml_file(root / "workflows" / "w.yaml", {"workflows": {
                "env/x": {
                    "operations": ["destroy"],
                    "target_keys": {"members": [{
                        "target_keys": ["env/a"],
                        "default_action": "destroy",
                        "selectors": {"match": {
                            "execution_context.ctl.operation": "destroy"}},
                    }]},
                }
            }})
            cfg = common.load_workflow_cfg(
                root, "p", "destroy", "env/x",
                {"execution_context.ctl.operation": "destroy"},
            )
            self.assertEqual("destroy", cfg["default_action"])
            self.assertEqual(
                {"match": {"execution_context.ctl.operation": "destroy"}},
                cfg["member_selectors"],
            )

    def test_a_running_workflow_already_shows_its_composition(self):
        """Recorded at RESOLUTION, not after the slow cfg and guardrail phases —
        otherwise a status read during them shows a running workflow with no
        members, while the composition was known the whole time."""
        common.write_run_metadata(self.run_dir, {
            "run_id": "w1", "run_type": "workflow", "action": "provision",
            "status": "in_progress", "updated_at": "2026-07-30T10:00:05Z",
        })
        row = self._record(
            {"a": {"target": TARGET, "action": "provision"}},
            {"default_action": "provision",
             "member_selectors": {"match": {"execution_context.ctl.operation":
                                            "provision"}}},
        )
        self.assertEqual("running", row["status"])
        self.assertEqual([TARGET], row["targets"])
        self.assertEqual("provision", row["default_action"])
        self.assertIn("selectors", row)
