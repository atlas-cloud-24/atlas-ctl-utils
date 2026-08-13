"""An exclusive relation's members exclude each other: one is in effect, the rest are superseded.

Two targets can be alternatives over one deployment — `env/core/baseline` and
`env/core/tech_jobs` name the same procedure with different overlays. Each owns
its own ctl-state directory and its own status row, so each computes freshness
against ITS OWN cfg and both read `up_to_date` while only one is deployed.

`superseded` is the missing verdict. It is deliberately NOT `outdated`: nothing
about the replaced member is stale — its record still matches its own cfg — an
alternative simply took its place. Helm names the same state the same way: a
release revision that WAS deployed and has since been replaced is `superseded`.

ctl cannot derive the grouping. A target names a procedure, a procedure names
steps, and only a step's tooling knows what it operates on — so the group is
declared in cfg and nothing else could supply it.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.run import actions as run_actions  # noqa: E402
from engine.state import status as state_status  # noqa: E402
from engine.state import status_rows as state_status_rows

BASELINE = "env/core/baseline/instances/env.type=dev"
TECH_JOBS = "env/core/tech_jobs/instances/env.type=dev"

# A relation names KEYS. The verdict is per INSTANCE: these two are alternatives in
# dev, and independently so in every other environment.
RELATIONS = {
    "env/core": {"members": ["env/core/baseline", "env/core/tech_jobs"]},
}


def _event(
    committed_at: str,
    action=run_actions.Action.PROVISION,
    *,
    run_id: str | None = None,
) -> dict:
    return {
        "action": action,
        "committed_at": committed_at,
        "run_id": run_id or committed_at,
    }


class TargetStandingResolutionTest(unittest.TestCase):
    def test_the_newest_provisioned_member_is_active(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            TECH_JOBS,
            {
                BASELINE: _event("2026-08-05T00:00:00Z"),
                TECH_JOBS: _event("2026-08-05T00:01:00Z"),
            },
        )
        self.assertEqual(state_status.Standing.ACTIVE, standing)
        self.assertIsNone(by)

    def test_the_other_member_is_superseded_and_says_by_whom(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            BASELINE,
            {
                BASELINE: _event("2026-08-05T00:00:00Z"),
                TECH_JOBS: _event("2026-08-05T00:01:00Z"),
            },
        )
        self.assertEqual(state_status.Standing.SUPERSEDED, standing)
        self.assertEqual(TECH_JOBS, by)

    def test_a_later_provision_switches_the_active_member_back(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            BASELINE,
            {
                BASELINE: _event("2026-08-05T00:02:00Z"),
                TECH_JOBS: _event("2026-08-05T00:01:00Z"),
            },
        )
        self.assertEqual(state_status.Standing.ACTIVE, standing)
        self.assertIsNone(by)

    def test_an_instance_in_no_group_is_untouched(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            "env/ops/dbs/instances/env.type=dev",
            {BASELINE: _event("2026-08-05T00:00:00Z")},
        )
        self.assertIsNone(standing)
        self.assertIsNone(by)

    def test_one_provisioned_member_is_active(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            BASELINE,
            {BASELINE: _event("2026-08-05T00:00:00Z")},
        )
        self.assertEqual(state_status.Standing.ACTIVE, standing)
        self.assertIsNone(by)

    def test_no_published_member_has_no_standing(self):
        standing, by = state_status.StandingResolver.target_from_evidence(RELATIONS, BASELINE, {})
        self.assertIsNone(standing)
        self.assertIsNone(by)

    def test_the_newest_destroy_leaves_no_active_member(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            TECH_JOBS,
            {
                BASELINE: _event("2026-08-05T00:00:00Z"),
                TECH_JOBS: _event("2026-08-05T00:01:00Z", run_actions.Action.DESTROY),
            },
        )
        self.assertIsNone(standing)
        self.assertIsNone(by)

    def test_run_id_breaks_an_equal_timestamp_tie(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            BASELINE,
            {
                BASELINE: _event("2026-08-05T00:00:00Z", run_id="r1"),
                TECH_JOBS: _event("2026-08-05T00:00:00Z", run_id="r2"),
            },
        )
        self.assertEqual(state_status.Standing.SUPERSEDED, standing)
        self.assertEqual(TECH_JOBS, by)

    def test_timestamp_orders_distinct_events_with_the_same_run_id(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            RELATIONS,
            BASELINE,
            {
                BASELINE: _event("2026-08-05T00:00:00Z", run_id="r1"),
                TECH_JOBS: _event("2026-08-05T00:01:00Z", run_id="r1"),
            },
        )
        self.assertEqual(state_status.Standing.SUPERSEDED, standing)
        self.assertEqual(TECH_JOBS, by)

    def test_duplicate_commit_coordinates_are_refused(self):
        with self.assertRaisesRegex(RuntimeError, "distinct commit coordinates"):
            state_status.StandingResolver.target_from_evidence(
                RELATIONS,
                BASELINE,
                {
                    BASELINE: _event("2026-08-05T00:00:00Z", run_id="r1"),
                    TECH_JOBS: _event("2026-08-05T00:00:00Z", run_id="r1"),
                },
            )

    def test_an_empty_group_registry_is_inert(self):
        standing, by = state_status.StandingResolver.target_from_evidence(
            {}, BASELINE, {BASELINE: _event("2026-08-05T00:00:00Z")}
        )
        self.assertIsNone(standing)
        self.assertIsNone(by)


class WorkflowStandingResolutionTest(unittest.TestCase):
    def test_a_declared_workflow_without_member_evidence_has_no_standing(self):
        standing, by = state_status.StandingResolver(Path("/unused")).workflow(
            RELATIONS, BASELINE, []
        )
        self.assertIsNone(standing)
        self.assertIsNone(by)

    def test_active_member_evidence_makes_the_workflow_active(self):
        standing, by = state_status.StandingResolver(Path("/unused")).workflow(
            RELATIONS, BASELINE, [{"standing": "active"}]
        )
        self.assertEqual(state_status.Standing.ACTIVE, standing)
        self.assertIsNone(by)


class SupersessionReachesTheRowTest(unittest.TestCase):
    """Standing must reach every applicable status row."""

    def _namespace(self, tmp: Path, addresses: list[str]) -> Path:
        import sys as _sys

        _sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
        from engine.kernel import yaml_io as kernel_yaml_io
        from engine.run import addressing as run_addressing

        namespace = tmp / "live"
        for index, address in enumerate(addresses):
            key, segments = run_addressing.split_target_instance_address(address)
            instance = namespace / run_addressing.compose_state_relpath("target", key, segments)
            (instance / "committed").mkdir(parents=True, exist_ok=True)
            kernel_yaml_io.write_yaml_file(
                instance / "committed" / "mutative.yaml",
                {
                    "run_id": f"r{index}",
                    "status": "ok",
                    "action": "provision",
                    "committed_at": f"2026-08-05T00:00:{index:02d}Z",
                    "ref_policy": "commit_required",
                },
            )
            slot = instance / "ok" / f"r{index}"
            slot.mkdir(parents=True, exist_ok=True)
            kernel_yaml_io.write_yaml_file(
                slot / "STATUS.yaml",
                {
                    "run_id": f"r{index}",
                    "status": "ok",
                    "action": "provision",
                    "updated_at": "2026-08-05T00:00:00Z",
                },
            )
        return namespace

    def test_the_replaced_member_reports_its_standing_and_by_whom(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            namespace = self._namespace(Path(tmp), [BASELINE, TECH_JOBS])
            rows = state_status_rows.compute_namespace_status_map(namespace, RELATIONS)
            baseline = rows["target"]["env/core/baseline"]["instances"]["env.type=dev"]["mutative"]
            tech_jobs = rows["target"]["env/core/tech_jobs"]["instances"]["env.type=dev"][
                "mutative"
            ]
            self.assertEqual("superseded", baseline["standing"])
            self.assertEqual(
                "target/env/core/tech_jobs/instances/env.type=dev",
                baseline["superseded_by"],
            )
            self.assertEqual("active", tech_jobs["standing"])

    def test_plan_row_uses_committed_mutative_standing(self):
        import tempfile

        from engine.kernel import yaml_io as kernel_yaml_io
        from engine.run import addressing as run_addressing

        with tempfile.TemporaryDirectory() as tmp:
            namespace = self._namespace(Path(tmp), [BASELINE, TECH_JOBS])
            key, segments = run_addressing.split_target_instance_address(TECH_JOBS)
            instance = namespace / run_addressing.compose_state_relpath("target", key, segments)
            kernel_yaml_io.write_yaml_file(
                instance / "committed" / "plan.yaml",
                {
                    "run_id": "plan-r1",
                    "status": "ok",
                    "action": "plan",
                    "committed_at": "2026-08-05T00:02:00Z",
                },
            )

            rows = state_status_rows.compute_namespace_status_map(namespace, RELATIONS)
            plan = rows["target"]["env/core/tech_jobs"]["instances"]["env.type=dev"]["plan"]
            self.assertEqual("active", plan["standing"])

    def test_without_a_declared_group_neither_is_superseded(self):
        """Guards the default: `exclusive_target_relations: {}` must leave every row alone."""

        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            namespace = self._namespace(Path(tmp), [BASELINE, TECH_JOBS])
            rows = state_status_rows.compute_namespace_status_map(namespace, {})
            baseline = rows["target"]["env/core/baseline"]["instances"]["env.type=dev"]["mutative"]
            self.assertNotIn("standing", baseline)


if __name__ == "__main__":
    unittest.main()
