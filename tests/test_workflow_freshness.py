"""A workflow's freshness is a FUNCTION OF ITS MEMBERS.

The child already computes whether it is fresh, so the composition asks it
rather than re-deriving. The obvious alternative — diffing recorded
`child_revisions` against each child's live pointer — compares `snapshot_sha256`,
which hashes the WHOLE `RUN.yaml` including `run_id` and timestamps. A child
re-run with identical inputs would read as drift, which is the confusion this
avoids.

Worst-of, in a fixed order: OUTDATED is a fact, UNDETERMINED is the absence of
one, and a composition cannot be fresher than its least fresh member. Each member
retains the target status row that supplied its freshness, so every presentation
reads facts from the report rather than reconstructing them.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.state import status as state_status  # noqa: E402
from engine.state import status_rows as state_status_rows

MEMBERS = [
    "target/env/core/baseline/instances/env.type=dev",
    "target/env/ops/dbs/instances/env.type=dev",
]


def _with_member_freshness(values: list[str | None]):
    """Each member answers for itself; the composition only combines."""

    answers = iter(values)

    def fake(namespace_root, action, spec):
        value = next(answers)
        return (
            {"address": spec["address"]}
            if value is None
            else {"address": spec["address"], "freshness": value}
        )

    return mock.patch.object(state_status, "compute_target_instance_status", fake)


class WorkflowFreshnessTest(unittest.TestCase):
    def _resolve(self, values, members=MEMBERS):
        with tempfile.TemporaryDirectory() as tmp, _with_member_freshness(values):
            return state_status_rows.workflow_member_freshness(Path(tmp), "provision", members)

    def test_every_member_fresh_makes_the_workflow_fresh(self):
        freshness, members, reasons = self._resolve(["up_to_date", "up_to_date"])
        self.assertEqual(state_status.Freshness.UP_TO_DATE, freshness)
        self.assertEqual(reasons, [])
        self.assertEqual([m["address"] for m in members], MEMBERS)

    def test_one_outdated_member_outdates_the_workflow(self):
        freshness, _, reasons = self._resolve(["up_to_date", "outdated"])
        self.assertEqual(state_status.Freshness.OUTDATED, freshness)
        self.assertEqual(reasons, ["target/env/ops/dbs/instances/env.type=dev: outdated"])

    def test_outdated_outranks_undetermined(self):
        """A fact beats the absence of one: something IS known to have changed."""

        freshness, _, _ = self._resolve(["undetermined", "outdated"])
        self.assertEqual(state_status.Freshness.OUTDATED, freshness)

    def test_one_undetermined_member_makes_the_workflow_undetermined(self):
        """Never `up_to_date` — a branch-pinned member cannot be checked, so the
        composition cannot claim it matches either."""

        freshness, _, reasons = self._resolve(["up_to_date", "undetermined"])
        self.assertEqual(state_status.Freshness.UNDETERMINED, freshness)
        self.assertEqual(reasons, ["target/env/ops/dbs/instances/env.type=dev: undetermined"])

    def test_a_member_carries_its_own_verdict(self):
        """A rolled-up value is unreadable without the member that caused it."""

        _, members, _ = self._resolve(["up_to_date", "outdated"])
        self.assertEqual(
            [
                ("target/env/core/baseline/instances/env.type=dev", "up_to_date"),
                ("target/env/ops/dbs/instances/env.type=dev", "outdated"),
            ],
            [(m["address"], m["freshness"]) for m in members],
        )

    def test_a_member_retains_the_target_status_row(self):
        """The report owns facts; a renderer must never read state to fill them."""

        answer = {
            "status": "passed",
            "last_action": "provision",
            "freshness": "up_to_date",
            "time": "2026-08-09T01:00:00Z",
            "label": "release",
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.object(
                state_status,
                "compute_target_instance_status",
                return_value=answer,
            ),
        ):
            _, members, _ = state_status_rows.workflow_member_freshness(
                Path(tmp),
                "provision",
                [MEMBERS[0]],
            )
        self.assertEqual({**answer, "address": MEMBERS[0]}, members[0])

    def test_a_workflow_with_no_members_has_no_freshness(self):
        """Not `up_to_date`: there is nothing to be fresh about, and claiming
        freshness for an empty composition would read as a verdict."""

        freshness, members, _ = self._resolve([], members=[])
        self.assertIsNone(freshness)
        self.assertEqual(members, [])

    def test_a_member_that_never_published_leaves_no_verdict(self):
        freshness, members, _ = self._resolve([None, "up_to_date"])
        self.assertEqual(state_status.Freshness.UP_TO_DATE, freshness)
        self.assertNotIn("freshness", members[0])

    def test_a_member_action_survives_into_the_row(self):
        """A member running a different action is recorded as `{instance, action}`;
        dropping the action makes a destroy member read like every other one."""

        recorded = [{"instance": MEMBERS[0], "action": "destroy"}, MEMBERS[1]]
        _, members, _ = self._resolve(["up_to_date", "up_to_date"], members=recorded)
        self.assertEqual("destroy", members[0]["action"])
        self.assertNotIn("action", members[1])


if __name__ == "__main__":
    unittest.main()
