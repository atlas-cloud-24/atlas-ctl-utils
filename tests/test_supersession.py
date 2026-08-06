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

from engine.state import status as state_status  # noqa: E402

BASELINE = "env/core/baseline/instances/env.type=dev"
TECH_JOBS = "env/core/tech_jobs/instances/env.type=dev"

# A relation names KEYS. The verdict is per INSTANCE: these two are alternatives in
# dev, and independently so in every other environment.
RELATIONS = {
    "env/core": {"members": ["env/core/baseline", "env/core/tech_jobs"]},
}


class ModeSupersessionTest(unittest.TestCase):
    def test_the_member_carrying_the_overlays_is_in_effect(self):
        freshness, by = state_status.resolve_supersession(
            RELATIONS, TECH_JOBS, {BASELINE: [], TECH_JOBS: ["tech_jobs"]}
        )
        self.assertIsNone(freshness)
        self.assertIsNone(by)

    def test_the_other_member_is_superseded_and_says_by_whom(self):
        freshness, by = state_status.resolve_supersession(
            RELATIONS, BASELINE, {BASELINE: [], TECH_JOBS: ["tech_jobs"]}
        )
        self.assertEqual(state_status.Freshness.SUPERSEDED, freshness)
        self.assertEqual(TECH_JOBS, by)

    def test_an_instance_in_no_group_is_untouched(self):
        """Most targets are in no group at all, and must not acquire a verdict."""

        freshness, by = state_status.resolve_supersession(
            RELATIONS, "env/ops/dbs/instances/env.type=dev", {BASELINE: [], TECH_JOBS: ["tech_jobs"]}
        )
        self.assertIsNone(freshness)
        self.assertIsNone(by)

    def test_one_published_member_supersedes_nothing(self):
        """Supersession is a relationship between what RAN. With one member
        published, nothing has been replaced — reporting the other as superseded
        would claim a switch that never happened."""

        freshness, by = state_status.resolve_supersession(RELATIONS, BASELINE, {BASELINE: []})
        self.assertIsNone(freshness)
        self.assertIsNone(by)

    def test_no_published_member_supersedes_nothing(self):
        freshness, by = state_status.resolve_supersession(RELATIONS, BASELINE, {})
        self.assertIsNone(freshness)
        self.assertIsNone(by)

    def test_an_empty_group_registry_is_inert(self):
        """Guards the default: an empty `exclusive_target_relations: {}` must change nothing."""

        freshness, by = state_status.resolve_supersession({}, BASELINE, {BASELINE: [], TECH_JOBS: ["x"]})
        self.assertIsNone(freshness)
        self.assertIsNone(by)


class SupersessionReachesTheRowTest(unittest.TestCase):
    """The verdict has to arrive in `ctl status`, not merely be computable.

    `resolve_supersession` was correct and uncalled for a while: the committed
    pointer did not publish `plt_overlays`, so nothing could tell which member a
    deployment carried.
    """

    def _namespace(self, tmp: Path, applied: dict[str, list[str]]) -> Path:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
        from engine.kernel import yaml_io as kernel_yaml_io
        from engine.run import addressing as run_addressing

        namespace = tmp / "live"
        for address, overlays in applied.items():
            key, segments = run_addressing.split_target_instance_address(address)
            instance = namespace / run_addressing.compose_state_relpath("target", key, segments)
            (instance / "committed").mkdir(parents=True, exist_ok=True)
            kernel_yaml_io.write_yaml_file(
                instance / "committed" / "mutative.yaml",
                {"run_id": "r1", "status": "ok", "action": "provision",
                 "committed_at": "2026-08-05T00:00:00Z", "plt_overlays": overlays,
                 "ref_policy": "commit_required"},
            )
            slot = instance / "ok" / "r1"
            slot.mkdir(parents=True, exist_ok=True)
            kernel_yaml_io.write_yaml_file(
                slot / "STATUS.yaml",
                {"run_id": "r1", "status": "ok", "action": "provision",
                 "updated_at": "2026-08-05T00:00:00Z"},
            )
        return namespace

    def test_the_replaced_member_reports_its_standing_and_by_whom(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            namespace = self._namespace(
                Path(tmp), {BASELINE: [], TECH_JOBS: ["tech_jobs"]}
            )
            rows = state_status.compute_namespace_status_map(namespace, RELATIONS)
            baseline = rows["target"]["env/core/baseline"]["instances"]["env.type=dev"]["mutative"]
            tech_jobs = rows["target"]["env/core/tech_jobs"]["instances"]["env.type=dev"]["mutative"]
            self.assertEqual("superseded", baseline["standing"])
            self.assertEqual(
                "target/env/core/tech_jobs/instances/env.type=dev",
                baseline["superseded_by"],
            )
            self.assertNotIn("standing", tech_jobs)

    def test_without_a_declared_group_neither_is_superseded(self):
        """Guards the default: `exclusive_target_relations: {}` must leave every row alone."""

        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            namespace = self._namespace(
                Path(tmp), {BASELINE: [], TECH_JOBS: ["tech_jobs"]}
            )
            rows = state_status.compute_namespace_status_map(namespace, {})
            baseline = rows["target"]["env/core/baseline"]["instances"]["env.type=dev"]["mutative"]
            self.assertNotIn("standing", baseline)


if __name__ == "__main__":
    unittest.main()
