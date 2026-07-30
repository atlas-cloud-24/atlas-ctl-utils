"""§Phase 69: `--all` covers the assignments DECLARED in coverage.yaml.

The list lives in the guardrail repository, beside the baselines it governs, so
adding an assignment and the baseline it produces land in one commit and one
review. Deliberately not derived from `fan_out_param_sets`: those declare what is
DEPLOYED together, which is a different question from what must be GUARDED.

The second test class is the drift guard in the other direction — a baseline that
exists but is no longer declared would never be regenerated and would go stale
without anything noticing. That is exactly how the pre-Phase-65 `iam_role_accounts`
baselines survived.
"""

import sys
import tempfile
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
ATLAS_ROOT = REPO_ROOT.parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))
sys.path.insert(0, str(REPO_ROOT / "cfg"))

import regenerate_guardrails as rg  # noqa: E402

GUARDRAILS_REPO = ATLAS_ROOT / "cfg" / "oxygen" / "oxygen-cfg-guardrails"

COVERAGE = """
guardrail_coverage:
  plt:
    common:
      aws.region: eu-west-2
    assignments:
      - landing_zone: live
        aws.account: dev
      - landing_zone: canary
        aws.account: prodlike
        aws.region: us-east-1
  ctl:
    common:
      aws.region: eu-west-2
    assignments:
      - landing_zone: live
"""


class LoadCoverageTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "coverage.yaml").write_text(COVERAGE)

    def tearDown(self):
        self._tmp.cleanup()

    def test_common_is_merged_into_every_assignment(self):
        first = rg.load_coverage_assignments(self.root, "plt")[0]
        self.assertEqual("eu-west-2", first["aws.region"])
        self.assertEqual("dev", first["aws.account"])

    def test_an_assignment_may_override_a_common_axis(self):
        """`common` is a default, not a lock — a genuinely different axis wins."""
        second = rg.load_coverage_assignments(self.root, "plt")[1]
        self.assertEqual("us-east-1", second["aws.region"])

    def test_modes_are_independent(self):
        self.assertEqual(2, len(rg.load_coverage_assignments(self.root, "plt")))
        self.assertEqual(1, len(rg.load_coverage_assignments(self.root, "ctl")))

    def test_missing_file_is_a_clear_error(self):
        with tempfile.TemporaryDirectory() as empty:
            with self.assertRaisesRegex(RuntimeError, "requires a coverage declaration"):
                rg.load_coverage_assignments(Path(empty), "plt")

    def test_unknown_mode_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "no guardrail_coverage.gcp block"):
            rg.load_coverage_assignments(self.root, "gcp")

    def test_duplicate_assignment_is_rejected(self):
        (self.root / "coverage.yaml").write_text(
            COVERAGE + """
      - landing_zone: live
"""
        )
        with self.assertRaisesRegex(RuntimeError, "duplicates an earlier assignment"):
            rg.load_coverage_assignments(self.root, "ctl")

    def test_empty_assignment_list_is_rejected(self):
        (self.root / "coverage.yaml").write_text(
            "guardrail_coverage:\n  plt:\n    assignments: []\n"
        )
        with self.assertRaisesRegex(RuntimeError, "must be a non-empty list"):
            rg.load_coverage_assignments(self.root, "plt")



@unittest.skipUnless(GUARDRAILS_REPO.is_dir(), "oxygen guardrail repo not present")
class CommittedBaselinesAreDeclaredTest(unittest.TestCase):
    """Every committed baseline must be reachable from coverage.yaml.

    Without this, a baseline can outlive its declaration: `--all` stops
    regenerating it, nothing reports it, and it drifts silently while still being
    enforced against real runs.
    """

    def _declared_identities(self, mode: str) -> set[frozenset]:
        out = set()
        for assignment in rg.load_coverage_assignments(GUARDRAILS_REPO, mode):
            items = set(assignment.items())
            # a baseline records the SUBSET its policy declares as instance_params,
            # so any subset of a declared assignment is reachable from it
            out.add(frozenset(items))
        return out

    def test_no_baseline_is_undeclared(self):
        orphans = []
        for path in sorted((GUARDRAILS_REPO / "invariants").rglob("*.yaml")):
            mode = path.parent.name
            if mode not in ("plt", "ctl"):
                continue
            declared = self._declared_identities(mode)
            doc = yaml.safe_load(path.read_text()) or {}
            for baseline in doc.get("guardrail_baselines") or []:
                params = (
                    (baseline.get("subject") or {}).get("instance") or {}
                ).get("params") or {}
                identity = {
                    k.replace("execution_context.params.", ""): str(v)
                    for k, v in params.items()
                }
                if not identity:
                    continue  # a policy with no instance_params owns one global baseline
                if not any(
                    set(identity.items()) <= assignment for assignment in declared
                ):
                    orphans.append(
                        f"{path.relative_to(GUARDRAILS_REPO)} :: {identity}"
                    )
        self.assertEqual(
            [],
            orphans,
            "baselines exist that coverage.yaml does not declare — they will never "
            "be regenerated:\n" + "\n".join(f"  {o}" for o in orphans),
        )


if __name__ == "__main__":
    unittest.main()
