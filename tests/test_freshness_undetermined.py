"""A branch-pinned target reports `undetermined`, never `up_to_date`.

`git_branch: main` records the commit that was deployed and nothing about where
`main` points now. Answering that is a REMOTE read; status is a pure local
computation, so `up_to_date` would be a claim the engine cannot support — and it
made that claim forever, for every branch-pinned target.

The value is not decoration: `--skip-up-to-date` already refuses to reuse a
member whose `ref_policy` is not `commit_required`, so the fact was known and
simply had no name in the row a reader sees.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.state import status as state_status  # noqa: E402

SPEC = {
    "target_definition_sha256": "def",
    "target_cfg_view_sha256": "view",
}


def _pointer(**overrides) -> dict:
    pointer = {
        "status": "ok",
        "target_definition_sha256": "def",
        "target_cfg_view_sha256": "view",
        "ref_policy": "commit_required",
    }
    pointer.update(overrides)
    return pointer


class FreshnessUndeterminedTest(unittest.TestCase):
    def test_a_commit_pinned_target_is_up_to_date(self):
        """Guards the rest: if everything were undetermined the axis would be noise."""

        freshness, reasons = state_status._freshness(_pointer(), SPEC)
        self.assertEqual(freshness, state_status.Freshness.UP_TO_DATE)
        self.assertEqual(reasons, [])

    def test_a_branch_pinned_target_is_undetermined(self):
        freshness, reasons = state_status._freshness(
            _pointer(ref_policy="local_dirty_allowed"), SPEC
        )
        self.assertEqual(freshness, state_status.Freshness.UNDETERMINED)
        self.assertEqual(reasons, ["source ref is a branch"])

    def test_a_changed_content_axis_outranks_an_unknowable_ref(self):
        """A branch-pinned target whose cfg changed is OUTDATED, not unknowable —
        that half is knowable either way, and it is the half that decides whether
        to re-run."""

        freshness, reasons = state_status._freshness(
            _pointer(ref_policy="local_dirty_allowed", target_cfg_view_sha256="other"),
            SPEC,
        )
        self.assertEqual(freshness, state_status.Freshness.OUTDATED)
        self.assertIn("target cfg view changed", reasons)

    def test_a_missing_pointer_is_outdated_not_undetermined(self):
        """Nothing was ever published, which is a fact, not an unknown."""

        freshness, _ = state_status._freshness(None, SPEC)
        self.assertEqual(freshness, state_status.Freshness.OUTDATED)


if __name__ == "__main__":
    unittest.main()
