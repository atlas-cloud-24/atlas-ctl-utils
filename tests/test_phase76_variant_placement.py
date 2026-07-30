"""A variant must place a target into a workflow whose entries carry actions.

Phase 73 gave every workflow a `default_action`, so `expand_workflow_imports`
stopped emitting bare strings and started emitting mappings. The placement code
still compared its anchor against the list with `in`, so the anchor matched
nothing, every variant was skipped at INFO level, and the run reported success
WITHOUT the target it was asked to add.

That shipped past the whole suite because every variant-touching test passed
`ctl_variants=[]` — the argument was threaded, never exercised. So these tests
build the post-Phase-73 entry shape from the real expansion rather than by hand,
and assert on what ends up in `target_runs`.
"""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


WORKFLOW = "env/baseline"
ANCHOR = "env/ops/dbs"
INSERTED = "env/ops/db_artificial_populator"


def _workflow_cfg(*, default_action="provision", target_keys=None) -> dict:
    """A workflow resolved the way the engine resolves one, not a hand-built dict."""
    workflows = {
        WORKFLOW: {
            "default_action": default_action,
            "target_keys": target_keys if target_keys is not None else [ANCHOR, "env/core/baseline"],
        }
    }
    cfg = {
        "meta": {"name": f"provision/{WORKFLOW}", "action": "provision"},
        "target_runs": common.expand_workflow_imports(workflows, WORKFLOW),
    }
    if default_action:
        cfg["default_action"] = default_action
    return cfg


def _apply(variant: dict, workflow_cfg: dict, *, name="db_pop", targets=None):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        common.write_yaml_file(root / "variants.yaml", {"variants": {"provision": {name: variant}}})
        inventory = {"targets": targets if targets is not None else {INSERTED: {}}}
        return common.apply_ctl_variants_to_workflow_cfg(
            root, workflow_cfg, inventory,
            execution_context={}, inventory_name="provision",
            workflow_name=WORKFLOW, ctl_variants=[name],
        )


def _placement(**overrides) -> dict:
    return {"target_key": INSERTED, "workflow_key": WORKFLOW,
            "after_target_key": ANCHOR, **overrides}


class PlacementTest(unittest.TestCase):
    """The regression: an anchor must resolve against action-carrying entries."""

    def test_a_variant_is_inserted_after_its_anchor(self):
        out = _apply(_placement(), _workflow_cfg())
        keys = [common.workflow_target_run_key(e) for e in out["target_runs"]]
        self.assertEqual([ANCHOR, INSERTED, "env/core/baseline"], keys)

    def test_before_anchor_inserts_ahead_of_it(self):
        out = _apply(
            _placement(after_target_key=None, before_target_key=ANCHOR), _workflow_cfg()
        )
        keys = [common.workflow_target_run_key(e) for e in out["target_runs"]]
        self.assertEqual([INSERTED, ANCHOR, "env/core/baseline"], keys)

    def test_an_absent_anchor_is_skipped_not_failed(self):
        out = _apply(_placement(after_target_key="env/nowhere"), _workflow_cfg())
        keys = [common.workflow_target_run_key(e) for e in out["target_runs"]]
        self.assertNotIn(INSERTED, keys)


class InsertedEntryTest(unittest.TestCase):
    """The latent second defect: a bare key would bypass the Phase 73 gate."""

    def test_the_inserted_entry_carries_the_workflow_default_action(self):
        out = _apply(_placement(), _workflow_cfg())
        entry = next(e for e in out["target_runs"]
                     if common.workflow_target_run_key(e) == INSERTED)
        self.assertEqual({"id": INSERTED, "target": INSERTED, "action": "provision"}, entry)

    def test_a_variant_may_declare_its_own_action(self):
        out = _apply(_placement(action="destroy"), _workflow_cfg())
        signatures = [common.workflow_target_run_signature(e) for e in out["target_runs"]]
        self.assertIn((INSERTED, "destroy"), signatures)

    def test_every_inserted_entry_has_an_action(self):
        """No entry may reach the pipeline actionless — the Phase 73 invariant."""
        out = _apply(_placement(), _workflow_cfg())
        self.assertTrue(all(
            common.workflow_target_run_signature(e)[1] for e in out["target_runs"]
        ))

    def test_an_unknown_action_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "expected one of"):
            _apply(_placement(action="bulldoze"), _workflow_cfg())


class DuplicateAndAmbiguityTest(unittest.TestCase):
    """A key may repeat when actions differ, so identity is the PAIR."""

    def test_the_same_key_and_action_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "duplicate target"):
            _apply(_placement(target_key=ANCHOR), _workflow_cfg(), targets={ANCHOR: {}})

    def test_the_same_key_with_a_different_action_is_allowed(self):
        out = _apply(
            _placement(target_key=ANCHOR, action="destroy"),
            _workflow_cfg(), targets={ANCHOR: {}},
        )
        signatures = [common.workflow_target_run_signature(e) for e in out["target_runs"]]
        self.assertIn((ANCHOR, "provision"), signatures)
        self.assertIn((ANCHOR, "destroy"), signatures)

    def test_an_ambiguous_anchor_is_refused_rather_than_resolved_by_position(self):
        """Two entries share the anchor key, so 'after' names no single place."""
        workflow_cfg = _workflow_cfg(target_keys=[
            {"key": ANCHOR, "action": "destroy"},
            {"key": ANCHOR, "action": "provision"},
        ])
        with self.assertRaisesRegex(RuntimeError, "matches 2 entries"):
            _apply(_placement(), workflow_cfg)


class HelperTest(unittest.TestCase):
    """The helpers the placement reuses, over both entry shapes."""

    def test_signature_reads_both_shapes(self):
        self.assertEqual(("a", None), common.workflow_target_run_signature("a"))
        self.assertEqual(
            ("a", "provision"),
            common.workflow_target_run_signature({"id": "a", "target": "a", "action": "provision"}),
        )

    def test_key_is_the_target_not_the_id(self):
        """`id` is display identity; an anchor names a TARGET."""
        entry = {"id": "run-1", "target": "env/ops/dbs", "action": "provision"}
        self.assertEqual("env/ops/dbs", common.workflow_target_run_key(entry))
        self.assertEqual("run-1", common.get_workflow_target_run_id(entry))

    def test_an_invalid_entry_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "invalid workflow target_run entry"):
            common.workflow_target_run_signature(42)

    def test_an_anchor_declared_twice_is_a_contradiction(self):
        with self.assertRaisesRegex(RuntimeError, "cannot set both"):
            common.variant_anchor(
                {"before_target_key": "a", "after_target_key": "b"}, label="variant 'v'"
            )

    def test_an_anchor_must_be_declared(self):
        with self.assertRaisesRegex(RuntimeError, "must define"):
            common.variant_anchor({}, label="variant 'v'")


if __name__ == "__main__":
    unittest.main()
