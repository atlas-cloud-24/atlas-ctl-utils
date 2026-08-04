"""A variant must place a target into a workflow whose entries carry actions.

 gave every workflow a `default_action`, so `expand_workflow_imports`
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

from engine.catalog import workflow as catalog_workflow
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import addressing as run_addressing

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
        "target_runs": catalog_workflow.expand_workflow_imports(workflows, WORKFLOW),
    }
    if default_action:
        cfg["default_action"] = default_action
    return cfg


def _apply(variant: dict, workflow_cfg: dict, *, name="db_pop", targets=None):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        kernel_yaml_io.write_yaml_file(root / "variants.yaml", {"variants": {"provision": {name: variant}}})
        action = {"targets": targets if targets is not None else {INSERTED: {}}}
        return catalog_workflow.apply_ctl_variants_to_workflow_cfg(
            root, workflow_cfg, action,
            execution_context={}, action="provision",
            workflow_name=WORKFLOW, ctl_variants=[name],
        )


def _placement(**overrides) -> dict:
    return {"target_key": INSERTED, "workflow_key": WORKFLOW,
            "after_target_key": ANCHOR, **overrides}


class PlacementTest(unittest.TestCase):
    """

    the regression: an anchor must resolve against action-carrying entries."""

    def test_a_variant_is_inserted_after_its_anchor(self):
        out = _apply(_placement(), _workflow_cfg())
        keys = [run_addressing.workflow_target_run_key(e) for e in out["target_runs"]]
        self.assertEqual([ANCHOR, INSERTED, "env/core/baseline"], keys)

    def test_before_anchor_inserts_ahead_of_it(self):
        out = _apply(
            _placement(after_target_key=None, before_target_key=ANCHOR), _workflow_cfg()
        )
        keys = [run_addressing.workflow_target_run_key(e) for e in out["target_runs"]]
        self.assertEqual([INSERTED, ANCHOR, "env/core/baseline"], keys)

    def test_an_absent_anchor_is_skipped_not_failed(self):
        out = _apply(_placement(after_target_key="env/nowhere"), _workflow_cfg())
        keys = [run_addressing.workflow_target_run_key(e) for e in out["target_runs"]]
        self.assertNotIn(INSERTED, keys)


class InsertedEntryTest(unittest.TestCase):
    """

    the latent second defect: a bare key would bypass the gate."""

    def test_the_inserted_entry_carries_the_workflow_default_action(self):
        out = _apply(_placement(), _workflow_cfg())
        entry = next(e for e in out["target_runs"]
                     if run_addressing.workflow_target_run_key(e) == INSERTED)
        self.assertEqual({"id": INSERTED, "target": INSERTED, "action": "provision"}, entry)

    def test_a_variant_may_declare_its_own_action(self):
        out = _apply(_placement(action="destroy"), _workflow_cfg())
        signatures = [run_addressing.workflow_target_run_signature(e) for e in out["target_runs"]]
        self.assertIn((INSERTED, "destroy"), signatures)

    def test_every_inserted_entry_has_an_action(self):
        """

        no entry may reach the pipeline actionless — the invariant."""

        out = _apply(_placement(), _workflow_cfg())
        self.assertTrue(all(
            run_addressing.workflow_target_run_signature(e)[1] for e in out["target_runs"]
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
        signatures = [run_addressing.workflow_target_run_signature(e) for e in out["target_runs"]]
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
    """

    the helpers the placement reuses, over both entry shapes."""

    def test_signature_reads_both_shapes(self):
        self.assertEqual(("a", None), run_addressing.workflow_target_run_signature("a"))
        self.assertEqual(
            ("a", "provision"),
            run_addressing.workflow_target_run_signature({"id": "a", "target": "a", "action": "provision"}),
        )

    def test_key_is_the_target_not_the_id(self):
        """`id` is display identity; an anchor names a TARGET."""

        entry = {"id": "run-1", "target": "env/ops/dbs", "action": "provision"}
        self.assertEqual("env/ops/dbs", run_addressing.workflow_target_run_key(entry))
        self.assertEqual("run-1", run_addressing.get_workflow_target_run_id(entry))

    def test_an_invalid_entry_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "invalid workflow target_run entry"):
            run_addressing.workflow_target_run_signature(42)

    def test_an_anchor_declared_twice_is_a_contradiction(self):
        with self.assertRaisesRegex(RuntimeError, "cannot set both"):
            catalog_workflow.variant_anchor(
                {"before_target_key": "a", "after_target_key": "b"}, label="variant 'v'"
            )

    def test_an_anchor_must_be_declared(self):
        with self.assertRaisesRegex(RuntimeError, "must define"):
            catalog_workflow.variant_anchor({}, label="variant 'v'")


if __name__ == "__main__":
    unittest.main()
