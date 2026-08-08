"""Two guards that were reading cfg in a shape real cfg never has.

Both defects came from the same place — the difference between a workflow as
DECLARED and a workflow as LOADED — and both were invisible because the tests
around them hand-built the loaded shape and got it wrong the same way the code
did. So every test here goes through `load_workflow_catalog` over cfg written on
disk the way real cfg is written, rather than through a literal.

Phase 94 — a member key is declared as `targets.<key>` and the loader resolves it
to `<key>`. The static instance-param gate was handed the RAW collection, so
every member looked up as missing: the union of member params was always empty,
the `missing` half could never fire, and the `extra` half fired on any
declaration. The dead half is the dangerous one — it exists to stop two target
instances sharing one workflow address and merging their histories.

Phase 95 — `load_workflow_cfg` resolved every workflow in the catalog against the
running context, so a workflow declaring no branch for the current operation
raised even when the run never touched it. A workflow with no `destroy` branch is
not malformed; member selectors are exactly how it says which operations it
applies to.
"""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cfg import resources as cfg_resources
from engine.catalog import workflow as catalog_workflow

# Written the way real cfg writes it: member keys are QUALIFIED references, and
# `default_action` lives inside the list it governs.
CFG = """
targets:
  env/wide:
    target_instance_params: [env.type, aws.account]
  env/narrow:
    target_instance_params: [aws.account]

workflows:
  env/correct:
    operation: provision
    workflow_instance_params: [env.type, aws.account]
    targets:
      default_action: provision
      keys:
      - targets.env/wide
      - targets.env/narrow
  env/missing_an_axis:
    operation: provision
    workflow_instance_params: [aws.account]
    targets:
      default_action: provision
      keys:
      - targets.env/wide
  env/destroyable:
    operation:
      members:
      - value: provision
        selectors:
          match:
            execution_context.params.operation: provision
      - value: destroy
        selectors:
          match:
            execution_context.params.operation: destroy
    targets:
      members:
      - default_action: provision
        keys: [targets.env/narrow]
        selectors:
          match:
            execution_context.params.operation: provision
      - default_action: destroy
        keys: [targets.env/narrow]
        selectors:
          match:
            execution_context.params.operation: destroy
  env/provision_only:
    operation: provision
    targets:
      members:
      - default_action: provision
        keys: [targets.env/narrow]
        selectors:
          match:
            execution_context.params.operation: provision
"""


def _cfg_root(stack: unittest.TestCase) -> Path:
    tmp = tempfile.TemporaryDirectory(prefix="atlas-workflow-cfg-")
    stack.addCleanup(tmp.cleanup)
    root = Path(tmp.name)
    (root / "cfg.yaml").write_text(CFG, encoding="utf-8")
    return root


class MemberKeysResolveBeforeTheyAreLookedUpTest(unittest.TestCase):
    """Phase 94."""

    def setUp(self):
        self.root = _cfg_root(self)
        self.workflows = catalog_workflow.load_workflow_catalog(self.root)
        self.targets = cfg_resources.collect_resource(self.root, "targets")

    def test_cfg_declares_qualified_keys(self):
        """Guards the fixture: written bare, every test below passes vacuously —
        which is exactly how the defect survived its own test suite."""

        self.assertIn("targets.env/wide", CFG)

    def test_the_loader_resolves_them_to_the_key_targets_are_indexed_by(self):
        keys = catalog_workflow.workflow_target_key_entries(
            {**self.workflows["env/correct"], "__name__": "env/correct"},
            self.workflows, label="env/correct",
        )
        self.assertEqual(keys, ["env/wide", "env/narrow"])
        for key in keys:
            self.assertIn(key, self.targets)

    def test_a_correct_declaration_passes(self):
        catalog_workflow.validate_all_workflow_instance_params(
            {"env/correct": self.workflows["env/correct"]}, self.targets
        )

    def test_a_missing_axis_is_refused(self):
        """THE dead half. Its members instance over `env.type` and the workflow
        does not declare it, so two target instances would share one workflow
        address and merge their histories. Before Phase 94 this passed."""

        with self.assertRaisesRegex(RuntimeError, "missing"):
            catalog_workflow.validate_all_workflow_instance_params(
                {"env/missing_an_axis": self.workflows["env/missing_an_axis"]},
                self.targets,
            )

    def test_raw_cfg_diagnoses_it_backwards(self):
        """Pins the defect itself, so the fix cannot be undone quietly.

        Handed the UNRESOLVED collection the union is empty, so this declaration
        — which really IS missing an axis — is reported as declaring one too
        many. Both halves wrong at once, and the error still names a real
        workflow, which is why it read as a cfg problem rather than an engine
        one.
        """

        raw = cfg_resources.collect_resource(self.root, "workflows", entry_depth=1)
        with self.assertRaisesRegex(RuntimeError, "declares"):
            catalog_workflow.validate_all_workflow_instance_params(
                {"env/missing_an_axis": raw["env/missing_an_axis"]}, self.targets
            )
        # A CORRECT declaration is refused too, which is why validate_cfg.py
        # could not complete for either consumer family.
        with self.assertRaisesRegex(RuntimeError, "declares"):
            catalog_workflow.validate_all_workflow_instance_params(
                {"env/correct": raw["env/correct"]}, self.targets
            )


class OnlyTheSelectedWorkflowMustApplyToTheOperationTest(unittest.TestCase):
    """Phase 95."""

    def setUp(self):
        self.root = _cfg_root(self)
        self.workflows = catalog_workflow.load_workflow_catalog(self.root)

    def _load(self, name, operation):
        return catalog_workflow.load_workflow_cfg(
            self.root, "local_dev", None, name,
            {"execution_context.params.operation": operation},
        )

    def test_the_closure_is_the_workflow_and_what_it_imports(self):
        workflows = {"a": {"import_workflows": ["b"]}, "b": {"import_workflows": ["c"]},
                     "c": {}, "unrelated": {}}
        self.assertEqual(
            catalog_workflow.workflow_import_closure(workflows, "a"), {"a", "b", "c"}
        )
        self.assertEqual(
            catalog_workflow.workflow_import_closure(workflows, "unrelated"), {"unrelated"}
        )

    def test_a_run_is_not_blocked_by_a_workflow_it_never_touches(self):
        """The reported failure: `--execution-params operation=destroy` on one workflow died on
        another that declares only provision."""

        cfg = self._load("env/destroyable", "destroy")
        self.assertEqual(cfg["operation"], "destroy")
        self.assertEqual([entry["target"] for entry in cfg["target_runs"]], ["env/narrow"])

    def test_the_selected_workflow_must_still_apply(self):
        """`None` is only "does not apply" for a bystander. For the workflow that
        was ASKED for, the request cannot be served and must be refused."""

        with self.assertRaisesRegex(RuntimeError, "did not resolve"):
            self._load("env/provision_only", "destroy")

    def test_a_malformed_bystander_is_still_refused(self):
        """The structural checks stay catalog-wide: they do not depend on the
        operation, so a broken declaration is broken whatever is running."""

        (self.root / "broken.yaml").write_text(
            "workflows:\n  env/broken:\n    default_action: provision\n"
            "    targets:\n      keys: [targets.env/narrow]\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(RuntimeError, "default_action"):
            self._load("env/destroyable", "destroy")


if __name__ == "__main__":
    unittest.main()
