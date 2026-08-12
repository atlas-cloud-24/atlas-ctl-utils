"""`default_action` lives INSIDE `targets`, in both the plain and branched forms.

It belongs to the list it governs — an action with no targets is nothing — so a
plain declaration carries it beside `keys`, and a branched one carries it inside
each member. At workflow level it would sit beside `workflow_instance_params`, a
genuinely workflow-scoped field, and read as a property of the workflow.

`workflow_target_branches` is the single place that reads the declared shape, so
every caller inherits the same refusals rather than each remembering to check.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.catalog import workflow as catalog_workflow  # noqa: E402

PLAIN = {"targets": {"default_action": "provision", "keys": ["env/core/baseline"]}}
BRANCHED = {
    "targets": {
        "members": [
            {
                "default_action": "provision",
                "keys": ["env/static/r53_public", "env/static/acm"],
                "selectors": {"in": {"execution_context.params.operation": ["plan", "provision"]}},
            },
            {
                "default_action": "destroy",
                "keys": ["env/static/acm", "env/static/r53_public"],
                "selectors": {"match": {"execution_context.params.operation": "destroy"}},
            },
        ]
    }
}


class WorkflowTargetBranchesTest(unittest.TestCase):
    def test_a_plain_declaration_is_one_branch(self):
        self.assertEqual(
            [(["env/core/baseline"], "provision")],
            catalog_workflow.WorkflowCatalog.target_branches(PLAIN, name="w"),
        )

    def test_a_branched_declaration_keeps_each_pair_together(self):
        """Each member's action belongs to that member's list, which is the whole
        point of moving the field inside."""

        self.assertEqual(
            [
                (["env/static/r53_public", "env/static/acm"], "provision"),
                (["env/static/acm", "env/static/r53_public"], "destroy"),
            ],
            catalog_workflow.WorkflowCatalog.target_branches(BRANCHED, name="w"),
        )

    def test_a_workflow_declaring_no_targets_has_no_branches(self):
        """Not an error: a workflow that only imports declares no list of its own."""

        self.assertEqual([], catalog_workflow.WorkflowCatalog.target_branches({}, name="w"))

    def test_default_action_at_workflow_level_is_refused(self):
        """Accepting it there would lose the value the moment the list branched,
        since the member's own would win."""

        with self.assertRaisesRegex(RuntimeError, "at workflow level"):
            catalog_workflow.WorkflowCatalog.target_branches(
                {"default_action": "provision", "targets": {"keys": ["a/b"]}}, name="w"
            )

    def test_a_plain_list_under_targets_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "must be a mapping"):
            catalog_workflow.WorkflowCatalog.target_branches({"targets": ["a/b"]}, name="w")


class TheRefusalsReachEveryCallerTest(unittest.TestCase):
    """The guard is worth nothing if only one entry point consults it."""

    def test_action_validation_reads_the_new_shape(self):
        catalog_workflow.WorkflowCatalog.validate_actions_declared({"w": PLAIN})
        with self.assertRaisesRegex(RuntimeError, "no action"):
            catalog_workflow.WorkflowCatalog.validate_actions_declared(
                {"w": {"targets": {"keys": ["a/b"]}}}
            )

    def test_action_validation_refuses_a_workflow_level_default_action(self):
        with self.assertRaisesRegex(RuntimeError, "at workflow level"):
            catalog_workflow.WorkflowCatalog.validate_actions_declared(
                {"w": {"default_action": "provision", "targets": {"keys": ["a/b"]}}}
            )

    def test_action_validation_refuses_maintenance_in_every_branch(self):
        for targets in (
            {"default_action": "maintenance", "keys": ["a/b"]},
            {"default_action": "provision", "keys": [{"key": "a/b", "action": "maintenance"}]},
        ):
            with (
                self.subTest(targets=targets),
                self.assertRaisesRegex(RuntimeError, "maintenance runner"),
            ):
                catalog_workflow.WorkflowCatalog.validate_actions_declared(
                    {"w": {"targets": targets}}
                )

    def test_action_validation_allows_a_default_action_reference(self):
        catalog_workflow.WorkflowCatalog.validate_actions_declared(
            {
                "w": {
                    "targets": {
                        "default_action": "${execution_context.params.operation}",
                        "keys": ["a/b"],
                    }
                }
            }
        )

    def test_the_static_key_walk_reads_both_forms(self):
        self.assertEqual(
            ["env/core/baseline"],
            catalog_workflow.WorkflowCatalog.target_key_entries(
                {**PLAIN, "__name__": "w"}, {}, label="w"
            ),
        )
        self.assertEqual(
            ["env/static/r53_public", "env/static/acm"],
            catalog_workflow.WorkflowCatalog.target_key_entries(
                {**BRANCHED, "__name__": "w"}, {}, label="w"
            ),
        )

    def test_the_static_key_walk_follows_imports(self):
        workflows = {"base": PLAIN}
        self.assertEqual(
            ["env/core/baseline", "env/ops/app"],
            catalog_workflow.WorkflowCatalog.target_key_entries(
                {
                    "__name__": "w",
                    "import_workflows": ["base"],
                    "targets": {"default_action": "provision", "keys": ["env/ops/app"]},
                },
                workflows,
                label="w",
            ),
        )


class ResolutionCollapsesToOneBranchTest(unittest.TestCase):
    """Cfg declares branches; a resolved run has exactly one.

    `expand_workflow_imports` therefore reads a FLAT `targets` list with
    `default_action` beside it — the runtime shape, not the declared one.
    """

    def test_import_expansion_reads_the_resolved_flat_shape(self):
        runs = catalog_workflow.WorkflowImports.expand(
            {
                "base": {"default_action": "provision", "targets": ["env/core/baseline"]},
                "w": {
                    "default_action": "provision",
                    "import_workflows": ["base"],
                    "targets": [{"key": "env/ops/app", "action": "destroy"}],
                },
            },
            "w",
        )
        self.assertEqual(
            [("env/core/baseline", "provision"), ("env/ops/app", "destroy")],
            [(r["target"], r["action"]) for r in runs],
        )


if __name__ == "__main__":
    unittest.main()
