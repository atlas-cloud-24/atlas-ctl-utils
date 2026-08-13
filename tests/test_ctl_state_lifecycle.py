import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import ctl_cfg_fixture
from engine.catalog import workflow as catalog_workflow
from engine.commands import selection as commands_selection

CFG = {
    "backends.yaml": (
        "ctl_state_backends:\n"
        "  live:\n"
        "    selectors:\n"
        "      match:\n"
        "        execution_context.params.landing_zone: live\n"
        "    provider: aws\n"
        "    backend_type: s3\n"
        "    bucket_name: oxygen-live-ctl-state\n"
        "    bucket_region: eu-west-2\n"
        "    execution_identity:\n"
        "      account: ctl_plane\n"
        "      operations:\n"
        "        read:\n          role: reader\n"
        "        sync:\n          role: synchronizer\n"
        "        maintenance:\n          role: maintainer\n"
    ),
    "target_sources.yaml": (
        "target_sources:\n  bootstrap:\n    repo_url: https://example.invalid/bootstrap.git\n"
    ),
    "domains.yaml": ("domains:\n  env:\n    description: workload environment platform\n"),
    "workflow.yaml": (
        "workflows:\n"
        "  env/bootstrap:\n"
        "    workflow_instance_params: [account, env_type]\n"
        "    targets:\n"
        "      default_action: provision\n"
        "      keys:\n"
        "      - targets.env/tfstate_backend\n"
    ),
}
TARGETS = (
    "targets:\n"
    "  env/tfstate_backend:\n"
    "    actions: [provision]\n"
    "    committed_result_reuse:\n"
    "      provision: true\n"
    "    source_key: target_sources.bootstrap\n"
    "    ref_key: env/${execution_context.params.env.type}\n"
    "    procedure_key: tfstate_backend\n"
    "    domains: [domains.env]\n"
    "    input_params: [account, env_type]\n"
    "    cfg_keys:\n"
    "      env: [ctl_state_s3_bucket_name]\n"
    "    target_instance_params:\n"
    "      - account\n"
    "      - env_type\n"
)


def make_cfg(test, tmp: str) -> Path:
    root = Path(tmp)
    for name, body in CFG.items():
        (root / name).write_text(body, encoding="utf-8")
    (root / "targets" / "provision").mkdir(parents=True)
    (root / "targets" / "provision" / "t.yaml").write_text(TARGETS, encoding="utf-8")
    ctl_cfg_fixture.declare_providers(root, "aws")
    # resolving an identity activates this root inside the engine; `activate`
    # takes the test's cleanup so the activation cannot outlive the temp dir and
    # surface as a nonsense failure in an unrelated test.
    return ctl_cfg_fixture.activate(test, root)


PARAMS = {"landing_zone": "live", "account": "dev", "env_type": "dev"}


class LifecycleWiringTests(unittest.TestCase):
    """6b/6c — namespace locator, instance identity, run dirs, outdate."""

    def test_locator_is_namespace_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(self, tmp)
            loc = commands_selection.resolve_run_locator_segments(
                root,
                run_type="target",
                action="provision",
                ctl_profile=None,
                execution_params=PARAMS,
                execution_runtime_mode="local",
                target_name="env/tfstate_backend",
            )
            self.assertEqual(loc, ["live"])

    def test_fan_out_and_procedure_stay_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(self, tmp)
            for run_type in ("fan_out", "procedure"):
                loc = commands_selection.resolve_run_locator_segments(
                    root,
                    run_type=run_type,
                    action="provision",
                    ctl_profile=None,
                    execution_params=PARAMS,
                    execution_runtime_mode="local",
                )
                self.assertEqual(loc, ["_local"], run_type)

    def test_target_instance_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(self, tmp)
            ident = commands_selection.resolve_run_instance_identity(
                root,
                run_type="target",
                action="provision",
                ctl_profile=None,
                execution_params=PARAMS,
                execution_runtime_mode="local",
                target_name="env/tfstate_backend",
            )
            self.assertEqual(ident["instance_segments"], ["account=dev", "env_type=dev"])
            self.assertEqual(
                ident["address"], "env/tfstate_backend/instances/account=dev/env_type=dev"
            )

    def test_a_workflow_has_no_composition_digest(self):
        """A workflow publishes history, so it has no composition
        digest and no identity document — only the members it ran with.

        its history IS partitioned, by the axes its members vary over,
        so the address carries instance segments — but they are declared params,
        never a hash of the composition."""

        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(self, tmp)
            ident = commands_selection.resolve_run_instance_identity(
                root,
                run_type="workflow",
                action="provision",
                ctl_profile=None,
                execution_params=PARAMS,
                execution_runtime_mode="local",
                workflow_name="env/bootstrap",
            )
            self.assertIsNone(ident["identity_doc"])
            self.assertEqual(["account=dev", "env_type=dev"], ident["instance_segments"])
            self.assertEqual("env/bootstrap/instances/account=dev/env_type=dev", ident["address"])
            self.assertEqual(
                ident["target_addresses"],
                ["env/tfstate_backend/instances/account=dev/env_type=dev"],
            )

    def test_a_workflow_must_declare_exactly_its_members_axes(self):
        """Under-declaring merges two instances' histories; over-declaring makes

        an address that can never differ.
        """

        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(self, tmp)
            workflows = root / "workflow.yaml"
            body = workflows.read_text()

            workflows.write_text(
                body.replace("    workflow_instance_params: [account, env_type]\n", "")
            )
            with self.assertRaisesRegex(RuntimeError, "missing"):
                commands_selection.resolve_run_instance_identity(
                    root,
                    run_type="workflow",
                    action="provision",
                    ctl_profile=None,
                    execution_params=PARAMS,
                    execution_runtime_mode="local",
                    workflow_name="env/bootstrap",
                )

            workflows.write_text(
                body.replace("[account, env_type]", "[account, env_type, landing_zone]")
            )
            with self.assertRaisesRegex(RuntimeError, "declares"):
                commands_selection.resolve_run_instance_identity(
                    root,
                    run_type="workflow",
                    action="provision",
                    ctl_profile=None,
                    execution_params=PARAMS,
                    execution_runtime_mode="local",
                    workflow_name="env/bootstrap",
                )

    def test_an_incomplete_union_does_not_flag_a_spare_axis(self):
        """A member absent from THIS action's action contributes unknown axes.

        Found by sweeping real cfg: under `destroy`, workflows whose targets
        register no destroy resolved an EMPTY union, so every declared param
        looked spare and five workflows failed the guard. Under-declaration is
        still checked — a missing axis is real whatever the action holds.
        """

        with tempfile.TemporaryDirectory() as tmp:
            make_cfg(self, tmp)
            # the target declares provision only, so a destroy action omits it
            union, complete = catalog_workflow.WorkflowInstanceParams.member_params(
                {"target_runs": ["env/absent_here"]}, {}, label="workflow 'w'"
            )
            self.assertEqual([], union)
            self.assertFalse(complete)
            # over-declaration is NOT raised while the union is incomplete
            self.assertEqual(
                ["account"],
                catalog_workflow.WorkflowInstanceParams.validate(
                    ["account"],
                    {"target_runs": ["env/absent_here"]},
                    {},
                    label="workflow 'w'",
                ),
            )

    def test_under_declaration_is_flagged_even_when_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            make_cfg(self, tmp)
            targets = {"a": {"target_instance_params": ["account"]}}
            with self.assertRaisesRegex(RuntimeError, "missing"):
                catalog_workflow.WorkflowInstanceParams.validate(
                    [], {"target_runs": ["a", "gone"]}, targets, label="workflow 'w'"
                )


if __name__ == "__main__":
    unittest.main()


class StaticWorkflowParamsGateTests(unittest.TestCase):
    """The whole-cfg pass: every workflow, every branch, no execution context.

    The per-run guard is exact but only ever sees the workflow being run, so a
    misdeclaration elsewhere stayed silent until someone ran it. This one belongs
    to the SKIPPABLE gate — a workflow absent from a run cannot corrupt it —
    while the per-run guard still fires unskippably for the one that is.
    """

    TARGETS = {
        "wide": {"target_instance_params": ["account", "env_type"]},
        "narrow": {"target_instance_params": ["account"]},
        "dispatching": {
            "target_instance_params": {
                "members": [
                    {
                        "params": ["domain", "env_type"],
                        "selectors": {"match": {"execution_context.params.domain": "env"}},
                    },
                    {
                        "params": ["domain"],
                        "selectors": {"match": {"execution_context.params.domain": "org"}},
                    },
                ]
            }
        },
    }

    def _check(self, workflows):
        catalog_workflow.WorkflowInstanceParams.validate_all(workflows, self.TARGETS)

    def test_a_correct_declaration_passes(self):
        self._check(
            {
                "w": {
                    "workflow_instance_params": ["account", "env_type"],
                    "targets": {"keys": ["wide", "narrow"]},
                }
            }
        )

    def test_a_missing_axis_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "missing"):
            self._check(
                {"w": {"workflow_instance_params": ["account"], "targets": {"keys": ["wide"]}}}
            )

    def test_a_spare_axis_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "declares"):
            self._check(
                {
                    "w": {
                        "workflow_instance_params": ["account", "env_type"],
                        "targets": {"keys": ["narrow"]},
                    }
                }
            )

    def test_a_branch_is_checked_against_the_target_branch_it_can_hold_with(self):
        """A members-shaped target contributes only the branches whose selectors
        could be satisfied together with the workflow branch's."""
        self._check(
            {
                "w": {
                    "workflow_instance_params": {
                        "members": [
                            {
                                "params": ["domain", "env_type"],
                                "selectors": {"match": {"execution_context.params.domain": "env"}},
                            },
                            {
                                "params": ["domain"],
                                "selectors": {"match": {"execution_context.params.domain": "org"}},
                            },
                        ]
                    },
                    "targets": {"keys": ["dispatching"]},
                }
            }
        )

    def test_an_unpinned_dispatch_axis_is_refused_rather_than_guessed(self):
        """One workflow branch matching two different target branches has no

        single correct declaration.
        """

        with self.assertRaisesRegex(RuntimeError, "does not pin"):
            self._check(
                {
                    "w": {
                        "workflow_instance_params": ["domain"],
                        "targets": {"keys": ["dispatching"]},
                    }
                }
            )

    def test_a_target_without_selectors_applies_to_every_branch(self):
        """A plain list means 'always', so it counts under any condition."""

        with self.assertRaisesRegex(RuntimeError, "missing"):
            self._check(
                {
                    "w": {
                        "workflow_instance_params": {
                            "members": [
                                {
                                    "params": ["account"],
                                    "selectors": {
                                        "match": {"execution_context.params.domain": "env"}
                                    },
                                },
                            ]
                        },
                        "targets": {"keys": ["wide"]},
                    }
                }
            )

    def test_imported_workflows_contribute_their_members(self):
        with self.assertRaisesRegex(RuntimeError, "missing"):
            self._check(
                {
                    "base": {"targets": {"keys": ["wide"]}},
                    "w": {
                        "workflow_instance_params": ["account"],
                        "import_workflows": ["base"],
                        "targets": {"keys": []},
                    },
                }
            )
