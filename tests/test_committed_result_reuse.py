"""Committed-result reuse is target policy, resolved for one concrete action.

The invocation may request reuse, but it cannot grant it. A target must classify
all of its actions explicitly, and the active run carries only the boolean for
the action that member will execute.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.catalog import target_catalog
from engine.catalog import workflow as catalog_workflow
from engine.commands import pipeline as commands_pipeline
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import request as run_request
from engine.run import selection as run_selection
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine_surface import patch_engine


class TargetReusePolicySchemaTest(unittest.TestCase):
    def test_synthetic_procedure_explicitly_disallows_reuse(self):
        with (
            mock.patch.object(
                catalog_workflow.cfg_resources,
                "collect_resource",
                return_value={"source": {"repo_path": "/tmp/source"}},
            ),
            mock.patch.object(
                catalog_workflow.execution_run_context,
                "load_domain_registry",
                return_value={},
            ),
            mock.patch.object(catalog_workflow.execution_run_context, "validate_domain_value"),
            mock.patch.object(
                target_catalog.TargetCatalog, "consent_opt_in_fields", return_value=set()
            ),
        ):
            workflow_cfg, action_cfg = catalog_workflow.WorkflowCatalog.procedure_cfg(
                Path("/cfg"),
                "plan",
                source="source",
                ref="context",
                domain_name="env",
                procedure="steps",
            )
            active = target_catalog.ActiveTargetRuns.build(
                workflow_cfg,
                action_cfg,
                repo_key="repo_path",
                require_branch_or_commit=False,
            )["procedure"]

        self.assertEqual(
            {"plan": False},
            action_cfg["targets"]["procedure"]["committed_result_reuse"],
        )
        self.assertIs(False, active["reuse_committed_result"])

    def test_policy_must_match_actions_exactly(self):
        entry = {
            "actions": ["plan", "provision"],
            "committed_result_reuse": {"plan": False, "provision": True},
        }
        self.assertEqual(
            {"plan": False, "provision": True},
            target_catalog.TargetActionPolicy(entry, label="target 't'").committed_result_reuse(),
        )

        for policy, reason in (
            (None, "must declare 'committed_result_reuse'"),
            ({"plan": True}, "missing actions"),
            ({"plan": True, "provision": True, "destroy": True}, "undeclared actions"),
            ({"plan": True, "provision": "yes"}, "must be booleans"),
        ):
            with self.subTest(policy=policy):
                candidate = {"actions": ["plan", "provision"]}
                if policy is not None:
                    candidate["committed_result_reuse"] = policy
                with self.assertRaisesRegex(RuntimeError, reason):
                    target_catalog.TargetActionPolicy(
                        candidate, label="target 't'"
                    ).committed_result_reuse()

    def test_policy_preserves_declared_mapping_order(self):
        policy = target_catalog.TargetActionPolicy(
            {
                "actions": ["plan", "provision"],
                "committed_result_reuse": {"provision": True, "plan": False},
            },
            label="target 't'",
        ).committed_result_reuse()

        self.assertEqual(["provision", "plan"], list(policy))

    def test_active_member_carries_only_its_actions_resolved_policy(self):
        action_cfg = {
            "target_sources": {"source": {"repo_path": "/tmp/source"}},
            "targets": {
                "target": {
                    "source": "source",
                    "ref": "context",
                    "procedure": "steps",
                    "domains": ["env"],
                    "cfg_keys": {"env": ["*"]},
                    "allowed_actions": ["plan", "provision"],
                    "committed_result_reuse": {"plan": False, "provision": True},
                }
            },
        }
        with mock.patch.object(
            target_catalog.TargetCatalog, "consent_opt_in_fields", return_value=set()
        ):
            inherited = target_catalog.ActiveTargetRuns.build(
                {
                    "meta": {"action": "provision"},
                    "target_runs": [{"id": "run", "target": "target"}],
                },
                action_cfg,
                repo_key="repo_path",
                require_branch_or_commit=False,
            )["run"]
            overridden = target_catalog.ActiveTargetRuns.build(
                {
                    "meta": {"action": "provision"},
                    "target_runs": [{"id": "run", "target": "target", "action": "plan"}],
                },
                action_cfg,
                repo_key="repo_path",
                require_branch_or_commit=False,
            )["run"]
            overridden_definition = target_catalog.ActiveTargetRuns.definition_document(overridden)

        self.assertIs(True, inherited["reuse_committed_result"])
        self.assertIs(False, overridden["reuse_committed_result"])
        self.assertIs(False, overridden_definition["reuse_committed_result"])

    def test_status_reuse_predicate_is_defensively_closed(self):
        with mock.patch.object(
            state_status.state_run_store, "target_instance_dir_for_run"
        ) as resolve_instance:
            revision = state_status.up_to_date_child_revision(
                Path("/unused"),
                {"reuse_committed_result": False},
                {},
                "provision",
            )
        self.assertIsNone(revision)
        resolve_instance.assert_not_called()


class WorkflowReuseGateTest(unittest.TestCase):
    def _run(self, *, reuse_committed_result: bool):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            run_dir.mkdir()
            kernel_yaml_io.write_yaml_file(
                state_run_store.run_metadata_path(run_dir),
                {"run_id": "w1", "run_type": "workflow", "action": "plan"},
            )
            cwd = Path.cwd()
            try:
                with patch_engine(
                    {"WorkflowChildren.build_command": mock.DEFAULT},
                    build_tooling_env=mock.DEFAULT,
                    materialize_step_utils=mock.DEFAULT,
                    mint_child_lock_grant=mock.DEFAULT,
                    latest_child_revision=mock.DEFAULT,
                    up_to_date_child_revision=mock.DEFAULT,
                    run_and_log=mock.DEFAULT,
                ) as patched:
                    patched["build_tooling_env"].return_value = {}
                    patched["materialize_step_utils"].return_value = run_dir
                    patched["WorkflowChildren.build_command"].return_value = ["ctl.py"]
                    patched["mint_child_lock_grant"].return_value = "grant"
                    patched["latest_child_revision"].return_value = None
                    patched["up_to_date_child_revision"].side_effect = (
                        lambda _run_dir, target_run, _context, _action: (
                            {"address": "target/instances/x", "run_id": "old"}
                            if target_run.get("reuse_committed_result")
                            else None
                        )
                    )
                    commands_pipeline.run_targets(
                        run_request.RunRequest(
                            ctl_cfg_root=Path(tmp),
                            ctl_profile="local_dev",
                            action="plan",
                            run_id="w1",
                            run_dir=run_dir,
                            credential_acquisition="provider",
                            execution_runtime_mode="local",
                            skip_up_to_date=True,
                        ),
                        run_selection.RunSelection(
                            kind="workflow", key="w", execution_context={}, provider_catalogs={}
                        ),
                        active_target_runs={
                            "member": {
                                "target": "target",
                                "reuse_committed_result": reuse_committed_result,
                            }
                        },
                        plt_targets_dir_path=Path(tmp),
                        execution_context_path=Path(tmp) / "context.yaml",
                        tooling_refs={},
                        credential_refresh_modes=None,
                        child_command_spec={"ctl_state_local_root": tmp},
                        secret_store=None,
                        plt_provider_dispatch=None,
                    )
                    return {
                        "reuse_checks": patched["up_to_date_child_revision"].call_count,
                        "child_runs": patched["run_and_log"].call_count,
                    }
            finally:
                os.chdir(cwd)

    def test_false_policy_executes_even_when_reuse_was_requested(self):
        self.assertEqual(
            {"reuse_checks": 1, "child_runs": 1},
            self._run(reuse_committed_result=False),
        )

    def test_true_policy_reuses_when_evidence_is_eligible(self):
        self.assertEqual(
            {"reuse_checks": 1, "child_runs": 0},
            self._run(reuse_committed_result=True),
        )


if __name__ == "__main__":
    unittest.main()
