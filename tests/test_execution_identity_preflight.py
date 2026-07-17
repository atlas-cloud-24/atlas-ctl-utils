import argparse
import logging
import logging.handlers
import tempfile
import unittest
import sys
from pathlib import Path
from unittest import mock

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
from utils import common
from utils.providers import aws as aws_adapter


class PreflightPolicyTests(unittest.TestCase):
    def test_force_skip_profile_permission_defaults_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                """ctl_profiles:
  strict: {ref_policy: commit_required}
  debug:
    ref_policy: commit_required
    allow_force_skip_execution_identity_preflight_check: true
"""
            )
            self.assertFalse(
                common.ctl_allows_force_skip_execution_identity_preflight_check(
                    root, "strict"
                )
            )
            self.assertTrue(
                common.ctl_allows_force_skip_execution_identity_preflight_check(
                    root, "debug"
                )
            )

    def test_check_only_and_force_skip_are_mutually_exclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                execution_param=[],
                ctl_state_local_root=tmp,
                provider_credential=None,
                execution_access_mode="standard",
                execution_identity_preflight_check_only=True,
                force_skip_execution_identity_preflight_check=True,
            )
            with self.assertRaisesRegex(RuntimeError, "mutually exclusive"):
                common.finalize_common_args(args)

    def test_provider_credential_requires_force_bypass(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                execution_param=[],
                ctl_state_local_root=tmp,
                provider_credential="substitute",
                execution_access_mode="standard",
                execution_identity_preflight_check_only=False,
                force_skip_execution_identity_preflight_check=False,
            )
            with self.assertRaisesRegex(RuntimeError, "cannot override"):
                common.finalize_common_args(args)

    def test_bypass_and_force_skip_are_incompatible(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                execution_param=[],
                ctl_state_local_root=tmp,
                provider_credential="substitute",
                execution_access_mode="force_bypass",
                execution_identity_preflight_check_only=False,
                force_skip_execution_identity_preflight_check=True,
            )
            with self.assertRaisesRegex(RuntimeError, "not applicable"):
                common.finalize_common_args(args)

    def test_force_skip_requires_profile_permission(self):
        workflow = {"stages": ["target"]}
        inventory = {"stage_targets": {"target": {}}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n  strict: {ref_policy: commit_required}\n"
            )
            with self.assertRaisesRegex(
                RuntimeError,
                "allow_force_skip_execution_identity_preflight_check",
            ):
                common.validate_execution_access(
                    root,
                    "strict",
                    workflow,
                    inventory,
                    execution_context={},
                    execution_access_mode="standard",
                    agreed_skip_ctl_state_backend_sync=False,
                    force_skip_ctl_state_backend_sync=False,
                    provider_credential=None,
                    force_skip_execution_identity_preflight_check=True,
                )


class PreflightArtifactTests(unittest.TestCase):
    def test_report_is_deterministic_and_target_scoped(self):
        class Adapter:
            def preflight_execution_identity(self, stage_id, stage, catalogs, **kwargs):
                del stage_id, catalogs, kwargs
                return {
                    "execution_identity_key": stage["execution_identity_key"],
                    "provider": "fixture",
                    "access_mode": "standard",
                    "status": "passed",
                    "provider_path": [
                        {
                            "display": f"principal: {stage['execution_identity_key']}",
                            "status": "passed",
                        }
                    ],
                }

        selection = {
            "selection_kind": "workflow",
            "selection_key": "example/workflow",
            "active_stages": {
                "one": {"target": "target/one", "execution_identity_key": "identity_one"},
                "two": {"target": "target/two", "execution_identity_key": "identity_two"},
            },
            "provider_adapter": Adapter(),
            "provider_catalogs": {},
            "execution_context": {},
        }
        report = common.build_execution_identity_preflight_report(
            selection,
            implementation_key="local",
            execution_access_mode="standard",
            provider_credential=None,
            force_skip=False,
        )
        self.assertEqual(report["status"], "passed")
        self.assertEqual(
            [result["target_key"] for result in report["results"]],
            ["target/one", "target/two"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            common.write_execution_identity_preflight_artifacts(artifacts, report)
            self.assertFalse(
                (artifacts / "execution_identity_preflight.yaml").exists()
            )
            rendered = (artifacts / "execution_identity_preflight.txt").read_text()
            self.assertIn("target: target/one [ passed \u2705 ]", rendered)
            self.assertIn("principal: identity_two [ passed \u2705 ]", rendered)

    def test_skipped_results_use_one_human_status_and_reason(self):
        report = {
            "selection": {"kind": "workflow", "key": "env/baseline"},
            "status": "passed",
            "results": [
                {
                    "target_key": "env/core/baseline",
                    "execution_identity_key": "env_dev_deploy",
                    "provider": "aws",
                    "access_mode": "force_bypass",
                    "status": "not_applicable",
                    "provider_path": [],
                    "reason": "execution identity was bypassed for this run",
                },
                {
                    "ctl_state_backend": "env",
                    "execution_identity_key": None,
                    "provider": None,
                    "access_mode": "agreed_direct",
                    "status": "not_applicable",
                    "provider_path": [],
                    "reason": "ctl-state sync force-skipped for this run",
                },
            ],
        }
        rendered = "\n".join(common._preflight_text_lines(report))
        self.assertIn("workflow: env/baseline [ passed ✅ ]", rendered)
        self.assertIn("target: env/core/baseline [ passed ✅ ]", rendered)
        self.assertIn(
            "execution_identity: env_dev_deploy [ skipped ⏭ ]", rendered
        )
        self.assertIn("ctl_state_backend: env [ skipped ⏭ ]", rendered)
        self.assertIn(
            "reason: execution identity was skipped for this run", rendered
        )
        self.assertIn(
            "reason: ctl-state sync force-skipped for this run", rendered
        )
        self.assertNotIn("not_applicable", rendered)
        self.assertNotIn("note:", rendered)

    def test_policy_failure_does_not_replace_identity_report(self):
        selection = {
            "selection_kind": "workflow",
            "selection_key": "landing_zone/bootstrap",
            "workflow_cfg": {"stages": ["target"]},
            "inventory_cfg": {"stage_targets": {"target": {}}},
            "execution_context": {},
            "active_stages": {},
        }
        with mock.patch.object(
            common,
            "validate_target_policy_constraints",
            side_effect=RuntimeError("commit-required policy"),
        ), mock.patch.object(
            common, "validate_execution_access"
        ), mock.patch.object(
            common, "validate_execution_runtime_mode"
        ), mock.patch.object(
            common, "validate_stages_have_commits"
        ):
            policy_report = common.build_ctl_policy_preflight_report(
                selection,
                ctl_cfg_root=Path("/unused"),
                ctl_profile="local_dev",
                ctl_ref_policy="local_dirty_allowed",
                execution_runtime_mode="local",
                execution_access_mode="standard",
                provider_credential=None,
                agreed_skip_ctl_state_backend_sync=False,
                force_skip_ctl_state_backend_sync=False,
                force_skip_execution_identity_preflight_check=False,
            )
        self.assertEqual(policy_report["status"], "failed")
        failed_checks = {
            check["name"]: check
            for check in policy_report["checks"]
            if check["status"] == "failed"
        }
        self.assertIn("target_policy_constraints", failed_checks)
        self.assertIn(
            "commit-required policy",
            failed_checks["target_policy_constraints"]["failure_reason"],
        )

    def test_fan_out_report_keeps_parameter_set_entries_distinct(self):
        workflow_report = {
            "selection": {"kind": "workflow", "key": "env/bootstrap"},
            "status": "passed",
            "results": [
                {
                    "target_key": "env/tfstate_backend",
                    "execution_identity_key": "env_deploy",
                    "provider": "aws",
                    "access_mode": "standard",
                    "status": "passed",
                    "provider_path": [
                        {
                            "display": "required_role: oxygen-dev-deploy",
                            "status": "passed",
                        }
                    ],
                }
            ],
        }
        dev = common.wrap_fan_out_preflight_child(
            workflow_report,
            {
                "fan_out_param_set_key": "non_prod_accounts",
                "fan_out_param_entry_key": "dev",
                "params": {"account": "dev", "env_type": "dev"},
            },
        )
        failed_workflow = {
            **workflow_report,
            "status": "failed",
            "results": [
                {
                    **workflow_report["results"][0],
                    "status": "failed",
                    "provider_path": [
                        {
                            "display": "required_role: oxygen-test-deploy",
                            "status": "failed",
                        }
                    ],
                }
            ],
        }
        test = common.wrap_fan_out_preflight_child(
            failed_workflow,
            {
                "fan_out_param_set_key": "non_prod_accounts",
                "fan_out_param_entry_key": "test",
                "params": {"account": "test", "env_type": "test"},
            },
        )
        report = {
            "selection": {"kind": "fan_out", "key": "env/bootstrap_non_prod"},
            "status": common.aggregate_execution_identity_preflight_status(
                [dev["status"], test["status"]]
            ),
            "children": [dev, test],
        }
        self.assertEqual(report["status"], "failed")
        with tempfile.TemporaryDirectory() as tmp:
            common.write_execution_identity_preflight_artifacts(Path(tmp), report)
            rendered = (
                Path(tmp) / "execution_identity_preflight.txt"
            ).read_text()
        self.assertIn(
            "fan_out_param_set: non_prod_accounts.dev [ passed \u2705 ]", rendered
        )
        self.assertIn(
            "fan_out_param_set: non_prod_accounts.test [ failed \u274c ]", rendered
        )
        self.assertEqual(rendered.count("workflow: env/bootstrap"), 2)
        self.assertIn("params: account=dev, env_type=dev", rendered)
        self.assertIn("required_role: oxygen-test-deploy [ failed \u274c ]", rendered)

    def test_unparameterized_fan_out_child_has_no_set_wrapper(self):
        report = {
            "selection": {
                "kind": "workflow",
                "key": "landing_zone/org_state/bootstrap",
            },
            "status": "passed",
            "results": [],
        }
        child = {
            "fan_out_param_set_key": None,
            "fan_out_param_entry_key": None,
            "params": {},
        }
        self.assertIs(common.wrap_fan_out_preflight_child(report, child), report)

    def test_unparameterized_fan_out_child_shows_effective_params(self):
        report = {
            "selection": {
                "kind": "workflow",
                "key": "landing_zone/org_state/bootstrap",
            },
            "status": "passed",
            "results": [],
        }
        child = {
            "fan_out_param_set_key": None,
            "fan_out_param_entry_key": None,
            "params": {},
        }
        wrapped = common.wrap_fan_out_preflight_child(
            report,
            child,
            effective_params={
                "landing_zone": "live",
                "main_tag": "oxygen",
                "region": "eu-west-2",
            },
        )
        rendered = "\n".join(common._preflight_text_lines(wrapped))
        self.assertIn(
            "workflow: landing_zone/org_state/bootstrap [ passed ✅ ]", rendered
        )
        self.assertIn(
            "params: landing_zone=live, main_tag=oxygen, region=eu-west-2",
            rendered,
        )
        self.assertNotIn("fan_out_param_set:", rendered)

    def test_fan_out_params_cannot_override_cli_execution_params(self):
        children = [
            {
                "label": "env/bootstrap[non_prod_accounts.dev]",
                "params": {"account": "dev", "env_type": "dev"},
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(
                RuntimeError,
                r"non_prod_accounts\.dev.*account.*--execution-params",
            ):
                common.validate_fan_out_param_collisions(
                    Path(tmp), children, {"account": "test"}
                )

    def test_fan_out_params_cannot_override_ctl_execution_params(self):
        children = [
            {
                "label": "env/bootstrap[region.eu_west_2]",
                "params": {"region": "eu-west-2"},
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "execution_params.yaml").write_text(
                "execution_params:\n  region: eu-west-1\n"
            )
            with self.assertRaisesRegex(
                RuntimeError, r"eu_west_2.*region.*ctl execution_params"
            ):
                common.validate_fan_out_param_collisions(root, children, {})

    def test_non_overlapping_fan_out_params_are_allowed(self):
        children = [
            {
                "label": "env/bootstrap[non_prod_accounts.dev]",
                "params": {"account": "dev", "env_type": "dev"},
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "execution_params.yaml").write_text(
                "execution_params:\n  main_tag: oxygen\n"
            )
            common.validate_fan_out_param_collisions(
                root, children, {"landing_zone": "live"}
            )

    def test_preflight_only_dirs_do_not_materialize_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = logging.handlers.MemoryHandler(capacity=10)
            run_dir, artifacts_dir, log_file = common.setup_preflight_run_dirs(
                "run-id",
                "plan",
                "target",
                "example",
                Path(tmp),
                memory,
                locator_segments=list(common.LOCAL_ONLY_LOCATOR),
            )
            self.assertTrue(artifacts_dir.is_dir())
            self.assertTrue(log_file.is_file())
            self.assertFalse((run_dir / "cfg").exists())
            self.assertFalse((run_dir / "stages_source").exists())
            self.assertFalse((run_dir / "stage_utils").exists())

    def test_command_log_redacts_provider_credential(self):
        rendered = common.redact_command_argv(
            ["runner.py", "--provider-credential", "sensitive-selector"]
        )
        self.assertEqual(
            rendered,
            ["runner.py", "--provider-credential", "<redacted>"],
        )
        self.assertNotIn("sensitive-selector", " ".join(rendered))

    def test_failure_reason_redacts_secret_values(self):
        error = RuntimeError("token=visible password:also-visible")
        rendered = common.credential_free_preflight_failure_reason(error)
        self.assertNotIn("visible", rendered)
        self.assertIn("<redacted>", rendered)


class AwsPreflightTests(unittest.TestCase):
    @staticmethod
    def catalogs():
        return {
            "execution_identities": {},
            "credential_sources": {},
            "account_registry": {},
            "ctl_role_chain": {"runner_role_key": "runner"},
            "stage_roles": {},
        }

    def test_direct_live_check_asserts_exact_caller(self):
        resolved = {
            "provider": "aws",
            "execution_identity_key": "dev",
            "account_key": "dev",
            "expected_account_id": "111111111111",
            "credential_source_key": "dev_entry",
            "credential_provider_kind": "direct_profile",
            "profile_name": "dev-profile",
            "permission_set_name": "DevDeployAccess",
        }
        caller = {
            "Account": "111111111111",
            "Arn": "arn:aws:sts::111111111111:assumed-role/AWSReservedSSO_DevDeployAccess_hash/session",
        }
        with mock.patch.object(
            aws_adapter, "resolve_stage_aws_access", return_value=resolved
        ), mock.patch.object(
            aws_adapter, "assert_profile_caller", return_value=caller
        ) as assertion:
            result = aws_adapter.preflight_execution_identity(
                "stage",
                {"execution_identity_key": "dev"},
                self.catalogs(),
                execution_context={"execution_context.params.main_tag": "oxygen"},
                implementation_key="local",
                execution_access_mode="agreed_direct",
            )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["provider_path"][-1]["observed_principal"], caller["Arn"])
        assertion.assert_called_once()

    def test_force_skip_keeps_static_path_and_makes_no_live_call(self):
        resolved = {
            "provider": "aws",
            "execution_identity_key": "prod",
            "account_key": "prod",
            "expected_account_id": "222222222222",
            "credential_source_key": "entry",
            "credential_provider_kind": "role_chain",
            "entry_profile_name": "entry-profile",
            "entry_account_id": "111111111111",
            "entry_permission_set_name": "CtlEntryAccess",
            "hop_role_arns": [
                "arn:aws:iam::111111111111:role/ctl-runner",
                "arn:aws:iam::222222222222:role/prod-deploy",
            ],
            "stage_role_key": "prod_deploy",
            "role_name": "prod-deploy",
        }
        with mock.patch.object(
            aws_adapter, "resolve_stage_aws_access", return_value=resolved
        ), mock.patch.object(aws_adapter, "assert_profile_caller") as assertion:
            result = aws_adapter.preflight_execution_identity(
                "stage",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={},
                implementation_key="local",
                live_check=False,
            )
        self.assertEqual(result["status"], "force_skipped")
        self.assertTrue(result["provider_path"])
        self.assertTrue(
            all(node["status"] == "force_skipped" for node in result["provider_path"])
        )
        self.assertNotIn("observed_principal", str(result))
        assertion.assert_not_called()

    def test_standard_checks_every_hop_and_final_caller(self):
        resolved = {
            "provider": "aws",
            "execution_identity_key": "prod",
            "account_key": "prod",
            "expected_account_id": "222222222222",
            "credential_source_key": "entry",
            "credential_provider_kind": "role_chain",
            "entry_profile_name": "entry-profile",
            "entry_account_id": "111111111111",
            "entry_role_name": "Entry",
            "hop_role_arns": [
                "arn:aws:iam::111111111111:role/ctl-runner",
                "arn:aws:iam::222222222222:role/prod-deploy",
            ],
            "stage_role_key": "prod_deploy",
            "role_name": "prod-deploy",
        }
        entry = {
            "Account": "111111111111",
            "Arn": "arn:aws:sts::111111111111:assumed-role/Entry/session",
        }
        final = {
            "Account": "222222222222",
            "Arn": "arn:aws:sts::222222222222:assumed-role/prod-deploy/session",
        }
        assume_results = [
            ({"AWS_ACCESS_KEY_ID": "one"}, {"Arn": "runner"}),
            ({"AWS_ACCESS_KEY_ID": "two"}, {"Arn": "final"}),
        ]
        with mock.patch.object(
            aws_adapter, "resolve_stage_aws_access", return_value=resolved
        ), mock.patch.object(
            aws_adapter, "assert_profile_caller", return_value=entry
        ), mock.patch.object(
            aws_adapter, "_assume_role_credentials", side_effect=assume_results
        ) as assume, mock.patch.object(
            aws_adapter, "_run_aws_json", return_value=final
        ):
            result = aws_adapter.preflight_execution_identity(
                "stage",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={"execution_context.params.main_tag": "oxygen"},
                implementation_key="local",
            )
        self.assertEqual(result["status"], "passed")
        self.assertEqual(assume.call_count, 2)
        self.assertEqual(result["provider_path"][-1]["observed_account"], "222222222222")

    def test_wrong_final_principal_fails(self):
        resolved = {
            "provider": "aws",
            "execution_identity_key": "prod",
            "account_key": "prod",
            "expected_account_id": "222222222222",
            "credential_source_key": "entry",
            "credential_provider_kind": "role_chain",
            "entry_profile_name": "entry-profile",
            "entry_account_id": "111111111111",
            "entry_role_name": "Entry",
            "hop_role_arns": [
                "arn:aws:iam::111111111111:role/ctl-runner",
                "arn:aws:iam::222222222222:role/prod-deploy",
            ],
            "stage_role_key": "prod_deploy",
            "role_name": "prod-deploy",
        }
        wrong_final = {
            "Account": "222222222222",
            "Arn": "arn:aws:sts::222222222222:assumed-role/wrong-role/session",
        }
        with mock.patch.object(
            aws_adapter, "resolve_stage_aws_access", return_value=resolved
        ), mock.patch.object(
            aws_adapter,
            "assert_profile_caller",
            return_value={
                "Account": "111111111111",
                "Arn": "arn:aws:sts::111111111111:assumed-role/Entry/session",
            },
        ), mock.patch.object(
            aws_adapter,
            "_assume_role_credentials",
            return_value=({"AWS_ACCESS_KEY_ID": "temporary"}, {}),
        ), mock.patch.object(
            aws_adapter, "_run_aws_json", return_value=wrong_final
        ):
            result = aws_adapter.preflight_execution_identity(
                "stage",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={},
                implementation_key="local",
            )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["provider_path"][-1]["status"], "failed")

    def test_static_error_still_fails_when_live_check_is_skipped(self):
        with mock.patch.object(
            aws_adapter,
            "resolve_stage_aws_access",
            side_effect=RuntimeError("missing stage role"),
        ):
            result = aws_adapter.preflight_execution_identity(
                "stage",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={},
                implementation_key="local",
                live_check=False,
            )
        self.assertEqual(result["status"], "failed")
        self.assertIn("missing stage role", result["failure_reason"])

    def test_bypass_result_redacts_substitute_credential(self):
        with mock.patch.object(
            aws_adapter,
            "resolve_stage_aws_access",
            return_value={"identity_bypass": "true"},
        ):
            result = aws_adapter.preflight_execution_identity(
                "stage",
                {"execution_identity_key": "env_dev_deploy"},
                self.catalogs(),
                execution_context={},
                implementation_key="local",
                execution_access_mode="force_bypass",
                provider_credential="do-not-render",
            )
        self.assertEqual(result["status"], "not_applicable")
        self.assertEqual(result["execution_identity_key"], "env_dev_deploy")
        self.assertNotIn("do-not-render", str(result))


if __name__ == "__main__":
    unittest.main()
