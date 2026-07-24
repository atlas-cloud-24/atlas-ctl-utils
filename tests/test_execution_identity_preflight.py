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


class PreflightRollupTests(unittest.TestCase):
    """Container status ladder: failed > (partial | not_evaluated) > passed —
    a container is NEVER a false green, and mixed passed/blocked is `partial`,
    not `not_evaluated` (which is reserved for fully-blocked containers)."""

    def test_status_ladder(self):
        f = common.aggregate_execution_identity_preflight_status
        self.assertEqual(f(["passed", "passed"]), "passed")
        self.assertEqual(f([]), "passed")
        self.assertEqual(f(["force_skipped", "passed"]), "passed")
        self.assertEqual(f(["passed", "failed"]), "failed")
        self.assertEqual(f(["failed", "not_evaluated"]), "failed")
        self.assertEqual(f(["not_evaluated", "not_evaluated"]), "not_evaluated")
        self.assertEqual(f(["passed", "not_evaluated"]), "partial")
        self.assertEqual(f(["passed", "force_skipped", "not_evaluated"]), "partial")
        # a block + only NEUTRAL non-checks (skipped backend row) is fully
        # not_evaluated — NOT partial (no genuine pass to make it mixed)
        self.assertEqual(f(["not_evaluated", "skipped"]), "not_evaluated")
        self.assertEqual(f(["not_evaluated", "force_skipped"]), "not_evaluated")
        self.assertEqual(f(["passed", "skipped"]), "passed")

    def test_partial_tag_renders(self):
        self.assertEqual(common._preflight_status_tag("partial"), "[ partial ⚠️ ]")


class PreflightPolicyTests(unittest.TestCase):
    def test_force_skip_profile_permission_defaults_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                """ctl_profiles:
  strict: {ref_policy: commit_required}
  debug:
    ref_policy: commit_required
    allow_force_skip_full_cfg_validation_gate: true
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

            self.assertFalse(
                common.ctl_allows_force_skip_full_cfg_validation_gate(
                    root, "strict"
                )
            )
            self.assertTrue(
                common.ctl_allows_force_skip_full_cfg_validation_gate(
                    root, "debug"
                )
            )

    def test_full_cfg_validation_gate_flag_is_on_every_execution_runner(self):
        for run_type in (
            "target", "workflow", "fan_out", "step_sequence", "maintenance"
        ):
            with self.subTest(run_type=run_type):
                parser = argparse.ArgumentParser()
                common.add_common_args(parser, run_type=run_type)
                action = next(
                    item
                    for item in parser._actions
                    if "--force-skip-full-cfg-validation-gate"
                    in item.option_strings
                )
                self.assertEqual(
                    action.dest, "force_skip_full_cfg_validation_gate"
                )

    def test_check_only_and_force_skip_are_mutually_exclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                execution_param=[],
                ctl_state_local_root=tmp,
                providers=["aws"],
                provider_options={"aws.credential_implementation": "profile"},
                execution_access_modes={"aws": "standard"},
                execution_identity_preflight_check_only=True,
                force_skip_execution_identity_preflight_check=["aws"],
            )
            with self.assertRaisesRegex(RuntimeError, "mutually exclusive"):
                common.finalize_common_args(args)

    def test_provider_options_must_match_the_mode_they_imply(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                execution_param=[],
                ctl_state_local_root=tmp,
                providers=["aws"],
                provider_options={
                    "aws.credential_implementation": "profile",
                    "aws.force_bypass_credential_profile": "substitute",
                },
                execution_access_modes={"aws": "standard"},
                execution_identity_preflight_check_only=False,
                force_skip_execution_identity_preflight_check=[],
            )
            with self.assertRaisesRegex(RuntimeError, "only valid in execution access mode"):
                common.finalize_common_args(args)

    def test_bypass_and_force_skip_are_incompatible(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                execution_param=[],
                ctl_state_local_root=tmp,
                providers=["aws"],
                provider_options={
                    "aws.credential_implementation": "profile",
                    "aws.force_bypass_credential_profile": "substitute",
                },
                execution_access_modes={"aws": "force_bypass"},
                execution_identity_preflight_check_only=False,
                force_skip_execution_identity_preflight_check=["aws"],
            )
            with self.assertRaisesRegex(RuntimeError, "without resolving an execution identity"):
                common.finalize_common_args(args)

    def test_force_skip_requires_profile_permission(self):
        workflow = {"target_runs": ["target"]}
        inventory = {"targets": {"target": {}}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n  strict:\n    ref_policy: commit_required\n"
                "    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [standard]\n"
                "      allowed_credential_implementation: [profile]\n"
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
                    execution_access_modes={"aws": "standard"},
                    agreed_defer_ctl_state_backend_sync=False,
                    force_skip_ctl_state_backend_sync=False,
                    provider_options={},
                    force_skip_execution_identity_preflight_check=["aws"],
                )


    def test_full_cfg_validation_gate_skip_requires_profile_permission(self):
        workflow = {"target_runs": ["target"]}
        inventory = {"targets": {"target": {}}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n  strict:\n    ref_policy: commit_required\n"
                "    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [standard]\n"
                "      allowed_credential_implementation: [profile]\n"
            )
            with self.assertRaisesRegex(
                RuntimeError,
                "allow_force_skip_full_cfg_validation_gate",
            ):
                common.validate_execution_access(
                    root,
                    "strict",
                    workflow,
                    inventory,
                    execution_context={
                        "execution_context.ctl.force_skip_full_cfg_validation_gate": True
                    },
                    execution_access_modes={"aws": "standard"},
                    agreed_defer_ctl_state_backend_sync=False,
                    force_skip_ctl_state_backend_sync=False,
                    provider_options={},
                )


class FullCfgValidationGateTests(unittest.TestCase):
    @staticmethod
    def _report(*, structural=False):
        return common.build_cfg_validation_report(
            [
                {
                    "cfg_path": "providers.example.bindings.unused",
                    "status": "failed",
                    "error": "unresolved placeholder",
                    "structural": structural,
                }
            ]
        )

    def test_failed_full_cfg_binding_blocks_by_default(self):
        report = self._report()
        common.apply_full_cfg_validation_gate(report, force_skip=False)
        self.assertEqual(report["gate"]["status"], "failed")
        with self.assertRaisesRegex(RuntimeError, "full cfg validation failed"):
            common.assert_full_cfg_validation_gate_accepted(report)

    def test_authorized_force_skips_only_the_aggregate_gate(self):
        report = self._report()
        common.apply_full_cfg_validation_gate(report, force_skip=True)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["gate"]["status"], "force_skipped")
        common.assert_full_cfg_validation_gate_accepted(report)
        rendered = "\n".join(common._cfg_validation_text_lines(report))
        self.assertIn(
            "full cfg validation gate [ skipped ⏭ ]", rendered
        )

    def test_unclassified_failure_cannot_be_force_skipped(self):
        report = common.build_cfg_validation_report(
            [{"cfg_path": "providers.future", "status": "failed"}]
        )
        common.apply_full_cfg_validation_gate(report, force_skip=True)
        self.assertEqual(report["gate"]["status"], "failed")

    def test_structural_failure_cannot_be_force_skipped(self):
        report = self._report(structural=True)
        common.apply_full_cfg_validation_gate(report, force_skip=True)
        self.assertEqual(report["gate"]["status"], "failed")
        with self.assertRaisesRegex(RuntimeError, "full cfg validation failed"):
            common.assert_full_cfg_validation_gate_accepted(report)


class PreflightArtifactTests(unittest.TestCase):
    def test_report_is_deterministic_and_target_scoped(self):
        class Adapter:
            def preflight_execution_identity(self, target_run_id, target_run, catalogs, **kwargs):
                del target_run_id, catalogs, kwargs
                return {
                    "execution_identity": target_run["execution_identity"],
                    "provider": "fixture",
                    "access_mode": "standard",
                    "status": "passed",
                    "provider_path": [
                        {
                            "display": f"principal: {target_run['execution_identity']['account']}",
                            "status": "passed",
                        }
                    ],
                }

        selection = {
            "selection_kind": "workflow",
            "selection_key": "example/workflow",
            "active_target_runs": {
                "one": {"target": "target/one", "execution_identity": {"provider": "aws", "account": "one", "roles": {"readwrite": "ctl_target"}}},
                "two": {"target": "target/two", "execution_identity": {"provider": "aws", "account": "two", "roles": {"readwrite": "ctl_target"}}},
            },
            "provider_adapter": Adapter(),
            "provider_catalogs": {},
            "execution_context": {},
        }
        report = common.build_execution_identity_preflight_report(
            selection,
            implementation_key="profile",
            execution_access_modes={"aws": "standard"},
            provider_options={},
            force_skip_providers=[],
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
            self.assertIn("principal: two [ passed \u2705 ]", rendered)

    def test_skipped_results_use_one_human_status_and_reason(self):
        report = {
            "selection": {"kind": "workflow", "key": "env/baseline"},
            "status": "passed",
            "results": [
                {
                    "target_key": "env/core/baseline",
                    "execution_identity": {"provider": "aws", "account": "dev", "roles": {"readwrite": "ctl_target_deploy"}},
                    "provider": "aws",
                    "access_mode": "force_bypass",
                    "status": "not_applicable",
                    "provider_path": [],
                    "reason": "execution identity was bypassed for this run",
                },
                {
                    "ctl_state_backend": "env",
                    "execution_identity": None,
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
            "execution_identity: aws:dev:readwrite=ctl_target_deploy [ skipped ⏭ ]", rendered
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
            "workflow_cfg": {"target_runs": ["target"]},
            "inventory_cfg": {"targets": {"target": {}}},
            "execution_context": {},
            "active_target_runs": {},
        }
        with mock.patch.object(
            common,
            "validate_target_policy_constraints_for_target",
            side_effect=RuntimeError("commit-required policy"),
        ), mock.patch.object(
            common, "validate_execution_access"
        ), mock.patch.object(
            common, "validate_execution_runtime_mode"
        ), mock.patch.object(
            common, "validate_target_runs_have_commits"
        ):
            policy_report = common.build_ctl_policy_preflight_report(
                selection,
                ctl_cfg_root=Path("/unused"),
                ctl_profile="local_dev",
                ctl_ref_policy="local_dirty_allowed",
                execution_runtime_mode="local",
                execution_access_modes={"aws": "standard"},
                provider_options={},
                agreed_defer_ctl_state_backend_sync=False,
                force_skip_ctl_state_backend_sync=False,
                force_skip_execution_identity_preflight_check=False,
            )
        self.assertEqual(policy_report["status"], "failed")
        # target_policy_constraints is now a per-target check (hybrid report)
        failed_targets = {
            target["target_key"]: target
            for target in policy_report["targets"]
            if target["status"] == "failed"
        }
        self.assertIn("target", failed_targets)
        target_checks = {
            check["name"]: check for check in failed_targets["target"]["checks"]
        }
        self.assertIn("target_policy_constraints", target_checks)
        self.assertIn(
            "commit-required policy",
            target_checks["target_policy_constraints"]["failure_reason"],
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
        # per-member params fold inline onto the workflow line
        self.assertIn("(account=dev, env_type=dev)", rendered)
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
        # An unparameterized child has no per-member params, so nothing folds onto
        # its workflow line; run-constant params live on the fan-out header instead.
        self.assertIn(
            "workflow: landing_zone/org_state/bootstrap [ passed ✅ ]", rendered
        )
        self.assertNotIn("(landing_zone=", rendered)
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
            self.assertFalse((run_dir / "target_sources").exists())
            self.assertFalse((run_dir / "step_utils").exists())

    def test_run_metadata_records_execution_access_modes_for_degraded_audit(self):
        """Each provider's access mode must be persisted structurally in RUN.yaml
        (not only in the logged command) so a later audit of committed run
        records shows which runs escalated, and for which provider."""
        with tempfile.TemporaryDirectory() as tmp:
            memory = logging.handlers.MemoryHandler(capacity=10)
            run_dir, _artifacts, _log = common.setup_preflight_run_dirs(
                "run-id", "plan", "target", "example", Path(tmp), memory,
                locator_segments=list(common.LOCAL_ONLY_LOCATOR),
                execution_access_modes={"aws": "force_bypass"},
            )
            meta = common.load_run_metadata(run_dir)
            self.assertEqual(
                meta.get("execution_access_modes"), {"aws": "force_bypass"}
            )

    def test_command_log_redacts_provider_option_values(self):
        # the engine cannot tell which of an adapter's option keys is sensitive,
        # so every provider-option VALUE is redacted
        rendered = common.redact_command_argv(
            ["runner.py", "--provider-options", "aws.x=sensitive-selector"]
        )
        self.assertEqual(
            rendered,
            ["runner.py", "--provider-options", "<redacted>"],
        )
        rendered = common.redact_command_argv(
            ["runner.py", "--provider-options=aws.x=sensitive-selector"]
        )
        self.assertEqual(rendered, ["runner.py", "--provider-options=<redacted>"])
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
            "target_roles": {},
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
            aws_adapter, "resolve_target_aws_access", return_value=resolved
        ), mock.patch.object(
            aws_adapter, "assert_profile_caller", return_value=caller
        ) as assertion:
            result = aws_adapter.preflight_execution_identity(
                "target_run",
                {"execution_identity_key": "dev"},
                self.catalogs(),
                execution_context={"execution_context.params.main_tag": "oxygen"},
                implementation_key="profile",
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
            "target_role_key": "prod_deploy",
            "role_name": "prod-deploy",
        }
        with mock.patch.object(
            aws_adapter, "resolve_target_aws_access", return_value=resolved
        ), mock.patch.object(aws_adapter, "assert_profile_caller") as assertion:
            result = aws_adapter.preflight_execution_identity(
                "target_run",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={},
                implementation_key="profile",
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
            "target_role_key": "prod_deploy",
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
            aws_adapter, "resolve_target_aws_access", return_value=resolved
        ), mock.patch.object(
            aws_adapter, "assert_profile_caller", return_value=entry
        ), mock.patch.object(
            aws_adapter, "_assume_role_credentials", side_effect=assume_results
        ) as assume, mock.patch.object(
            aws_adapter, "_run_aws_json", return_value=final
        ):
            result = aws_adapter.preflight_execution_identity(
                "target_run",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={"execution_context.params.main_tag": "oxygen"},
                implementation_key="profile",
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
            "target_role_key": "prod_deploy",
            "role_name": "prod-deploy",
        }
        wrong_final = {
            "Account": "222222222222",
            "Arn": "arn:aws:sts::222222222222:assumed-role/wrong-role/session",
        }
        with mock.patch.object(
            aws_adapter, "resolve_target_aws_access", return_value=resolved
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
                "target_run",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={},
                implementation_key="profile",
            )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["provider_path"][-1]["status"], "failed")

    def test_static_error_still_fails_when_live_check_is_skipped(self):
        with mock.patch.object(
            aws_adapter,
            "resolve_target_aws_access",
            side_effect=RuntimeError("missing target_run role"),
        ):
            result = aws_adapter.preflight_execution_identity(
                "target_run",
                {"execution_identity_key": "prod"},
                self.catalogs(),
                execution_context={},
                implementation_key="profile",
                live_check=False,
            )
        self.assertEqual(result["status"], "failed")
        self.assertIn("missing target_run role", result["failure_reason"])

    def test_bypass_result_redacts_substitute_credential(self):
        with mock.patch.object(
            aws_adapter,
            "resolve_target_aws_access",
            return_value={"identity_bypass": "true"},
        ):
            result = aws_adapter.preflight_execution_identity(
                "target_run",
                {"execution_identity_key": "env_dev_deploy"},
                self.catalogs(),
                execution_context={},
                implementation_key="profile",
                execution_access_mode="force_bypass",
                provider_options={"force_bypass_credential_profile": "do-not-render"},
                live_check=False,
            )
        self.assertEqual(result["status"], "not_applicable")
        self.assertEqual(result["execution_identity"], "<unresolved>")
        self.assertNotIn("do-not-render", str(result))


if __name__ == "__main__":
    unittest.main()
