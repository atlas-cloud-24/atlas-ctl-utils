import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


assert_spec = importlib.util.spec_from_file_location(
    "assert_aws_access",
    REPO_ROOT / "stage_utils" / "assert_aws_access.py",
)
assert_aws_access = importlib.util.module_from_spec(assert_spec)
assert_spec.loader.exec_module(assert_aws_access)


class AwsAccessResolutionTests(unittest.TestCase):
    def setUp(self):
        # aws_access_contexts: direct ctl-owned profile_name + expect (no plt org catalog)
        self.levels = {
            "org_admin": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-org-admin",
                    "expect": {"permission_set_name": "AdministratorAccess"},
                }
            },
            "non_prod_deploy": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-${execution_context.params.env_type}-deploy",
                    "expect": {"permission_set_name": "NonProdDeploy"},
                }
            },
            "non_prod_readonly": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-${execution_context.params.env_type}-readonly",
                    "expect": {"permission_set_name": "ReadOnlyAccess"},
                }
            },
        }
        self.account_registry = {
            "dev": "111111111111",
            "management": "333333333333",
        }
        self.identities = {
            "org_admin": {
                "provider": "aws",
                "account_key": "management",
                "access_context_key": "org_admin",
            },
            "env_deploy": {
                "provider": "aws",
                "account_key": "${execution_context.params.env_type}",
                "access_context_key": "non_prod_deploy",
            },
            "env_readonly": {
                "provider": "aws",
                "account_key": "${execution_context.params.env_type}",
                "access_context_key": "non_prod_readonly",
            },
        }

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_profile_name_resolves_account_profile_and_permission_set(self):
        with mock.patch.object(
            common,
            "resolve_configured_profile_account_id",
            return_value="111111111111",
        ):
            resolved = common.resolve_stage_aws_access(
                {
                    "execution_identity_key": "env_deploy",
                },
                self.identities,
                self.levels,
                execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                implementation_key="local",
                account_registry=self.account_registry,
            )

        self.assertEqual(resolved["account_key"], "dev")
        self.assertEqual(resolved["profile_name"], "oxygen-dev-deploy")
        self.assertEqual(resolved["permission_set_name"], "NonProdDeploy")
        self.assertEqual(resolved["expected_account_id"], "111111111111")

    @mock.patch.dict(
        os.environ,
        {"ATLAS_AWS_PROFILE_NON_PROD_DEPLOY": "custom-dev-deploy"},
        clear=True,
    )
    def test_profile_override_cannot_change_account(self):
        account_ids = {
            "oxygen-dev-deploy": "111111111111",
            "custom-dev-deploy": "222222222222",
        }
        with mock.patch.object(
            common,
            "resolve_configured_profile_account_id",
            side_effect=account_ids.__getitem__,
        ):
            with self.assertRaisesRegex(RuntimeError, "profile override"):
                common.resolve_stage_aws_access(
                    {
                        "execution_identity_key": "env_deploy",
                    },
                    self.identities,
                    self.levels,
                    execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                    implementation_key="local",
                    account_registry=self.account_registry,
                )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_direct_profile_requires_and_resolves_principal_expectation(self):
        with mock.patch.object(
            common,
            "resolve_configured_profile_account_id",
            return_value="333333333333",
        ):
            resolved = common.resolve_stage_aws_access(
                {
                    "execution_identity_key": "org_admin",
                },
                self.identities,
                self.levels,
                execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                implementation_key="local",
                account_registry=self.account_registry,
            )
        self.assertEqual(resolved["profile_name"], "oxygen-org-admin")
        self.assertEqual(resolved["permission_set_name"], "AdministratorAccess")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_missing_execution_identity_fails(self):
        with self.assertRaisesRegex(RuntimeError, "is not defined in execution_identities"):
            common.resolve_stage_aws_access(
                {"execution_identity_key": "missing"},
                self.identities,
                self.levels,
                execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                implementation_key="local",
                account_registry=self.account_registry,
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_missing_runner_implementation_fails(self):
        with self.assertRaisesRegex(RuntimeError, "has no 'ci' implementation"):
            common.resolve_stage_aws_access(
                {
                    "execution_identity_key": "env_deploy",
                },
                self.identities,
                self.levels,
                execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                implementation_key="ci",
            )

    def test_unknown_runtime_placeholder_fails(self):
        with self.assertRaisesRegex(RuntimeError, "not found in execution context"):
            common.resolve_runtime_scalar(
                "${unknown}_deploy",
                {"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                label="test",
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_account_registry_rejects_conflicting_profile_ids(self):
        account_ids = {
            "oxygen-dev-deploy": "111111111111",
            "oxygen-dev-readonly": "222222222222",
        }
        stages = {
            "deploy": {"execution_identity_key": "env_deploy"},
            "readonly": {"execution_identity_key": "env_readonly"},
        }
        with mock.patch.object(
            common,
            "resolve_configured_profile_account_id",
            side_effect=account_ids.__getitem__,
        ):
            with self.assertRaisesRegex(RuntimeError, "AWS account registry maps"):
                common.validate_active_stage_aws_access(
                    stages,
                    self.identities,
                    self.levels,
                    execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                    implementation_key="local",
                    account_registry=self.account_registry,
                )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_non_aws_stage_drops_inherited_credentials(self):
        stage_env = {
            "AWS_PROFILE": "wrong",
            "AWS_DEFAULT_PROFILE": "wrong-default",
            "AWS_CONFIG_FILE": "/tmp/wrong-config",
            "AWS_SHARED_CREDENTIALS_FILE": "/tmp/wrong-credentials",
            "AWS_ACCESS_KEY_ID": "secret",
        }
        common.configure_stage_aws_env(
            "test",
            {},
            stage_env,
            self.identities,
            self.levels,
            execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
            implementation_key="local",
            account_registry={},
        )
        self.assertNotIn("AWS_PROFILE", stage_env)
        self.assertNotIn("AWS_DEFAULT_PROFILE", stage_env)
        self.assertNotIn("AWS_CONFIG_FILE", stage_env)
        self.assertNotIn("AWS_SHARED_CREDENTIALS_FILE", stage_env)
        self.assertNotIn("AWS_ACCESS_KEY_ID", stage_env)

    @mock.patch.dict(os.environ, {"AWS_PROFILE": "ambient-dev-profile"}, clear=True)
    def test_profile_only_requires_explicit_aws_profile_arg(self):
        resolved = common.resolve_stage_aws_access(
            {},
            self.identities,
            self.levels,
            execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
            implementation_key="local",
            allow_profile_only=True,
        )
        self.assertIsNone(resolved)

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_profile_only_access_uses_explicit_aws_profile_arg(self):
        resolved = common.resolve_stage_aws_access(
            {},
            self.identities,
            self.levels,
            execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
            implementation_key="local",
            allow_profile_only=True,
            profile_only_aws_profile="legacy-dev-profile",
        )
        self.assertEqual(resolved["profile_name"], "legacy-dev-profile")
        self.assertEqual(resolved["credential_provider_kind"], "aws_profile_only")
        self.assertEqual(resolved["profile_only"], "true")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_profile_only_configures_stage_env_without_expected_account(self):
        stage_env = {"AWS_ACCESS_KEY_ID": "secret"}
        common.configure_stage_aws_env(
            "test",
            {},
            stage_env,
            self.identities,
            self.levels,
            execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
            implementation_key="local",
            account_registry={},
            allow_profile_only=True,
            profile_only_aws_profile="legacy-dev-profile",
        )
        self.assertEqual(stage_env["AWS_PROFILE"], "legacy-dev-profile")
        self.assertEqual(stage_env["ATLAS_AWS_PROFILE_ONLY_ACCESS"], "true")
        self.assertNotIn("ATLAS_AWS_EXPECT_ACCOUNT_ID", stage_env)
        self.assertNotIn("AWS_ACCESS_KEY_ID", stage_env)

    def test_profile_only_validation_requires_all_selected_stages_without_identity(self):
        stages = {
            "declared": {"execution_identity_key": "env_deploy"},
            "fallback": {},
        }
        with self.assertRaisesRegex(RuntimeError, "--aws-profile can be used only"):
            common.validate_active_stage_aws_access(
                stages,
                self.identities,
                self.levels,
                execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                implementation_key="local",
                account_registry=self.account_registry,
                allow_profile_only=True,
                profile_only_aws_profile="legacy-dev-profile",
            )

    def test_profile_only_validation_rejects_override_of_declared_identity(self):
        stages = {"declared": {"execution_identity_key": "env_deploy"}}
        with self.assertRaisesRegex(RuntimeError, "declared execution identities cannot be overridden"):
            common.validate_active_stage_aws_access(
                stages,
                self.identities,
                self.levels,
                execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                implementation_key="local",
                account_registry=self.account_registry,
                allow_profile_only=True,
                profile_only_aws_profile="legacy-dev-profile",
            )

    def test_profile_only_validation_requires_profile_when_all_identities_missing(self):
        stages = {"fallback": {}}
        with self.assertRaisesRegex(RuntimeError, "require --aws-profile"):
            common.validate_active_stage_aws_access(
                stages,
                self.identities,
                self.levels,
                execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
                implementation_key="local",
                account_registry=self.account_registry,
                allow_profile_only=True,
            )

    def test_profile_only_validation_allows_all_missing_with_profile_and_policy(self):
        stages = {"fallback": {}}
        resolved = common.validate_active_stage_aws_access(
            stages,
            self.identities,
            self.levels,
            execution_context={"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"},
            implementation_key="local",
            allow_profile_only=True,
            profile_only_aws_profile="legacy-dev-profile",
        )
        self.assertEqual(resolved, {})


class CallerIdentityAssertionTests(unittest.TestCase):
    def test_identity_center_role_is_anchored(self):
        caller = {
            "Account": "111111111111",
            "Arn": (
                "arn:aws:sts::111111111111:assumed-role/"
                "AWSReservedSSO_NonProdDeployAccess_abc123/session"
            ),
        }
        account_id, arn = assert_aws_access.validate_caller_identity(
            caller,
            expected_account_id="111111111111",
            expected_permission_set_name="NonProdDeployAccess",
        )
        self.assertEqual(account_id, "111111111111")
        self.assertEqual(arn, caller["Arn"])

        rotated = dict(caller)
        rotated["Arn"] = (
            "arn:aws:sts::111111111111:assumed-role/"
            "AWSReservedSSO_NonProdDeployAccess_rotated/session"
        )
        assert_aws_access.validate_caller_identity(
            rotated,
            expected_account_id="111111111111",
            expected_permission_set_name="NonProdDeployAccess",
        )

    def test_loose_permission_set_prefix_is_rejected(self):
        caller = {
            "Account": "111111111111",
            "Arn": (
                "arn:aws:sts::111111111111:assumed-role/"
                "prefix-AWSReservedSSO_NonProdDeployAccess_abc/session"
            ),
        }
        with self.assertRaisesRegex(RuntimeError, "permission-set mismatch"):
            assert_aws_access.validate_caller_identity(
                caller,
                expected_account_id="111111111111",
                expected_permission_set_name="NonProdDeployAccess",
            )

    def test_assume_role_name_must_match_exactly(self):
        caller = {
            "Account": "222222222222",
            "Arn": (
                "arn:aws:sts::222222222222:assumed-role/"
                "OrganizationAccountAccessRole/session"
            ),
        }
        assert_aws_access.validate_caller_identity(
            caller,
            expected_account_id="222222222222",
            expected_role_name="OrganizationAccountAccessRole",
        )
        with self.assertRaisesRegex(RuntimeError, "AWS role mismatch"):
            assert_aws_access.validate_caller_identity(
                caller,
                expected_account_id="222222222222",
                expected_role_name="OtherRole",
            )


if __name__ == "__main__":
    unittest.main()
