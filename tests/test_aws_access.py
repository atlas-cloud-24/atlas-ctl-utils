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
    REPO_ROOT / "stages" / "assert_aws_access.py",
)
assert_aws_access = importlib.util.module_from_spec(assert_spec)
assert_spec.loader.exec_module(assert_aws_access)


class AwsAccessResolutionTests(unittest.TestCase):
    def setUp(self):
        self.levels = {
            "org_admin": {
                "local": {
                    "profile_name": "${main_tag}-org-admin",
                    "expect": {"permission_set_name": "AdministratorAccess"},
                }
            },
            "env_deploy": {
                "local": {"profile_key": "${plt_env}_deploy"}
            },
        }
        self.catalogs = {
            "profiles": {
                "dev_deploy": {
                    "profile_name": "${main_tag}-dev-deploy",
                    "account_key": "dev",
                    "permission_set_key": "non_prod_deploy",
                }
            },
            "permission_sets": {
                "non_prod_deploy": {"name": "NonProdDeployAccess"}
            },
        }

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_profile_key_derives_account_profile_and_permission_set(self):
        with mock.patch.object(
            common,
            "resolve_configured_profile_account_id",
            return_value="111111111111",
        ):
            resolved = common.resolve_stage_aws_access(
                {
                    "aws_account_key": "${plt_env}",
                    "aws_access_context_key": "env_deploy",
                },
                self.levels,
                self.catalogs,
                main_tag="oxygen",
                plt_env="dev",
                implementation_key="local",
            )

        self.assertEqual(resolved["account_key"], "dev")
        self.assertEqual(resolved["profile_name"], "oxygen-dev-deploy")
        self.assertEqual(resolved["permission_set_name"], "NonProdDeployAccess")
        self.assertEqual(resolved["expected_account_id"], "111111111111")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_profile_catalog_account_must_match_target(self):
        with self.assertRaisesRegex(RuntimeError, "target requires 'prod'"):
            common.resolve_stage_aws_access(
                {
                    "aws_account_key": "prod",
                    "aws_access_context_key": "env_deploy",
                },
                self.levels,
                self.catalogs,
                main_tag="oxygen",
                plt_env="dev",
                implementation_key="local",
            )

    @mock.patch.dict(
        os.environ,
        {"ATLAS_AWS_PROFILE_ENV_DEPLOY": "custom-dev-deploy"},
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
                        "aws_account_key": "dev",
                        "aws_access_context_key": "env_deploy",
                    },
                    self.levels,
                    self.catalogs,
                    main_tag="oxygen",
                    plt_env="dev",
                    implementation_key="local",
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
                    "aws_account_key": "management",
                    "aws_access_context_key": "org_admin",
                },
                self.levels,
                self.catalogs,
                main_tag="oxygen",
                plt_env="dev",
                implementation_key="local",
            )
        self.assertEqual(resolved["profile_name"], "oxygen-org-admin")
        self.assertEqual(resolved["permission_set_name"], "AdministratorAccess")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_missing_account_or_access_context_fails(self):
        with self.assertRaisesRegex(RuntimeError, "must define both"):
            common.resolve_stage_aws_access(
                {"aws_account_key": "dev"},
                self.levels,
                self.catalogs,
                main_tag="oxygen",
                plt_env="dev",
                implementation_key="local",
            )


    @mock.patch.dict(os.environ, {}, clear=True)
    def test_missing_runner_implementation_fails(self):
        with self.assertRaisesRegex(RuntimeError, "has no 'ci' implementation"):
            common.resolve_stage_aws_access(
                {
                    "aws_account_key": "dev",
                    "aws_access_context_key": "env_deploy",
                },
                self.levels,
                self.catalogs,
                main_tag="oxygen",
                plt_env="dev",
                implementation_key="ci",
            )

    def test_unknown_runtime_placeholder_fails(self):
        with self.assertRaisesRegex(RuntimeError, "unavailable runtime value"):
            common.resolve_runtime_scalar(
                "${unknown}_deploy",
                {"main_tag": "oxygen", "plt_env": "dev"},
                label="test",
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_account_registry_rejects_conflicting_profile_ids(self):
        levels = {
            **self.levels,
            "env_readonly": {"local": {"profile_key": "${plt_env}_readonly"}},
        }
        catalogs = {
            "profiles": {
                **self.catalogs["profiles"],
                "dev_readonly": {
                    "profile_name": "${main_tag}-dev-readonly",
                    "account_key": "dev",
                    "permission_set_key": "read_only_access",
                },
            },
            "permission_sets": {
                **self.catalogs["permission_sets"],
                "read_only_access": {"name": "ReadOnlyAccess"},
            },
        }
        account_ids = {
            "oxygen-dev-deploy": "111111111111",
            "oxygen-dev-readonly": "222222222222",
        }
        stages = {
            "deploy": {
                "aws_account_key": "dev",
                "aws_access_context_key": "env_deploy",
            },
            "readonly": {
                "aws_account_key": "dev",
                "aws_access_context_key": "env_readonly",
            },
        }
        with mock.patch.object(
            common,
            "resolve_configured_profile_account_id",
            side_effect=account_ids.__getitem__,
        ):
            with self.assertRaisesRegex(RuntimeError, "Conflicting AWS account IDs"):
                common.validate_active_stage_aws_access(
                    stages,
                    levels,
                    catalogs,
                    main_tag="oxygen",
                    plt_env="dev",
                    implementation_key="local",
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
            self.levels,
            self.catalogs,
            main_tag="oxygen",
            plt_env="dev",
            implementation_key="local",
            account_registry={},
        )
        self.assertNotIn("AWS_PROFILE", stage_env)
        self.assertNotIn("AWS_DEFAULT_PROFILE", stage_env)
        self.assertNotIn("AWS_CONFIG_FILE", stage_env)
        self.assertNotIn("AWS_SHARED_CREDENTIALS_FILE", stage_env)
        self.assertNotIn("AWS_ACCESS_KEY_ID", stage_env)


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
