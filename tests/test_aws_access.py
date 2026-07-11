import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402
from utils.providers import aws as aws_adapter  # noqa: E402


assert_spec = importlib.util.spec_from_file_location(
    "assert_aws_access",
    REPO_ROOT / "stage_utils" / "assert_aws_access.py",
)
assert_aws_access = importlib.util.module_from_spec(assert_spec)
assert_spec.loader.exec_module(assert_aws_access)


class AwsAccessResolutionTests(unittest.TestCase):
    def setUp(self):
        # Phase 14: EVERY local credential source declares its real principal;
        # the entry source also declares its account (expect.account_key)
        self.credential_sources = {
            "org_admin": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-org-admin",
                    "expect": {"permission_set_name": "AdministratorAccess"},
                }
            },
            "non_prod_deploy": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-${execution_context.params.env_type}-deploy",
                    "expect": {"permission_set_name": "NonProdDeployAccess"},
                }
            },
            "non_prod_readonly": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-${execution_context.params.env_type}-readonly",
                    "expect": {"permission_set_name": "NonProdReadOnlyAccess"},
                }
            },
            "ctl_state_synchronizer": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-${identity.account_key}-ctl-state-synchronizer",
                    "expect": {"role_name": "oxygen-ctl-state-synchronizer"},
                }
            },
            "ctl_entry": {
                "local": {
                    "profile_name": "${execution_context.params.main_tag}-ctl-entry",
                    "expect": {
                        "account_key": "ctl_plane",
                        "permission_set_name": "oxygen-live-ctl-entry",
                    },
                }
            },
        }
        self.account_registry = {
            "dev": "111111111111",
            "management": "333333333333",
            "ctl_plane": "444444444444",
        }
        self.identities = {
            "org_admin": {
                "provider": "aws",
                "account_key": "management",
                "direct_credential_source_key": "org_admin",
            },
            "env_deploy": {
                "provider": "aws",
                "account_key": "${execution_context.params.env_type}",
                "ctl_stage_role_key": "ctl_stage",
                "direct_credential_source_key": "non_prod_deploy",
            },
            "env_readonly": {
                "provider": "aws",
                "account_key": "${execution_context.params.env_type}",
                "ctl_stage_role_key": "ctl_stage",
                "direct_credential_source_key": "non_prod_readonly",
            },
            "env_no_stage_role": {
                "provider": "aws",
                "account_key": "${execution_context.params.env_type}",
            },
            "ctl_state_dev_synchronizer": {
                "provider": "aws",
                "account_key": "dev",
                "direct_credential_source_key": "ctl_state_synchronizer",
            },
        }
        self.stage_roles = {
            "ctl_runner": {
                "account_key": "ctl_plane",
                "role_name": "${execution_context.params.main_tag}-ctl-runner",
            },
            "ctl_stage": {
                "role_name": "${execution_context.params.main_tag}-ctl-stage",
            },
            "org_ctl_member_account_roles": {
                "account_key": "management",
                "role_name": "${execution_context.params.main_tag}-ctl-stage-org-member-account-roles",
            },
        }
        self.ctl_role_chain = {
            "entry_credential_source_key": "ctl_entry",
            "runner_role_key": "ctl_runner",
        }
        self.context = {
            "execution_context.params.main_tag": "oxygen",
            "execution_context.params.env_type": "dev",
        }

    # --- direct mode (--agreed-skip-ctl-role-chain) ---

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_direct_mode_resolves_profile_account_and_principal(self):
        with mock.patch.object(
            aws_adapter,
            "resolve_configured_profile_account_id",
            return_value="111111111111",
        ):
            resolved = aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "env_deploy"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
                execution_access_mode="direct",
            )
        self.assertEqual(resolved["account_key"], "dev")
        self.assertEqual(resolved["profile_name"], "oxygen-dev-deploy")
        self.assertEqual(resolved["expected_account_id"], "111111111111")
        self.assertEqual(resolved["credential_provider_kind"], "direct_profile")
        # Phase 14: direct mode carries the source's declared principal
        self.assertEqual(resolved["permission_set_name"], "NonProdDeployAccess")
        self.assertNotIn("role_name", resolved)

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_direct_mode_interpolates_identity_account_key(self):
        with mock.patch.object(
            aws_adapter,
            "resolve_configured_profile_account_id",
            return_value="111111111111",
        ):
            resolved = aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "ctl_state_dev_synchronizer"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
                execution_access_mode="direct",
            )
        self.assertEqual(resolved["profile_name"], "oxygen-dev-ctl-state-synchronizer")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_direct_mode_requires_direct_credential_source_key(self):
        with self.assertRaisesRegex(RuntimeError, "no direct_credential_source_key"):
            aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "env_no_stage_role"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
                execution_access_mode="direct",
            )

    def test_execution_identity_loader_rejects_pre_rename_fields(self):
        for removed_field in ("access_context_key", "direct_access_context_key"):
            with self.subTest(removed_field=removed_field):
                identity = {
                    "provider": "aws",
                    "account_key": "dev",
                    removed_field: "non_prod_deploy",
                }
                with mock.patch.object(
                    common,
                    "collect_resource",
                    return_value={"env_deploy": identity},
                ):
                    with self.assertRaisesRegex(RuntimeError, f"removed {removed_field}"):
                        common.load_execution_identities_cfg(Path("/unused"))

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
            aws_adapter,
            "resolve_configured_profile_account_id",
            side_effect=account_ids.__getitem__,
        ):
            with self.assertRaisesRegex(RuntimeError, "profile override"):
                aws_adapter.resolve_stage_aws_access(
                    {"execution_identity_key": "env_deploy"},
                    self.identities,
                    self.credential_sources,
                    execution_context=self.context,
                    implementation_key="local",
                    account_registry=self.account_registry,
                    execution_access_mode="direct",
                )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_missing_execution_identity_fails(self):
        with self.assertRaisesRegex(RuntimeError, "is not defined in execution_identities"):
            aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "missing"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_missing_runner_implementation_fails(self):
        with self.assertRaisesRegex(RuntimeError, "has no 'ci' implementation"):
            aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "env_deploy"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="ci",
                account_registry=self.account_registry,
                execution_access_mode="direct",
            )

    def test_unknown_runtime_placeholder_fails(self):
        with self.assertRaisesRegex(RuntimeError, "not found in execution context"):
            common.resolve_runtime_scalar(
                "${unknown}_deploy",
                self.context,
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
            aws_adapter,
            "resolve_configured_profile_account_id",
            side_effect=account_ids.__getitem__,
        ):
            with self.assertRaisesRegex(RuntimeError, "AWS account registry maps"):
                aws_adapter.validate_active_stage_aws_access(
                    stages,
                    self.identities,
                    self.credential_sources,
                    execution_context=self.context,
                    implementation_key="local",
                    account_registry=self.account_registry,
                    execution_access_mode="direct",
                )

    # --- chain mode (default) ---

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_derives_runner_and_stage_role_arns(self):
        resolved = aws_adapter.resolve_stage_aws_access(
            {"execution_identity_key": "env_deploy"},
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="local",
            account_registry=self.account_registry,
            ctl_role_chain=self.ctl_role_chain,
            stage_roles=self.stage_roles,
        )
        self.assertEqual(resolved["credential_provider_kind"], "role_chain")
        self.assertEqual(resolved["entry_profile_name"], "oxygen-ctl-entry")
        self.assertEqual(resolved["entry_permission_set_name"], "oxygen-live-ctl-entry")
        self.assertEqual(
            resolved["hop_role_arns"],
            [
                "arn:aws:iam::444444444444:role/oxygen-ctl-runner",
                "arn:aws:iam::111111111111:role/oxygen-ctl-stage",
            ],
        )
        self.assertEqual(resolved["role_name"], "oxygen-ctl-stage")
        self.assertEqual(resolved["expected_account_id"], "111111111111")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_identity_stage_role_override(self):
        identities = dict(self.identities)
        identities["org_ctl_member_account_roles"] = {
            "provider": "aws",
            "account_key": "management",
            "ctl_stage_role_key": "org_ctl_member_account_roles",
        }
        resolved = aws_adapter.resolve_stage_aws_access(
            {"execution_identity_key": "org_ctl_member_account_roles"},
            identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="local",
            account_registry=self.account_registry,
            ctl_role_chain=self.ctl_role_chain,
            stage_roles=self.stage_roles,
        )
        self.assertEqual(
            resolved["hop_role_arns"][-1],
            "arn:aws:iam::333333333333:role/oxygen-ctl-stage-org-member-account-roles",
        )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_requires_ctl_stage_role_key(self):
        # Phase 15: a target identity without ctl_stage_role_key is invalid in chain mode
        with self.assertRaisesRegex(RuntimeError, "declares no ctl_stage_role_key"):
            aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "env_no_stage_role"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
                ctl_role_chain=self.ctl_role_chain,
                stage_roles=self.stage_roles,
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_requires_ctl_role_chain_cfg(self):
        with self.assertRaisesRegex(RuntimeError, "require providers.aws.ctl_role_chain"):
            aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "env_deploy"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
            )

    # --- identity bypass (--force-skip-execution-identity) ---

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_identity_bypass_uses_substitute_credential_even_with_identity(self):
        resolved = aws_adapter.resolve_stage_aws_access(
            {"execution_identity_key": "env_deploy"},
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="local",
            execution_access_mode="bypass",
            provider_credential="my-sandbox-admin",
        )
        self.assertEqual(resolved["profile_name"], "my-sandbox-admin")
        self.assertEqual(resolved["credential_provider_kind"], "substitute_credential")
        self.assertEqual(resolved["identity_bypass"], "true")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_identity_bypass_requires_substitute_credential(self):
        with self.assertRaisesRegex(RuntimeError, "requires the --provider-credential"):
            aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "env_deploy"},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                execution_access_mode="bypass",
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_identity_bypass_configures_stage_env_without_checks(self):
        stage_env = {"AWS_ACCESS_KEY_ID": "secret"}
        aws_adapter.configure_stage_aws_env(
            "test",
            {"execution_identity_key": "env_deploy"},
            stage_env,
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="local",
            account_registry={},
            execution_access_mode="bypass",
            provider_credential="my-sandbox-admin",
        )
        self.assertEqual(stage_env["AWS_PROFILE"], "my-sandbox-admin")
        self.assertEqual(stage_env["ATLAS_AWS_PROFILE_ONLY_ACCESS"], "true")
        self.assertNotIn("ATLAS_AWS_EXPECT_ACCOUNT_ID", stage_env)
        self.assertNotIn("AWS_ACCESS_KEY_ID", stage_env)

    # --- Phase 14: expect everywhere ---

    def test_local_source_without_expect_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, r"must declare\s+expect"):
            aws_adapter._validate_aws_credential_source_implementation(
                "bare", "local", {"profile_name": "some-profile"}, Path("/cfg")
            )

    def test_local_source_with_multiple_principals_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            aws_adapter._validate_aws_credential_source_implementation(
                "ambiguous",
                "local",
                {
                    "profile_name": "some-profile",
                    "expect": {
                        "permission_set_name": "ReadOnlyAccess",
                        "role_name": "SomeRole",
                    },
                },
                Path("/cfg"),
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_direct_mode_binding_exports_principal_expectation(self):
        stage_env: dict[str, str] = {}
        with mock.patch.object(
            aws_adapter,
            "resolve_configured_profile_account_id",
            return_value="111111111111",
        ):
            aws_adapter.configure_stage_aws_env(
                "stage",
                {"execution_identity_key": "env_deploy"},
                stage_env,
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
                execution_access_mode="direct",
            )
        self.assertEqual(stage_env["ATLAS_AWS_EXPECT_ACCOUNT_ID"], "111111111111")
        self.assertEqual(stage_env["ATLAS_AWS_EXPECT_PERMISSION_SET_NAME"], "NonProdDeployAccess")

    def test_direct_expect_account_key_conflict_is_rejected(self):
        sources = dict(self.credential_sources)
        sources["org_admin"] = {
            "local": {
                "profile_name": "oxygen-org-admin",
                "expect": {"account_key": "dev", "permission_set_name": "AdministratorAccess"},
            }
        }
        with mock.patch.object(
            aws_adapter,
            "resolve_configured_profile_account_id",
            return_value="333333333333",
        ):
            with self.assertRaisesRegex(RuntimeError, "expect.account_key resolves to"):
                aws_adapter.resolve_stage_aws_access(
                    {"execution_identity_key": "org_admin"},
                    self.identities,
                    sources,
                    execution_context=self.context,
                    implementation_key="local",
                    account_registry=self.account_registry,
                    execution_access_mode="direct",
                )

    def test_chain_resolution_requires_entry_account_key(self):
        sources = dict(self.credential_sources)
        sources["ctl_entry"] = {
            "local": {
                "profile_name": "oxygen-ctl-entry",
                "expect": {"permission_set_name": "oxygen-live-ctl-entry"},
            }
        }
        with self.assertRaisesRegex(RuntimeError, r"must declare\s+expect.account_key"):
            aws_adapter.resolve_stage_aws_access(
                {"execution_identity_key": "env_deploy"},
                self.identities,
                sources,
                execution_context=self.context,
                implementation_key="local",
                account_registry=self.account_registry,
                ctl_role_chain=self.ctl_role_chain,
                stage_roles=self.stage_roles,
            )

    def test_entry_validation_is_exact_not_substring(self):
        # a caller whose role EMBEDS the expected set name as a substring must fail
        caller = {
            "Account": "444444444444",
            "Arn": "arn:aws:sts::444444444444:assumed-role/"
                   "AWSReservedSSO_oxygen-live-ctl-entry-evil_abc123/session",
        }
        with self.assertRaisesRegex(RuntimeError, "permission-set mismatch"):
            assert_aws_access.validate_caller_identity(
                caller,
                expected_account_id="444444444444",
                expected_permission_set_name="oxygen-live-ctl-entry",
            )

    # --- identity coverage ---

    def test_identity_coverage_rejects_missing_identity_without_bypass(self):
        stages = {"declared": {"execution_identity_key": "env_deploy"}, "bare": {}}
        with self.assertRaisesRegex(RuntimeError, "have no execution_identity_key"):
            common.validate_execution_identity_coverage(
                stages, execution_access_mode="standard"
            )

    def test_identity_coverage_allows_anything_under_bypass(self):
        stages = {"declared": {"execution_identity_key": "env_deploy"}, "bare": {}}
        common.validate_execution_identity_coverage(
            stages, execution_access_mode="bypass"
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
        aws_adapter.configure_stage_aws_env(
            "test",
            {},
            stage_env,
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="local",
            account_registry={},
        )
        self.assertNotIn("AWS_PROFILE", stage_env)
        self.assertNotIn("AWS_DEFAULT_PROFILE", stage_env)
        self.assertNotIn("AWS_CONFIG_FILE", stage_env)
        self.assertNotIn("AWS_SHARED_CREDENTIALS_FILE", stage_env)
        self.assertNotIn("AWS_ACCESS_KEY_ID", stage_env)

    def test_synchronizer_asserts_selected_profile_principal(self):
        with (
            mock.patch.object(
                common, "load_execution_identities_cfg", return_value=self.identities
            ),
            mock.patch.object(
                aws_adapter,
                "load_aws_credential_sources_cfg",
                return_value=self.credential_sources,
            ),
            mock.patch.object(
                aws_adapter,
                "load_aws_account_registry_cfg",
                return_value=self.account_registry,
            ),
            mock.patch.object(
                aws_adapter,
                "resolve_configured_profile_account_id",
                return_value="111111111111",
            ),
            mock.patch.object(aws_adapter, "assert_profile_caller") as assertion,
        ):
            credential = aws_adapter.resolve_synchronizer_credential(
                "ctl_state_dev_synchronizer",
                Path("/cfg"),
                execution_context=self.context,
                implementation_key="local",
            )

        self.assertEqual(credential, "oxygen-dev-ctl-state-synchronizer")
        assertion.assert_called_once_with(
            "oxygen-dev-ctl-state-synchronizer",
            expected_account_id="111111111111",
            expect_principal={"role_name": "oxygen-ctl-state-synchronizer"},
            label="ctl-state synchronizer identity " + repr("ctl_state_dev_synchronizer"),
        )


class CallerIdentityAssertionTests(unittest.TestCase):
    def test_exactly_one_principal_expectation_is_required(self):
        caller = {
            "Account": "111111111111",
            "Arn": "arn:aws:sts::111111111111:assumed-role/SomeRole/session",
        }
        with self.assertRaisesRegex(RuntimeError, "exactly one expected"):
            assert_aws_access.validate_caller_identity(
                caller, expected_account_id="111111111111"
            )
        with self.assertRaisesRegex(RuntimeError, "exactly one expected"):
            assert_aws_access.validate_caller_identity(
                caller,
                expected_account_id="111111111111",
                expected_permission_set_name="ReadOnlyAccess",
                expected_role_name="SomeRole",
            )

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
