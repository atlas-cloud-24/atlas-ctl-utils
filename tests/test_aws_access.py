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
    REPO_ROOT / "step_utils" / "assert_aws_access.py",
)
assert_aws_access = importlib.util.module_from_spec(assert_spec)
assert_spec.loader.exec_module(assert_aws_access)


class AwsAccessResolutionTests(unittest.TestCase):
    def setUp(self):
        # Phase 14: EVERY local credential source declares its real principal;
        # the entry source also declares its account (expect.account_key)
        self.credential_sources = {
            "org_admin": {
                "profile": {
                    "profile_name": "${execution_context.params.main_tag}-org-admin",
                    "expect": {"permission_set_name": "AdministratorAccess"},
                }
            },
            "non_prod_deploy": {
                "profile": {
                    "profile_name": "${execution_context.params.main_tag}-${execution_context.params.env_type}-deploy",
                    "expect": {"permission_set_name": "NonProdDeployAccess"},
                }
            },
            "non_prod_readonly": {
                "profile": {
                    "profile_name": "${execution_context.params.main_tag}-${execution_context.params.env_type}-readonly",
                    "expect": {"permission_set_name": "NonProdReadOnlyAccess"},
                }
            },
            "ctl_state_synchronizer": {
                "profile": {
                    "profile_name": "${execution_context.params.main_tag}-${identity.account_key}-ctl-state-synchronizer",
                    "expect": {"role_name": "oxygen-ctl-state-synchronizer"},
                }
            },
            "ctl_entry": {
                "profile": {
                    "profile_name": "${execution_context.params.main_tag}-ctl-entry",
                    "expect": {
                        "account_key": "ctl_plane",
                        "permission_set_name": "CtlEntryAccess",
                    },
                }
            },
        }
        self.account_registry = {
            "dev": "111111111111",
            "management": "333333333333",
            "ctl_plane": "444444444444",
        }
        # §Phase 53: a target declares its execution inline — provider, account,
        # and a role per authorization class (the ACTION picks the class).
        self.executions = {
            "org_admin": {
                "provider": "aws",
                "account": "management",
                "roles": {"readwrite": "ctl_target", "readonly": "ctl_target"},
                "agreed_direct_credential_source_keys": ["org_admin"],
            },
            "env_deploy": {
                "provider": "aws",
                "account": "${execution_context.params.env_type}",
                "roles": {"readwrite": "ctl_target", "readonly": "ctl_target"},
                "agreed_direct_credential_source_keys": ["non_prod_deploy"],
            },
            "env_readonly": {
                "provider": "aws",
                "account": "${execution_context.params.env_type}",
                "roles": {"readwrite": "ctl_target", "readonly": "ctl_target"},
                "agreed_direct_credential_source_keys": ["non_prod_readonly"],
            },
            "env_no_target_role": {
                "provider": "aws",
                "account": "${execution_context.params.env_type}",
                "roles": {"readonly": "ctl_target"},
            },
            # a ctl-state operation block: one role, keyed by OPERATION
            "ctl_state_dev_synchronizer": {
                "account": "dev",
                "operation": "sync",
                "role": "synchronizer",
                "agreed_direct_credential_source_key": "ctl_state_synchronizer",
            },
        }
        self.identities = {}
        self.target_roles = {
            "ctl_runner": {
                "account_key": "ctl_plane",
                "role_name": "${execution_context.params.main_tag}-ctl-runner",
            },
            "ctl_target": {
                "role_name": "${execution_context.params.main_tag}-ctl-target_run",
            },
            "org_ctl_member_account_roles": {
                "account_key": "management",
                "role_name": "${execution_context.params.main_tag}-ctl-target_run-org-member-account-roles",
            },
        }
        self.ctl_role_chain = {
            "entry_credential_source_key": "ctl_entry",
            "runner_role_key": "ctl_runner",
        }
        self.context = {
            "execution_context.params.main_tag": "oxygen",
            "execution_context.params.env_type": "dev",
            # the action selects the authorization class
            "execution_context.ctl.action": "provision",
            # the run declares its participating providers
            "execution_context.ctl.providers": ["aws"],
        }

    # --- direct mode (--agreed-skip-ctl-role-chain) ---

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_direct_mode_resolves_profile_account_and_principal(self):
        with mock.patch.object(
            aws_adapter,
            "resolve_configured_profile_account_id",
            return_value="111111111111",
        ):
            resolved = aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_deploy"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
                execution_access_mode="agreed_direct",
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
            resolved = aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["ctl_state_dev_synchronizer"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
                execution_access_mode="agreed_direct",
            )
        self.assertEqual(resolved["profile_name"], "oxygen-dev-ctl-state-synchronizer")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_direct_mode_requires_direct_credential_source_key(self):
        with self.assertRaisesRegex(RuntimeError, "no agreed_direct_credential_source_keys"):
            aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_no_target_role"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
                execution_access_mode="agreed_direct",
            )

    def test_removed_execution_identity_key_is_a_migration_error(self):
        # §Phase 53: the identity bundle is dissolved; a target still carrying the
        # old key must fail loud naming its replacement (hard cutover, no alias).
        with self.assertRaisesRegex(RuntimeError, r"execution must be a non-empty mapping"):
            common.validate_target_execution_identity({}, label="target 'env/static/x'")
        with self.assertRaisesRegex(RuntimeError, "unknown fields"):
            common.validate_target_execution_identity(
                {"provider": "aws", "account": "dev",
                 "roles": {"readwrite": "ctl_target"},
                 "execution_identity_key": "env_deploy"},
                label="target 'env/static/x'",
            )

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
                aws_adapter.resolve_target_aws_access(
                    {"execution_identity": self.executions["env_deploy"]},
                    self.identities,
                    self.credential_sources,
                    execution_context=self.context,
                    implementation_key="profile",
                    account_registry=self.account_registry,
                    execution_access_mode="agreed_direct",
                )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_execution_less_target_run_resolves_to_none(self):
        # Coverage is validated separately; a lone resolve has nothing to resolve.
        self.assertIsNone(
            aws_adapter.resolve_target_aws_access(
                {},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
            )
        )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_unimplemented_credential_implementation_fails(self):
        # Only 'local' (profile-based) acquisition is built; 'ci'
        # (AssumeRoleWithWebIdentity — planned rename: web_identity) is declared and
        # validated in cfg but not implemented, so it must fail explicitly.
        with self.assertRaisesRegex(RuntimeError, "is not implemented"):
            aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_deploy"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="ci",
                account_registry=self.account_registry,
                execution_access_mode="agreed_direct",
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
        target_runs = {
            "deploy": {"execution_identity": self.executions["env_deploy"]},
            "readonly": {"execution_identity": self.executions["env_readonly"]},
        }
        with mock.patch.object(
            aws_adapter,
            "resolve_configured_profile_account_id",
            side_effect=account_ids.__getitem__,
        ):
            with self.assertRaisesRegex(RuntimeError, "AWS account registry maps"):
                aws_adapter.validate_active_target_run_aws_access(
                    target_runs,
                    self.identities,
                    self.credential_sources,
                    execution_context=self.context,
                    implementation_key="profile",
                    account_registry=self.account_registry,
                    execution_access_mode="agreed_direct",
                )

    # --- chain mode (default) ---

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_derives_runner_and_target_role_arns(self):
        resolved = aws_adapter.resolve_target_aws_access(
            {"execution_identity": self.executions["env_deploy"]},
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="profile",
            account_registry=self.account_registry,
            ctl_role_chain=self.ctl_role_chain,
            target_roles=self.target_roles,
        )
        self.assertEqual(resolved["credential_provider_kind"], "role_chain")
        self.assertEqual(resolved["entry_profile_name"], "oxygen-ctl-entry")
        self.assertEqual(resolved["entry_permission_set_name"], "CtlEntryAccess")
        self.assertEqual(
            resolved["hop_role_arns"],
            [
                "arn:aws:iam::444444444444:role/oxygen-ctl-runner",
                "arn:aws:iam::111111111111:role/oxygen-ctl-target_run",
            ],
        )
        self.assertEqual(resolved["role_name"], "oxygen-ctl-target_run")
        self.assertEqual(resolved["expected_account_id"], "111111111111")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_identity_target_role_override(self):
        resolved = aws_adapter.resolve_target_aws_access(
            {
                "execution_identity": {
                    "provider": "aws",
                    "account": "management",
                    "roles": {"readwrite": "org_ctl_member_account_roles"},
                }
            },
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="profile",
            account_registry=self.account_registry,
            ctl_role_chain=self.ctl_role_chain,
            target_roles=self.target_roles,
        )
        self.assertEqual(
            resolved["hop_role_arns"][-1],
            "arn:aws:iam::333333333333:role/oxygen-ctl-target_run-org-member-account-roles",
        )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_requires_ctl_target_role_key(self):
        # Phase 15: a target identity without ctl_target_role_key is invalid in chain mode
        with self.assertRaisesRegex(RuntimeError, "declares no roles.readwrite"):
            aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_no_target_role"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
                ctl_role_chain=self.ctl_role_chain,
                target_roles=self.target_roles,
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_chain_mode_requires_ctl_role_chain_cfg(self):
        with self.assertRaisesRegex(RuntimeError, "require providers.aws.ctl_role_chain"):
            aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_deploy"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
            )

    # --- identity bypass (--force-skip-execution-identity) ---

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_identity_bypass_uses_substitute_credential_even_with_identity(self):
        resolved = aws_adapter.resolve_target_aws_access(
            {"execution_identity": self.executions["env_deploy"]},
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="profile",
            execution_access_mode="force_bypass",
            provider_options={"force_bypass_credential_profile": "my-sandbox-admin"},
        )
        self.assertEqual(resolved["profile_name"], "my-sandbox-admin")
        self.assertEqual(resolved["credential_provider_kind"], "substitute_credential")
        self.assertEqual(resolved["identity_bypass"], "true")

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_identity_bypass_requires_substitute_credential(self):
        with self.assertRaisesRegex(RuntimeError, "requires the substitute credential"):
            aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_deploy"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                execution_access_mode="force_bypass",
            )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_identity_bypass_configures_target_env_without_checks(self):
        # inherited ambient credentials are scrubbed; the box gets ONLY the
        # substitute profile's host-resolved env credentials
        target_env = {"AWS_ACCESS_KEY_ID": "inherited-ambient"}
        with mock.patch.object(
            aws_adapter,
            "export_profile_credentials",
            return_value={"AWS_ACCESS_KEY_ID": "AKIASUB", "AWS_SECRET_ACCESS_KEY": "s"},
        ) as export:
            aws_adapter.configure_target_aws_env(
                "test",
                {"execution_identity": self.executions["env_deploy"]},
                target_env,
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry={},
                execution_access_mode="force_bypass",
                provider_options={"force_bypass_credential_profile": "my-sandbox-admin"},
            )
        export.assert_called_once_with("my-sandbox-admin")
        self.assertEqual(target_env["AWS_ACCESS_KEY_ID"], "AKIASUB")
        self.assertEqual(target_env["ATLAS_AWS_PROFILE_ONLY_ACCESS"], "true")
        self.assertNotIn("ATLAS_AWS_EXPECT_ACCOUNT_ID", target_env)
        self.assertNotIn("AWS_PROFILE", target_env)

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
        target_env: dict[str, str] = {}
        with mock.patch.object(
            aws_adapter,
            "resolve_configured_profile_account_id",
            return_value="111111111111",
        ), mock.patch.object(
            aws_adapter,
            "export_profile_credentials",
            return_value={"AWS_ACCESS_KEY_ID": "AKIADIR", "AWS_SECRET_ACCESS_KEY": "s"},
        ):
            aws_adapter.configure_target_aws_env(
                "target_run",
                {"execution_identity": self.executions["env_deploy"]},
                target_env,
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
                execution_access_mode="agreed_direct",
            )
        self.assertEqual(target_env["AWS_ACCESS_KEY_ID"], "AKIADIR")
        self.assertNotIn("AWS_PROFILE", target_env)
        self.assertEqual(target_env["ATLAS_AWS_EXPECT_ACCOUNT_ID"], "111111111111")
        self.assertEqual(target_env["ATLAS_AWS_EXPECT_PERMISSION_SET_NAME"], "NonProdDeployAccess")

    def test_direct_expect_account_key_conflict_is_rejected(self):
        sources = dict(self.credential_sources)
        sources["org_admin"] = {
            "profile": {
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
                aws_adapter.resolve_target_aws_access(
                    {"execution_identity": self.executions["org_admin"]},
                    self.identities,
                    sources,
                    execution_context=self.context,
                    implementation_key="profile",
                    account_registry=self.account_registry,
                    execution_access_mode="agreed_direct",
                )

    def test_chain_blocks_placeholder_entry_account(self):
        account_registry = dict(self.account_registry)
        account_registry["ctl_plane"] = "<live-ctl-plane-account-id>"
        with self.assertRaisesRegex(
            common.ProviderConfigBlockedError,
            r"accounts_registry\.ctl_plane\.account_id must be a 12-digit account id",
        ):
            aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_deploy"]},
                self.identities,
                self.credential_sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=account_registry,
                ctl_role_chain=self.ctl_role_chain,
                target_roles=self.target_roles,
                validate_local_credential=False,
            )

    def test_chain_resolution_requires_entry_account_key(self):
        sources = dict(self.credential_sources)
        sources["ctl_entry"] = {
            "profile": {
                "profile_name": "oxygen-ctl-entry",
                "expect": {"permission_set_name": "CtlEntryAccess"},
            }
        }
        with self.assertRaisesRegex(RuntimeError, r"must declare\s+expect.account_key"):
            aws_adapter.resolve_target_aws_access(
                {"execution_identity": self.executions["env_deploy"]},
                self.identities,
                sources,
                execution_context=self.context,
                implementation_key="profile",
                account_registry=self.account_registry,
                ctl_role_chain=self.ctl_role_chain,
                target_roles=self.target_roles,
            )

    def test_entry_validation_is_exact_not_substring(self):
        # a caller whose role EMBEDS the expected set name as a substring must fail
        caller = {
            "Account": "444444444444",
            "Arn": "arn:aws:sts::444444444444:assumed-role/"
                   "AWSReservedSSO_CtlEntryAccess-evil_abc123/session",
        }
        with self.assertRaisesRegex(RuntimeError, "permission-set mismatch"):
            assert_aws_access.validate_caller_identity(
                caller,
                expected_account_id="444444444444",
                expected_permission_set_name="CtlEntryAccess",
            )

    # --- identity coverage ---

    def test_identity_coverage_rejects_missing_identity_without_bypass(self):
        target_runs = {"declared": {"execution_identity": self.executions["env_deploy"]}, "bare": {}}
        with self.assertRaisesRegex(RuntimeError, "have no execution_identity block"):
            common.validate_target_execution_identity_coverage(
                target_runs, execution_access_modes={"aws": "standard"}
            )

    def test_identity_coverage_allows_anything_under_bypass(self):
        target_runs = {"declared": {"execution_identity": self.executions["env_deploy"]}, "bare": {}}
        common.validate_target_execution_identity_coverage(
            target_runs, execution_access_modes={"aws": "force_bypass"}
        )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_non_aws_target_drops_inherited_credentials(self):
        target_env = {
            "AWS_PROFILE": "wrong",
            "AWS_DEFAULT_PROFILE": "wrong-default",
            "AWS_CONFIG_FILE": "/tmp/wrong-config",
            "AWS_SHARED_CREDENTIALS_FILE": "/tmp/wrong-credentials",
            "AWS_ACCESS_KEY_ID": "secret",
        }
        aws_adapter.configure_target_aws_env(
            "test",
            {},
            target_env,
            self.identities,
            self.credential_sources,
            execution_context=self.context,
            implementation_key="profile",
            account_registry={},
        )
        self.assertNotIn("AWS_PROFILE", target_env)
        self.assertNotIn("AWS_DEFAULT_PROFILE", target_env)
        self.assertNotIn("AWS_CONFIG_FILE", target_env)
        self.assertNotIn("AWS_SHARED_CREDENTIALS_FILE", target_env)
        self.assertNotIn("AWS_ACCESS_KEY_ID", target_env)

    def test_synchronizer_asserts_selected_profile_principal(self):
        with (
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
            credential = aws_adapter.resolve_ctl_state_credential(
                self.executions["ctl_state_dev_synchronizer"],
                Path("/cfg"),
                execution_context=self.context,
                implementation_key="profile",
                operation="sync",
                bucket_name="oxygen-live-ctl-state",
                execution_access_mode="agreed_direct",
            )

        self.assertEqual(credential, "oxygen-dev-ctl-state-synchronizer")
        assertion.assert_called_once_with(
            "oxygen-dev-ctl-state-synchronizer",
            expected_account_id="111111111111",
            expect_principal={"role_name": "oxygen-ctl-state-synchronizer"},
            label="ctl-state sync execution",
        )


class CallerIdentityAssertionTests(unittest.TestCase):
    @mock.patch.dict(
        os.environ,
        {
            "AWS_PROFILE": "private-profile",
            "ATLAS_AWS_PROFILE_ONLY_ACCESS": "true",
        },
        clear=True,
    )
    def test_profile_only_success_output_contains_no_identity_details(self):
        caller = {
            "Account": "111111111111",
            "Arn": "arn:aws:iam::111111111111:user/private-user",
        }
        with (
            mock.patch.object(
                assert_aws_access, "get_caller_identity", return_value=caller
            ),
            mock.patch("builtins.print") as output,
        ):
            self.assertEqual(assert_aws_access.main(), 0)

        output.assert_called_once_with("AWS access validation passed")
        rendered = str(output.call_args)
        self.assertNotIn("private-profile", rendered)
        self.assertNotIn("111111111111", rendered)
        self.assertNotIn("private-user", rendered)

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


class AccountExpectationCheckTests(unittest.TestCase):
    """§Phase 52: bypass has no role chain, so the account binding is asserted."""

    CTX = {"execution_context.params.aws.account": "dev"}
    REGISTRY = {"dev": "111111111111", "seam": "<live-seam-account-id>"}
    EXECUTION = {"provider": "aws", "account": "${execution_context.params.aws.account}"}

    def _check(self, caller_account, *, execution=None, registry=None, options=None):
        with mock.patch.object(
            aws_adapter, "cached_caller_identity",
            return_value={"Account": caller_account, "Arn": "arn:aws:iam::x:user/y"},
        ):
            return aws_adapter.check_account_expectation(
                self.EXECUTION if execution is None else execution,
                execution_context=self.CTX,
                account_registry=self.REGISTRY if registry is None else registry,
                profile_name="substitute",
                provider_options=options,
                label="target",
            )

    def test_matching_account_passes(self):
        self.assertEqual(self._check("111111111111")["status"], "passed")

    def test_wrong_account_is_a_hard_failure_naming_both_ids(self):
        result = self._check("999999999999")
        self.assertEqual(result["status"], "failed")
        self.assertIn("999999999999", result["failure_reason"])
        self.assertIn("111111111111", result["failure_reason"])

    def test_placeholder_registry_id_declares_no_expectation(self):
        result = self._check("999999999999",
                             execution={"provider": "aws", "account": "seam"})
        self.assertEqual(result["status"], "not_applicable")

    def test_unknown_account_key_declares_no_expectation(self):
        result = self._check("999999999999", registry={})
        self.assertEqual(result["status"], "not_applicable")

    def test_execution_less_target_declares_no_expectation(self):
        self.assertEqual(self._check("999999999999", execution=None if False else {})["status"],
                         "not_applicable")

    def test_force_skip_option_bypasses_it(self):
        result = self._check(
            "999999999999",
            options={"force_skip_account_expectation_check": "true"},
        )
        self.assertEqual(result["status"], "force_skipped")

    def test_caller_identity_is_cached_per_credential(self):
        aws_adapter._CALLER_IDENTITY_CACHE.clear()
        with mock.patch.object(
            aws_adapter, "_assertion",
            return_value=mock.Mock(get_caller_identity=mock.Mock(
                return_value={"Account": "111111111111"})),
        ) as assertion:
            aws_adapter.cached_caller_identity("substitute")
            aws_adapter.cached_caller_identity("substitute")
            calls = assertion.return_value.get_caller_identity.call_count
        aws_adapter._CALLER_IDENTITY_CACHE.clear()
        self.assertEqual(calls, 1)


if __name__ == "__main__":
    unittest.main()
