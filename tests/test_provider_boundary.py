"""Engine-core provider-boundary tests (Phase 13).

The engine core (runners/utils/common.py and the engine cfg tools) must carry
no AWS vocabulary: no provider-named CLI arguments, field validation, branches,
ARN construction, subprocess invocations, target_run env handling, or user-facing
errors. AWS lives only in utils/providers/aws.py, its tests, providers.aws.*
cfg, and labelled documentation examples.
"""
import os
import re
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
from utils import common  # noqa: E402
from utils.providers import aws as aws_adapter  # noqa: E402
from utils.providers import get_adapter  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE_CORE_FILES = (
    REPO_ROOT / "runners" / "utils" / "common.py",
    REPO_ROOT / "cfg" / "validate_cfg.py",
    REPO_ROOT / "cfg" / "regenerate_guardrails.py",
)
FORBIDDEN = re.compile(r"(?i)(\baws\b|aws_|_aws|-aws|arn:|s3://|\bsts\b|\bboto)")
# §12: the AWS-implementation term ctl_role_chain must not leak into engine-core
FORBIDDEN_PUBLIC = re.compile(r"(ctl_role_chain|role.chain|skip_ctl_role_chain)")


class ProviderBoundaryTests(unittest.TestCase):
    def test_engine_core_has_no_provider_tokens(self):
        for path in ENGINE_CORE_FILES:
            text = path.read_text()
            hits = [
                f"{path.name}:{number}: {line.strip()}"
                for number, line in enumerate(text.splitlines(), start=1)
                if FORBIDDEN.search(line)
            ]
            self.assertEqual(hits, [], "engine-core provider tokens:\n" + "\n".join(hits))
            public_hits = [
                f"{path.name}:{number}: {line.strip()}"
                for number, line in enumerate(text.splitlines(), start=1)
                if FORBIDDEN_PUBLIC.search(line) and "removed" not in line
            ]
            self.assertEqual(public_hits, [], "engine-core role-chain leakage:\n" + "\n".join(public_hits))

    def test_unknown_provider_is_a_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "no provider adapter registered"):
            get_adapter("gcp")

    def test_adapter_contract_operations_exist(self):
        for operation in (
            "validate_catalog",
            "validate_target_execution_identity",
            "describe",
            "supported_execution_access_modes",
            "supports_identity_preflight",
            "validate_provider_options",
            "validate_profile_policy",
            "authorize_run",
            "load_runtime_catalogs",
            "collect_provider_cfg_findings",
            "resolve_target_cfg_references",
            "validate_active_target_access",
            "preflight_execution_identity",
            "materialize_target_binding",
            "target_assertion_argv",
            "validate_state_backend_entry",
            "resolve_ctl_state_credential",
            "create_state_syncer",
            "normal_execution_access_mode",
            "resolves_execution_identity",
            "target_consent",
            "execution_access_mode_from_options",
        ):
            self.assertTrue(callable(getattr(aws_adapter, operation, None)), operation)


class ContractWrapperTests(unittest.TestCase):
    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_validate_and_bind_wrappers_run_in_bypass_mode(self):
        catalogs = {
            "execution_identities": {},
            "credential_sources": {},
            "account_registry": {},
            "ctl_role_chain": None,
            "target_roles": {},
        }
        target_runs = {"target_run": {}}
        aws_adapter.validate_active_target_access(
            target_runs,
            catalogs,
            execution_context={},
            implementation_key="profile",
            execution_access_mode="force_bypass",
            provider_options={"force_bypass_credential_profile": "substitute"},
        )
        target_env: dict[str, str] = {}
        with unittest.mock.patch.object(
            aws_adapter,
            "export_profile_credentials",
            return_value={"AWS_ACCESS_KEY_ID": "AKIA", "AWS_SECRET_ACCESS_KEY": "s"},
        ) as export:
            aws_adapter.materialize_target_binding(
                "target_run",
                {},
                target_env,
                catalogs,
                execution_context={},
                implementation_key="profile",
                execution_access_mode="force_bypass",
                provider_options={"force_bypass_credential_profile": "substitute"},
            )
        export.assert_called_once_with("substitute")
        self.assertEqual(target_env.get("AWS_ACCESS_KEY_ID"), "AKIA")
        self.assertNotIn("AWS_PROFILE", target_env)


class CtlRoleChainLoaderTests(unittest.TestCase):
    def test_rejects_removed_target_role_key(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "providers" / "aws").mkdir(parents=True)
            (root / "providers" / "aws" / "ctl_role_chain.yaml").write_text(
                "providers:\n  aws:\n    ctl_role_chain:\n"
                "      entry_credential_source_key: ctl_entry\n"
                "      runner_role_key: ctl_runner\n"
                "      target_role_key: ctl_target\n"
            )
            with self.assertRaisesRegex(RuntimeError, "target_role_key is removed"):
                aws_adapter.load_aws_ctl_role_chain_cfg(root)


class ProfileBindingTests(unittest.TestCase):
    def test_profile_resolves_to_env_credentials_only(self):
        # The host resolves the profile (`aws configure export-credentials`);
        # the box receives plain env credentials — no file is ever written.
        with unittest.mock.patch.object(
            aws_adapter,
            "_run_aws_json",
            return_value={
                "AccessKeyId": "AKIAWANTED",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            },
        ) as run_json:
            credentials = aws_adapter.export_profile_credentials("wanted")
        self.assertEqual(
            credentials,
            {
                "AWS_ACCESS_KEY_ID": "AKIAWANTED",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_SESSION_TOKEN": "token",
            },
        )
        self.assertIn("--profile", run_json.call_args.args[0])

    def test_incomplete_export_fails_loud(self):
        with unittest.mock.patch.object(
            aws_adapter, "_run_aws_json", return_value={"AccessKeyId": "AKIA"}
        ):
            with self.assertRaisesRegex(RuntimeError, "SecretAccessKey"):
                aws_adapter.export_profile_credentials("wanted")



class CredentialPathIteratorTests(unittest.TestCase):
    """§12.3: the AWS credential-path executor makes no assumption about the
    number of hops (production = 2; the iterator supports 1/2/3 and rejects
    cyclic/empty paths)."""

    def test_validate_rejects_empty_and_cyclic(self):
        with self.assertRaisesRegex(RuntimeError, "no role hops"):
            aws_adapter.validate_credential_path([])
        with self.assertRaisesRegex(RuntimeError, "repeats a role ARN"):
            aws_adapter.validate_credential_path([
                "arn:aws:iam::111111111111:role/a",
                "arn:aws:iam::111111111111:role/a",
            ])

    def test_iterator_supports_one_two_three_hops(self):
        seen = []

        def fake_run(cmd, capture_output, text, env):
            import types
            if "get-caller-identity" in cmd:
                out = '{"Account": "111111111111", "Arn": "arn:aws:sts::111111111111:assumed-role/Entry/s"}'
            else:
                # record the assumed role arn
                seen.append(cmd[cmd.index("--role-arn") + 1])
                out = '{"Credentials": {"AccessKeyId": "AK", "SecretAccessKey": "SK", "SessionToken": "ST"}}'
            return types.SimpleNamespace(returncode=0, stdout=out, stderr="")

        for hops in (
            ["arn:aws:iam::1:role/one"],
            ["arn:aws:iam::1:role/one", "arn:aws:iam::2:role/two"],
            ["arn:aws:iam::1:role/one", "arn:aws:iam::2:role/two", "arn:aws:iam::3:role/three"],
        ):
            seen.clear()
            with unittest.mock.patch("subprocess.run", side_effect=fake_run):
                creds = aws_adapter.assume_ctl_role_chain(
                    "entry-profile", hops,
                    session_name="s", entry_expected_account_id="111111111111",
                    entry_role_name="Entry",
                )
            self.assertEqual(seen, hops)  # every hop assumed, in order
            self.assertEqual(creds["AWS_ACCESS_KEY_ID"], "AK")


class ExecutionAccessModeTests(unittest.TestCase):
    def test_profile_modes_default_and_gate(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  strict:\n    ref_policy: commit_required\n"
                "  boot:\n    ref_policy: commit_required\n    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [standard, agreed_direct]\n"
                "      allowed_credential_implementation: [profile]\n"
            )
            # provider policy is DECLARED: no allowed_providers is a hard error
            with self.assertRaisesRegex(RuntimeError, "must declare allowed_providers"):
                common.ctl_allowed_providers(root, "strict")
            self.assertEqual(common.ctl_allowed_providers(root, "boot"), ["aws"])
            # the block is opaque to the engine; the adapter reads it
            policy = common.ctl_profile_provider_policy(root, "boot", "aws")
            self.assertEqual(
                policy["allowed_execution_access_modes"], ["standard", "agreed_direct"]
            )
            aws_adapter.authorize_run(
                policy, execution_access_mode="standard",
                provider_options={"credential_implementation": "profile"}, label="p",
            )
            with self.assertRaisesRegex(RuntimeError, "is not allowed by"):
                aws_adapter.authorize_run(
                    policy, execution_access_mode="force_bypass",
                    provider_options={}, label="p",
                )

    def test_mode_consent_is_the_adapters_answer(self):
        # WHICH modes need per-target consent, and in WHICH field, is the
        # adapter's call — the engine only asks.
        self.assertEqual(
            aws_adapter.target_consent("agreed_direct"),
            {
                "opt_in_field": "allow_agreed_direct_execution_access",
                "execution_field": "agreed_direct_credential_source_keys",
            },
        )
        self.assertIsNone(aws_adapter.target_consent("standard"))
        self.assertIsNone(aws_adapter.target_consent("force_bypass"))

    def test_consent_needs_both_the_opt_in_and_the_sources(self):
        # declaring the sources is NOT opting in: a target may name sources it
        # uses elsewhere while withholding consent to be run this way.
        import tempfile

        workflow = {"target_runs": ["target"]}
        execution = {
            "provider": "aws",
            "agreed_direct_credential_source_keys": ["admin"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n  boot:\n    ref_policy: commit_required\n"
                "    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [agreed_direct]\n"
                "      allowed_credential_implementation: [profile]\n"
            )

            def check(target_cfg):
                common.validate_execution_access(
                    root,
                    "boot",
                    workflow,
                    {"targets": {"target": target_cfg}},
                    execution_context={},
                    execution_access_modes={"aws": "agreed_direct"},
                    agreed_defer_ctl_state_backend_sync=False,
                    force_skip_ctl_state_backend_sync=False,
                    provider_options={},
                )

            with self.assertRaisesRegex(RuntimeError, "allow_agreed_direct_execution_access"):
                check({"execution_identity": execution})
            with self.assertRaisesRegex(RuntimeError, "agreed_direct_credential_source_keys"):
                check({
                    "allow_agreed_direct_execution_access": True,
                    "execution_identity": {"provider": "aws"},
                })
            check({
                "allow_agreed_direct_execution_access": True,
                "execution_identity": execution,
            })

    def test_modes_that_resolve_no_identity_are_declared(self):
        self.assertFalse(aws_adapter.resolves_execution_identity("force_bypass"))
        self.assertTrue(aws_adapter.resolves_execution_identity("standard"))
        self.assertEqual(aws_adapter.normal_execution_access_mode(), "standard")

    def test_credential_implementation_is_required(self):
        with self.assertRaisesRegex(RuntimeError, "aws.credential_implementation"):
            aws_adapter.validate_provider_options({})
        aws_adapter.validate_provider_options({"credential_implementation": "profile"})

    def test_option_grants_are_enforced_from_the_provider_block(self):
        policy = {
            "allowed_execution_access_modes": ["force_bypass"],
            "allowed_credential_implementation": ["profile"],
        }
        opts = {"force_skip_account_expectation_check": "true"}
        with self.assertRaisesRegex(RuntimeError, "allow_force_skip_account_expectation_check"):
            aws_adapter.authorize_run(policy, execution_access_mode="force_bypass",
                                      provider_options=opts, label="p")
        granted = dict(policy, allow_force_skip_account_expectation_check=True)
        aws_adapter.authorize_run(granted, execution_access_mode="force_bypass",
                                  provider_options=opts, label="p")
        # a credential implementation the profile does not allow is refused
        with self.assertRaisesRegex(RuntimeError, "credential implementation"):
            aws_adapter.authorize_run(
                granted, execution_access_mode="force_bypass",
                provider_options={"credential_implementation": "web_identity"}, label="p")
        with self.assertRaisesRegex(RuntimeError, "must be 'true' or 'false'"):
            aws_adapter.validate_provider_options({
                "credential_implementation": "profile",
                "force_skip_account_expectation_check": "yes",
            })

    def test_options_may_imply_a_mode(self):
        self.assertEqual(
            aws_adapter.execution_access_mode_from_options(
                {"force_bypass_credential_profile": "dev"}
            ),
            "force_bypass",
        )
        self.assertIsNone(aws_adapter.execution_access_mode_from_options({}))
