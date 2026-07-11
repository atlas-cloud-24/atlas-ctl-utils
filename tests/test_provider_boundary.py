"""Engine-core provider-boundary tests (Phase 13).

The engine core (runners/utils/common.py and the engine cfg tools) must carry
no AWS vocabulary: no provider-named CLI arguments, field validation, branches,
ARN construction, subprocess invocations, stage env handling, or user-facing
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
            "validate_execution_identity",
            "load_runtime_catalogs",
            "validate_active_stage_access",
            "materialize_stage_binding",
            "stage_assertion_argv",
            "validate_state_backend_entry",
            "resolve_synchronizer_credential",
            "create_state_syncer",
            "derive_provider_facts",
            "synthesize_validation_provider_facts",
            "normalize_provider_credential",
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
            "stage_roles": {},
        }
        stages = {"stage": {}}
        aws_adapter.validate_active_stage_access(
            stages,
            catalogs,
            execution_context={},
            implementation_key="local",
            execution_access_mode="bypass",
            provider_credential="substitute",
        )
        stage_env: dict[str, str] = {}
        aws_adapter.materialize_stage_binding(
            "stage",
            {},
            stage_env,
            catalogs,
            execution_context={},
            implementation_key="local",
            execution_access_mode="bypass",
            provider_credential="substitute",
        )
        self.assertEqual(stage_env.get("AWS_PROFILE"), "substitute")


class CtlRoleChainLoaderTests(unittest.TestCase):
    def test_rejects_removed_stage_role_key(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "providers" / "aws").mkdir(parents=True)
            (root / "providers" / "aws" / "ctl_role_chain.yaml").write_text(
                "providers:\n  aws:\n    ctl_role_chain:\n"
                "      entry_credential_source_key: ctl_entry\n"
                "      runner_role_key: ctl_runner\n"
                "      stage_role_key: ctl_stage\n"
            )
            with self.assertRaisesRegex(RuntimeError, "stage_role_key is removed"):
                aws_adapter.load_aws_ctl_role_chain_cfg(root)


class ProfileBindingTests(unittest.TestCase):
    def test_binding_holds_only_the_selected_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config = tmp_path / "config"
            config.write_text(
                "[profile wanted]\n"
                "sso_session = corp\n"
                "sso_account_id = 111111111111\n"
                "[sso-session corp]\n"
                "sso_start_url = https://example.awsapps.com/start\n"
                "[profile other]\n"
                "sso_account_id = 222222222222\n"
            )
            credentials = tmp_path / "credentials"
            credentials.write_text(
                "[wanted]\naws_access_key_id = AKIAWANTED\n[other]\naws_access_key_id = AKIAOTHER\n"
            )
            binding_dir = tmp_path / "binding"
            stage_env: dict[str, str] = {}
            env_override = {
                "AWS_CONFIG_FILE": str(config),
                "AWS_SHARED_CREDENTIALS_FILE": str(credentials),
            }
            original = {key: os.environ.get(key) for key in env_override}
            os.environ.update(env_override)
            try:
                aws_adapter.materialize_profile_binding(binding_dir, "wanted", stage_env)
            finally:
                for key, value in original.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

            generated_config = (binding_dir / "config").read_text()
            generated_credentials = (binding_dir / "credentials").read_text()
            self.assertIn("[profile wanted]", generated_config)
            self.assertIn("[sso-session corp]", generated_config)
            self.assertNotIn("other", generated_config)
            self.assertIn("[wanted]", generated_credentials)
            self.assertNotIn("AKIAOTHER", generated_credentials)
            self.assertEqual(stage_env["ATLAS_PROVIDER_BINDING_DIR"], str(binding_dir))


if __name__ == "__main__":
    unittest.main()


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
                "  strict: { ref_policy: commit_required }\n"
                "  boot: { ref_policy: commit_required, allowed_execution_access_modes: [standard, direct] }\n"
            )
            self.assertEqual(common.ctl_allowed_execution_access_modes(root, "strict"), {"standard"})
            self.assertEqual(common.ctl_allowed_execution_access_modes(root, "boot"), {"standard", "direct"})

    def test_target_direct_access_default_and_validate(self):
        self.assertFalse(common.target_allows_direct_execution_access({}))
        self.assertTrue(
            common.target_allows_direct_execution_access({"allow_direct_execution_access": True})
        )
        self.assertFalse(
            common.target_allows_direct_execution_access({"allow_direct_execution_access": False})
        )
        with self.assertRaisesRegex(RuntimeError, "must be a boolean"):
            common.target_allows_direct_execution_access({"allow_direct_execution_access": ["direct"]})
