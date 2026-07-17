"""Execution-identity group tests (Phase 10).

A group entry declares its provider and lists concrete members guarded by the
unified selectors schema; the engine resolves EXACTLY ONE concrete member per
run context. Replaces name-templated execution_identity_key. Groups are
provider-homogeneous and validated at load.
"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
from utils import common  # noqa: E402


def _identities():
    return {
        "env_dev_deploy": {"provider": "aws", "account_key": "dev", "direct_credential_source_key": "np"},
        "env_prod_deploy": {"provider": "aws", "account_key": "prod", "direct_credential_source_key": "p"},
        "env_deploy": {
            "provider": "aws",
            "members": [
                {"identity_key": "env_dev_deploy", "selectors": {"match": {"execution_context.params.account": "dev"}}},
                {"identity_key": "env_prod_deploy", "selectors": {"match": {"execution_context.params.account": "prod"}}},
            ],
        },
    }


class IdentityGroupResolveTests(unittest.TestCase):
    def test_group_resolves_per_context(self):
        ids = _identities()
        k, cfg = common.resolve_execution_identity_entry(
            ids, "env_deploy",
            {"execution_context.params.provider": "aws", "execution_context.params.account": "dev"},
        )
        self.assertEqual(k, "env_dev_deploy")
        self.assertEqual(cfg["account_key"], "dev")

    def test_concrete_passthrough(self):
        ids = _identities()
        k, cfg = common.resolve_execution_identity_entry(ids, "env_dev_deploy", {})
        self.assertEqual((k, cfg["account_key"]), ("env_dev_deploy", "dev"))

    def test_no_member_matches(self):
        ids = _identities()
        with self.assertRaisesRegex(RuntimeError, "exactly one member must match"):
            common.resolve_execution_identity_entry(
                ids, "env_deploy",
                {"execution_context.params.provider": "aws", "execution_context.params.account": "sandbox"},
            )

    def test_wrong_provider_fails_before_matching(self):
        ids = _identities()
        with self.assertRaisesRegex(RuntimeError, "is provider 'aws', but the run provider is 'gcp'"):
            common.resolve_execution_identity_entry(
                ids, "env_deploy",
                {"execution_context.params.provider": "gcp", "execution_context.params.account": "dev"},
            )


class IdentityGroupLoadValidationTests(unittest.TestCase):
    def _load(self, body: str):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "execution_identities.yaml").write_text(body)
            return common.load_execution_identities_cfg(root)

    def test_valid_group_loads(self):
        ids = self._load(
            "execution_identities:\n"
            "  c_dev: { provider: aws, account_key: dev, ctl_stage_role_key: r, direct_credential_source_key: s }\n"
            "  g:\n"
            "    provider: aws\n"
            "    members:\n"
            "      - { identity_key: c_dev, selectors: { match: { execution_context.params.account: dev } } }\n"
        )
        self.assertTrue(common.execution_identity_is_group(ids["g"]))

    def test_provider_heterogeneous_group_rejected(self):
        # validator in isolation (avoids the per-entry provider-adapter lookup)
        ids = {
            "c_gcp": {"provider": "gcp", "account_key": "dev"},
            "g": {"provider": "aws", "members": [
                {"identity_key": "c_gcp", "selectors": {"match": {"execution_context.params.account": "dev"}}}]},
        }
        with self.assertRaisesRegex(RuntimeError, "provider-homogeneous"):
            common._validate_execution_identity_group("g", ids["g"], ids, Path("/x"))

    def test_member_must_reference_concrete(self):
        with self.assertRaisesRegex(RuntimeError, "not defined|concrete identity"):
            self._load(
                "execution_identities:\n"
                "  g:\n"
                "    provider: aws\n"
                "    members:\n"
                "      - { identity_key: missing, selectors: { match: { execution_context.params.account: dev } } }\n"
            )


if __name__ == "__main__":
    unittest.main()
