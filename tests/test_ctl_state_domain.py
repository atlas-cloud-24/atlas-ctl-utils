import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


class OptionalWriterIdentityTests(unittest.TestCase):
    CTX = {"execution_context.params.main_tag": "oxygen", "execution_context.params.env_type": "dev"}

    def _load(self, body: str) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "ctl_state.yaml").write_text(body, encoding="utf-8")
            return common.load_ctl_state_backends_cfg(Path(tmp))

    def test_identity_key_is_optional(self):
        cfg = self._load(
            "ctl_state_backends:\n  env:\n    provider: aws\n    backend_type: s3\n    bucket_name: b\n    bucket_region: eu-central-1\n"
        )
        self.assertNotIn("execution_identity_key", cfg["env"])

    def test_identity_key_when_present_must_be_non_empty(self):
        with self.assertRaisesRegex(RuntimeError, "execution_identity_key must be a non-empty string"):
            self._load(
                "ctl_state_backends:\n  env:\n    provider: aws\n    backend_type: s3\n    bucket_name: b\n    bucket_region: r\n    execution_identity_key: '  '\n"
            )


class ResultsBootstrapDetectionTests(unittest.TestCase):
    INVENTORY = {
        "stage_targets": {
            "env/ctl-state": {"sub_workflow": "env_ctl_state_bucket", "provisions_ctl_state_bucket": True},
            "env/core/baseline": {"sub_workflow": "baseline"},
        }
    }

    def test_true_when_a_bootstrap_target_is_in_the_run(self):
        wf = {"stages": ["env/ctl-state"]}
        self.assertTrue(common.run_provisions_ctl_state_bucket(wf, self.INVENTORY))

    def test_true_when_mixed_workflow_includes_a_bootstrap_target(self):
        wf = {"stages": ["env/core/baseline", {"target": "env/ctl-state"}]}
        self.assertTrue(common.run_provisions_ctl_state_bucket(wf, self.INVENTORY))

    def test_false_for_a_normal_run(self):
        wf = {"stages": ["env/core/baseline"]}
        self.assertFalse(common.run_provisions_ctl_state_bucket(wf, self.INVENTORY))

    def test_false_when_target_missing_or_no_flag(self):
        self.assertFalse(common.run_provisions_ctl_state_bucket({"stages": ["unknown"]}, self.INVENTORY))
        self.assertFalse(common.run_provisions_ctl_state_bucket({"stages": []}, self.INVENTORY))


class RequiredTargetPathsTests(unittest.TestCase):
    def test_collects_top_level_paths_from_cfg_roots(self):
        stages = {
            "a": {"cfg_root": "/env"},
            "b": {"cfg_root": "/env/sub"},
            "c": {"cfg_root": "/org"},
        }
        self.assertEqual(common.required_target_paths_for_stages(stages), {"/env", "/org"})

    def test_root_cfg_root_means_all_scopes(self):
        stages = {"a": {"cfg_root": "/env"}, "b": {"cfg_root": "/"}}
        self.assertIsNone(common.required_target_paths_for_stages(stages))

    def test_missing_cfg_root_defaults_to_all(self):
        self.assertIsNone(common.required_target_paths_for_stages({"a": {}}))


class ResolveRunDomainTests(unittest.TestCase):
    INVENTORY = {
        "stage_targets": {
            "env/core/baseline": {"ctl_state_backend_key": "env"},
            "env/ops/app": {"ctl_state_backend_key": "env"},
            "org/baseline": {"ctl_state_backend_key": "deployments"},
            "dev/local": {},  # key commented out (skip hatch)
        }
    }
    REGISTRY = {"env": {}, "deployments": {}}

    def test_single_domain(self):
        wf = {"stages": ["env/core/baseline", "env/ops/app"]}
        self.assertEqual(common.resolve_run_domain(wf, self.INVENTORY, self.REGISTRY), "env")

    def test_none_when_all_absent(self):
        wf = {"stages": ["dev/local"]}
        self.assertIsNone(common.resolve_run_domain(wf, self.INVENTORY, self.REGISTRY))

    def test_mixed_domains_is_hard_error(self):
        wf = {"stages": ["env/core/baseline", "org/baseline"]}
        with self.assertRaisesRegex(RuntimeError, "spans multiple ctl-state domains"):
            common.resolve_run_domain(wf, self.INVENTORY, self.REGISTRY)

    def test_unknown_domain_rejected(self):
        inv = {"stage_targets": {"t": {"ctl_state_backend_key": "staging"}}}
        with self.assertRaisesRegex(RuntimeError, "no ctl_state_backends entry"):
            common.resolve_run_domain({"stages": ["t"]}, inv, self.REGISTRY)


if __name__ == "__main__":
    unittest.main()
