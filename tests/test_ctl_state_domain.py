import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


class OperationIdentityTests(unittest.TestCase):
    def _load(self, body: str) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "ctl_state.yaml").write_text(body, encoding="utf-8")
            return common.load_ctl_state_backends_cfg(Path(tmp))

    def test_operation_identities_are_optional_for_structural_loading(self):
        cfg = self._load(
            "ctl_state_backends:\n  env:\n    provider: aws\n    backend_type: s3\n    bucket_name: b\n    bucket_region: eu-central-1\n"
        )
        self.assertNotIn("execution", cfg["env"])

    def test_legacy_single_identity_key_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "unsupported keys"):
            self._load(
                "ctl_state_backends:\n  env:\n    provider: aws\n    backend_type: s3\n    bucket_name: b\n    bucket_region: r\n    execution_identity_key: old\n"
            )

    def test_operation_identity_must_be_non_empty(self):
        with self.assertRaisesRegex(
            RuntimeError, "operations.sync.role must be a non-empty string"
        ):
            self._load(
                "ctl_state_backends:\n  env:\n    provider: aws\n    backend_type: s3\n    bucket_name: b\n    bucket_region: r\n    execution_identity:\n      account: ctl_plane\n      operations:\n        sync:\n          role: '  '\n"
            )


class ResultsBootstrapDetectionTests(unittest.TestCase):
    INVENTORY = {
        "targets": {
            "env/ctl-state-backend": {"step_sequence": "env_ctl_state_backend", "provisions_ctl_state_backend": True},
            "env/core/baseline": {"step_sequence": "baseline"},
        }
    }

    def test_true_when_a_bootstrap_target_is_in_the_run(self):
        wf = {"target_runs": ["env/ctl-state-backend"]}
        self.assertTrue(common.run_provisions_ctl_state_backend(wf, self.INVENTORY))

    def test_true_when_mixed_workflow_includes_a_bootstrap_target(self):
        wf = {"target_runs": ["env/core/baseline", {"target": "env/ctl-state-backend"}]}
        self.assertTrue(common.run_provisions_ctl_state_backend(wf, self.INVENTORY))

    def test_false_for_a_normal_run(self):
        wf = {"target_runs": ["env/core/baseline"]}
        self.assertFalse(common.run_provisions_ctl_state_backend(wf, self.INVENTORY))

    def test_false_when_target_missing_or_no_flag(self):
        self.assertFalse(common.run_provisions_ctl_state_backend({"target_runs": ["unknown"]}, self.INVENTORY))
        self.assertFalse(common.run_provisions_ctl_state_backend({"target_runs": []}, self.INVENTORY))


class RequiredTargetPathsTests(unittest.TestCase):
    def test_collects_top_level_paths_from_cfg_roots(self):
        target_runs = {
            "a": {"cfg_root": "/env"},
            "b": {"cfg_root": "/env/sub"},
            "c": {"cfg_root": "/org"},
        }
        self.assertEqual(common.required_target_paths_for_target_runs(target_runs), {"/env", "/org"})

    def test_root_cfg_root_means_all_scopes(self):
        target_runs = {"a": {"cfg_root": "/env"}, "b": {"cfg_root": "/"}}
        self.assertIsNone(common.required_target_paths_for_target_runs(target_runs))

    def test_missing_cfg_root_defaults_to_all(self):
        self.assertIsNone(common.required_target_paths_for_target_runs({"a": {}}))




if __name__ == "__main__":
    unittest.main()
