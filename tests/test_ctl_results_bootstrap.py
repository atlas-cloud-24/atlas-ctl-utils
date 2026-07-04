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
            (Path(tmp) / "ctl_results.yaml").write_text(body, encoding="utf-8")
            return common.load_ctl_results_cfg(Path(tmp))

    def test_identity_key_is_optional(self):
        cfg = self._load(
            "ctl_results:\n  backends:\n    env:\n      bucket_name: ${execution_context.params.main_tag}-x\n"
        )
        self.assertNotIn("execution_identity_key", cfg["env"])
        backend = common.resolve_ctl_results_backend(cfg, {"env_type": "dev"}, self.CTX)
        self.assertEqual(backend["bucket_name"], "oxygen-x")
        self.assertNotIn("execution_identity_key", backend)

    def test_identity_key_when_present_must_be_non_empty(self):
        with self.assertRaisesRegex(RuntimeError, "execution_identity_key must be a non-empty string"):
            self._load(
                "ctl_results:\n  backends:\n    env:\n      bucket_name: b\n      execution_identity_key: '  '\n"
            )


class ResultsBootstrapDetectionTests(unittest.TestCase):
    INVENTORY = {
        "stage_targets": {
            "env/ctl-results": {"sub_workflow": "env_ctl_results_bucket", "results_sync_bootstrap": True},
            "env/core/baseline": {"sub_workflow": "baseline"},
        }
    }

    def test_true_when_a_bootstrap_target_is_in_the_run(self):
        wf = {"stages": ["env/ctl-results"]}
        self.assertTrue(common.run_requests_results_bootstrap(wf, self.INVENTORY))

    def test_true_when_mixed_workflow_includes_a_bootstrap_target(self):
        wf = {"stages": ["env/core/baseline", {"target": "env/ctl-results"}]}
        self.assertTrue(common.run_requests_results_bootstrap(wf, self.INVENTORY))

    def test_false_for_a_normal_run(self):
        wf = {"stages": ["env/core/baseline"]}
        self.assertFalse(common.run_requests_results_bootstrap(wf, self.INVENTORY))

    def test_false_when_target_missing_or_no_flag(self):
        self.assertFalse(common.run_requests_results_bootstrap({"stages": ["unknown"]}, self.INVENTORY))
        self.assertFalse(common.run_requests_results_bootstrap({"stages": []}, self.INVENTORY))


if __name__ == "__main__":
    unittest.main()
