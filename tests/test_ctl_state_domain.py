import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine_surface import engine_defines
from engine.run import selectors as run_selectors
from engine.state import sync as state_sync


class OperationIdentityTests(unittest.TestCase):
    def _load(self, body: str) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "ctl_state.yaml").write_text(body, encoding="utf-8")
            return state_sync.CtlStateBackends.load(Path(tmp))

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
            "env/ctl-state-backend": {"procedure": "env_ctl_state_backend", "provisions_ctl_state_backend": True},
            "env/core/baseline": {"procedure": "baseline"},
        }
    }

    def test_true_when_a_bootstrap_target_is_in_the_run(self):
        wf = {"target_runs": ["env/ctl-state-backend"]}
        self.assertTrue(state_sync.run_provisions_ctl_state_backend(wf, self.INVENTORY))

    def test_true_when_mixed_workflow_includes_a_bootstrap_target(self):
        wf = {"target_runs": ["env/core/baseline", {"target": "env/ctl-state-backend"}]}
        self.assertTrue(state_sync.run_provisions_ctl_state_backend(wf, self.INVENTORY))

    def test_false_for_a_normal_run(self):
        wf = {"target_runs": ["env/core/baseline"]}
        self.assertFalse(state_sync.run_provisions_ctl_state_backend(wf, self.INVENTORY))

    def test_false_when_target_missing_or_no_flag(self):
        self.assertFalse(state_sync.run_provisions_ctl_state_backend({"target_runs": ["unknown"]}, self.INVENTORY))
        self.assertFalse(state_sync.run_provisions_ctl_state_backend({"target_runs": []}, self.INVENTORY))


class ScopeConditionTests(unittest.TestCase):
    """/61: a plt scope declares its own CONDITION and activates iff the
    run reads its domain. The former `required_target_paths` filter decided the
    same thing from the other side and was removed — two mechanisms deciding one
    thing can disagree."""

    SCOPE = {"contains": {"execution_context.target.domains": "env"}}

    def test_scope_activates_when_the_run_reads_its_domain(self):
        self.assertTrue(run_selectors.selector_matches(
            self.SCOPE, {"execution_context.target.domains": ["env", "org"]}, label="t"))

    def test_scope_is_dropped_when_it_does_not(self):
        self.assertFalse(run_selectors.selector_matches(
            self.SCOPE, {"execution_context.target.domains": ["org"]}, label="t"))

    def test_scope_is_dropped_when_no_domains_are_declared(self):
        self.assertFalse(run_selectors.selector_matches(self.SCOPE, {}, label="t"))

    def test_the_removed_filter_is_gone(self):
        self.assertFalse(engine_defines("required_target_paths_for_target_runs"))


if __name__ == "__main__":
    unittest.main()
