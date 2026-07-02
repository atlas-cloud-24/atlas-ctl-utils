import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


CTL_RESULTS_YAML = """\
ctl_results:
  backends:
    env:
      bucket_name: ${main_tag}-${env_type}-ctl-results
      execution_identity_key: ctl_results_env_writer
    deployments:
      bucket_name: ${main_tag}-deployments-ctl-results
      execution_identity_key: ctl_results_deployments_writer
"""


class CtlResultsCfgTests(unittest.TestCase):
    def test_loads_backends_by_structure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "not-a-special-name.yaml", CTL_RESULTS_YAML)

            cfg = common.load_ctl_results_cfg(root)
            self.assertEqual(set(cfg), {"env", "deployments"})

    def test_absent_resource_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(common.load_ctl_results_cfg(Path(tmp)))

    def test_rejects_unknown_tier(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_results.yaml", "ctl_results:\n  backends:\n    org:\n      bucket_name: x\n      execution_identity_key: y\n")

            with self.assertRaisesRegex(RuntimeError, "tier must be env or deployments"):
                common.load_ctl_results_cfg(root)

    def test_backend_resolution_env_tier_interpolates_bucket_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_results.yaml", CTL_RESULTS_YAML)
            cfg = common.load_ctl_results_cfg(root)

            backend = common.resolve_ctl_results_backend(
                cfg,
                {"env_type": "dev"},
                {"main_tag": "oxygen", "env_type": "dev"},
            )
            self.assertEqual(backend["tier"], "env")
            self.assertEqual(backend["bucket_name"], "oxygen-dev-ctl-results")
            self.assertEqual(backend["execution_identity_key"], "ctl_results_env_writer")

    def test_backend_resolution_deployments_tier_without_env_selector(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_results.yaml", CTL_RESULTS_YAML)
            cfg = common.load_ctl_results_cfg(root)

            backend = common.resolve_ctl_results_backend(cfg, {}, {"main_tag": "oxygen"})
            self.assertEqual(backend["tier"], "deployments")
            self.assertEqual(backend["bucket_name"], "oxygen-deployments-ctl-results")


class CtlResultsPolicyTests(unittest.TestCase):
    def _ctl_root(self, tmp: str, results_sync_line: str) -> Path:
        root = Path(tmp)
        write(
            root / "selectors.yaml",
            "selectors:\n  ctl_context:\n    test_ctx:\n      ref_policy: commit_required\n" + results_sync_line,
        )
        return root

    def test_policy_defaults_to_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "")
            self.assertEqual(common.ctl_results_sync_policy(root, "test_ctx"), "disabled")

    def test_policy_reads_context_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "      results_sync: required\n")
            self.assertEqual(common.ctl_results_sync_policy(root, "test_ctx"), "required")

    def test_policy_rejects_unknown_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "      results_sync: sometimes\n")
            with self.assertRaisesRegex(RuntimeError, "results_sync must be one of"):
                common.ctl_results_sync_policy(root, "test_ctx")


if __name__ == "__main__":
    unittest.main()
