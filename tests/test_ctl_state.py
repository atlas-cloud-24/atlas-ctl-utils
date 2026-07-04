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


CTL_STATE_BUCKETS_YAML = """\
ctl_state_buckets:
  env:
    bucket_name: ${execution_context.params.main_tag}-${execution_context.params.env_type}-ctl-state
    bucket_region: eu-central-1
    execution_identity_key: ctl_state_env_writer
  deployments:
    bucket_name: ${execution_context.params.main_tag}-deployments-ctl-state
    bucket_region: us-east-1
    execution_identity_key: ctl_state_deployments_writer
"""


class CtlStateBucketsCfgTests(unittest.TestCase):
    def test_loads_domains_by_structure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "not-a-special-name.yaml", CTL_STATE_BUCKETS_YAML)

            cfg = common.load_ctl_state_buckets_cfg(root)
            self.assertEqual(set(cfg), {"env", "deployments"})
            self.assertEqual(cfg["deployments"]["bucket_region"], "us-east-1")

    def test_absent_resource_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(common.load_ctl_state_buckets_cfg(Path(tmp)))

    def test_rejects_unknown_domain(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_state.yaml", "ctl_state_buckets:\n  org:\n    bucket_name: x\n    bucket_region: y\n")

            with self.assertRaisesRegex(RuntimeError, "domain must be env or deployments"):
                common.load_ctl_state_buckets_cfg(root)

    def test_rejects_missing_region(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_state.yaml", "ctl_state_buckets:\n  env:\n    bucket_name: x\n")

            with self.assertRaisesRegex(RuntimeError, "bucket_region must be a non-empty string"):
                common.load_ctl_state_buckets_cfg(root)


class CtlStateSkipPolicyTests(unittest.TestCase):
    def _ctl_root(self, tmp: str, policy_line: str) -> Path:
        root = Path(tmp)
        write(
            root / "ctl_profiles.yaml",
            "ctl_profiles:\n  test_ctx:\n    ref_policy: commit_required\n" + policy_line,
        )
        return root

    def test_defaults_to_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "")
            self.assertFalse(common.ctl_allows_skip_ctl_state_bucket_sync(root, "test_ctx"))

    def test_reads_profile_bool(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "    allow_skip_ctl_state_bucket_sync: true\n")
            self.assertTrue(common.ctl_allows_skip_ctl_state_bucket_sync(root, "test_ctx"))

    def test_rejects_non_bool(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "    allow_skip_ctl_state_bucket_sync: sometimes\n")
            with self.assertRaisesRegex(RuntimeError, "must be a bool"):
                common.ctl_allows_skip_ctl_state_bucket_sync(root, "test_ctx")


if __name__ == "__main__":
    unittest.main()
