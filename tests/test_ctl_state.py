import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402
from utils.providers import aws as aws_adapter  # noqa: E402


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


CTL_STATE_BACKENDS_YAML = """\
ctl_state_backends:
  env:
    provider: aws
    backend_type: s3
    bucket_name: ${execution_context.params.main_tag}-${execution_context.params.env_type}-ctl-state
    bucket_region: eu-central-1
    execution_identity:
      account: ctl_plane
      operations:
        read:
          role: reader
        sync:
          role: synchronizer
        maintenance:
          role: maintainer
  deployments:
    provider: aws
    backend_type: s3
    bucket_name: ${execution_context.params.main_tag}-deployments-ctl-state
    bucket_region: us-east-1
    execution_identity:
      account: ctl_plane
      operations:
        read:
          role: reader
        sync:
          role: synchronizer
        maintenance:
          role: maintainer
"""


class CtlStateBucketsCfgTests(unittest.TestCase):
    def test_loads_domains_by_structure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "not-a-special-name.yaml", CTL_STATE_BACKENDS_YAML)

            cfg = common.load_ctl_state_backends_cfg(root)
            self.assertEqual(set(cfg), {"env", "deployments"})
            self.assertEqual(cfg["deployments"]["bucket_region"], "us-east-1")

    def test_absent_resource_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(common.load_ctl_state_backends_cfg(Path(tmp)))

    def test_accepts_consumer_defined_domain(self):
        # domains are consumer vocabulary; the engine accepts any snake_case key
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_state.yaml", "ctl_state_backends:\n  org:\n    provider: aws\n    backend_type: s3\n    bucket_name: x\n    bucket_region: y\n")
            cfg = common.load_ctl_state_backends_cfg(root)
            self.assertEqual(set(cfg), {"org"})


    def test_legacy_bucket_schema_is_ignored(self):
        # legacy ctl_state_buckets alias removed (hard cutover): the section is
        # simply not read as a backend registry any more
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_state.yaml", "ctl_state_buckets:\n  env:\n    bucket_name: x\n    bucket_region: y\n")
            self.assertIsNone(common.load_ctl_state_backends_cfg(root))

    def test_rejects_non_snake_domain(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_state.yaml", "ctl_state_backends:\n  Org-State:\n    provider: aws\n    backend_type: s3\n    bucket_name: x\n    bucket_region: y\n")

            with self.assertRaisesRegex(RuntimeError, "must be a snake_case key"):
                common.load_ctl_state_backends_cfg(root)

    def test_rejects_missing_region(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_state.yaml", "ctl_state_backends:\n  env:\n    provider: aws\n    backend_type: s3\n    bucket_name: x\n")

            with self.assertRaisesRegex(RuntimeError, "bucket_region must be a non-empty string"):
                common.load_ctl_state_backends_cfg(root)


class RunRecordPublicationTests(unittest.TestCase):
    """§Phase 57: a run prefix publishes a RECORD, never the whole run dir."""

    def test_push_filters_are_an_allowlist(self):
        captured = {}

        class FakeResult:
            returncode = 0
            stderr = ""

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "ns" / "provision" / "workflow" / "wf" / "runs" / "rid"
            run_dir.mkdir(parents=True)
            syncer = aws_adapter.CtlStateSyncer(
                root, "bucket", "eu-west-2", "profile", run_dir, required=False
            )
            syncer.ready = True

            def fake_run_aws(argv):
                captured["argv"] = argv
                return FakeResult()

            syncer._run_aws = fake_run_aws
            syncer.push_run(run_dir, "test")

        argv = captured["argv"]
        # everything is excluded first — the includes are the allowlist
        self.assertEqual(argv[argv.index("--exclude")], "--exclude")
        self.assertEqual(argv[argv.index("--exclude") + 1], "*")
        self.assertLess(argv.index("--exclude"), argv.index("--include"))
        included = {argv[i + 1] for i, a in enumerate(argv) if a == "--include"}
        for member in common.RUN_RECORD_MEMBERS:
            self.assertIn(member, included)
            self.assertIn(f"{member}/*", included)
        # §gates: the run's verdicts are a published record member
        self.assertIn("gates", common.RUN_RECORD_MEMBERS)
        # the build workspace is not a record member, under any spelling
        self.assertNotIn("target_sources", included)
        self.assertNotIn("target_sources/*", included)

    def test_workspace_lives_outside_every_run_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "ns" / "provision" / "workflow" / "wf" / "runs" / "rid"
            run_dir.mkdir(parents=True)
            common.write_yaml_file(
                run_dir / common.RUN_METADATA_FILENAME,
                {"run_id": "rid", "ctl_state_local_root": str(root)},
            )
            workspace = common.run_workspace_dir(run_dir)
            self.assertEqual(
                workspace, root / "_local" / "workspaces" / "rid"
            )
            # no sync of any run prefix can ever reach it
            self.assertNotIn(str(run_dir), str(workspace))

    def test_workspace_is_unknown_before_the_run_records_its_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs" / "rid"
            run_dir.mkdir(parents=True)
            self.assertIsNone(common.run_workspace_dir(run_dir))
            common.cleanup_run_workspace(run_dir)  # tolerated, not an error


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
            self.assertFalse(common.ctl_allows_agreed_defer_ctl_state_backend_sync(root, "test_ctx"))
            self.assertFalse(common.ctl_allows_force_skip_ctl_state_backend_sync(root, "test_ctx"))
            # provider policy has no engine-granted default: it is declared
            with self.assertRaisesRegex(RuntimeError, "must declare allowed_providers"):
                common.ctl_allowed_providers(root, "test_ctx")

    def test_reads_profile_bool(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "    allow_agreed_defer_ctl_state_backend_sync: true\n")
            self.assertTrue(common.ctl_allows_agreed_defer_ctl_state_backend_sync(root, "test_ctx"))

    def test_rejects_non_bool(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._ctl_root(tmp, "    allow_agreed_defer_ctl_state_backend_sync: sometimes\n")
            with self.assertRaisesRegex(RuntimeError, "must be a bool"):
                common.ctl_allows_agreed_defer_ctl_state_backend_sync(root, "test_ctx")


if __name__ == "__main__":
    unittest.main()
