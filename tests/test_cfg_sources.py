import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class CfgSourceTests(unittest.TestCase):
    def _write_sources(self, root: Path, body: str) -> None:
        write(root / "cfg_sources.yaml", "cfg_sources:\n" + body)

    def test_requires_exact_companion_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_sources(root, "  plt:\n    repo_path: ../plt\n")
            with self.assertRaisesRegex(RuntimeError, "must define exactly"):
                common.load_cfg_sources(root)

    def test_accepts_local_companion_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_sources(
                root,
                "  plt:\n    repo_path: ../plt\n"
                "  guardrails:\n    repo_path: ../guardrails\n",
            )
            self.assertEqual(
                common.load_cfg_sources(root)["guardrails"]["repo_path"],
                "../guardrails",
            )

    def test_commit_policy_rejects_branch_and_local_path(self):
        sources = {
            "plt": {"repo_url": "https://example.invalid/plt.git", "ref": {"branch": "main"}},
            "guardrails": {"repo_path": "../guardrails"},
        }
        with self.assertRaisesRegex(RuntimeError, "commit-pinned cfg sources"):
            common.validate_cfg_source_refs(sources, "commit_required")

    def test_commit_policy_accepts_exact_commits(self):
        sources = {
            key: {"repo_url": f"https://example.invalid/{key}.git", "ref": {"commit": "abc123"}}
            for key in common.CFG_SOURCE_KEYS
        }
        common.validate_cfg_source_refs(sources, "commit_required")


    def test_materializes_bound_local_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            (root / "plt").mkdir()
            (root / "guardrails").mkdir()
            self._write_sources(
                ctl,
                "  plt:\n    repo_path: ../plt\n"
                "  guardrails:\n    repo_path: ../guardrails\n",
            )
            roots = common.materialize_cfg_sources(
                ctl, ref_policy="local_dirty_allowed", run_cfg_dir=root / "run"
            )
            self.assertEqual(roots["plt"], (root / "plt").resolve())
            self.assertEqual(roots["guardrails"], (root / "guardrails").resolve())

    def test_remote_materialization_uses_each_bound_ref(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            self._write_sources(
                ctl,
                "  plt:\n    repo_url: https://example.invalid/plt.git\n"
                "    ref:\n      commit: plt-sha\n"
                "  guardrails:\n    repo_url: https://example.invalid/guardrails.git\n"
                "    ref:\n      commit: guard-sha\n",
            )

            def fake_clone(repo_url, branch, commit, destination, token):
                destination.mkdir(parents=True)

            with mock.patch.object(common, "git_clone", side_effect=fake_clone) as clone:
                roots = common.materialize_cfg_sources(
                    ctl, ref_policy="commit_required", run_cfg_dir=root / "run", token="token"
                )
            self.assertEqual(set(roots), {"plt", "guardrails"})
            self.assertEqual([call.args[2] for call in clone.call_args_list], ["plt-sha", "guard-sha"])

    def test_orchestrator_exposes_only_ctl_cfg_selection(self):
        runner_common = (
            REPO_ROOT.parent / "atlas-ctl-orchestrator" / "runners" / "_runner_common.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("--plt-cfg", runner_common)
        self.assertIn("materialize_cfg_sources", runner_common)


if __name__ == "__main__":
    unittest.main()
