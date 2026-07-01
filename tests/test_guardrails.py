import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


class GuardrailTests(unittest.TestCase):
    def test_ctl_guardrails_are_discovered_by_top_level_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("oxygen", label="test")
            (root / "not-a-special-name.yaml").write_text(
                f"guardrails:\n  guarded_vars:\n    main_tag: {expected}\n",
                encoding="utf-8",
            )

            common.verify_ctl_guardrails(root, {"main_tag": "oxygen"})

    def test_ctl_guardrails_reject_changed_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("oxygen", label="test")
            (root / "guardrails.yaml").write_text(
                f"guardrails:\n  guarded_vars:\n    main_tag: {expected}\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "guarded ctl var .*main_tag.* changed"):
                common.verify_ctl_guardrails(root, {"main_tag": "argon"})

    def test_plt_guardrails_use_reserved_root_file_and_resolved_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "plt"
            merged = Path(tmp) / "merged"
            root.mkdir()
            (merged / "deployments").mkdir(parents=True)
            expected = common.guard_value_hash("oxygen-deployments-tfstate", label="test")
            (root / common.PLT_GUARDRAILS_FILENAME).write_text(
                f"guarded_vars:\n  deployments_tfstate_s3_bucket_name: {expected}\n",
                encoding="utf-8",
            )
            (merged / "deployments" / "tfstate.yaml").write_text(
                "deployments_tfstate_s3_bucket_name: ${main_tag}-deployments-tfstate\n",
                encoding="utf-8",
            )

            common.verify_plt_guardrails(root, merged, {}, {"main_tag": "oxygen"})

    def test_plt_scope_guardrails_apply_to_active_scope_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "plt"
            merged = Path(tmp) / "merged"
            scope = root / "env" / "dev"
            scope.mkdir(parents=True)
            (scope / common.SCOPE_META_FILENAME).write_text(
                "type: scope\ntarget_path: /env\nselectors:\n  env_type: dev\n",
                encoding="utf-8",
            )
            expected = common.guard_value_hash("eu-west-2", label="test")
            (scope / common.PLT_GUARDRAILS_FILENAME).write_text(
                f"guarded_vars:\n  aws_region: {expected}\n",
                encoding="utf-8",
            )
            (merged / "env").mkdir(parents=True)
            (merged / "env" / "general.yaml").write_text("aws_region: eu-west-2\n", encoding="utf-8")

            common.verify_plt_guardrails(root, merged, {"env_type": "dev"}, {})

    def test_plt_guardrails_ignore_normal_guardrails_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "plt"
            root.mkdir()
            (root / "guardrails.yaml").write_text(
                "guarded_vars:\n  aws_region: " + "0" * 64 + "\n",
                encoding="utf-8",
            )

            self.assertEqual(common.load_plt_guarded_vars(root), {})


if __name__ == "__main__":
    unittest.main()
