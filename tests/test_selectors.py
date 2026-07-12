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


class CtlProfilesTests(unittest.TestCase):
    def test_profiles_load_by_top_level_key_not_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "not-a-special-name.yaml",
                  "ctl_profiles:\n  local_dev:\n    ref_policy: local_dirty_allowed\n")

            self.assertEqual(common.ctl_ref_policy(root, "local_dev"), "local_dirty_allowed")

    def test_unknown_profile_lists_known(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_profiles.yaml", "ctl_profiles:\n  local_dev:\n    ref_policy: x\n")

            with self.assertRaisesRegex(RuntimeError, "known profiles: local_dev"):
                common.ctl_profile_policy(root, "nope")


class ExecutionContextTests(unittest.TestCase):
    def _ctl_root(self, tmp: str) -> Path:
        root = Path(tmp)
        write(root / "execution_params.yaml",
              "execution_params:\n  main_tag: oxygen\n"
              "  derived: ${execution_context.params.env_type}\n")
        return root

    def test_builds_two_namespaces_flat(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = common.build_execution_context(
                self._ctl_root(tmp), action="plan", ctl_profile="commit_required",
                execution_runtime="local",
                execution_params={"env_type": "dev"})
        self.assertEqual(ctx["execution_context.ctl.action"], "plan")
        self.assertEqual(ctx["execution_context.ctl.profile"], "commit_required")
        self.assertEqual(ctx["execution_context.params.env_type"], "dev")
        self.assertEqual(ctx["execution_context.params.main_tag"], "oxygen")
        self.assertEqual(ctx["execution_context.params.derived"], "dev")

    def test_cfg_param_ref_to_absent_is_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = common.build_execution_context(
                self._ctl_root(tmp), action="plan", ctl_profile="p",
                execution_runtime="local", execution_params={})
        self.assertNotIn("execution_context.params.derived", ctx)

    def test_cli_cfg_param_collision_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "collides with a --execution-params"):
                common.build_execution_context(
                    self._ctl_root(tmp), action="plan", ctl_profile="p",
                    execution_runtime="local",
                    execution_params={"main_tag": "other"})

    def test_nested_view_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = common.build_execution_context(
                self._ctl_root(tmp), action="plan", ctl_profile="p",
                execution_runtime="local",
                execution_params={"env_type": "dev"})
        nested = common.execution_context_nested(ctx)
        self.assertEqual(nested["execution_context"]["ctl"]["action"], "plan")
        self.assertEqual(nested["execution_context"]["params"]["main_tag"], "oxygen")


class SelectorMatchTests(unittest.TestCase):
    CTX = {
        "execution_context.ctl.action": "plan",
        "execution_context.ctl.profile": "commit_required",
        "execution_context.params.env_type": "dev",
    }

    def test_fully_qualified_match(self):
        self.assertTrue(common.selector_matches(
            {"execution_context.params.env_type": ["dev", "test"]}, self.CTX, label="t"))

    def test_promoted_keys_are_selectable(self):
        self.assertTrue(common.selector_matches(
            {"execution_context.ctl.profile": ["commit_required"]}, self.CTX, label="t"))

    def test_missing_key_means_no_match(self):
        self.assertFalse(common.selector_matches(
            {"execution_context.params.region": ["eu-west-2"]}, self.CTX, label="t"))

    def test_bare_key_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "fully-qualified execution-context path"):
            common.selector_matches({"env_type": ["dev"]}, self.CTX, label="t")

    def test_constraints_enforce_allowed_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "execution_context_constraints.yaml",
                  "execution_context_constraints:\n"
                  "  - when:\n      execution_context.params.env_type: [prod]\n"
                  "    allowed_values:\n      execution_context.ctl.action: [provision, plan, readonly]\n")
            ctx = dict(self.CTX)
            ctx["execution_context.params.env_type"] = "prod"
            ctx["execution_context.ctl.action"] = "destroy"
            with self.assertRaisesRegex(RuntimeError, "allows execution_context.ctl.action"):
                common.validate_execution_context_constraints(root, ctx)


if __name__ == "__main__":
    unittest.main()
