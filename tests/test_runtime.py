"""Execution-runtime tests (Phase 26).

CTL owns the execution box: it selects one runtime per run and invokes the
ctl-owned dispatcher (step_utils/run_step.sh) — never a per-target_run run script.
Target runs declare only their box (step.yaml runtime.image / docker_build) and a
runtime constraint (supported_execution_runtime_modes); CTL reconciles the selected runtime
against the ctl profile and every active target_run.
"""
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
from utils import common  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
# atlas-stack root (…/atlas-stack) holds the plt-* consumer repos with target_runs.
ATLAS_STACK = REPO_ROOT.parent


class RuntimePrimitivesTests(unittest.TestCase):
    def test_step_supported_execution_runtimes_default_and_validate(self):
        self.assertEqual(common.step_supported_execution_runtime_modes({}, label="x"), {"local", "ci"})
        self.assertEqual(
            common.step_supported_execution_runtime_modes({"supported_execution_runtime_modes": ["ci"]}, label="x"), {"ci"}
        )
        for bad in (
            {"supported_execution_runtime_modes": ["teleport"]},
            {"supported_execution_runtime_modes": []},
            {"supported_execution_runtime_modes": "ci"},
        ):
            with self.assertRaises(RuntimeError):
                common.step_supported_execution_runtime_modes(bad, label="x")

    def test_ctl_allowed_execution_runtimes_default_and_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  open: { ref_policy: commit_required }\n"
                "  local_only: { ref_policy: commit_required, allowed_execution_runtime_modes: [local] }\n"
            )
            self.assertEqual(common.ctl_allowed_execution_runtime_modes(root, "open"), {"local", "ci"})
            self.assertEqual(common.ctl_allowed_execution_runtime_modes(root, "local_only"), {"local"})

    def test_validate_execution_runtime_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  open: { ref_policy: commit_required }\n"
                "  local_only: { ref_policy: commit_required, allowed_execution_runtime_modes: [local] }\n"
            )
            common.validate_execution_runtime_mode(root, "open", "local")  # ok
            common.validate_execution_runtime_mode(root, "open", "ci")  # ok
            common.validate_execution_runtime_mode(root, "local_only", "local")  # ok
            with self.assertRaisesRegex(RuntimeError, "not allowed by ctl profile"):
                common.validate_execution_runtime_mode(root, "local_only", "ci")
            with self.assertRaisesRegex(RuntimeError, "unknown execution runtime"):
                common.validate_execution_runtime_mode(root, "open", "teleport")

    def test_ctl_ref_policy_validates_against_closed_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  strict: { ref_policy: commit_required }\n"
                "  loose: { ref_policy: local_dirty_allowed }\n"
                "  typo: { ref_policy: commit_requird }\n"
                "  empty: { ref_policy: '' }\n"
            )
            # both known values resolve
            self.assertEqual(common.ctl_ref_policy(root, "strict"), "commit_required")
            self.assertEqual(common.ctl_ref_policy(root, "loose"), "local_dirty_allowed")
            # a typo fails loud instead of silently degrading to permissive
            with self.assertRaisesRegex(RuntimeError, "unknown ref_policy"):
                common.ctl_ref_policy(root, "typo")
            # empty is still caught by the non-empty guard
            with self.assertRaisesRegex(RuntimeError, "non-empty ref_policy"):
                common.ctl_ref_policy(root, "empty")

    def test_step_box_name_valid_and_unique(self):
        a = common._step_box_name("landing_zone/org/baseline", "provision/infra")
        b = common._step_box_name("env/ops/app", "provision/ecr")
        for name in (a, b):
            self.assertRegex(name, r"^[a-z0-9._-]+$")
            self.assertFalse(name.startswith("-") or name.endswith("-"))
        self.assertNotEqual(a, b)


class RuntimeContractTests(unittest.TestCase):
    """The target_run/CTL boundary is enforced, not conventional."""

    def _step_yamls(self):
        return sorted(ATLAS_STACK.glob("*/atlas_ctl_adapter/steps/*/*/step.yaml"))

    def test_no_per_step_run_scripts_remain(self):
        # CTL owns the box; target_runs must not carry run/ scripts.
        leftover = list(ATLAS_STACK.glob("*/atlas_ctl_adapter/steps/*/*/run"))
        self.assertEqual(leftover, [], f"target_runs still carry run/ dirs: {leftover}")

    def test_every_step_declares_a_valid_box(self):
        step_yamls = self._step_yamls()
        self.assertTrue(step_yamls, "no step.yaml files discovered")
        for sy in step_yamls:
            meta = common.load_yaml(sy) or {}
            runtime_cfg = meta.get("runtime") or {}
            self.assertIn(
                runtime_cfg.get("image"), common.STEP_IMAGES,
                f"{sy} runtime.image missing/invalid",
            )
            self.assertIsInstance(
                runtime_cfg.get("docker_build", False), bool,
                f"{sy} runtime.docker_build must be bool",
            )
            # supported_execution_runtime_modes (if present) must validate
            common.step_supported_execution_runtime_modes(runtime_cfg, label=str(sy))

    def test_steps_do_not_invoke_docker_directly(self):
        # src/step.sh is runtime-neutral work; only ecr/frontend legitimately use
        # docker, and that goes through the ctl-declared docker_build capability —
        # never a raw `docker run/build` in target_run work.
        for step_sh in ATLAS_STACK.glob("*/atlas_ctl_adapter/steps/*/*/src/step.sh"):
            text = step_sh.read_text()
            self.assertNotRegex(
                text, r"(?m)^\s*docker\s+(build|run)\b",
                f"{step_sh} invokes docker directly; the box is CTL-owned",
            )


if __name__ == "__main__":
    unittest.main()
