"""Execution-runtime tests (Phase 26).

CTL owns the execution box: it selects one runtime per run and invokes the
ctl-owned dispatcher (stage_utils/run_stage.sh) — never a per-stage run script.
Stages declare only their box (stage.yaml runtime.image / docker_build) and a
runtime constraint (supported_execution_runtime_modes); CTL reconciles the selected runtime
against the ctl profile and every active stage.
"""
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
from utils import common  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
# atlas-stack root (…/atlas-stack) holds the plt-* consumer repos with stages.
ATLAS_STACK = REPO_ROOT.parent


class RuntimePrimitivesTests(unittest.TestCase):
    def test_stage_supported_execution_runtimes_default_and_validate(self):
        self.assertEqual(common.stage_supported_execution_runtime_modes({}, label="x"), {"local", "ci"})
        self.assertEqual(
            common.stage_supported_execution_runtime_modes({"supported_execution_runtime_modes": ["ci"]}, label="x"), {"ci"}
        )
        for bad in (
            {"supported_execution_runtime_modes": ["teleport"]},
            {"supported_execution_runtime_modes": []},
            {"supported_execution_runtime_modes": "ci"},
        ):
            with self.assertRaises(RuntimeError):
                common.stage_supported_execution_runtime_modes(bad, label="x")

    def test_ctl_allowed_execution_runtimes_default_and_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  open: { ref_policy: commit_required }\n"
                "  local_only: { ref_policy: commit_required, allow_execution_runtime_modes: [local] }\n"
            )
            self.assertEqual(common.ctl_allowed_execution_runtime_modes(root, "open"), {"local", "ci"})
            self.assertEqual(common.ctl_allowed_execution_runtime_modes(root, "local_only"), {"local"})

    def test_validate_execution_runtime_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  open: { ref_policy: commit_required }\n"
                "  local_only: { ref_policy: commit_required, allow_execution_runtime_modes: [local] }\n"
            )
            common.validate_execution_runtime_mode(root, "open", "local")  # ok
            common.validate_execution_runtime_mode(root, "open", "ci")  # ok
            common.validate_execution_runtime_mode(root, "local_only", "local")  # ok
            with self.assertRaisesRegex(RuntimeError, "not allowed by ctl profile"):
                common.validate_execution_runtime_mode(root, "local_only", "ci")
            with self.assertRaisesRegex(RuntimeError, "unknown execution runtime"):
                common.validate_execution_runtime_mode(root, "open", "teleport")

    def test_stage_box_name_valid_and_unique(self):
        a = common._stage_box_name("landing_zone/org/stack_sets", "provision/infra")
        b = common._stage_box_name("env/ops/app", "provision/ecr")
        for name in (a, b):
            self.assertRegex(name, r"^[a-z0-9._-]+$")
            self.assertFalse(name.startswith("-") or name.endswith("-"))
        self.assertNotEqual(a, b)


class RuntimeContractTests(unittest.TestCase):
    """The stage/CTL boundary is enforced, not conventional."""

    def _stage_yamls(self):
        return sorted(ATLAS_STACK.glob("*/atlas_ctl_adapter/stages/*/*/stage.yaml"))

    def test_no_per_stage_run_scripts_remain(self):
        # CTL owns the box; stages must not carry run/ scripts.
        leftover = list(ATLAS_STACK.glob("*/atlas_ctl_adapter/stages/*/*/run"))
        self.assertEqual(leftover, [], f"stages still carry run/ dirs: {leftover}")

    def test_every_stage_declares_a_valid_box(self):
        stage_yamls = self._stage_yamls()
        self.assertTrue(stage_yamls, "no stage.yaml files discovered")
        for sy in stage_yamls:
            meta = common.load_yaml(sy) or {}
            runtime_cfg = meta.get("runtime") or {}
            self.assertIn(
                runtime_cfg.get("image"), common.STAGE_IMAGES,
                f"{sy} runtime.image missing/invalid",
            )
            self.assertIsInstance(
                runtime_cfg.get("docker_build", False), bool,
                f"{sy} runtime.docker_build must be bool",
            )
            # supported_execution_runtime_modes (if present) must validate
            common.stage_supported_execution_runtime_modes(runtime_cfg, label=str(sy))

    def test_stages_do_not_invoke_docker_directly(self):
        # src/stage.sh is runtime-neutral work; only ecr/frontend legitimately use
        # docker, and that goes through the ctl-declared docker_build capability —
        # never a raw `docker run/build` in stage work.
        for stage_sh in ATLAS_STACK.glob("*/atlas_ctl_adapter/stages/*/*/src/stage.sh"):
            text = stage_sh.read_text()
            self.assertNotRegex(
                text, r"(?m)^\s*docker\s+(build|run)\b",
                f"{stage_sh} invokes docker directly; the box is CTL-owned",
            )


if __name__ == "__main__":
    unittest.main()
