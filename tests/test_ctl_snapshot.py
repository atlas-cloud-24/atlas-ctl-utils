import sys
import tempfile
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cfg import materialize as cfg_materialize
from engine.execution import run_context as execution_run_context

CTX = {
    "execution_context.params.main_tag": "oxygen",
    "execution_context.params.env.type": "dev",
    "execution_context.ctl.action": "provision",
}


class ResolveCtlStructureTests(unittest.TestCase):
    def test_resolves_nested_placeholders_only(self):
        raw = {
            "meta": {"name": "commit_required/provision/env/core", "action": "provision"},
            "target_runs": [
                {"target": "env/core/baseline", "ref": "env/${execution_context.params.env.type}"},
            ],
            "literal": "no-placeholder",
            "count": 3,
        }
        resolved = execution_run_context.resolve_ctl_structure(raw, CTX, label="workflow")
        self.assertEqual(resolved["target_runs"][0]["ref"], "env/dev")
        self.assertEqual(resolved["literal"], "no-placeholder")
        self.assertEqual(resolved["count"], 3)

    def test_unresolved_placeholder_is_error(self):
        with self.assertRaisesRegex(RuntimeError, "not found in execution context"):
            execution_run_context.resolve_ctl_structure({"x": "${execution_context.params.absent}"}, CTX, label="w")


class WriteCtlCfgSnapshotTests(unittest.TestCase):
    def test_writes_resolved_self_describing_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            ctl_dir = cfg_materialize.write_ctl_cfg_snapshot(
                run_dir,
                ctl_profile="commit_required",
                ctl_profile_policy_cfg={"ref_policy": "commit", "allow_skip_ctl_state_sync": False},
                action="provision",
                workflow_cfg={"meta": {"action": "provision"}, "target_runs": ["env/core/baseline"]},
                action_cfg={"targets": {"env/core/baseline": {"ref_key": "env/${execution_context.params.env.type}"}}},
                active_target_runs={"env/core/baseline": {"branch": "main", "commit": "abc123"}},
                refs={"global": {"tooling": {"commit": "def456"}}},
                execution_context=CTX,
            )
            self.assertEqual(ctl_dir, run_dir / "cfg" / "ctl")
            action = yaml.safe_load((ctl_dir / "action.yaml").read_text())
            self.assertEqual(
                action["targets"]["env/core/baseline"]["ref_key"], "env/dev"
            )
            profile = yaml.safe_load((ctl_dir / "profile.yaml").read_text())
            self.assertEqual(profile["ctl_profile"], "commit_required")
            self.assertEqual(profile["policy"]["allow_skip_ctl_state_sync"], False)
            active = yaml.safe_load((ctl_dir / "active_target_runs.yaml").read_text())
            self.assertEqual(active["env/core/baseline"]["commit"], "abc123")


if __name__ == "__main__":
    unittest.main()
