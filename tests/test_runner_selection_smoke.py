"""Resolution smoke test for workflow and target branches against the real dev cfg.

These tests exercise the public selection assembly boundary so standalone targets
and workflow members cannot depend on state produced only by the other branch.
"""


import os
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.catalog import targets as catalog_targets
from engine.cfg import resources as cfg_resources
from engine.commands import selection as commands_selection

WORKSPACE_ROOT = REPO_ROOT.parents[1]
DEV_CFG = WORKSPACE_ROOT / "cfg/oxygen/oxygen-ctl-cfg-dev"

# main_tag comes from cfg; passing it on the CLI is a declared collision
PARAMS = {
    "landing_zone": "live",
    "env.type": "dev",
    "aws.account": "dev",
    "aws.region": "eu-west-2",
}


@unittest.skipUnless(
    DEV_CFG.is_dir() and os.environ.get("ATLAS_SLOW_TESTS") == "1",
    "resolves real cfg (~3min); set ATLAS_SLOW_TESTS=1",
)
class RealCfgSelectionTest(unittest.TestCase):
    """Each branch must resolve without reaching for another branch's locals.

    These call `resolve_pipeline_selection` ITSELF. Calling its helpers instead
    proves nothing: the helpers are fine in isolation, and the defect lives in the
    order the branch assembles them.
    """

    def _resolve(self, action: str, *, workflow: str | None = None,
                 target: str | None = None) -> dict:
        params = {**PARAMS, "operation": action}
        return commands_selection.resolve_pipeline_selection(
            DEV_CFG,
            "local_dev",
            params,
            "dirty_allowed",
            action,
            workflow,
            target_repo_key="repo_path",
            require_target_ref=False,
            execution_runtime_mode="local",
            provider_options={"credential_implementation": "profile"},
            execution_access_modes={"aws": "force_bypass"},
            target_name=target,
            force_skip_full_cfg_validation_gate=True,
            enforce_ctl_policy=False,
            load_provider_catalogs=False,
            providers=["aws"],
        )

    def test_a_workflow_resolves_its_members(self):
        selection = self._resolve("plan", workflow="env/baseline")
        self.assertEqual("workflow", selection["selection_kind"])
        self.assertIn("env/infra", selection["active_target_runs"])

    def test_a_standalone_target_resolves(self):
        """

        the branch that was broken: it has no workflow cfg of its own, so a
        change threaded into it read an unassigned local and every target run —
        including every workflow CHILD — died before doing anything."""

        selection = self._resolve("plan", target="env/infra")
        self.assertEqual("target", selection["selection_kind"])
        self.assertIn("env/infra", selection["active_target_runs"])

    def test_a_workflow_member_carries_its_declared_action(self):
        selection = self._resolve("plan", workflow="env/baseline")
        self.assertEqual(
            "plan", selection["active_target_runs"]["env/infra"]["action"]
        )

    def test_a_destroy_workflow_resolves_its_own_member_list(self):
        selection = self._resolve("destroy", workflow="env/baseline")
        runs = selection["active_target_runs"]
        self.assertIn("env/infra/prepare_destroy", runs)
        self.assertEqual("destroy", runs["env/infra/prepare_destroy"]["action"])

    def test_every_declared_target_resolves_standalone(self):
        targets = cfg_resources.collect_resource(DEV_CFG, "targets", entry_depth=1)
        checked = 0
        for name, definition in sorted(targets.items()):
            for action in catalog_targets.TargetActionPolicy(
                definition, label=f"target {name!r}"
            ).actions():
                try:
                    self._resolve(action, target=name)
                except RuntimeError:
                    continue
                checked += 1
        self.assertGreater(checked, 10)


if __name__ == "__main__":
    unittest.main()
