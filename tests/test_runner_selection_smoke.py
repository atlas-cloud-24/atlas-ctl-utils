"""Resolution smoke test: every selection kind, against the REAL dev cfg.

This exists because unit tests kept passing while the runner path was broken.
`resolve_pipeline_selection` branches on selection kind — procedure, target,
workflow, fan-out — and each branch builds its cfg in a different ORDER. A change
threaded into one branch reads a variable another branch has not assigned yet, and
nothing that mocks cfg can see it: the failure is in the wiring, not the logic.

The bug this was written for: the target branch called
`workflow_member_actions(workflow_cfg)` before `workflow_cfg` was assigned, so
every standalone target run — including every workflow child — died with an
UnboundLocalError. 487 unit tests and full cfg validation were green.
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

DEV_CFG = Path("/home/valerii/programs/atlas/cfg/oxygen/oxygen-ctl-cfg-dev")

# main_tag comes from cfg; passing it on the CLI is a declared collision
PARAMS = {
    "landing_zone": "live",
    "env.type": "dev",
    "aws.account": "dev",
    "aws.region": "eu-west-2",
}


def _context(action: str) -> dict:
    ctx = {
        "execution_context.ctl.action": action,
        "execution_context.params.operation": action,
        "execution_context.ctl.profile": "local_dev",
        "execution_context.ctl.providers": ["aws"],
    }
    ctx.update({f"execution_context.params.{k}": v for k, v in PARAMS.items()})
    return ctx


@unittest.skipUnless(
    DEV_CFG.is_dir() and os.environ.get("ATLAS_SLOW_TESTS") == "1",
    "resolves real cfg (~3min); set ATLAS_SLOW_TESTS=1",
)
class SelectionResolvesForEveryKindTest(unittest.TestCase):
    """Each branch must resolve without reaching for another branch's locals.

    These call `resolve_pipeline_selection` ITSELF. Calling its helpers instead
    proves nothing: the helpers are fine in isolation, and the defect lives in the
    order the branch assembles them.
    """

    def _resolve(self, action: str, *, workflow: str | None = None,
                 target: str | None = None) -> dict:
        return commands_selection.resolve_pipeline_selection(
            DEV_CFG,
            "local_dev",
            dict(PARAMS),
            "dirty_allowed",
            action,
            workflow,
            ctl_variants=[],
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
        self.assertIn("env/core/baseline", selection["active_target_runs"])

    def test_a_standalone_target_resolves(self):
        """

        the branch that was broken: it has no workflow cfg of its own, so a
        change threaded into it read an unassigned local and every target run —
        including every workflow CHILD — died before doing anything."""

        selection = self._resolve("plan", target="env/core/baseline")
        self.assertEqual("target", selection["selection_kind"])
        self.assertIn("env/core/baseline", selection["active_target_runs"])

    def test_a_workflow_member_carries_its_declared_action(self):
        selection = self._resolve("plan", workflow="env/baseline")
        self.assertEqual(
            "plan", selection["active_target_runs"]["env/core/baseline"]["action"]
        )

    def test_a_destroy_workflow_resolves_its_own_member_list(self):
        selection = self._resolve("destroy", workflow="env/baseline")
        runs = selection["active_target_runs"]
        self.assertIn("env/core/prepare_destroy", runs)
        self.assertEqual("destroy", runs["env/core/prepare_destroy"]["action"])

    def test_every_declared_workflow_resolves_for_each_operation(self):
        """

        breadth over depth: one pass across the whole cfg reaches shapes a
        hand-picked example never would."""

        workflows = cfg_resources.collect_resource(DEV_CFG, "workflows", entry_depth=1)
        checked = 0
        for name, definition in sorted(workflows.items()):
            for action in catalog_targets.entry_actions(definition, label=f"workflow {name!r}"):
                try:
                    self._resolve(action, workflow=name)
                except RuntimeError:
                    continue  # selectors exclude this combination; not a wiring fault
                checked += 1
        self.assertGreater(checked, 5)

    def test_every_declared_target_resolves_standalone(self):
        targets = cfg_resources.collect_resource(DEV_CFG, "targets", entry_depth=1)
        checked = 0
        for name, definition in sorted(targets.items()):
            for action in catalog_targets.entry_actions(definition, label=f"target {name!r}"):
                try:
                    self._resolve(action, target=name)
                except RuntimeError:
                    continue
                checked += 1
        self.assertGreater(checked, 10)


if __name__ == "__main__":
    unittest.main()
