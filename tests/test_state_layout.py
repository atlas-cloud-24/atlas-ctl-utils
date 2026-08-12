"""State is partitioned by status GROUP, not by action.

    was   <ns>/<action>/<kind>/<key>/instances/<seg>/committed.yaml
    now   <ns>/<kind>/<key>/instances/<seg>/committed/<group>.yaml

Provision and destroy are two directions of ONE state, so they share an instance
and differ only in the group file they publish. Plan and readonly are independent
facts and keep their own files — the property that makes the partition earn its
place, because a plan must never erase the record of the last deployment.
"""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import ctl_cfg_fixture
from engine.catalog import target_catalog
from engine.catalog import workflow as catalog_workflow
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.run import selectors as run_selectors
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine_surface import engine_defines

KEY, SEGMENTS = "env/seed/baseline", ["env.type=dev", "aws.account=dev"]
SPEC = {
    "kind": "target",
    "key": KEY,
    "segments": SEGMENTS,
    "address": run_addressing.target_instance_address(KEY, SEGMENTS),
}


def _instance(namespace: Path) -> Path:
    return namespace / run_addressing.compose_state_relpath("target", KEY, SEGMENTS)


def _publish(namespace: Path, group: str, **facts) -> None:
    kernel_yaml_io.write_yaml_file(
        state_run_store.committed_pointer_path(_instance(namespace), group),
        {"run_id": "r1", "status": "ok", "committed_at": "2026-07-30T10:00:00Z", **facts},
    )


def _slot(namespace: Path, state: str, group: str, **facts) -> None:
    kernel_yaml_io.write_yaml_file(
        state_run_store.state_slot_dir(_instance(namespace), state, group) / "STATUS.yaml",
        {"run_id": "r2", "status": state, **facts},
    )


class PathShapeTest(unittest.TestCase):
    def test_the_path_carries_no_action(self):
        self.assertEqual(
            "target/env/seed/baseline/instances/env.type=dev/aws.account=dev",
            run_addressing.compose_state_relpath("target", KEY, SEGMENTS).as_posix(),
        )

    def test_a_singleton_has_no_instances_layer(self):
        self.assertEqual(
            "workflow/org/bootstrap_admin",
            run_addressing.compose_state_relpath("workflow", "org/bootstrap_admin", []).as_posix(),
        )

    def test_maintenance_has_a_separate_state_owner(self):
        self.assertEqual(
            "maintenance/unlock-ctl-state/lock-1",
            run_addressing.compose_state_relpath(
                "maintenance", "unlock-ctl-state/lock-1", []
            ).as_posix(),
        )

    def test_an_unknown_kind_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unknown state kind"):
            run_addressing.compose_state_relpath("fan_out", "x", [])

    def test_the_pointer_lives_under_its_group(self):
        self.assertEqual(
            "committed/mutative.yaml",
            state_run_store.committed_pointer_path(Path("i"), "mutative")
            .relative_to("i")
            .as_posix(),
        )

    def test_an_unknown_group_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unknown state group"):
            state_run_store.committed_pointer_path(Path("i"), "nonsense")

    def test_parsing_is_the_inverse(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            parsed = run_addressing.parse_state_relpath(namespace, _instance(namespace))
            self.assertEqual("target", parsed["kind"])
            self.assertEqual(KEY, parsed["key"])
            self.assertEqual(SEGMENTS, parsed["instance_segments"])
            self.assertNotIn("action", parsed)


class ActionMapsToGroupTest(unittest.TestCase):
    def test_the_two_directions_share_one_group(self):
        self.assertEqual("mutative", run_actions.action_group("provision"))
        self.assertEqual("mutative", run_actions.action_group("destroy"))

    def test_the_independent_facts_keep_their_own(self):
        self.assertEqual("plan", run_actions.action_group("plan"))
        self.assertEqual("readonly", run_actions.action_group("readonly"))
        self.assertEqual("maintenance", run_actions.action_group("maintenance"))

    def test_an_unknown_action_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unknown action"):
            run_actions.action_group("rebuild")


class GroupsDoNotOverwriteTest(unittest.TestCase):
    """

    the reason the partition exists: a plan must not erase a deployment."""

    def test_a_plan_and_a_deployment_pointer_coexist(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "mutative", run_id="provisioned")
            _publish(namespace, "plan", run_id="planned")
            self.assertEqual(
                "provisioned",
                state_run_store.read_committed_pointer(_instance(namespace), "mutative")["run_id"],
            )
            self.assertEqual(
                "planned",
                state_run_store.read_committed_pointer(_instance(namespace), "plan")["run_id"],
            )

    def test_a_failed_plan_and_a_failed_deployment_coexist(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _slot(namespace, "failed", "plan", run_id="p")
            _slot(namespace, "failed", "mutative", run_id="d")
            self.assertEqual(
                "p",
                state_run_store.read_instance_state_slot(_instance(namespace), "failed", "plan")[
                    "run_id"
                ],
            )
            self.assertEqual(
                "d",
                state_run_store.read_instance_state_slot(
                    _instance(namespace), "failed", "mutative"
                )["run_id"],
            )

    def test_a_deployment_run_publishes_only_its_own_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            run_dir = _instance(namespace) / "runs" / "r9"
            run_dir.mkdir(parents=True)
            state_run_store.write_run_metadata(
                run_dir,
                {
                    "run_id": "r9",
                    "action": "destroy",
                    "run_type": "target",
                    "result_name": KEY,
                    "ctl_state_local_root": str(namespace),
                    "ctl_state_locator": [],
                },
            )
            state_run_store.publish_committed_pointer(
                run_dir, state_status.build_status_payload(run_dir, "ok")
            )
            published = sorted(q.name for q in (_instance(namespace) / "committed").iterdir())
            self.assertEqual(["mutative.yaml"], published)


class RowShapeTest(unittest.TestCase):
    def test_a_row_carries_status_freshness_and_at_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "mutative", action="provision")
            row = state_status.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertEqual("passed", row["status"])
            self.assertEqual("up_to_date", row["freshness"])
            self.assertNotIn("state", row)
            self.assertNotIn("action", row)

    def test_the_canonical_order_is_status_freshness_at(self):
        self.assertEqual(
            ["status", "freshness", "time"],
            list(
                run_addressing.order_axes(
                    {"time": "t", "freshness": "outdated", "status": "passed"}
                )
            ),
        )

    def test_a_live_slot_reads_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "mutative", action="provision")
            _slot(namespace, "in_progress", "mutative", action="provision")
            self.assertEqual(
                "running",
                state_status.compute_target_instance_status(namespace, "provision", SPEC)["status"],
            )

    def test_a_destroyed_instance_reports_no_freshness(self):
        """

        nothing is left for the inputs to have moved away from."""

        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "mutative", action="destroy")
            row = state_status.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertEqual("passed", row["status"])
            self.assertNotIn("freshness", row)

    def test_an_interrupted_run_reports_no_freshness(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "mutative", action="provision")
            _slot(namespace, "failed", "mutative", action="provision", mutation_started=True)
            row = state_status.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertEqual("failed", row["status"])
            self.assertNotIn("freshness", row)

    def test_nothing_ran_means_no_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            row = state_status.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertNotIn("status", row)


class NamespaceMapTest(unittest.TestCase):
    def test_one_instance_reports_each_group_it_published(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "mutative", action="provision")
            _publish(namespace, "plan", action="plan")
            rows = state_status.compute_namespace_status_map(namespace)
            instance = rows["target"][KEY]["instances"]["/".join(SEGMENTS)]
            self.assertEqual({"plan", "mutative"}, set(instance))
            self.assertEqual("passed", instance["mutative"]["status"])
            self.assertEqual("passed", instance["plan"]["status"])


if __name__ == "__main__":
    unittest.main()


class MemberEntryActionTest(unittest.TestCase):
    """A workflow member entry names a target, optionally with its own
    action. The action belongs to the target, not to the member list."""

    def test_a_bare_key_takes_the_declared_default(self):
        self.assertEqual(
            [("env/ops/ecr", "provision")],
            target_catalog.TargetEntries.normalize(
                ["env/ops/ecr"], label="wf", default_action="provision"
            ),
        )

    def test_a_bare_key_with_no_default_is_refused(self):
        """

        not runnable: the engine cannot know what to do with the target."""

        with self.assertRaisesRegex(RuntimeError, "has no action"):
            target_catalog.TargetEntries.normalize(["env/ops/ecr"], label="wf")

    def test_a_mapping_declares_its_own_action(self):
        self.assertEqual(
            [("env/ops/app", "destroy")],
            target_catalog.TargetEntries.normalize(
                [{"key": "env/ops/app", "action": "destroy"}], label="wf"
            ),
        )

    def test_a_key_may_repeat_with_differing_actions(self):
        """

        order is load-bearing: the last write to the pointer wins."""

        entries = target_catalog.TargetEntries.normalize(
            [
                {"key": "env/seed/baseline", "action": "destroy"},
                {"key": "env/seed/baseline", "action": "provision"},
            ],
            label="wf",
        )
        self.assertEqual(
            [("env/seed/baseline", "destroy"), ("env/seed/baseline", "provision")],
            entries,
        )

    def test_the_same_key_and_action_twice_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "duplicate target entry"):
            target_catalog.TargetEntries.normalize(
                ["env/ops/ecr", {"key": "env/ops/ecr"}],
                label="wf",
                default_action="provision",
            )

    def test_an_unknown_action_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "declares action"):
            target_catalog.TargetEntries.normalize(
                [{"key": "env/ops/app", "action": "rebuild"}], label="wf"
            )

    def test_maintenance_is_refused_as_a_workflow_member_action(self):
        with self.assertRaisesRegex(RuntimeError, "maintenance runner"):
            target_catalog.TargetEntries.normalize(
                [{"key": "env/ops/app", "action": "maintenance"}], label="wf"
            )

    def test_an_unsupported_field_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unsupported keys"):
            target_catalog.TargetEntries.normalize(
                [{"key": "env/ops/app", "params": {"env.type": "dev"}}], label="wf"
            )

    def test_keys_only_is_still_the_action_cfg_shape(self):
        self.assertEqual(
            ["a/b", "c/d"],
            run_addressing.normalize_target_keys(
                ["a/b", {"key": "c/d", "action": "destroy"}],
                label="wf",
            ),
        )


class MemberActionReachesTheChildTest(unittest.TestCase):
    """The declaration is worthless unless the child actually runs that action."""

    WORKFLOWS = {
        "env/baseline": {
            "default_action": "provision",
            "targets": [
                "env/core/baseline",
                {"key": "env/ops/app", "action": "destroy"},
            ],
        }
    }

    def test_import_expansion_keeps_the_declared_action(self):
        runs = catalog_workflow.WorkflowImports.expand(self.WORKFLOWS, "env/baseline")
        self.assertEqual(
            {"id": "env/core/baseline", "target": "env/core/baseline", "action": "provision"},
            runs[0],
        )
        self.assertEqual(
            {"id": "env/ops/app", "target": "env/ops/app", "action": "destroy"}, runs[1]
        )

    def test_a_repeated_key_with_differing_actions_expands(self):
        runs = catalog_workflow.WorkflowImports.expand(
            {
                "env/seed": {
                    "default_action": "provision",
                    "targets": [
                        {"key": "env/seed/baseline", "action": "destroy"},
                        {"key": "env/seed/baseline", "action": "provision"},
                    ],
                }
            },
            "env/seed",
        )
        self.assertEqual(["destroy", "provision"], [r["action"] for r in runs])

    def test_the_same_key_and_action_twice_is_still_refused(self):
        with self.assertRaisesRegex(RuntimeError, "duplicate target entry"):
            catalog_workflow.WorkflowImports.expand(
                {"w": {"default_action": "provision", "targets": ["a/b", {"key": "a/b"}]}},
                "w",
            )

    def test_the_member_actions_are_collected_for_the_action_cfg(self):
        cfg = {
            "target_runs": catalog_workflow.WorkflowImports.expand(self.WORKFLOWS, "env/baseline")
        }
        self.assertEqual(
            {"provision", "destroy"}, catalog_workflow.WorkflowCatalog.member_actions(cfg)
        )

    def test_the_child_argv_carries_the_member_action(self):
        spec = {
            "ctl_entrypoint": "ctl.py",
            "ctl_cfg_root": "/cfg",
            "ctl_profile": "local_dev",
            "ctl_state_local_root": "/state",
            "execution_runtime_mode": "local",
            "action": "provision",
        }
        argv = catalog_workflow.WorkflowChildren.build_command(
            spec,
            "env/ops/app",
            parent_run_dir=Path("/state/runs/w1"),
            parent_run_id="w1",
            action="destroy",
        )
        self.assertEqual("destroy", argv[argv.index("--action") + 1])

    def test_a_child_without_a_declared_action_inherits_the_operation(self):
        spec = {
            "ctl_entrypoint": "ctl.py",
            "ctl_cfg_root": "/cfg",
            "ctl_profile": "local_dev",
            "ctl_state_local_root": "/state",
            "execution_runtime_mode": "local",
            "action": "provision",
        }
        argv = catalog_workflow.WorkflowChildren.build_command(
            spec,
            "env/core/baseline",
            parent_run_dir=Path("/state/runs/w1"),
            parent_run_id="w1",
        )
        self.assertEqual("provision", argv[argv.index("--action") + 1])


class TargetActionPolicyTest(unittest.TestCase):
    """A target declares concrete engine actions and their policy."""

    def test_a_target_declares_actions(self):
        self.assertEqual(
            ["provision"],
            target_catalog.TargetActionPolicy(
                {"actions": ["provision"]}, label="target 't'"
            ).actions(),
        )

    def test_workflow_operation_vocabulary_is_not_target_policy(self):
        with self.assertRaisesRegex(RuntimeError, "does not accept 'operations'"):
            target_catalog.TargetActionPolicy(
                {"actions": ["provision"], "operations": ["provision"]},
                label="target 't'",
            ).actions()

    def test_actions_are_required(self):
        with self.assertRaisesRegex(RuntimeError, "must declare 'actions'"):
            target_catalog.TargetActionPolicy({}, label="target 't'").actions()


class DispatchGuardTest(unittest.TestCase):
    """/73: a member may dispatch only on a fact that determines the
    instance path."""

    def _members(self, ref, value="provision"):
        return [{"keys": ["a/b"], "selectors": {"match": {ref: value}}}]

    def test_dispatching_on_operation_is_an_instance_axis(self):
        self.assertEqual(
            {"operation"},
            run_selectors.collect_member_dispatch_axes(
                self._members("execution_context.params.operation"), label="wf"
            ),
        )

    def test_dispatching_on_the_action_is_still_allowed(self):
        self.assertEqual(
            set(),
            run_selectors.collect_member_dispatch_axes(
                self._members("execution_context.ctl.action"), label="wf"
            ),
        )

    def test_dispatching_on_a_params_axis_returns_it(self):
        self.assertEqual(
            {"env.type"},
            run_selectors.collect_member_dispatch_axes(
                self._members("execution_context.params.env.type", "dev"), label="wf"
            ),
        )

    def test_dispatching_on_any_other_ctl_fact_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "is not allowed"):
            run_selectors.collect_member_dispatch_axes(
                self._members("execution_context.ctl.profile", "local_dev"), label="wf"
            )


class WorkflowIsAddressedByParamsTest(unittest.TestCase):
    """A workflow instance is addressed by its DECLARED params.

        The composition sha256 this class used to test is gone. It was introduced to
        keep a teardown of A and a deploy of A apart, but answered that
        differently and better: params ADDRESS where a hash IDENTIFIES, and the two
        directions share one instance while publishing into different GROUP files
    . The hash then survived only in the status path, where it composed
        a prefix no run has ever written.

        What replaces those tests is the property that actually matters: the run side
        and the status side must produce the SAME address.
    """

    KEY = "env/workload_permissions_boundary"
    PARAMS = ["env.type", "aws.account"]
    CONTEXT = {
        "execution_context.params.env.type": "dev",
        "execution_context.params.aws.account": "dev",
    }

    def setUp(self):
        # the status side resolves target runs through the adapter
        ctl_cfg_fixture.cfg_root(self, "aws")

    def _run_side_prefix(self):
        segments = run_addressing.resolve_target_instance_segments(
            self.PARAMS, self.CONTEXT, label="workflow"
        )
        return run_addressing.compose_state_relpath("workflow", self.KEY, segments).as_posix()

    def _status_side_spec(self, action="provision"):
        return catalog_workflow.WorkflowArtifacts.selection_state_spec(
            {
                "selection_kind": "workflow",
                "selection_key": self.KEY,
                "workflow_cfg": {
                    "meta": {"name": f"{action}/{self.KEY}", "action": action},
                    "workflow_instance_params": self.PARAMS,
                },
                "execution_context": self.CONTEXT,
                "active_target_runs": {
                    "tr1": {
                        "target": f"{self.KEY}/baseline",
                        "target_instance_params": self.PARAMS,
                        "action": action,
                    }
                },
            }
        )

    def test_the_status_path_addresses_what_a_run_writes(self):
        """The defect: status hydrated `instances/sha256=<digest>` while runs write
        `instances/env.type=dev/aws.account=dev`, so a targeted remote query pulled
        nothing and reported no state."""
        self.assertEqual(self._run_side_prefix(), self._status_side_spec()["prefix"])

    def test_the_address_is_readable_params_not_a_digest(self):
        spec = self._status_side_spec()
        self.assertEqual(["env.type=dev", "aws.account=dev"], spec["segments"])
        self.assertNotIn("sha256=", spec["prefix"])

    def test_opposite_actions_share_one_instance(self):
        """Provision and destroy are two directions of ONE state, so they address
        the same instance and differ only in the group file they publish."""
        self.assertEqual(
            self._status_side_spec("provision")["prefix"],
            self._status_side_spec("destroy")["prefix"],
        )
        self.assertNotEqual(run_actions.action_group("provision"), run_actions.action_group("plan"))

    def test_a_members_shaped_declaration_still_resolves(self):
        """

        params may DISPATCH on context, and both sides must resolve it the same
        way — which is why the resolution has one definition."""

        declared = {
            "members": [
                {
                    "selectors": {"match": {"execution_context.params.env.type": "dev"}},
                    "params": ["env.type"],
                },
                {
                    "selectors": {"match": {"execution_context.params.env.type": "prod"}},
                    "params": ["env.type", "aws.account"],
                },
            ]
        }
        self.assertEqual(
            ["env.type"],
            catalog_workflow.WorkflowInstanceParams.resolve_declared(
                declared, self.CONTEXT, label="workflow"
            ),
        )

    def test_the_composition_hash_is_gone(self):
        """Guards the phase: leaving it importable invites a second addressing
        scheme back in beside the first."""
        self.assertFalse(engine_defines("workflow_composition_sha256"))


class ActionMustBeDeclaredTest(unittest.TestCase):
    """A cfg gate: a workflow entry the engine cannot run is refused at validation.

    Without this, an entry with no action and no `default_action` produced
    `action=None` and the engine simply did not know what to do with the target.
    """

    def _refuses(self, workflows):
        with self.assertRaisesRegex(RuntimeError, "no action"):
            catalog_workflow.WorkflowCatalog.validate_actions_declared(workflows)

    def test_a_bare_list_with_no_default_is_refused(self):
        self._refuses({"w": {"targets": {"keys": ["a/b"]}}})

    def test_a_member_with_no_default_is_refused(self):
        self._refuses({"w": {"targets": {"members": [{"keys": ["a/b"], "selectors": {}}]}}})

    def test_a_declared_default_is_accepted(self):
        catalog_workflow.WorkflowCatalog.validate_actions_declared(
            {"w": {"targets": {"default_action": "provision", "keys": ["a/b"]}}}
        )

    def test_per_key_actions_without_a_default_are_accepted(self):
        catalog_workflow.WorkflowCatalog.validate_actions_declared(
            {"w": {"targets": {"keys": [{"key": "a/b", "action": "destroy"}]}}}
        )

    def test_one_bare_key_among_declared_ones_is_still_refused(self):
        self._refuses({"w": {"targets": {"keys": [{"key": "a/b", "action": "destroy"}, "c/d"]}}})

    def test_the_real_ctl_cfg_declares_an_action_everywhere(self):
        import glob

        import yaml

        root = Path("/home/valerii/programs/atlas/cfg/oxygen/oxygen-ctl-cfg/workflows")
        if not root.is_dir():
            self.skipTest("oxygen ctl cfg not present")
        workflows = {}
        for path in glob.glob(str(root / "*.yaml")):
            workflows.update((yaml.safe_load(Path(path).read_text()) or {}).get("workflows") or {})
        self.assertGreater(len(workflows), 10)
        catalog_workflow.WorkflowCatalog.validate_actions_declared(workflows)


class DefaultActionResolutionTest(unittest.TestCase):
    """`default_action` is a literal, or a reference to the invocation."""

    CONTEXT = {"execution_context.params.operation": "destroy"}

    def test_a_literal_is_taken_as_is(self):
        self.assertEqual(
            "provision", run_selectors.resolve_default_action("provision", self.CONTEXT, label="wf")
        )

    def test_a_reference_follows_the_invocation(self):
        self.assertEqual(
            "destroy",
            run_selectors.resolve_default_action(
                "${execution_context.params.operation}", self.CONTEXT, label="wf"
            ),
        )

    def test_an_unbound_reference_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "not bound"):
            run_selectors.resolve_default_action(
                "${execution_context.params.operation}", {}, label="wf"
            )

    def test_none_stays_none(self):
        self.assertIsNone(run_selectors.resolve_default_action(None, self.CONTEXT, label="wf"))
