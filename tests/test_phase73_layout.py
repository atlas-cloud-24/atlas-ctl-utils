"""§Phase 73: state is partitioned by status GROUP, not by action.

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

from utils import common  # noqa: E402


KEY, SEGMENTS = "env/seed/baseline", ["env.type=dev", "aws.account=dev"]
SPEC = {
    "kind": "target",
    "key": KEY,
    "segments": SEGMENTS,
    "address": common.target_instance_address(KEY, SEGMENTS),
}


def _instance(namespace: Path) -> Path:
    return namespace / common.compose_state_relpath("target", KEY, SEGMENTS)


def _publish(namespace: Path, group: str, **facts) -> None:
    common.write_yaml_file(
        common.committed_pointer_path(_instance(namespace), group),
        {"run_id": "r1", "status": "ok", "committed_at": "2026-07-30T10:00:00Z", **facts},
    )


def _slot(namespace: Path, state: str, group: str, **facts) -> None:
    common.write_yaml_file(
        common.state_slot_dir(_instance(namespace), state, group) / "STATUS.yaml",
        {"run_id": "r2", "status": state, **facts},
    )


class PathShapeTest(unittest.TestCase):
    def test_the_path_carries_no_action(self):
        self.assertEqual(
            "target/env/seed/baseline/instances/env.type=dev/aws.account=dev",
            common.compose_state_relpath("target", KEY, SEGMENTS).as_posix(),
        )

    def test_a_singleton_has_no_instances_layer(self):
        self.assertEqual(
            "workflow/org/bootstrap_admin",
            common.compose_state_relpath("workflow", "org/bootstrap_admin", []).as_posix(),
        )

    def test_an_unknown_kind_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unknown state kind"):
            common.compose_state_relpath("fan_out", "x", [])

    def test_the_pointer_lives_under_its_group(self):
        self.assertEqual(
            "committed/deployment.yaml",
            common.committed_pointer_path(Path("i"), "deployment").relative_to("i").as_posix(),
        )

    def test_an_unknown_group_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unknown state group"):
            common.committed_pointer_path(Path("i"), "nonsense")

    def test_parsing_is_the_inverse(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            parsed = common.parse_state_relpath(namespace, _instance(namespace))
            self.assertEqual("target", parsed["kind"])
            self.assertEqual(KEY, parsed["key"])
            self.assertEqual(SEGMENTS, parsed["instance_segments"])
            self.assertNotIn("action", parsed)


class ActionMapsToGroupTest(unittest.TestCase):
    def test_the_two_directions_share_one_group(self):
        self.assertEqual("deployment", common.action_group("provision"))
        self.assertEqual("deployment", common.action_group("destroy"))

    def test_the_independent_facts_keep_their_own(self):
        self.assertEqual("plan", common.action_group("plan"))
        self.assertEqual("readonly", common.action_group("readonly"))
        self.assertEqual("maintenance", common.action_group("maintenance"))

    def test_an_unknown_action_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unknown action"):
            common.action_group("rebuild")


class GroupsDoNotOverwriteTest(unittest.TestCase):
    """The reason the partition exists: a plan must not erase a deployment."""

    def test_a_plan_and_a_deployment_pointer_coexist(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "deployment", run_id="provisioned")
            _publish(namespace, "plan", run_id="planned")
            self.assertEqual(
                "provisioned",
                common.read_committed_pointer(_instance(namespace), "deployment")["run_id"],
            )
            self.assertEqual(
                "planned",
                common.read_committed_pointer(_instance(namespace), "plan")["run_id"],
            )

    def test_a_failed_plan_and_a_failed_deployment_coexist(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _slot(namespace, "failed", "plan", run_id="p")
            _slot(namespace, "failed", "deployment", run_id="d")
            self.assertEqual(
                "p", common.read_instance_state_slot(_instance(namespace), "failed", "plan")["run_id"]
            )
            self.assertEqual(
                "d",
                common.read_instance_state_slot(_instance(namespace), "failed", "deployment")["run_id"],
            )

    def test_a_deployment_run_publishes_only_its_own_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            run_dir = _instance(namespace) / "runs" / "r9"
            run_dir.mkdir(parents=True)
            common.write_run_metadata(run_dir, {
                "run_id": "r9", "action": "destroy", "run_type": "target",
                "result_name": KEY, "ctl_state_local_root": str(namespace),
                "ctl_state_locator": [],
            })
            common.publish_committed_pointer(
                run_dir, common.build_status_payload(run_dir, "ok")
            )
            published = sorted(
                q.name for q in (_instance(namespace) / "committed").iterdir()
            )
            self.assertEqual(["deployment.yaml"], published)


class RowShapeTest(unittest.TestCase):
    def test_a_row_carries_status_freshness_and_at_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "deployment", action="provision")
            row = common.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertEqual("passed", row["status"])
            self.assertEqual("up_to_date", row["freshness"])
            self.assertNotIn("state", row)
            self.assertNotIn("action", row)

    def test_the_canonical_order_is_status_freshness_at(self):
        self.assertEqual(
            ["status", "freshness", "at"],
            list(common.order_axes({"at": "t", "freshness": "outdated", "status": "passed"})),
        )

    def test_a_live_slot_reads_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "deployment", action="provision")
            _slot(namespace, "in_progress", "deployment", action="provision")
            self.assertEqual(
                "running",
                common.compute_target_instance_status(namespace, "provision", SPEC)["status"],
            )

    def test_a_destroyed_instance_reports_no_freshness(self):
        """Nothing is left for the inputs to have moved away from."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "deployment", action="destroy")
            row = common.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertEqual("passed", row["status"])
            self.assertNotIn("freshness", row)

    def test_an_interrupted_run_reports_no_freshness(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "deployment", action="provision")
            _slot(namespace, "failed", "deployment", action="provision", mutation_started=True)
            row = common.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertEqual("failed", row["status"])
            self.assertNotIn("freshness", row)

    def test_nothing_ran_means_no_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            row = common.compute_target_instance_status(namespace, "provision", SPEC)
            self.assertNotIn("status", row)


class NamespaceMapTest(unittest.TestCase):
    def test_one_instance_reports_each_group_it_published(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _publish(namespace, "deployment", action="provision")
            _publish(namespace, "plan", action="plan")
            rows = common.compute_namespace_status_map(namespace)
            instance = rows["target"][KEY]["instances"]["/".join(SEGMENTS)]
            self.assertEqual({"plan", "deployment"}, set(instance))
            self.assertEqual("passed", instance["deployment"]["status"])
            self.assertEqual("passed", instance["plan"]["status"])


if __name__ == "__main__":
    unittest.main()


class MemberEntryActionTest(unittest.TestCase):
    """§Phase 73: a workflow member entry names a target, optionally with its own
    action. The action belongs to the target, not to the member list."""

    def test_a_bare_key_takes_the_declared_default(self):
        self.assertEqual(
            [("env/ops/ecr", "provision")],
            common.normalize_target_entries(
                ["env/ops/ecr"], label="wf", default_action="provision"
            ),
        )

    def test_a_bare_key_with_no_default_is_refused(self):
        """Not runnable: the engine cannot know what to do with the target."""
        with self.assertRaisesRegex(RuntimeError, "has no action"):
            common.normalize_target_entries(["env/ops/ecr"], label="wf")

    def test_a_mapping_declares_its_own_action(self):
        self.assertEqual(
            [("env/ops/app", "destroy")],
            common.normalize_target_entries(
                [{"key": "env/ops/app", "action": "destroy"}], label="wf"
            ),
        )

    def test_a_key_may_repeat_with_differing_actions(self):
        """Order is load-bearing: the last write to the pointer wins."""
        entries = common.normalize_target_entries(
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
            common.normalize_target_entries(
                ["env/ops/ecr", {"key": "env/ops/ecr"}], label="wf",
                default_action="provision",
            )

    def test_an_unknown_action_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "declares action"):
            common.normalize_target_entries(
                [{"key": "env/ops/app", "action": "rebuild"}], label="wf"
            )

    def test_an_unsupported_field_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "unsupported keys"):
            common.normalize_target_entries(
                [{"key": "env/ops/app", "params": {"env.type": "dev"}}], label="wf"
            )

    def test_keys_only_is_still_the_inventory_shape(self):
        self.assertEqual(
            ["a/b", "c/d"],
            common.normalize_target_keys(
                ["a/b", {"key": "c/d", "action": "destroy"}], label="wf",
            ),
        )


class MemberActionReachesTheChildTest(unittest.TestCase):
    """The declaration is worthless unless the child actually runs that action."""

    WORKFLOWS = {
        "env/baseline": {
            "default_action": "provision",
            "target_keys": [
                "env/core/baseline",
                {"key": "env/ops/app", "action": "destroy"},
            ],
        }
    }

    def test_import_expansion_keeps_the_declared_action(self):
        runs = common.expand_workflow_imports(self.WORKFLOWS, "env/baseline")
        self.assertEqual(
            {"id": "env/core/baseline", "target": "env/core/baseline",
             "action": "provision"}, runs[0])
        self.assertEqual({"id": "env/ops/app", "target": "env/ops/app",
                          "action": "destroy"}, runs[1])

    def test_a_repeated_key_with_differing_actions_expands(self):
        runs = common.expand_workflow_imports(
            {
                "env/seed": {
                    "default_action": "provision",
                    "target_keys": [
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
            common.expand_workflow_imports(
                {"w": {"default_action": "provision",
                       "target_keys": ["a/b", {"key": "a/b"}]}},
                "w",
            )

    def test_the_member_actions_are_collected_for_the_inventory(self):
        cfg = {"target_runs": common.expand_workflow_imports(self.WORKFLOWS, "env/baseline")}
        self.assertEqual({"provision", "destroy"},
                         common.workflow_member_actions(cfg))

    def test_the_child_argv_carries_the_member_action(self):
        spec = {
            "ctl_entrypoint": "ctl.py", "ctl_cfg_root": "/cfg",
            "ctl_profile": "local_dev", "ctl_state_local_root": "/state",
            "execution_runtime_mode": "local", "action": "provision",
        }
        argv = common.build_child_target_command(
            spec, "env/ops/app",
            parent_run_dir=Path("/state/runs/w1"), parent_run_id="w1",
            action="destroy",
        )
        self.assertEqual("destroy", argv[argv.index("--action") + 1])

    def test_a_child_without_a_declared_action_inherits_the_operation(self):
        spec = {
            "ctl_entrypoint": "ctl.py", "ctl_cfg_root": "/cfg",
            "ctl_profile": "local_dev", "ctl_state_local_root": "/state",
            "execution_runtime_mode": "local", "action": "provision",
        }
        argv = common.build_child_target_command(
            spec, "env/core/baseline",
            parent_run_dir=Path("/state/runs/w1"), parent_run_id="w1",
        )
        self.assertEqual("provision", argv[argv.index("--action") + 1])


class OperationSpellingTest(unittest.TestCase):
    """A workflow declares `operations:`, a target declares `actions:`."""

    def test_a_workflow_may_spell_it_operations(self):
        self.assertEqual(
            ["plan", "provision"],
            common.entry_actions({"operations": ["plan", "provision"]}, label="wf"),
        )

    def test_a_target_still_spells_it_actions(self):
        self.assertEqual(
            ["provision"],
            common.entry_actions({"actions": ["provision"]}, label="tg"),
        )

    def test_declaring_both_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "both 'actions' and 'operations'"):
            common.entry_actions(
                {"actions": ["provision"], "operations": ["provision"]}, label="wf"
            )

    def test_neither_is_still_refused(self):
        with self.assertRaisesRegex(RuntimeError, "must declare"):
            common.entry_actions({}, label="wf")


class DispatchGuardTest(unittest.TestCase):
    """§Phase 32/73: a member may dispatch only on a fact that determines the
    instance path."""

    def _members(self, ref, value="provision"):
        return [{"target_keys": ["a/b"], "selectors": {"match": {ref: value}}}]

    def test_dispatching_on_the_operation_is_allowed(self):
        self.assertEqual(
            set(),
            common.collect_member_dispatch_axes(
                self._members("execution_context.ctl.operation"), label="wf"
            ),
        )

    def test_dispatching_on_the_action_is_still_allowed(self):
        self.assertEqual(
            set(),
            common.collect_member_dispatch_axes(
                self._members("execution_context.ctl.action"), label="wf"
            ),
        )

    def test_dispatching_on_a_params_axis_returns_it(self):
        self.assertEqual(
            {"env.type"},
            common.collect_member_dispatch_axes(
                self._members("execution_context.params.env.type", "dev"), label="wf"
            ),
        )

    def test_dispatching_on_any_other_ctl_fact_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "is not allowed"):
            common.collect_member_dispatch_axes(
                self._members("execution_context.ctl.profile", "local_dev"), label="wf"
            )


class CompositionIdentityTest(unittest.TestCase):
    """A member is (address, action), so opposite compositions are distinct.

    Hashing addresses alone made a teardown of a target and a deploy of the same
    target hash identically — one instance, one pointer, each overwriting the
    other. The defect only became reachable once members carried their own action.
    """

    ADDRESS = "env/seed/baseline/instances/env.type=dev"

    def test_opposite_actions_are_different_compositions(self):
        self.assertNotEqual(
            common.workflow_composition_sha256([self.ADDRESS], ["provision"]),
            common.workflow_composition_sha256([self.ADDRESS], ["destroy"]),
        )

    def test_the_same_members_and_actions_are_one_composition(self):
        """Two intents doing identical work ARE the same composition."""
        self.assertEqual(
            common.workflow_composition_sha256([self.ADDRESS], ["provision"]),
            common.workflow_composition_sha256([self.ADDRESS], ["provision"]),
        )

    def test_order_still_distinguishes(self):
        a, b = self.ADDRESS, "env/ops/app/instances/env.type=dev"
        self.assertNotEqual(
            common.workflow_composition_sha256([a, b], ["destroy", "provision"]),
            common.workflow_composition_sha256([b, a], ["provision", "destroy"]),
        )

    def test_a_repeated_key_with_differing_actions_is_its_own_composition(self):
        rebuild = common.workflow_composition_sha256(
            [self.ADDRESS, self.ADDRESS], ["destroy", "provision"]
        )
        deploy = common.workflow_composition_sha256([self.ADDRESS], ["provision"])
        self.assertNotEqual(rebuild, deploy)

    def test_a_mismatched_action_list_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "one action per address"):
            common.workflow_composition_sha256([self.ADDRESS], ["provision", "destroy"])

    def test_addresses_alone_still_hash(self):
        """Callers that have no per-member action pass none."""
        self.assertEqual(8, len(common.workflow_composition_sha256([self.ADDRESS])))


class ActionMustBeDeclaredTest(unittest.TestCase):
    """A cfg gate: a workflow entry the engine cannot run is refused at validation.

    Without this, an entry with no action and no `default_action` produced
    `action=None` and the engine simply did not know what to do with the target.
    """

    def _refuses(self, workflows):
        with self.assertRaisesRegex(RuntimeError, "no action"):
            common.validate_workflow_actions_declared(workflows)

    def test_a_bare_list_with_no_default_is_refused(self):
        self._refuses({"w": {"target_keys": ["a/b"]}})

    def test_a_member_with_no_default_is_refused(self):
        self._refuses({"w": {"target_keys": {"members": [
            {"target_keys": ["a/b"], "selectors": {}}]}}})

    def test_a_declared_default_is_accepted(self):
        common.validate_workflow_actions_declared(
            {"w": {"default_action": "provision", "target_keys": ["a/b"]}}
        )

    def test_per_key_actions_without_a_default_are_accepted(self):
        common.validate_workflow_actions_declared(
            {"w": {"target_keys": [{"key": "a/b", "action": "destroy"}]}}
        )

    def test_one_bare_key_among_declared_ones_is_still_refused(self):
        self._refuses({"w": {"target_keys": [
            {"key": "a/b", "action": "destroy"}, "c/d"]}})

    def test_the_real_ctl_cfg_declares_an_action_everywhere(self):
        import glob

        import yaml

        root = Path("/home/valerii/programs/atlas/cfg/oxygen/oxygen-ctl-cfg/workflows")
        if not root.is_dir():
            self.skipTest("oxygen ctl cfg not present")
        workflows = {}
        for path in glob.glob(str(root / "*.yaml")):
            workflows.update((yaml.safe_load(open(path)) or {}).get("workflows") or {})
        self.assertGreater(len(workflows), 10)
        common.validate_workflow_actions_declared(workflows)


class DefaultActionResolutionTest(unittest.TestCase):
    """`default_action` is a literal, or a reference to the invocation."""

    CONTEXT = {"execution_context.ctl.operation": "destroy"}

    def test_a_literal_is_taken_as_is(self):
        self.assertEqual("provision", common.resolve_default_action(
            "provision", self.CONTEXT, label="wf"))

    def test_a_reference_follows_the_invocation(self):
        self.assertEqual("destroy", common.resolve_default_action(
            "${execution_context.ctl.operation}", self.CONTEXT, label="wf"))

    def test_an_unbound_reference_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "not bound"):
            common.resolve_default_action(
                "${execution_context.ctl.operation}", {}, label="wf")

    def test_none_stays_none(self):
        self.assertIsNone(common.resolve_default_action(None, self.CONTEXT, label="wf"))
