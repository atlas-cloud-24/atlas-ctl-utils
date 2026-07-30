"""A live run is visible in status, and outdating happens at mutation START.

Before this, both facts were recorded and neither was read: the in_progress slot
sat beside committed.yaml unopened, and mark_outdated_for_run ran only from the
run's terminal paths — so across the whole mutation window a dependent read
`current` while its dependency was actively changing.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


TARGET_SPEC = {
    "kind": "target",
    "key": "env/core",
    "segments": ["account=dev"],
    "address": "env/core/instances/account=dev",
    "prefix": "provision/target/env/core/instances/account=dev",
}


def _write_committed(namespace: Path, prefix: str, **facts) -> None:
    common.write_yaml_file(
        namespace / prefix / "committed.yaml",
        {"run_id": "r1", "status": "ok", **facts},
    )


def _write_in_progress(namespace: Path, prefix: str, **facts) -> None:
    common.write_yaml_file(
        namespace / prefix / "in_progress" / "STATUS.yaml",
        {"run_id": "r2", "action": "provision", "status": "in_progress", **facts},
    )


def _write_failed(namespace: Path, prefix: str, **facts) -> None:
    common.write_yaml_file(
        namespace / prefix / "failed" / "STATUS.yaml",
        {
            "run_id": "r3",
            "action": "provision",
            "status": "failed",
            "error": {"summary": "boom"},
            **facts,
        },
    )


class RunStatusAxisTest(unittest.TestCase):
    def test_committed_target_with_no_live_slot_stays_current(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, TARGET_SPEC["prefix"])
            result = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )
            self.assertEqual("passed", result["status"])
            self.assertEqual("provisioned", result["state"])

    def test_live_slot_makes_the_target_read_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, TARGET_SPEC["prefix"])
            _write_in_progress(namespace, TARGET_SPEC["prefix"])
            result = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )
            self.assertEqual("running", result["status"])
            self.assertIn("r2", result["reasons"][0])

    def test_running_outranks_destroyed(self):
        """A re-provision of a torn-down target must not read `destroyed`."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            for action, run_id, committed_at in (
                ("provision", "p1", "2026-01-01T00:00:00+00:00"),
                ("destroy", "d1", "2026-01-02T00:00:00+00:00"),
            ):
                prefix = common.compose_state_relpath(
                    action, "target", "env/core", ["account=dev"]
                )
                _write_committed(
                    namespace, str(prefix), run_id=run_id, committed_at=committed_at
                )
            spec = {**TARGET_SPEC, "prefix": "plan/target/env/core/instances/account=dev"}
            self.assertEqual(
                common.compute_target_instance_status(namespace, "destroy", spec).get("state"),
                "destroyed",
            )
            # A re-provision writes its slot under the PROVISION prefix while the
            # destroy pointer is still the newest committed thing. Reading slots
            # from the newest-committed direction alone missed it entirely.
            _write_in_progress(
                namespace,
                str(common.compose_state_relpath(
                    "provision", "target", "env/core", ["account=dev"]
                )),
            )
            live = common.compute_target_instance_status(namespace, "destroy", spec)
            self.assertEqual("running", live["status"])
            self.assertEqual("provision", live["action"])
            self.assertEqual("destroyed", live["state"])

    def test_reason_distinguishes_mutating_from_not_yet_mutating(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_in_progress(namespace, TARGET_SPEC["prefix"])
            not_yet = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )["reasons"][0]
            self.assertIn("not yet mutating", not_yet)

            _write_in_progress(namespace, TARGET_SPEC["prefix"], mutation_started=True)
            mutating = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )["reasons"][0]
            self.assertIn("mutating", mutating)
            self.assertNotIn("not yet", mutating)

    def test_workflow_reads_running_from_a_running_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            workflow = {
                "kind": "workflow",
                "key": "env/baseline",
                "segments": ["sha256-x"],
                "address": "env/baseline/sha256-x",
                "prefix": "provision/workflow/env/baseline/instances/sha256-x",
                "target_specs": [TARGET_SPEC],
                "workflow_definition_sha256": "wf",
            }
            _write_committed(namespace, workflow["prefix"], workflow_definition_sha256="wf")
            _write_committed(namespace, TARGET_SPEC["prefix"])
            _write_in_progress(namespace, TARGET_SPEC["prefix"], mutation_started=True)
            result = common.compute_workflow_instance_status(
                namespace, "provision", workflow
            )
            self.assertEqual("running", result["status"])


class FailedAxisTest(unittest.TestCase):
    """A failed run is durable and needs a human — `outdated` does not say that."""

    def test_a_mutating_failure_leaves_the_state_partial(self):
        """`partial` is an EXISTENCE fact, not a restatement of the failure.

        Resources were changed and no complete record describes them, which is
        what the destroy guard and `forget` must refuse on.
        """
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, TARGET_SPEC["prefix"])
            _write_failed(namespace, TARGET_SPEC["prefix"], mutation_started=True)
            result = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )
            self.assertEqual("partial", result["state"])
            self.assertEqual("failed", result["status"])
            self.assertEqual("none", result["freshness"])

    def test_a_failure_before_mutating_leaves_the_state_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, TARGET_SPEC["prefix"])
            _write_failed(namespace, TARGET_SPEC["prefix"])
            result = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )
            self.assertEqual("provisioned", result["state"])
            self.assertEqual("failed", result["status"])

    def test_partial_also_applies_mid_run(self):
        """Independent of `status`: a run still going is half-built too."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, TARGET_SPEC["prefix"])
            _write_in_progress(namespace, TARGET_SPEC["prefix"], mutation_started=True)
            result = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )
            self.assertEqual("partial", result["state"])
            self.assertEqual("running", result["status"])

    def test_a_plan_action_has_no_state_or_freshness(self):
        """A plan creates nothing, so asking what it left behind is meaningless."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            spec = {**TARGET_SPEC, "prefix": "plan/target/env/core/instances/account=dev"}
            _write_committed(namespace, spec["prefix"])
            result = common.compute_target_instance_status(namespace, "plan", spec)
            self.assertEqual("passed", result["status"])
            self.assertNotIn("state", result)
            self.assertNotIn("freshness", result)

    def test_failed_slot_beats_the_committed_pointer(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, TARGET_SPEC["prefix"])
            _write_failed(namespace, TARGET_SPEC["prefix"])
            result = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )
            self.assertEqual("failed", result["status"])
            self.assertIn("boom", result["reasons"][0])
            self.assertIn("r3", result["reasons"][0])

    def test_running_outranks_failed(self):
        """A retry is already underway, so the old failure is being answered."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_failed(namespace, TARGET_SPEC["prefix"])
            _write_in_progress(namespace, TARGET_SPEC["prefix"])
            self.assertEqual(
                common.compute_target_instance_status(
                    namespace, "provision", TARGET_SPEC
                )["status"],
                "running",
            )

    def test_reason_flags_a_failure_that_had_already_mutated(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_failed(namespace, TARGET_SPEC["prefix"], mutation_started=True)
            reason = common.compute_target_instance_status(
                namespace, "provision", TARGET_SPEC
            )["reasons"][0]
            self.assertIn("after mutation started", reason)

    def test_workflow_reads_failed_from_a_failed_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            workflow = {
                "kind": "workflow",
                "key": "env/baseline",
                "segments": ["sha256-x"],
                "address": "env/baseline/sha256-x",
                "prefix": "provision/workflow/env/baseline/instances/sha256-x",
                "target_specs": [TARGET_SPEC],
                "workflow_definition_sha256": "wf",
            }
            _write_committed(namespace, workflow["prefix"], workflow_definition_sha256="wf")
            _write_committed(namespace, TARGET_SPEC["prefix"])
            _write_failed(namespace, TARGET_SPEC["prefix"])
            self.assertEqual(
                common.compute_workflow_instance_status(
                    namespace, "provision", workflow
                )["status"],
                "failed",
            )

    def test_reprovision_after_destroy_is_visible_on_the_workflow_row(self):
        """The namespace row, not just the compute — this is what a user reads.

        A destroy commits, then a provision starts. Until it commits, the destroy
        pointer is still the newest, and the deployment group used to be built
        from that pointer alone with `status: passed` hardcoded — so the running
        provision showed up nowhere for its entire duration.
        """
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            key, segments = "env/seed", ["sha256=cb57fe50"]
            prefixes = {
                action: str(
                    common.compose_state_relpath(action, "workflow", key, segments)
                )
                for action in ("provision", "destroy")
            }
            _write_committed(
                namespace, prefixes["provision"],
                committed_at="2026-07-29T14:10:00+00:00",
            )
            _write_committed(
                namespace, prefixes["destroy"],
                committed_at="2026-07-29T14:20:01+00:00",
            )
            settled = common.compute_namespace_status_map(namespace)
            row = settled["workflow"][key]["instances"]["/".join(segments)]["deployment"]
            self.assertEqual("destroy", row["action"])
            self.assertEqual("passed", row["status"])

            _write_in_progress(namespace, prefixes["provision"])
            live = common.compute_namespace_status_map(namespace)
            row = live["workflow"][key]["instances"]["/".join(segments)]["deployment"]
            self.assertEqual("provision", row["action"])
            self.assertEqual("running", row["status"])
            self.assertEqual("destroyed", row["state"])

            _write_in_progress(
                namespace, prefixes["provision"], mutation_started=True
            )
            mutating = common.compute_namespace_status_map(namespace)
            row = mutating["workflow"][key]["instances"]["/".join(segments)]["deployment"]
            self.assertEqual("partial", row["state"])
            self.assertEqual("running", row["status"])

    def test_success_clears_the_failed_slot(self):
        """mark_run_succeeded removes it, so `failed` cannot outlive its run."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            run_dir = namespace / TARGET_SPEC["prefix"] / "runs" / "r4"
            run_dir.mkdir(parents=True)
            _write_failed(namespace, TARGET_SPEC["prefix"])
            common.remove_state_slot(run_dir, "failed")
            _write_committed(namespace, TARGET_SPEC["prefix"])
            self.assertEqual(
                common.compute_target_instance_status(
                    namespace, "provision", TARGET_SPEC
                )["status"],
                "passed",
            )


class OutdateAtMutationStartTest(unittest.TestCase):
    """mark_mutation_started must outdate the affected pointers immediately.

    Scope note: mark_outdated_for_run supersedes the SIBLING ACTIONS of the same
    target instance (this provision invalidating that instance's plan/destroy
    pointer), never other targets. The timing is what changes here, not the set.
    """

    def _run_dir(self, root: Path) -> Path:
        run_dir = root / "provision" / "target" / "env" / "core" / "instances" / "account=dev" / "runs" / "r9"
        run_dir.mkdir(parents=True)
        common.write_yaml_file(
            common.run_metadata_path(run_dir),
            {
                "run_id": "r9",
                "action": "provision",
                "run_type": "target",
                "result_key": "provision/target/env/core",
                "result_name": "env/core",
                "target_keys": ["env/core"],
                "target_addresses": ["env/core/instances/account=dev"],
                "ctl_state_local_root": str(root),
                "ctl_state_locator": [],
                "instance": ["account=dev"],
            },
        )
        return run_dir

    def _sibling(self, root: Path) -> Path:
        sibling = root / "plan" / "target" / "env" / "core" / "instances" / "account=dev"
        common.write_yaml_file(
            sibling / "committed.yaml",
            {"run_id": "old", "status": "ok", "target_keys": ["env/core"]},
        )
        return sibling

    def test_sibling_is_outdated_before_the_run_finishes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = self._run_dir(root)
            sibling = self._sibling(root)
            common.mark_mutation_started(run_dir, "tr1")
            pointer = common.load_yaml(sibling / "committed.yaml")
            self.assertEqual(pointer["status"], "outdated")
            self.assertEqual(pointer["outdated"]["caused_by"]["run_id"], "r9")

    def test_a_run_that_never_mutated_outdates_nothing(self):
        """The mutation_started guard still holds: preparation failures touch
        no resources, so they must not invalidate anyone's result."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = self._run_dir(root)
            sibling = self._sibling(root)
            common.mark_outdated_for_run(run_dir, include_current_result=False)
            self.assertEqual(
                common.load_yaml(sibling / "committed.yaml")["status"], "ok"
            )


if __name__ == "__main__":
    unittest.main()


class StatusGroupTest(unittest.TestCase):
    """A row is grouped by action CLASS, and an empty group is omitted.

    Before this, the namespace map keyed rows by address alone and always
    computed the provision perspective, so a planned-but-never-provisioned
    target appeared with no state and a `passed` nobody had earned.
    """

    def _map(self, namespace: Path):
        return common.compute_namespace_status_map(namespace)

    def test_a_planned_only_target_reports_plan_and_no_deployment(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, "plan/target/env/core/instances/account=dev")
            row = self._map(namespace)["target"]["env/core"]["instances"]["account=dev"]
            self.assertEqual({"status": "passed"}, row["plan"])
            self.assertNotIn("deployment", row)

    def test_provision_and_destroy_share_one_group(self):
        """They are two directions of the same state, not two things that happened."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, "provision/target/env/core/instances/account=dev")
            row = self._map(namespace)["target"]["env/core"]["instances"]["account=dev"]
            self.assertEqual({"deployment"}, set(row))
            self.assertEqual("provisioned", row["deployment"]["state"])

    def test_a_target_planned_then_provisioned_reports_both(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, "plan/target/env/core/instances/account=dev")
            _write_committed(namespace, "provision/target/env/core/instances/account=dev")
            row = self._map(namespace)["target"]["env/core"]["instances"]["account=dev"]
            self.assertEqual({"plan", "deployment"}, set(row))

    def test_nothing_ran_means_no_row_at_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual({}, self._map(Path(tmp)))

    def test_passed_requires_a_committed_pointer(self):
        """`passed` is a claim of success; absence of a failure is not one."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            status, _, _ = common._run_status(namespace / "nowhere")
            self.assertIsNone(status)


class StatusFilterTest(unittest.TestCase):
    """`--kind` and `--group` narrow what --all PRINTS; they select nothing."""

    MAP = {
        "target": {
            "env/core": {
                "instances": {
                    "account=dev": {
                        "plan": {"status": "passed"},
                        "deployment": {"state": "provisioned", "freshness": "current"},
                    }
                }
            },
            "env/acm": {"instances": {"account=dev": {"plan": {"status": "passed"}}}},
        },
        "workflow": {
            "env/baseline": {"instances": {"sha256=x": {"deployment": {"state": "provisioned"}}}}
        },
    }

    def test_kind_keeps_only_matching_rows(self):
        got = common.filter_status_map(self.MAP, ["workflow"], None)
        self.assertEqual({"workflow"}, set(got))
        self.assertEqual({"env/baseline"}, set(got["workflow"]))

    def test_group_keeps_only_matching_groups(self):
        got = common.filter_status_map(self.MAP, None, ["deployment"])
        self.assertEqual({"deployment"}, set(got["target"]["env/core"]["instances"]["account=dev"]))

    def test_a_row_left_with_no_group_is_dropped(self):
        """Not shown empty: an empty row reads as "nothing happened here", a
        different claim from "you asked not to see it"."""
        got = common.filter_status_map(self.MAP, None, ["deployment"])
        self.assertNotIn("env/acm", got["target"])

    def test_both_filters_compose(self):
        got = common.filter_status_map(self.MAP, ["target"], ["plan"])
        self.assertEqual({"target"}, set(got))
        self.assertEqual({"env/core", "env/acm"}, set(got["target"]))

    def test_no_filter_is_the_whole_map(self):
        self.assertEqual(self.MAP, common.filter_status_map(self.MAP, None, None))


class TemplateNestingTest(unittest.TestCase):
    """Instances are filed under the TEMPLATE they materialize.

    `kind -> template -> instances -> segments -> groups`, so every
    materialization of one declared key reads as a single block. A singleton has
    no segments and therefore no instance to name — its groups sit directly under
    the template, exactly as the state dir omits the `instances/` layer.
    """

    def test_instances_of_one_template_are_one_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            for account in ("dev", "stg"):
                _write_committed(
                    namespace,
                    f"provision/target/env/core/instances/account={account}",
                )
            block = common.compute_namespace_status_map(namespace)["target"]["env/core"]
            self.assertEqual({"account=dev", "account=stg"}, set(block["instances"]))

    def test_a_singleton_has_no_instances_level(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, "provision/target/env/core")
            block = common.compute_namespace_status_map(namespace)["target"]["env/core"]
            self.assertNotIn("instances", block)
            self.assertEqual("provisioned", block["deployment"]["state"])

    def test_filtering_a_singleton_template_drops_it_whole(self):
        singleton = {"target": {"env/core": {"plan": {"status": "passed"}}}}
        self.assertEqual({}, common.filter_status_map(singleton, None, ["deployment"]))


class StructureAndSortTest(unittest.TestCase):
    """`--structure` picks the shape; `--sort` picks the order within it.

    Grouping and global chronological order are in conflict: a tree must place a
    whole template before its siblings, so a strictly time-ordered sequence is
    only expressible as a flat list. That is the whole reason `flat` exists.
    """

    MAP = {
        "target": {
            "A": {"instances": {"i1": {"deployment": {"status": "passed", "at": "2026-01-01T00:00:00Z"}},
                                "i2": {"deployment": {"status": "passed", "at": "2026-01-03T00:00:00Z"}}}},
            "B": {"instances": {"i1": {"deployment": {"status": "passed", "at": "2026-01-02T00:00:00Z"}},
                                "i2": {"deployment": {"status": "passed", "at": "2026-01-04T00:00:00Z"}}}},
        }
    }

    def test_nested_sorts_templates_by_their_newest_instance(self):
        got = common.structure_status_map(self.MAP, "nested", "time:desc")
        self.assertEqual(["B", "A"], list(got["target"]))

    def test_nested_sorts_instances_within_a_template(self):
        got = common.structure_status_map(self.MAP, "nested", "time:desc")
        self.assertEqual(["i2", "i1"], list(got["target"]["B"]["instances"]))

    def test_flat_gives_a_strictly_chronological_sequence(self):
        """The order a tree cannot express: B/i2, A/i2, B/i1, A/i1."""
        got = common.structure_status_map(self.MAP, "flat", "time:desc")
        self.assertEqual(
            [
                "target/B/instances/i2",
                "target/A/instances/i2",
                "target/B/instances/i1",
                "target/A/instances/i1",
            ],
            [row["address"] for row in got["instances"]],
        )

    def test_flat_is_one_list_across_kinds(self):
        """Splitting by kind would break global order exactly as nesting does."""
        mixed = {
            **self.MAP,
            "workflow": {
                "W": {"instances": {"i1": {"deployment": {"status": "passed", "at": "2026-01-05T00:00:00Z"}}}}
            },
        }
        got = common.structure_status_map(mixed, "flat", "time:desc")
        self.assertEqual(["instances"], list(got))
        self.assertEqual("workflow/W/instances/i1", got["instances"][0]["address"])

    def test_flat_rows_carry_their_own_address_and_group(self):
        row = common.structure_status_map(self.MAP, "flat", "time:asc")["instances"][0]
        self.assertEqual("target/A/instances/i1", row["address"])
        self.assertEqual("deployment", row["group"])
        self.assertEqual("passed", row["status"])

    def test_address_sort_is_the_default_direction_ascending(self):
        got = common.structure_status_map(self.MAP, "nested", "address")
        self.assertEqual(["A", "B"], list(got["target"]))

    def test_unknown_sort_field_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "unknown"):
            common.parse_sort("size:desc")

    def test_unknown_sort_direction_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "asc or desc"):
            common.parse_sort("time:sideways")


class CrossDirectionRunStatusTest(unittest.TestCase):
    """Provision and destroy are two directions of ONE state.

    They live under separate action prefixes on disk, so a reader that takes run
    slots from the prefix of the newest COMMITTED pointer cannot see a run going
    the other way until it commits.
    """

    KEY = "env/core"
    SEGMENTS = ["account=dev"]

    def _prefix(self, action, kind="target"):
        return str(common.compose_state_relpath(action, kind, self.KEY, self.SEGMENTS))

    def _spec(self, action="provision"):
        return {
            "kind": "target",
            "key": self.KEY,
            "segments": self.SEGMENTS,
            "address": common.target_instance_address(self.KEY, self.SEGMENTS),
            "prefix": self._prefix(action),
        }

    def test_a_destroy_running_on_a_provisioned_target_is_visible(self):
        """The reverse direction of the reported bug."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, self._prefix("provision"))
            _write_in_progress(namespace, self._prefix("destroy"), action="destroy")
            row = common.compute_target_instance_status(
                namespace, "provision", self._spec()
            )
            self.assertEqual("destroy", row["action"])
            self.assertEqual("running", row["status"])
            self.assertEqual("provisioned", row["state"])

    def test_a_failure_in_the_other_direction_is_visible(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(
                namespace, self._prefix("destroy"),
                committed_at="2026-01-02T00:00:00+00:00",
            )
            _write_failed(namespace, self._prefix("provision"))
            row = common.compute_target_instance_status(
                namespace, "destroy", self._spec("destroy")
            )
            self.assertEqual("provision", row["action"])
            self.assertEqual("failed", row["status"])
            self.assertEqual("destroyed", row["state"])

    def test_a_live_run_outranks_a_stale_slot_left_by_the_other_direction(self):
        """One live run per instance, but a slot can outlive the run that wrote it."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, self._prefix("provision"))
            _write_failed(
                namespace, self._prefix("destroy"),
                action="destroy", updated_at="2026-01-01T00:00:00+00:00",
            )
            _write_in_progress(
                namespace, self._prefix("provision"),
                updated_at="2026-01-02T00:00:00+00:00",
            )
            row = common.compute_target_instance_status(
                namespace, "provision", self._spec()
            )
            self.assertEqual("running", row["status"])
            self.assertEqual("provision", row["action"])

    def test_the_freshest_slot_of_a_kind_decides(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_committed(namespace, self._prefix("provision"))
            _write_failed(
                namespace, self._prefix("provision"),
                updated_at="2026-01-01T00:00:00+00:00",
            )
            _write_failed(
                namespace, self._prefix("destroy"),
                action="destroy", updated_at="2026-01-03T00:00:00+00:00",
            )
            row = common.compute_target_instance_status(
                namespace, "provision", self._spec()
            )
            self.assertEqual("failed", row["status"])
            self.assertEqual("destroy", row["action"])

    def test_a_first_ever_run_reports_before_anything_is_committed(self):
        """No pointer in either direction, but the run is already live."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_in_progress(namespace, self._prefix("provision"))
            row = common.compute_target_instance_status(
                namespace, "provision", self._spec()
            )
            self.assertEqual("running", row["status"])
            self.assertEqual("provision", row["action"])
            self.assertNotIn("state", row)

    def test_a_first_ever_workflow_run_reports_before_anything_is_committed(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_in_progress(
                namespace, self._prefix("provision", kind="workflow"),
                mutation_started=True,
            )
            axes = common._deployment_workflow_axes(
                namespace, self.KEY, self.SEGMENTS
            )
            self.assertEqual(
                {"action": "provision", "state": "partial", "status": "running"}, axes
            )

    def test_a_plan_run_still_reads_only_its_own_prefix(self):
        """A plan owns no state, so the lifecycle directions say nothing about it."""
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            _write_in_progress(namespace, self._prefix("provision"))
            spec = {**self._spec(), "prefix": self._prefix("plan")}
            row = common.compute_target_instance_status(namespace, "plan", spec)
            self.assertNotIn("status", row)
            self.assertNotIn("state", row)


class WorkflowSpawnMutationMarkTest(unittest.TestCase):
    """A workflow marks its OWN mutation when it spawns a mutating child.

    The spawn branch ends in `continue`, so it never reaches the inline mark used
    by a target running its own steps. Without a mark of its own a workflow run's
    slot reads `mutation_started: false` however much its children changed, and
    `partial` cannot surface on the composition row.
    """

    KEY = "env/seed"
    SEGMENTS = ["sha256=x"]

    def _run(self, tmp, inventory_name, on_spawn=None):
        from unittest import mock

        namespace = Path(tmp) / "live"
        instance_dir = namespace / common.compose_state_relpath(
            inventory_name, "workflow", self.KEY, self.SEGMENTS
        )
        run_dir = instance_dir / "runs" / "r1"
        run_dir.mkdir(parents=True)
        payload = {
            "run_id": "r1",
            "run_type": "workflow",
            "action": inventory_name,
            "status": "in_progress",
            "mutation_started": False,
            "updated_at": common.utc_timestamp(),
        }
        common.write_yaml_file(common.run_metadata_path(run_dir), payload)
        common.write_state_slot(run_dir, "in_progress", payload)

        def spawn(*args, **kwargs):
            if on_spawn is not None:
                on_spawn(instance_dir)

        cwd = Path.cwd()
        try:
            with mock.patch.multiple(
                common,
                build_tooling_env=mock.DEFAULT,
                materialize_step_utils=mock.DEFAULT,
                build_child_target_command=mock.DEFAULT,
                mint_child_lock_grant=mock.DEFAULT,
                latest_child_revision=mock.DEFAULT,
                run_and_log=mock.DEFAULT,
                ctl_state_push=mock.DEFAULT,
                mark_outdated_for_run=mock.DEFAULT,
            ) as patched:
                patched["build_tooling_env"].return_value = {}
                patched["materialize_step_utils"].return_value = run_dir
                patched["build_child_target_command"].return_value = ["ctl.py"]
                patched["mint_child_lock_grant"].return_value = "grant"
                patched["latest_child_revision"].return_value = None
                patched["run_and_log"].side_effect = spawn
                common.run_targets(
                    {"tr1": {"target": "env/seed/baseline"}},
                    run_dir,
                    Path(tmp),
                    Path(tmp) / "ctx.yaml",
                    inventory_name,
                    {},
                    "r1",
                    {},
                    False,
                    None,
                    {},
                    "aws",
                    "local",
                    child_command_spec={"ctl_state_local_root": str(tmp)},
                )
        finally:
            os.chdir(cwd)
        return common.read_instance_state_slot(instance_dir, "in_progress")

    def test_spawning_a_provision_child_marks_the_workflow(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIs(True, self._run(tmp, "provision").get("mutation_started"))

    def test_spawning_a_destroy_child_marks_the_workflow(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIs(True, self._run(tmp, "destroy").get("mutation_started"))

    def test_the_mark_lands_before_the_child_runs(self):
        """Marking after the spawn would leave the whole child run unrecorded."""
        seen = {}

        def capture(instance_dir):
            seen["at_spawn"] = common.read_instance_state_slot(
                instance_dir, "in_progress"
            ).get("mutation_started")

        with tempfile.TemporaryDirectory() as tmp:
            self._run(tmp, "provision", on_spawn=capture)
            self.assertIs(True, seen["at_spawn"])

    def test_a_plan_child_marks_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIs(False, self._run(tmp, "plan").get("mutation_started"))
