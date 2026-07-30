import sys
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402
from utils.providers import aws  # noqa: E402


class Phase31StateTests(unittest.TestCase):
    def tearDown(self):
        common._CTL_STATE_SYNCER = None
        common._CTL_STATE_DEFER_CONFIG = None
        common._CTL_STATE_SYNC_NOTE = {"mode": "disabled"}

    def test_target_address_maps_to_hive_path(self):
        self.assertEqual(
            common.ctl_state_target_address_prefix(
                "provision", "env/core/instances/account=dev/env_type=dev"
            ),
            "provision/target/env/core/instances/account=dev/env_type=dev",
        )

    def test_duplicate_fan_out_materializations_fail(self):
        selection = {
            "selection_kind": "target",
            "selection_key": "env/core",
            "workflow_cfg": {"meta": {"action": "provision"}},
            "execution_context": {"execution_context.params.account": "dev"},
            "active_target_runs": {
                "env/core": {
                    "target": "env/core",
                    "target_instance_params": ["account"],
                }
            },
        }
        with self.assertRaisesRegex(RuntimeError, "duplicate state owners"):
            common.validate_unique_fan_out_materializations(
                [selection, dict(selection)]
            )

    def test_committed_rerun_requires_matching_clean_commits(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parent = (
                root
                / "live/provision/workflow/env/baseline/instances/sha256-x/runs/w1"
            )
            parent.mkdir(parents=True)
            common.write_run_metadata(
                parent,
                {
                    "run_id": "w1",
                    "action": "provision",
                    "run_type": "workflow",
                    "result_name": "env/baseline",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                },
            )
            run_dir = (
                root
                / "live/provision/target/env/core/instances/account=dev/runs/r1"
            )
            run_dir.mkdir(parents=True)
            facts = {
                "source_commit": "a" * 40,
                "cfg_source_commit": "b" * 40,
                "source_state": "clean",
                "ref_policy": "commit_required",
                "target_definition_sha256": "c" * 64,
                "target_cfg_view_sha256": "d" * 64,
            }
            common.write_run_metadata(
                run_dir,
                {
                    "run_id": "r1",
                    "action": "provision",
                    "run_type": "target",
                    "result_name": "env/core",
                    "result_key": "provision/target/env/core",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                    "instance": ["account=dev"],
                    "instance_address": "env/core/instances/account=dev",
                    **facts,
                },
            )
            payload = common.build_status_payload(run_dir, "ok")
            common.publish_committed_pointer(run_dir, payload)
            target_run = {
                "target": "env/core",
                "target_instance_params": ["account"],
                **facts,
            }
            context = {"execution_context.params.account": "dev"}
            revision = common.committed_target_revision_if_skippable(
                parent, target_run, context
            )
            self.assertEqual(revision["run_id"], "r1")
            target_run["source_state"] = "dirty"
            self.assertIsNone(
                common.committed_target_revision_if_skippable(
                    parent, target_run, context
                )
            )

    def test_destroy_is_computed_from_newest_lifecycle_revision(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            spec = {
                "kind": "target",
                "key": "env/core",
                "segments": ["account=dev"],
                "address": "env/core/instances/account=dev",
                "prefix": "plan/target/env/core/instances/account=dev",
            }
            for action, run_id, committed_at in (
                ("provision", "p1", "2026-01-01T00:00:00+00:00"),
                ("destroy", "d1", "2026-01-02T00:00:00+00:00"),
            ):
                path = namespace / common.compose_state_relpath(
                    action, "target", "env/core", ["account=dev"]
                )
                common.write_yaml_file(
                    path / "committed.yaml",
                    {
                        "run_id": run_id,
                        "status": "ok",
                        "committed_at": committed_at,
                    },
                )
            result = common.compute_target_instance_status(
                namespace, "destroy", spec
            )
            self.assertEqual("destroyed", result["state"])

    def test_workflow_status_detects_child_revision_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            target = {
                "kind": "target",
                "key": "env/core",
                "segments": ["account=dev"],
                "address": "env/core/instances/account=dev",
                "prefix": "provision/target/env/core/instances/account=dev",
            }
            workflow = {
                "kind": "workflow",
                "key": "env/baseline",
                "segments": ["sha256-x"],
                "address": "env/baseline/sha256-x",
                "prefix": "provision/workflow/env/baseline/instances/sha256-x",
                "target_specs": [target],
                "workflow_definition_sha256": "definition",
            }
            common.write_yaml_file(
                namespace / target["prefix"] / "committed.yaml",
                {
                    "run_id": "child-1",
                    "snapshot_sha256": "child-sha-1",
                    "status": "ok",
                },
            )
            common.write_yaml_file(
                namespace / workflow["prefix"] / "committed.yaml",
                {
                    "run_id": "workflow-1",
                    "status": "ok",
                    "workflow_definition_sha256": "definition",
                    "target_addresses": [target["address"]],
                    "child_revisions": [
                        {
                            "address": target["address"],
                            "run_id": "child-1",
                            "snapshot_sha256": "child-sha-1",
                        }
                    ],
                },
            )
            current = common.compute_workflow_instance_status(
                namespace, "provision", workflow
            )
            self.assertEqual("current", current["freshness"])
            pointer = common.load_yaml(
                namespace / target["prefix"] / "committed.yaml"
            )
            pointer["run_id"] = "child-2"
            common.write_yaml_file(
                namespace / target["prefix"] / "committed.yaml", pointer
            )
            outdated = common.compute_workflow_instance_status(
                namespace, "provision", workflow
            )
            self.assertEqual("outdated", outdated["freshness"])
            self.assertIn(
                "env/core/instances/account=dev: committed revision changed",
                outdated["reasons"],
            )

    def test_pending_manifest_drains_runs_before_pointers(self):
        class FakeSyncer:
            def __init__(self, results_root):
                self.results_root = results_root
                self.events = []

            def publish_identity(self, path):
                self.events.append(("identity", path.name))

            def push_run(self, path, reason):
                self.events.append(("run", path.name))

            def publish_committed_pointer(self, path):
                self.events.append(("pointer", path.parent.name))

            def summary(self):
                return {"mode": "synced"}

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "live/provision/target/env/core/runs/r1"
            run_dir.mkdir(parents=True)
            common.write_run_metadata(
                run_dir,
                {
                    "run_id": "r1",
                    "action": "provision",
                    "run_type": "target",
                    "result_name": "env/core",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                    "ctl_state_namespace": "live",
                },
            )
            common.write_current_status(
                run_dir, common.build_status_payload(run_dir, "ok")
            )
            pointer = common.publish_committed_pointer(
                run_dir, common.build_status_payload(run_dir, "ok")
            )
            manifest = common.queue_ctl_state_run(run_dir, pointer)
            self.assertTrue(manifest.is_file())
            syncer = FakeSyncer(root / "live")
            common._CTL_STATE_SYNCER = syncer
            self.assertEqual(common.drain_pending_ctl_state_sync(), 1)
            self.assertEqual(
                [kind for kind, _ in syncer.events], ["run", "pointer"]
            )
            self.assertFalse(manifest.exists())



class Phase56OverlayTests(unittest.TestCase):
    context = {"execution_context.params.env.type": "dev"}

    @staticmethod
    def add_overlay(root: Path, name: str, payload: dict) -> None:
        overlay_root = root / "_overlays" / name
        common.write_yaml_file(
            overlay_root / "__meta__.yaml",
            {
                "type": "overlay",
                "name": name,
                "selectors": {
                    "in": {"execution_context.params.env.type": ["dev"]}
                },
            },
        )
        common.write_yaml_file(overlay_root / "env" / "common.yaml", payload)



    def test_active_target_keeps_cfg_overlay_and_backend_facts(self):
        active = common.build_active_target_runs(
            {"target_runs": [{"id": "run", "target": "target"}]},
            {
                "target_sources": {
                    "source": {"repo_path": "/tmp/source"}
                },
                "targets": {
                    "target": {
                        "source": "source",
                        "ref": "context",
                        "procedure": "steps",
                        "domains": ["env"],
                        "cfg_keys": {"env": ["*"]},
                        "requires_plt_overlays": ["required"],
                        "provisions_ctl_state_backend": True,
                    }
                },
            },
            repo_key="repo_path",
            require_branch_or_commit=False,
        )["run"]
        self.assertEqual(active["domains"], ["env"])
        self.assertEqual(active["cfg_keys"], {"env": ["*"]})
        self.assertEqual(active["requires_plt_overlays"], ["required"])
        self.assertTrue(active["provisions_ctl_state_backend"])

    def test_explicit_overlay_order_changes_merge_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "source"
            self.add_overlay(root, "first", {"service": {"mode": "first"}})
            self.add_overlay(root, "second", {"service": {"mode": "second"}})
            left = Path(tmp) / "left"
            right = Path(tmp) / "right"
            left.mkdir()
            right.mkdir()
            common.apply_selected_overlays_to_cfg_root(
                root,
                left,
                ["first", "second"],
                execution_context=self.context,
            )
            common.apply_selected_overlays_to_cfg_root(
                root,
                right,
                ["second", "first"],
                execution_context=self.context,
            )
            self.assertEqual(
                common.load_yaml(left / "env" / "common.yaml")["service"]["mode"],
                "second",
            )
            self.assertEqual(
                common.load_yaml(right / "env" / "common.yaml")["service"]["mode"],
                "first",
            )

    def test_required_overlays_append_in_target_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("explicit", "required_a", "required_b"):
                self.add_overlay(root, name, {name: {"enabled": True}})
            resolved = common.resolve_run_plt_overlays(
                root,
                ["explicit"],
                {
                    "first": {"requires_plt_overlays": ["required_a"]},
                    "second": {
                        "requires_plt_overlays": ["required_a", "required_b"]
                    },
                },
                execution_context=self.context,
            )
            self.assertEqual(
                resolved, ["explicit", "required_a", "required_b"]
            )

    def test_automatic_conflict_requires_explicit_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.add_overlay(root, "explicit", {"service": {"mode": "one"}})
            self.add_overlay(root, "required", {"service": {"mode": "two"}})
            active = {
                "target": {"requires_plt_overlays": ["required"]}
            }
            with self.assertRaisesRegex(
                RuntimeError, "complete ordered overlay list explicitly"
            ):
                common.resolve_run_plt_overlays(
                    root,
                    ["explicit"],
                    active,
                    execution_context=self.context,
                )
            self.assertEqual(
                common.resolve_run_plt_overlays(
                    root,
                    ["explicit", "required"],
                    active,
                    execution_context=self.context,
                ),
                ["explicit", "required"],
            )

    def test_target_definition_hash_uses_resolved_stable_fields(self):
        target = {
            "target": "env/core",
            "source": "foundation",
            "ref": "env",
            "commit": "a" * 40,
            "procedure": "baseline",
            "domains": ["env"],
            "cfg_keys": {"env": ["*"]},
            "target_instance_params": ["account"],
            "requires_plt_overlays": ["tech_jobs"],
            "execution_identities": {"aws": {"account": "dev"}},
            "repo_path": "/machine/one",
            "token_type": "IGNORED_TOKEN",
        }
        first = common.canonical_sha256(
            common.target_definition_document(target)
        )
        same_definition = dict(target, repo_path="/machine/two")
        same_definition["plt_overlays"] = ["unrelated"]
        self.assertEqual(
            first,
            common.canonical_sha256(
                common.target_definition_document(same_definition)
            ),
        )
        changed = dict(target, procedure="other")
        self.assertNotEqual(
            first,
            common.canonical_sha256(
                common.target_definition_document(changed)
            ),
        )

    def test_cfg_view_hash_is_path_and_content_sensitive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            common.write_yaml_file(root / "a.yaml", {"value": 1})
            first = common.directory_content_sha256(root)
            common.write_yaml_file(root / "a.yaml", {"value": 2})
            self.assertNotEqual(first, common.directory_content_sha256(root))
            (root / "a.yaml").rename(root / "b.yaml")
            self.assertNotEqual(first, common.directory_content_sha256(root))

    def test_target_status_reports_definition_and_cfg_view_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            spec = {
                "kind": "target",
                "key": "env/core",
                "segments": ["account=dev"],
                "address": "env/core/instances/account=dev",
                "prefix": "provision/target/env/core/instances/account=dev",
                "target_definition_sha256": "new-definition",
                "target_cfg_view_sha256": "new-view",
            }
            common.write_yaml_file(
                namespace / spec["prefix"] / "committed.yaml",
                {
                    "run_id": "old",
                    "status": "ok",
                    "target_definition_sha256": "old-definition",
                    "target_cfg_view_sha256": "old-view",
                },
            )
            result = common.compute_target_instance_status(
                namespace, "provision", spec
            )
            self.assertEqual("outdated", result["freshness"])
            self.assertIn("target definition changed", result["reasons"])
            self.assertIn("target cfg view changed", result["reasons"])


    def test_partial_workflow_publishes_only_successful_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parent = (
                root
                / "live/provision/workflow/env/baseline/instances/sha256-x/runs/w1"
            )
            parent.mkdir(parents=True)
            common.write_run_metadata(
                parent,
                {
                    "run_id": "w1",
                    "action": "provision",
                    "run_type": "workflow",
                    "result_name": "env/baseline",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                },
            )
            facts = {
                "source_commit": "a" * 40,
                "cfg_source_commit": "b" * 40,
                "source_state": "clean",
                "ref_policy": "commit_required",
                "target_definition_sha256": "c" * 64,
                "target_cfg_view_sha256": "d" * 64,
                "plt_overlays": ["temporary"],
            }
            successful, _ = common.begin_workflow_target_run(
                parent, {"target": "env/one", **facts}, {}
            )
            common.finish_workflow_target_run(successful)
            failed, _ = common.begin_workflow_target_run(
                parent, {"target": "env/two", **facts}, {}
            )
            common.finish_workflow_target_run(
                failed, error=RuntimeError("failed child")
            )
            self.assertTrue(
                common.committed_pointer_path(
                    common.ctl_state_dir_from_run_dir(successful)
                ).is_file()
            )
            self.assertFalse(
                common.committed_pointer_path(
                    common.ctl_state_dir_from_run_dir(failed)
                ).is_file()
            )
            self.assertFalse(
                common.committed_pointer_path(
                    common.ctl_state_dir_from_run_dir(parent)
                ).is_file()
            )

    def test_pre_phase56_pointer_is_not_reusable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parent = root / "live/provision/workflow/w/instances/x/runs/w1"
            parent.mkdir(parents=True)
            common.write_run_metadata(
                parent,
                {
                    "run_id": "w1",
                    "action": "provision",
                    "run_type": "workflow",
                    "result_name": "w",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                },
            )
            instance = (
                root
                / "live/provision/target/env/core/instances/account=dev"
            )
            common.write_yaml_file(
                instance / "committed.yaml",
                {
                    "run_id": "old",
                    "status": "ok",
                    "source_commit": "a" * 40,
                    "cfg_source_commit": "b" * 40,
                    "source_state": "clean",
                    "ref_policy": "commit_required",
                },
            )
            target_run = {
                "target": "env/core",
                "target_instance_params": ["account"],
                "source_commit": "a" * 40,
                "cfg_source_commit": "b" * 40,
                "source_state": "clean",
                "ref_policy": "commit_required",
                "target_definition_sha256": "c" * 64,
                "target_cfg_view_sha256": "d" * 64,
            }
            self.assertIsNone(
                common.committed_target_revision_if_skippable(
                    parent,
                    target_run,
                    {"execution_context.params.account": "dev"},
                )
            )


class AwsBackendProbeTests(unittest.TestCase):
    def test_probe_classifies_absent_denied_and_failed(self):
        cases = [
            ("An error occurred (404) when calling HeadBucket", "absent"),
            ("An error occurred (403) AccessDenied", "denied"),
            ("Could not connect to the endpoint URL", "failed"),
        ]
        for stderr, expected in cases:
            with self.subTest(expected=expected), patch.object(
                aws.subprocess,
                "run",
                return_value=CompletedProcess([], 255, "", stderr),
            ):
                result = aws.probe_state_backend(
                    "bucket", "eu-west-2", "profile"
                )
                self.assertEqual(result["status"], expected)


if __name__ == "__main__":
    unittest.main()
