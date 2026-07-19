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
                "provision", "env/core/account=dev/env_type=dev"
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
                    "instance_address": "env/core/account=dev",
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
                "address": "env/core/account=dev",
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
                namespace, "plan", spec
            )
            self.assertEqual(result["verdict"], "destroyed")
            self.assertEqual(result["lifecycle"], "destroyed")

    def test_workflow_status_detects_child_revision_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace = Path(tmp)
            target = {
                "kind": "target",
                "key": "env/core",
                "segments": ["account=dev"],
                "address": "env/core/account=dev",
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
            self.assertEqual(current["verdict"], "current")
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
            self.assertEqual(outdated["verdict"], "outdated")
            self.assertIn(
                "env/core/account=dev: committed revision changed",
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
