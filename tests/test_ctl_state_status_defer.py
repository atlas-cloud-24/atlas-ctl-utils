import sys
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import atlas_ctl_adapter_aws as aws
import ctl_cfg_fixture
from engine.catalog import targets as catalog_targets
from engine.catalog import workflow as catalog_workflow
from engine.cfg import overlays as cfg_overlays
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine.state import sync as state_sync


class CtlStateAddressAndCommitTests(unittest.TestCase):
    def tearDown(self):
        state_sync.PUBLICATION.reset()

    def test_target_address_maps_to_hive_path(self):
        self.assertEqual(
            state_sync.ctl_state_target_address_prefix(
                "provision", "env/core/instances/account=dev/env_type=dev"
            ),
            "target/env/core/instances/account=dev/env_type=dev",
        )

    def test_publication_scope_names_every_grouped_pointer_object(self):
        with tempfile.TemporaryDirectory() as tmp:
            namespace_root = Path(tmp) / "live"
            run_dir = namespace_root / "target/env/core/runs/r1"
            run_dir.mkdir(parents=True)
            state_run_store.write_run_metadata(
                run_dir,
                {
                    "action": "provision",
                    "target_addresses": ["env/child"],
                },
            )

            keys, run_prefixes = state_sync.CtlStatePublication._run_access_scope(
                {"results_root": namespace_root, "run_dir": run_dir}
            )

            expected_prefixes = ("target/env/core", "target/env/child")
            for prefix in expected_prefixes:
                self.assertIn(f"{prefix}/identity.yaml", keys)
                for group in run_actions.RESULT_GROUPS:
                    self.assertIn(f"{prefix}/committed/{group}.yaml", keys)
            self.assertFalse(any(key.endswith("/committed.yaml") for key in keys))
            self.assertEqual(["target/env/core/runs/r1"], run_prefixes)

    def test_adapter_hydrates_only_the_requested_grouped_pointers(self):
        with tempfile.TemporaryDirectory() as tmp:
            syncer = aws.CtlStateSyncer(
                Path(tmp),
                "state-bucket",
                "eu-west-2",
                "profile",
                Path(tmp),
                required=True,
            )
            with patch.object(
                syncer, "ensure_ready", return_value=True
            ), patch.object(syncer, "pull_object") as pull_object:
                syncer.hydrate_instance(
                    "target/env/core",
                    ["target/env/child"],
                    committed_groups=("mutative", "plan"),
                )

            self.assertEqual(
                [
                    "target/env/core/identity.yaml",
                    "target/env/core/committed/mutative.yaml",
                    "target/env/core/committed/plan.yaml",
                    "target/env/child/identity.yaml",
                    "target/env/child/committed/mutative.yaml",
                    "target/env/child/committed/plan.yaml",
                ],
                [entry.args[0] for entry in pull_object.call_args_list],
            )

    def test_duplicate_fan_out_materializations_fail(self):
        # resolving each selection's owner goes through the adapter
        ctl_cfg_fixture.cfg_root(self, "aws")
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
            catalog_workflow.validate_unique_fan_out_materializations(
                [selection, dict(selection)]
            )

    def test_committed_rerun_requires_matching_clean_commits(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parent = (
                root
                / "live/workflow/env/baseline/instances/sha256-x/runs/w1"
            )
            parent.mkdir(parents=True)
            state_run_store.write_run_metadata(
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
                / "live/target/env/core/instances/account=dev/runs/r1"
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
            state_run_store.write_run_metadata(
                run_dir,
                {
                    "run_id": "r1",
                    "action": "provision",
                    "run_type": "target",
                    "result_name": "env/core",
                    "result_key": "target/env/core",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                    "instance": ["account=dev"],
                    "instance_address": "env/core/instances/account=dev",
                    **facts,
                },
            )
            payload = state_status.build_status_payload(run_dir, "ok")
            state_run_store.publish_committed_pointer(run_dir, payload)
            target_run = {
                "target": "env/core",
                "target_instance_params": ["account"],
                "reuse_committed_result": True,
                **facts,
            }
            context = {"execution_context.params.account": "dev"}
            revision = state_status.up_to_date_child_revision(
                parent, target_run, context, "provision"
            )
            self.assertEqual(revision["run_id"], "r1")
            target_run["source_state"] = "dirty"
            self.assertIsNone(
                state_status.up_to_date_child_revision(
                    parent, target_run, context, "provision"
                )
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
            run_dir = root / "live/target/env/core/runs/r1"
            run_dir.mkdir(parents=True)
            state_run_store.write_run_metadata(
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
            state_run_store.write_current_status(
                run_dir, state_status.build_status_payload(run_dir, "ok")
            )
            pointer = state_run_store.publish_committed_pointer(
                run_dir, state_status.build_status_payload(run_dir, "ok")
            )
            manifest = state_sync.PendingSyncQueue.enqueue(run_dir, pointer)
            self.assertTrue(manifest.is_file())
            syncer = FakeSyncer(root / "live")
            state_sync.PUBLICATION.syncer = syncer
            self.assertEqual(state_sync.PUBLICATION.drain_pending(), 1)
            self.assertEqual(
                [kind for kind, _ in syncer.events], ["run", "pointer"]
            )
            self.assertFalse(manifest.exists())



class OverlayAndDefinitionHashTests(unittest.TestCase):
    context = {"execution_context.params.env.type": "dev"}

    def setUp(self):
        # resolving a target's definition validates its execution identity,
        # which dispatches to a declared provider's adapter
        ctl_cfg_fixture.cfg_root(self, "aws")

    @staticmethod
    def add_overlay(root: Path, name: str, payload: dict) -> None:
        """An overlay mirrors SOURCE paths, so its payload sits under the scope
        root it patches — here `domains/env/dev`, the scope the fixture activates."""

        overlay_root = root / "_overlays" / name
        kernel_yaml_io.write_yaml_file(
            overlay_root / "__meta__.yaml",
            {
                "type": "overlay",
                "name": name,
                "selectors": {
                    "in": {"execution_context.params.env.type": ["dev"]}
                },
            },
        )
        kernel_yaml_io.write_yaml_file(
            overlay_root / "domains" / "env" / "dev" / "common.yaml", payload
        )

    @staticmethod
    def add_scope(root: Path, payload: dict | None = None) -> list[dict]:
        """The one scope the overlay fixtures patch, in the shape merge_scopes
        hands to the overlay pass."""

        scope_root = root / "domains" / "env" / "dev"
        kernel_yaml_io.write_yaml_file(
            scope_root / "__meta__.yaml",
            {
                "type": "scope",
                "target_path": "/env",
                "selectors": {"match": {"execution_context.params.env.type": "dev"}},
            },
        )
        kernel_yaml_io.write_yaml_file(scope_root / "common.yaml", payload or {"service": {"mode": "base"}})
        return [{
            "scope_root": scope_root,
            "target_path": "/env",
            "scope_path": "/domains/env/dev",
        }]



    def test_active_target_keeps_cfg_overlay_and_backend_facts(self):
        active = catalog_targets.build_active_target_runs(
            {
                "meta": {"action": "provision"},
                "target_runs": [{"id": "run", "target": "target"}],
            },
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
                        "allowed_actions": ["provision"],
                        "committed_result_reuse": {"provision": True},
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

    def _apply(self, root: Path, dest: Path, order: list[str], scopes: list[dict]) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "env").mkdir(parents=True, exist_ok=True)
        cfg_overlays.apply_selected_overlays_to_merged_cfg(
            root,
            dest,
            order,
            active_scopes=scopes,
            execution_context=self.context,
            merged_files={},
            source_log_roots=(root.resolve(),),
            dest_log_roots=(dest.resolve(),),
            skip_filenames=set(),
        )

    def test_an_overlay_beats_the_composed_result(self):
        """The reason the merge point moved.

        Merged into the SOURCE tree, an overlay patches ONE preset level and a
        narrower scope still overrides it — so switching an overlay on could
        silently change nothing. Merged over the COMPOSED result it always wins,
        which is what "switched on" has to mean.
        """

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "source"
            scopes = self.add_scope(root, {"service": {"mode": "composed"}})
            self.add_overlay(root, "first", {"service": {"mode": "overlaid"}})
            merged = Path(tmp) / "merged"
            (merged / "env").mkdir(parents=True)
            # what merge_scopes leaves behind
            kernel_yaml_io.write_yaml_file(
                merged / "env" / "common.yaml", {"service": {"mode": "composed"}}
            )
            self._apply(root, merged, ["first"], scopes)
            self.assertEqual(
                kernel_yaml_io.load_yaml(merged / "env" / "common.yaml")["service"]["mode"],
                "overlaid",
            )

    def test_an_overlay_that_matches_no_active_scope_is_refused(self):
        """Its payload mirrors scope paths, so one under a scope this run does
        not activate would apply to nothing — silently, which is how an overlay
        rots into a no-op."""

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "source"
            self.add_scope(root)
            self.add_overlay(root, "first", {"service": {"mode": "overlaid"}})
            # the overlay's payload sits under domains/env/dev; this run activated none
            merged = Path(tmp) / "merged"
            (merged / "env").mkdir(parents=True)
            with self.assertRaisesRegex(RuntimeError, "matched no active cfg scope"):
                self._apply(root, merged, ["first"], [])

    def test_explicit_overlay_order_changes_merge_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "source"
            scopes = self.add_scope(root)
            self.add_overlay(root, "first", {"service": {"mode": "first"}})
            self.add_overlay(root, "second", {"service": {"mode": "second"}})
            left = Path(tmp) / "left"
            right = Path(tmp) / "right"
            self._apply(root, left, ["first", "second"], scopes)
            self._apply(root, right, ["second", "first"], scopes)
            self.assertEqual(
                kernel_yaml_io.load_yaml(left / "env" / "common.yaml")["service"]["mode"],
                "second",
            )
            self.assertEqual(
                kernel_yaml_io.load_yaml(right / "env" / "common.yaml")["service"]["mode"],
                "first",
            )

    def test_required_overlays_append_in_target_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("explicit", "required_a", "required_b"):
                self.add_overlay(root, name, {name: {"enabled": True}})
            resolved = cfg_overlays.resolve_run_plt_overlays(
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

    def test_two_overlays_may_not_set_one_leaf_differently(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.add_overlay(root, "explicit", {"service": {"mode": "one"}})
            self.add_overlay(root, "required", {"service": {"mode": "two"}})
            active = {
                "target": {"requires_plt_overlays": ["required"]}
            }
            with self.assertRaisesRegex(
                RuntimeError, "cannot set one leaf differently"
            ):
                cfg_overlays.resolve_run_plt_overlays(
                    root,
                    ["explicit"],
                    active,
                    execution_context=self.context,
                )
            self.assertEqual(
                cfg_overlays.resolve_run_plt_overlays(
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
            "secret_key": "IGNORED_SECRET",
        }
        first = kernel_paths.canonical_sha256(
            catalog_targets.target_definition_document(target)
        )
        same_definition = dict(target, repo_path="/machine/two")
        same_definition["plt_overlays"] = ["unrelated"]
        self.assertEqual(
            first,
            kernel_paths.canonical_sha256(
                catalog_targets.target_definition_document(same_definition)
            ),
        )
        changed = dict(target, procedure="other")
        self.assertNotEqual(
            first,
            kernel_paths.canonical_sha256(
                catalog_targets.target_definition_document(changed)
            ),
        )

    def test_cfg_view_hash_is_path_and_content_sensitive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            kernel_yaml_io.write_yaml_file(root / "a.yaml", {"value": 1})
            first = kernel_paths.directory_content_sha256(root)
            kernel_yaml_io.write_yaml_file(root / "a.yaml", {"value": 2})
            self.assertNotEqual(first, kernel_paths.directory_content_sha256(root))
            (root / "a.yaml").rename(root / "b.yaml")
            self.assertNotEqual(first, kernel_paths.directory_content_sha256(root))

    def test_pre_phase56_pointer_is_not_reusable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parent = root / "live/workflow/w/instances/x/runs/w1"
            parent.mkdir(parents=True)
            state_run_store.write_run_metadata(
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
                / "live/target/env/core/instances/account=dev"
            )
            kernel_yaml_io.write_yaml_file(
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
                "reuse_committed_result": True,
            }
            self.assertIsNone(
                state_status.up_to_date_child_revision(
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


class SkipUpToDateActionTest(unittest.TestCase):
    """Reuse compares the ACTION, so one target under two actions
    cannot have both members skipped."""

    FACTS = {
        "source_commit": "a" * 40,
        "cfg_source_commit": "b" * 40,
        "source_state": "clean",
        "ref_policy": "commit_required",
        "target_definition_sha256": "c" * 64,
        "target_cfg_view_sha256": "d" * 64,
    }

    def _tree(self, root, published_action):
        parent = root / "live/workflow/env/seed/instances/sha256-x/runs/w1"
        parent.mkdir(parents=True)
        state_run_store.write_run_metadata(parent, {
            "run_id": "w1", "action": "provision", "run_type": "workflow",
            "result_name": "env/seed", "ctl_state_local_root": str(root),
            "ctl_state_locator": ["live"],
        })
        run_dir = (root / f"live/{published_action}/target/env/seed/baseline"
                        / "instances/account=dev/runs/r1")
        run_dir.mkdir(parents=True)
        state_run_store.write_run_metadata(run_dir, {
            "run_id": "r1", "action": published_action, "run_type": "target",
            "result_name": "env/seed/baseline",
            "ctl_state_local_root": str(root), "ctl_state_locator": ["live"],
            "instance": ["account=dev"], **self.FACTS,
        })
        state_run_store.publish_committed_pointer(
            run_dir, state_status.build_status_payload(run_dir, "ok")
        )
        return parent
