"""§Phase 50 — standalone read-only status runner.

Covers the three pieces that are hard to exercise end-to-end:
  * §50.9 — a workflow run must NOT outdate the fresh provision pointer its own
    child just committed, while it MUST still outdate the superseded destroy
    sibling (cross-action supersession).
  * §50.10 — compute_namespace_status_map: flat address -> verdict, lifecycle
    collapsed, targets + workflows only.
  * finalize_status_args — slim-parser normalization + validation.
"""

import argparse
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


ADDRESS = "env/seed/baseline/env_type=dev/account=dev"


def _seed_target_pointer(root: Path, action: str, *, status: str, run_id: str, when: str):
    d = (
        root / "live" / action / "target/env/seed/baseline/instances"
        / "env_type=dev" / "account=dev"
    )
    d.mkdir(parents=True, exist_ok=True)
    common.write_yaml_file(
        d / "committed.yaml",
        {
            "run_id": run_id,
            "status": status,
            "committed_at": when,
            "target_keys": ["env/seed/baseline"],
        },
    )
    return d / "committed.yaml"


def _seed_workflow_pointer(root: Path, action: str, key: str, seg: str, *, when: str, status: str = "ok", child_revisions=None):
    d = root / "live" / action / "workflow" / key / "instances" / seg
    d.mkdir(parents=True, exist_ok=True)
    payload = {"run_id": f"{action}-wf", "status": status, "committed_at": when}
    if child_revisions is not None:
        payload["child_revisions"] = child_revisions
    common.write_yaml_file(d / "committed.yaml", payload)
    return d / "committed.yaml"


class Phase50SelfOutdateFixTests(unittest.TestCase):
    """§50.9: the workflow's outdate sweep excludes its own fresh same-action
    commit, but still supersedes the cross-action sibling."""

    def test_workflow_does_not_outdate_its_own_fresh_child_but_supersedes_destroy(self):
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            # the fresh provision pointer the workflow's child just committed…
            provision = _seed_target_pointer(
                root, "provision", status="ok", run_id="child-c79a",
                when="2026-07-21T16:08:58Z",
            )
            # …and an OLDER destroy pointer for the same instance (superseded).
            destroy = _seed_target_pointer(
                root, "destroy", status="ok", run_id="old-destroy",
                when="2026-07-21T14:07:52Z",
            )
            run_dir = (
                root / "live/provision/workflow/env/seed/instances/sha256-abc/runs/rwf"
            )
            run_dir.mkdir(parents=True)
            common.write_run_metadata(
                run_dir,
                {
                    "run_id": "wf-a191", "action": "provision", "run_type": "workflow",
                    "result_name": "env/seed",
                    "result_key": "provision/workflow/env/seed",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                    "instance": ["sha256-abc"],
                    "target_addresses": [ADDRESS],
                    "target_keys": ["env/seed/baseline"],
                    "mutation_started": True,
                },
            )
            common.mark_outdated_for_run(run_dir, include_current_result=False)

            fresh = common.load_status_mapping(provision)
            superseded = common.load_status_mapping(destroy)
            # the bug was: the workflow marked its OWN fresh provision outdated.
            self.assertEqual(fresh.get("status"), "ok", "fresh provision must survive")
            # cross-action supersession still fires.
            self.assertEqual(superseded.get("status"), "outdated")


class Phase50NamespaceMapTests(unittest.TestCase):
    def test_flat_address_verdict_map(self):
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            _seed_target_pointer(
                root, "provision", status="ok", run_id="p1",
                when="2026-07-21T16:00:00Z",
            )
            namespace_root = root / "live"
            rows = common.compute_namespace_status_map(namespace_root)
            self.assertEqual(rows, {f"target/{ADDRESS}": "current"})

    def test_newer_destroy_reads_destroyed(self):
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            _seed_target_pointer(
                root, "provision", status="ok", run_id="p1",
                when="2026-07-21T14:00:00Z",
            )
            _seed_target_pointer(
                root, "destroy", status="ok", run_id="d1",
                when="2026-07-21T16:00:00Z",
            )
            rows = common.compute_namespace_status_map(root / "live")
            self.assertEqual(rows[f"target/{ADDRESS}"], "destroyed")

    def test_provision_composition_all_children_destroyed_reads_destroyed(self):
        """A deployable composition whose children are ALL destroyed reads
        `destroyed`, not `outdated` — mirroring the target-level rule."""
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            # child provisioned, then destroyed newer (target reads `destroyed`)
            _seed_target_pointer(
                root, "provision", status="ok", run_id="p1",
                when="2026-07-21T14:00:00Z",
            )
            _seed_target_pointer(
                root, "destroy", status="ok", run_id="d1",
                when="2026-07-21T16:00:00Z",
            )
            # the provision composition that once deployed that child
            _seed_workflow_pointer(
                root, "provision", "env/seed", "sha256-abc",
                when="2026-07-21T14:00:01Z",
                child_revisions=[{"address": ADDRESS, "run_id": "p1", "status": "ok"}],
            )
            rows = common.compute_namespace_status_map(root / "live")
            self.assertEqual(rows[f"target/{ADDRESS}"], "destroyed")
            self.assertEqual(rows["workflow/env/seed/sha256-abc"], "destroyed")

    def test_teardown_only_workflow_gets_no_reconciled_row(self):
        """A destroy-only workflow key (a pure teardown) owns no reconciled state,
        so it never appears in the map — its effect shows on the target row. The
        deployable composition (a provision key) still gets its row."""
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            _seed_target_pointer(
                root, "provision", status="ok", run_id="p1",
                when="2026-07-21T16:00:00Z",
            )
            # deployable composition — a provision workflow key recording the child
            _seed_workflow_pointer(
                root, "provision", "env/seed", "sha256-abc",
                when="2026-07-21T16:00:01Z",
                child_revisions=[{"address": ADDRESS, "run_id": "p1", "status": "ok"}],
            )
            # a pure teardown — destroy-only workflow key over the same child
            _seed_workflow_pointer(
                root, "destroy", "env/seed/teardown", "sha256-abc",
                when="2026-07-21T15:00:00Z",
                child_revisions=[{"address": ADDRESS, "run_id": "d1", "status": "ok"}],
            )
            rows = common.compute_namespace_status_map(root / "live")
            self.assertIn("workflow/env/seed/sha256-abc", rows)
            self.assertNotIn("workflow/env/seed/teardown/sha256-abc", rows)
            self.assertEqual(rows[f"target/{ADDRESS}"], "current")


class Phase50FinalizeStatusArgsTests(unittest.TestCase):
    def _ns(self, **kw):
        base = dict(
            execution_param=[("provider", "aws"), ("landing_zone", "live")],
            all=False, target=None, workflow=None, fan_out=None,
            action=None, scope="local", ctl_state_local_root="/tmp/x",
            provider_options={}, write_cache=False,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_local_requires_root(self):
        with self.assertRaises(RuntimeError):
            common.finalize_status_args(self._ns(all=True, ctl_state_local_root=None))

    def test_local_rejects_provider_options(self):
        with self.assertRaises(RuntimeError):
            common.finalize_status_args(
                self._ns(
                    all=True,
                    provider_options={"aws.force_bypass_credential_profile": "prof"},
                )
            )

    def test_remote_rejects_local_root(self):
        with self.assertRaises(RuntimeError):
            common.finalize_status_args(
                self._ns(all=True, scope="remote", ctl_state_local_root="/tmp/x")
            )

    def test_remote_default_is_the_providers_normal_mode(self):
        args = self._ns(
            all=True, scope="remote", ctl_state_local_root=None,
            provider_options={"aws.credential_implementation": "profile"},
        )
        common.finalize_status_args(args)
        self.assertEqual(args.providers, ["aws"])
        self.assertEqual(args.execution_access_modes, {"aws": "standard"})

    def test_remote_substitute_credential_option_implies_its_mode(self):
        args = self._ns(
            all=True, scope="remote", ctl_state_local_root=None,
            provider_options={
                "aws.credential_implementation": "profile",
                "aws.force_bypass_credential_profile": "dev-profile",
            },
        )
        common.finalize_status_args(args)
        self.assertEqual(args.execution_access_modes, {"aws": "force_bypass"})
        self.assertEqual(
            args.provider_options["aws.force_bypass_credential_profile"],
            "dev-profile",
        )

    def test_targeted_requires_action(self):
        with self.assertRaises(RuntimeError):
            common.finalize_status_args(self._ns(target="env/seed/baseline", action=None))

    def test_all_does_not_require_action(self):
        args = self._ns(all=True)
        common.finalize_status_args(args)
        self.assertEqual(args.execution_params, {"provider": "aws", "landing_zone": "live"})
        self.assertEqual(args.status, "local")

    def test_write_cache_requires_all(self):
        with self.assertRaises(RuntimeError):
            common.finalize_status_args(
                self._ns(target="env/seed/baseline", action="provision", write_cache=True)
            )

    def test_write_cache_remote_requires_root(self):
        with self.assertRaises(RuntimeError):
            common.finalize_status_args(
                self._ns(all=True, scope="remote", ctl_state_local_root=None, write_cache=True)
            )


class Phase50WriteCacheTests(unittest.TestCase):
    def test_local_all_write_cache_persists_self_dated_map(self):
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            _seed_target_pointer(
                root, "provision", status="ok", run_id="p1",
                when="2026-07-21T16:00:00Z",
            )
            args = argparse.Namespace(
                execution_param=[("provider", "aws"), ("landing_zone", "live")],
                all=True, target=None, workflow=None, fan_out=None,
                action=None, scope="local", ctl_state_local_root=str(root),
                provider_options={}, write_cache=True, ctl_profile="local_dev",
            )
            common.finalize_status_args(args)
            with unittest.mock.patch.object(
                common, "build_execution_context", return_value={}
            ), unittest.mock.patch.object(
                common, "resolve_ctl_state_namespace", return_value=("live", {})
            ):
                report = common.run_status_all_command(Path("/nonexistent-cfg"), args)

            cache_path = root / "live" / "status_cache.yaml"
            self.assertTrue(cache_path.is_file())
            cache = common.load_status_mapping(cache_path)
            self.assertIs(cache["advisory"], True)
            self.assertEqual(cache["scope"], "local")
            self.assertEqual(cache["source"], "status runner")
            self.assertIn("computed_at", cache)
            self.assertEqual(cache["instances"], {f"target/{ADDRESS}": "current"})
            self.assertEqual(report["cache_written"], cache_path.as_posix())

    def test_default_writes_nothing(self):
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            _seed_target_pointer(
                root, "provision", status="ok", run_id="p1",
                when="2026-07-21T16:00:00Z",
            )
            args = argparse.Namespace(
                execution_param=[("provider", "aws"), ("landing_zone", "live")],
                all=True, target=None, workflow=None, fan_out=None,
                action=None, scope="local", ctl_state_local_root=str(root),
                provider_options={}, write_cache=False, ctl_profile="local_dev",
            )
            common.finalize_status_args(args)
            with unittest.mock.patch.object(
                common, "build_execution_context", return_value={}
            ), unittest.mock.patch.object(
                common, "resolve_ctl_state_namespace", return_value=("live", {})
            ):
                common.run_status_all_command(Path("/nonexistent-cfg"), args)
            self.assertFalse((root / "live" / "status_cache.yaml").exists())


if __name__ == "__main__":
    unittest.main()
