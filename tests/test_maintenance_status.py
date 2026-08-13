"""Maintenance owns audit status separately from targets and workflows."""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import addressing as run_addressing
from engine.state import lifecycle as state_lifecycle
from engine.state import run_store as state_run_store
from engine.state import status_rows as state_status_rows


class MaintenanceStatusTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.namespace = Path(self._tmp.name) / "live"

    def _run(self) -> Path:
        run_dir = (
            self.namespace
            / run_addressing.compose_state_relpath("maintenance", "unlock-ctl-state/lock-1", [])
            / "runs"
            / "r1"
        )
        run_dir.mkdir(parents=True)
        kernel_yaml_io.write_yaml_file(
            state_run_store.run_metadata_path(run_dir),
            {
                "run_id": "r1",
                "run_type": "maintenance",
                "action": "maintenance",
                "status": "ok",
                "updated_at": "2026-08-09T10:00:00Z",
            },
        )
        state_lifecycle.record_maintenance_request(
            run_dir,
            operation="unlock-ctl-state",
            subject="lock-1",
            scope="both",
        )
        return run_dir

    def _manifest(self) -> None:
        kernel_yaml_io.write_yaml_file(
            self.namespace / "_maintenance" / "history-prune" / "m1" / "manifest.yaml",
            {
                "operation": "history-prune",
                "maintenance_id": "m1",
                "dry_run": True,
                "created_at": "2026-08-09T11:00:00Z",
                "candidate_run_ids": ["old-run"],
            },
        )

    def _target(self) -> None:
        instance_dir = self.namespace / run_addressing.compose_state_relpath(
            "target", "env/repair", ["env.type=dev"]
        )
        kernel_yaml_io.write_yaml_file(
            state_run_store.committed_pointer_path(instance_dir, "maintenance"),
            {
                "run_id": "t1",
                "status": "ok",
                "action": "maintenance",
                "committed_at": "2026-08-09T12:00:00Z",
            },
        )

    def test_run_and_manifest_owners_form_one_maintenance_report(self):
        self._run()
        self._manifest()
        self._target()

        rows = state_status_rows.maintenance_status_rows(self.namespace)

        self.assertEqual(
            ["maintenance", "history-prune", "unlock-ctl-state"],
            [row["operation"] for row in rows],
        )
        self.assertEqual("target/env/repair/instances/env.type=dev", rows[0]["subject"])
        self.assertEqual("dry_run", rows[1]["status"])
        self.assertEqual("passed", rows[2]["status"])
        self.assertEqual("lock-1", rows[2]["subject"])
        self.assertEqual("both", rows[2]["scope"])

    def test_ordinary_status_excludes_maintenance(self):
        self._run()
        self._target()
        self.assertEqual({}, state_status_rows.compute_namespace_status_map(self.namespace))

    def test_remote_index_hydrates_grouped_pointers_and_opted_in_manifests(self):
        class Syncer:
            def __init__(self):
                self.pulled = []

            @staticmethod
            def list_object_keys():
                return [
                    "target/env/repair/committed/maintenance.yaml",
                    "maintenance/unlock/lock-1/runs/r1/RUN.yaml",
                    "_maintenance/history-prune/m1/manifest.yaml",
                    "status_cache.yaml",
                ]

            def pull_object(self, key):
                self.pulled.append(key)

        ordinary = Syncer()
        state_run_store.hydrate_ctl_state_index(ordinary)
        self.assertEqual(
            [
                "target/env/repair/committed/maintenance.yaml",
                "maintenance/unlock/lock-1/runs/r1/RUN.yaml",
            ],
            ordinary.pulled,
        )

        maintenance = Syncer()
        state_run_store.hydrate_ctl_state_index(maintenance, include_maintenance_manifests=True)
        self.assertEqual(
            [
                "target/env/repair/committed/maintenance.yaml",
                "maintenance/unlock/lock-1/runs/r1/RUN.yaml",
                "_maintenance/history-prune/m1/manifest.yaml",
            ],
            maintenance.pulled,
        )


if __name__ == "__main__":
    unittest.main()
