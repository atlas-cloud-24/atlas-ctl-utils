import logging
import logging.handlers
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


CFG = {
    "backends.yaml": (
        "ctl_state_backends:\n"
        "  live:\n"
        "    selectors:\n"
        "      match:\n"
        "        execution_context.params.landing_zone: live\n"
        "    provider: aws\n"
        "    backend_type: s3\n"
        "    bucket_name: oxygen-live-ctl-state\n"
        "    bucket_region: eu-west-2\n"
        "    execution_identity:\n"
        "      account: ctl_plane\n"
        "      operations:\n"
        "        read:\n          role: reader\n"
        "        sync:\n          role: synchronizer\n"
        "        maintenance:\n          role: maintainer\n"
    ),
    "target_sources.yaml": (
        "target_sources:\n"
        "  bootstrap:\n"
        "    repo_url: https://example.invalid/bootstrap.git\n"
    ),
    "cfg_file_sets.yaml": (
        "cfg_file_sets:\n"
        "  env_backend:\n"
        "    cfg_root: /env\n"
        "    cfg_files:\n"
        "      - ctl_state.yaml\n"
    ),
    "workflow.yaml": (
        "workflows:\n"
        "  env/bootstrap:\n"
        "    actions: [provision]\n"
        "    target_keys:\n"
        "      - env/tfstate_backend\n"
    ),
}
TARGETS = (
    "targets:\n"
    "  env/tfstate_backend:\n"
    "    actions: [provision]\n"
    "    source_key: bootstrap\n"
    "    ref_key: env/${execution_context.params.env_type}\n"
    "    step_sequence_key: tfstate_backend\n"
    "    cfg_file_set_key: env_backend\n"
    "    target_instance_params:\n"
    "      - account\n"
    "      - env_type\n"
)


def make_cfg(tmp: str) -> Path:
    root = Path(tmp)
    for name, body in CFG.items():
        (root / name).write_text(body, encoding="utf-8")
    (root / "targets" / "provision").mkdir(parents=True)
    (root / "targets" / "provision" / "t.yaml").write_text(TARGETS, encoding="utf-8")
    return root


PARAMS = {"landing_zone": "live", "account": "dev", "env_type": "dev"}


class LifecycleWiringTests(unittest.TestCase):
    """§Phase 31 6b/6c — namespace locator, instance identity, run dirs, outdate."""

    def test_locator_is_namespace_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(tmp)
            loc = common.resolve_run_locator_segments(
                root, run_type="target", action="provision", ctl_profile=None,
                execution_params=PARAMS, execution_runtime_mode="local",
                target_name="env/tfstate_backend",
            )
            self.assertEqual(loc, ["live"])

    def test_fan_out_and_step_sequence_stay_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(tmp)
            for run_type in ("fan_out", "step_sequence"):
                loc = common.resolve_run_locator_segments(
                    root, run_type=run_type, action="provision", ctl_profile=None,
                    execution_params=PARAMS, execution_runtime_mode="local",
                )
                self.assertEqual(loc, ["_local"], run_type)

    def test_target_instance_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(tmp)
            ident = common.resolve_run_instance_identity(
                root, run_type="target", action="provision", ctl_profile=None,
                execution_params=PARAMS, execution_runtime_mode="local",
                target_name="env/tfstate_backend",
            )
            self.assertEqual(ident["instance_segments"], ["account=dev", "env_type=dev"])
            self.assertEqual(
                ident["address"], "env/tfstate_backend/account=dev/env_type=dev"
            )

    def test_workflow_instance_identity_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(tmp)
            ident = common.resolve_run_instance_identity(
                root, run_type="workflow", action="provision", ctl_profile=None,
                execution_params=PARAMS, execution_runtime_mode="local",
                workflow_name="env/bootstrap",
            )
            self.assertEqual(len(ident["instance_segments"]), 1)
            self.assertTrue(ident["instance_segments"][0].startswith("sha256-"))
            self.assertEqual(
                ident["target_addresses"],
                ["env/tfstate_backend/account=dev/env_type=dev"],
            )
            doc = ident["identity_doc"]["workflow_instance"]
            self.assertEqual(doc["workflow"], "env/bootstrap")
            self.assertNotIn("status", doc)

    def test_run_dirs_nest_under_instance_and_write_identity(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as state:
            root = make_cfg(tmp)
            ident = common.resolve_run_instance_identity(
                root, run_type="workflow", action="provision", ctl_profile=None,
                execution_params=PARAMS, execution_runtime_mode="local",
                workflow_name="env/bootstrap",
            )
            mem = logging.handlers.MemoryHandler(capacity=10)
            run_dir, _, _ = common.setup_preflight_run_dirs(
                "0" * 8, "provision", "workflow", "env/bootstrap", Path(state), mem,
                locator_segments=["live"],
                instance_segments=ident["instance_segments"],
                instance_address=ident["address"],
                target_addresses=ident["target_addresses"],
                identity_doc=ident["identity_doc"],
                parent_fan_out_run_id="fo-123",
            )
            sha = ident["instance_segments"][0]
            expected = (
                Path(state) / "live/provision/workflow/env/bootstrap/instances" / sha
            )
            self.assertEqual(run_dir, expected / "runs" / ("0" * 8))
            self.assertTrue((expected / "identity.yaml").exists())
            meta = common.load_run_metadata(run_dir)
            self.assertEqual(meta["ctl_state_namespace"], "live")
            self.assertEqual(meta["fan_out_run_id"], "fo-123")
            self.assertEqual(meta["instance"], [sha])

    def test_outdate_is_instance_scoped(self):
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            # committed plan results for the SAME target in two instances —
            # §Phase 31: committed.yaml pointer at the instance dir
            for env in ("dev", "test"):
                d = (
                    root / "live/plan/target/env/tfstate_backend/instances"
                    / f"account={env}" / f"env_type={env}"
                )
                d.mkdir(parents=True)
                common.write_yaml_file(
                    d / "committed.yaml",
                    {"status": "succeeded", "target_keys": ["env/tfstate_backend"]},
                )
            # a provision run against dev only
            run_dir = (
                root / "live/provision/target/env/tfstate_backend/instances"
                / "account=dev/env_type=dev/runs/r1"
            )
            run_dir.mkdir(parents=True)
            common.write_run_metadata(
                run_dir,
                {
                    "run_id": "r1", "action": "provision", "run_type": "target",
                    "result_name": "env/tfstate_backend",
                    "result_key": "provision/target/env/tfstate_backend",
                    "ctl_state_local_root": str(root),
                    "ctl_state_locator": ["live"],
                    "instance": ["account=dev", "env_type=dev"],
                    "target_addresses": ["env/tfstate_backend/account=dev/env_type=dev"],
                    "target_keys": ["env/tfstate_backend"],
                    "mutation_started": True,
                },
            )
            common.mark_outdated_for_run(run_dir, include_current_result=True)
            dev = common.load_status_mapping(
                root / "live/plan/target/env/tfstate_backend/instances"
                / "account=dev/env_type=dev/committed.yaml"
            )
            test = common.load_status_mapping(
                root / "live/plan/target/env/tfstate_backend/instances"
                / "account=test/env_type=test/committed.yaml"
            )
            self.assertEqual(dev.get("status"), "outdated")
            self.assertEqual(test.get("status"), "succeeded")


if __name__ == "__main__":
    unittest.main()
