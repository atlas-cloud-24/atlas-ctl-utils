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
    "domains.yaml": (
        "domains:\n"
        "  env:\n"
        "    description: workload environment platform\n"
    ),
    "workflow.yaml": (
        "workflows:\n"
        "  env/bootstrap:\n"
        "    default_action: provision\n"
        "    target_keys:\n"
        "      - env/tfstate_backend\n"
    ),
}
TARGETS = (
    "targets:\n"
    "  env/tfstate_backend:\n"
    "    actions: [provision]\n"
    "    source_key: bootstrap\n"
    "    ref_key: env/${execution_context.params.env.type}\n"
    "    procedure_key: tfstate_backend\n"
    "    domains: [env]\n"
    "    input_params: [account, env_type]\n"
    "    cfg_keys:\n"
    "      env: [ctl_state_s3_bucket_name]\n"
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

    def test_fan_out_and_procedure_stay_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(tmp)
            for run_type in ("fan_out", "procedure"):
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
                ident["address"], "env/tfstate_backend/instances/account=dev/env_type=dev"
            )

    def test_a_workflow_has_no_instance_layer(self):
        """§Phase 73: a workflow publishes history, so it has no composition
        digest and no identity document — only the members it ran with."""
        with tempfile.TemporaryDirectory() as tmp:
            root = make_cfg(tmp)
            ident = common.resolve_run_instance_identity(
                root, run_type="workflow", action="provision", ctl_profile=None,
                execution_params=PARAMS, execution_runtime_mode="local",
                workflow_name="env/bootstrap",
            )
            self.assertEqual([], ident["instance_segments"])
            self.assertEqual("env/bootstrap", ident["address"])
            self.assertIsNone(ident["identity_doc"])
            self.assertEqual(
                ident["target_addresses"],
                ["env/tfstate_backend/instances/account=dev/env_type=dev"],
            )

if __name__ == "__main__":
    unittest.main()
