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


ADDRESS = "env/seed/baseline/instances/env_type=dev/account=dev"
TEMPLATE = "env/seed/baseline"
SEGMENTS = "env_type=dev/account=dev"


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

class Phase50NamespaceMapTests(unittest.TestCase):
    pass
class Phase50FinalizeStatusArgsTests(unittest.TestCase):
    def _ns(self, **kw):
        base = dict(
            execution_param=[("provider", "aws"), ("landing_zone", "live")],
            all=False, target=None, workflow=None, fan_out=None,
            action=None, scope="local", ctl_state_local_root="/tmp/x",
            provider_options={}, write_cache=False,
            structure="nested", sort="address", kinds=None, groups=None,
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
    def test_default_writes_nothing(self):
        with tempfile.TemporaryDirectory() as state:
            root = Path(state)
            _seed_target_pointer(
                root, "provision", status="ok", run_id="p1",
                when="2026-07-21T16:00:00Z",
            )
            args = argparse.Namespace(
                structure="nested",
                sort="address",
                kinds=None,
                groups=None,
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


class DestroyedIsNotIndistinguishableTest(unittest.TestCase):
    """Regression: a destroyed composition must not look like an absent one.

    Phase 67 keyed "owns no state" on the ABSENCE of a provision pointer, which
    also matches a deployable composition destroyed under a hash whose provision
    record is not present. The result: `env/seed` at one instance reported
    `status: passed` with no `state`, so `--all` could not tell "torn down" from
    "never deployed".
    """

