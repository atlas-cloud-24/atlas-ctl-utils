"""A query must never mutate local ctl-state.

`--status` gained an explicit scope. The guarantee under test: NEITHER scope
writes to the local ctl-state tree. `remote` hydrates into a throwaway root
(pull_object is an unconditional overwrite, so hydrating into the real tree
destroys a force-skipped, local-only pointer); `local` never calls the bucket.
"""


import argparse
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.commands import maintenance as commands_maintenance
from engine.kernel import yaml_io as kernel_yaml_io
from engine.cli import args as cli_args

LOCAL_ONLY_POINTER = {"run_id": "force-skipped-run-x", "status": "ok"}
NAMESPACE = "live"
PREFIX = "target/env/core/instances/account=dev"


def _seed_local_only_pointer(root: Path) -> Path:
    """A run made with --force-skip-ctl-state-backend-sync: it exists ONLY
    locally and can never reach the bucket."""

    pointer = root / NAMESPACE / PREFIX / "committed.yaml"
    kernel_yaml_io.write_yaml_file(pointer, LOCAL_ONLY_POINTER)
    return pointer


from engine.execution import run_context as execution_run_context


from engine.state import run_store as state_run_store


from engine.state import sync as state_sync


class _RecordingSyncer:
    """

    stands in for the armed reader; records what it was asked to hydrate and
    simulates pull_object's unconditional overwrite into its own root."""

    def __init__(self, root: Path):
        self.root = root
        self.hydrated: list[str] = []

    def hydrate_instance(self, prefix, child_prefixes=None):
        self.hydrated.append(prefix)
        kernel_yaml_io.write_yaml_file(
            self.root / prefix / "committed" / "mutative.yaml",
            {"run_id": "older-bucket-run-y"},
        )

    def pull_object(self, key):
        kernel_yaml_io.write_yaml_file(self.root / key, {"run_id": "older-bucket-run-y"})
        return True

    def put_object(self, key, path):
        # the sweep pushes ONE root-level status_cache.yaml.
        self.hydrated.append(f"put:{key}")
        return True


def _status_args(local_root: Path, scope: str) -> argparse.Namespace:
    return argparse.Namespace(
        status=scope,
        action="provision",
        target="env/core",
        workflow=None,
        fan_out=None,
        ctl_profile="dev",
        ctl_variants=[],
        execution_params={"account": "dev"},
        execution_runtime_mode="local",
        force_skip_full_cfg_validation_gate=False,
        execution_access_modes={"aws": "standard"},
        provider_options={},
        ctl_ref_policy="commit_required",
        ctl_state_local_root=local_root,
    )


SPEC = {
    "kind": "target",
    "key": "env/core",
    "segments": ["account=dev"],
    "address": "env/core/instances/account=dev",
    "prefix": PREFIX,
}
SELECTION = {
    "selection_key": "env/core",
    "execution_context": {"execution_context.params.account": "dev"},
}


class StatusScopeTests(unittest.TestCase):
    def test_status_lives_in_the_slim_parser_not_the_run_parser(self):
        # Status is not a MODE on the run runners: the standalone status.py owns
        # it, so the run parser must not carry --status.
        run_parser = argparse.ArgumentParser()
        cli_args.add_common_args(run_parser, run_type="target")
        self.assertFalse(
            any("--status" in item.option_strings for item in run_parser._actions),
            "--status must be gone from the run parser",
        )
        # The slim status parser: --scope is the explicit-scope arg (local|remote),
        # and breadth (--all/--target/--workflow/--fan-out) is a required choice.
        status_parser = argparse.ArgumentParser()
        cli_args.add_status_args(status_parser)
        scope = next(
            item for item in status_parser._actions if "--scope" in item.option_strings
        )
        self.assertEqual(tuple(scope.choices), ("local", "remote"))
        self.assertIsNone(scope.default)
        with self.assertRaises(SystemExit):  # no breadth, no scope
            status_parser.parse_args(
                ["--ctl-cfg", "x", "--ctl-profile", "dev"]
            )
        with self.assertRaises(SystemExit):  # two breadths are mutually exclusive
            status_parser.parse_args(
                [
                    "--ctl-cfg", "x", "--ctl-profile", "dev",
                    "--all", "--target", "env/core", "--scope", "local",
                ]
            )

class Phase42SweepScopeTests(unittest.TestCase):
    def test_status_sweep_hydrates_into_a_throwaway_root(self):
        """

        the sweep hydrates EVERY pointer in the namespace, so running it
        against the real local root would clobber local-only records wholesale.
        Its advisory status_cache.yaml still reaches the bucket."""

        with tempfile.TemporaryDirectory() as tmp:
            local_root = Path(tmp)
            pointer = _seed_local_only_pointer(local_root)
            before = pointer.read_bytes()
            armed_roots: list[Path] = []

            def fake_arm(cfg_root, context, ctl_state_root, *, operation, **kwargs):
                armed_roots.append(Path(ctl_state_root))
                namespace_root = Path(ctl_state_root) / NAMESPACE
                return NAMESPACE, namespace_root, _RecordingSyncer(namespace_root)

            args = _status_args(local_root, None)
            with patch.object(
                execution_run_context, "build_execution_context", return_value={}
            ), patch.object(
                state_sync.CtlStateAccess, "arm_operation", side_effect=fake_arm
            ), patch.object(
                state_run_store, "hydrate_ctl_state_index", return_value=[]
            ):
                commands_maintenance.run_ctl_state_status_sweep(Path("/nonexistent-cfg"), args)

            for root in armed_roots:
                self.assertNotEqual(root, local_root)
                self.assertFalse(root.exists())
            self.assertEqual(pointer.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
