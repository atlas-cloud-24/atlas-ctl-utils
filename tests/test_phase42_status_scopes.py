"""§Phase 42 — a query must never mutate local ctl-state.

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

from utils import common  # noqa: E402


LOCAL_ONLY_POINTER = {"run_id": "force-skipped-run-x", "status": "ok"}
NAMESPACE = "live"
PREFIX = "provision/target/env/core/instances/account=dev"


def _seed_local_only_pointer(root: Path) -> Path:
    """A run made with --force-skip-ctl-state-backend-sync: it exists ONLY
    locally and can never reach the bucket."""
    pointer = root / NAMESPACE / PREFIX / "committed.yaml"
    common.write_yaml_file(pointer, LOCAL_ONLY_POINTER)
    return pointer


class _RecordingSyncer:
    """Stands in for the armed reader; records what it was asked to hydrate and
    simulates pull_object's unconditional overwrite into its own root."""

    def __init__(self, root: Path):
        self.root = root
        self.hydrated: list[str] = []

    def hydrate_instance(self, prefix, child_prefixes=None):
        self.hydrated.append(prefix)
        common.write_yaml_file(
            self.root / prefix / "committed.yaml",
            {"run_id": "older-bucket-run-y"},
        )

    def pull_object(self, key):
        common.write_yaml_file(self.root / key, {"run_id": "older-bucket-run-y"})
        return True

    def put_object(self, key, path):
        # §Phase 50.10: the sweep pushes ONE root-level status_cache.yaml.
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
    "address": "env/core/account=dev",
    "prefix": PREFIX,
}
SELECTION = {
    "selection_key": "env/core",
    "execution_context": {"execution_context.params.account": "dev"},
}


class Phase42StatusScopeTests(unittest.TestCase):
    def test_status_removed_from_run_parser_and_lives_in_the_slim_parser(self):
        # §Phase 50: status is no longer a MODE on the run runners — the
        # standalone status.py owns it. The run parser must not carry --status.
        run_parser = argparse.ArgumentParser()
        common.add_common_args(run_parser, run_type="target")
        self.assertFalse(
            any("--status" in item.option_strings for item in run_parser._actions),
            "--status must be gone from the run parser",
        )
        # The slim status parser: --scope is the explicit-scope arg (local|remote),
        # and breadth (--all/--target/--workflow/--fan-out) is a required choice.
        status_parser = argparse.ArgumentParser()
        common.add_status_args(status_parser)
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

    def test_remote_hydrates_into_a_throwaway_root_not_the_local_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            local_root = Path(tmp)
            pointer = _seed_local_only_pointer(local_root)
            before = pointer.read_bytes()
            armed_roots: list[Path] = []

            def fake_arm(cfg_root, selection, ctl_state_local_root, **kwargs):
                armed_roots.append(Path(ctl_state_local_root))
                namespace_root = Path(ctl_state_local_root) / NAMESPACE
                return NAMESPACE, namespace_root, _RecordingSyncer(namespace_root)

            with patch.object(
                common, "resolve_pipeline_selection", return_value=SELECTION
            ), patch.object(
                common, "selection_state_spec", return_value=SPEC
            ), patch.object(
                common, "_arm_ctl_state_reader", side_effect=fake_arm
            ):
                report = common.run_status_command(
                    Path("/nonexistent-cfg"),
                    _status_args(local_root, "remote"),
                    run_type="target",
                )

            self.assertEqual(report["scope"], "remote")
            # The bucket view was hydrated somewhere OTHER than the local tree,
            # and that somewhere is gone afterwards.
            scratch = armed_roots[0]
            self.assertNotEqual(scratch, local_root)
            self.assertFalse(scratch.exists())
            # The local-only pointer survived a read-only query untouched.
            self.assertEqual(pointer.read_bytes(), before)

    def test_local_reads_the_tree_with_no_bucket_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            local_root = Path(tmp)
            pointer = _seed_local_only_pointer(local_root)
            before = pointer.read_bytes()

            def fail_if_armed(*args, **kwargs):
                raise AssertionError("local scope must not arm a ctl-state reader")

            with patch.object(
                common, "resolve_pipeline_selection", return_value=SELECTION
            ), patch.object(
                common, "selection_state_spec", return_value=SPEC
            ), patch.object(
                common, "_arm_ctl_state_reader", side_effect=fail_if_armed
            ), patch.object(
                common, "resolve_ctl_state_namespace", return_value=(NAMESPACE, {})
            ):
                report = common.run_status_command(
                    Path("/nonexistent-cfg"),
                    _status_args(local_root, "local"),
                    run_type="target",
                )

            self.assertEqual(report["scope"], "local")
            self.assertEqual(report["namespace"], NAMESPACE)
            # local is the ONLY view that can still see the force-skipped run.
            self.assertEqual(pointer.read_bytes(), before)
            self.assertEqual(
                report["results"][0]["run_id"], LOCAL_ONLY_POINTER["run_id"]
            )


class Phase42SweepScopeTests(unittest.TestCase):
    def test_status_sweep_hydrates_into_a_throwaway_root(self):
        """The sweep hydrates EVERY pointer in the namespace, so running it
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
                common, "build_execution_context", return_value={}
            ), patch.object(
                common, "_arm_ctl_state_operation", side_effect=fake_arm
            ), patch.object(
                common, "hydrate_ctl_state_index", return_value=[]
            ):
                common.run_ctl_state_status_sweep(Path("/nonexistent-cfg"), args)

            for root in armed_roots:
                self.assertNotEqual(root, local_root)
                self.assertFalse(root.exists())
            self.assertEqual(pointer.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
