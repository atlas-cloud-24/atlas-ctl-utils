import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402
from utils.providers import aws  # noqa: E402


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class AccountRegistryTests(unittest.TestCase):
    def _root(self, body: str):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        write(root / "accounts.yaml", "providers:\n  aws:\n    accounts:\n" + body)
        return temporary, root

    def test_selector_member_resolves_exactly_one_account(self):
        temporary, root = self._root(
            "      ctl_plane:\n"
            "        members:\n"
            "        - selectors: {match: {execution_context.params.landing_zone: live}}\n"
            "          account_id: '111111111111'\n"
            "        - selectors: {match: {execution_context.params.landing_zone: canary}}\n"
            "          account_id: '222222222222'\n"
        )
        with temporary:
            result = aws.load_aws_account_registry_cfg(
                root,
                execution_context={"execution_context.params.landing_zone": "live"},
            )
        self.assertEqual(result, {"ctl_plane": "111111111111"})

    def test_selector_member_rejects_zero_and_multiple_matches(self):
        for second_selector, expected in (("canary", "matched 0"), ("live", "matched 2")):
            with self.subTest(expected=expected):
                temporary, root = self._root(
                    "      ctl_plane:\n"
                    "        members:\n"
                    "        - selectors: {match: {execution_context.params.landing_zone: live}}\n"
                    "          account_id: '111111111111'\n"
                    f"        - selectors: {{match: {{execution_context.params.landing_zone: {second_selector}}}}}\n"
                    "          account_id: '222222222222'\n"
                )
                with temporary, self.assertRaisesRegex(RuntimeError, expected):
                    aws.load_aws_account_registry_cfg(
                        root,
                        execution_context={"execution_context.params.landing_zone": "qa" if expected.endswith("0") else "live"},
                    )

    def test_selected_placeholder_is_rejected(self):
        temporary, root = self._root(
            "      ctl_plane:\n"
            "        members:\n"
            "        - selectors: {match: {execution_context.params.landing_zone: live}}\n"
            "          account_id: <live-ctl-plane-account-id>\n"
        )
        with temporary, self.assertRaisesRegex(RuntimeError, "12-digit account id"):
            aws.load_aws_account_registry_cfg(
                root,
                execution_context={"execution_context.params.landing_zone": "live"},
            )

    def test_duplicate_physical_id_across_branches_is_rejected(self):
        temporary, root = self._root(
            "      ctl_plane:\n"
            "        members:\n"
            "        - selectors: {match: {execution_context.params.landing_zone: live}}\n"
            "          account_id: '111111111111'\n"
            "        - selectors: {match: {execution_context.params.landing_zone: canary}}\n"
            "          account_id: '111111111111'\n"
        )
        with temporary, self.assertRaisesRegex(RuntimeError, "duplicate physical account ids"):
            aws.load_aws_account_registry_cfg(root)


class SessionPolicyTests(unittest.TestCase):
    def test_sync_policy_is_limited_to_approved_run_and_pointer(self):
        policy = aws.build_ctl_state_session_policy(
            "example-state",
            "sync",
            object_keys=["provision/target/app/committed.yaml"],
            object_prefixes=["provision/target/app/runs/019-test"],
        )
        serialized = str(policy)
        self.assertIn("provision/target/app/runs/019-test/*", serialized)
        self.assertIn("provision/target/app/committed.yaml", serialized)
        self.assertNotIn("arn:aws:s3:::example-state/*", serialized)
        delete_resources = [
            statement["Resource"]
            for statement in policy["Statement"]
            if "s3:DeleteObject" in statement.get("Action", [])
        ]
        self.assertEqual(delete_resources, ["arn:aws:s3:::example-state/locks/mutation.yaml"])

    def test_sync_policy_requires_explicit_scope(self):
        with self.assertRaisesRegex(RuntimeError, "requires approved object"):
            aws.build_ctl_state_session_policy("example-state", "sync")

    def test_maintenance_policy_has_exact_delete_and_manifest_write_only(self):
        policy = aws.build_ctl_state_session_policy(
            "example-state",
            "maintenance",
            object_keys=[
                "provision/target/app/runs/old/STATUS.yaml",
                "_maintenance/history-prune/report/manifest.yaml",
            ],
        )
        serialized = str(policy)
        self.assertNotIn("DeleteObjectVersion", serialized)
        put_statements = [
            statement for statement in policy["Statement"]
            if "s3:PutObject" in statement.get("Action", [])
        ]
        self.assertEqual(len(put_statements), 1)
        self.assertEqual(
            put_statements[0]["Resource"],
            ["arn:aws:s3:::example-state/_maintenance/history-prune/report/manifest.yaml"],
        )


class ConditionalPointerTests(unittest.TestCase):
    def test_pointer_publication_conflict_fails_without_last_writer_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pointer = root / "provision" / "target" / "app" / "committed.yaml"
            write(pointer, "run_id: test\n")
            syncer = aws.CtlStateSyncer(root, "bucket", "eu-west-2", "profile", root, required=True)
            syncer.ready = True
            not_found = subprocess.CompletedProcess([], 1, "", "404 Not Found")
            conflict = subprocess.CompletedProcess([], 1, "", "PreconditionFailed")
            with mock.patch.object(syncer, "_run_aws", side_effect=[not_found, conflict]) as run:
                with self.assertRaisesRegex(RuntimeError, "committed pointer conflict"):
                    syncer.publish_committed_pointer(pointer)
            self.assertIn("--if-none-match", run.call_args_list[-1].args[0])

    def test_existing_pointer_uses_if_match_etag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pointer = root / "provision" / "target" / "app" / "committed.yaml"
            write(pointer, "run_id: test\n")
            key = pointer.relative_to(root).as_posix()
            syncer = aws.CtlStateSyncer(root, "bucket", "eu-west-2", "profile", root, required=True)
            syncer.ready = True
            syncer.object_etags[key] = '"etag-1"'
            with mock.patch.object(
                syncer,
                "_run_aws",
                return_value=subprocess.CompletedProcess([], 0, "{}", ""),
            ) as run:
                syncer.publish_committed_pointer(pointer)
            args = run.call_args.args[0]
            self.assertEqual(args[args.index("--if-match") + 1], '"etag-1"')


class _MemorySyncer:
    def __init__(self, keys=()):
        self.keys = list(keys)
        self.puts = []
        self.deletes = []

    def list_object_keys(self, prefix=""):
        return [key for key in self.keys if key.startswith(prefix)]

    def pull_object(self, key):
        return True

    def put_object(self, key, path):
        self.puts.append((key, Path(path)))

    def delete_object_keys(self, keys):
        self.deletes.extend(keys)


class HistoryPruneTests(unittest.TestCase):
    def _args(self, root, run_id, *, cascade=False, apply=True):
        return mock.Mock(
            action="provision",
            ctl_profile="ctl_state_maintenance",
            execution_params={},
            execution_runtime_mode="local",
            ctl_state_local_root=root,
            execution_access_mode="standard",
            provider_credential=None,
            prune_run_id=[run_id],
            prune_before=None,
            prune_kind=None,
            cascade=cascade,
            apply_history_prune=apply,
        )

    def _run(self, root, args, reader, maintainer=None):
        returns = [("live", root / "live", reader)]
        if maintainer is not None:
            returns.append(("live", root / "live", maintainer))
        with (
            mock.patch.object(common, "ctl_allows_ctl_state_history_maintenance", return_value=True),
            mock.patch.object(common, "build_execution_context", return_value={}),
            mock.patch.object(common, "_arm_ctl_state_operation", side_effect=returns) as arm,
        ):
            result = common.run_ctl_state_history_prune(Path("/cfg"), args)
        return result, arm

    def test_current_committed_revision_cannot_be_pruned(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            namespace = root / "live"
            run_id = "old-target-run"
            key = f"provision/target/app/runs/{run_id}/STATUS.yaml"
            write(namespace / "provision/target/app/committed.yaml", f"run_id: {run_id}\n")
            with self.assertRaisesRegex(RuntimeError, "current committed revisions"):
                self._run(root, self._args(root, run_id), _MemorySyncer([key]))

    def test_retained_workflow_reference_requires_explicit_cascade(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            namespace = root / "live"
            target_run = "old-target-run"
            workflow_run = "old-workflow-run"
            target_key = f"provision/target/app/runs/{target_run}/STATUS.yaml"
            workflow_key = f"provision/workflow/deploy/runs/{workflow_run}/snapshot.yaml"
            write(
                namespace / workflow_key,
                "snapshot_of_run: old-workflow-run\n"
                "child_revisions:\n"
                "- run_id: old-target-run\n",
            )
            with self.assertRaisesRegex(RuntimeError, "referenced by retained workflow"):
                self._run(
                    root,
                    self._args(root, target_run),
                    _MemorySyncer([target_key, workflow_key]),
                )

    def test_cascade_deletes_target_and_referencing_workflow_with_exact_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            namespace = root / "live"
            target_run = "old-target-run"
            workflow_run = "old-workflow-run"
            target_key = f"provision/target/app/runs/{target_run}/STATUS.yaml"
            workflow_key = f"provision/workflow/deploy/runs/{workflow_run}/snapshot.yaml"
            write(
                namespace / workflow_key,
                "snapshot_of_run: old-workflow-run\n"
                "child_revisions:\n"
                "- run_id: old-target-run\n",
            )
            maintainer = _MemorySyncer()
            result, arm = self._run(
                root,
                self._args(root, target_run, cascade=True),
                _MemorySyncer([target_key, workflow_key]),
                maintainer,
            )
            self.assertEqual(result["candidate_run_ids"], [target_run, workflow_run])
            self.assertEqual(maintainer.deletes, [target_key, workflow_key])
            maintenance_scope = arm.call_args_list[1].kwargs["object_keys"]
            self.assertIn(target_key, maintenance_scope)
            self.assertIn(workflow_key, maintenance_scope)
            self.assertTrue(any(key.startswith("_maintenance/history-prune/") for key in maintenance_scope))

    def test_dry_run_writes_manifest_without_deleting_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_id = "old-target-run"
            key = f"provision/target/app/runs/{run_id}/STATUS.yaml"
            maintainer = _MemorySyncer()
            result, _ = self._run(
                root,
                self._args(root, run_id, apply=False),
                _MemorySyncer([key]),
                maintainer,
            )
            self.assertTrue(result["dry_run"])
            self.assertEqual(maintainer.deletes, [])
            self.assertEqual(len(maintainer.puts), 1)


if __name__ == "__main__":
    unittest.main()
