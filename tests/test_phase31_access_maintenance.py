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
        for landing_zone in ("live", "canary"):
            write(
                root / "_inputs" / "aws" / "account_registries" / f"{landing_zone}.yaml",
                "accounts_registry:\n" + body,
            )
        write(
            root / "ctl_sources.yaml",
            "ctl_sources:\n  aws.accounts_registry:\n"
            "    type: map\n"
            "    conflict_resolution: error\n"
            "    sources:\n"
            "    - provider: local\n"
            "      format: yaml\n"
            "      file_path:\n"
            "        members:\n"
            "        - value: /_inputs/aws/account_registries/live.yaml\n"
            "          selectors:\n"
            "            match:\n"
            "              execution_context.params.landing_zone: live\n"
            "        - value: /_inputs/aws/account_registries/canary.yaml\n"
            "          selectors:\n"
            "            match:\n"
            "              execution_context.params.landing_zone: canary\n",
        )
        return temporary, root

    def test_zone_file_is_selected_by_landing_zone(self):
        """A landing zone is a separate AWS organization, so the zone selects the
        FILE and each file holds one flat inventory — no selectors, no merging."""
        temporary, root = self._root("  ctl_plane:\n    slug: ctl-plane\n    account_id: '111111111111'\n")
        root_path = Path(temporary.name)
        write(
            root_path / "_inputs" / "aws" / "account_registries" / "canary.yaml",
            "accounts_registry:\n  ctl_plane:\n    slug: ctl-plane\n    account_id: '222222222222'\n",
        )
        with temporary:
            for landing_zone, expected in (("live", "111111111111"), ("canary", "222222222222")):
                self.assertEqual(
                    aws.load_aws_account_registry_cfg(
                        root,
                        execution_context={"execution_context.params.landing_zone": landing_zone},
                    ),
                    {"ctl_plane": expected},
                )

    def test_unknown_landing_zone_is_rejected(self):
        temporary, root = self._root("  ctl_plane:\n    slug: ctl-plane\n    account_id: '111111111111'\n")
        with temporary, self.assertRaisesRegex(RuntimeError, "must resolve exactly one member"):
            aws.load_aws_account_registry_cfg(
                root, execution_context={"execution_context.params.landing_zone": "qa"}
            )

    def test_selected_placeholder_is_rejected(self):
        temporary, root = self._root(
            "  ctl_plane:\n"
            "    slug: ctl-plane\n"
            "    account_id: <live-ctl-plane-account-id>\n"
        )
        with temporary, self.assertRaisesRegex(RuntimeError, "12-digit account id"):
            aws.load_aws_account_registry_cfg(
                root,
                execution_context={"execution_context.params.landing_zone": "live"},
            )

    def test_catalog_reports_placeholder_as_concrete_binding_failure(self):
        temporary, root = self._root(
            "  management:\n"
            "    slug: management\n"
            "    account_id: <live-management-account-id>\n"
        )
        with temporary:
            aws.validate_catalog(root)
            findings = aws.collect_provider_cfg_findings(
                root,
                execution_context={"execution_context.params.landing_zone": "live"},
            )
        self.assertEqual(findings[0]["status"], "failed")
        self.assertFalse(findings[0]["structural"])
        self.assertIn("12-digit account id", findings[0]["error"])

    def test_runtime_catalog_allows_unrelated_placeholder_but_selected_access_is_strict(
        self,
    ):
        temporary, root = self._root(
            "  management:\n"
            "    slug: management\n"
            "    account_id: <live-management-account-id>\n"
            "  dev:\n"
            "    slug: dev\n"
            "    account_id: '111111111111'\n"
        )
        with temporary:
            context = {
                "execution_context.params.provider": "aws",
                "execution_context.ctl.providers": ["aws"],
                "execution_context.params.landing_zone": "live",
            }
            catalogs = aws.load_runtime_catalogs(root, execution_context=context)
            self.assertEqual(
                catalogs["account_registry"]["management"],
                "<live-management-account-id>",
            )
            executions = {
                "dev_direct": {
                    "provider": "aws",
                    "account": "dev",
                    "roles": {"readwrite": "ctl_target"},
                    "agreed_direct_credential_source_keys": ["dev"],
                },
                "management_direct": {
                    "provider": "aws",
                    "account": "management",
                    "roles": {"readwrite": "ctl_target"},
                    "agreed_direct_credential_source_keys": ["management"],
                },
            }
            catalogs["credential_sources"] = {
                key: {
                    "profile": {
                        "profile_name": key,
                        "expect": {"account_key": account_key, "role_name": "Admin"},
                    }
                }
                for key, account_key in (
                    ("dev", "dev"),
                    ("management", "management"),
                )
            }

            dev_cfg_result = aws.resolve_target_cfg_references(
                "dev",
                {"execution_identities": {"aws": executions["dev_direct"]}},
                catalogs,
                execution_context=context,
                implementation_key="profile",
                execution_access_mode="agreed_direct",
            )
            self.assertEqual(dev_cfg_result["status"], "passed")

            management_cfg_result = aws.resolve_target_cfg_references(
                "management",
                {"execution_identities": {"aws": executions["management_direct"]}},
                catalogs,
                execution_context=context,
                implementation_key="profile",
                execution_access_mode="agreed_direct",
            )
            self.assertEqual(management_cfg_result["status"], "failed")
            self.assertIn(
                "12-digit account id",
                management_cfg_result["failure_reason"],
            )

            with mock.patch.object(
                aws,
                "resolve_configured_profile_account_id",
                return_value="111111111111",
            ):
                aws.validate_active_target_access(
                    {"dev": {"execution_identities": {"aws": executions["dev_direct"]}}},
                    catalogs,
                    execution_context=context,
                    implementation_key="profile",
                    execution_access_mode="agreed_direct",
                )
            self.assertEqual(
                catalogs["validated_account_registry"],
                {"dev": "111111111111"},
            )

            with self.assertRaisesRegex(
                common.ProviderConfigBlockedError,
                r"accounts_registry\.management\.account_id must be a 12-digit account id",
            ):
                aws.validate_active_target_access(
                    {
                        "management": {
                            "execution_identities": {"aws": executions["management_direct"]}
                        }
                    },
                    catalogs,
                    execution_context=context,
                    implementation_key="profile",
                    execution_access_mode="agreed_direct",
                )


class AccountSlugTests(unittest.TestCase):
    """§Phase 59: the account's AWS-facing spelling is a property OF THE ACCOUNT,
    so the adapter derives it from the registry rather than every caller passing
    it. S3 bucket names reject the underscores the internal keys use."""

    def _root(self, body: str):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        for landing_zone in ("live", "canary"):
            write(
                root / "_inputs" / "aws" / "account_registries" / f"{landing_zone}.yaml",
                "accounts_registry:\n" + body,
            )
        write(
            root / "ctl_sources.yaml",
            "ctl_sources:\n  aws.accounts_registry:\n"
            "    type: map\n"
            "    conflict_resolution: error\n"
            "    sources:\n"
            "    - provider: local\n"
            "      format: yaml\n"
            "      file_path:\n"
            "        members:\n"
            "        - value: /_inputs/aws/account_registries/live.yaml\n"
            "          selectors:\n"
            "            match:\n"
            "              execution_context.params.landing_zone: live\n"
            "        - value: /_inputs/aws/account_registries/canary.yaml\n"
            "          selectors:\n"
            "            match:\n"
            "              execution_context.params.landing_zone: canary\n",
        )
        return temporary, root

    ENTRY = (
        "  non_prod_email_svc:\n"
        "    slug: non-prod-email-svc\n"
        "    account_id: '111111111111'\n"
    )

    def test_derived_params_publishes_the_slug_for_the_declared_account(self):
        temporary, root = self._root(self.ENTRY)
        with temporary:
            self.assertEqual(
                aws.derived_params(
                    root, {"aws.account": "non_prod_email_svc", "landing_zone": "live"}
                ),
                {"aws.account_slug": "non-prod-email-svc"},
            )

    def test_no_declared_account_derives_nothing(self):
        temporary, root = self._root(self.ENTRY)
        with temporary:
            self.assertEqual(aws.derived_params(root, {"landing_zone": "live"}), {})

    def test_unknown_account_is_rejected(self):
        temporary, root = self._root(self.ENTRY)
        with temporary, self.assertRaisesRegex(RuntimeError, "cannot derive aws.account_slug"):
            aws.derived_params(root, {"aws.account": "nope", "landing_zone": "live"})

    def test_missing_slug_is_rejected(self):
        temporary, root = self._root("  ctl_plane:\n    account_id: '111111111111'\n")
        with temporary, self.assertRaisesRegex(RuntimeError, r"ctl_plane\.slug is required"):
            aws.load_aws_account_slugs(root, "live")

    def test_slug_must_be_a_legal_bucket_name_fragment(self):
        temporary, root = self._root(
            "  ctl_plane:\n    slug: ctl_plane\n    account_id: '111111111111'\n"
        )
        with temporary, self.assertRaisesRegex(RuntimeError, "separated by single hyphens"):
            aws.load_aws_account_slugs(root, "live")


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


class ToolLockIsNotTheEngineTest(unittest.TestCase):
    """Releasing a TOOL's state lock left the engine entirely (§Phase 67).

    It used to be a maintenance action against a target, which forced the engine
    to read the step's SOURCE — grepping `step.sh` for `./bin/tf.sh <dir> init
    <var>` — to discover where that tool kept its state. That was the only place
    ctl reached inside a step instead of going through `step.yaml`, and it broke
    silently whenever a repo renamed the script or wrapped the call.

    A repo now declares an `unlock` procedure and the engine runs it by name.
    """

    def test_the_binding_resolver_is_gone(self):
        for name in (
            "FORCE_UNLOCK_INIT_RE",
            "resolve_force_unlock_tfstate_binding",
            "force_unlock_resource_kind",
        ):
            self.assertFalse(
                hasattr(common, name), f"{name} still exists in engine core"
            )

    def test_maintenance_against_a_target_points_at_the_procedure(self):
        """The error names the replacement rather than only refusing."""
        source = Path(common.__file__).read_text()
        self.assertIn("does not operate on a target", source)
        self.assertIn("--procedure unlock", source)


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
            execution_access_modes={"aws": "standard"},
            provider_options={},
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
            target_key = f"provision/target/app/runs/{target_run}/RUN.yaml"
            workflow_key = f"provision/workflow/deploy/runs/{workflow_run}/RUN.yaml"
            write(
                namespace / workflow_key,
                "run_id: old-workflow-run\n"
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
            target_key = f"provision/target/app/runs/{target_run}/RUN.yaml"
            workflow_key = f"provision/workflow/deploy/runs/{workflow_run}/RUN.yaml"
            write(
                namespace / workflow_key,
                "run_id: old-workflow-run\n"
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
