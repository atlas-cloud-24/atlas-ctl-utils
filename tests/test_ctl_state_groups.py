import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import ctl_cfg_fixture
from engine.catalog import targets as catalog_targets
from engine.catalog import workflow as catalog_workflow
from engine.commands import pipeline as commands_pipeline
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import selectors as run_selectors
from engine.state import run_store as state_run_store
from engine.cli import args as cli_args

LIVE_CTX = {"execution_context.params.landing_zone": "live"}
CANARY_CTX = {"execution_context.params.landing_zone": "canary"}


def _write(root: Path, name: str, body: str) -> None:
    (root / name).write_text(body, encoding="utf-8")


class FanOutMemberSchemaTests(unittest.TestCase):
    """param-set members are {params, selectors?}; selector-gated
    members drop per the frozen execution context; domain params validate
    against the registry (3d)."""

    def _root(self, tmp: str) -> Path:
        root = Path(tmp)
        _write(root, "fan_outs.yaml", (
            "fan_outs:\n"
            "  lz/all:\n"
            "    runs:\n"
            "      - workflow_key: wf/one\n"
            "        fan_out_param_set_key: state_domains\n"
            "    failure_mode: stop\n"
        ))
        _write(root, "domains.yaml", (
            "domains:\n  org: {}\n  dev: {}\n  prodlike: {}\n"
        ))
        return root

    def test_selector_gated_members_drop_per_zone(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root, "param_sets.yaml", (
                "fan_out_param_sets:\n"
                "  state_domains:\n"
                "    org:\n"
                "      params:\n"
                "        domain: org\n"
                "    dev:\n"
                "      params:\n"
                "        domain: dev\n"
                "      selectors:\n"
                "        match:\n"
                "          execution_context.params.landing_zone: live\n"
                "    prodlike:\n"
                "      params:\n"
                "        domain: prodlike\n"
                "      selectors:\n"
                "        match:\n"
                "          execution_context.params.landing_zone: canary\n"
            ))
            live = catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)
            self.assertEqual(
                [c["fan_out_param_entry_key"] for c in live["children"]], ["org", "dev"]
            )
            canary = catalog_workflow.expand_fan_out(root, "lz/all", CANARY_CTX)
            self.assertEqual(
                [c["fan_out_param_entry_key"] for c in canary["children"]], ["org", "prodlike"]
            )

    def test_selectors_inside_params_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root, "param_sets.yaml", (
                "fan_out_param_sets:\n"
                "  state_domains:\n"
                "    org:\n"
                "      params:\n"
                "        domain: org\n"
                "        selectors: bad\n"
            ))
            with self.assertRaisesRegex(RuntimeError, "selectors must be a member field"):
                catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)

    def test_bare_map_member_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root, "param_sets.yaml", (
                "fan_out_param_sets:\n"
                "  state_domains:\n"
                "    org:\n"
                "      domain: org\n"
            ))
            with self.assertRaisesRegex(RuntimeError, "unsupported keys"):
                catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)

    def test_unknown_domain_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root, "param_sets.yaml", (
                "fan_out_param_sets:\n"
                "  state_domains:\n"
                "    typo:\n"
                "      params:\n"
                "        domain: identiy\n"
            ))
            with self.assertRaisesRegex(RuntimeError, "unknown domain 'identiy'"):
                catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)

    def test_all_members_dropped_is_hard_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root, "param_sets.yaml", (
                "fan_out_param_sets:\n"
                "  state_domains:\n"
                "    dev:\n"
                "      params:\n"
                "        domain: dev\n"
                "      selectors:\n"
                "        match:\n"
                "          execution_context.params.landing_zone: live\n"
            ))
            with self.assertRaisesRegex(RuntimeError, "no member of fan_out_param_set"):
                catalog_workflow.expand_fan_out(root, "lz/all", CANARY_CTX)


class SelectorGroupResolverTests(unittest.TestCase):
    """selector-membered group entries resolve to exactly one
    member value."""

    GROUP = {
        "members": [
            {
                "cfg_file_set_key": "env_backend",
                "selectors": {"in": {"execution_context.params.domain": ["dev", "test"]}},
            },
            {
                "cfg_file_set_key": "org",
                "selectors": {"match": {"execution_context.params.domain": "org"}},
            },
        ]
    }

    def test_resolves_exactly_one_member(self):
        value = run_selectors.resolve_selector_group_member(
            self.GROUP, {"execution_context.params.domain": "org"},
            value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
        )
        self.assertEqual(value, "org")

    def test_in_selector_member(self):
        value = run_selectors.resolve_selector_group_member(
            self.GROUP, {"execution_context.params.domain": "test"},
            value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
        )
        self.assertEqual(value, "env_backend")

    def test_no_match_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "matched 0"):
            run_selectors.resolve_selector_group_member(
                self.GROUP, {"execution_context.params.domain": "identity"},
                value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
            )

    def test_bad_member_shape_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "group member must be"):
            run_selectors.resolve_selector_group_member(
                {"members": [{"wrong_field": "x"}]},
                {"execution_context.params.domain": "org"},
                value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
            )


class TargetDomainKeysInventoryTests(unittest.TestCase):
    """A target DECLARES the domains it reads and, per domain, the
    content keys it consumes. A domain-GENERIC target takes its domain from the
    execution context and stays unresolved without one."""

    def _root(self, tmp: str) -> Path:
        root = Path(tmp)
        _write(root, "target_sources.yaml", (
            "target_sources:\n"
            "  bootstrap:\n"
            "    repo_url: https://example.invalid/bootstrap.git\n"
        ))
        _write(root, "domains.yaml", "domains:\n  org: {}\n  env: {}\n")
        _write(root, "cfg_key_sets.yaml", (
            "cfg_key_sets:\n"
            "  tfstate_backend_key_set:\n"
            "    cfg_keys:\n"
            "      - main_tag\n"
            "      - tfstate_s3_bucket_name\n"
        ))
        (root / "targets" / "provision").mkdir(parents=True)
        _write(root / "targets" / "provision", "t.yaml", (
            "targets:\n"
            "  lz/tfstate_backend:\n"
            "    actions: [provision]\n"
            "    source_key: bootstrap\n"
            "    ref_key: state_backend\n"
            "    procedure_key: tfstate_backend\n"
            "    domains: [\"${execution_context.params.domain}\"]\n"
            "    cfg_key_sets:\n"
            "      \"${execution_context.params.domain}\": [tfstate_backend_key_set]\n"
        ))
        # loading an action validates each target's execution identity, which
        # dispatches to the adapter of a provider this root has to declare
        return ctl_cfg_fixture.activate(self, ctl_cfg_fixture.declare_providers(root, "aws"))

    def test_generic_domain_resolves_from_the_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            inv = catalog_targets.load_action_cfg(
                root, "provision", {"execution_context.params.domain": "org"}
            )
            target = inv["targets"]["lz/tfstate_backend"]
            self.assertEqual(target["domains"], ["org"])
            # the cfg_key_set expands to its member KEYS
            self.assertEqual(
                target["cfg_keys"], {"org": ["main_tag", "tfstate_s3_bucket_name"]}
            )

    def test_generic_domain_unresolved_without_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            inv = catalog_targets.load_action_cfg(root, "provision")
            target = inv["targets"]["lz/tfstate_backend"]
            self.assertIsNone(target["domains"])
            self.assertEqual(
                target["domains_unresolved"], ["execution_context.params.domain"]
            )

    def test_unresolved_domains_survive_the_ctl_cfg_snapshot(self):
        """

        the snapshot resolves every scalar it walks, so the deferred marker
        must record AXIS NAMES, never the raw `${...}` template."""

        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            inv = catalog_targets.load_action_cfg(root, "provision")
            marker = inv["targets"]["lz/tfstate_backend"]["domains_unresolved"]
            self.assertEqual(marker, ["execution_context.params.domain"])
            self.assertFalse(any("${" in entry for entry in marker))
            # the real failure this guards: run_pipeline snapshots the action
            execution_run_context.resolve_ctl_structure(inv, {}, label="action.provision")

    def test_unknown_domain_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            with self.assertRaisesRegex(RuntimeError, "unknown domain"):
                catalog_targets.load_action_cfg(
                    root, "provision", {"execution_context.params.domain": "nope"}
                )

    def test_cfg_keys_for_an_undeclared_domain_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root / "targets" / "provision", "t.yaml", (
                "targets:\n"
                "  t:\n"
                "    actions: [provision]\n"
                "    source_key: bootstrap\n"
                "    ref_key: r\n"
                "    procedure_key: s\n"
                "    domains: [env]\n"
                "    cfg_keys:\n"
                "      env: [main_tag]\n"
                "      org: [main_tag]\n"
            ))
            with self.assertRaisesRegex(RuntimeError, "not in its domains"):
                catalog_targets.load_action_cfg(root, "provision", {})

    def test_declared_domain_without_cfg_keys_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root / "targets" / "provision", "t.yaml", (
                "targets:\n"
                "  t:\n"
                "    actions: [provision]\n"
                "    source_key: bootstrap\n"
                "    ref_key: r\n"
                "    procedure_key: s\n"
                "    domains: [env, org]\n"
                "    cfg_keys:\n"
                "      env: [main_tag]\n"
            ))
            with self.assertRaisesRegex(RuntimeError, "no\\s+cfg_keys entry"):
                catalog_targets.load_action_cfg(root, "provision", {})


class TargetInputParamsTests(unittest.TestCase):
    """A target declares the coordinates it reads; instance params
    must be a SUBSET of them, checked on every members branch."""

    def _root(self, tmp: str, target_body: str) -> Path:
        root = Path(tmp)
        _write(root, "target_sources.yaml",
               "target_sources:\n  bootstrap:\n    repo_url: https://example.invalid/b.git\n")
        _write(root, "domains.yaml", "domains:\n  env: {}\n")
        _write(root, "cfg_key_sets.yaml",
               "cfg_key_sets:\n  k:\n    cfg_keys: [main_tag]\n")
        _write(root, "param_sets.yaml",
               "param_sets:\n  base:\n    input_params: [main_tag, landing_zone]\n")
        (root / "targets" / "provision").mkdir(parents=True)
        _write(root / "targets" / "provision", "t.yaml",
               "targets:\n  t:\n    actions: [provision]\n    source_key: bootstrap\n"
               "    ref_key: r\n    procedure_key: s\n    domains: [env]\n"
               "    cfg_key_sets:\n      env: [k]\n" + target_body)
        return ctl_cfg_fixture.activate(self, ctl_cfg_fixture.declare_providers(root, "aws"))

    def test_param_set_expands_and_instance_subset_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp,
                "    input_param_sets: [base]\n"
                "    input_params: [env_type]\n"
                "    target_instance_params: [env_type]\n")
            t = catalog_targets.load_action_cfg(root, "provision", {})["targets"]["t"]
            self.assertEqual(t["input_params"], ["main_tag", "landing_zone", "env_type"])
            self.assertEqual(t["target_instance_params"], ["env_type"])

    def test_instance_param_not_declared_as_input_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp,
                "    input_param_sets: [base]\n"
                "    target_instance_params: [env_type]\n")
            with self.assertRaisesRegex(RuntimeError, "not\\s+declared input params"):
                catalog_targets.load_action_cfg(root, "provision", {})

    def test_unselected_members_branch_is_also_checked(self):
        """A branch this context does not select must still be valid."""

        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp,
                "    input_param_sets: [base]\n"
                "    input_params: [env_type]\n"
                "    target_instance_params:\n"
                "      members:\n"
                "      - params: [env_type]\n"
                "        selectors:\n          match:\n            execution_context.params.env.type: dev\n"
                "      - params: [nope]\n"
                "        selectors:\n          match:\n            execution_context.params.env.type: prod\n")
            with self.assertRaisesRegex(RuntimeError, "not\\s+declared input params"):
                catalog_targets.load_action_cfg(
                    root, "provision", {"execution_context.params.env.type": "dev"}
                )

    def test_input_param_and_static_var_may_not_intersect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp,
                "    input_params: [main_tag]\n"
                "    static_vars:\n      main_tag: oxygen\n")
            with self.assertRaisesRegex(RuntimeError, "BOTH an input param and a"):
                catalog_targets.load_action_cfg(root, "provision", {})

    def test_static_var_must_be_literal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp,
                "    input_params: [main_tag]\n"
                "    static_vars:\n      mode:\n        members: [a]\n")
            with self.assertRaisesRegex(RuntimeError, "must be a literal scalar"):
                catalog_targets.load_action_cfg(root, "provision", {})


class CfgKeyProjectionTests(unittest.TestCase):
    """Assertions 1 + 5 at the projection boundary."""

    DOC = {"main_tag": "oxygen", "foundation": {"networking": {"vpc": 1}, "dns": 2},
           "plt_a_tfstate_key": "a", "plt_b_tfstate_key": "b"}

    def test_exact_glob_and_dotted_path(self):
        self.assertEqual(
            execution_references.project_cfg_keys(self.DOC, ["main_tag"], label="t"), {"main_tag": "oxygen"}
        )
        self.assertEqual(
            sorted(execution_references.project_cfg_keys(self.DOC, ["plt_*"], label="t")),
            ["plt_a_tfstate_key", "plt_b_tfstate_key"],
        )
        self.assertEqual(
            execution_references.project_cfg_keys(self.DOC, ["foundation.networking"], label="t"),
            {"foundation": {"networking": {"vpc": 1}}},
        )

    def test_missing_key_is_an_error(self):
        with self.assertRaisesRegex(RuntimeError, "does not resolve"):
            execution_references.project_cfg_keys(self.DOC, ["nope"], label="t")

    def test_glob_matching_nothing_is_a_stale_declaration(self):
        with self.assertRaisesRegex(RuntimeError, "matched no key"):
            execution_references.project_cfg_keys(self.DOC, ["zzz_*"], label="t")


class FanOutExtraParamsTests(unittest.TestCase):
    """A run may pin a constant param over every member of a SHARED
    param set, so one account list serves several domains instead of being
    copied per domain. Additive only."""

    def _root(self, tmp: str, run_extra: str) -> Path:
        root = Path(tmp)
        _write(root, "fan_outs.yaml", (
            "fan_outs:\n"
            "  lz/all:\n"
            "    runs:\n"
            "      - workflow_key: wf/one\n"
            "        fan_out_param_set_key: accounts\n"
            f"{run_extra}"
            "    failure_mode: stop\n"
        ))
        _write(root, "domains.yaml", "domains:\n  org: {}\n  notifications: {}\n")
        _write(root, "param_sets.yaml", (
            "fan_out_param_sets:\n"
            "  accounts:\n"
            "    ctl_plane:\n"
            "      params:\n"
            "        aws.account: ctl_plane\n"
            "    identity:\n"
            "      params:\n"
            "        aws.account: identity\n"
        ))
        return root

    def test_extra_params_are_merged_into_every_member(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, "        extra_params:\n          domain: notifications\n")
            children = catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)["children"]
            self.assertEqual(
                [c["params"] for c in children],
                [
                    {"aws.account": "ctl_plane", "domain": "notifications"},
                    {"aws.account": "identity", "domain": "notifications"},
                ],
            )
            # The member name alone no longer identifies a child: one param set
            # may back several runs of the same workflow.
            self.assertEqual(
                [c["label"] for c in children],
                ["wf/one[notifications:ctl_plane]", "wf/one[notifications:identity]"],
            )

    def test_collision_with_a_member_param_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, "        extra_params:\n          aws.account: org\n")
            with self.assertRaisesRegex(RuntimeError, "define each param\\s+in one place"):
                catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)

    def test_domain_value_is_validated_against_the_registry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, "        extra_params:\n          domain: nope\n")
            with self.assertRaises(RuntimeError):
                catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)

    def test_extra_params_without_a_param_set_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, "fan_outs.yaml", (
                "fan_outs:\n"
                "  lz/all:\n"
                "    runs:\n"
                "      - workflow_key: wf/one\n"
                "        extra_params:\n"
                "          domain: notifications\n"
                "    failure_mode: stop\n"
            ))
            _write(root, "domains.yaml", "domains:\n  notifications: {}\n")
            _write(root, "param_sets.yaml", "fan_out_param_sets: {}\n")
            with self.assertRaisesRegex(RuntimeError, "requires fan_out_param_set_key"):
                catalog_workflow.expand_fan_out(root, "lz/all", LIVE_CTX)


if __name__ == "__main__":
    unittest.main()


class ChildTargetCommandTests(unittest.TestCase):
    """A workflow spawns `ctl.py target` per child. The argv must be
    ACCEPTED BY THE REAL PARSER — a flag that does not exist, or a dropped one,
    would make the child run differently rather than fail."""

    SPEC = {
        "ctl_entrypoint": Path("/x/ctl.py"),
        "ctl_cfg_root": Path("/cfg"),
        "ctl_profile": "local_dev",
        "ctl_state_local_root": "/state",
        "execution_runtime_mode": "local",
        "action": "plan",
        "providers": ["aws"],
        "execution_params": {"landing_zone": "live", "env.type": "dev"},
        "provider_options": {"aws.credential_implementation": "profile"},
        "execution_access_modes": {"aws": "force_bypass"},
        "plt_overlays": ["db_artificial_populator"],
        "force_skip_execution_identity_preflight_check": ["aws"],
        "agreed_defer_ctl_state_backend_sync": False,
        "force_skip_ctl_state_backend_sync": False,
        "force_skip_guardrails": False,
        "force_skip_full_cfg_validation_gate": True,
        # the cadence has no default, so a child cannot inherit one —
        # the parent's choice must travel in the frozen spec.
        "credential_refresh_modes": {"aws": "per_target"},
    }

    def _argv(self):
        return catalog_workflow.build_child_target_command(
            self.SPEC, "env/core/baseline",
            parent_run_dir=Path("/run"), parent_run_id="PARENT",
        )

    def test_every_flag_is_accepted_by_the_target_parser(self):
        import argparse
        parser = argparse.ArgumentParser()
        cli_args.add_common_args(parser, run_type="target")
        args = parser.parse_args(self._argv()[3:])   # drop python, ctl.py, "target"
        self.assertEqual(args.target, "env/core/baseline")
        self.assertEqual(args.action, "plan")
        self.assertEqual(args.parent_workflow_run_id, "PARENT")

    def test_no_credential_is_carried_in_argv(self):
        """

        the lock grant travels by ENVIRONMENT. argv is visible in `ps` and in
        the logged command line, so a credential there could be replayed into a
        concurrent run while the parent is still going."""

        argv = self._argv()
        self.assertIn("--parent-workflow-run-id", argv)      # provenance
        self.assertNotIn("--parent-ctl-state-lock-id", argv)  # never a credential

    def test_settings_are_carried_verbatim(self):
        argv = self._argv()
        for expected in ("landing_zone=live", "env.type=dev",
                         "aws.credential_implementation=profile", "aws=force_bypass",
                         "db_artificial_populator", "--force-skip-full-cfg-validation-gate"):
            self.assertIn(expected, argv, f"child would run without {expected!r}")
        # a false flag must NOT appear
        self.assertNotIn("--force-skip-guardrails", argv)


class RunnerProvidersWiringTests(unittest.TestCase):
    """A run DECLARES its providers. Every runner must pass them into the
    selection — `target.py` did not, so `ctl.py target` failed with 'no providers
    declared' the first time that path was used. A latent defect in a
    route nothing exercised; this pins all of them."""

    ORCHESTRATOR = (
        Path(__file__).resolve().parents[2] / "atlas-ctl-orchestrator" / "runners"
    )

    def test_every_runner_declares_providers(self):
        missing = []
        for name in ("workflow.py", "target.py", "procedure.py"):
            body = (self.ORCHESTRATOR / name).read_text()
            if "providers=args.providers" not in body:
                missing.append(name)
        self.assertEqual(missing, [], f"runners not declaring providers: {missing}")

    def test_run_pipeline_forwards_providers_to_its_own_selection(self):
        """

        the path used when a runner passes no preflight_selection."""

        import inspect
        source = inspect.getsource(commands_pipeline.run_pipeline)
        self.assertIn("providers=providers", source)


class ChildLockGrantTests(unittest.TestCase):
    """A child runs under its parent's ctl-state lock only by
    redeeming a SINGLE-USE grant. The parent run id must NOT authorise anything —
    it is printed in logs, stored in run metadata and visible in `ps`, so treating
    it as a credential lets anyone start a concurrent run against the same
    ctl-state while the parent is still going."""

    def _root(self, tmp):
        root = Path(tmp)
        kernel_yaml_io.write_yaml_file(
            state_run_store.ctl_state_lock_metadata_path(root),
            {"run_id": "PARENT", "run_type": "workflow"},
        )
        return root

    def setUp(self):
        state_run_store._REDEEMED_CHILD_GRANT = None

    def test_grant_is_single_use(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            grant = state_run_store.mint_child_lock_grant(root, child_kind="target", child_key="t/a")
            self.assertTrue(state_run_store.consume_child_lock_grant(
                root, grant, child_kind="target", child_key="t/a"))
            state_run_store._REDEEMED_CHILD_GRANT = None          # a DIFFERENT process
            self.assertFalse(state_run_store.consume_child_lock_grant(
                root, grant, child_kind="target", child_key="t/a"),
                "a spent grant must not be replayable")

    def test_grant_is_bound_to_its_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            grant = state_run_store.mint_child_lock_grant(root, child_kind="target", child_key="t/a")
            self.assertFalse(state_run_store.consume_child_lock_grant(
                root, grant, child_kind="target", child_key="t/OTHER"))
            self.assertFalse(state_run_store.consume_child_lock_grant(
                root, grant, child_kind="workflow", child_key="t/a"))

    def test_parent_run_id_does_not_authorise(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            self.assertFalse(state_run_store.consume_child_lock_grant(
                root, "PARENT", child_kind="target", child_key="t/a"),
                "the public run id must never grant lock bypass")

    def test_redemption_is_idempotent_within_one_run(self):
        """

        the lock decision is asked twice per run; that is not a replay."""

        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            grant = state_run_store.mint_child_lock_grant(root, child_kind="target", child_key="t/a")
            for _ in range(2):
                self.assertTrue(state_run_store.consume_child_lock_grant(
                    root, grant, child_kind="target", child_key="t/a"))
