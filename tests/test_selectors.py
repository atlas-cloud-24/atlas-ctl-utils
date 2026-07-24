import argparse
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class CtlProfilesTests(unittest.TestCase):
    def test_profiles_load_by_top_level_key_not_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "not-a-special-name.yaml",
                  "ctl_profiles:\n  local_dev:\n    ref_policy: local_dirty_allowed\n")

            self.assertEqual(common.ctl_ref_policy(root, "local_dev"), "local_dirty_allowed")

    def test_unknown_profile_lists_known(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "ctl_profiles.yaml", "ctl_profiles:\n  local_dev:\n    ref_policy: x\n")

            with self.assertRaisesRegex(RuntimeError, "known profiles: local_dev"):
                common.ctl_profile_policy(root, "nope")


class ExecutionContextTests(unittest.TestCase):
    def _ctl_root(self, tmp: str) -> Path:
        root = Path(tmp)
        write(root / "execution_params.yaml",
              "execution_params:\n  main_tag: oxygen\n"
              "  derived: ${execution_context.params.env_type}\n")
        return root

    def test_builds_two_namespaces_flat(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = common.build_execution_context(
                self._ctl_root(tmp), action="plan", ctl_profile="commit_required",
                execution_runtime_mode="local",
                execution_params={"env_type": "dev"})
        self.assertEqual(ctx["execution_context.ctl.action"], "plan")
        self.assertEqual(ctx["execution_context.ctl.profile"], "commit_required")
        self.assertEqual(ctx["execution_context.params.env_type"], "dev")
        self.assertEqual(ctx["execution_context.params.main_tag"], "oxygen")
        self.assertEqual(ctx["execution_context.params.derived"], "dev")

    def test_cfg_param_ref_to_absent_is_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = common.build_execution_context(
                self._ctl_root(tmp), action="plan", ctl_profile="p",
                execution_runtime_mode="local", execution_params={})
        self.assertNotIn("execution_context.params.derived", ctx)

    def test_cli_cfg_param_collision_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "collides with a --execution-params"):
                common.build_execution_context(
                    self._ctl_root(tmp), action="plan", ctl_profile="p",
                    execution_runtime_mode="local",
                    execution_params={"main_tag": "other"})

    def test_nested_view_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctx = common.build_execution_context(
                self._ctl_root(tmp), action="plan", ctl_profile="p",
                execution_runtime_mode="local",
                execution_params={"env_type": "dev"})
        nested = common.execution_context_nested(ctx)
        self.assertEqual(nested["execution_context"]["ctl"]["action"], "plan")
        self.assertEqual(nested["execution_context"]["params"]["main_tag"], "oxygen")


class SelectorMatchTests(unittest.TestCase):
    CTX = {
        "execution_context.ctl.action": "plan",
        "execution_context.ctl.profile": "commit_required",
        "execution_context.params.env_type": "dev",
    }

    def test_fully_qualified_match(self):
        self.assertTrue(common.selector_matches(
            {"execution_context.params.env_type": ["dev", "test"]}, self.CTX, label="t"))

    def test_promoted_keys_are_selectable(self):
        self.assertTrue(common.selector_matches(
            {"execution_context.ctl.profile": ["commit_required"]}, self.CTX, label="t"))

    def test_missing_key_means_no_match(self):
        self.assertFalse(common.selector_matches(
            {"execution_context.params.region": ["eu-west-2"]}, self.CTX, label="t"))

    def test_bare_key_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "fully-qualified execution-context path"):
            common.selector_matches({"env_type": ["dev"]}, self.CTX, label="t")

    def test_constraints_enforce_allowed_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "execution_context_constraints.yaml",
                  "execution_context_constraints:\n"
                  "  - when_all:\n      - execution_context.params.env_type: [prod]\n"
                  "    allowed_values:\n      execution_context.ctl.action: [provision, plan, readonly]\n")
            ctx = dict(self.CTX)
            ctx["execution_context.params.env_type"] = "prod"
            ctx["execution_context.ctl.action"] = "destroy"
            with self.assertRaisesRegex(RuntimeError, "allows execution_context.ctl.action"):
                common.validate_execution_context_constraints(root, ctx)


class ProviderNamespacedParamKeyTests(unittest.TestCase):
    """Provider-specific params are namespaced (`aws.account`); neutral params stay
    single-segment. The context is a flat dotted map, so a dotted key is just a
    longer key and selectors keep matching full paths."""

    def test_neutral_and_dotted_keys_are_valid(self):
        for key in ("env_type", "landing_zone", "aws.account", "azure.subscription", "a.b.c"):
            self.assertTrue(common.CONTEXT_KEY_RE.fullmatch(key), key)

    def test_malformed_dotted_keys_are_rejected(self):
        for key in ("aws.", ".aws", "aws..account", "aws.1account", "aws-account", ""):
            self.assertFalse(common.CONTEXT_KEY_RE.fullmatch(key), key)

    def test_dotted_param_reaches_the_context_under_its_full_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "execution_params.yaml", "execution_params:\n  main_tag: oxygen\n")
            ctx = common.build_execution_context(
                root,
                action="provision",
                ctl_profile="local_dev",
                execution_params={"aws.account": "dev", "env_type": "dev"},
                execution_access_modes={"aws": "standard"},
                execution_runtime_mode="local",
            )
            self.assertEqual(ctx["execution_context.params.aws.account"], "dev")
            self.assertEqual(ctx["execution_context.params.env_type"], "dev")

    def test_dotted_param_is_selectable(self):
        ctx = {"execution_context.params.aws.account": "dev"}
        self.assertTrue(
            common.selector_matches(
                {"execution_context.params.aws.account": ["dev"]}, ctx, label="t"
            )
        )
        self.assertFalse(
            common.selector_matches(
                {"execution_context.params.aws.account": ["prod"]}, ctx, label="t"
            )
        )


class ContainsMatcherTests(unittest.TestCase):
    """`contains` asks the inverse of match/in: is this value AMONG a list-valued
    fact? Generic — nothing provider-specific; providers is just its first user."""

    CTX = {
        "execution_context.ctl.providers": ["aws", "azure"],
        "execution_context.params.env_type": "dev",
    }

    def test_member_matches(self):
        for provider in ("aws", "azure"):
            self.assertTrue(common.selector_matches(
                {"execution_context.ctl.providers": {"contains": provider}},
                self.CTX, label="t"))

    def test_non_member_does_not_match(self):
        self.assertFalse(common.selector_matches(
            {"execution_context.ctl.providers": {"contains": "gcp"}},
            self.CTX, label="t"))

    def test_contains_and_scalar_predicates_combine(self):
        self.assertTrue(common.selector_matches(
            {"execution_context.ctl.providers": {"contains": "aws"},
             "execution_context.params.env_type": ["dev"]},
            self.CTX, label="t"))
        self.assertFalse(common.selector_matches(
            {"execution_context.ctl.providers": {"contains": "aws"},
             "execution_context.params.env_type": ["prod"]},
            self.CTX, label="t"))

    def test_structured_contains_form(self):
        self.assertTrue(common.selector_matches(
            {"contains": {"execution_context.ctl.providers": ["aws"]}},
            self.CTX, label="t"))

    def test_missing_fact_does_not_match(self):
        self.assertFalse(common.selector_matches(
            {"execution_context.ctl.providers": {"contains": "aws"}}, {}, label="t"))

    def test_scalar_fact_is_treated_as_a_single_member(self):
        self.assertTrue(common.selector_matches(
            {"execution_context.params.env_type": {"contains": "dev"}},
            self.CTX, label="t"))


class ProviderCoverageTests(unittest.TestCase):
    """Every selected target's provider must be among the run's declared providers."""

    @staticmethod
    def _run(provider):
        return {"t": {"execution_identity": {"provider": provider, "account": "a",
                                    "roles": {"readwrite": "r"}}}}

    def test_declared_provider_passes(self):
        common.validate_target_provider_coverage(self._run("aws"), ["aws", "azure"])

    def test_undeclared_provider_fails_loud(self):
        with self.assertRaisesRegex(RuntimeError, "not declared in --providers"):
            common.validate_target_provider_coverage(self._run("azure"), ["aws"])

    def test_execution_less_run_is_ignored(self):
        common.validate_target_provider_coverage({"t": {}}, ["aws"])


class ProviderOptionsTests(unittest.TestCase):
    """ONE generic engine arg; the engine routes, adapters own the vocabulary."""

    def test_parses_and_merges_namespaced_keys(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--provider-options", dest="provider_options",
                            action=common.ProviderOptionsAction, default={})
        args = parser.parse_args([
            "--provider-options", "aws.credential_implementation=profile,aws.x=1",
            "--provider-options", "azure.y=2",
        ])
        self.assertEqual(args.provider_options, {
            "aws.credential_implementation": "profile", "aws.x": "1", "azure.y": "2"})

    def test_key_must_be_provider_namespaced(self):
        with self.assertRaisesRegex(Exception, "provider-namespaced"):
            common.parse_provider_options("credential_implementation=profile")

    def test_malformed_pair_rejected(self):
        with self.assertRaisesRegex(Exception, "<provider>.<key>=<value>"):
            common.parse_provider_options("aws.bogus")

    def test_subset_for_one_provider_strips_the_prefix(self):
        options = {"aws.credential_implementation": "profile", "azure.k": "v"}
        self.assertEqual(common.provider_options_for(options, "aws"),
                         {"credential_implementation": "profile"})
        self.assertEqual(common.provider_options_for(options, "gcp"), {})

    def test_options_must_address_a_declared_provider(self):
        with self.assertRaisesRegex(RuntimeError, "not declared in --providers"):
            common.validate_provider_options_addressing({"azure.k": "v"}, ["aws"])
        common.validate_provider_options_addressing({"aws.k": "v"}, ["aws"])

    def test_credential_implementation_is_independent_of_runtime_mode(self):
        # WHERE you run and HOW you authenticate are separate axes.
        self.assertEqual(
            common.resolve_provider_implementation_key(
                {"aws.credential_implementation": "web_identity"}, "aws"),
            "web_identity")
        # REQUIRED — the engine has no implementation to default to
        with self.assertRaisesRegex(RuntimeError, "no credential implementation declared"):
            common.resolve_provider_implementation_key({}, "aws")


class ConstraintGateTests(unittest.TestCase):
    """`when_all` (AND) / `when_any` (OR) replace the removed AND-only `when`."""

    BASE = {
        "execution_context.params.env_type": "dev",
        "execution_context.params.account": "dev",
        "execution_context.ctl.action": "destroy",
    }

    @staticmethod
    def _root(tmp: str, body: str) -> Path:
        root = Path(tmp)
        write(root / "execution_context_constraints.yaml",
              "execution_context_constraints:\n" + body)
        return root

    def test_when_any_ors_two_paths_in_one_rule(self):
        # Previously this needed TWO duplicated rules, because `when` was AND-only.
        body = ("  - when_any:\n"
                "      - execution_context.params.account: [prod]\n"
                "      - execution_context.params.env_type: [prod]\n"
                "    allowed_values:\n"
                "      execution_context.ctl.action: [provision, plan, readonly]\n")
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, body)
            # neither axis is prod -> gate does not match -> destroy allowed
            common.validate_execution_context_constraints(root, dict(self.BASE))
            # only account=prod -> gate matches via the FIRST entry
            ctx = dict(self.BASE, **{"execution_context.params.account": "prod"})
            with self.assertRaisesRegex(RuntimeError, "allows execution_context.ctl.action"):
                common.validate_execution_context_constraints(root, ctx)
            # only env_type=prod -> gate matches via the SECOND entry
            ctx = dict(self.BASE, **{"execution_context.params.env_type": "prod"})
            with self.assertRaisesRegex(RuntimeError, "allows execution_context.ctl.action"):
                common.validate_execution_context_constraints(root, ctx)

    def test_when_all_requires_every_entry(self):
        body = ("  - when_all:\n"
                "      - execution_context.params.account: [prod]\n"
                "      - execution_context.params.env_type: [prod]\n"
                "    allowed_values:\n"
                "      execution_context.ctl.action: [provision]\n")
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, body)
            # only one axis prod -> gate does NOT match
            ctx = dict(self.BASE, **{"execution_context.params.account": "prod"})
            common.validate_execution_context_constraints(root, ctx)
            # both prod -> gate matches
            ctx = dict(self.BASE, **{
                "execution_context.params.account": "prod",
                "execution_context.params.env_type": "prod",
            })
            with self.assertRaisesRegex(RuntimeError, "allows execution_context.ctl.action"):
                common.validate_execution_context_constraints(root, ctx)

    def test_when_all_and_when_any_are_anded(self):
        body = ("  - when_all:\n"
                "      - execution_context.params.env_type: [prod]\n"
                "    when_any:\n"
                "      - execution_context.params.account: [prod]\n"
                "      - execution_context.params.account: [prodlike]\n"
                "    allowed_values:\n"
                "      execution_context.ctl.action: [provision]\n")
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, body)
            # when_any satisfied but when_all not -> no match
            ctx = dict(self.BASE, **{"execution_context.params.account": "prod"})
            common.validate_execution_context_constraints(root, ctx)
            # both satisfied -> match
            ctx = dict(self.BASE, **{
                "execution_context.params.env_type": "prod",
                "execution_context.params.account": "prodlike",
            })
            with self.assertRaisesRegex(RuntimeError, "allows execution_context.ctl.action"):
                common.validate_execution_context_constraints(root, ctx)

    def test_no_gate_means_always_applies(self):
        body = ("  - require_present:\n"
                "      - execution_context.params.landing_zone\n")
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, body)
            with self.assertRaisesRegex(RuntimeError, "requires 'execution_context.params.landing_zone'"):
                common.validate_execution_context_constraints(root, dict(self.BASE))

    def test_removed_when_key_is_a_migration_error(self):
        body = ("  - when:\n"
                "      execution_context.params.env_type: [prod]\n"
                "    allowed_values:\n"
                "      execution_context.ctl.action: [provision]\n")
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, body)
            with self.assertRaisesRegex(RuntimeError, r"uses `when`, which is removed"):
                common.validate_execution_context_constraints(root, dict(self.BASE))

    def test_unknown_field_is_rejected(self):
        body = ("  - when_all:\n"
                "      - execution_context.params.env_type: [prod]\n"
                "    when_provider: aws\n")
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, body)
            with self.assertRaisesRegex(RuntimeError, "unknown fields"):
                common.validate_execution_context_constraints(root, dict(self.BASE))

    def test_gate_must_be_a_non_empty_list_of_mappings(self):
        for body in (
            "  - when_all: []\n    require_present: []\n",
            "  - when_any:\n      - execution_context.params.env_type: [prod]\n"
            "    when_all: not-a-list\n",
        ):
            with tempfile.TemporaryDirectory() as tmp:
                root = self._root(tmp, body)
                with self.assertRaisesRegex(RuntimeError, "must be a non-empty list"):
                    common.validate_execution_context_constraints(root, dict(self.BASE))


class ExecutionParamsArgTests(unittest.TestCase):
    """--execution-params accepts comma-separated pairs AND repetition, and the
    dest stays a FLAT list[tuple] so selectors_to_map/normalizers are unaffected."""

    @staticmethod
    def _parser():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--execution-params",
            dest="execution_param",
            action=common.ExecutionParamsAction,
            default=[],
        )
        return parser

    def test_comma_separated_pairs_in_one_flag(self):
        args = self._parser().parse_args(
            ["--execution-params", "landing_zone=live,env_type=dev"]
        )
        self.assertEqual(
            args.execution_param, [("landing_zone", "live"), ("env_type", "dev")]
        )

    def test_repeated_flag_still_works_and_extends(self):
        args = self._parser().parse_args(
            ["--execution-params", "landing_zone=live", "--execution-params", "env_type=dev"]
        )
        self.assertEqual(
            args.execution_param, [("landing_zone", "live"), ("env_type", "dev")]
        )

    def test_comma_and_repetition_mix(self):
        args = self._parser().parse_args(
            ["--execution-params", "a=1,b=2", "--execution-params", "c=3"]
        )
        self.assertEqual(args.execution_param, [("a", "1"), ("b", "2"), ("c", "3")])

    def test_values_are_whitespace_trimmed(self):
        args = self._parser().parse_args(["--execution-params", " a = 1 , b = 2 "])
        self.assertEqual(args.execution_param, [("a", "1"), ("b", "2")])

    def test_result_feeds_selectors_to_map_unchanged(self):
        args = self._parser().parse_args(["--execution-params", "a=1,b=2"])
        self.assertEqual(
            common.selectors_to_map(args.execution_param, label="execution param"),
            {"a": "1", "b": "2"},
        )

    def test_duplicate_key_still_rejected_downstream(self):
        args = self._parser().parse_args(["--execution-params", "a=1,a=2"])
        with self.assertRaisesRegex(RuntimeError, "duplicate execution param selector"):
            common.selectors_to_map(args.execution_param, label="execution param")

    def test_malformed_pair_is_a_parser_error(self):
        with self.assertRaises(SystemExit):
            self._parser().parse_args(["--execution-params", "a=1,bogus"])

    def test_empty_value_is_a_parser_error(self):
        with self.assertRaises(SystemExit):
            self._parser().parse_args(["--execution-params", "a="])

    def test_default_list_is_not_mutated_across_parsers(self):
        first = self._parser().parse_args(["--execution-params", "a=1"])
        second = self._parser().parse_args([])
        self.assertEqual(first.execution_param, [("a", "1")])
        self.assertEqual(second.execution_param, [])


if __name__ == "__main__":
    unittest.main()
