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


def entry_yaml(value: str, *, indent: str) -> str:
    """guarded_vars entry block: value + matching hash (baseline self-integrity)."""
    e = common.guard_entry(value, label="test")
    return f"{indent}value: '{e['value']}'\n{indent}hash: {e['hash']}\n"


def ctl_declarations_yaml(ref: str, axes: list[str] | None = None) -> str:
    content = f"ctl_guardrail_declarations:\n- ref: {ref}\n"
    if axes:
        content += "  baseline_axes:\n"
        content += "".join(f"  - {axis}\n" for axis in axes)
    return content


def guardrails_root(plt_root: Path) -> Path:
    return plt_root.parent / "guardrails"


def verify_plt_guardrails(plt_root: Path, rendered_dir: Path, scope_params: dict[str, str]) -> None:
    execution_context = common.execution_context_from_scope_params(scope_params)
    common.verify_plt_guardrails(
        plt_root,
        guardrails_root(plt_root),
        rendered_dir,
        execution_context,
        scope_params,
    )


def make_plt_tree(root: Path, *, baseline_value: str | None, policy: bool = True) -> Path:
    """One /env target instance with aws_region rendered as eu-west-2."""
    plt = root / "plt"
    if policy:
        write(
            plt / common.PLT_GUARDRAILS_FILENAME,
            "plt_guardrail_policies:\n"
            "  env:\n"
            "    match_target_path: /env\n"
            "    protected_vars: [aws_region]\n",
        )
    write(
        plt / "env" / "dev" / common.SCOPE_META_FILENAME,
        "type: scope\ntarget_path: /env\nselectors:\n  match:\n"
        "    execution_context.params.env_type: dev\nimports: []\n",
    )
    if baseline_value is not None:
        common.write_plt_guardrail_baseline(
            guardrails_root(plt),
            instance={
                "target_path": "/env",
                "scopes": {
                    "/env/dev": {"execution_context.params.env_type": "dev"},
                },
            },
            protected_values={"aws_region": baseline_value},
        )
    rendered = root / "rendered"
    write(rendered / "env" / "general.yaml", "aws_region: eu-west-2\n")
    return plt


def make_composed_plt_tree(
    root: Path,
    *,
    landing_zone: str = "live",
    baseline_region: str | None = "eu-west-2",
) -> Path:
    """One /env target composed from /env/dev and /account/dev."""
    plt = root / "plt"
    write(
        plt / common.PLT_GUARDRAILS_DIRNAME / "first.yaml",
        "plt_guardrail_policies:\n"
        "  env:\n"
        "    match_target_path: /env\n"
        "    protected_vars: [aws_region]\n",
    )
    write(
        plt / common.PLT_GUARDRAILS_DIRNAME / "second.yaml",
        "plt_guardrail_policies:\n"
        "  account:\n"
        "    match_target_path: /env\n"
        "    protected_vars: [account]\n",
    )
    write(
        plt / common.SCOPE_COMPOSITION_FILENAME,
        "scope_composition:\n"
        "- target_path: /env\n"
        "  scopes: [/env, /account]\n"
        "  instance_dimensions:\n"
        "  - execution_context.params.landing_zone\n",
    )
    write(
        plt / "env" / "dev" / common.SCOPE_META_FILENAME,
        "type: scope\ntarget_path: /env\nselectors:\n  match:\n"
        "    execution_context.params.env_type: dev\nimports: []\n",
    )
    write(
        plt / "account" / "dev" / common.SCOPE_META_FILENAME,
        "type: scope\ntarget_path: /env\nselectors:\n  match:\n"
        "    execution_context.params.account: dev\nimports: []\n",
    )
    if baseline_region is not None:
        common.write_plt_guardrail_baseline(
            guardrails_root(plt),
            instance={
                "target_path": "/env",
                "scopes": {
                    "/account/dev": {"execution_context.params.account": "dev"},
                    "/env/dev": {"execution_context.params.env_type": "dev"},
                },
                "dimensions": {
                    "execution_context.params.landing_zone": landing_zone,
                },
            },
            protected_values={"account": "dev", "aws_region": baseline_region},
        )
    rendered = root / "rendered"
    write(rendered / "env" / "general.yaml", "account: dev\naws_region: eu-west-2\n")
    return plt


class CtlGuardrailTests(unittest.TestCase):
    def _write_baseline(
        self, root: Path, ref: str, value: str, axes: dict[str, str] | None = None
    ) -> Path:
        return common.write_ctl_guardrail_baseline(
            root / "guardrails", ref=ref, axes=axes or {}, value=value
        )

    def test_ctl_declarations_are_discovered_by_top_level_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ref = "execution_context.params.main_tag"
            write(root / "not-a-special-name.yaml", ctl_declarations_yaml(ref))
            self._write_baseline(root, ref, "oxygen")

            common.verify_ctl_guardrails(
                root, root / "guardrails", {ref: "oxygen"}, None
            )

    def test_ctl_guardrails_reject_changed_resolved_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ref = "execution_context.params.main_tag"
            write(root / "guardrails.yaml", ctl_declarations_yaml(ref))
            self._write_baseline(root, ref, "oxygen")

            with self.assertRaisesRegex(RuntimeError, "guarded ctl var .*main_tag.* changed"):
                common.verify_ctl_guardrails(
                    root, root / "guardrails", {ref: "argon"}, None
                )

    def test_ctl_guardrails_reject_ctl_namespace_ref(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root / "guardrails.yaml",
                ctl_declarations_yaml("execution_context.ctl.action"),
            )

            with self.assertRaisesRegex(RuntimeError, "per-run control switches"):
                common.load_ctl_guard_declarations(root)

    def test_ctl_guardrails_verify_resolved_registry_values_by_axes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ref = "ctl_state_backends.org.bucket_name"
            axis = "execution_context.params.landing_zone"
            write(
                root / "ctl_state.yaml",
                "ctl_state_backends:\n  org:\n"
                "    provider: aws\n    backend_type: s3\n"
                "    bucket_name: \x24{execution_context.params.main_tag}-"
                "\x24{execution_context.params.landing_zone}-org-ctl-state\n"
                "    bucket_region: eu-west-2\n",
            )
            write(root / "guardrails.yaml", ctl_declarations_yaml(ref, [axis]))
            self._write_baseline(root, ref, "oxygen-live-org-ctl-state", {axis: "live"})
            self._write_baseline(root, ref, "oxygen-canary-org-ctl-state", {axis: "canary"})

            for landing_zone in ("live", "canary"):
                common.verify_ctl_guardrails(
                    root,
                    root / "guardrails",
                    {
                        "execution_context.params.main_tag": "oxygen",
                        axis: landing_zone,
                    },
                    "org",
                )

            write(
                root / "ctl_state.yaml",
                "ctl_state_backends:\n  org:\n"
                "    provider: aws\n    backend_type: s3\n"
                "    bucket_name: \x24{execution_context.params.main_tag}-evil-org-ctl-state\n"
                "    bucket_region: eu-west-2\n",
            )
            with self.assertRaisesRegex(RuntimeError, "changed"):
                common.verify_ctl_guardrails(
                    root,
                    root / "guardrails",
                    {"execution_context.params.main_tag": "oxygen", axis: "live"},
                    "org",
                )

    def test_ctl_guardrails_reject_baseline_axes_different_from_declaration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ref = "execution_context.params.main_tag"
            write(root / "guardrails.yaml", ctl_declarations_yaml(ref))
            self._write_baseline(
                root, ref, "oxygen", {"execution_context.params.region": "eu-west-2"}
            )

            with self.assertRaisesRegex(RuntimeError, "has axes .* expected"):
                common.verify_ctl_guardrails(
                    root, root / "guardrails", {ref: "oxygen"}, None
                )

    def test_ctl_guardrails_reject_registry_ref_with_bad_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root / "guardrails.yaml",
                ctl_declarations_yaml("ctl_state_backends.env.execution_identity_key"),
            )

            with self.assertRaisesRegex(RuntimeError, "must be ctl_state_backends"):
                common.load_ctl_guard_declarations(root)

    def test_ctl_guardrails_reject_bare_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root / "guardrails.yaml", ctl_declarations_yaml("main_tag"))

            with self.assertRaisesRegex(RuntimeError, "fully-qualified execution-context path"):
                common.load_ctl_guard_declarations(root)


class PltGuardrailTests(unittest.TestCase):
    def test_verifies_protected_var_against_rendered_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")
            verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_changed_rendered_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-central-1")
            with self.assertRaisesRegex(RuntimeError, "expected .*eu-central-1.*got .*eu-west-2"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_coverage_fails_when_policy_has_no_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value=None)
            with self.assertRaisesRegex(RuntimeError, "have no baseline"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_baseline_value_without_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2", policy=False)
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "plt_guardrail_policies:\n  other:\n    match_target_path: /org\n"
                "    protected_vars: [other_var]\n",
            )
            with self.assertRaisesRegex(RuntimeError, "no plt guardrail policy"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_unresolved_rendered_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="placeholder")
            write(root / "rendered" / "env" / "general.yaml", "aws_region: ${run_id}\n")
            with self.assertRaisesRegex(RuntimeError, "not fully resolved after render"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_policy_selectors_gate_the_final_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value=None)
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "plt_guardrail_policies:\n  env:\n    match_target_path: /env\n"
                "    selectors:\n      match:\n        execution_context.params.env_type: prod\n"
                "    protected_vars: [aws_region]\n",
            )
            verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_one_file_many_files_and_renamed_files_are_equivalent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            one = root / "one"
            many = root / "many"
            content = (
                "plt_guardrail_policies:\n"
                "  env:\n    match_target_path: /env\n    protected_vars: [aws_region]\n"
                "  account:\n    match_target_path: /env\n    protected_vars: [account]\n"
            )
            write(one / common.PLT_GUARDRAILS_FILENAME, content)
            write(
                many / common.PLT_GUARDRAILS_DIRNAME / "arbitrary-a.yaml",
                "plt_guardrail_policies:\n  env:\n    match_target_path: /env\n"
                "    protected_vars: [aws_region]\n",
            )
            write(
                many / common.PLT_GUARDRAILS_DIRNAME / "renamed.yaml",
                "plt_guardrail_policies:\n  account:\n    match_target_path: /env\n"
                "    protected_vars: [account]\n",
            )
            def comparable(root_path: Path) -> dict:
                loaded = common.load_plt_guardrail_policies(root_path)
                return {
                    name: {key: value for key, value in policy.items() if key != "origin"}
                    for name, policy in loaded.items()
                }
            self.assertEqual(comparable(one), comparable(many))

    def test_root_file_and_directory_can_contribute_different_policies(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "plt_guardrail_policies:\n  env:\n    match_target_path: /env\n"
                "    protected_vars: [aws_region]\n",
            )
            write(
                plt / common.PLT_GUARDRAILS_DIRNAME / "anything.yaml",
                "plt_guardrail_policies:\n  org:\n    match_target_path: /org\n"
                "    protected_vars: [account]\n",
            )
            self.assertEqual(set(common.load_plt_guardrail_policies(plt)), {"env", "org"})

    def test_duplicate_policy_key_across_files_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            content = (
                "plt_guardrail_policies:\n  env:\n    match_target_path: /env\n"
                "    protected_vars: [aws_region]\n"
            )
            write(plt / "__guardrails__" / "a.yaml", content)
            write(plt / "__guardrails__" / "b.yaml", content)
            with self.assertRaisesRegex(RuntimeError, "duplicate plt guardrail policy"):
                common.load_plt_guardrail_policies(plt)

    def test_duplicate_active_protected_var_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")
            write(
                plt / "__guardrails__" / "duplicate.yaml",
                "plt_guardrail_policies:\n  second:\n    match_target_path: /env\n"
                "    protected_vars: [aws_region]\n",
            )
            with self.assertRaisesRegex(RuntimeError, "declared by both policies"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_composed_target_identity_uses_scopes_and_extra_dimensions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_composed_plt_tree(root)
            verify_plt_guardrails(
                plt,
                root / "rendered",
                {"env_type": "dev", "account": "dev", "landing_zone": "live"},
            )
            baselines = common.load_plt_guardrail_baselines(guardrails_root(plt))
            self.assertEqual(len(baselines), 1)
            instance = next(iter(baselines.values()))["instance"]
            self.assertEqual(set(instance["scopes"]), {"/env/dev", "/account/dev"})
            self.assertEqual(
                instance["dimensions"],
                {"execution_context.params.landing_zone": "live"},
            )

    def test_missing_instance_dimension_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_composed_plt_tree(root)
            with self.assertRaisesRegex(RuntimeError, "instance dimension .* has no value"):
                verify_plt_guardrails(
                    plt,
                    root / "rendered",
                    {"env_type": "dev", "account": "dev"},
                )

    def test_different_dimension_selects_a_different_instance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_composed_plt_tree(root, landing_zone="live")
            with self.assertRaisesRegex(RuntimeError, "have no baseline"):
                verify_plt_guardrails(
                    plt,
                    root / "rendered",
                    {"env_type": "dev", "account": "dev", "landing_zone": "canary"},
                )

    def test_duplicate_instance_is_rejected_even_when_filename_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")
            generated = common.plt_guardrail_baseline_file(guardrails_root(plt), "/env")
            duplicate = guardrails_root(plt) / "renamed-anything.yaml"
            write(duplicate, generated.read_text(encoding="utf-8"))
            with self.assertRaisesRegex(RuntimeError, "duplicate plt guardrail baseline identity"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_generated_baseline_contains_values_without_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")
            generated = common.plt_guardrail_baseline_file(guardrails_root(plt), "/env")
            content = generated.read_text(encoding="utf-8")
            self.assertIn("protected_values:", content)
            self.assertNotIn("hash:", content)
            self.assertNotIn("axes:", content)
            self.assertNotIn("scope_path:", content)

    def test_empty_dimensions_are_rejected_instead_of_being_file_semantics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")
            generated = common.plt_guardrail_baseline_file(guardrails_root(plt), "/env")
            content = generated.read_text(encoding="utf-8")
            generated.write_text(
                content.replace("    scopes:\n", "    dimensions: {}\n    scopes:\n"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "dimensions must be omitted when empty"):
                common.load_plt_guardrail_baselines(guardrails_root(plt))

    def test_legacy_guardrail_collections_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(plt / common.PLT_GUARDRAILS_FILENAME, "declare: []\n")
            with self.assertRaisesRegex(RuntimeError, "unsupported collections.*declare"):
                common.load_plt_guardrail_policies(plt)


if __name__ == "__main__":
    unittest.main()
