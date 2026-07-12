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


def make_plt_tree(root: Path, *, baseline_value: str | None, declared: bool = True) -> Path:
    """One env/dev scope + rendered tree with aws_region: eu-west-2."""
    plt = root / "plt"
    if declared:
        write(
            plt / common.PLT_GUARDRAILS_FILENAME,
            "declare:\n  - var: aws_region\n    match_target_path: /env\n",
        )
    write(
        plt / "env" / "dev" / common.SCOPE_META_FILENAME,
        "type: scope\ntarget_path: /env\nselectors:\n  match:\n    execution_context.params.env_type: dev\nimports: []\n",
    )
    if baseline_value is not None:
        common.write_plt_guardrail_baseline(
            guardrails_root(plt),
            scope_path="/env/dev",
            axes={},
            guarded_vars={"aws_region": common.guard_entry(baseline_value, label="test")},
        )
    rendered = root / "rendered"
    write(rendered / "env" / "general.yaml", "aws_region: eu-west-2\n")
    return plt


def make_axes_plt_tree(root: Path, *, bucket: str, baseline_for: dict[str, str]) -> Path:
    """env/dev scope whose declare file carries baseline_axes: landing_zone.

    baseline_for: {lz_value: baselined_bucket_name} — one axis baseline file each.
    """
    plt = root / "plt"
    write(
        plt / common.PLT_GUARDRAILS_FILENAME,
        "baseline_axes:\n  - execution_context.params.landing_zone\n"
        "declare:\n  - var: tfstate_s3_bucket_name\n    match_target_path: /env\n",
    )
    write(
        plt / "env" / "dev" / common.SCOPE_META_FILENAME,
        "type: scope\ntarget_path: /env\nselectors:\n  match:\n    execution_context.params.env_type: dev\nimports: []\n",
    )
    for lz, name in baseline_for.items():
        common.write_plt_guardrail_baseline(
            guardrails_root(plt),
            scope_path="/env/dev",
            axes={"execution_context.params.landing_zone": lz},
            guarded_vars={"tfstate_s3_bucket_name": common.guard_entry(name, label="test")},
        )
    rendered = root / "rendered"
    write(rendered / "env" / "tfstate.yaml", f"tfstate_s3_bucket_name: {bucket}\n")
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
    def test_verifies_declared_var_against_rendered_scope_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")

            verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_changed_rendered_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-central-1")

            with self.assertRaisesRegex(RuntimeError, "expected 'eu-central-1'.*got 'eu-west-2'"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_coverage_fails_when_declared_var_has_no_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value=None)

            with self.assertRaisesRegex(RuntimeError, "have no baseline"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_baseline_hash_without_declaration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2", declared=False)
            write(plt / common.PLT_GUARDRAILS_FILENAME, "declare:\n  - var: other_var\n    match_target_path: /org\n")

            with self.assertRaisesRegex(RuntimeError, "no matching declaration"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_unresolved_rendered_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="placeholder")
            write(root / "rendered" / "env" / "general.yaml", "aws_region: ${run_id}\n")

            with self.assertRaisesRegex(RuntimeError, "not fully resolved after render"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_declaration_selectors_narrow_within_scope_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value=None)
            # prod-only declaration: must not require a baseline in the dev scope
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "declare:\n"
                "  - var: aws_region\n"
                "    match_target_path: /env\n"
                "    selectors:\n"
                "      match:\n"
                "        execution_context.params.env_type: prod\n",
            )

            verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_declarations_load_from_guardrails_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(plt / "__guardrails__" / "env.yaml", "declare:\n  - var: aws_region\n    match_target_path: /env\n")
            write(plt / "__guardrails__" / "org.yaml", "declare:\n  - var: aws_region\n    match_target_path: /org\n")

            declarations = common.load_plt_guard_declarations(plt)
            self.assertEqual(
                sorted(d["match_target_path"] for d in declarations),
                ["/env", "/org"],
            )

    def test_declarations_directory_rejects_cross_file_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(plt / "__guardrails__" / "a.yaml", "declare:\n  - var: aws_region\n    match_target_path: /env\n")
            write(plt / "__guardrails__" / "b.yaml", "declare:\n  - var: aws_region\n    match_target_path: /env\n")

            with self.assertRaisesRegex(RuntimeError, "duplicate declaration"):
                common.load_plt_guard_declarations(plt)

    def test_declarations_reject_legacy_path_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "declare:\n  - path: aws_region\n    match_target_path: /env\n",
            )

            with self.assertRaisesRegex(RuntimeError, "unsupported keys.*path"):
                common.load_plt_guard_declarations(plt)

    def test_declarations_reject_file_and_directory_together(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(plt / common.PLT_GUARDRAILS_FILENAME, "declare: []\n")
            write(plt / "__guardrails__" / "env.yaml", "declare: []\n")

            with self.assertRaisesRegex(RuntimeError, "keep exactly one"):
                common.load_plt_guard_declarations(plt)

    def test_declaration_requires_match_target_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(plt / common.PLT_GUARDRAILS_FILENAME, "declare:\n  - var: aws_region\n")

            with self.assertRaisesRegex(RuntimeError, "match_target_path"):
                common.load_plt_guard_declarations(plt)

    def test_scope_meta_rejects_import_dir_without_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            # dir exists and even has a non-yaml file, but no yaml cfg payload
            write(plt / "_common" / "all" / "notes.md", "not cfg\n")
            write(
                plt / "env" / "dev" / common.SCOPE_META_FILENAME,
                "type: scope\ntarget_path: /env\nselectors:\n  match:\n    execution_context.params.env_type: dev\n"
                "imports:\n  - /_common/all\n",
            )

            with self.assertRaisesRegex(RuntimeError, "at least one yaml cfg file"):
                common.discover_active_cfg_scopes(plt, scope_params={"env_type": "dev"})

    def test_scope_meta_rejects_legacy_selectors_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(
                plt / "env" / "dev" / common.SCOPE_META_FILENAME,
                "type: scope\ntarget_path: /env\nselectors:\n  env_type: dev\nimports: []\n",
            )

            with self.assertRaisesRegex(RuntimeError, "match/in form"):
                common.discover_active_cfg_scopes(plt, scope_params={"env_type": "dev"})


class BaselineAxesTests(unittest.TestCase):
    def test_axis_baseline_selected_by_param_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_axes_plt_tree(
                root, bucket="oxygen-live-dev-tfstate",
                baseline_for={"live": "oxygen-live-dev-tfstate", "canary": "oxygen-canary-dev-tfstate"},
            )
            # live rendered tree verifies against the live baseline file
            verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev", "landing_zone": "live"})
            # same scope dir, canary params -> canary baseline; live-rendered value must FAIL it
            with self.assertRaisesRegex(RuntimeError, "changed"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev", "landing_zone": "canary"})

    def test_missing_axis_param_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_axes_plt_tree(root, bucket="x", baseline_for={"live": "x"})
            with self.assertRaisesRegex(RuntimeError, "has no value in this run"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_legacy_scope_local_baselines_are_not_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_axes_plt_tree(root, bucket="x", baseline_for={"live": "x"})
            write(plt / "env" / "dev" / common.PLT_GUARDRAILS_FILENAME, "guarded_vars: {}\n")
            verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev", "landing_zone": "live"})

    def test_duplicate_explicit_identity_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")
            generated = common.plt_guardrail_baseline_file(guardrails_root(plt), "/env/dev")
            duplicate = guardrails_root(plt) / "duplicate.yaml"
            write(duplicate, generated.read_text(encoding="utf-8"))
            with self.assertRaisesRegex(RuntimeError, "duplicate plt guardrail baseline identity"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_empty_axes_are_omitted_and_rejected_if_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_value="eu-west-2")
            generated = common.plt_guardrail_baseline_file(guardrails_root(plt), "/env/dev")
            content = generated.read_text(encoding="utf-8")
            self.assertNotIn("axes:", content)
            generated.write_text(
                content.replace("  guarded_vars:\n", "  axes: {}\n  guarded_vars:\n"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "axes must be omitted when empty"):
                verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_axis_value_is_content_not_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_axes_plt_tree(root, bucket="x", baseline_for={"live/eu-west-2": "x"})
            verify_plt_guardrails(
                plt,
                root / "rendered",
                {"env_type": "dev", "landing_zone": "live/eu-west-2"},
            )
            output = common.plt_guardrail_baseline_file(guardrails_root(plt), "/env/dev")
            self.assertEqual(output.name, "dev.yaml")
            self.assertNotIn("live", output.name)


if __name__ == "__main__":
    unittest.main()
