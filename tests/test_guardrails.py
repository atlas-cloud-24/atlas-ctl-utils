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


def make_plt_tree(root: Path, *, baseline_hash: str | None, declared: bool = True) -> Path:
    """One env/dev scope + rendered tree with aws_region: eu-west-2."""
    plt = root / "plt"
    if declared:
        write(
            plt / common.PLT_GUARDRAILS_FILENAME,
            "declare:\n  - path: aws_region\n    match_target_path: /env\n",
        )
    write(
        plt / "env" / "dev" / common.SCOPE_META_FILENAME,
        "type: scope\ntarget_path: /env\nscope_identity:\n  env_type: dev\nimports: []\n",
    )
    if baseline_hash is not None:
        write(
            plt / "env" / "dev" / common.PLT_GUARDRAILS_FILENAME,
            f"hashes:\n  aws_region: {baseline_hash}\n",
        )
    rendered = root / "rendered"
    write(rendered / "env" / "general.yaml", "aws_region: eu-west-2\n")
    return plt


class CtlGuardrailTests(unittest.TestCase):
    def test_ctl_guardrails_are_discovered_by_top_level_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("oxygen", label="test")
            write(
                root / "not-a-special-name.yaml",
                f"guardrails:\n  guarded_vars:\n    execution_context.params.main_tag: {expected}\n",
            )

            common.verify_ctl_guardrails(root, {"execution_context.params.main_tag": "oxygen"})

    def test_ctl_guardrails_reject_changed_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("oxygen", label="test")
            write(
                root / "guardrails.yaml",
                f"guardrails:\n  guarded_vars:\n    execution_context.params.main_tag: {expected}\n",
            )

            with self.assertRaisesRegex(RuntimeError, "guarded ctl var .*main_tag.* changed"):
                common.verify_ctl_guardrails(root, {"execution_context.params.main_tag": "argon"})

    def test_ctl_guardrails_reject_ctl_namespace_ref(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("provision", label="test")
            write(
                root / "guardrails.yaml",
                f"guardrails:\n  guarded_vars:\n    execution_context.ctl.action: {expected}\n",
            )

            with self.assertRaisesRegex(RuntimeError, "per-run switches and can never be guarded"):
                common.verify_ctl_guardrails(root, {"execution_context.ctl.action": "provision"})

    def test_ctl_guardrails_reject_bare_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("oxygen", label="test")
            write(root / "guardrails.yaml", f"guardrails:\n  guarded_vars:\n    main_tag: {expected}\n")

            with self.assertRaisesRegex(RuntimeError, "fully-qualified execution-context path"):
                common.verify_ctl_guardrails(root, {"execution_context.params.main_tag": "oxygen"})


class PltGuardrailTests(unittest.TestCase):
    def test_verifies_declared_var_against_rendered_scope_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("eu-west-2", label="test")
            plt = make_plt_tree(root, baseline_hash=expected)

            common.verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_changed_rendered_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            wrong = common.guard_value_hash("eu-central-1", label="test")
            plt = make_plt_tree(root, baseline_hash=wrong)

            with self.assertRaisesRegex(RuntimeError, "guarded plt var .*aws_region.* changed"):
                common.verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_coverage_fails_when_declared_var_has_no_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_hash=None)

            with self.assertRaisesRegex(RuntimeError, "have no baseline"):
                common.verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_baseline_hash_without_declaration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stray = common.guard_value_hash("eu-west-2", label="test")
            plt = make_plt_tree(root, baseline_hash=stray, declared=False)
            write(plt / common.PLT_GUARDRAILS_FILENAME, "declare:\n  - path: other_var\n    match_target_path: /org\n")

            with self.assertRaisesRegex(RuntimeError, "no matching declaration"):
                common.verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_rejects_unresolved_rendered_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = common.guard_value_hash("placeholder", label="test")
            plt = make_plt_tree(root, baseline_hash=expected)
            write(root / "rendered" / "env" / "general.yaml", "aws_region: ${run_id}\n")

            with self.assertRaisesRegex(RuntimeError, "not fully resolved after render"):
                common.verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_declaration_selectors_narrow_within_scope_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = make_plt_tree(root, baseline_hash=None)
            # prod-only declaration: must not require a baseline in the dev scope
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "declare:\n"
                "  - path: aws_region\n"
                "    match_target_path: /env\n"
                "    selectors:\n"
                "      env_type: prod\n",
            )

            common.verify_plt_guardrails(plt, root / "rendered", {"env_type": "dev"})

    def test_declarations_load_from_guardrails_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(plt / "__guardrails__" / "env.yaml", "declare:\n  - path: aws_region\n    match_target_path: /env\n")
            write(plt / "__guardrails__" / "org.yaml", "declare:\n  - path: aws_region\n    match_target_path: /org\n")

            declarations = common.load_plt_guard_declarations(plt)
            self.assertEqual(
                sorted(d["match_target_path"] for d in declarations),
                ["/env", "/org"],
            )

    def test_declarations_directory_rejects_cross_file_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(plt / "__guardrails__" / "a.yaml", "declare:\n  - path: aws_region\n    match_target_path: /env\n")
            write(plt / "__guardrails__" / "b.yaml", "declare:\n  - path: aws_region\n    match_target_path: /env\n")

            with self.assertRaisesRegex(RuntimeError, "duplicate declaration"):
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
            write(plt / common.PLT_GUARDRAILS_FILENAME, "declare:\n  - path: aws_region\n")

            with self.assertRaisesRegex(RuntimeError, "match_target_path"):
                common.load_plt_guard_declarations(plt)

    def test_scope_meta_rejects_import_dir_without_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            # dir exists and even has a non-yaml file, but no yaml cfg payload
            write(plt / "_common" / "all" / "notes.md", "not cfg\n")
            write(
                plt / "env" / "dev" / common.SCOPE_META_FILENAME,
                "type: scope\ntarget_path: /env\nscope_identity:\n  env_type: dev\n"
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

            with self.assertRaisesRegex(RuntimeError, "scope_identity"):
                common.discover_active_cfg_scopes(plt, scope_params={"env_type": "dev"})


if __name__ == "__main__":
    unittest.main()
