import sys
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common, guardrails  # noqa: E402


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def context(**params) -> dict[str, object]:
    return {
        f"execution_context.params.{key}": value
        for key, value in params.items()
    }


def write_plt_policy(
    plt_root: Path,
    *,
    protected_paths: list[str],
    instance_params: list[str] | None = None,
    selectors: dict | None = None,
) -> None:
    policy = {
        "subject": {
            "kind": "plt_rendered_target",
            "target_path": "/env",
        },
        "protected_paths": protected_paths,
    }
    if instance_params:
        policy["instance_params"] = instance_params
    if selectors:
        policy["selectors"] = selectors
    write(
        plt_root / common.PLT_GUARDRAILS_DIRNAME / "env.yaml",
        yaml.safe_dump(
            {"guardrail_policies": {"env": policy}},
            sort_keys=False,
        ),
    )


def write_env_scope(plt_root: Path) -> None:
    write(
        plt_root / "env" / "dev" / common.SCOPE_META_FILENAME,
        "type: scope\n"
        "target_path: /env\n"
        "selectors:\n"
        "  match:\n"
        "    execution_context.params.env_type: dev\n"
        "imports: []\n",
    )


class JsonPointerTest(unittest.TestCase):
    def test_nested_list_and_escaped_keys(self):
        document = {
            "roles": [{"policy/name": {"enabled": True}}],
        }
        self.assertIs(
            guardrails.json_pointer_get(
                document,
                "/roles/0/policy~1name/enabled",
                label="test",
            ),
            True,
        )

    def test_explicit_null_differs_from_missing(self):
        self.assertIsNone(
            guardrails.json_pointer_get({"value": None}, "/value", label="test")
        )
        with self.assertRaisesRegex(RuntimeError, "does not exist"):
            guardrails.json_pointer_get({}, "/value", label="test")

    def test_invalid_escape_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "invalid JSON Pointer escape"):
            guardrails.json_pointer_tokens("/bad~2path", label="test")

    def test_overlapping_paths_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write_plt_policy(
                plt,
                protected_paths=["/roles", "/roles/admin"],
            )
            with self.assertRaisesRegex(RuntimeError, "overlapping protected paths"):
                guardrails.load_guardrail_policies(plt, owner="plt")


class PolicyTest(unittest.TestCase):
    def test_policy_files_are_organization_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write_plt_policy(plt, protected_paths=["/region"])
            write(
                plt / common.PLT_GUARDRAILS_DIRNAME / "arbitrary-name.yaml",
                "guardrail_policies:\n"
                "  identity:\n"
                "    subject:\n"
                "      kind: plt_rendered_target\n"
                "      target_path: /identity\n"
                "    protected_paths: [/account_id]\n",
            )
            policies = guardrails.load_guardrail_policies(plt, owner="plt")
            self.assertEqual(set(policies), {"env", "identity"})

    def test_subject_kind_is_owner_checked(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "guardrail_policies:\n"
                "  wrong:\n"
                "    subject:\n"
                "      kind: ctl_cfg\n"
                "    protected_paths: [/value]\n",
            )
            with self.assertRaisesRegex(RuntimeError, "invalid for plt policies"):
                guardrails.load_guardrail_policies(plt, owner="plt")

    def test_legacy_collections_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write(
                plt / common.PLT_GUARDRAILS_FILENAME,
                "plt_guardrail_policies: {}\n",
            )
            with self.assertRaisesRegex(RuntimeError, "legacy guardrail collections"):
                guardrails.load_guardrail_policies(plt, owner="plt")

    def test_selector_refs_must_be_instance_params(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write_plt_policy(
                plt,
                protected_paths=["/region"],
                selectors={
                    "match": {
                        "execution_context.params.env_type": "dev",
                    }
                },
            )
            with self.assertRaisesRegex(RuntimeError, "must include every selector ref"):
                guardrails.load_guardrail_policies(plt, owner="plt")

    def test_conditional_selector_activates_only_its_subset(self):
        with tempfile.TemporaryDirectory() as tmp:
            plt = Path(tmp)
            write_plt_policy(
                plt,
                protected_paths=["/region"],
                instance_params=["execution_context.params.env_type"],
                selectors={
                    "match": {
                        "execution_context.params.env_type": "dev",
                    }
                },
            )
            policies = guardrails.load_guardrail_policies(plt, owner="plt")
            self.assertEqual(
                len(
                    guardrails.active_guardrail_policies(
                        policies,
                        context(env_type="dev"),
                    )
                ),
                1,
            )
            self.assertEqual(
                guardrails.active_guardrail_policies(
                    policies,
                    context(env_type="prod"),
                ),
                [],
            )

    def test_scope_selector_identity_must_be_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plt = root / "plt"
            rendered = root / "rendered"
            write_plt_policy(plt, protected_paths=["/region"])
            write_env_scope(plt)
            write(rendered / "env" / "value.yaml", "region: eu-west-2\n")
            with self.assertRaisesRegex(RuntimeError, "active scope selector ref"):
                guardrails.materialize_plt_guardrails(
                    root / "ctl",
                    plt,
                    rendered,
                    context(env_type="dev"),
                    {"env_type": "dev"},
                )


class BaselineTest(unittest.TestCase):
    def test_native_nested_values_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subject = {
                "kind": "plt_rendered_target",
                "target_path": "/env",
                "instance": {
                    "params": {
                        "execution_context.params.env_type": "dev",
                    }
                },
            }
            value = {
                "enabled": True,
                "count": 2,
                "items": ["a", None],
                "empty_list": [],
                "empty_mapping": {},
                "empty_string": "",
            }
            guardrails.write_guardrail_baseline(
                root,
                subject=subject,
                values={"/settings": value},
            )
            loaded = guardrails.load_guardrail_baselines(root)
            self.assertEqual(
                loaded[guardrails.subject_identity(subject)]["values"]["/settings"],
                value,
            )

    def test_baseline_identity_is_type_sensitive(self):
        string_subject = {
            "kind": "ctl_cfg",
            "instance": {
                "params": {
                    "execution_context.params.value": "1",
                }
            },
        }
        int_subject = {
            "kind": "ctl_cfg",
            "instance": {
                "params": {
                    "execution_context.params.value": 1,
                }
            },
        }
        self.assertNotEqual(
            guardrails.subject_identity(string_subject),
            guardrails.subject_identity(int_subject),
        )

    def test_empty_instance_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "non-empty"):
            guardrails.subject_identity(
                {
                    "kind": "ctl_cfg",
                    "instance": {"params": {}},
                }
            )


class VerificationTest(unittest.TestCase):
    def test_ctl_cfg_and_execution_context_subjects(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            baselines = root / "baselines"
            write(
                ctl / "guardrails.yaml",
                "guardrail_policies:\n"
                "  cfg:\n"
                "    subject:\n"
                "      kind: ctl_cfg\n"
                "    protected_paths: [/settings/enabled]\n"
                "  context:\n"
                "    subject:\n"
                "      kind: execution_context\n"
                "    protected_paths: [/params/main_tag]\n",
            )
            write(
                ctl / "settings.yaml",
                "settings:\n"
                "  enabled: true\n",
            )
            guardrails.write_guardrail_baseline(
                baselines,
                subject={"kind": "ctl_cfg"},
                values={"/settings/enabled": True},
            )
            guardrails.write_guardrail_baseline(
                baselines,
                subject={"kind": "execution_context"},
                values={"/params/main_tag": "oxygen"},
            )
            guardrails.verify_ctl_guardrails(
                ctl,
                baselines,
                context(main_tag="oxygen"),
            )

    def test_false_differs_from_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            baselines = root / "baselines"
            write(
                ctl / "guardrails.yaml",
                "guardrail_policies:\n"
                "  cfg:\n"
                "    subject:\n"
                "      kind: ctl_cfg\n"
                "    protected_paths: [/settings/enabled]\n",
            )
            write(ctl / "settings.yaml", "settings:\n  enabled: 0\n")
            guardrails.write_guardrail_baseline(
                baselines,
                subject={"kind": "ctl_cfg"},
                values={"/settings/enabled": False},
            )
            with self.assertRaisesRegex(RuntimeError, "guardrail mismatch"):
                guardrails.verify_ctl_guardrails(ctl, baselines, {})

    def test_plt_nested_path_verification(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            plt = root / "plt"
            rendered = root / "rendered"
            baselines = root / "baselines"
            write_plt_policy(
                plt,
                protected_paths=["/settings/regions/0"],
                instance_params=["execution_context.params.env_type"],
            )
            write_env_scope(plt)
            write(
                rendered / "env" / "settings.yaml",
                "settings:\n"
                "  regions:\n"
                "    - eu-west-2\n",
            )
            subject = {
                "kind": "plt_rendered_target",
                "target_path": "/env",
                "instance": {
                    "params": {
                        "execution_context.params.env_type": "dev",
                    }
                },
            }
            guardrails.write_guardrail_baseline(
                baselines,
                subject=subject,
                values={"/settings/regions/0": "eu-west-2"},
            )
            guardrails.verify_plt_guardrails(
                ctl,
                plt,
                baselines,
                rendered,
                context(env_type="dev"),
                {"env_type": "dev"},
            )

    def test_universal_policy_covers_new_scope_and_fails_without_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            plt = root / "plt"
            rendered = root / "rendered"
            write_plt_policy(
                plt,
                protected_paths=["/region"],
                instance_params=["execution_context.params.env_type"],
            )
            write(
                plt / "env" / "test" / common.SCOPE_META_FILENAME,
                "type: scope\n"
                "target_path: /env\n"
                "selectors:\n"
                "  match:\n"
                "    execution_context.params.env_type: test\n"
                "imports: []\n",
            )
            write(rendered / "env" / "value.yaml", "region: eu-west-2\n")
            with self.assertRaisesRegex(RuntimeError, "has no baseline"):
                guardrails.verify_plt_guardrails(
                    ctl,
                    plt,
                    root / "baselines",
                    rendered,
                    context(env_type="test"),
                    {"env_type": "test"},
                )

    def test_missing_baseline_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            write(
                ctl / "guardrails.yaml",
                "guardrail_policies:\n"
                "  cfg:\n"
                "    subject:\n"
                "      kind: ctl_cfg\n"
                "    protected_paths: [/settings/value]\n",
            )
            write(ctl / "settings.yaml", "settings:\n  value: stable\n")
            with self.assertRaisesRegex(RuntimeError, "has no baseline"):
                guardrails.verify_ctl_guardrails(
                    ctl,
                    root / "baselines",
                    {},
                )

    def test_baseline_path_without_policy_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctl = root / "ctl"
            baselines = root / "baselines"
            write(
                ctl / "guardrails.yaml",
                "guardrail_policies:\n"
                "  cfg:\n"
                "    subject:\n"
                "      kind: ctl_cfg\n"
                "    protected_paths: [/settings/value]\n",
            )
            write(ctl / "settings.yaml", "settings:\n  value: stable\n")
            guardrails.write_guardrail_baseline(
                baselines,
                subject={"kind": "ctl_cfg"},
                values={
                    "/settings/value": "stable",
                    "/settings/unowned": "bad",
                },
            )
            with self.assertRaisesRegex(RuntimeError, "have no authored policy"):
                guardrails.verify_ctl_guardrails(ctl, baselines, {})


if __name__ == "__main__":
    unittest.main()
