import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


LIVE_CTX = {"execution_context.params.landing_zone": "live"}
CANARY_CTX = {"execution_context.params.landing_zone": "canary"}


def _write(root: Path, name: str, body: str) -> None:
    (root / name).write_text(body, encoding="utf-8")


class FanOutMemberSchemaTests(unittest.TestCase):
    """§Phase 31 3b: param-set members are {params, selectors?}; selector-gated
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
            "    max_parallel: 1\n"
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
            live = common.expand_fan_out(root, "lz/all", LIVE_CTX)
            self.assertEqual(
                [c["fan_out_param_entry_key"] for c in live["children"]], ["org", "dev"]
            )
            canary = common.expand_fan_out(root, "lz/all", CANARY_CTX)
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
                common.expand_fan_out(root, "lz/all", LIVE_CTX)

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
                common.expand_fan_out(root, "lz/all", LIVE_CTX)

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
                common.expand_fan_out(root, "lz/all", LIVE_CTX)

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
                common.expand_fan_out(root, "lz/all", CANARY_CTX)


class SelectorGroupResolverTests(unittest.TestCase):
    """§Phase 31 3c: selector-membered group entries resolve to exactly one
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
        value = common.resolve_selector_group_member(
            self.GROUP, {"execution_context.params.domain": "org"},
            value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
        )
        self.assertEqual(value, "org")

    def test_in_selector_member(self):
        value = common.resolve_selector_group_member(
            self.GROUP, {"execution_context.params.domain": "test"},
            value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
        )
        self.assertEqual(value, "env_backend")

    def test_no_match_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "matched 0"):
            common.resolve_selector_group_member(
                self.GROUP, {"execution_context.params.domain": "identity"},
                value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
            )

    def test_bad_member_shape_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "group member must be"):
            common.resolve_selector_group_member(
                {"members": [{"wrong_field": "x"}]},
                {"execution_context.params.domain": "org"},
                value_field="cfg_file_set_key", label="cfg_file_set group state_backend",
            )


class CfgFileSetGroupInventoryTests(unittest.TestCase):
    """§Phase 31 3c: a target whose cfg_file_set_key names a group resolves per
    the execution context at load time; groups cannot be composed."""

    def _root(self, tmp: str) -> Path:
        root = Path(tmp)
        _write(root, "target_sources.yaml", (
            "target_sources:\n"
            "  bootstrap:\n"
            "    repo_url: https://example.invalid/bootstrap.git\n"
        ))
        _write(root, "cfg_file_sets.yaml", (
            "cfg_file_sets:\n"
            "  state_backend:\n"
            "    members:\n"
            "      - cfg_file_set_key: org\n"
            "        selectors:\n"
            "          match:\n"
            "            execution_context.params.domain: org\n"
            "      - cfg_file_set_key: env_backend\n"
            "        selectors:\n"
            "          in:\n"
            "            execution_context.params.domain: [dev, test]\n"
            "  org:\n"
            "    cfg_root: /org\n"
            "    cfg_files:\n"
            "      - tfstate.yaml\n"
            "  env_backend:\n"
            "    cfg_root: /env\n"
            "    cfg_files:\n"
            "      - ctl_state.yaml\n"
        ))
        (root / "targets" / "provision").mkdir(parents=True)
        _write(root / "targets" / "provision", "t.yaml", (
            "targets:\n"
            "  lz/tfstate_backend:\n"
            "    actions: [provision]\n"
            "    source_key: bootstrap\n"
            "    ref_key: state_backend\n"
            "    step_sequence_key: tfstate_backend\n"
            "    cfg_file_set_key: state_backend\n"
        ))
        return root

    def test_group_resolves_with_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            inv = common.load_inventory_cfg(
                root, "provision", {"execution_context.params.domain": "org"}
            )
            target = inv["targets"]["lz/tfstate_backend"]
            self.assertEqual(target["cfg_root"], "/org")
            self.assertEqual(target["cfg_files"], ["tfstate.yaml"])

    def test_group_unresolved_without_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            inv = common.load_inventory_cfg(root, "provision")
            target = inv["targets"]["lz/tfstate_backend"]
            self.assertEqual(target["cfg_file_set_group_unresolved"], "state_backend")
            self.assertEqual(target["cfg_files"], [])

    def test_group_cannot_be_composed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            _write(root, "extra_sets.yaml", (
                "cfg_file_sets:\n"
                "  bad_compose:\n"
                "    cfg_root: /x\n"
                "    cfg_file_set_keys:\n"
                "      - state_backend\n"
                "    cfg_files:\n"
                "      - a.yaml\n"
            ))
            with self.assertRaisesRegex(RuntimeError, "cannot be\\s+composed"):
                common.resolve_cfg_file_set_files(
                    "bad_compose",
                    common.collect_resource(root, "cfg_file_sets"),
                    root,
                )


if __name__ == "__main__":
    unittest.main()
