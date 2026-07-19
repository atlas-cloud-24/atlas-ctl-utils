import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def ctx(**params):
    return {f"execution_context.params.{k}": v for k, v in params.items()}


class TargetInstanceSegmentsTests(unittest.TestCase):
    """§Phase 31 6a / Q1g / Q1j — instance identity path contract."""

    def test_declaration_order_hive_segments(self):
        segs = common.resolve_target_instance_segments(
            ["account", "env_type"], ctx(account="dev", env_type="dev"), label="t"
        )
        self.assertEqual(segs, ["account=dev", "env_type=dev"])
        self.assertEqual(common.instance_relpath(segs), "instances/account=dev/env_type=dev")

    def test_singleton_no_instances_layer(self):
        self.assertEqual(
            common.resolve_target_instance_segments(None, ctx(), label="t"), []
        )
        self.assertEqual(common.instance_relpath([]), "")

    def test_missing_context_param_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "not in the execution context"):
            common.resolve_target_instance_segments(["account"], ctx(env_type="dev"), label="t")

    def test_bad_value_charset_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, r"must match \[a-z0-9_.-\]\+"):
            common.resolve_target_instance_segments(["account"], ctx(account="Dev"), label="t")

    def test_slash_in_value_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, r"must match"):
            common.resolve_target_instance_segments(
                ["domain"], ctx(domain="env/dev"), label="t"
            )

    def test_duplicate_param_is_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "lists 'account' twice"):
            common.resolve_target_instance_segments(
                ["account", "account"], ctx(account="dev"), label="t"
            )

    def test_suffix_length_cap_no_fallback(self):
        long_val = "a" * 130
        with self.assertRaisesRegex(RuntimeError, "exceeds 128"):
            common.resolve_target_instance_segments(
                ["account"], ctx(account=long_val), label="t"
            )

    def test_split_instance_segments_boundary(self):
        parts = ["account=dev", "env_type=dev", "runs", "019f"]
        inst, rest = common.split_instance_segments(parts)
        self.assertEqual(inst, ["account=dev", "env_type=dev"])
        self.assertEqual(rest, ["runs", "019f"])

    def test_split_singleton_structure_first(self):
        inst, rest = common.split_instance_segments(["committed.yaml"])
        self.assertEqual(inst, [])
        self.assertEqual(rest, ["committed.yaml"])


if __name__ == "__main__":
    unittest.main()


class StateRelpathTests(unittest.TestCase):
    """§Phase 31 6b — namespace-relative state tree compose/parse."""

    def test_compose_target_instance(self):
        p = common.compose_state_relpath(
            "provision", "target", "env/core", ["account=stg", "env_type=stg"]
        )
        self.assertEqual(
            str(p), "provision/target/env/core/instances/account=stg/env_type=stg"
        )

    def test_compose_singleton(self):
        p = common.compose_state_relpath("provision", "target", "landing_zone/org/baseline", [])
        self.assertEqual(str(p), "provision/target/landing_zone/org/baseline")

    def test_compose_rejects_unknown_kind(self):
        with self.assertRaisesRegex(RuntimeError, "unknown state kind"):
            common.compose_state_relpath("provision", "fan_out", "x", [])

    def test_parse_roundtrip_instance(self):
        root = Path("/tmp/ns")
        d = root / "provision/target/env/core/instances/account=stg/env_type=stg"
        parsed = common.parse_state_relpath(root, d)
        self.assertEqual(parsed["kind"], "target")
        self.assertEqual(parsed["key"], "env/core")
        self.assertEqual(parsed["instance_segments"], ["account=stg", "env_type=stg"])
        self.assertEqual(parsed["address"], "target/env/core/account=stg/env_type=stg")

    def test_parse_singleton_stops_at_structural(self):
        root = Path("/tmp/ns")
        d = root / "provision/target/landing_zone/org/baseline/runs"
        parsed = common.parse_state_relpath(root, d)
        self.assertEqual(parsed["key"], "landing_zone/org/baseline")
        self.assertEqual(parsed["instance_segments"], [])

    def test_parse_workflow_kind(self):
        root = Path("/tmp/ns")
        d = root / "provision/workflow/env/bootstrap/instances/sha256-abc"
        parsed = common.parse_state_relpath(root, d)
        self.assertEqual(parsed["kind"], "workflow")
        self.assertEqual(parsed["instance_segments"], ["sha256-abc"])

    def test_parse_outside_root_is_none(self):
        self.assertIsNone(common.parse_state_relpath(Path("/tmp/ns"), Path("/tmp/other/x")))
