"""Distinct target signatures, and a step path that agrees
with the action it was reached under.

Both replace duplication with a check. The signature guard catches one target
declared twice; the path guard catches a manifest entry filed under one action
while pointing at another action's directory — which is what the step-level
`action:` field was briefly introduced for, before it was reverted as a duplicate
of the manifest's own grouping.
"""

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.catalog import target_catalog
from engine.cfg import materialize as cfg_materialize

BASE = {
    "source_key": "target_sources.seed",
    "ref_key": "env/live",
    "procedure_key": "baseline",
    "domains": ["domains.env"],
    "input_params": ["env.type"],
    "target_instance_params": ["env.type", "aws.account"],
}


class DistinctTargetSignatureTest(unittest.TestCase):
    def test_an_identical_signature_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "same input signature"):
            target_catalog.TargetCatalog.validate_distinct_signatures(
                {"env/a": BASE, "env/b": dict(BASE)}
            )

    def test_a_different_procedure_is_accepted(self):
        target_catalog.TargetCatalog.validate_distinct_signatures(
            {"env/a": BASE, "env/b": {**BASE, "procedure_key": "other"}}
        )

    def test_different_cfg_keys_are_accepted(self):
        """The signature must be COMPLETE or this is a false positive: same
        source, procedure and params, but different cfg, so different resources."""
        target_catalog.TargetCatalog.validate_distinct_signatures(
            {
                "env/a": {**BASE, "cfg_keys": {"env": {"a": "a"}}},
                "env/b": {**BASE, "cfg_keys": {"env": {"b": "b"}}},
            }
        )

    def test_an_incomplete_declaration_is_left_to_its_own_validator(self):
        target_catalog.TargetCatalog.validate_distinct_signatures(
            {
                "env/a": {"source_key": "target_sources.seed"},
                "env/b": {"source_key": "target_sources.seed"},
            }
        )

    def test_the_real_ctl_cfg_has_no_duplicates(self):
        import glob

        import yaml

        root = Path("/home/valerii/programs/atlas/cfg/oxygen/oxygen-ctl-cfg/targets")
        if not root.is_dir():
            self.skipTest("oxygen ctl cfg not present")
        definitions = {}
        for path in glob.glob(str(root / "*.yaml")):
            definitions.update((yaml.safe_load(open(path)) or {}).get("targets") or {})
        self.assertGreater(len(definitions), 0)
        target_catalog.TargetCatalog.validate_distinct_signatures(definitions)


class StepPathMatchesActionTest(unittest.TestCase):
    def _repo(self, tmp: Path, path: str) -> Path:
        adapter = tmp / cfg_materialize.ADAPTER_DIR
        (adapter / "steps/destroy/infra/src").mkdir(parents=True)
        (adapter / "steps/provision/infra/src").mkdir(parents=True)
        for action in ("destroy", "provision"):
            (adapter / f"steps/{action}/infra/step.yaml").write_text(
                "id: provision/infra\nproviders: [execution_providers.aws]\n"
                "runtime:\n  image: infra\n"
            )
        (adapter / "manifest.yaml").write_text(
            textwrap.dedent(f"""
                manifest:
                  provision:
                    provision/infra:
                      path: {path}
            """)
        )
        (adapter / "procedures.yaml").write_text(
            textwrap.dedent("""
                procedures:
                  provision:
                    baseline:
                      steps:
                      - provision/infra
            """)
        )
        return tmp

    def test_a_path_under_another_action_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = self._repo(Path(tmp), "atlas_ctl_adapter/steps/destroy/infra")
            with self.assertRaisesRegex(RuntimeError, "must sit under"):
                cfg_materialize.get_repo_local_steps(repo, "provision", "baseline")

    def test_a_matching_path_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = self._repo(Path(tmp), "atlas_ctl_adapter/steps/provision/infra")
            ids, steps = cfg_materialize.get_repo_local_steps(repo, "provision", "baseline")
            self.assertEqual(["provision/infra"], ids)
            self.assertEqual(1, len(steps))
            self.assertNotIn("plt", steps[0].to_document())


if __name__ == "__main__":
    unittest.main()
