"""When credentials are acquired is a declared cadence.

They were acquired once while the target repo was prepared, and every step of the
sequence ran under that frozen session — so a sequence outliving it failed on a
later step, worst case mid-apply.
"""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import ctl_cfg_fixture
from engine.catalog import workflow as catalog_workflow
from engine.cfg import materialize as cfg_materialize
from engine.execution import adapters as execution_adapters
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import policy as run_policy
from engine_surface import engine_source


class _Adapter:
    PROVIDER_NAME = "aws"

    def __init__(self):
        self.calls = 0

    def materialize_target_binding(self, target_run_id, target_run, target_env, catalogs, **kw):
        self.calls += 1
        target_env["AWS_ACCESS_KEY_ID"] = f"AKIA{self.calls}"


class RebindScopeTest(unittest.TestCase):
    """The cadence is the engine's; the acquisition stays the provider's."""

    def _rebind(self, adapter, providers):
        env = {}
        cfg_materialize.rebind_step_credentials(
            providers,
            target_run_id="t1",
            target_run={},
            step_env=env,
            provider_adapter=adapter,
            provider_catalogs={},
            execution_context={},
            credential_acquisition="local",
            execution_access_modes={"aws": "standard"},
            provider_options={},
        )
        return env

    def test_a_step_declaring_the_provider_is_rebound(self):
        a = _Adapter()
        self.assertEqual("AKIA1", self._rebind(a, ["aws"])["AWS_ACCESS_KEY_ID"])

    def test_each_call_yields_a_different_session(self):
        """A fresh acquisition, not the same value copied again."""

        a = _Adapter()
        first = self._rebind(a, ["aws"])["AWS_ACCESS_KEY_ID"]
        second = self._rebind(a, ["aws"])["AWS_ACCESS_KEY_ID"]
        self.assertNotEqual(first, second)

    def test_a_step_not_declaring_the_provider_is_left_alone(self):
        """Scoped by the step's own `providers`, per."""

        a = _Adapter()
        self.assertEqual({}, self._rebind(a, ["azure"]))
        self.assertEqual(0, a.calls)

    def test_a_step_declaring_nothing_is_left_alone(self):
        a = _Adapter()
        self.assertEqual({}, self._rebind(a, []))
        self.assertEqual(0, a.calls)

    def test_no_adapter_is_a_no_op(self):
        cfg_materialize.rebind_step_credentials(
            ["aws"],
            target_run_id="t1",
            target_run={},
            step_env={},
            provider_adapter=None,
            provider_catalogs={},
            execution_context={},
            credential_acquisition="local",
            execution_access_modes=None,
            provider_options=None,
        )


class RefreshPolicyTest(unittest.TestCase):
    """Per provider, against that provider's own allowlist."""

    def _cfg(self, tmp: str, modes) -> Path:
        root = Path(tmp)
        kernel_yaml_io.write_yaml_file(
            root / "ctl_profiles.yaml",
            {
                "ctl_profiles": {
                    "p": {
                        "description": "d",
                        "ref_policy": "local_dirty_allowed",
                        "allowed_providers": ["aws"],
                        "aws": {"allowed_credential_refresh_modes": modes},
                    }
                }
            },
        )
        # the cadence check asks the adapter, so the run's providers must be
        # declared in the same root the profile allows them from
        return ctl_cfg_fixture.activate(self, ctl_cfg_fixture.declare_providers(root, "aws"))

    def test_a_mode_outside_the_allowlist_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._cfg(tmp, ["per_target"])
            with self.assertRaisesRegex(RuntimeError, "does not allow credential refresh"):
                run_policy.validate_credential_refresh_modes(
                    root, "p", {"aws": "per_step"}, ["aws"]
                )

    def test_an_allowed_mode_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._cfg(tmp, ["per_target", "per_step"])
            self.assertEqual(
                {"aws": "per_step"},
                run_policy.validate_credential_refresh_modes(
                    root, "p", {"aws": "per_step"}, ["aws"]
                ),
            )

    def test_every_participating_provider_must_be_named(self):
        """No inherited default: an unnamed provider is a skipped choice."""

        with tempfile.TemporaryDirectory() as tmp:
            root = self._cfg(tmp, ["per_target"])
            with self.assertRaisesRegex(RuntimeError, "must name every provider"):
                run_policy.validate_credential_refresh_modes(root, "p", {}, ["aws"])

    def test_naming_a_provider_not_in_the_run_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._cfg(tmp, ["per_target"])
            with self.assertRaisesRegex(RuntimeError, "not in --providers"):
                run_policy.validate_credential_refresh_modes(
                    root, "p", {"aws": "per_target", "azure": "per_step"}, ["aws"]
                )


if __name__ == "__main__":
    unittest.main()


class ChildInheritsNothingTest(unittest.TestCase):
    """A workflow spawns children by argv, so the choice must travel explicitly.

    With no default, a child that was not told would fail at its own parser —
    which is the intended shape, but only if the parent actually passes it.
    """

    SPEC = {
        "ctl_entrypoint": Path("/x/ctl.py"),
        "ctl_cfg_root": Path("/cfg"),
        "ctl_profile": "local_dev",
        "ctl_state_local_root": "/state",
        "execution_runtime_mode": "local",
        "action": "plan",
        "providers": ["aws"],
        "execution_params": {"landing_zone": "live"},
        "provider_options": {},
        "execution_access_modes": {"aws": "standard"},
        "credential_refresh_modes": {"aws": "per_step"},
    }

    def test_the_parents_choice_reaches_the_child_argv(self):
        argv = catalog_workflow.WorkflowChildren.build_command(
            self.SPEC,
            "env/core/baseline",
            parent_run_dir=Path("/run"),
            parent_run_id="PARENT",
        )
        self.assertIn("--credential-refresh-mode", argv)
        self.assertEqual("aws=per_step", argv[argv.index("--credential-refresh-mode") + 1])


class CadenceMatchesAccessModeTest(unittest.TestCase):
    """`per_step` only acquires a NEW session on the standard path.

    Under agreed_direct and force_bypass the acquisition hands back the
    credential the run already resolved, so accepting the cadence there would
    report refresh the run does not have.
    """

    def _cfg(self, tmp):
        root = Path(tmp)
        kernel_yaml_io.write_yaml_file(
            root / "ctl_profiles.yaml",
            {
                "ctl_profiles": {
                    "p": {
                        "description": "d",
                        "ref_policy": "local_dirty_allowed",
                        "allowed_providers": ["aws"],
                        "aws": {"allowed_credential_refresh_modes": ["per_target", "per_step"]},
                    }
                }
            },
        )
        return ctl_cfg_fixture.activate(self, ctl_cfg_fixture.declare_providers(root, "aws"))

    def _check(self, root, mode, access):
        return run_policy.validate_credential_refresh_modes(
            root, "p", {"aws": mode}, ["aws"], {"aws": access}
        )

    def test_per_step_is_refused_under_agreed_direct(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "has no effect"):
                self._check(self._cfg(tmp), "per_step", "agreed_direct")

    def test_per_step_is_refused_under_force_bypass(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError, "has no effect"):
                self._check(self._cfg(tmp), "per_step", "force_bypass")

    def test_per_step_is_accepted_under_standard(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                {"aws": "per_step"}, self._check(self._cfg(tmp), "per_step", "standard")
            )

    def test_per_target_is_valid_under_every_access_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._cfg(tmp)
            for access in ("standard", "agreed_direct", "force_bypass"):
                self.assertEqual({"aws": "per_target"}, self._check(root, "per_target", access))


class AdapterOwnsCadenceVocabularyTest(unittest.TestCase):
    """The engine asks the adapter which cadences exist and where they work.

    Before this the engine named `per_step` and the `standard` access mode in its
    own strings, so a provider that refreshes differently could not be added
    without editing engine code.
    """

    def setUp(self):
        ctl_cfg_fixture.cfg_root(self, "aws")

    def test_adapter_declares_its_cadences(self):
        adapter = execution_adapters.get_adapter("aws")
        self.assertEqual({"per_target", "per_step"}, adapter.supported_credential_refresh_modes())

    def test_an_undeclared_cadence_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "not supported by aws"):
            run_policy.validate_cadence_supported("aws", "per_universe")

    def test_a_cadence_is_refused_where_it_cannot_mint_a_session(self):
        with self.assertRaisesRegex(RuntimeError, "has no effect"):
            run_policy.validate_cadence_against_access_mode(
                {"aws": "per_step"}, {"aws": "force_bypass"}
            )

    def test_a_cadence_is_allowed_where_the_adapter_says_it_works(self):
        run_policy.validate_cadence_against_access_mode({"aws": "per_step"}, {"aws": "standard"})
        run_policy.validate_cadence_against_access_mode(
            {"aws": "per_target"}, {"aws": "force_bypass"}
        )

    def test_the_engine_names_no_cadence_of_its_own(self):
        """A ratchet: cadence strings belong in the adapter, not the engine."""

        source = (
            engine_source().split("def validate_cadence_against_access_mode")[1].split("\ndef ")[0]
        )
        self.assertNotIn("per_step", source)
        self.assertNotIn("standard", source)
