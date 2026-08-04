"""Engine-core provider-boundary tests (Phase 13).

The engine core (runners/utils/common.py and the engine cfg tools) must carry
no AWS vocabulary: no provider-named CLI arguments, field validation, branches,
ARN construction, subprocess invocations, target_run env handling, or user-facing
errors. AWS lives only in utils/providers/aws.py, its tests, providers.aws.*
cfg, and labelled documentation examples.
"""
import os
import re
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
from utils import common  # noqa: E402
import atlas_ctl_adapter_aws as aws_adapter  # noqa: E402
from utils.providers import get_adapter  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE_CORE_FILES = (
    REPO_ROOT / "runners" / "utils" / "common.py",
    REPO_ROOT / "cfg" / "validate_cfg.py",
    REPO_ROOT / "cfg" / "regenerate_guardrails.py",
)
FORBIDDEN = re.compile(r"(?i)(\baws\b|aws_|_aws|-aws|arn:|s3://|\bsts\b|\bboto)")
# §12: the AWS-implementation term ctl_role_chain must not leak into engine-core
FORBIDDEN_PUBLIC = re.compile(r"(ctl_role_chain|role.chain|skip_ctl_role_chain)")


class ProviderBoundaryTests(unittest.TestCase):
    def test_engine_core_has_no_provider_tokens(self):
        for path in ENGINE_CORE_FILES:
            text = path.read_text()
            hits = [
                f"{path.name}:{number}: {line.strip()}"
                for number, line in enumerate(text.splitlines(), start=1)
                if FORBIDDEN.search(line)
            ]
            self.assertEqual(hits, [], "engine-core provider tokens:\n" + "\n".join(hits))
            public_hits = [
                f"{path.name}:{number}: {line.strip()}"
                for number, line in enumerate(text.splitlines(), start=1)
                if FORBIDDEN_PUBLIC.search(line) and "removed" not in line
            ]
            self.assertEqual(public_hits, [], "engine-core role-chain leakage:\n" + "\n".join(public_hits))

    def test_unknown_provider_is_a_hard_error(self):
        with self.assertRaisesRegex(RuntimeError, "no provider adapter registered"):
            get_adapter("gcp")

    def test_adapter_contract_operations_exist(self):
        for operation in (
            "validate_catalog",
            "validate_target_execution_identity",
            "describe",
            "supported_execution_access_modes",
            "supports_identity_preflight",
            "validate_provider_options",
            "validate_profile_policy",
            "authorize_run",
            "load_runtime_catalogs",
            "collect_provider_cfg_findings",
            "resolve_target_cfg_references",
            "validate_active_target_access",
            "preflight_execution_identity",
            "materialize_target_binding",
            "target_assertion_argv",
            "validate_state_backend_entry",
            "resolve_ctl_state_credential",
            "create_state_syncer",
            "normal_execution_access_mode",
            "resolves_execution_identity",
            "target_consent",
            "execution_access_mode_from_options",
        ):
            self.assertTrue(callable(getattr(aws_adapter, operation, None)), operation)


class ContractWrapperTests(unittest.TestCase):
    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_validate_and_bind_wrappers_run_in_bypass_mode(self):
        catalogs = {
            "execution_identities": {},
            "credential_sources": {},
            "account_registry": {},
            "ctl_role_chain": None,
            "target_roles": {},
        }
        target_runs = {"target_run": {}}
        aws_adapter.validate_active_target_access(
            target_runs,
            catalogs,
            execution_context={},
            implementation_key="profile",
            execution_access_mode="force_bypass",
            provider_options={"force_bypass_credential_profile": "substitute"},
        )
        target_env: dict[str, str] = {}
        with unittest.mock.patch.object(
            aws_adapter.execution,
            "export_profile_credentials",
            return_value={"AWS_ACCESS_KEY_ID": "AKIA", "AWS_SECRET_ACCESS_KEY": "s"},
        ) as export:
            aws_adapter.materialize_target_binding(
                "target_run",
                {},
                target_env,
                catalogs,
                execution_context={},
                implementation_key="profile",
                execution_access_mode="force_bypass",
                provider_options={"force_bypass_credential_profile": "substitute"},
            )
        export.assert_called_once_with("substitute")
        self.assertEqual(target_env.get("AWS_ACCESS_KEY_ID"), "AKIA")
        self.assertNotIn("AWS_PROFILE", target_env)


class CtlRoleChainLoaderTests(unittest.TestCase):
    def test_rejects_removed_target_role_key(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "providers" / "aws").mkdir(parents=True)
            (root / "providers" / "aws" / "ctl_role_chain.yaml").write_text(
                "providers:\n  aws:\n    ctl_role_chain:\n"
                "      entry_credential_source_key: ctl_entry\n"
                "      runner_role_key: ctl_runner\n"
                "      target_role_key: ctl_target\n"
            )
            with self.assertRaisesRegex(RuntimeError, "target_role_key is removed"):
                aws_adapter.load_aws_ctl_role_chain_cfg(root)


class ProfileBindingTests(unittest.TestCase):
    def test_profile_resolves_to_env_credentials_only(self):
        # The host resolves the profile (`aws configure export-credentials`);
        # the box receives plain env credentials — no file is ever written.
        with unittest.mock.patch.object(
            aws_adapter.credentials,
            "_run_aws_json",
            return_value={
                "AccessKeyId": "AKIAWANTED",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            },
        ) as run_json:
            credentials = aws_adapter.export_profile_credentials("wanted")
        self.assertEqual(
            credentials,
            {
                "AWS_ACCESS_KEY_ID": "AKIAWANTED",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "AWS_SESSION_TOKEN": "token",
            },
        )
        self.assertIn("--profile", run_json.call_args.args[0])

    def test_incomplete_export_fails_loud(self):
        with unittest.mock.patch.object(
            aws_adapter.credentials, "_run_aws_json", return_value={"AccessKeyId": "AKIA"}
        ):
            with self.assertRaisesRegex(RuntimeError, "SecretAccessKey"):
                aws_adapter.export_profile_credentials("wanted")



class CredentialPathIteratorTests(unittest.TestCase):
    """§12.3: the AWS credential-path executor makes no assumption about the
    number of hops (production = 2; the iterator supports 1/2/3 and rejects
    cyclic/empty paths)."""

    def test_validate_rejects_empty_and_cyclic(self):
        with self.assertRaisesRegex(RuntimeError, "no role hops"):
            aws_adapter.validate_credential_path([])
        with self.assertRaisesRegex(RuntimeError, "repeats a role ARN"):
            aws_adapter.validate_credential_path([
                "arn:aws:iam::111111111111:role/a",
                "arn:aws:iam::111111111111:role/a",
            ])

    def test_iterator_supports_one_two_three_hops(self):
        seen = []

        def fake_run(cmd, capture_output, text, env):
            import types
            if "get-caller-identity" in cmd:
                out = '{"Account": "111111111111", "Arn": "arn:aws:sts::111111111111:assumed-role/Entry/s"}'
            else:
                # record the assumed role arn
                seen.append(cmd[cmd.index("--role-arn") + 1])
                out = '{"Credentials": {"AccessKeyId": "AK", "SecretAccessKey": "SK", "SessionToken": "ST"}}'
            return types.SimpleNamespace(returncode=0, stdout=out, stderr="")

        for hops in (
            ["arn:aws:iam::1:role/one"],
            ["arn:aws:iam::1:role/one", "arn:aws:iam::2:role/two"],
            ["arn:aws:iam::1:role/one", "arn:aws:iam::2:role/two", "arn:aws:iam::3:role/three"],
        ):
            seen.clear()
            with unittest.mock.patch("subprocess.run", side_effect=fake_run):
                creds = aws_adapter.assume_ctl_role_chain(
                    "entry-profile", hops,
                    session_name="s", entry_expected_account_id="111111111111",
                    entry_role_name="Entry",
                )
            self.assertEqual(seen, hops)  # every hop assumed, in order
            self.assertEqual(creds["AWS_ACCESS_KEY_ID"], "AK")


class ExecutionAccessModeTests(unittest.TestCase):
    def test_profile_modes_default_and_gate(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  strict:\n    ref_policy: commit_required\n"
                "  boot:\n    ref_policy: commit_required\n    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [standard, agreed_direct]\n"
                "      allowed_credential_implementation: [profile]\n"
            )
            # provider policy is DECLARED: no allowed_providers is a hard error
            with self.assertRaisesRegex(RuntimeError, "must declare allowed_providers"):
                common.ctl_allowed_providers(root, "strict")
            self.assertEqual(common.ctl_allowed_providers(root, "boot"), ["aws"])
            # the block is opaque to the engine; the adapter reads it
            policy = common.ctl_profile_provider_policy(root, "boot", "aws")
            self.assertEqual(
                policy["allowed_execution_access_modes"], ["standard", "agreed_direct"]
            )
            aws_adapter.authorize_run(
                policy, execution_access_mode="standard",
                provider_options={"credential_implementation": "profile"}, label="p",
            )
            with self.assertRaisesRegex(RuntimeError, "is not allowed by"):
                aws_adapter.authorize_run(
                    policy, execution_access_mode="force_bypass",
                    provider_options={}, label="p",
                )

    def test_mode_consent_is_the_adapters_answer(self):
        # WHICH modes need per-target consent, and in WHICH field, is the
        # adapter's call — the engine only asks.
        self.assertEqual(
            aws_adapter.target_consent("agreed_direct"),
            {
                "opt_in_field": "allow_agreed_direct_execution_access",
                "execution_field": "agreed_direct_credential_source_keys",
            },
        )
        self.assertIsNone(aws_adapter.target_consent("standard"))
        self.assertIsNone(aws_adapter.target_consent("force_bypass"))

    def test_consent_needs_both_the_opt_in_and_the_sources(self):
        # declaring the sources is NOT opting in: a target may name sources it
        # uses elsewhere while withholding consent to be run this way.
        import tempfile

        workflow = {"target_runs": ["target"]}
        execution = {
            "provider": "aws",
            "agreed_direct_credential_source_keys": ["admin"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n  boot:\n    ref_policy: commit_required\n"
                "    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [agreed_direct]\n"
                "      allowed_credential_implementation: [profile]\n"
            )

            def check(target_cfg):
                common.validate_execution_access(
                    root,
                    "boot",
                    workflow,
                    {"targets": {"target": target_cfg}},
                    execution_context={},
                    execution_access_modes={"aws": "agreed_direct"},
                    agreed_defer_ctl_state_backend_sync=False,
                    force_skip_ctl_state_backend_sync=False,
                    provider_options={},
                )

            with self.assertRaisesRegex(RuntimeError, "allow_agreed_direct_execution_access"):
                check({"execution_identities": {"aws": execution}})
            with self.assertRaisesRegex(RuntimeError, "agreed_direct_credential_source_keys"):
                check({
                    "allow_agreed_direct_execution_access": True,
                    "execution_identities": {"aws": {}},
                })
            check({
                "allow_agreed_direct_execution_access": True,
                "execution_identities": {"aws": execution},
            })

    def test_modes_that_resolve_no_identity_are_declared(self):
        self.assertFalse(aws_adapter.resolves_execution_identity("force_bypass"))
        self.assertTrue(aws_adapter.resolves_execution_identity("standard"))
        self.assertEqual(aws_adapter.normal_execution_access_mode(), "standard")

    def test_credential_implementation_is_required(self):
        with self.assertRaisesRegex(RuntimeError, "aws.credential_implementation"):
            aws_adapter.validate_provider_options({})
        aws_adapter.validate_provider_options({"credential_implementation": "profile"})

    def test_option_grants_are_enforced_from_the_provider_block(self):
        policy = {
            "allowed_execution_access_modes": ["force_bypass"],
            "allowed_credential_implementation": ["profile"],
        }
        opts = {"force_skip_account_expectation_check": "true"}
        with self.assertRaisesRegex(RuntimeError, "allow_force_skip_account_expectation_check"):
            aws_adapter.authorize_run(policy, execution_access_mode="force_bypass",
                                      provider_options=opts, label="p")
        granted = dict(policy, allow_force_skip_account_expectation_check=True)
        aws_adapter.authorize_run(granted, execution_access_mode="force_bypass",
                                  provider_options=opts, label="p")
        # a credential implementation the profile does not allow is refused
        with self.assertRaisesRegex(RuntimeError, "credential implementation"):
            aws_adapter.authorize_run(
                granted, execution_access_mode="force_bypass",
                provider_options={"credential_implementation": "web_identity"}, label="p")
        with self.assertRaisesRegex(RuntimeError, "must be 'true' or 'false'"):
            aws_adapter.validate_provider_options({
                "credential_implementation": "profile",
                "force_skip_account_expectation_check": "yes",
            })

    def test_options_may_imply_a_mode(self):
        self.assertEqual(
            aws_adapter.execution_access_mode_from_options(
                {"force_bypass_credential_profile": "dev"}
            ),
            "force_bypass",
        )
        self.assertIsNone(aws_adapter.execution_access_mode_from_options({}))


class AdapterInternalLayeringTest(unittest.TestCase):
    """§Phase 74: the AWS adapter is three parts and the direction is one way.

        _base <- credentials <- {execution, ctl_state} <- catalog

    `credentials` exists precisely so `execution` and `ctl_state` never import
    each other. The phase expected that cycle to cost seventeen names; measuring
    it showed ONE function on the wrong side — `resolve_target_aws_access`, which
    resolves an identity BLOCK to a credential and is therefore credentials' job,
    not execution's. Nothing else had to move.

    Without this test the direction is a comment. One `from ..execution import`
    inside `ctl_state` would restore the cycle and nothing else would notice.
    """

    ALLOWED = {
        "_base": set(),
        "credentials": {"_base"},
        "execution": {"_base", "credentials"},
        "ctl_state": {"_base", "credentials"},
        # whole-catalog validation touches every part by design
        "catalog": {"_base", "credentials", "execution", "ctl_state"},
    }

    def _package(self) -> Path:
        import atlas_ctl_adapter_aws as pkg
        return Path(pkg.__file__).parent

    def test_the_three_parts_exist(self):
        """Guards the suite: a flat module would make everything below vacuous."""
        for name in self.ALLOWED:
            self.assertTrue((self._package() / f"{name}.py").is_file(), name)

    def test_no_part_imports_against_the_direction(self):
        import ast

        offenders = []
        for part, allowed in self.ALLOWED.items():
            tree = ast.parse((self._package() / f"{part}.py").read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module and ".aws." in node.module:
                    dep = node.module.rsplit(".", 1)[1]
                    if dep not in allowed:
                        offenders.append(f"{part} -> {dep}")
        self.assertEqual(
            [], offenders,
            "an edge against the direction puts the two CONTRACTS back in a cycle, "
            "which is the thing credentials/ was extracted to prevent:\n"
            + "\n".join(offenders),
        )

    def test_no_part_uses_a_star_import(self):
        """`import *` drops every underscored name, and most of this adapter's
        internals are underscored — the first split lost `_run_aws_json` and 38
        tests with it, silently."""
        for part in self.ALLOWED:
            body = (self._package() / f"{part}.py").read_text()
            with self.subTest(part=part):
                self.assertNotIn("import *", body)

    def test_the_facade_still_exposes_the_flat_surface(self):
        """The adapter contract is module-level callables. Splitting the file must
        not move a name in the contract, so every engine-facing callable and every
        internal the tests reach for stays reachable on `utils.providers.aws`."""
        import atlas_ctl_adapter_aws as pkg

        for name in ("validate_catalog", "describe", "materialize_target_binding",
                     "resolve_ctl_state_credential", "create_state_syncer",
                     "preflight_execution_identity", "target_assertion_argv",
                     "_run_aws_json", "_assume_role_credentials", "subprocess"):
            with self.subTest(name=name):
                self.assertTrue(hasattr(pkg, name), f"{name} left the adapter surface")


class AdapterActivationTest(unittest.TestCase):
    """§Phase 74: building a context puts the adapter on the import path.

    Once the adapter became its own repository, reaching it stopped being free —
    something has to activate the declared checkout before the first adapter
    call. `ctl.py` gets that through `finalize_common_args`, and the engine's
    OTHER entry points (`validate_cfg.py`, `regenerate_guardrails.py`) never call
    it, so both shipped unable to run at all: "adapter package is not importable"
    on their first line of real work.

    Fixed by consolidating onto the OWNING construct rather than repeating the
    call per entry point — every adapter-reaching path builds a context first,
    and `build_execution_context` already holds the cfg root the declaration
    lives in. That is what makes a FUTURE entry point safe too, which a
    per-entry-point rule could only detect after someone wrote one.

    Verified in a SUBPROCESS, and it has to be: `conftest.py` puts the adapter on
    `sys.path` for the whole suite, so an in-process check passes no matter what
    the engine does — which is exactly why 691 tests were green while the real
    command could not import the adapter at all.
    """

    def _dev_cfg_root(self) -> Path:
        root = REPO_ROOT.parent.parent / "cfg/oxygen/oxygen-ctl-cfg-dev"
        if not (root / "local_repos.yaml").is_file():
            self.skipTest("dev cfg reflection not generated")
        return root

    # The assignment `validate_cfg` is run under; enough for a context to build.
    PARAMS = {
        "landing_zone": "live",
        "env.type": "dev",
        "aws.account": "dev",
        "aws.region": "eu-west-2",
        "aws.account_provisioning_mode": "direct",
    }

    def _run_clean(self, body: str) -> subprocess.CompletedProcess:
        """A python that knows only `runners` — no conftest, no adapter path."""
        script = (
            f"import sys\nsys.path.insert(0, {str(REPO_ROOT / 'runners')!r})\n"
            f"PARAMS = {self.PARAMS!r}\n"
        ) + body
        return subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True, text=True, timeout=120,
            # An inherited PYTHONPATH could supply the adapter and hide the point.
            env={k: v for k, v in os.environ.items() if k != "PYTHONPATH"},
        )

    def test_the_adapter_is_not_importable_without_activation(self):
        """Guards the test below: if a bare python could already import the
        adapter, that test would pass while proving nothing."""
        done = self._run_clean("import atlas_ctl_adapter_aws\nprint('IMPORTED')\n")
        self.assertNotEqual(0, done.returncode, f"unexpectedly importable: {done.stdout}")
        self.assertIn("ModuleNotFoundError", done.stderr)

    def test_building_a_context_makes_the_adapter_importable(self):
        done = self._run_clean(
            "from utils import common\n"
            f"common.activate_provider_adapters({str(self._dev_cfg_root())!r})\n"
            "import atlas_ctl_adapter_aws\n"
            "print('IMPORTED')\n"
        )
        self.assertEqual(0, done.returncode, done.stderr)
        self.assertIn("IMPORTED", done.stdout)

    def test_context_building_activates_before_it_reaches_an_adapter(self):
        """The real path: `build_execution_context` must not need a caller to
        have activated first. Run for its IMPORT behaviour only — the call is
        expected to fail on missing params, and MUST NOT fail on the adapter."""
        done = self._run_clean(
            "from pathlib import Path\n"
            "from utils import common\n"
            "try:\n"
            "    common.build_execution_context(\n"
            f"        Path({str(self._dev_cfg_root())!r}), action=None,\n"
            "        ctl_profile='local_dev', execution_params=dict(PARAMS),\n"
            "        execution_runtime_mode='local', providers=['aws'])\n"
            "    print('BUILT')\n"
            "except Exception as exc:\n"
            "    print('RAISED', type(exc).__name__, exc)\n"
        )
        output = done.stdout + done.stderr
        # It must get PAST the adapter, not merely fail somewhere else first: a
        # wrong argument type made the first version of this test die before the
        # adapter was ever reached, so deleting the activation changed nothing.
        self.assertIn("BUILT", output, f"never reached the adapter: {output}")
