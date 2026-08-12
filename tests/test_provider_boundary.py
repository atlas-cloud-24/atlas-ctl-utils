"""Engine-core provider-boundary tests.

The engine core (the engine library under `runners/` and the engine cfg tools
under `cfg/`) must carry no AWS vocabulary: no provider-named CLI arguments,
field validation, branches, ARN construction, subprocess invocations, target_run
env handling, or user-facing errors. AWS lives only in the adapter repository,
its tests, providers.aws.* cfg, and labelled documentation examples.

Which files that covers is DISCOVERED, not enumerated. A by-name list narrows
itself the moment engine code moves: `common.py` splitting into a
`runners/engine/` package would leave the boundary guarding whichever files kept
the listed names, and the suite would stay green while covering a fraction of the
engine. The walk below covers a module the moment it exists.
"""


import functools
import os
import re
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
import atlas_ctl_adapter_aws as aws_adapter
import ctl_cfg_fixture
from engine.run import policy as run_policy
from engine.execution.adapters import get_adapter

REPO_ROOT = Path(__file__).resolve().parents[1]

# The engine's SOURCE ROOTS, walked whole. `runners/` is the engine library and
# its entry points; `cfg/` is the engine cfg tooling. Both are walked rather than
# listed so that a package which does not exist yet is covered on the day it
# lands, with no edit here.
ENGINE_SOURCE_ROOTS = (REPO_ROOT / "runners", REPO_ROOT / "cfg")

# No subtree of the engine is exempt. The provider registry IS the cfg
# declaration, so nowhere in the engine may a provider name appear.
EXCLUDED_SUBTREES = ()

# Directory NAMES dropped wherever in the walk they occur.
EXCLUDED_DIR_NAMES = frozenset(
    {
        # Generated copies of source, not source — and a stale .pyc has produced a
        # false green in this repo before.
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        # Tests NAME the vocabulary they keep out of engine core: this very file
        # says "aws" dozens of times. Scanning them would forbid the rule from
        # stating itself.
        "tests",
    }
)

# The provider ADAPTER package needs no exclusion entry: it implements the
# provider contract, so naming AWS is its job, and it lives in its own repository
# (`atlas-ctl-adapter-aws`) outside every source root above. That is structural,
# not incidental — `test_the_adapter_package_is_outside_every_source_root` holds
# it there, so vendoring the adapter back under `runners/` goes red rather than
# quietly turning the boundary into a self-contradiction.

FORBIDDEN = re.compile(r"(?i)(\baws\b|aws_|_aws|-aws|arn:|s3://|\bsts\b|\bboto)")
# §12: the AWS-implementation term ctl_role_chain must not leak into engine-core
FORBIDDEN_PUBLIC = re.compile(r"(ctl_role_chain|role.chain|skip_ctl_role_chain)")


@functools.cache
def engine_core_files() -> tuple[Path, ...]:
    """Every engine-core module, discovered by walking the source roots.

    Raises instead of returning nothing. A walk that matches zero files satisfies
    every assertion built on top of it, so silence here would read as a pass — the
    one failure mode a discovery-based rule has that an enumerated list does not.
    """

    found: list[Path] = []
    for root in ENGINE_SOURCE_ROOTS:
        if not root.is_dir():
            raise RuntimeError(
                f"engine source root does not exist: {root} — the provider boundary "
                "would cover nothing and every test below it would pass vacuously"
            )
        before = len(found)
        for dirpath, dirnames, filenames in os.walk(root):
            here = Path(dirpath)
            dirnames[:] = sorted(
                name
                for name in dirnames
                if name not in EXCLUDED_DIR_NAMES and (here / name) not in EXCLUDED_SUBTREES
            )
            found.extend(here / name for name in sorted(filenames) if name.endswith(".py"))
        if len(found) == before:
            raise RuntimeError(
                f"engine source root contributed no modules: {root} — either it moved "
                "or an exclusion swallowed it whole"
            )
    return tuple(found)


class EngineCoreDiscoveryTests(unittest.TestCase):
    """Guards the walk itself, which every boundary assertion below trusts.

    Without this the walk is free to match nothing — a mistyped root, an
    exclusion that grew too wide, a package that moved — and the boundary tests
    would iterate an empty tuple and report success.
    """

    # What the walk finds today. A floor, not a list: it is raised when the engine
    # grows, and a drop below it means coverage was lost, not that code was tidied.
    MINIMUM_ENGINE_CORE_FILES = 10

    # The files the boundary used to name explicitly, kept as a SUBSET check so a
    # rewrite of the walk can only ever widen coverage, never quietly narrow it.
    HISTORICALLY_ENUMERATED = (
        REPO_ROOT / "cfg" / "validate_cfg.py",
        REPO_ROOT / "cfg" / "regenerate_guardrails.py",
    )

    def test_the_walk_finds_the_engine(self):
        files = engine_core_files()
        self.assertGreaterEqual(
            len(files),
            self.MINIMUM_ENGINE_CORE_FILES,
            f"the walk found {len(files)} engine-core modules, fewer than the "
            f"{self.MINIMUM_ENGINE_CORE_FILES} it must cover:\n"
            + "\n".join(str(path) for path in files),
        )

    def test_the_engine_core_package_is_covered(self):
        """The walk must provably reach the engine, not merely find SOME file.

        Asserted against the package rather than a filename: a filename check
        answers a question the split already made meaningless, while a package
        that contributes nothing means the walk missed the engine entirely.
        """

        package = REPO_ROOT / "runners" / "engine"
        covered = [path for path in engine_core_files() if package in path.parents]
        self.assertTrue(
            covered, f"the walk reached no module under {package}"
        )
        self.assertIn(package / "commands" / "pipeline.py", set(covered))

    def test_the_walk_still_covers_what_the_old_list_named(self):
        covered = set(engine_core_files())
        missing = [str(path) for path in self.HISTORICALLY_ENUMERATED if path not in covered]
        self.assertEqual(
            [], missing, "coverage lost versus the old by-name list:\n" + "\n".join(missing)
        )

    def test_the_adapter_package_is_outside_every_source_root(self):
        """The adapter is excluded by ROLE — it implements the provider contract —
        and that exclusion is enforced by WHERE it lives, not by a skip rule."""

        adapter = Path(aws_adapter.__file__).resolve().parent
        for root in ENGINE_SOURCE_ROOTS:
            self.assertFalse(
                adapter.is_relative_to(root),
                f"the provider adapter moved inside engine core: {adapter} under {root}",
            )


class ProviderBoundaryTests(unittest.TestCase):
    def test_engine_core_has_no_provider_tokens(self):
        for path in engine_core_files():
            text = path.read_text()
            label = path.relative_to(REPO_ROOT)
            hits = [
                f"{label}:{number}: {line.strip()}"
                for number, line in enumerate(text.splitlines(), start=1)
                if FORBIDDEN.search(line)
            ]
            self.assertEqual(hits, [], "engine-core provider tokens:\n" + "\n".join(hits))
            public_hits = [
                f"{label}:{number}: {line.strip()}"
                for number, line in enumerate(text.splitlines(), start=1)
                if FORBIDDEN_PUBLIC.search(line) and "removed" not in line
            ]
            self.assertEqual(public_hits, [], "engine-core role-chain leakage:\n" + "\n".join(public_hits))

    def test_unknown_provider_is_a_hard_error(self):
        """Unknown means UNDECLARED. The engine holds no adapter list to be absent
        from, so the only registry a provider can be missing from is the cfg
        declaration, and the error has to say which one it read."""

        ctl_cfg_fixture.cfg_root(self, "aws")
        with self.assertRaisesRegex(RuntimeError, "is not declared in execution_providers.yaml"):
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
        # the wrappers dispatch back through the engine's provider lookup
        ctl_cfg_fixture.cfg_root(self, "aws")
        catalogs = {
            "execution_identities": {},
            "credential_sources": {},
            "account_registry": {},
            "ctl_role_chain": None,
            "sso_sessions": {},
            "target_roles": {},
        }
        target_runs = {"target_run": {}}
        aws_adapter.validate_active_target_access(
            target_runs,
            catalogs,
            execution_context={},
            implementation_key="sso",
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
                implementation_key="sso",
                execution_access_mode="force_bypass",
                provider_options={"force_bypass_credential_profile": "substitute"},
            )
        export.assert_called_once_with("substitute")
        self.assertEqual(target_env.get("AWS_ACCESS_KEY_ID"), "AKIA")
        self.assertNotIn("AWS_PROFILE", target_env)


class CtlRoleChainLoaderTests(unittest.TestCase):
    def test_rejects_removed_target_role_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "providers" / "aws").mkdir(parents=True)
            (root / "providers" / "aws" / "ctl_role_chain.yaml").write_text(
                "providers:\n  aws:\n    ctl_role_chain:\n"
                "      entry_credential_source_key: target_sources.ctl_entry\n"
                "      runner_role_key: ctl_runner\n"
                "      target_role_key: ctl_target\n"
            )
            with self.assertRaisesRegex(RuntimeError, "target_role_key is removed"):
                aws_adapter.load_aws_ctl_role_chain_cfg(root)


class SsoCredentialAcquisitionTests(unittest.TestCase):
    """A credential source returns the env triple; nothing else crosses.

    The box receives ONLY env credentials — no config file, no mounted
    credential material, no profile concept inside the box.
    """

    def test_sso_acquisition_returns_env_credentials_only(self):
        response = {"roleCredentials": {
            "accessKeyId": "AKIA", "secretAccessKey": "SECRET",
            "sessionToken": "TOKEN", "expiration": 123,
        }}
        with unittest.mock.patch.object(
            aws_adapter.credentials, "_run_aws_json", return_value=response
        ), unittest.mock.patch.object(
            aws_adapter.credentials, "_sso_access_token", return_value="t"
        ):
            creds = aws_adapter.acquire_aws_sso_credentials(
                {"start_url": "https://x", "sso_region": "eu-west-2", "session_name": "s"},
                "111111111111", "CtlEntryAccess", label="test",
            )
        self.assertEqual(
            sorted(creds),
            ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"],
        )

    def test_incomplete_sso_response_fails_loud(self):
        response = {"roleCredentials": {"accessKeyId": "AKIA", "sessionToken": "T"}}
        with unittest.mock.patch.object(
            aws_adapter.credentials, "_run_aws_json", return_value=response
        ), unittest.mock.patch.object(
            aws_adapter.credentials, "_sso_access_token", return_value="t"
        ), self.assertRaisesRegex(RuntimeError, "secretAccessKey"):
            aws_adapter.acquire_aws_sso_credentials(
                {"start_url": "https://x", "sso_region": "eu-west-2", "session_name": "s"},
                "111111111111", "CtlEntryAccess", label="test",
            )

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
                    {"AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK",
                     "AWS_SESSION_TOKEN": "ST"},
                    hops,
                    session_name="s", entry_expected_account_id="111111111111",
                    entry_role_name="Entry",
                )
            self.assertEqual(seen, hops)  # every hop assumed, in order
            self.assertEqual(creds["AWS_ACCESS_KEY_ID"], "AK")


class ExecutionAccessModeTests(unittest.TestCase):
    def test_profile_modes_default_and_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n"
                "  strict:\n    ref_policy: commit_required\n"
                "  boot:\n    ref_policy: commit_required\n    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [standard, agreed_direct]\n"
                "      allowed_credential_acquisition: [sso]\n"
            )
            ctl_cfg_fixture.activate(self, ctl_cfg_fixture.declare_providers(root, "aws"))
            # provider policy is DECLARED: no allowed_providers is a hard error
            with self.assertRaisesRegex(RuntimeError, "must declare allowed_providers"):
                run_policy.ctl_allowed_providers(root, "strict")
            self.assertEqual(run_policy.ctl_allowed_providers(root, "boot"), ["aws"])
            # the block is opaque to the engine; the adapter reads it
            policy = run_policy.ctl_profile_provider_policy(root, "boot", "aws")
            self.assertEqual(
                policy["allowed_execution_access_modes"], ["standard", "agreed_direct"]
            )
            aws_adapter.authorize_run(
                policy, execution_access_mode="standard",
                provider_options={"credential_acquisition": "sso"}, label="p",
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
                "execution_field": "allowed_agreed_direct_credential_sources",
            },
        )
        self.assertIsNone(aws_adapter.target_consent("standard"))
        self.assertIsNone(aws_adapter.target_consent("force_bypass"))

    def test_consent_needs_both_the_opt_in_and_the_sources(self):
        # declaring the sources is NOT opting in: a target may name sources it
        # uses elsewhere while withholding consent to be run this way.

        workflow = {"target_runs": ["target"]}
        execution = {
            "provider": "aws",
            "allowed_agreed_direct_credential_sources": ["admin"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ctl_profiles.yaml").write_text(
                "ctl_profiles:\n  boot:\n    ref_policy: commit_required\n"
                "    allowed_providers: [aws]\n"
                "    aws:\n      allowed_execution_access_modes: [agreed_direct]\n"
                "      allowed_credential_acquisition: [sso]\n"
            )
            ctl_cfg_fixture.activate(self, ctl_cfg_fixture.declare_providers(root, "aws"))

            def check(target_cfg):
                run_policy.validate_execution_access(
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
            with self.assertRaisesRegex(RuntimeError, "allowed_agreed_direct_credential_sources"):
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

    def test_credential_acquisition_is_required(self):
        with self.assertRaisesRegex(RuntimeError, "aws.credential_acquisition"):
            aws_adapter.validate_provider_options({})
        aws_adapter.validate_provider_options({"credential_acquisition": "sso"})

    def test_option_grants_are_enforced_from_the_provider_block(self):
        policy = {
            "allowed_execution_access_modes": ["force_bypass"],
            "allowed_credential_acquisition": ["sso"],
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
                provider_options={"credential_acquisition": "web_identity"}, label="p")
        with self.assertRaisesRegex(RuntimeError, "must be 'true' or 'false'"):
            aws_adapter.validate_provider_options({
                "credential_acquisition": "sso",
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
    """The AWS adapter is three parts and the direction is one way.

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
        """

        guards the suite: a flat module would make everything below vacuous."""

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
        """

        the adapter contract is module-level callables. Splitting the file must
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
    """Building a context puts the adapter on the import path.

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
        """

        guards the test below: if a bare python could already import the
        adapter, that test would pass while proving nothing."""

        done = self._run_clean("import atlas_ctl_adapter_aws\nprint('IMPORTED')\n")
        self.assertNotEqual(0, done.returncode, f"unexpectedly importable: {done.stdout}")
        self.assertIn("ModuleNotFoundError", done.stderr)

    def test_building_a_context_makes_the_adapter_importable(self):
        done = self._run_clean(
            "from engine.execution import providers\n"
            f"providers.activate_provider_adapters({str(self._dev_cfg_root())!r})\n"
            "import atlas_ctl_adapter_aws\n"
            "print('IMPORTED')\n"
        )
        self.assertEqual(0, done.returncode, done.stderr)
        self.assertIn("IMPORTED", done.stdout)

    def test_context_building_activates_before_it_reaches_an_adapter(self):
        """

        the real path: `build_execution_context` must not need a caller to
        have activated first. Run for its IMPORT behaviour only — the call is
        expected to fail on missing params, and MUST NOT fail on the adapter."""

        done = self._run_clean(
            "from pathlib import Path\n"
            "from engine.execution import run_context\n"
            "try:\n"
            "    run_context.build_execution_context(\n"
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
