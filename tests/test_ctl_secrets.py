"""A secret is declared once and referenced by key.

What this pins is the reason the registry exists rather than the mechanism: an
undeclared key is refused, a missing value is refused as a MISSING DECLARATION
rather than surfacing later as an authentication failure, and the secret that
fetches an adapter must resolve without one — otherwise a provider-backed secret
would need its own adapter fetched, using a secret, using an adapter.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.cfg import secrets as cfg_secrets


def cfg_root(tmp: str, *, secrets: dict, providers: dict | None = None) -> Path:
    root = Path(tmp)
    (root / "__meta__.yaml").write_text(yaml.safe_dump({"cfg_root": {"kind": "ctl"}}))
    (root / "ctl_secrets.yaml").write_text(yaml.safe_dump({"ctl_secrets": secrets}))
    if providers is not None:
        (root / "ctl_providers.yaml").write_text(
            yaml.safe_dump({"ctl_providers": providers})
        )
    return root


class DeclarationIsRequiredTest(unittest.TestCase):
    def test_an_undeclared_key_is_refused_and_says_what_is_declared(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"github_token": {"provider": "env", "name": "X"}})
            with self.assertRaises(RuntimeError) as caught:
                cfg_secrets.SecretStore(root).resolve("typo", label="a source")
            message = str(caught.exception)
            self.assertIn("typo", message)
            self.assertIn("github_token", message)

    def test_an_entry_without_a_provider_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"name": "X"}})
            with self.assertRaisesRegex(RuntimeError, "must declare a provider"):
                cfg_secrets.SecretStore(root).resolve("k", label="a source")

    def test_an_empty_key_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"provider": "env", "name": "X"}})
            with self.assertRaisesRegex(RuntimeError, "non-empty string"):
                cfg_secrets.SecretStore(root).resolve("", label="a source")


class EnvProviderTest(unittest.TestCase):
    def test_it_reads_the_named_variable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"provider": "env", "name": "ATLAS_TEST_SECRET"}})
            with mock.patch.dict(os.environ, {"ATLAS_TEST_SECRET": "value-from-env"}):
                self.assertEqual(
                    "value-from-env",
                    cfg_secrets.SecretStore(root).resolve("k", label="a source"),
                )

    def test_an_unset_variable_is_refused_rather_than_resolving_to_nothing(self):
        """A secret that resolves to `None` fails later as an authentication
        error, which reads as a wrong credential rather than a missing one."""
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"provider": "env", "name": "ATLAS_ABSENT"}})
            with mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "is not set"):
                    cfg_secrets.SecretStore(root).resolve("k", label="a source")

    def test_env_requires_a_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"provider": "env"}})
            with self.assertRaisesRegex(RuntimeError, "requires a `name`"):
                cfg_secrets.SecretStore(root).resolve("k", label="a source")


class FileProviderTest(unittest.TestCase):
    def test_it_reads_the_file_without_its_trailing_newline(self):
        with tempfile.TemporaryDirectory() as tmp:
            secret_file = Path(tmp) / "secret"
            secret_file.write_text("s3cret\n")
            root = cfg_root(tmp, secrets={"k": {"provider": "file", "path": str(secret_file)}})
            self.assertEqual(
                "s3cret", cfg_secrets.SecretStore(root).resolve("k", label="a source")
            )

    def test_a_missing_file_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"provider": "file", "path": f"{tmp}/absent"}})
            with self.assertRaisesRegex(RuntimeError, "secret file not found"):
                cfg_secrets.SecretStore(root).resolve("k", label="a source")


class ProviderBackedSecretsTest(unittest.TestCase):
    def test_a_provider_that_does_not_implement_secrets_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"provider": "somecloud", "id": "x"}})
            with mock.patch.object(
                cfg_secrets.execution_providers, "get_provider_adapter",
                return_value=object(),
            ):
                with self.assertRaisesRegex(RuntimeError, "does not implement"):
                    cfg_secrets.SecretStore(root).resolve("k", label="a source")

    def test_the_adapter_receives_the_entry_without_the_provider_key(self):
        """The adapter validates its OWN vocabulary, so `provider` — the only
        field the engine understands — is not part of what it is handed."""
        seen = {}
        calls = {}

        class Adapter:
            @staticmethod
            def resolve_secret(options, **contract):
                seen.update(options)
                calls.update(contract)
                return "from-adapter"

        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(tmp, secrets={"k": {"provider": "somecloud", "id": "x", "key": "p"}})
            with mock.patch.object(
                cfg_secrets.execution_providers, "get_provider_adapter",
                return_value=Adapter,
            ):
                value = cfg_secrets.SecretStore(root).resolve("k", label="a source")
        self.assertEqual("from-adapter", value)
        self.assertEqual({"id": "x", "key": "p"}, seen)
        # the adapter reaches AWS, so it is given the run's identity — not left
        # to whatever credentials the process happens to hold
        self.assertEqual(
            {"ctl_cfg_root", "execution_context", "implementation_key",
             "execution_access_mode", "provider_options"},
            set(calls),
        )


class AdapterFetchSecretMustBePrimitiveTest(unittest.TestCase):
    """The cycle this model exists to prevent.

    An adapter is a FETCHED package and fetching needs a secret, so a secret
    resolved BY an adapter cannot be the secret that fetches one.
    """

    def test_a_primitive_secret_may_fetch_an_adapter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(
                tmp,
                secrets={"github_token": {"provider": "env", "name": "X"}},
                providers={"somecloud": {"implements": ["secrets"],
                                         "source": {"secret_key": "github_token"}}},
            )
            cfg_secrets.validate_declared_secrets(root)

    def test_a_provider_backed_secret_may_not_fetch_an_adapter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(
                tmp,
                secrets={"circular": {"provider": "somecloud", "id": "x"}},
                providers={"somecloud": {"implements": ["secrets"],
                                         "source": {"secret_key": "circular"}}},
            )
            with self.assertRaisesRegex(RuntimeError, "must resolve without one"):
                cfg_secrets.validate_declared_secrets(root)

    def test_an_adapter_source_naming_an_undeclared_secret_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(
                tmp,
                secrets={"github_token": {"provider": "env", "name": "X"}},
                providers={"somecloud": {"source": {"secret_key": "absent"}}},
            )
            with self.assertRaisesRegex(RuntimeError, "not declared"):
                cfg_secrets.validate_declared_secrets(root)


class TheRealCfgTreeIsValidTest(unittest.TestCase):
    """The consumer's own cfg, so a shape that only works in a fixture fails."""

    def test_the_dev_cfg_reflection_declares_resolvable_secrets(self):
        dev = Path(__file__).resolve().parents[3] / "cfg/oxygen/oxygen-ctl-cfg-dev"
        if not dev.is_dir():
            self.skipTest(f"dev cfg reflection not generated: {dev}")
        cfg_secrets.validate_declared_secrets(dev)
        self.assertIn("github_token", cfg_secrets.SecretStore(dev).declared)


if __name__ == "__main__":
    unittest.main()


class DeclaredContractsAreBackedTest(unittest.TestCase):
    """`implements:` was accepted and read by nobody.

    A provider could claim a contract its package did not satisfy, and the
    mismatch surfaced mid-run as an AttributeError — which reads as an engine bug
    rather than as the cfg error it is.
    """

    def test_the_real_cfg_declares_only_contracts_its_adapters_back(self):
        dev = Path(__file__).resolve().parents[3] / "cfg/oxygen/oxygen-ctl-cfg-dev"
        if not dev.is_dir():
            self.skipTest(f"dev cfg reflection not generated: {dev}")
        cfg_secrets.execution_providers.validate_declared_contracts(dev)

    def test_a_contract_with_no_callable_behind_it_is_refused(self):
        class Adapter:
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(
                tmp,
                secrets={"k": {"provider": "env", "name": "X"}},
                providers={"somecloud": {"implements": ["secrets"]}},
            )
            with mock.patch.object(
                cfg_secrets.execution_providers, "get_provider_adapter",
                return_value=Adapter,
            ):
                with self.assertRaisesRegex(RuntimeError, "resolve_secret"):
                    cfg_secrets.execution_providers.validate_declared_contracts(root)

    def test_an_unknown_contract_name_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(
                tmp,
                secrets={"k": {"provider": "env", "name": "X"}},
                providers={"somecloud": {"implements": ["teleportation"]}},
            )
            with self.assertRaisesRegex(RuntimeError, "unknown contracts"):
                cfg_secrets.execution_providers.validate_declared_contracts(root)

    def test_an_empty_implements_is_refused(self):
        """There is no default: a provider that implements nothing is a typo."""
        with tempfile.TemporaryDirectory() as tmp:
            root = cfg_root(
                tmp,
                secrets={"k": {"provider": "env", "name": "X"}},
                providers={"somecloud": {"implements": []}},
            )
            with self.assertRaisesRegex(RuntimeError, "non-empty `implements`"):
                cfg_secrets.execution_providers.validate_declared_contracts(root)
