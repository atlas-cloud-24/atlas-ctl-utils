"""How the AWS adapter turns a declaration into credentials.

Atlas performs the exchange, never the login: it reads the proof `aws sso login`
left behind, trades it, and refuses when there is nothing left to trade.
"""

import json
import os
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

import atlas_ctl_adapter_aws as aws_adapter
from atlas_ctl_adapter_aws import credentials as credentials_module

START_URL = "https://example.awsapps.com/start"
SESSION = {"start_url": START_URL, "sso_region": "eu-west-2", "session_name": "example-main"}


def _stamp(**delta):
    return (datetime.now(UTC) + timedelta(**delta)).strftime("%Y-%m-%dT%H:%M:%SZ")


class SsoTokenTests(unittest.TestCase):
    """The proof is read from the CLI's cache, and renewed rather than re-logged."""

    def setUp(self):
        self.home = Path(TemporaryDirectory().name)
        self.cache = self.home / ".aws" / "sso" / "cache"
        self.cache.mkdir(parents=True)
        patcher = mock.patch.object(
            credentials_module.Path, "home", staticmethod(lambda: self.home)
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.calls = []

    def _cli(self, response):
        def run(cmd, env_extra=None):
            self.calls.append(cmd)
            return response

        return mock.patch.object(credentials_module, "_run_aws_json", run)

    def _write(self, name="session.json", **overrides):
        entry = {
            "startUrl": START_URL,
            "region": "eu-west-2",
            "accessToken": "OLD",
            "expiresAt": _stamp(hours=-1),
            "refreshToken": "RT-1",
            "clientId": "CID",
            "clientSecret": "CSEC",
            "registrationExpiresAt": _stamp(days=20),
        }
        entry.update(overrides)
        path = self.cache / name
        path.write_text(json.dumps(entry))
        return path

    def test_an_unexpired_token_is_used_and_nothing_is_called(self):
        self._write(accessToken="FRESH", expiresAt=_stamp(hours=5))
        with self._cli({}):
            token = credentials_module._sso_access_token(SESSION, label="t")
        self.assertEqual(token, "FRESH")
        self.assertEqual(self.calls, [])

    def test_an_expired_token_is_renewed_from_its_refresh_token(self):
        path = self._write()
        with self._cli({"accessToken": "NEW", "expiresIn": 28800, "refreshToken": "RT-2"}):
            token = credentials_module._sso_access_token(SESSION, label="t")
        self.assertEqual(token, "NEW")
        command = self.calls[-1]
        self.assertIn("sso-oidc", command)
        self.assertIn("create-token", command)
        self.assertEqual(command[command.index("--grant-type") + 1], "refresh_token")
        self.assertEqual(command[command.index("--refresh-token") + 1], "RT-1")

        # written back, because the refresh token ROTATES: keeping the new one
        # in memory leaves the copy on disk dead for every other aws command
        stored = json.loads(path.read_text())
        self.assertEqual(stored["accessToken"], "NEW")
        self.assertEqual(stored["refreshToken"], "RT-2")
        self.assertGreater(stored["expiresAt"], _stamp(hours=1))
        self.assertEqual(oct(path.stat().st_mode)[-3:], "600")

    def test_a_token_inside_the_skew_window_is_treated_as_expired(self):
        # the exchange that follows must still be inside the window when it lands
        self._write(expiresAt=_stamp(seconds=5))
        with self._cli({"accessToken": "NEW", "expiresIn": 28800}):
            credentials_module._sso_access_token(SESSION, label="t")
        self.assertEqual(len(self.calls), 1)

    def test_a_dead_registration_sends_the_operator_to_login(self):
        # past registrationExpiresAt the client secret is dead, so there is
        # nothing to renew WITH and create-token must not be attempted
        self._write(registrationExpiresAt=_stamp(days=-1))
        with self._cli({}), self.assertRaisesRegex(RuntimeError, "aws sso login"):
            credentials_module._sso_access_token(SESSION, label="t")
        self.assertEqual(self.calls, [])

    def test_no_refresh_token_sends_the_operator_to_login(self):
        self._write(refreshToken="")
        with self._cli({}), self.assertRaisesRegex(RuntimeError, "expired and cannot be renewed"):
            credentials_module._sso_access_token(SESSION, label="t")

    def test_no_cached_session_names_the_login_command(self):
        with self._cli({}), self.assertRaisesRegex(RuntimeError, "example-main"):
            credentials_module._sso_access_token(SESSION, label="t")

    def test_the_session_is_matched_by_start_url_not_by_filename(self):
        # a host holding two organizations holds two tokens; picking the first
        # file would pick an arbitrary organization
        self._write(
            "aaa.json",
            startUrl="https://other.awsapps.com/start",
            accessToken="OTHER",
            expiresAt=_stamp(hours=5),
        )
        self._write("zzz.json", accessToken="MINE", expiresAt=_stamp(hours=5))
        with self._cli({}):
            self.assertEqual(credentials_module._sso_access_token(SESSION, label="t"), "MINE")


class ForceBypassCredentialTests(unittest.TestCase):
    """Bypass means Atlas acquires nothing, so the operator states which credential."""

    ENV = {"AWS_ACCESS_KEY_ID": "ASIA1", "AWS_SECRET_ACCESS_KEY": "s", "AWS_SESSION_TOKEN": "t"}

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_neither_input_names_both(self):
        with self.assertRaisesRegex(RuntimeError, "force_bypass_profile.*force_bypass_env"):
            credentials_module.force_bypass_credentials({})

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_both_inputs_are_refused(self):
        with self.assertRaisesRegex(RuntimeError, "mutually exclusive"):
            credentials_module.force_bypass_credentials(
                {"force_bypass_profile": "p", "force_bypass_env": "true"}
            )

    @mock.patch.dict(os.environ, ENV, clear=True)
    def test_the_exported_triple_is_passed_through(self):
        self.assertEqual(
            credentials_module.force_bypass_credentials({"force_bypass_env": "true"}),
            self.ENV,
        )

    @mock.patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "ASIA1"}, clear=True)
    def test_a_partial_environment_names_what_is_missing(self):
        with self.assertRaisesRegex(RuntimeError, "AWS_SECRET_ACCESS_KEY"):
            credentials_module.force_bypass_credentials({"force_bypass_env": "true"})

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_a_profile_is_resolved_on_the_host_into_a_triple(self):
        # a step runs in a container with no ~/.aws, so a profile NAME crossing
        # that boundary would name nothing at all
        exported = {
            "Version": 1,
            "AccessKeyId": "ASIA9",
            "SecretAccessKey": "sec",
            "SessionToken": "tok",
            "Expiration": _stamp(hours=1),
        }
        with mock.patch.object(credentials_module, "_run_aws_json", return_value=exported) as run:
            credential = credentials_module.force_bypass_credentials(
                {"force_bypass_profile": "sandbox"}
            )
        self.assertEqual(
            credential,
            {
                "AWS_ACCESS_KEY_ID": "ASIA9",
                "AWS_SECRET_ACCESS_KEY": "sec",
                "AWS_SESSION_TOKEN": "tok",
            },
        )
        self.assertEqual(
            run.call_args.args[0],
            [
                "aws",
                "configure",
                "export-credentials",
                "--profile",
                "sandbox",
                "--format",
                "process",
            ],
        )

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_a_profile_exporting_nothing_is_an_error_not_an_empty_credential(self):
        with (
            mock.patch.object(credentials_module, "_run_aws_json", return_value={}),
            self.assertRaisesRegex(RuntimeError, "exported no AWS_ACCESS_KEY_ID"),
        ):
            credentials_module.force_bypass_credentials({"force_bypass_profile": "sandbox"})


class AcquisitionVocabularyTests(unittest.TestCase):
    """An entry acquires; a hop continues. Only one of them can start a run."""

    def test_a_hop_is_not_an_acquisition_and_both_are_source_kinds(self):
        self.assertNotIn("assume_role", aws_adapter.CREDENTIAL_ACQUISITIONS)
        self.assertIn("assume_role", aws_adapter.CREDENTIAL_HOPS)
        self.assertEqual(
            set(aws_adapter.CREDENTIAL_SOURCE_KINDS),
            set(aws_adapter.CREDENTIAL_ACQUISITIONS) | set(aws_adapter.CREDENTIAL_HOPS),
        )

    def test_every_unimplemented_acquisition_is_a_declared_one(self):
        self.assertTrue(
            set(aws_adapter.UNIMPLEMENTED_CREDENTIAL_ACQUISITIONS)
            <= set(aws_adapter.CREDENTIAL_ACQUISITIONS)
        )

    def test_each_kind_validates_its_own_required_fields(self):
        cases = [
            ("sso", {"session_key": "s", "account_key": "a", "permission_set_name": "P"}, None),
            ("sso", {"session_key": "s"}, "account_key"),
            ("assume_role", {"from": "other", "role_name": "r"}, None),
            ("assume_role", {"role_name": "r"}, "from"),
            ("atmos", {"identity": "dev-admin"}, None),
            ("atmos", {}, "identity"),
            ("atmos", {"identity": "d", "bogus": "x"}, "unknown fields"),
        ]
        for kind, cfg, expected in cases:
            with self.subTest(kind=kind, cfg=cfg):
                if expected is None:
                    credentials_module._validate_aws_credential_source_implementation(
                        "entry", kind, cfg, Path("/cfg")
                    )
                else:
                    with self.assertRaisesRegex(RuntimeError, expected):
                        credentials_module._validate_aws_credential_source_implementation(
                            "entry", kind, cfg, Path("/cfg")
                        )

    def test_an_unknown_kind_names_the_ones_that_exist(self):
        with self.assertRaisesRegex(RuntimeError, "expected one of"):
            credentials_module._validate_aws_credential_source_implementation(
                "entry", "profile", {}, Path("/cfg")
            )


if __name__ == "__main__":
    unittest.main()
