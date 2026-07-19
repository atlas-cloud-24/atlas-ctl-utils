import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
renderer_spec = importlib.util.spec_from_file_location(
    "render_aws_sso_profiles",
    REPO_ROOT / "step_utils" / "providers" / "aws" / "render_sso_profiles.py",
)
renderer = importlib.util.module_from_spec(renderer_spec)
renderer_spec.loader.exec_module(renderer)


def profile_model() -> dict:
    return {
        "sessions": {
            "non_prod": {
                "session_name": "oxygen-non-prod",
                "start_url_env": "SSO_START_URL",
                "region": "eu-west-2",
            }
        },
        "profiles": {
            "dev_deploy": {
                "profile_name": "oxygen-live-dev-deploy",
                "session_key": "non_prod",
                "account_id": "111111111111",
                "role_name": "NonProdDeployAccess",
                "region": "eu-west-2",
            },
            "test_readonly": {
                "profile_name": "oxygen-live-test-readonly",
                "session_key": "non_prod",
                "account_id": "222222222222",
                "role_name": "NonProdReadOnlyAccess",
                "region": "eu-west-2",
            },
        },
    }


class AwsSsoProfileRendererTests(unittest.TestCase):
    def test_renders_one_shared_session_and_multiple_profiles(self):
        rendered = renderer.render_model(profile_model())

        self.assertEqual(rendered.count("sso_start_url"), 1)
        self.assertIn('"${SSO_START_URL}"', rendered)
        self.assertIn("profile.oxygen-live-dev-deploy.sso_account_id 111111111111", rendered)
        self.assertIn("profile.oxygen-live-test-readonly.sso_role_name NonProdReadOnlyAccess", rendered)

    def test_rejects_profile_with_missing_session(self):
        model = profile_model()
        model["profiles"]["dev_deploy"]["session_key"] = "missing"

        with self.assertRaisesRegex(RuntimeError, "references missing session"):
            renderer.render_model(model)

    def test_rejects_invalid_account_id(self):
        model = profile_model()
        model["profiles"]["dev_deploy"]["account_id"] = "123"

        with self.assertRaisesRegex(RuntimeError, "12-digit AWS account ID"):
            renderer.render_model(model)

    def test_cli_writes_an_executable_script(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "profiles.json"
            output_path = Path(tmp_dir) / "configure.sh"
            input_path.write_text(json.dumps(profile_model()), encoding="utf-8")

            with mock.patch(
                "sys.argv",
                [
                    "render_sso_profiles.py",
                    "--input-json",
                    str(input_path),
                    "--output",
                    str(output_path),
                ],
            ):
                self.assertEqual(renderer.main(), 0)

            self.assertTrue(output_path.read_text(encoding="utf-8").startswith("#!/usr/bin/env bash"))
            self.assertNotEqual(os.stat(output_path).st_mode & 0o111, 0)


if __name__ == "__main__":
    unittest.main()
