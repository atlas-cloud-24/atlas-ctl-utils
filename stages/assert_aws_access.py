#!/usr/bin/env python3
"""Assert that the active AWS credentials match the target account and access context."""

import json
import os
import re
import subprocess


ASSUMED_ROLE_ARN_RE = re.compile(
    r"^arn:[^:]+:sts::(?P<account_id>\d{12}):assumed-role/"
    r"(?P<role_name>[^/]+)/(?P<session_name>[^/]+)$"
)


def validate_caller_identity(
    caller: dict,
    *,
    expected_account_id: str,
    expected_permission_set_name: str | None = None,
    expected_role_name: str | None = None,
) -> tuple[str, str]:
    if bool(expected_permission_set_name) == bool(expected_role_name):
        raise RuntimeError(
            "exactly one expected permission-set name or role name is required"
        )

    actual_account_id = caller.get("Account")
    actual_arn = caller.get("Arn")
    if actual_account_id != expected_account_id:
        raise RuntimeError(
            f"AWS account mismatch: expected {expected_account_id}, got {actual_account_id}"
        )
    if not isinstance(actual_arn, str):
        raise RuntimeError("STS GetCallerIdentity returned no Arn")

    match = ASSUMED_ROLE_ARN_RE.fullmatch(actual_arn)
    if not match:
        raise RuntimeError(f"AWS principal is not an assumed-role ARN: {actual_arn}")
    if match.group("account_id") != expected_account_id:
        raise RuntimeError(
            f"AWS ARN account mismatch: expected {expected_account_id}, "
            f"got {match.group('account_id')}"
        )

    actual_role_name = match.group("role_name")
    if expected_permission_set_name:
        pattern = re.compile(
            rf"^AWSReservedSSO_{re.escape(expected_permission_set_name)}_[^/]+$"
        )
        if not pattern.fullmatch(actual_role_name):
            raise RuntimeError(
                "AWS SSO permission-set mismatch: expected role "
                f"AWSReservedSSO_{expected_permission_set_name}_<suffix>, "
                f"got {actual_role_name}"
            )
    elif actual_role_name != expected_role_name:
        raise RuntimeError(
            f"AWS role mismatch: expected {expected_role_name}, got {actual_role_name}"
        )

    return actual_account_id, actual_arn


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is empty: {name}")
    return value


def main() -> int:
    profile_name = require_env("AWS_PROFILE")
    expected_account_id = require_env("ATLAS_AWS_EXPECT_ACCOUNT_ID")
    permission_set_name = os.getenv("ATLAS_AWS_EXPECT_PERMISSION_SET_NAME", "").strip() or None
    role_name = os.getenv("ATLAS_AWS_EXPECT_ROLE_NAME", "").strip() or None

    result = subprocess.run(
        [
            "aws",
            "sts",
            "get-caller-identity",
            "--profile",
            profile_name,
            "--output",
            "json",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"AWS access assertion failed for profile {profile_name!r}: {detail}"
        )

    try:
        caller = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("STS GetCallerIdentity returned invalid JSON") from exc

    actual_account_id, actual_arn = validate_caller_identity(
        caller,
        expected_account_id=expected_account_id,
        expected_permission_set_name=permission_set_name,
        expected_role_name=role_name,
    )
    print(
        "AWS access: "
        f"account_key={os.getenv('ATLAS_AWS_ACCOUNT_KEY', '')} "
        f"access_context_key={os.getenv('ATLAS_AWS_ACCESS_CONTEXT_KEY', '')} "
        f"implementation_key={os.getenv('ATLAS_AWS_IMPLEMENTATION_KEY', '')} "
        f"profile={profile_name} account={actual_account_id} arn={actual_arn}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
