#!/usr/bin/env python3
"""Render AWS CLI SSO profile configuration commands from a normalized model."""

import argparse
import json
import os
import re
import shlex
from pathlib import Path


ACCOUNT_ID_RE = re.compile(r"^\d{12}$")
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_json_mapping(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON file: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"expected mapping in JSON file: {path}")
    return value


def require_mapping(value, label: str) -> dict:
    if not isinstance(value, dict):
        raise RuntimeError(f"expected mapping for {label}")
    return value


def require_string(value, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"expected non-empty string for {label}")
    return value


def configure_set(path: str, value: str, *, raw: bool = False) -> str:
    rendered_value = value if raw else shlex.quote(value)
    return f"aws configure set {shlex.quote(path)} {rendered_value}"


def build_commands(model: dict) -> list[str]:
    sessions = require_mapping(model.get("sessions"), "sessions")
    profiles = require_mapping(model.get("profiles"), "profiles")
    if not sessions:
        raise RuntimeError("sessions is empty")
    if not profiles:
        raise RuntimeError("profiles is empty")

    normalized_sessions = {}
    session_names = set()
    start_url_env_names = set()
    for session_key in sorted(sessions):
        session = require_mapping(sessions[session_key], f"sessions.{session_key}")
        session_name = require_string(
            session.get("session_name"),
            f"sessions.{session_key}.session_name",
        )
        if session_name in session_names:
            raise RuntimeError(f"duplicate AWS SSO session name: {session_name}")
        session_names.add(session_name)

        start_url_env = require_string(
            session.get("start_url_env"),
            f"sessions.{session_key}.start_url_env",
        )
        if not ENV_NAME_RE.fullmatch(start_url_env):
            raise RuntimeError(
                f"invalid environment variable name for sessions.{session_key}.start_url_env: "
                f"{start_url_env}"
            )
        start_url_env_names.add(start_url_env)

        normalized_sessions[session_key] = {
            "session_name": session_name,
            "start_url_env": start_url_env,
            "region": require_string(
                session.get("region"),
                f"sessions.{session_key}.region",
            ),
            "registration_scopes": require_string(
                session.get("registration_scopes", "sso:account:access"),
                f"sessions.{session_key}.registration_scopes",
            ),
        }

    normalized_profiles = {}
    profile_names = set()
    for profile_key in sorted(profiles):
        profile = require_mapping(profiles[profile_key], f"profiles.{profile_key}")
        profile_name = require_string(
            profile.get("profile_name"),
            f"profiles.{profile_key}.profile_name",
        )
        if profile_name in profile_names:
            raise RuntimeError(f"duplicate AWS profile name: {profile_name}")
        profile_names.add(profile_name)

        session_key = require_string(
            profile.get("session_key"),
            f"profiles.{profile_key}.session_key",
        )
        if session_key not in normalized_sessions:
            raise RuntimeError(
                f"profiles.{profile_key}.session_key references missing session {session_key!r}"
            )

        account_id = require_string(
            profile.get("account_id"),
            f"profiles.{profile_key}.account_id",
        )
        if not ACCOUNT_ID_RE.fullmatch(account_id):
            raise RuntimeError(
                f"profiles.{profile_key}.account_id must be a 12-digit AWS account ID"
            )

        normalized_profiles[profile_key] = {
            "profile_name": profile_name,
            "session_key": session_key,
            "account_id": account_id,
            "role_name": require_string(
                profile.get("role_name"),
                f"profiles.{profile_key}.role_name",
            ),
            "region": require_string(
                profile.get("region"),
                f"profiles.{profile_key}.region",
            ),
        }

    lines = ["#!/usr/bin/env bash", "set -euo pipefail", ""]
    for env_name in sorted(start_url_env_names):
        lines.append(
            f': "${{{env_name}:?export {env_name} from the IAM Identity Center access portal first}}"'
        )

    lines.extend(["", "# SSO sessions"])
    for session_key in sorted(normalized_sessions):
        session = normalized_sessions[session_key]
        session_name = session["session_name"]
        start_url = f'"${{{session["start_url_env"]}}}"'
        lines.append(
            configure_set(
                f"sso-session.{session_name}.sso_start_url",
                start_url,
                raw=True,
            )
        )
        lines.append(
            configure_set(
                f"sso-session.{session_name}.sso_region",
                session["region"],
            )
        )
        lines.append(
            configure_set(
                f"sso-session.{session_name}.sso_registration_scopes",
                session["registration_scopes"],
            )
        )
        lines.append("")

    lines.append("# AWS profiles")
    for profile_key in sorted(normalized_profiles):
        profile = normalized_profiles[profile_key]
        profile_name = profile["profile_name"]
        session_name = normalized_sessions[profile["session_key"]]["session_name"]
        lines.append(configure_set(f"profile.{profile_name}.sso_session", session_name))
        lines.append(
            configure_set(f"profile.{profile_name}.sso_account_id", profile["account_id"])
        )
        lines.append(configure_set(f"profile.{profile_name}.sso_role_name", profile["role_name"]))
        lines.append(configure_set(f"profile.{profile_name}.region", profile["region"]))
        lines.append("")

    lines.append("# Login/verify examples")
    for profile_key in sorted(normalized_profiles):
        profile_name = normalized_profiles[profile_key]["profile_name"]
        lines.append(f"# aws sso login --profile {shlex.quote(profile_name)}")
        lines.append(f"# aws sts get-caller-identity --profile {shlex.quote(profile_name)}")
    return lines


def render_model(model: dict) -> str:
    return "\n".join(build_commands(model)) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render an executable AWS CLI SSO profile configuration script.",
    )
    parser.add_argument("--input-json", required=True, help="Normalized sessions/profiles JSON model")
    parser.add_argument("--output", help="Output script path; stdout when omitted")
    args = parser.parse_args()

    rendered = render_model(load_json_mapping(Path(args.input_json)))
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        os.chmod(output_path, 0o755)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
