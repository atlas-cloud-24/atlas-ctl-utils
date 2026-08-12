"""What a git ref is, and getting the repository it names.

The engine pins sources by commit, so these are the only places that turn a
declared ref into a checkout — and the only places that report back what was
actually checked out."""

import argparse
import hashlib
import logging
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import yaml

from engine.kernel import process as kernel_process


def git_clone(
    repo_url: str, branch: str | None, commit: str | None, dest: Path, token: str | None = None
):
    env = os.environ.copy()
    askpass_path: str | None = None
    if token:
        fd, askpass_path = tempfile.mkstemp(suffix=".sh")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(
                    "#!/bin/sh\n"
                    'case "$1" in\n'
                    "  *Username*) printf '%s\\n' \"${GIT_HTTP_USERNAME:-x-access-token}\" ;;\n"
                    "  *Password*) printf '%s\\n' \"${GIT_HTTP_PASSWORD:-}\" ;;\n"
                    "  *) printf '\\n' ;;\n"
                    "esac\n"
                )
            os.chmod(askpass_path, 0o700)
        except Exception:
            os.unlink(askpass_path)
            raise

        env["GIT_ASKPASS"] = askpass_path
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_HTTP_USERNAME"] = "x-access-token"
        env["GIT_HTTP_PASSWORD"] = token

    try:
        # Commit pinned → checkout exact commit
        if commit:
            cmd = ["git", "clone", repo_url, str(dest)]
            logging.info(f"Running command: git clone {repo_url} {dest}")
            kernel_process.run_and_log(cmd, env=env)

            cmd = f"git checkout {commit}"
            logging.info(f"Running command: {cmd}")
            kernel_process.run_and_log(cmd.split(), cwd=dest, env=env)
            return

        # No commit → use branch HEAD
        if not branch:
            raise RuntimeError(f"❌ Either branch or commit must be provided for repo {repo_url}")

        cmd = ["git", "clone", "--branch", branch, "--depth", "1", repo_url, str(dest)]
        logging.info(f"Running command: git clone --branch {branch} --depth 1 {repo_url} {dest}")
        kernel_process.run_and_log(cmd, env=env)
    finally:
        if askpass_path:
            os.unlink(askpass_path)


def parse_repo_url_ref(value: str) -> tuple[str, str | None, str | None]:
    """Parse URL@branch=name or URL@commit=sha format into (url, branch, commit).

    Examples:
        https://github.com/org/repo@branch=main -> (url, "main", None)
        https://github.com/org/repo@commit=abc123 -> (url, None, "abc123")

    Returns:
        tuple: (repo_url, branch, commit) where one of branch/commit is None
    """

    if "@" not in value:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Expected URL@branch=name or URL@commit=sha"
        )

    # Split on last @ to handle URLs that might contain @
    idx = value.rfind("@")
    repo_url = value[:idx]
    ref_part = value[idx + 1 :]

    if not repo_url or not ref_part:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Both URL and ref are required."
        )

    parsed = urlparse(repo_url)
    if not parsed.scheme or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Remote cfg must use URL@branch=name or URL@commit=sha"
        )

    if ref_part.startswith("branch="):
        branch = ref_part[7:]  # len("branch=") = 7
        if not branch:
            raise argparse.ArgumentTypeError(
                f"Invalid format: '{value}'. Branch name cannot be empty."
            )
        return repo_url, branch, None
    elif ref_part.startswith("commit="):
        commit = ref_part[7:]  # len("commit=") = 7
        if not commit:
            raise argparse.ArgumentTypeError(
                f"Invalid format: '{value}'. Commit sha cannot be empty."
            )
        return repo_url, None, commit
    else:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{value}'. Expected @branch=name or @commit=sha"
        )


def git_source_facts(path: Path) -> tuple[str | None, str]:
    """

    return the checked-out commit and reproducibility state of one cfg source."""

    root = Path(path)
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
    )
    if commit.returncode != 0:
        return None, "dirty"
    status = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"],
        capture_output=True,
        text=True,
    )
    state = "clean" if status.returncode == 0 and not status.stdout.strip() else "dirty"
    return commit.stdout.strip(), state


log = logging.getLogger(__name__)


def _run_git(git_dir: Path, *args: str) -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "-C", str(git_dir), *args],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return out.strip()
    except Exception:
        return None


def get_repo_url_safe(git_dir: str | Path) -> str | None:
    git_dir = Path(git_dir)
    raw_url = _run_git(git_dir, "remote", "get-url", "origin")
    if not raw_url:
        return None

    if raw_url.startswith("http://") or raw_url.startswith("https://"):
        scheme, rest = raw_url.split("://", 1)
        if "@" in rest:
            rest = rest.split("@", 1)[1]
        return f"{scheme}://{rest}"

    return raw_url


def get_git_meta(git_dir: str | Path, generator: str = None) -> dict:
    git_dir = Path(git_dir)

    repo_url = get_repo_url_safe(git_dir)
    branch = _run_git(git_dir, "rev-parse", "--abbrev-ref", "HEAD")
    commit = _run_git(git_dir, "rev-parse", "HEAD")

    try:
        hasher = hashlib.sha256()
        for p in sorted(git_dir.rglob("*")):
            if p.is_file():
                hasher.update(str(p.relative_to(git_dir)).encode("utf-8"))
                hasher.update(p.read_bytes())
        dir_hash = hasher.hexdigest()
    except Exception as e:
        log.warning("Failed to compute dir hash for %s: %s", git_dir, e)
        dir_hash = None

    return {
        "repo_url": repo_url,
        "branch": branch,
        "commit": commit,
        "dir_hash": dir_hash,
        "generated_at": datetime.now(UTC).replace(tzinfo=None).isoformat() + "Z",
        "generator": generator,
    }


def write_git_meta_to_file(
    git_dir: str | Path,
    dest_dir: str | Path,
    filename: str,
    generator: str,
) -> Path | None:
    git_dir = Path(git_dir)

    meta = get_git_meta(git_dir, generator)
    if not meta:
        log.warning("No git detected for %s - %s will not be written", git_dir, filename)
        return None

    dest = Path(dest_dir) if dest_dir is not None else git_dir
    dest.mkdir(parents=True, exist_ok=True)

    git_meta_path = dest / filename
    git_meta_path.write_text(yaml.safe_dump(meta, sort_keys=False))

    return git_meta_path
