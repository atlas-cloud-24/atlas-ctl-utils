import subprocess
import hashlib
import datetime
import logging
from pathlib import Path

import yaml

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
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
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
