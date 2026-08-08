"""Filesystem paths and the hashes taken over them.

Path handling and content hashing sit together because they answer the same
question from two directions: WHERE a thing is, and WHETHER it is the same
thing as before."""

import hashlib
import json
import shutil

from pathlib import Path

def format_path_for_log(path: str | Path, relative_roots: tuple[Path, ...] = ()) -> str:
    """Prefer a relative display path when the path is under a known root.

    A materialized preset is shown by the IMPORT it came from, not by its scratch
    directory: the scratch is freed at the end of the discovery pass, so its path
    names nothing a reader could go and look at.
    """
    path_obj = Path(path).expanduser()
    if not path_obj.is_absolute():
        return str(path_obj)

    for workspace, import_path in _MATERIALIZED_IMPORT_LABELS.items():
        try:
            inside = path_obj.relative_to(workspace)
        except ValueError:
            continue
        return f"{import_path.rstrip('/')}/{inside}" if str(inside) != "." else import_path

    for root in relative_roots:
        try:
            return str(path_obj.relative_to(root))
        except ValueError:
            continue

    return str(path_obj)


# Materialized dir -> the cfg-absolute import path it was composed from, so a log
# line names the preset rather than a scratch path that will not exist afterwards
_MATERIALIZED_IMPORT_LABELS: dict[Path, str] = {}


def normalize_cfg_absolute_path(raw_value, *, label: str, allow_root: bool = False) -> str:
    """

    normalize a cfg-root absolute path used by plt metadata."""

    if not isinstance(raw_value, str) or not raw_value.strip():
        raise RuntimeError(f"{label} must be a non-empty string")
    value = raw_value.strip()
    if "\\" in value:
        raise RuntimeError(f"{label} must use forward slashes: {value}")
    if not value.startswith("/"):
        raise RuntimeError(f"{label} must start with /: {value}")

    parts = [part for part in value.split("/") if part]
    if any(part in (".", "..") for part in parts):
        raise RuntimeError(f"{label} must not contain . or ..: {value}")
    normalized = "/" + "/".join(parts)
    if normalized == "/" and not allow_root:
        raise RuntimeError(f"{label} must not be /")
    return normalized


def cfg_abs_path_to_dir(cfg_root: Path, abs_path: str, *, label: str) -> Path:
    """

    resolve a normalized cfg-root absolute path to a directory under cfg_root."""

    normalized = normalize_cfg_absolute_path(abs_path, label=label, allow_root=True)
    rel = normalized.lstrip("/")
    path = (cfg_root / rel).resolve() if rel else cfg_root.resolve()
    try:
        path.relative_to(cfg_root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"{label} escapes cfg root: {abs_path}") from exc
    return path


def canonical_sha256(value: object) -> str:
    """

    hash a JSON-compatible value with stable mapping-key ordering."""

    canonical = json.dumps(
        value, separators=(",", ":"), sort_keys=True, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def directory_content_sha256(path: Path) -> str:
    """

    hash a directory view from sorted relative paths and exact file bytes."""

    digest = hashlib.sha256()
    files = (
        sorted(item for item in Path(path).rglob("*") if item.is_file())
        if Path(path).is_dir()
        else []
    )
    for item in files:
        relative = item.relative_to(path).as_posix().encode("utf-8")
        content = item.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _newest(groups: dict) -> str:
    """

    the latest `time` across a row's groups; '' when none carries one."""

    return max((g.get("time") or "" for g in groups.values()), default="")


def _remove_path(path: Path) -> None:
    """

    remove an existing file, directory, or symlink."""

    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def ctl_utils_root() -> Path:
    """The atlas-ctl-utils checkout the running engine was imported from.

    Resolved by NAME rather than by counting `..` steps. A counted path still
    resolves to something that EXISTS once a module moves a directory deeper, so
    the failure surfaces far from its cause — which is what happened to every
    caller of this when the engine became a package.
    """
    for parent in Path(__file__).resolve().parents:
        if parent.name == "runners":
            return parent.parent
    raise RuntimeError(f"❌ engine is not installed under a 'runners' directory: {__file__}")
