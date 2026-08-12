"""Turning CLI and cfg text into values, and back.

Every one of these is total on its input or raises — none of them guesses. The
engine reads scalars from two untrusted places, argv and YAML, and this is
where that text stops being text."""

import argparse
import collections
from pathlib import Path


def str2bool(v: str) -> bool:
    """

    convert 'true'/'false' string to boolean."""

    if isinstance(v, str):
        value = v.lower()
        if value == "true":
            return True
        if value == "false":
            return False

    raise argparse.ArgumentTypeError(f"Expected 'true' or 'false', got: {type(v).__name__} ({v!r})")


def parse_relative_paths_arg(value: str, *, root_dir_name: str, item_label: str) -> list[str]:
    """

    parse comma-separated relative paths under a cfg root directory."""

    if value is None:
        return []

    raw = [v.strip() for v in value.split(",") if v.strip()]
    if not raw:
        return []
    if len(raw) == 1 and raw[0].lower() in ("none", "null", "-"):
        return []

    for item in raw:
        path = Path(item)
        if path.is_absolute():
            raise argparse.ArgumentTypeError(
                f"{item_label} path must be relative to {root_dir_name}/: {item}"
            )
        if ".." in path.parts:
            raise argparse.ArgumentTypeError(f"{item_label} path must not contain '..': {item}")

    duplicates = [item for item, count in collections.Counter(raw).items() if count > 1]
    if duplicates:
        raise argparse.ArgumentTypeError(
            f"{item_label} paths must be unique under {root_dir_name}/; duplicates: {', '.join(sorted(duplicates))}"
        )

    return raw


def _require_non_empty_string(value, label: str, path: Path | None = None) -> str:
    suffix = f": {path}" if path is not None else ""
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty string{suffix}")
    return value.strip()


def parse_comma_list(value: str) -> list[str]:
    """Comma-separated list arg, order-preserving and duplicate-free."""
    items: list[str] = []
    for raw in value.split(","):
        item = raw.strip()
        if not item:
            continue
        if item not in items:
            items.append(item)
    if not items:
        raise argparse.ArgumentTypeError(f"expected a comma-separated list, got: {value!r}")
    return items
