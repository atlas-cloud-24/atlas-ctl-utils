#!/usr/bin/env python3
"""Temporarily comment target execution_identity_key entries in a ctl cfg tree.

This is a bootstrap/local-dev escape hatch. It lets ctl fall back to explicit
--aws-profile for targets whose normal execution identities cannot be validated yet.
Use --restore after the org/account/profile cfg is ready.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


MARKER = "atlas-tmp-profile-only"
IDENTITY_RE = re.compile(r"^(?P<indent>\s*)execution_identity_key:\s*(?P<value>.+?)\s*$")
RESTORE_RE = re.compile(
    rf"^(?P<indent>\s*)# {MARKER}: execution_identity_key:\s*(?P<value>.+?)\s*$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Comment or restore execution_identity_key lines under targets/**/*.yaml "
            "so local_dev can temporarily fall back to explicit --aws-profile."
        )
    )
    parser.add_argument(
        "ctl_cfg",
        help="Ctl cfg root, for example cfg/oxygen/oxygen-ctl-cfg-dev.",
    )
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Restore lines previously commented by this script.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print files that would change without writing them.",
    )
    return parser.parse_args()


def target_files(ctl_cfg: Path) -> list[Path]:
    """Files that may carry execution_identity_key: targets/**, plus any yaml
    defining the ctl_state_backends registry (or legacy ctl_state_buckets; the writer identities)."""
    targets_root = ctl_cfg / "targets"
    if not targets_root.is_dir():
        raise RuntimeError(f"targets directory not found: {targets_root}")
    files = sorted(targets_root.rglob("*.yaml"))
    for path in sorted(ctl_cfg.rglob("*.yaml")):
        if targets_root in path.parents:
            continue
        if re.search(r"^ctl_state_(?:backends|buckets):\s*$", path.read_text(encoding="utf-8"), re.MULTILINE):
            files.append(path)
    return files


def comment_identities(text: str) -> tuple[str, int]:
    changed = 0
    output: list[str] = []
    for line in text.splitlines():
        if MARKER in line:
            output.append(line)
            continue
        match = IDENTITY_RE.match(line)
        if match:
            output.append(
                f"{match.group('indent')}# {MARKER}: execution_identity_key: {match.group('value')}"
            )
            changed += 1
        else:
            output.append(line)
    return "\n".join(output) + ("\n" if text.endswith("\n") else ""), changed


def restore_identities(text: str) -> tuple[str, int]:
    changed = 0
    output: list[str] = []
    for line in text.splitlines():
        match = RESTORE_RE.match(line)
        if match:
            output.append(f"{match.group('indent')}execution_identity_key: {match.group('value')}")
            changed += 1
        else:
            output.append(line)
    return "\n".join(output) + ("\n" if text.endswith("\n") else ""), changed


def main() -> int:
    args = parse_args()
    ctl_cfg = Path(args.ctl_cfg).expanduser().resolve()
    if not ctl_cfg.is_dir():
        raise RuntimeError(f"ctl cfg directory not found: {ctl_cfg}")

    total = 0
    for path in target_files(ctl_cfg):
        original = path.read_text(encoding="utf-8")
        updated, changed = restore_identities(original) if args.restore else comment_identities(original)
        if changed == 0:
            continue
        total += changed
        rel = path.relative_to(ctl_cfg)
        action = "restore" if args.restore else "comment"
        print(f"{action}: {rel} ({changed})")
        if not args.dry_run:
            path.write_text(updated, encoding="utf-8")

    mode = "would change" if args.dry_run else "changed"
    print(f"{mode}: {total} execution_identity_key line(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
