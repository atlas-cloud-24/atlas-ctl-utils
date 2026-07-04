#!/usr/bin/env python3
"""Temporarily comment target ctl_state_bucket_key entries in a ctl cfg tree.

This is the second condition of the ctl-state sync-skip triad (mirroring the
profile-only identity hatch): with the keys absent on all active targets, a
profile that sets allow_skip_ctl_state_bucket_sync: true may run with
--skip-ctl-state-bucket-sync. Use --restore to reinstate the keys.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


MARKER = "atlas-tmp-no-ctl-state"
KEY_RE = re.compile(r"^(?P<indent>\s*)ctl_state_bucket_key:\s*(?P<value>.+?)\s*$")
RESTORE_RE = re.compile(
    rf"^(?P<indent>\s*)# {MARKER}: ctl_state_bucket_key:\s*(?P<value>.+?)\s*$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Comment or restore ctl_state_bucket_key lines under targets/**/*.yaml "
            "so local_dev can run with --skip-ctl-state-bucket-sync."
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
    targets_root = ctl_cfg / "targets"
    if not targets_root.is_dir():
        raise RuntimeError(f"targets directory not found: {targets_root}")
    return sorted(targets_root.rglob("*.yaml"))


def comment_keys(text: str) -> tuple[str, int]:
    changed = 0
    output: list[str] = []
    for line in text.splitlines():
        if MARKER in line:
            output.append(line)
            continue
        match = KEY_RE.match(line)
        if match:
            output.append(
                f"{match.group('indent')}# {MARKER}: ctl_state_bucket_key: {match.group('value')}"
            )
            changed += 1
        else:
            output.append(line)
    return "\n".join(output) + ("\n" if text.endswith("\n") else ""), changed


def restore_keys(text: str) -> tuple[str, int]:
    changed = 0
    output: list[str] = []
    for line in text.splitlines():
        match = RESTORE_RE.match(line)
        if match:
            output.append(f"{match.group('indent')}ctl_state_bucket_key: {match.group('value')}")
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
        updated, changed = restore_keys(original) if args.restore else comment_keys(original)
        if changed == 0:
            continue
        total += changed
        rel = path.relative_to(ctl_cfg)
        action = "restore" if args.restore else "comment"
        print(f"{action}: {rel} ({changed})")
        if not args.dry_run:
            path.write_text(updated, encoding="utf-8")

    mode = "would change" if args.dry_run else "changed"
    print(f"{mode}: {total} ctl_state_bucket_key line(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
