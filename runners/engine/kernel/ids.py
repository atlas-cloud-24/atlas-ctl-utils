"""Identifiers and timestamps that runs are recorded under.

UUIDv7 rather than v4 because a run id is sorted by time constantly — in
listings, in status, in the state tree — and a v4 would force every one of
those to read a timestamp field it would rather not need."""

import argparse
import time
import uuid

from datetime import UTC, datetime

SERVICE_ID = "atlas-ctl-orchestrator-local"


_UUID7_LAST_TIMESTAMP_MS = -1


_UUID7_COUNTER = 0


def validate_uuid7(v: str) -> str:
    """Validate that a string is a valid UUID version 7."""
    try:
        parsed = uuid.UUID(v)
        if parsed.version != 7:
            raise argparse.ArgumentTypeError(f"UUID must be version 7, got version {parsed.version}: {v}")
        return v
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid UUID format: {v}")


def _uuid7_timestamp_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000) & ((1 << 48) - 1)


def generate_uuid7() -> str:
    """

    generate a monotonic UUIDv7 string for one ctl run execution."""

    global _UUID7_LAST_TIMESTAMP_MS, _UUID7_COUNTER

    timestamp_ms = _uuid7_timestamp_ms()
    if timestamp_ms > _UUID7_LAST_TIMESTAMP_MS:
        _UUID7_LAST_TIMESTAMP_MS = timestamp_ms
        _UUID7_COUNTER = 0
    else:
        timestamp_ms = _UUID7_LAST_TIMESTAMP_MS
        _UUID7_COUNTER += 1
        if _UUID7_COUNTER >= (1 << 12):
            while timestamp_ms <= _UUID7_LAST_TIMESTAMP_MS:
                time.sleep(0.001)
                timestamp_ms = _uuid7_timestamp_ms()
            _UUID7_LAST_TIMESTAMP_MS = timestamp_ms
            _UUID7_COUNTER = 0

    rand_a = _UUID7_COUNTER
    rand_b = uuid.uuid4().int & ((1 << 62) - 1)
    value = (timestamp_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return str(uuid.UUID(int=value))


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _uuid7_datetime(run_id: str) -> datetime | None:
    try:
        parsed = uuid.UUID(run_id)
    except (ValueError, AttributeError):
        return None
    if parsed.version != 7:
        return None
    timestamp_ms = parsed.int >> 80
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
