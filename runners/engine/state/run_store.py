"""Run records, locks and state slots as they sit on disk.

Storage only. Deciding WHEN a record changes is the lifecycle's job and reading
records to reach a verdict is status's; keeping those out means both can depend
on this module without depending on each other."""

import argparse
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import shutil
import socket
import uuid

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from engine.kernel import git as kernel_git
from engine.kernel import ids as kernel_ids
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing

CTL_RESULTS_LOCK_FILENAME = ".ctl.lock"


CTL_RESULTS_LOCK_META_FILENAME = ".ctl.lock.yaml"


RUN_METADATA_FILENAME = "RUN.yaml"


class RunStatus(StrEnum):
    """The `status:` a run record carries.

    A record states what the RUN did. `outdated` is the one value no run writes
    about itself — a later mutation writes it onto an already-committed pointer —
    which is why it belongs to the record rather than to the run's own lifecycle.
    """

    IN_PROGRESS = "in_progress"
    OK = "ok"
    FAILED = "failed"
    OUTDATED = "outdated"


class StateSlot(StrEnum):
    """An OPEN run's mark on an instance: one directory per slot.

    A slot is not a status value even where the two spell the same word: it is a
    place, cleared by the run that ends. `in_progress` present and `failed`
    absent is a fact about the instance, not about any record.
    """

    IN_PROGRESS = "in_progress"
    FAILED = "failed"


# Reserved local-only ctl-state root — never synced, never a locator.
# Locator segments must start alphanumeric, so "_local" cannot collide.
LOCAL_ONLY_LOCATOR = ("_local",)


# The build workspace (repo checkout + provider cache) lives under the
# reserved never-synced `_local` tree, NOT inside a run prefix — a run prefix is
# published to the backend and `s3 sync` has no --delete, so anything that lands
# there is permanent.
LOCAL_WORKSPACES_DIRNAME = "workspaces"


# What a published run prefix contains. An ALLOWLIST, never an exclude
# list: publication is irreversible, so anything new under a run dir must default
# to NOT published. The engine decides WHAT a record is; the adapter uploads it.
RUN_RECORD_MEMBERS = (
    "RUN.yaml",
    "source_refs.yaml",
    "gates",
    "logs",
    "artifacts",
    "execution",
    "cfg",
)


@contextlib.contextmanager
def target_run_log(child_run_dir: Path | None):
    """A target run writes its OWN log while it executes.

    Implemented as a SECOND file handler rather than by redirecting: the workflow
    keeps the aggregate of everything (the operator's single reading surface) and
    the target gets its own copy, which is what makes a target run independently
    inspectable. Duplication is deliberate.
    """

    if child_run_dir is None:
        yield None
        return
    logs_dir = child_run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{kernel_ids.SERVICE_ID}_{child_run_dir.name}.log"
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(handler)
    try:
        yield log_path
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()


def ctl_state_dir_from_run_dir(run_dir: Path) -> Path:
    if run_dir.parent.name != "runs":
        raise RuntimeError(f"run_dir must be under a runs/ directory: {run_dir}")
    return run_dir.parent.parent


def run_metadata_path(run_dir: Path) -> Path:
    return Path(run_dir) / RUN_METADATA_FILENAME


def load_run_metadata(run_dir: Path) -> dict:
    path = run_metadata_path(run_dir)
    if not path.is_file():
        return {"run_id": Path(run_dir).name}
    data = kernel_yaml_io.load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ run metadata must be a mapping: {path}")
    data.setdefault("run_id", Path(run_dir).name)
    return data


def write_run_metadata(run_dir: Path, metadata: dict) -> None:
    kernel_yaml_io.write_yaml_file(run_metadata_path(run_dir), metadata)


def update_run_metadata(run_dir: Path, updates: dict) -> dict:
    metadata = load_run_metadata(run_dir)
    metadata.update(updates)
    write_run_metadata(run_dir, metadata)
    return metadata


def current_status_path(run_dir: Path) -> Path:
    # §consolidated: status is merged INTO RUN.yaml (was a separate STATUS.yaml).
    # RUN.yaml is written at start (in_progress) and updated with the outcome.
    return Path(run_dir) / RUN_METADATA_FILENAME


def load_current_status(run_dir: Path) -> dict:
    path = current_status_path(run_dir)
    if not path.is_file():
        return {}
    data = kernel_yaml_io.load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ run STATUS.yaml must be a mapping: {path}")
    return data


def write_current_status(run_dir: Path, payload: dict) -> None:
    kernel_yaml_io.write_yaml_file(current_status_path(run_dir), payload)


def state_slot_dir(instance_dir: Path, state: StateSlot, group: str) -> Path:
    """`<state>/<group>/` — group-scoped for the same reason pointers are: a failed
    plan and a failed deployment on one instance must not collide."""

    return Path(instance_dir) / state / group


STATE_SLOT_NAMES = tuple(StateSlot)


def run_state_group(run_dir: Path, action: str | None = None) -> str:
    """The group THIS run's state slot belongs to.

    A workflow's group is DERIVED from its members and recorded on the
    run, so a composition that plans one member and provisions another publishes
    its state under one group — the same one its status row reports. Everything
    else publishes under its own action's group.

    The recorded group wins because it is the more informed answer: it exists only
    once the composition is resolved, and until then `--action` is the best a run
    can say about itself.
    """

    metadata = load_run_metadata(run_dir)
    recorded = metadata.get("group")
    if recorded:
        return str(recorded)
    return run_actions.action_group(str(action or metadata.get("action")))


def move_state_slots_to_group(run_dir: Path, group: str) -> None:
    """Re-home this run's open state slots when its group becomes known.

    `mark_run_started` writes `in_progress` before a workflow's members are
    resolved, so the slot lands under the run action's group and the derived group
    can differ. Without the move the run finishes and looks for its slot in the new
    group, leaves the old one behind, and the instance reports an in-progress run
    that ended — so the slot follows the group rather than the group being
    weakened to whatever was knowable first.

    Slots are found by SEARCHING for this run's own, never by recomputing where it
    must have put them: an instance holds slots from other runs, and the group a
    slot was written under depends on what was known at the time.
    """
    instance_dir = ctl_state_dir_from_run_dir(run_dir)
    run_path = f"runs/{Path(run_dir).name}"
    for state in STATE_SLOT_NAMES:
        for candidate in sorted(run_actions.GROUP_BY_ACTION.values()):
            if candidate == group:
                continue
            source = state_slot_dir(instance_dir, state, candidate)
            status = source / "STATUS.yaml"
            if not status.is_file():
                continue
            if (kernel_yaml_io.load_yaml(status) or {}).get("run_path") != run_path:
                continue                # another run's slot in the same instance
            destination = state_slot_dir(instance_dir, state, group)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                shutil.rmtree(destination)
            shutil.move(str(source), str(destination))


def remove_state_slot(run_dir: Path, state: StateSlot) -> None:
    group = run_state_group(run_dir)
    slot_dir = state_slot_dir(ctl_state_dir_from_run_dir(run_dir), state, group)
    if slot_dir.exists():
        shutil.rmtree(slot_dir)


def run_workspace_dir(run_dir: Path) -> Path | None:
    """This run's build workspace root.

    `<ctl_state_local_root>/_local/workspaces/<run_id>/` — outside every run
    prefix, so no sync can reach it whatever it publishes. Returns None when the
    run has no recorded local root yet (a failure before RUN.yaml exists).
    """
    local_root = load_run_metadata(run_dir).get("ctl_state_local_root")
    if not local_root:
        return None
    return (
        Path(local_root)
        .joinpath(*LOCAL_ONLY_LOCATOR)
        / LOCAL_WORKSPACES_DIRNAME
        / Path(run_dir).name
    )


def cleanup_run_workspace(run_dir: Path) -> None:
    """

    drop the run's build workspace (the materialized repo checkout +
    provider cache). It is used only DURING the run and is fully
    reproducible from the pinned source_commit/cfg_source_commit in RUN.yaml;
    nothing reads it after the run."""

    workspace = run_workspace_dir(run_dir)
    if workspace is not None and workspace.exists():
        shutil.rmtree(workspace, ignore_errors=True)


def write_state_slot(run_dir: Path, state: StateSlot, payload: dict) -> None:
    group = run_state_group(run_dir, payload.get("action"))
    slot_dir = state_slot_dir(ctl_state_dir_from_run_dir(run_dir), state, group)
    slot_payload = dict(payload)
    slot_payload["state_slot"] = state
    slot_payload["run_path"] = f"runs/{run_dir.name}"
    kernel_yaml_io.write_yaml_file(slot_dir / "STATUS.yaml", slot_payload)
    kernel_yaml_io.write_yaml_file(
        slot_dir / "MANIFEST.yaml",
        {
            "run_id": run_dir.name,
            "run_path": f"runs/{run_dir.name}",
            "status_path": f"runs/{run_dir.name}/RUN.yaml",
            "artifacts_path": f"runs/{run_dir.name}/artifacts",
            "updated_at": slot_payload["updated_at"],
        },
    )


# Facts denormalized onto committed.yaml so readers (outdate/status) need no
# second file open; the pointer is still the single publication object.
# Only facts NOT derivable from the pointer's own path/run_id: everything
# else (action, run_type, result name/key, instance segments/address,
# target_keys/addresses) is encoded in the instance dir path or duplicated in
# child_revisions — never denormalized into the pointer.
_COMMITTED_FACT_KEYS = (
    # `action` is recorded because a pointer must say which direction
    # published it. Without it, reuse eligibility could not compare the action and
    # A workflow holding one target under two actions skipped both members.
    "action",
    "child_revisions", "source_commit", "cfg_source_commit",
    "source_state", "ref_policy", "workflow_definition_sha256",
    "target_definition_sha256", "target_cfg_view_sha256",
    # What this run APPLIED. A mode group's members share one deployment, so the
    # member in effect is the one whose overlays the deployment carries — without
    # this the others cannot know they were replaced.
    "plt_overlays",
    # Which INVOCATION published this revision. Targets come from different
    # source repositories, so no commit says which of them were one deployment;
    # this does, and it is denormalized here for the same reason the rest are —
    # a status read opens the pointer and nothing else.
    # METADATA, NEVER IDENTITY: it is deliberately absent from the `expected`
    # comparison in up_to_date_child_revision, so re-running the same deployment
    # under a different label still reuses the committed result rather than
    # materializing a new instance and resetting staleness.
    "label",
)


def committed_pointer_path(instance_dir: Path, group: str) -> Path:
    """`committed/<group>.yaml` — one published pointer per status group.

    A directory rather than one action-keyed file so publication stays atomic per
    group: a deployment run writes one file and touches nothing else."""

    if group not in run_actions.RESULT_GROUPS:
        raise RuntimeError(f"❌ unknown state group {group!r} (expected {run_actions.RESULT_GROUPS})")
    return Path(instance_dir) / "committed" / f"{group}.yaml"


def write_run_snapshot(run_dir: Path, payload: dict) -> str:
    """§consolidated: the run's RUN.yaml IS the snapshot — no separate
    snapshot.yaml. Write the frozen record (this run's payload) to RUN.yaml and
    return its sha256. Self-contained (does not assume write_current_status ran):
    RUN.yaml on disk always equals the hashed content, and the skip-up-to-date
    check re-reads RUN.yaml and verifies this digest. RUN.yaml must not change
    after commit or that verification breaks."""
    kernel_yaml_io.write_yaml_file(Path(run_dir) / RUN_METADATA_FILENAME, payload)
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def publish_committed_pointer(run_dir: Path, payload: dict) -> Path | None:
    """Publish the instance's committed.yaml pointer to this run's snapshot
. Writes the snapshot first (manifest-last), then the pointer
    with the denormalized facts + status readers need. The physical
    conditional write to the backend is the syncer's job; locally this is the
    authoritative record."""
    # Only a state owner publishes. A workflow owns execution, so its
    # RUN.yaml IS the record — persistent composition status would be a claim
    # nobody re-checks, because ctl never observes the cloud.
    if payload.get("run_type") == "workflow":
        write_run_snapshot(run_dir, payload)
        return None
    snapshot_sha = write_run_snapshot(run_dir, payload)
    run_id = Path(run_dir).name
    # Snapshot key is derivable (runs/<run_id>/snapshot.yaml) — not stored.
    pointer = {
        "run_id": run_id,
        "snapshot_sha256": snapshot_sha,
        "committed_at": payload.get("updated_at") or kernel_ids.utc_timestamp(),
        "status": payload.get("status", RunStatus.OK),
    }
    for key in _COMMITTED_FACT_KEYS:
        if payload.get(key) is not None:
            pointer[key] = payload[key]
    instance_dir = ctl_state_dir_from_run_dir(run_dir)
    group = run_actions.action_group(str(payload.get("action")))
    kernel_yaml_io.write_yaml_file(committed_pointer_path(instance_dir, group), pointer)
    return committed_pointer_path(instance_dir, group)


def read_committed_pointer(instance_dir: Path, group: str = "mutative") -> dict | None:
    path = committed_pointer_path(instance_dir, group)
    if not path.is_file():
        return None
    data = kernel_yaml_io.load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ committed.yaml must be a mapping: {path}")
    return data


def read_instance_state_slot(
    instance_dir: Path, state: StateSlot, group: str = "mutative"
) -> dict | None:
    """A run slot sitting beside committed.yaml in the SAME instance dir.

    `write_state_slot` anchors slots at `ctl_state_dir_from_run_dir(run_dir)`,
    which is the instance dir, so a status reader needs no run-log scan to learn
    what happened to this instance most recently."""

    path = state_slot_dir(instance_dir, state, group) / "STATUS.yaml"
    if not path.is_file():
        return None
    data = kernel_yaml_io.load_yaml(path) or {}
    return data if isinstance(data, dict) else None


def rewrite_in_progress_slot_if_present(run_dir: Path, payload: dict) -> None:
    slot_dir = state_slot_dir(
        ctl_state_dir_from_run_dir(run_dir),
        StateSlot.IN_PROGRESS,
        run_state_group(run_dir),
    )
    if slot_dir.exists():
        write_state_slot(run_dir, StateSlot.IN_PROGRESS, payload)


def record_run_target_keys(run_dir: Path, target_keys: list[str]) -> None:
    normalized = run_addressing.normalize_target_keys(target_keys, label="target_keys")
    metadata = update_run_metadata(run_dir, {"target_keys": normalized})
    status = load_current_status(run_dir)
    if status:
        status.update({"target_keys": normalized, "updated_at": kernel_ids.utc_timestamp()})
        for key in ("action", "run_type", "result_name", "result_key", "ctl_state_local_root", "ctl_state_dir", "run_dir", "log_path"):
            if key in metadata:
                status[key] = metadata[key]
        write_current_status(run_dir, status)
        if status.get("status") == RunStatus.IN_PROGRESS:
            rewrite_in_progress_slot_if_present(run_dir, status)


def iter_committed_status_paths(ctl_state_local_root: Path):
    root = Path(ctl_state_local_root)
    if not root.is_dir():
        return
    # The committed record is the committed.yaml pointer at the
    # instance dir (was committed/STATUS.yaml).
    yield from sorted(root.rglob("committed/*.yaml"))


def load_status_mapping(path: Path) -> dict:
    data = kernel_yaml_io.load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ STATUS.yaml must contain a mapping: {path}")
    return data


def update_committed_manifest(status_path: Path, payload: dict) -> None:
    manifest_path = status_path.parent / "MANIFEST.yaml"
    manifest = {
        "run_id": payload.get("run_id"),
        "run_path": payload.get("run_path") or (f"runs/{payload.get('run_id')}" if payload.get("run_id") else None),
        "status_path": payload.get("status_path") or (f"runs/{payload.get('run_id')}/RUN.yaml" if payload.get("run_id") else None),
        "artifacts_path": payload.get("artifacts_path") or (f"runs/{payload.get('run_id')}/artifacts" if payload.get("run_id") else None),
        "updated_at": payload.get("updated_at"),
    }
    kernel_yaml_io.write_yaml_file(manifest_path, {k: v for k, v in manifest.items() if v is not None})


def ctl_state_lock_path(ctl_state_local_root: Path) -> Path:
    return Path(ctl_state_local_root) / CTL_RESULTS_LOCK_FILENAME


def ctl_state_lock_metadata_path(ctl_state_local_root: Path) -> Path:
    return Path(ctl_state_local_root) / CTL_RESULTS_LOCK_META_FILENAME


def load_ctl_state_lock_metadata(ctl_state_local_root: Path) -> dict:
    path = ctl_state_lock_metadata_path(ctl_state_local_root)
    if not path.is_file():
        return {}
    data = kernel_yaml_io.load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ ctl-state lock metadata must be a mapping: {path}")
    return data


CHILD_LOCK_GRANT_ENV = "ATLAS_CHILD_LOCK_GRANT"


# A grant is spent ONCE globally, but the lock decision is asked twice per run
# (prepare, then complete_prepare_after_preflight). Remember the redemption for
# this process so the second ask does not look like a replay.
_REDEEMED_CHILD_GRANT: str | None = None


def mint_child_lock_grant(
    ctl_state_local_root: Path, *, child_kind: str, child_key: str
) -> str:
    """Mint a SINGLE-USE grant letting one child run under the lock
    this process holds.

    The run id cannot serve as the credential: it is printed in logs, stored in
    run metadata and visible in `ps`, and it stays valid for the whole run — so
    anyone who reads it can start a concurrent run against the same ctl-state
    while the parent is still going. A nonce is unguessable, is BOUND to the one
    child it was minted for, and is CONSUMED on use, so a leaked value buys
    nothing.
    """

    metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
    if not metadata:
        raise RuntimeError("❌ cannot mint a child lock grant without a held ctl-state lock")
    grant = uuid.uuid4().hex
    grants = dict(metadata.get("child_lock_grants") or {})
    grants[grant] = {"kind": child_kind, "key": child_key}
    metadata["child_lock_grants"] = grants
    kernel_yaml_io.write_yaml_file(ctl_state_lock_metadata_path(ctl_state_local_root), metadata)
    return grant


def consume_child_lock_grant(
    ctl_state_local_root: Path, grant: str | None, *, child_kind: str, child_key: str | None
) -> bool:
    """

    redeem a child grant exactly once, for the child it was minted for."""

    global _REDEEMED_CHILD_GRANT
    if not grant:
        return False
    if grant == _REDEEMED_CHILD_GRANT:
        return True
    metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
    grants = dict(metadata.get("child_lock_grants") or {})
    claim = grants.get(grant)
    if not isinstance(claim, dict):
        return False
    if claim.get("kind") != child_kind or (
        claim.get("key") is not None and claim.get("key") != child_key
    ):
        return False
    del grants[grant]                       # single use
    metadata["child_lock_grants"] = grants
    kernel_yaml_io.write_yaml_file(ctl_state_lock_metadata_path(ctl_state_local_root), metadata)
    _REDEEMED_CHILD_GRANT = grant
    return True


def ctl_state_lock_matches(ctl_state_local_root: Path, lock_id: str | None) -> bool:
    if not lock_id:
        return False
    metadata = load_ctl_state_lock_metadata(ctl_state_local_root)
    return metadata.get("run_id") == lock_id


def format_ctl_state_lock_error(ctl_state_local_root: Path, metadata: dict, *, reason: str) -> str:
    lock_id = metadata.get("run_id") or "unknown"
    details = [
        f"❌ ctl-state local root is locked: {ctl_state_local_root}",
        f"reason: {reason}",
        f"lock_id/run_id: {lock_id}",
    ]
    for key in ("action", "run_type", "result_name", "run_dir", "host", "pid", "started_at"):
        value = metadata.get(key)
        if value not in (None, ""):
            details.append(f"{key}: {value}")
    details.append("If the owning ctl process is gone, run maintenance unlock-ctl-state with --lock-id " + str(lock_id))
    return "\n".join(details)


class CtlResultsLock:
    """

    local ctl-state root lock backed by flock plus explicit metadata."""

    def __init__(self, ctl_state_local_root: Path):
        self.ctl_state_local_root = Path(ctl_state_local_root)
        self.lock_path = ctl_state_lock_path(self.ctl_state_local_root)
        self.metadata_path = ctl_state_lock_metadata_path(self.ctl_state_local_root)
        self._file = None
        self.run_id: str | None = None

    def acquire(self, *, allow_stale_metadata: bool = False) -> "CtlResultsLock":
        self.ctl_state_local_root.mkdir(parents=True, exist_ok=True)
        self._file = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            metadata = load_ctl_state_lock_metadata(self.ctl_state_local_root)
            self._file.close()
            self._file = None
            raise RuntimeError(
                format_ctl_state_lock_error(
                    self.ctl_state_local_root,
                    metadata,
                    reason="another ctl process still holds the OS lock",
                )
            ) from exc

        if not allow_stale_metadata:
            metadata = load_ctl_state_lock_metadata(self.ctl_state_local_root)
            if metadata:
                self.release(clear_metadata=False)
                raise RuntimeError(
                    format_ctl_state_lock_error(
                        self.ctl_state_local_root,
                        metadata,
                        reason="stale ctl lock metadata exists",
                    )
                )
        return self

    def write_metadata(self, payload: dict) -> None:
        if self._file is None:
            raise RuntimeError("❌ cannot write ctl lock metadata before acquiring the lock")
        self.run_id = payload.get("run_id")
        kernel_yaml_io.write_yaml_file(self.metadata_path, payload)
        self._file.seek(0)
        self._file.truncate()
        self._file.write(f"run_id: {payload.get('run_id', '')}\n")
        self._file.flush()
        os.fsync(self._file.fileno())

    def release(self, *, clear_metadata: bool = True) -> None:
        if self._file is None:
            return
        remove_lock_file = clear_metadata
        try:
            if clear_metadata and self.metadata_path.exists():
                metadata = load_ctl_state_lock_metadata(self.ctl_state_local_root)
                if not self.run_id or metadata.get("run_id") == self.run_id:
                    self.metadata_path.unlink()
                else:
                    remove_lock_file = False
            elif not clear_metadata:
                remove_lock_file = False

            if clear_metadata and remove_lock_file:
                self._file.seek(0)
                self._file.truncate()
                self._file.flush()
                os.fsync(self._file.fileno())
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None


def acquire_ctl_state_lock(ctl_state_local_root: Path) -> CtlResultsLock:
    return CtlResultsLock(ctl_state_local_root).acquire()


def release_ctl_state_lock(lock: CtlResultsLock | None) -> None:
    if lock is not None:
        lock.release()


def should_bypass_ctl_state_lock(args: argparse.Namespace, run_type: str) -> bool:
    """Whether this run proceeds WITHOUT acquiring the ctl-state lock.

    Two cases, both requiring the caller to PROVE it knows the held lock's id:

    - `unlock-ctl-state` maintenance, which exists to release that very lock;
    - workflow-spawned child target run, which executes under the
      lock its parent already holds. The lock is `flock(LOCK_EX | LOCK_NB)`, so a
      child that tried to acquire it would fail outright — exactly one holder, and
      children run provably under it. Authorisation is a SINGLE-USE grant minted
      per child and redeemed once, so the public run id cannot be replayed into a
      concurrent run; an FD handed down instead would be unforgeable but could not
      cross a container or host boundary.
    """

    if run_type == "maintenance":
        return (
            getattr(args, "maintenance_action", None) == "unlock-ctl-state"
            and ctl_state_lock_matches(
                args.ctl_state_local_root, getattr(args, "lock_id", None)
            )
        )
    # A child runs under its parent's lock only by redeeming a
    # SINGLE-USE grant the parent minted for it, passed by environment so it is
    # absent from `ps` and from the logged command line. The parent run id is NOT
    # A credential — it is public.
    return consume_child_lock_grant(
        args.ctl_state_local_root,
        os.environ.get(CHILD_LOCK_GRANT_ENV),
        child_kind=run_type,
        child_key=getattr(args, run_type, None) if run_type != "fan_out" else None,
    )


def write_ctl_state_lock_metadata(
    lock: CtlResultsLock,
    *,
    run_id: str,
    action: str,
    run_type: str,
    result_name: str,
    run_dir: Path,
) -> None:
    lock.write_metadata(
        {
            "run_id": run_id,
            "action": action,
            "run_type": run_type,
            "result_name": result_name,
            "run_dir": str(run_dir),
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": kernel_ids.utc_timestamp(),
        }
    )


def hydrate_ctl_state_index(syncer) -> list[str]:
    keys = syncer.list_object_keys()
    for key in keys:
        if key.endswith("/committed.yaml") or key.endswith("/RUN.yaml"):
            syncer.pull_object(key)
    return keys


_MUTATION_LOCK_HELD: dict | None = None


def enforce_mutation_lock(
    syncer, *, action: str, run_id: str, parent_run_id: str | None = None,
    ttl_seconds: int | None = None,
) -> None:
    """The interim global mutation lock, enforced at the
    namespace bucket. Mutating runs acquire it exclusively; non-mutating runs
    proceed. No syncer (sync skipped or deferred) means no
    reachable backend — the lock is skipped with a log line (the bootstrap-defer
    window is single-operator by definition)."""

    global _MUTATION_LOCK_HELD
    if syncer is None:
        logging.info("mutation lock skipped: no armed ctl-state syncer")
        return
    if action not in run_actions.MUTATING_ACTIONS:
        return                          # never acquires, never blocks: no read needed
    existing = syncer.read_mutation_lock()
    outcome = evaluate_mutation_lock(
        existing, action=action, run_id=run_id, parent_run_id=parent_run_id,
        ttl_seconds=ttl_seconds,
    )
    decision = outcome["decision"]
    if decision == "blocked":
        raise RuntimeError(
            f"❌ ctl-state namespace is locked by run {outcome['holder']!r} "
            f"(mutation in progress); retry after it completes or expires"
        )
    if decision == "proceed":
        return
    if decision == "break_and_acquire":
        logging.warning(
            "breaking stale mutation lock of run %s", outcome["lock_doc"].get("broke_lock_of")
        )
        syncer.delete_mutation_lock()
    if not syncer.write_mutation_lock(outcome["lock_doc"]):
        current = syncer.read_mutation_lock() or {}
        raise RuntimeError(
            f"❌ ctl-state namespace lock lost to run {current.get('run_id')!r}; "
            "retry after it completes"
        )
    _MUTATION_LOCK_HELD = {"syncer": syncer, "run_id": run_id}
    logging.info("mutation lock acquired (run %s)", run_id)


def release_mutation_lock_if_held(run_id: str | None = None) -> None:
    global _MUTATION_LOCK_HELD
    held = _MUTATION_LOCK_HELD
    if held is None:
        return
    if run_id is not None and held["run_id"] != run_id:
        return
    try:
        held["syncer"].delete_mutation_lock()
        logging.info("mutation lock released (run %s)", held["run_id"])
    finally:
        _MUTATION_LOCK_HELD = None


def target_run_source_facts(target_run: dict) -> tuple[str | None, str]:
    commit = target_run.get("commit")
    repo_path = target_run.get("repo_path")
    if repo_path:
        actual_commit, state = kernel_git.git_source_facts(Path(repo_path))
        return (str(commit).strip() if commit else actual_commit), state
    return (str(commit).strip() if commit else None), ("clean" if commit else "dirty")


def target_instance_dir_for_run(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
    action: str | None = None,
) -> tuple[Path, str]:
    metadata = load_run_metadata(parent_run_dir)
    target_key = run_actions.normalize_result_name(
        target_run.get("target"), label="workflow target key"
    )
    segments = run_addressing.resolve_target_instance_segments(
        target_run.get("target_instance_params"),
        execution_context,
        label=f"target {target_key}",
    )
    namespace_root = Path(metadata["ctl_state_local_root"]).joinpath(
        *(metadata.get("ctl_state_locator") or [])
    )
    return (
        namespace_root
        / run_addressing.compose_state_relpath("target", target_key, segments),
        run_addressing.target_instance_address(target_key, segments),
    )


# The TTL is what makes a lock BREAKABLE, so it must exceed the
# longest mutation a namespace can hold — at one hour a long apply outlived its
# own lock, the next mutating run broke it, and the result was the two concurrent
# mutators the lock exists to prevent. Six hours is past any single apply.
# Erring long is safe because expiry is not the only way out: `unlock-ctl-state`
# Releases an abandoned lock deliberately, with the operator deciding the run is
# really gone. A TTL short enough to self-clear is a TTL short enough to break a
# live run, and only one of those two failures is silent.
MUTATION_LOCK_TTL_SECONDS = 21600


def build_mutation_lock_doc(
    run_id: str, action: str, *, broke_lock_of: str | None = None,
    ttl_seconds: int | None = None,
) -> dict:
    now = datetime.now(UTC)
    ttl = MUTATION_LOCK_TTL_SECONDS if ttl_seconds is None else ttl_seconds
    doc = {
        "run_id": run_id,
        "run_type": "mutation",
        "action": action,
        "acquired_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=ttl)).isoformat(),
        # Recorded so a later reader can tell a lock taken under a long TTL from
        # one taken under a short one, rather than inferring it from the window.
        "ttl_seconds": ttl,
    }
    if broke_lock_of:
        doc["broke_lock_of"] = broke_lock_of
    return doc


def mutation_lock_is_stale(lock_doc: dict, *, now: datetime | None = None) -> bool:
    expires = lock_doc.get("expires_at")
    if not isinstance(expires, str):
        return True  # malformed locks are breakable, not deadlocks
    try:
        expiry = datetime.fromisoformat(expires)
    except ValueError:
        return True
    return (now or datetime.now(UTC)) >= expiry


def evaluate_mutation_lock(
    existing_lock: dict | None, *, action: str, run_id: str, parent_run_id: str | None = None,
    ttl_seconds: int | None = None,
) -> dict:
    """Pure decision logic for the interim global mutation lock.

    Returns {decision, lock_doc?, holder?}: mutating actions ACQUIRE (or BREAK a
    stale lock, recording broke_lock_of); a live holder blocks them. The physical
    conditional write/read is the backend adapter's job.

    a NON-MUTATING action always proceeds. It never acquires, so
    blocking it only ever denied a read — and it denied exactly the read most
    worth having, since `status` reports on the very run holding the lock. What
    ctl-state holds is run HISTORY, not resource state: a reader during a
    mutation sees a record that is being appended to, which is what "a run is in
    progress" means and what the status row already says.
    """

    mutating = action in run_actions.MUTATING_ACTIONS
    if not mutating:
        return {"decision": "proceed"}
    if existing_lock is None:
        return {"decision": "acquire", "lock_doc": build_mutation_lock_doc(run_id, action, ttl_seconds=ttl_seconds)}
    if mutation_lock_is_stale(existing_lock):
        return {
            "decision": "break_and_acquire",
            "lock_doc": build_mutation_lock_doc(
                run_id, action, broke_lock_of=str(existing_lock.get("run_id")),
                ttl_seconds=ttl_seconds,
            ),
        }
    holder = str(existing_lock.get("run_id"))
    # A workflow holds the namespace for the whole run and spawns its targets as
    # child processes. A child meeting its OWN parent's lock is not contention —
    # the namespace is already held on its behalf, and blocking it would make
    # every sync-armed workflow fail on its first child. Mirrors the single-use
    # local-lock grant, which covers the flock but not this backend lock.
    if parent_run_id and holder == str(parent_run_id):
        logging.info("mutation lock held by parent run %s; proceeding as its child", holder)
        return {"decision": "proceed"}
    return {"decision": "blocked", "holder": holder}
