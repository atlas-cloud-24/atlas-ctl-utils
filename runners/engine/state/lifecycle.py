"""The transitions of a run: started, mutating, succeeded, failed.

Outdating fires when a MUTATION starts, not when the run does — a run that has
not yet changed anything has not yet invalidated anything, and marking it early
would report dependents as stale on a run that turns out to be a no-op."""

import logging
import sys
from pathlib import Path

from engine.kernel import ids as kernel_ids
from engine.kernel import process as kernel_process
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine.state import sync as state_sync


def log_target_run_banner(target_run_id: str, *, ch: str = "#", min_width: int = 70) -> None:
    title = f" {target_run_id} "
    width = max(min_width, len(title) + 2)  # ensure it always fits
    line = ch * width
    mid  = title.center(width, ch)
    logging.info(line)
    logging.info(mid)
    logging.info(line)


def mark_run_started(run_dir: Path) -> None:
    payload = state_status.build_status_payload(run_dir, state_run_store.RunStatus.IN_PROGRESS)
    state_run_store.write_current_status(run_dir, payload)
    state_run_store.write_state_slot(run_dir, state_run_store.StateSlot.IN_PROGRESS, payload)


def record_maintenance_request(
    run_dir: Path,
    *,
    operation: str,
    subject: str | None = None,
    scope: str | None = None,
) -> None:
    """Persist the operator request before maintenance enters its lifecycle.

    The result path already encodes operation and subject; recording them as
    fields keeps the audit row self-describing and makes the requested scope
    visible while the run is still in progress.
    """

    state_run_store.update_run_metadata(
        run_dir,
        {
            "maintenance_operation": operation,
            **({"maintenance_subject": subject} if subject else {}),
            **({"maintenance_scope": scope} if scope else {}),
        },
    )


def record_workflow_members(
    run_dir: Path, active_target_runs: dict, workflow_cfg: dict
) -> None:
    """A workflow run records the composition it ran, as history.

    it records target INSTANCES, not keys. A workflow's own instance
    params are the UNION of its members', so a member varying over fewer axes has
    a SHORTER address — which cannot be reconstructed from the workflow's segments.
    The addresses are already resolved on the run (`target_addresses`), so keying
    by name would throw away a fact the run already holds.

    Each entry mirrors the cfg shape — a bare address when it takes the member's
    `default_action`, `{instance, action}` when it differs — so the record reads
    like the declaration that produced it. `member_selectors` is the matched
    member's own block copied verbatim, which points back at that declaration and
    keeps the engine from needing to know a field called "operation".
    """
    default_action = workflow_cfg.get("default_action")
    resolved_addresses = {
        run_addressing.split_target_instance_address(address)[0]: address
        for address in (state_run_store.load_run_metadata(run_dir).get("target_addresses") or [])
        if isinstance(address, str)
    }
    target_instances: list = []
    for target_run in active_target_runs.values():
        key = target_run.get("target")
        if not key:
            continue
        # A key with no resolved address is a singleton target, whose address IS
        # its key — never a silent omission.
        address = resolved_addresses.get(key, key)
        action = target_run.get("action")
        if action and action != default_action:
            target_instances.append({"instance": address, "action": action})
        else:
            target_instances.append(address)
    facts: dict = {"target_instances": target_instances}
    operation = workflow_cfg.get("operation")
    if operation is not None:
        if not isinstance(operation, str) or not operation:
            raise RuntimeError("❌ resolved workflow operation must be a non-empty string")
        facts["operation"] = operation
    if default_action:
        facts["default_action"] = default_action
    actions = run_addressing.workflow_actions(facts)
    if actions:
        facts["actions"] = actions
    effect = run_addressing.workflow_effect(facts)
    if effect:
        facts["effect"] = effect
    # Derived here, where the composition is known, so a reader of the record
    # never has to re-derive it and cannot derive it differently. `group` is the
    # internal state channel; public status reads `effect` + `actions`.
    group = run_addressing.workflow_group(facts)
    if group:
        facts["group"] = group
        # The run already opened an `in_progress` slot under its own action's
        # group, because members were not resolved yet. Move it, so the state slot
        # and the status row agree on ONE group for this run.
        state_run_store.move_state_slots_to_group(run_dir, group)
    if workflow_cfg.get("member_selectors"):
        facts["member_selectors"] = workflow_cfg["member_selectors"]
    state_run_store.update_run_metadata(run_dir, facts)
    status = state_run_store.load_current_status(run_dir)
    if status:
        status.update({**facts, "updated_at": kernel_ids.utc_timestamp()})
        state_run_store.write_current_status(run_dir, status)


def mark_mutation_started(run_dir: Path, target_run_id: str) -> None:
    metadata = state_run_store.update_run_metadata(
        run_dir,
        {
            "mutation_started": True,
            "mutation_started_at": kernel_ids.utc_timestamp(),
            "mutation_target_run_id": target_run_id,
        },
    )
    status = state_run_store.load_current_status(run_dir)
    if status:
        status.update(
            {
                "mutation_started": True,
                "mutation_started_at": metadata["mutation_started_at"],
                "mutation_target_run_id": target_run_id,
                "updated_at": kernel_ids.utc_timestamp(),
            }
        )
        state_run_store.write_current_status(run_dir, status)
        if status.get("status") == state_run_store.RunStatus.IN_PROGRESS:
            state_run_store.rewrite_in_progress_slot_if_present(run_dir, status)
    # Outdate at mutation START, not at run end. From here real resources are
    # changing, so every dependent result IS stale — deferring the marks to
    # mark_run_succeeded/failed leaves readers a `current` verdict across the
    # entire mutation window, the longest stretch of the run.
    state_status.mark_outdated_for_run(run_dir, include_current_result=False)
    state_sync.PUBLICATION.push(f"mutation started ({target_run_id})")


def print_failure_summary(payload: dict) -> None:
    error = payload.get("error") or {}
    print("Run failed", file=sys.stderr)
    if payload.get("result_key"):
        print(f"result: {payload['result_key']}", file=sys.stderr)
    if payload.get("mutation_target_run_id"):
        print(f"target_run: {payload['mutation_target_run_id']}", file=sys.stderr)
    if error.get("summary"):
        print(f"error: {error['summary']}", file=sys.stderr)
    if payload.get("log_path"):
        print(f"log: {payload['log_path']}", file=sys.stderr)


def mark_run_succeeded(run_dir: Path) -> None:
    payload = state_status.build_status_payload(run_dir, state_run_store.RunStatus.OK, {"ctl_state_sync": state_sync.PUBLICATION.summary()})
    state_run_store.write_current_status(run_dir, payload)
    pointer_path = state_run_store.publish_committed_pointer(run_dir, payload)
    state_run_store.remove_state_slot(run_dir, state_run_store.StateSlot.IN_PROGRESS)
    state_run_store.remove_state_slot(run_dir, state_run_store.StateSlot.FAILED)
    state_status.mark_outdated_for_run(run_dir, include_current_result=False)
    metadata = state_run_store.load_run_metadata(run_dir)
    state_run_store.cleanup_run_workspace(run_dir)
    state_sync.PUBLICATION.publish_or_queue_run(
        run_dir,
        pointer_path,
        reason="run succeeded",
        dependencies=list(metadata.get("target_addresses") or []),
    )
    state_run_store.release_mutation_lock_if_held()


def mark_run_failed(run_dir: Path, exc: BaseException) -> None:
    metadata = state_run_store.load_run_metadata(run_dir)
    extracted = kernel_process.extract_error_summary(metadata.get("log_path"), str(exc))
    payload = state_status.build_status_payload(
        run_dir,
        state_run_store.RunStatus.FAILED,
        {
            "error": {
                "type": type(exc).__name__,
                "summary": extracted["summary"],
            },
            "log_path": metadata.get("log_path"),
            "tail_lines": extracted["tail_lines"],
            "ctl_state_sync": state_sync.PUBLICATION.summary(),
        },
    )
    state_run_store.write_current_status(run_dir, payload)
    state_run_store.write_state_slot(run_dir, state_run_store.StateSlot.FAILED, payload)
    state_run_store.remove_state_slot(run_dir, state_run_store.StateSlot.IN_PROGRESS)
    state_status.mark_outdated_for_run(run_dir, include_current_result=True)
    state_run_store.cleanup_run_workspace(run_dir)
    state_sync.PUBLICATION.publish_or_queue_run(run_dir, None, reason="run failed")
    state_run_store.release_mutation_lock_if_held()
    print_failure_summary(payload)


def mark_run_force_unlocked(run_dir: Path, metadata: dict, maintenance_run_dir: Path) -> None:
    run_metadata = state_run_store.load_run_metadata(run_dir)
    metadata_updates = {}
    for key in ("action", "run_type", "result_name", "run_dir"):
        if not run_metadata.get(key) and metadata.get(key):
            metadata_updates[key] = metadata[key]
    if metadata_updates:
        run_metadata = state_run_store.update_run_metadata(run_dir, metadata_updates)

    payload = state_status.build_status_payload(
        run_dir,
        state_run_store.RunStatus.FAILED,
        {
            "failure_reason": "force_unlocked",
            "error": {
                "type": "ForceUnlocked",
                "summary": "ctl-state lock was cleared by maintenance unlock-ctl-state",
            },
            "force_unlocked": {
                "at": kernel_ids.utc_timestamp(),
                "maintenance_run_id": maintenance_run_dir.name,
                "lock_metadata": metadata,
            },
        },
    )
    state_run_store.write_current_status(run_dir, payload)
    state_run_store.write_state_slot(run_dir, state_run_store.StateSlot.FAILED, payload)
    state_run_store.remove_state_slot(run_dir, state_run_store.StateSlot.IN_PROGRESS)

    mutating = payload.get("action") in run_actions.MUTATING_ACTIONS
    force_outdated = mutating and payload.get("mutation_started") is not False
    state_status.mark_outdated_for_run(run_dir, include_current_result=True, force=force_outdated)


def force_unlock_ctl_state_lock(ctl_state_local_root: Path, lock_id: str, maintenance_run_dir: Path) -> bool:
    metadata = state_run_store.load_ctl_state_lock_metadata(ctl_state_local_root)
    if not metadata:
        return False

    active_run_id = metadata.get("run_id")
    if active_run_id != lock_id:
        raise RuntimeError(
            f"❌ ctl-state lock id mismatch: active lock_id/run_id is {active_run_id!r}, got {lock_id!r}"
        )

    lock = state_run_store.CtlResultsLock(ctl_state_local_root).acquire(allow_stale_metadata=True)
    try:
        metadata = state_run_store.load_ctl_state_lock_metadata(ctl_state_local_root)
        if metadata.get("run_id") != lock_id:
            raise RuntimeError(
                f"❌ ctl-state lock changed while unlock-ctl-state was starting: expected {lock_id!r}, got {metadata.get('run_id')!r}"
            )

        raw_run_dir = metadata.get("run_dir")
        if not isinstance(raw_run_dir, str) or not raw_run_dir:
            raise RuntimeError("❌ ctl-state lock metadata is missing run_dir")
        run_dir = Path(raw_run_dir).expanduser().resolve()
        root = Path(ctl_state_local_root).resolve()
        try:
            run_dir.relative_to(root)
        except ValueError as exc:
            raise RuntimeError(f"❌ ctl-state lock run_dir is outside ctl_state_local_root: {run_dir}") from exc

        mark_run_force_unlocked(run_dir, metadata, maintenance_run_dir)
        logging.warning("Ctl-state lock released for run_id=%s", lock_id)
        lock.run_id = lock_id
        return True
    finally:
        lock.release(clear_metadata=True)


def begin_workflow_target_run(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
) -> tuple[Path, str | None]:
    """Materialize one workflow-selected target as its canonical target run."""
    parent_metadata = state_run_store.load_run_metadata(parent_run_dir)
    if parent_metadata.get("run_type") != "workflow":
        return parent_run_dir, None
    target_key = run_actions.normalize_result_name(
        target_run.get("target"), label="workflow target key"
    )
    segments = run_addressing.resolve_target_instance_segments(
        target_run.get("target_instance_params"),
        execution_context,
        label=f"target {target_key}",
    )
    address = run_addressing.target_instance_address(target_key, segments)
    ctl_state_root = Path(parent_metadata["ctl_state_local_root"])
    locator = list(parent_metadata.get("ctl_state_locator") or [])
    namespace_root = ctl_state_root.joinpath(*locator)
    instance_dir = namespace_root / run_addressing.compose_state_relpath(
        "target", target_key, segments
    )
    # A target instance's identity is fully encoded in its path
    # (<key>/instances/<seg>/…) — no identity.yaml is written (§minimal files).
    child_run_id = kernel_ids.generate_uuid7()
    child_run_dir = instance_dir / "runs" / child_run_id
    # The target owns its own log; the workflow keeps the aggregate.
    child_logs_dir = child_run_dir / "logs"
    child_logs_dir.mkdir(parents=True, exist_ok=True)
    child_log_path = child_logs_dir / f"{kernel_ids.SERVICE_ID}_{child_run_id}.log"
    state_run_store.write_run_metadata(
        child_run_dir,
        {
            "run_id": child_run_id,
            "action": parent_metadata["action"],
            "run_type": "target",
            "result_name": target_key,
            "result_key": f"{parent_metadata['action']}/target/{target_key}",
            "ctl_state_local_root": str(ctl_state_root),
            "ctl_state_locator": locator,
            "ctl_state_namespace": parent_metadata.get("ctl_state_namespace"),
            "ctl_state_dir": str(instance_dir),
            "run_dir": str(child_run_dir),
            "log_path": str(child_log_path),
            "parent_log_path": parent_metadata.get("log_path"),
            "target_keys": [target_key],
            "instance": segments,
            "instance_address": address,
            "parent_workflow_run_id": parent_metadata.get("run_id"),
            #(b4): name the parent by INSTANCE too, so a target record
            # says which workflow instance it belongs to without loading the parent.
            "parent_workflow_instance_address": parent_metadata.get("instance_address"),
            "fan_out_run_id": parent_metadata.get("fan_out_run_id"),
            "mutation_started": False,
            **{
                key: target_run[key]
                for key in (
                    "source_commit", "cfg_source_commit", "source_state", "ref_policy",
                    "plt_overlays", "target_definition_sha256", "target_cfg_view_sha256",
                )
                if target_run.get(key) is not None
            },
        },
    )
    mark_run_started(child_run_dir)
    return child_run_dir, address


def finish_workflow_target_run(
    child_run_dir: Path, *, error: BaseException | None = None
) -> dict | None:
    """

    finalize and publish one workflow child without publishing the workflow."""

    if error is not None:
        payload = state_status.build_status_payload(
            child_run_dir, state_run_store.RunStatus.FAILED,
            {"error": {"type": type(error).__name__, "summary": str(error)}},
        )
        state_run_store.write_current_status(child_run_dir, payload)
        state_run_store.write_state_slot(child_run_dir, state_run_store.StateSlot.FAILED, payload)
        state_run_store.remove_state_slot(child_run_dir, state_run_store.StateSlot.IN_PROGRESS)
        state_sync.PUBLICATION.publish_or_queue_run(
            child_run_dir, None, reason="workflow child failed"
        )
        return None

    payload = state_status.build_status_payload(
        child_run_dir, state_run_store.RunStatus.OK, {"ctl_state_sync": state_sync.PUBLICATION.summary()}
    )
    state_run_store.write_current_status(child_run_dir, payload)
    pointer_path = state_run_store.publish_committed_pointer(child_run_dir, payload)
    state_run_store.remove_state_slot(child_run_dir, state_run_store.StateSlot.IN_PROGRESS)
    state_run_store.remove_state_slot(child_run_dir, state_run_store.StateSlot.FAILED)
    state_sync.PUBLICATION.publish_or_queue_run(
        child_run_dir,
        pointer_path,
        reason="workflow child succeeded",
    )
    pointer = state_run_store.read_committed_pointer(state_run_store.ctl_state_dir_from_run_dir(child_run_dir)) or {}
    return {
        "address": payload.get("instance_address") or payload.get("result_name"),
        "run_id": pointer.get("run_id"),
        "snapshot_sha256": pointer.get("snapshot_sha256"),
        "status": pointer.get("status"),
    }


def print_run_summary(run_id: str, log_file: Path) -> None:
    """Print run summary at the end."""
    print(f"Run id: {run_id}")
    print(f"Log file: {log_file}")
