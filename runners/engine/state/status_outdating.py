"""Marking a committed result outdated, and forgetting one entirely.

The WRITE half of status: it reads a computed status to decide, and nothing
in the computation reads back into it. That one-way dependency is why this
splits off cleanly.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path

from engine.cfg import resources as cfg_resources
from engine.kernel import ids as kernel_ids
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.state import run_store as state_run_store
from engine.state import status as state_status


def mark_committed_status_outdated(
    status_path: Path, status: dict, *, reason: str, caused_by: dict | None = None
) -> None:
    # The outdate marker is written onto the committed.yaml pointer
    # itself (the target-instance's committed record, Q1c) — no separate slot.
    payload = dict(status)
    payload["status"] = state_run_store.RunStatus.OUTDATED
    payload["updated_at"] = kernel_ids.utc_timestamp()
    outdated = {
        "reason": reason,
        "at": payload["updated_at"],
    }
    if caused_by is not None:
        outdated["caused_by"] = caused_by
    payload["outdated"] = outdated
    kernel_yaml_io.write_yaml_file(status_path, payload)


def mark_outdated_for_run(
    run_dir: Path, *, include_current_result: bool, force: bool = False
) -> None:
    metadata = state_run_store.load_run_metadata(run_dir)
    action = metadata.get("action")
    if action not in run_actions.MUTATING_ACTIONS:
        return
    if not force and metadata.get("mutation_started") is not True:
        return

    affected_target_keys = state_status.status_target_keys(metadata)
    if not affected_target_keys:
        return
    affected = set(affected_target_keys)

    ctl_state_local_root = metadata.get("ctl_state_local_root")
    current_result_key = metadata.get("result_key")
    if not isinstance(ctl_state_local_root, str) or not ctl_state_local_root:
        return

    caused_by = {
        "action": metadata.get("action"),
        "run_type": metadata.get("run_type"),
        "result_name": metadata.get("result_name"),
        "result_key": metadata.get("result_key"),
        "run_id": metadata.get("run_id") or Path(run_dir).name,
        "target_keys": affected_target_keys,
    }

    # A mutation outdates results in ITS OWN namespace tree only,
    # and only for the SAME target-instance addresses (Q1c: sibling ACTIONS of
    # one instance, never sibling instances — dev's mutation must not touch
    # test's results even though the target keys match).
    locator = metadata.get("ctl_state_locator") or []
    scan_root = Path(ctl_state_local_root).joinpath(*locator)
    affected_addresses = {a for a in (metadata.get("target_addresses") or []) if isinstance(a, str)}
    run_instance = metadata.get("instance") or []

    for status_path in state_run_store.iter_committed_status_paths(scan_root):
        status = state_run_store.load_status_mapping(status_path)
        info = state_status.status_result_info(Path(ctl_state_local_root), status_path, status)
        if info is None:
            continue
        if info.get("action") == "readonly":
            continue
        if not include_current_result and info.get("result_key") == current_result_key:
            continue
        if affected_addresses:
            # instance-aware matching: a candidate target result matches when
            # its own address is affected; a candidate workflow result matches
            # when it shares this run's instance (its own sibling actions).
            candidate_address = info.get("address")
            candidate_instance = info.get("instance") or []
            candidate_is_run_sibling = (
                info.get("result_name") == metadata.get("result_name")
                and candidate_instance == run_instance
            )
            if candidate_address not in affected_addresses and not candidate_is_run_sibling:
                continue
            # .9: never outdate a result THIS run graph just committed
            # on its OWN action. A workflow provision commits its child target
            # provision pointers, then sweeps — without this guard it re-marks
            # its own fresh output stale (the child's own earlier sweep had
            # protected that pointer, but only via its own result_key, which the
            # workflow-level sweep does not match). Cross-action supersession
            # (this provision outdating the sibling DESTROY pointer) still fires,
            # because that pointer's action differs from this run's action.
            if (
                not include_current_result
                and info.get("action") == action
                and candidate_address in affected_addresses
            ):
                continue
        else:
            # Legacy metadata without addresses: match by target-key overlap
            committed_keys = set(state_status.status_target_keys(status))
            if not committed_keys or not committed_keys.intersection(affected):
                continue
        mark_committed_status_outdated(
            status_path,
            status,
            reason="affected_by_mutating_run",
            caused_by=caused_by,
        )


def mark_removed_definitions_outdated(ctl_state_local_root: Path, ctl_cfg_root: Path) -> None:
    try:
        workflows = cfg_resources.collect_resource(ctl_cfg_root, "workflows", entry_depth=1)
    except Exception as exc:
        logging.warning("Skipping definition_removed scan: failed to load workflows: %s", exc)
        workflows = {}
    try:
        targets = cfg_resources.collect_resource(ctl_cfg_root, "targets", entry_depth=1)
    except Exception as exc:
        logging.warning("Skipping definition_removed scan: failed to load targets: %s", exc)
        targets = {}

    for status_path in state_run_store.iter_committed_status_paths(Path(ctl_state_local_root)):
        status = state_run_store.load_status_mapping(status_path)
        info = state_status.status_result_info(Path(ctl_state_local_root), status_path, status)
        if info is None:
            continue
        run_type = info.get("run_type")
        action = info.get("action")
        result_name = info.get("result_name")
        if run_type == "workflow":
            entry = workflows.get(result_name)
            exists = isinstance(entry, dict) and action in (entry.get("actions") or [])
        elif run_type == "target":
            entry = targets.get(result_name)
            exists = isinstance(entry, dict) and action in (entry.get("actions") or [])
        else:
            continue
        if exists or status.get("status") == state_run_store.RunStatus.OUTDATED:
            continue
        mark_committed_status_outdated(
            status_path,
            status,
            reason="definition_removed",
            caused_by={
                "action": action,
                "run_type": run_type,
                "result_name": result_name,
                "result_key": info.get("result_key"),
            },
        )


def forget_selection(
    namespace_root: Path,
    older_than: str,
    addresses: list[str],
) -> list[dict]:
    """Resolve the two filters to instance directories.

    Both filters are always supplied — neither defaults — so a forget always
    states both dimensions and nothing is removed on a filter the caller did not
    write down. `any` and `all` are the explicit wide values.

    An ADDRESS may name a template or an instance: depth decides scope, so
    `.../env/core/baseline` selects every instance under it and
    `.../instances/env.type=dev/...` selects one.
    """

    cutoff = None
    if older_than != "any":
        try:
            cutoff = datetime.fromisoformat(older_than)
        except ValueError as error:
            raise RuntimeError("❌ --older-than must be `any` or ISO-8601") from error
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=UTC)

    wanted = None if addresses == ["all"] else [a.strip("/") for a in addresses]
    selected: list[dict] = []
    for pointer_path in sorted(Path(namespace_root).rglob("committed/*.yaml")):
        instance_dir = pointer_path.parent
        rel = instance_dir.relative_to(namespace_root).as_posix()
        if wanted is not None and not any(rel == a or rel.startswith(a + "/") for a in wanted):
            continue
        pointer = state_run_store.read_committed_pointer(instance_dir) or {}
        when = pointer.get("committed_at")
        if cutoff is not None:
            if not when:
                continue
            try:
                stamp = datetime.fromisoformat(str(when).replace("Z", "+00:00"))
            except ValueError:
                continue
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=UTC)
            if stamp >= cutoff:
                continue
        selected.append({"address": rel, "dir": instance_dir, "at": when})
    return selected


def forget_guard(
    namespace_root: Path,
    rel: str,
    *,
    accept_orphans: bool,
    cascade: bool,
    referenced_by: dict[str, set[str]],
) -> str | None:
    """Why this instance may not be forgotten, or None.

    Read straight off the status axes rather than joined from a collapsed
    summary — which is the whole reason `state` and `status` are separate fields.
    """
    instance_dir = namespace_root / rel
    if (
        state_run_store.read_instance_state_slot(
            instance_dir, state_run_store.StateSlot.IN_PROGRESS
        )
        is not None
    ):
        # No override: the run republishes the record moments later, so forgetting
        # it now would look like it worked and would not have.
        return "a run is in progress on it"
    parsed = run_addressing.parse_state_relpath(namespace_root, instance_dir)
    if parsed and parsed["kind"] == "target":
        computed = state_status.compute_target_instance_status(
            namespace_root,
            "provision",
            {
                "kind": "target",
                "key": parsed["key"],
                "segments": list(parsed["instance_segments"]),
                "address": rel,
                "prefix": run_addressing.compose_state_relpath(
                    "target", parsed["key"], list(parsed["instance_segments"])
                ).as_posix(),
            },
        )
        state = computed.get("state")
        if state in ("provisioned", "partial") and not accept_orphans:
            return f"state is {state}; pass --accept-orphaned-resources"
    referrers = referenced_by.get(rel, set())
    if referrers and not cascade:
        return (
            f"referenced by retained workflow runs ({', '.join(sorted(referrers))}); pass --cascade"
        )
    return None


def workflow_references(namespace_root: Path) -> dict[str, set[str]]:
    """Which retained workflow runs point at each instance address.

    Forgetting a record a workflow still names leaves that workflow describing a
    member with nothing behind it, so it is refused unless the caller says
    `--cascade`.
    """
    references: dict[str, set[str]] = {}
    for pointer_path in Path(namespace_root).rglob("committed/*.yaml"):
        pointer = state_run_store.read_committed_pointer(pointer_path.parent) or {}
        for child in pointer.get("child_revisions") or []:
            if not isinstance(child, dict) or not child.get("address"):
                continue
            key, segments = run_addressing.split_target_instance_address(str(child["address"]))
            rel = run_addressing.compose_state_relpath("target", key, segments).as_posix()
            references.setdefault(rel, set()).add(
                pointer_path.parent.relative_to(namespace_root).as_posix()
            )
    return references
