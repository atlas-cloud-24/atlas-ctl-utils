"""What the recorded state MEANS: fresh, stale, failed, absent.

A verdict is computed from run records, never stored, so it cannot drift from
them. Only the forward side of a paired action can go stale — a destroy record
describes an instance that is gone, and nothing can make that answer stale."""

import hashlib
import json
import logging
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from engine.cfg import resources as cfg_resources
from engine.kernel import ids as kernel_ids
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.state import run_store as state_run_store


class Verdict(StrEnum):
    """The `status` axis of a row: what a reader acts on.

    Computed, never stored, and deliberately NOT the record's own vocabulary — a
    record says `ok`, a row says `passed`, because a row answers about an INSTANCE
    and may reach its verdict from a child rather than from any record of its own.
    """

    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"


class Freshness(StrEnum):
    """The `freshness` axis: does the published result still match its inputs?

    A second axis rather than a `status` value, because the two are independent —
    a published result can be `passed` and `outdated` at once, and collapsing them
    would let a live verdict hide a stale one.
    """

    UP_TO_DATE = "up_to_date"
    OUTDATED = "outdated"
    # A branch ref names a moving commit. The commit that was deployed is
    # recorded; where the branch points NOW is a remote read, and status is a
    # pure local computation. Reporting `up_to_date` would be a claim the engine
    # cannot support, so it reports that it cannot know — the same refusal the
    # project applies to a cfg-entry-ref across a state boundary.
    UNDETERMINED = "undetermined"


class Standing(StrEnum):
    """The `standing` axis inside an exclusive relation."""

    ACTIVE = "active"
    SUPERSEDED = "superseded"


def build_status_payload(
    run_dir: Path, status: state_run_store.RunStatus, extra: dict | None = None
) -> dict:
    payload = dict(state_run_store.load_run_metadata(run_dir))
    payload["run_id"] = Path(run_dir).name
    payload["status"] = status
    payload["updated_at"] = kernel_ids.utc_timestamp()
    if extra:
        payload.update(extra)
    return payload


def in_progress_verdict_reason(slot: dict) -> str:
    run_id = slot.get("run_id") or "unknown"
    action = slot.get("action") or "run"
    if slot.get("mutation_started") is True:
        return f"{action} mutating under run {run_id}"
    return f"{action} in progress under run {run_id} (not yet mutating)"


def failed_verdict_reason(slot: dict) -> str:
    run_id = slot.get("run_id") or "unknown"
    action = slot.get("action") or "run"
    summary = (slot.get("error") or {}).get("summary")
    mutated = " after mutation started" if slot.get("mutation_started") is True else ""
    return f"{action} failed{mutated} under run {run_id}" + (
        f": {summary}" if summary else ""
    )


def status_result_info(ctl_state_local_root: Path, status_path: Path, status: dict) -> dict | None:
    # committed.yaml lives directly in the instance dir (its parent),
    # unlike the old committed/STATUS.yaml (parent.parent).
    result_dir = status_path.parent
    parsed = run_addressing.parse_result_dir(ctl_state_local_root, result_dir)
    if parsed is None:
        return None
    info = dict(parsed)
    for key in ("action", "run_type", "result_name", "result_key"):
        if isinstance(status.get(key), str) and status[key]:
            info[key] = status[key]
    return info


def status_target_keys(status: dict) -> list[str]:
    raw = status.get("target_keys") or []
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, str) and item]


def mark_committed_status_outdated(status_path: Path, status: dict, *, reason: str, caused_by: dict | None = None) -> None:
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


def mark_outdated_for_run(run_dir: Path, *, include_current_result: bool, force: bool = False) -> None:
    metadata = state_run_store.load_run_metadata(run_dir)
    action = metadata.get("action")
    if action not in run_actions.MUTATING_ACTIONS:
        return
    if not force and metadata.get("mutation_started") is not True:
        return

    affected_target_keys = status_target_keys(metadata)
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
    affected_addresses = {
        a for a in (metadata.get("target_addresses") or []) if isinstance(a, str)
    }
    run_instance = metadata.get("instance") or []

    for status_path in state_run_store.iter_committed_status_paths(scan_root):
        status = state_run_store.load_status_mapping(status_path)
        info = status_result_info(Path(ctl_state_local_root), status_path, status)
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
            #.9: never outdate a result THIS run graph just committed
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
            committed_keys = set(status_target_keys(status))
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
        info = status_result_info(Path(ctl_state_local_root), status_path, status)
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


def _freshness(pointer: dict | None, spec: dict) -> tuple[str, list[str]]:
    """

    does the committed record still match what is declared?

    Meaningful only where a record exists; the caller supplies `none` otherwise.
    """

    if pointer is None:
        return Freshness.OUTDATED, []
    reasons: list[str] = []
    if pointer.get("status") == state_run_store.RunStatus.OUTDATED or pointer.get("outdated"):
        outdated = pointer.get("outdated") or {}
        reasons.append(str(outdated.get("reason") or "target marker"))
    for fact_key, reason in (
        ("target_definition_sha256", "target definition changed"),
        ("target_cfg_view_sha256", "target cfg view changed"),
    ):
        expected = spec.get(fact_key)
        if expected is not None and pointer.get(fact_key) != expected:
            reasons.append(reason)
    if reasons:
        return Freshness.OUTDATED, reasons
    # A changed CONTENT axis is knowable either way, so it is reported first: a
    # branch-pinned target whose cfg changed is outdated, not merely unknowable.
    if pointer.get("ref_policy") not in (None, "commit_required"):
        return Freshness.UNDETERMINED, ["source ref is a branch"]
    return Freshness.UP_TO_DATE, reasons


def _run_status(instance_dir: Path, group: str = "mutative") -> dict:
    """What the last (or current) run on this instance did.

    Returns `status`, its `reasons`, whether a mutation had begun, and — the part
    a row must not take from anywhere else — the RUN THAT PRODUCED THAT STATUS.

    `run_id`/`at` travel with `status` because they describe one event. Reading
    the status from a slot and the timestamp from the committed pointer mixes two
    different runs: while the newest run succeeds they agree, so it looks correct;
    the moment a run FAILS after a success the row reports the failure's status
    beside the success's time and id, and a reader chasing the failure opens the
    wrong run (observed 2026-08-03).
    """

    for state, verdict, reason in (
        (state_run_store.StateSlot.IN_PROGRESS, Verdict.RUNNING, in_progress_verdict_reason),
        (state_run_store.StateSlot.FAILED, Verdict.FAILED, failed_verdict_reason),
    ):
        slot = state_run_store.read_instance_state_slot(instance_dir, state, group)
        if slot is not None:
            return {
                "status": verdict,
                "reasons": [reason(slot)],
                "mutation_started": slot.get("mutation_started") is True,
                "run_id": slot.get("run_id"),
                "time": slot.get("updated_at"),
                "action": slot.get("action"),
                "label": slot.get("label"),
                "parent_workflow_instance": slot.get("parent_workflow_instance_address"),
                "parent_workflow_run_id": slot.get("parent_workflow_run_id"),
            }
    # No slot AND no committed pointer means nothing ever ran here. `passed`
    # Would be a claim of success nobody made, so the caller gets None and omits
    # the group entirely — absence stays absence.
    pointer = state_run_store.read_committed_pointer(instance_dir, group)
    if pointer is None:
        return {"status": None, "reasons": [], "mutation_started": False,
                "run_id": None, "time": None, "action": None, "label": None,
                "parent_workflow_instance": None, "parent_workflow_run_id": None}
    return {
        "status": Verdict.PASSED,
        "reasons": [],
        "mutation_started": False,
        "run_id": pointer.get("run_id"),
        "time": pointer.get("committed_at"),
        "action": pointer.get("action"),
        "label": pointer.get("label"),
        "parent_workflow_instance": pointer.get("parent_workflow_instance_address"),
        "parent_workflow_run_id": pointer.get("parent_workflow_run_id"),
    }


def _mutating_run_status(
    namespace_root: Path, kind: str, key: str, segments: list[str]
) -> tuple[str | None, list[str], bool, str | None]:
    """The live run axes of a deployment instance.

    provision and destroy share one instance directory and one
    `committed/mutative.yaml`, so the slot and the pointer are read from one
    place and the action comes from whichever record is there.
    """
    instance_dir = namespace_root / run_addressing.compose_state_relpath(kind, key, segments)
    for state, status, describe in (
        (state_run_store.StateSlot.IN_PROGRESS, Verdict.RUNNING, in_progress_verdict_reason),
        (state_run_store.StateSlot.FAILED, Verdict.FAILED, failed_verdict_reason),
    ):
        slot = state_run_store.read_instance_state_slot(instance_dir, state, "mutative")
        if slot is not None:
            return (
                status,
                [describe(slot)],
                slot.get("mutation_started") is True,
                slot.get("action"),
            )
    pointer = state_run_store.read_committed_pointer(instance_dir, "mutative")
    if pointer is None:
        return None, [], False, None
    return Verdict.PASSED, [], False, pointer.get("action")


def compute_target_instance_status(
    namespace_root: Path, action: str, spec: dict
) -> dict:
    """Status of one target instance, on the two axes a row carries.

    `state` and `action` are gone. `provisioned`/`destroyed` asserted
    what exists in the cloud, which ctl never observes — a destroy run directly in
    the repo empties the tool's own state while ctl kept reporting `provisioned`.
    A row reports what ctl's own runs did: whether one is live or broken, and
    whether the published result still matches its inputs.
    """

    group = run_actions.action_group(action)
    instance_dir = namespace_root / run_addressing.compose_state_relpath(
        "target", spec["key"], spec["segments"]
    )
    pointer = state_run_store.read_committed_pointer(instance_dir, group)
    run = _run_status(instance_dir, group)
    status, reasons, mutation_started = run["status"], run["reasons"], run["mutation_started"]

    result: dict = {
        "kind": "target",
        "key": spec["key"],
        "address": spec["address"],
    }
    if status is not None:
        result["status"] = status
    # Freshness applies to a published result only. An interrupted run changed
    # resources and committed nothing, so its pointer describes nothing for inputs
    # to have moved away from; `up_to_date` would be false and `outdated`
    # Understates it.
    # `state` (`provisioned`/`destroyed`) because that asserts
    # what exists in the cloud, which ctl never observes. The ACTION is a
    # different fact and one ctl owns outright. Without it the direction is
    # inferred from `freshness` being ABSENT, which is an implicit signal.
    # It comes from the run that produced `status`, NOT from the committed
    # pointer: a failed destroy after a successful provision reported
    # `status: failed` beside `last_action: provision`, which reads as "the
    # provision failed" when the provision succeeded and the DESTROY failed.
    # Same rule as `at` and `run_id` — one row, one run.
    if run["action"]:
        result["last_action"] = run["action"]
    if (
        pointer
        and not mutation_started
        and run_actions.action_can_go_stale(str(pointer.get("action")))
    ):
        freshness, freshness_reasons = _freshness(pointer, spec)
        result["freshness"] = freshness
        reasons = reasons + freshness_reasons

    standing, superseded_by = StandingResolver(namespace_root).target(
        spec.get("relations") or {},
        spec["address"],
        spec.get("segments") or [],
    )
    if standing is not None:
        result["standing"] = standing
    if superseded_by is not None:
        result["superseded_by"] = run_addressing.qualified_address(
            "target", superseded_by
        )

    # The workflow INSTANCE this run belonged to, closing the loop the workflow
    # row opens: a workflow instance names the target instances it drove, and a
    # target instance names the workflow instance that drove it. Absent for a
    # target run invoked directly, which is a real distinction and not a gap.
    if run["parent_workflow_instance"]:
        result["parent_workflow"] = run_addressing.qualified_address(
            "workflow", run["parent_workflow_instance"]
        )
    elif run["parent_workflow_run_id"]:
        # An older run recorded only the id. It goes under its own key: one field
        # meaning "an address, or else an id" makes every reader branch on shape.
        result["parent_workflow_run_id"] = run["parent_workflow_run_id"]
    # `run_id`/`at` come from whichever run produced `status`, never from the
    # committed pointer when the two are different runs.
    if run["run_id"]:
        result["run_id"] = run["run_id"]
    if run["time"]:
        result["time"] = run["time"]
    # The INVOCATION that run belonged to, which is what makes targets from
    # unrelated source repositories readable as one deployment. Same rule as `at`
    # and `run_id` — it comes from the run that produced `status`, never from a
    # pointer a different run wrote.
    if run["label"]:
        result["label"] = run["label"]
    # The last PUBLISHED result is a separate fact, and only worth stating when
    # it is not the run above — otherwise it would repeat `at` on every row.
    if pointer and pointer.get("run_id") != run["run_id"]:
        result["committed_at"] = pointer.get("committed_at")
        result["committed_run_id"] = pointer.get("run_id")
    if reasons:
        result["reasons"] = reasons
    return result


def compute_workflow_instance_status(
    namespace_root: Path, action: str, spec: dict
) -> dict:
    """Status of one workflow instance, rolled up from its members.

    a composition reports `status` and `freshness` and nothing else. It
    holds no `state`, because once members carry their own action a composition can
    hold a destroy member and a provision member at once and no single word is true
    of it; and no `action`, for the same reason. Both surviving axes roll up without
    reference to direction — running or failed when ANY member is, outdated as soon
    as ANY member is — which is why one row shape serves both kinds.
    """

    group = run_actions.action_group(action)
    workflow_dir = namespace_root / run_addressing.compose_state_relpath(
        "workflow", spec["key"], spec["segments"]
    )
    pointer = state_run_store.read_committed_pointer(workflow_dir, group)
    run = _run_status(workflow_dir, group)
    status, reasons, mutation_started = run["status"], run["reasons"], run["mutation_started"]

    children: list[dict] = []
    recorded = {
        item.get("address"): item
        for item in ((pointer or {}).get("child_revisions") or [])
        if isinstance(item, dict)
    }
    drift: list[str] = []
    for target_spec in spec["target_specs"]:
        child = compute_target_instance_status(namespace_root, action, target_spec)
        child_pointer = state_run_store.read_committed_pointer(
            namespace_root
            / run_addressing.compose_state_relpath("target", target_spec["key"], target_spec["segments"]),
            group,
        )
        expected = recorded.get(target_spec["address"])
        if child.get("freshness") == Freshness.OUTDATED:
            drift.append(f"{target_spec['address']}: outdated")
        elif expected is None:
            drift.append(f"{target_spec['address']}: not recorded by workflow")
        elif (
            expected.get("run_id") != (child_pointer or {}).get("run_id")
            or expected.get("snapshot_sha256")
            != (child_pointer or {}).get("snapshot_sha256")
        ):
            drift.append(f"{target_spec['address']}: committed revision changed")
        children.append(child)

    if pointer is not None:
        if pointer.get("workflow_definition_sha256") != spec[
            "workflow_definition_sha256"
        ]:
            drift.append("workflow definition changed")
        pointer_addresses = [
            str(item.get("address"))
            for item in (pointer.get("child_revisions") or [])
            if isinstance(item, dict)
        ]
        if pointer_addresses != [item["address"] for item in spec["target_specs"]]:
            drift.append("workflow target order or set changed")

    # status: a live or broken child makes the composition live or broken.
    if status in (None, Verdict.PASSED):
        running = [c for c in children if c.get("status") == Verdict.RUNNING]
        failed = [c for c in children if c.get("status") == Verdict.FAILED]
        if running:
            status = Verdict.RUNNING
            reasons = [f"{c['address']}: running" for c in running]
        elif failed:
            status = Verdict.FAILED
            reasons = [f"{c['address']}: failed" for c in failed]

    result: dict = {
        "kind": "workflow",
        "key": spec["key"],
        "address": spec["address"],
    }
    if status is not None:
        result["status"] = status
    if group == "mutative" and pointer is not None and not mutation_started:
        own_freshness, own_reasons = _freshness(pointer, spec)
        result["freshness"] = (
            Freshness.OUTDATED if (drift or own_freshness == Freshness.OUTDATED) else own_freshness
        )
        reasons = reasons + own_reasons + drift

    if run["run_id"]:
        result["run_id"] = run["run_id"]
    if run["time"]:
        result["time"] = run["time"]
    # The invocation this composition belonged to (see
    # compute_target_instance_status).
    if run["label"]:
        result["label"] = run["label"]
    if pointer and pointer.get("run_id") != run["run_id"]:
        result["committed_at"] = pointer.get("committed_at")
        result["committed_run_id"] = pointer.get("run_id")
    if reasons:
        result["reasons"] = list(dict.fromkeys(reasons))
    result["children"] = children
    return result


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
        if wanted is not None and not any(
            rel == a or rel.startswith(a + "/") for a in wanted
        ):
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
    if state_run_store.read_instance_state_slot(instance_dir, state_run_store.StateSlot.IN_PROGRESS) is not None:
        # No override: the run republishes the record moments later, so forgetting
        # it now would look like it worked and would not have.
        return "a run is in progress on it"
    parsed = run_addressing.parse_state_relpath(namespace_root, instance_dir)
    if parsed and parsed["kind"] == "target":
        computed = compute_target_instance_status(
            namespace_root,
            "provision",
            {
                "kind": "target",
                "key": parsed["key"],
                "segments": list(parsed["instance_segments"]),
                "address": rel,
                "prefix": run_addressing.compose_state_relpath("target", parsed["key"], list(parsed["instance_segments"])
                ).as_posix(),
            },
        )
        state = computed.get("state")
        if state in ("provisioned", "partial") and not accept_orphans:
            return f"state is {state}; pass --accept-orphaned-resources"
    referrers = referenced_by.get(rel, set())
    if referrers and not cascade:
        return (
            "referenced by retained workflow runs "
            f"({', '.join(sorted(referrers))}); pass --cascade"
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


def _targeted_workflow_status(namespace_root: Path, action: str, spec: dict) -> dict:
    """A workflow owns execution, so its `status` comes from its LAST RUN — and
    its `freshness` from its members, who each answer for themselves.

    Reading a committed pointer here returned an empty row once workflows stopped
    publishing one; a workflow still publishes none, so the run record is the
    only source for what it DID. What is DEPLOYED is the members' answer.
    """

    result = {"kind": "workflow", "key": spec["key"], "address": spec["address"]}
    last_run = workflow_last_run(
        namespace_root, spec["key"], spec.get("segments") or []
    )
    if last_run:
        result.update(
            {k: v for k, v in last_run.items() if k not in ("target_instances",)}
        )
    members = [
        target_spec["address"] for target_spec in (spec.get("target_specs") or [])
    ] or list((last_run or {}).get("target_instances") or [])
    freshness, member_rows, _ = workflow_member_freshness(
        namespace_root, action, members, spec.get("relations") or {}
    )
    standing, superseded_by = StandingResolver(namespace_root).workflow(
        spec.get("exclusive_workflow_relations") or {},
        spec["address"],
        member_rows,
    )
    return _ordered_workflow_row(
        result,
        standing=standing,
        superseded_by=superseded_by,
        freshness=freshness,
        members=member_rows,
    )


def _compute_status_results(
    namespace_root: Path, action: str, selection_labels: list[str], specs: list[dict]
) -> list[dict]:
    """One result per selected spec, each naming WHAT WAS ASKED FOR beside it.

    `selection_labels` names the query — a fan-out child's display name, or the
    selection key of a targeted read. It is spelled in full because a row also
    carries `label`, which is the operator's name for the INVOCATION that
    produced the record; the two are unrelated facts and must not share a word.
    """

    results = []
    for selection_label, spec in zip(selection_labels, specs, strict=True):
        computed = (
            compute_target_instance_status(namespace_root, action, spec)
            if spec["kind"] == "target"
            else _targeted_workflow_status(namespace_root, action, spec)
        )
        computed["selection"] = selection_label
        results.append(computed)
    return results


class StandingResolver:
    """Standing inside declared exclusive target and workflow relations.

    Standing is computed from committed mutative target evidence and never
    persisted. One resolver owns the namespace read for both target rows and
    the workflow rows derived from them.
    """

    def __init__(self, namespace_root: Path):
        self._namespace_root = Path(namespace_root)

    def target(
        self, relations: dict, address: str, segments: list[str]
    ) -> tuple[Standing | None, str | None]:
        return self.target_from_evidence(
            relations,
            address,
            self._target_evidence(relations, segments),
        )

    def workflow(
        self,
        exclusive_workflow_relations: dict,
        address: str,
        member_rows: list[dict],
    ) -> tuple[Standing | None, str | None]:
        """Map target standing back onto an exclusive workflow relation."""

        key, segments = run_addressing.split_target_instance_address(address)
        for group in (exclusive_workflow_relations or {}).values():
            member_keys = list((group or {}).get("members") or [])
            if key not in member_keys:
                continue
            members = [
                run_addressing.workflow_instance_address(member_key, segments)
                for member_key in member_keys
            ]
            if not any(entry.get("standing") for entry in member_rows):
                return None, None
            replaced_by = {
                run_addressing.unqualified_address(str(entry["superseded_by"]))
                for entry in member_rows
                if entry.get("standing") == Standing.SUPERSEDED
                and entry.get("superseded_by")
            }
            if not replaced_by:
                return Standing.ACTIVE, None
            for sibling in members:
                if sibling == address:
                    continue
                sibling_key, sibling_segments = (
                    run_addressing.split_target_instance_address(sibling)
                )
                for _, run_row in workflow_last_run_by_group(
                    self._namespace_root, sibling_key, sibling_segments
                ).items():
                    sibling_targets = {
                        run_addressing.unqualified_address(
                            entry["instance"] if isinstance(entry, dict) else entry
                        )
                        for entry in (run_row.get("target_instances") or [])
                    }
                    if sibling_targets & replaced_by:
                        return Standing.SUPERSEDED, sibling
            # A member was replaced by a target no sibling has run: the group
            # says these exclude each other, and something outside it took effect.
            return Standing.SUPERSEDED, None
        return None, None

    def _target_evidence(
        self, relations: dict, segments: list[str]
    ) -> dict[str, dict]:
        """Latest committed mutative event for each target relation member."""

        evidence: dict[str, dict] = {}
        for group in (relations or {}).values():
            for member_key in (group or {}).get("members") or []:
                member = run_addressing.target_instance_address(member_key, segments)
                instance_dir = self._namespace_root / run_addressing.compose_state_relpath(
                    "target", member_key, segments
                )
                pointer = state_run_store.read_committed_pointer(
                    instance_dir, run_actions.Group.MUTATIVE
                )
                if pointer is not None:
                    evidence[member] = pointer
        return evidence

    @staticmethod
    def target_from_evidence(
        relations: dict, address: str, evidence_by_address: dict[str, dict]
    ) -> tuple[Standing | None, str | None]:
        """Resolve target standing from committed mutative relation events.

        An exclusive relation names alternatives over one deployment. Its newest
        committed event decides the current standing: provision makes that member
        active, while destroy leaves the relation with no active member. Run ids
        break equal-timestamp ties because ctl run ids are time-sortable UUIDv7s.
        """

        key, segments = run_addressing.split_target_instance_address(address)
        for group in (relations or {}).values():
            member_keys = list((group or {}).get("members") or [])
            if key not in member_keys:
                continue
            members = [
                run_addressing.target_instance_address(member_key, segments)
                for member_key in member_keys
            ]
            evidence = {
                member: evidence_by_address[member]
                for member in members
                if member in evidence_by_address
            }
            if address not in evidence:
                return None, None
            for member, event in evidence.items():
                if not isinstance(event, dict):
                    raise RuntimeError(
                        f"❌ target standing evidence for {member!r} must be a mapping"
                    )
                if event.get("action") not in run_actions.MUTATING_ACTIONS:
                    raise RuntimeError(
                        f"❌ target standing evidence for {member!r} must record "
                        "a mutating action"
                    )
                if (
                    not isinstance(event.get("committed_at"), str)
                    or not event["committed_at"]
                ):
                    raise RuntimeError(
                        f"❌ target standing evidence for {member!r} must record "
                        "committed_at"
                    )
                if not isinstance(event.get("run_id"), str) or not event["run_id"]:
                    raise RuntimeError(
                        f"❌ target standing evidence for {member!r} must record run_id"
                    )
            coordinates = [
                (event["committed_at"], event["run_id"])
                for event in evidence.values()
            ]
            if len(coordinates) != len(set(coordinates)):
                raise RuntimeError(
                    f"❌ target standing evidence for relation members {members} "
                    "must have distinct commit coordinates"
                )
            in_effect = max(
                evidence,
                key=lambda member: (
                    evidence[member]["committed_at"], evidence[member]["run_id"]
                ),
            )
            if evidence[in_effect]["action"] != run_actions.Action.PROVISION:
                return None, None
            if address == in_effect:
                return Standing.ACTIVE, None
            return Standing.SUPERSEDED, in_effect
        return None, None


def _ordered_workflow_row(
    row: dict, *, standing: Standing | None, freshness: str | None, members: list[dict],
    superseded_by: str | None = None,
) -> dict:
    """One field order for every workflow row: status, standing, freshness, at,
    default_action, members — outcome first, then what is in effect, then when.

    A dict preserves insertion order and a status map is read by humans, so the
    order is part of the output rather than a formatting accident.
    """

    ordered: dict = {}
    for key in ("status",):
        if key in row:
            ordered[key] = row[key]
    if standing is not None:
        ordered["standing"] = standing
    if superseded_by is not None:
        ordered["superseded_by"] = run_addressing.qualified_address(
            "workflow", superseded_by
        )
    if freshness is not None:
        ordered["freshness"] = freshness
    for key in (
        "last_operation", "actions", "time", "run_id", "selectors",
        "default_action",
    ):
        if key in row:
            ordered[key] = row[key]
    if members:
        ordered["members"] = members
    for key, value in row.items():
        ordered.setdefault(key, value)
    return ordered


def workflow_member_freshness(
    namespace_root: Path, action: str, member_addresses: list[str],
    relations: dict | None = None,
) -> tuple[str | None, list[dict], list[str]]:
    """A workflow's freshness is a FUNCTION OF ITS MEMBERS.

    Returns `(freshness, members, reasons)`. The child already computes whether
    it is fresh, so the composition asks it rather than re-deriving. Comparing
    recorded child revisions instead would compare `snapshot_sha256`, which
    hashes the whole RUN.yaml — `run_id` and timestamps included — so a child
    re-run with identical inputs would read as drift.

    Worst-of, in a fixed order: OUTDATED is a fact, UNDETERMINED is the absence
    of one, and a composition cannot be fresher than its least fresh member. Each
    member carries its own verdict, because a rolled-up value is unreadable
    without the member that caused it.
    """

    members: list[dict] = []
    for recorded in member_addresses:
        # A member that runs an action other than the workflow's default is
        # recorded as `{instance, action}`; that action must survive into the
        # row, or a destroy member reads like every other one.
        member_action = action
        if isinstance(recorded, dict):
            address = str(recorded.get("instance"))
            member_action = str(recorded.get("action") or action)
        else:
            address = str(recorded)
        unqualified = run_addressing.unqualified_address(address)
        key, segments = run_addressing.split_target_instance_address(unqualified)
        child = compute_target_instance_status(
            namespace_root, member_action,
            {"kind": "target", "key": key, "segments": segments,
             "address": unqualified, "relations": relations or {}},
        )
        entry = {"address": run_addressing.qualified_address("target", unqualified)}
        if isinstance(recorded, dict) and recorded.get("action"):
            entry["action"] = str(recorded["action"])
        # A nested workflow table is useful only when its members answer as
        # target rows rather than as address-only annotations. The child has
        # already computed these facts, so retain the status-report vocabulary
        # here instead of making the renderer read state or invent another report.
        for field in (
            "status", "last_action", "standing", "superseded_by",
            "freshness", "time", "label",
        ):
            if child.get(field) is not None:
                entry[field] = str(child[field])
        members.append(entry)

    verdicts = {entry["freshness"] for entry in members if "freshness" in entry}
    for value in (Freshness.OUTDATED, Freshness.UNDETERMINED, Freshness.UP_TO_DATE):
        if str(value) not in verdicts:
            continue
        if value == Freshness.UP_TO_DATE:
            return str(value), members, []
        return str(value), members, [
            f"{entry['address']}: {entry['freshness']}"
            for entry in members
            if entry.get("freshness") == str(value)
        ]
    return None, members, []


def workflow_last_run_by_group(
    namespace_root: Path, key: str, segments: list[str] | None = None
) -> dict[str, dict]:
    """The latest run of this workflow instance IN EACH GROUP.

    A workflow instance is partitioned by group exactly as a target instance is:
    a `plan` run and a `provision` run are separate facts about the same
    instance, and reporting only the newest collapses them — a failed plan then
    reads as a failed deployment, under the wrong group.
    """

    runs_dir = (
        Path(namespace_root)
        / run_addressing.compose_state_relpath("workflow", key, segments or [])
        / "runs"
    )
    if not runs_dir.is_dir():
        return {}
    latest: dict[str, dict] = {}
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        metadata = state_run_store.load_run_metadata(run_dir)
        if not metadata.get("updated_at"):
            continue
        # Prefer the record's group. A record without one derives it from its
        # members; its own action is the final available group evidence.
        group = metadata.get("group") or run_addressing.workflow_group(metadata)
        if not group and metadata.get("action"):
            group = run_actions.action_group(str(metadata["action"]))
        if not group:
            continue
        previous = latest.get(str(group))
        if previous is None or str(metadata.get("updated_at")) > str(previous.get("updated_at")):
            latest[str(group)] = metadata
    return {group: _workflow_run_row(metadata) for group, metadata in latest.items()}


def workflow_last_run_by_effect(
    namespace_root: Path, key: str, segments: list[str] | None = None
) -> dict[str, dict]:
    """The latest workflow run for each public mutability effect.

    Internal plan and readonly state channels remain independent on disk. Public
    workflow status combines them as ``non_mutative`` and selects the newest run
    of that effect; its exact member actions remain on the row. Mutative runs are
    unaffected because provision and destroy already share one internal channel.
    """

    runs_dir = (
        Path(namespace_root)
        / run_addressing.compose_state_relpath("workflow", key, segments or [])
        / "runs"
    )
    if not runs_dir.is_dir():
        return {}
    latest: dict[str, tuple[str, dict]] = {}
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        metadata = state_run_store.load_run_metadata(run_dir)
        if not metadata.get("updated_at"):
            continue
        state_group = metadata.get("group") or run_addressing.workflow_group(metadata)
        if not state_group and metadata.get("action"):
            state_group = run_actions.action_group(str(metadata["action"]))
        if not state_group or state_group == run_actions.Group.MAINTENANCE:
            continue
        effect = metadata.get("effect") or run_addressing.workflow_effect(metadata)
        if not effect:
            continue
        previous = latest.get(str(effect))
        if previous is None or str(metadata["updated_at"]) > str(
            previous[1].get("updated_at")
        ):
            latest[str(effect)] = (str(state_group), metadata)
    return {
        effect: {
            "state_group": state_group,
            "row": _workflow_run_row(metadata),
        }
        for effect, (state_group, metadata) in latest.items()
    }


def workflow_last_run(
    namespace_root: Path, key: str, segments: list[str] | None = None
) -> dict | None:
    """The most recent run of one workflow INSTANCE — its record, not its state.

    a workflow owns execution, so there is no pointer to read. The row
    is the last run: what it did, what selected its members, and which members it
    ran with.

    per INSTANCE. One key fanned across environments used to report a
    single row — whichever child finished last — so "did this succeed in test?"
    was unanswerable from the workflow.
    """
    runs_dir = (
        namespace_root / run_addressing.compose_state_relpath("workflow", key, segments) / "runs"
    )
    if not runs_dir.is_dir():
        return None
    records = []
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        metadata = state_run_store.load_run_metadata(run_dir)
        if not metadata.get("updated_at"):
            continue
        records.append(metadata)
    if not records:
        return None
    latest = max(records, key=lambda m: str(m.get("updated_at") or ""))
    return _workflow_run_row(latest)


def _workflow_run_row(metadata: dict) -> dict:
    """One run record as a row: what it did, what selected its members, and which
    members it ran with."""

    row: dict = {"status": _run_conclusion(metadata), "time": metadata.get("updated_at")}
    if metadata.get("operation"):
        row["last_operation"] = metadata["operation"]
    actions = metadata.get("actions") or run_addressing.workflow_actions(metadata)
    if actions:
        row["actions"] = list(actions)
    # The matched member's own selector block, copied verbatim: it points back at
    # the cfg that produced this run, and nothing depends on the engine knowing a
    # field called "operation". A member that matched with none omits it.
    if metadata.get("member_selectors"):
        row["selectors"] = metadata["member_selectors"]
    if metadata.get("default_action"):
        row["default_action"] = metadata["default_action"]
    # A workflow publishes no pointer, so its run record is the only place the
    # invocation label can be read from.
    if metadata.get("label"):
        row["label"] = metadata["label"]
    # The group this run belonged to. Prefer the recorded value; a record without
    # one derives it from its available member or runner action evidence.
    group = metadata.get("group") or run_addressing.workflow_group(metadata)
    if group:
        row["group"] = group
    target_instances = metadata.get("target_instances")
    if target_instances:
        # Qualified for the same reason the parent link is: these point at the
        # TARGET rows in this map, and an address a reader can paste back into a
        # query beats one they have to prefix by hand.
        row["target_instances"] = [
            run_addressing.qualified_address("target", entry)
            if isinstance(entry, str)
            else {**entry, "instance": run_addressing.qualified_address("target", entry["instance"])}
            for entry in target_instances
        ]
    return row


def _run_conclusion(metadata: dict) -> str:
    """A run's outcome, in the vocabulary a target row already uses."""

    status = str(metadata.get("status") or "")
    if status == state_run_store.RunStatus.OK:
        return Verdict.PASSED
    if status == state_run_store.RunStatus.IN_PROGRESS:
        return Verdict.RUNNING
    return Verdict.FAILED if status else Verdict.PASSED


def maintenance_status_rows(namespace_root: Path) -> list[dict]:
    """Durable maintenance activity, newest first.

    Direct target maintenance remains under the target owner. Maintenance-runner
    activity uses the dedicated ``maintenance`` owner. Ctl-state history pruning
    is intentionally out of band and owns an immutable audit manifest instead.
    This projection reads every owner without folding maintenance into ordinary
    target/workflow status.
    """

    namespace_root = Path(namespace_root)
    rows: list[dict] = []

    target_instances: set[tuple[str, tuple[str, ...]]] = set()
    maintenance_group = run_actions.Group.MAINTENANCE
    target_paths = [
        pointer.parent.parent
        for pointer in namespace_root.rglob(f"committed/{maintenance_group}.yaml")
    ]
    for state in state_run_store.STATE_SLOT_NAMES:
        target_paths.extend(
            slot.parent.parent.parent
            for slot in namespace_root.rglob(
                f"{state}/{maintenance_group}/STATUS.yaml"
            )
        )
    for instance_dir in target_paths:
        parsed = run_addressing.parse_state_relpath(namespace_root, instance_dir)
        if parsed is None or parsed["kind"] != run_actions.ResultKind.TARGET:
            continue
        target_instances.add(
            (parsed["key"], tuple(parsed["instance_segments"]))
        )
    for key, segments_tuple in target_instances:
        segments = list(segments_tuple)
        address = run_addressing.target_instance_address(key, segments)
        computed = compute_target_instance_status(
            namespace_root,
            run_actions.Action.MAINTENANCE,
            {
                "kind": "target",
                "key": key,
                "segments": segments,
                "address": address,
                "relations": {},
            },
        )
        if not computed.get("status"):
            continue
        row = {
            "source": "target",
            "operation": run_actions.Action.MAINTENANCE,
            "status": computed["status"],
            "subject": run_addressing.qualified_address("target", address),
            "time": computed.get("time"),
            "id": computed.get("run_id"),
        }
        for field in ("last_action", "label"):
            if computed.get(field):
                row[field] = computed[field]
        rows.append(row)

    maintenance_root = namespace_root / run_actions.ResultKind.MAINTENANCE
    if maintenance_root.is_dir():
        for runs_dir in maintenance_root.rglob("runs"):
            parsed = run_addressing.parse_state_relpath(
                namespace_root, runs_dir.parent
            )
            if parsed is None or parsed["kind"] != run_actions.ResultKind.MAINTENANCE:
                continue
            operation, separator, subject = str(parsed["key"]).partition("/")
            for run_dir in runs_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                metadata = state_run_store.load_run_metadata(run_dir)
                if not metadata.get("updated_at"):
                    continue
                row = {
                    "source": "run",
                    "operation": metadata.get("maintenance_operation") or operation,
                    "status": _run_conclusion(metadata),
                    "time": metadata["updated_at"],
                    "id": metadata.get("run_id") or run_dir.name,
                }
                resolved_subject = metadata.get("maintenance_subject") or (
                    subject if separator else None
                )
                if resolved_subject:
                    row["subject"] = resolved_subject
                if metadata.get("maintenance_scope"):
                    row["scope"] = metadata["maintenance_scope"]
                if (metadata.get("error") or {}).get("summary"):
                    row["error"] = metadata["error"]["summary"]
                rows.append(row)

    audit_root = namespace_root / "_maintenance"
    if audit_root.is_dir():
        for manifest_path in audit_root.rglob("manifest.yaml"):
            manifest = kernel_yaml_io.load_yaml(manifest_path) or {}
            if not isinstance(manifest, dict):
                continue
            operation = manifest.get("operation")
            timestamp = manifest.get("applied_at") or manifest.get("created_at")
            identifier = manifest.get("maintenance_id") or manifest_path.parent.name
            if not operation or not timestamp:
                continue
            row = {
                "source": "manifest",
                "operation": operation,
                "status": (
                    "applied"
                    if manifest.get("applied_at")
                    else "dry_run"
                    if manifest.get("dry_run") is True
                    else "recorded"
                ),
                "time": timestamp,
                "id": identifier,
            }
            for field in (
                "scope", "selection", "candidate_run_ids", "object_keys",
                "delete_object_versions",
            ):
                if field in manifest:
                    row[field] = manifest[field]
            rows.append(row)

    return sorted(
        rows,
        key=lambda row: (str(row.get("time") or ""), str(row.get("id") or "")),
        reverse=True,
    )


def compute_namespace_status_map(
    namespace_root: Path,
    relations: dict | None = None,
    exclusive_workflow_relations: dict | None = None,
) -> dict[str, dict[str, dict[str, dict[str, str]]]]:
    """Every target and workflow instance under the namespace root.

    Target rows retain their internal result groups. Workflow rows expose only
    their public mutability effect and preserve exact actions as row data:

        status      running | passed | failed
        group       mutative | non_mutative       (workflows)
        actions     exact resolved member actions (workflows)
        freshness   up_to_date | outdated     (a published deployment only)
        time        when the record was published

    `status` comes from the live run slots — a success clears the failed slot, so
    `failed` cannot outlive the run that caused it — and `freshness` from the
    published pointer. Fan-outs own no state and never appear."""
    namespace_root = Path(namespace_root)
    if not namespace_root.is_dir():
        return {}
    targets: set[tuple[str, tuple[str, ...]]] = set()
    workflows: set[tuple[str, tuple[str, ...]]] = set()
    # An instance is discovered by anything it has PUBLISHED or is DOING. Scanning
    # only for committed pointers hid a first-ever run entirely: it has written a
    # slot and no pointer, so `--all` reported an empty namespace while a run was
    # in flight.
    discovered: list[Path] = [
        pointer.parent.parent for pointer in namespace_root.rglob("committed/*.yaml")
    ]
    for state in state_run_store.STATE_SLOT_NAMES:
        discovered += [
            slot.parent.parent.parent
            for slot in namespace_root.rglob(f"{state}/*/STATUS.yaml")
        ]
    for instance_dir in discovered:
        parsed = run_addressing.parse_state_relpath(namespace_root, instance_dir)
        if parsed is None or parsed["kind"] != "target":
            continue
        targets.add((parsed["key"], tuple(parsed["instance_segments"])))
    # A workflow is discovered by its RUNS. It publishes no pointer, so
    # there is nothing else to find it by. the parsed segments are KEPT,
    # so a key fanned across environments yields one row per environment instead of
    # collapsing to whichever child ran last.
    workflow_root = namespace_root / "workflow"
    if workflow_root.is_dir():
        for runs_dir in workflow_root.rglob("runs"):
            parsed = run_addressing.parse_state_relpath(namespace_root, runs_dir.parent)
            if parsed is not None and parsed["kind"] == "workflow":
                workflows.add((parsed["key"], tuple(parsed["instance_segments"])))
    rows: dict[str, dict[str, dict[str, dict[str, str]]]] = {
        kind: {} for kind in run_actions.STATUS_RESULT_KINDS
    }
    for key, seg in targets:
        segments = list(seg)
        address = run_addressing.target_instance_address(key, segments)
        groups: dict[str, dict[str, str]] = {}
        spec = {
            "kind": "target",
            "key": key,
            "segments": segments,
            "address": address,
            "relations": relations or {},
        }
        for group in run_actions.STATUS_RESULT_GROUPS:
            axes = run_addressing._axis_row(
                compute_target_instance_status(
                    namespace_root, run_actions.STATUS_GROUP_ACTION[group], spec
                )
            )
            # An empty group means no run of that class ever touched this
            # instance — it is omitted rather than reported as anything.
            if axes:
                groups[group] = axes
        if groups:
            run_addressing._place_instance(rows, "target", key, segments, groups)
    for key, seg in workflows:
        segments = list(seg)
        # Workflow rows are keyed by public EFFECT. Internal plan and readonly
        # channels are deliberately not exposed as competing workflow statuses;
        # the exact resolved actions state what the selected run actually did.
        groups: dict[str, dict] = {}
        for effect, result in workflow_last_run_by_effect(
            namespace_root, key, segments
        ).items():
            state_group = result["state_group"]
            run_row = result["row"]
            row = {k: v for k, v in run_row.items() if k not in ("group", "target_instances")}
            members = list(run_row.get("target_instances") or [])
            default_action = run_row.get("default_action") or (
                run_actions.STATUS_GROUP_ACTION[state_group]
            )
            freshness, member_rows, _ = workflow_member_freshness(
                namespace_root,
                str(default_action),
                members,
                relations,
            )
            # No `reasons`: every member carries its own verdict below, so a
            # reason string would restate what the reader can already see.
            address = run_addressing.workflow_instance_address(key, segments)
            standing, superseded_by = StandingResolver(namespace_root).workflow(
                exclusive_workflow_relations or {},
                address,
                member_rows,
            )
            groups[effect] = _ordered_workflow_row(
                row,
                standing=standing,
                superseded_by=superseded_by,
                freshness=freshness,
                members=member_rows,
            )
        if not groups:
            continue
        if segments:
            run_addressing._place_instance(rows, "workflow", key, segments, groups)
        else:
            rows["workflow"][key] = groups
    # Kind is the OUTER key, so a reader sees where the workflows are and where
    # the targets are without parsing a prefix off every address.
    return {
        kind: dict(sorted(instances.items()))
        for kind, instances in rows.items()
        if instances
    }


class SortField(StrEnum):
    """What `--sort` may order a status map by: every scalar fact a row carries.

    One vocabulary, in one order, because a sortable field and a table column are
    the same thing — the question "which of these ran last / failed / is
    unlabelled" is asked by ordering the column you are already reading. The
    table's `FLAT_COLUMNS` IS this tuple; declaring them separately would let a
    column appear that could not be sorted by, with nothing to say why.

    `time`, not `at`: a field name has to work as both a column heading and a
    `--sort` value, and `--sort at` reads as an unfinished sentence.
    """

    ADDRESS = "address"
    GROUP = "group"
    STATUS = "status"
    LAST_ACTION = "last_action"
    LAST_OPERATION = "last_operation"
    ACTIONS = "actions"
    STANDING = "standing"
    FRESHNESS = "freshness"
    TIME = "time"
    MEMBERS = "members"
    LABEL = "label"


class SortDirection(StrEnum):
    """The optional `:asc|desc` half of `--sort`."""

    ASC = "asc"
    DESC = "desc"


class StatusStructure(StrEnum):
    """The shape `--all` emits.

    A tree cannot express a globally chronological order, so the two shapes are a
    real choice and neither is a default.
    """

    NESTED = "nested"
    FLAT = "flat"


SORT_FIELDS = tuple(SortField)
STATUS_STRUCTURES = tuple(StatusStructure)

# What a NESTED map can be ordered by. A flat row has one value per field, so
# every field orders it. A nested one orders SETS — a template holds many
# instances, an instance many groups — and only these two aggregate over a set
# without inventing a meaning: a template's name is its own, and its newest row
# is the one a reader is looking for. "The worst status in this template" or "the
# largest label under it" are not questions, and answering them deterministically
# would be worse than refusing, because the order would look considered.
NESTED_SORT_FIELDS = (SortField.ADDRESS, SortField.TIME)

# What `--filter` may narrow by: every sortable field, plus `kind`.
#
# `kind` is filterable without being a column. The flat shape carries it as the
# first segment of `address` and the nested shape as its outer key, so printing
# it again would repeat what a reader can already see — but "only the workflows"
# is a question worth asking, and it has to be askable through the one mechanism.
FILTER_FIELDS = ("kind", *SORT_FIELDS)


def parse_filters(items: list[str] | None) -> dict[str, list[str]]:
    """`FIELD=VALUE` pairs -> `{field: [values]}`, order preserved.

    Repeating a field collects ALTERNATIVES rather than overwriting:
    `group=non_mutative,group=mutative` is either of them, and different fields
    all have to hold. A single trailing `*` declares prefix matching; every
    other value is exact.
    """

    filters: dict[str, list[str]] = {}
    for item in items or []:
        field, separator, value = item.partition("=")
        field, value = field.strip(), value.strip()
        if not separator or not field or not value:
            raise RuntimeError(
                f"❌ --filter must use FIELD=VALUE, got: {item!r}"
            )
        if field not in FILTER_FIELDS:
            raise RuntimeError(
                f"❌ --filter field {field!r} unknown; expected one of "
                f"{', '.join(FILTER_FIELDS)}"
            )
        if "*" in value and (
            value == "*" or value.count("*") != 1 or not value.endswith("*")
        ):
            raise RuntimeError(
                "❌ --filter supports one trailing * after a non-empty prefix, "
                f"got: {value!r}"
            )
        values = filters.setdefault(field, [])
        if value not in values:
            values.append(value)
    return filters


def parse_sort(raw: str, *, structure: str | None = None) -> list[tuple[str, bool]]:
    """`<field>[:asc|desc][,<field>[:asc|desc]...]` -> ordered `(field, descending)`.

    A LIST, because one field rarely decides an order on its own: sorting by
    `members` puts every one-member workflow together and says nothing about
    which of them ran last, and sorting by `status` groups the failures without
    ordering them. Keys apply left to right — the first decides, each later one
    breaks the ties the earlier ones left — and each carries its OWN direction,
    so `members:desc,time` is the biggest compositions, oldest first.

    `structure` narrows the field set when it is known; omitting it validates the
    fields alone, which is what a caller ordering an already-shaped map needs.
    """

    keys: list[tuple[str, bool]] = []
    # Split here rather than through parse_comma_list, which DEDUPES: it exists
    # for name lists where a repeat is harmless, and silently collapsing
    # `time,time` would make the duplicate guard below unreachable for exactly
    # the spelling most likely to be a mistake.
    for item in (part.strip() for part in str(raw).split(",")):
        if not item:
            continue
        field, _, direction = item.partition(":")
        if field not in SORT_FIELDS:
            raise RuntimeError(
                f"❌ --sort field {field!r} unknown; expected one of {', '.join(SORT_FIELDS)}"
            )
        if structure == StatusStructure.NESTED and field not in NESTED_SORT_FIELDS:
            raise RuntimeError(
                f"❌ --sort {field!r} shapes --structure flat, where a row has one "
                f"value for it; a nested map orders sets of rows, which only "
                f"{', '.join(NESTED_SORT_FIELDS)} do without inventing a meaning"
            )
        if direction not in ("", SortDirection.ASC, SortDirection.DESC):
            raise RuntimeError(f"❌ --sort direction {direction!r} must be asc or desc")
        if any(field == existing for existing, _ in keys):
            raise RuntimeError(
                f"❌ --sort names {field!r} twice; a field that already decided "
                "the order cannot break its own ties"
            )
        keys.append((field, direction == SortDirection.DESC))
    if not keys:
        raise RuntimeError("❌ --sort must name at least one field")
    return keys


def sort_rows(rows: list[dict], keys: list[tuple[str, bool]]) -> list[dict]:
    """Order rows by every key, the first deciding and the rest breaking ties.

    Applied from the LAST key backwards over a stable sort, which is the standard
    way to get lexicographic ordering with a per-key direction — building one
    composite key cannot, because reversing it would reverse every field at once.

    Address and group are the final tie-break, always ascending: two runs can
    agree on every declared key, and an order that then varies between two reads
    of the same namespace is unusable for comparing them.
    """

    ordered = sorted(rows, key=lambda row: (row.get("address") or "", row.get("group") or ""))
    for field, descending in reversed(keys):
        ordered.sort(key=lambda row: sort_value(row, field), reverse=descending)
    return ordered


def actions_text(actions) -> str:
    """Exact workflow actions as one shell-safe table/filter value."""

    return "+".join(str(action) for action in actions) if isinstance(actions, list) else ""


def sort_value(row: dict, field: str):
    """One row's ordering value for one field.

    `members` orders by HOW MANY, not by the text of a list: the flat shape
    already renders it as a count, because a row carrying its own address has
    nowhere to nest the objects. `actions` uses the same plus-separated text the
    table displays. Every other field orders as text, and an absent value sorts
    as empty — which puts the rows that never had one together at one end rather
    than scattering them.
    """

    if field == SortField.MEMBERS:
        members = row.get("members")
        return len(members) if isinstance(members, list) else 0
    if field == SortField.ACTIONS:
        return actions_text(row.get("actions"))
    return str(row.get(field) or "")


def walk_status_map(instances: dict):
    """Every row in a nested map, as `(kind, template, segments, group, row)`.

    ONE walk, used by both the filter and the flat shape, because they have to
    agree on what a row IS. They did not have to before — the filter matched on
    the tree's own keys while only the flat shape ever composed an address — and
    a general filter that matches on `address` makes the two answer the same
    question, which is exactly where a second walk would drift.

    `segments` is the instance path exactly as the state layout writes it —
    `param=value` joined by `/` — and empty for a singleton, which has none.
    """

    for kind, templates in instances.items():
        for template, body in templates.items():
            bodies = (
                list(body[run_addressing.INSTANCES_MARKER].items())
                if run_addressing.INSTANCES_MARKER in body
                else [("", body)]
            )
            for segments, groups in bodies:
                for group, row in groups.items():
                    yield kind, template, segments or "", group, row


def flat_row(kind: str, template: str, segments: str, group: str, row: dict) -> dict:
    """One row of the flat shape: its own address, its group, then its axes.

    The kind is a path SEGMENT of the address rather than a field of its own —
    splitting the flat list by kind would break a globally chronological order for
    the same reason template nesting does.
    """

    address = "/".join([
        kind,
        run_addressing.instance_address(
            template, segments.split("/") if segments else []
        ),
    ])
    return {"address": address, "group": group, **row}


def structure_status_map(instances: dict, structure: str, sort: str) -> dict:
    """Order and shape a status map.

    `nested` keeps the kind -> template -> instances tree and sorts on TWO levels:
    templates by their newest instance, instances by their newest group. A tree
    cannot express a globally chronological order — grouping and global ordering
    are in conflict — so `flat` exists for that: a LIST of one row per group,
    each carrying its own address, which can be ordered by every field a row has.
    """

    keys = parse_sort(sort, structure=structure)

    if structure == StatusStructure.FLAT:
        return {
            "instances": sort_rows(
                [flat_row(*entry) for entry in walk_status_map(instances)], keys
            )
        }

    # A nested map orders SETS, so it uses only the fields that aggregate over
    # one (parse_sort has already refused the others). Applied from the last key
    # backwards over a stable sort, exactly as sort_rows does for a flat row.
    def _by(keys, value_of):
        def order(items):
            ordered = list(items)
            for field, descending in reversed(keys):
                ordered.sort(key=lambda item: value_of(item, field), reverse=descending)
            return ordered
        return order

    def _template_value(item, field):
        template, body = item
        if field == SortField.ADDRESS:
            return template
        rows = (
            body[run_addressing.INSTANCES_MARKER].values()
            if run_addressing.INSTANCES_MARKER in body
            else [body]
        )
        return max((kernel_paths._newest(row) for row in rows), default="")

    def _instance_value(item, field):
        segments, groups = item
        return segments if field == SortField.ADDRESS else kernel_paths._newest(groups)

    order_templates = _by(keys, _template_value)
    order_instances = _by(keys, _instance_value)
    ordered: dict = {}
    for kind, templates in instances.items():
        kind_out: dict = {}
        for template, body in order_templates(templates.items()):
            if run_addressing.INSTANCES_MARKER in body:
                kind_out[template] = {
                    run_addressing.INSTANCES_MARKER: dict(
                        order_instances(body[run_addressing.INSTANCES_MARKER].items())
                    )
                }
            else:
                kind_out[template] = body
        ordered[kind] = kind_out
    return ordered


def _filter_value_matches(value, pattern: str) -> bool:
    """Exact match, or prefix match when the declared pattern ends in `*`."""

    text = str(value)
    return text.startswith(pattern[:-1]) if pattern.endswith("*") else text == pattern


def filter_status_map(instances: dict, filters: dict[str, list[str]] | None) -> dict:
    """Narrow a namespace map to the rows matching every filter.

    Values of ONE field are alternatives
    (`group=non_mutative,group=mutative` is either), and different fields all
    have to hold. A value with one trailing `*` matches that prefix; a value
    without it remains exact.

    A template or instance whose every row is filtered out is DROPPED rather than
    shown empty: an empty one would read as "nothing happened here", which is a
    different claim from "you asked not to see it".

    Matching is on the row as the FLAT shape presents it, so what a filter tests
    is what a reader can see — including `address`, which the nested tree spells
    across three levels and never as one string.
    """

    if not filters:
        return instances

    def matches(kind, template, segments, group, row) -> bool:
        candidate = {"kind": kind, **flat_row(kind, template, segments, group, row)}
        return all(
            any(
                _filter_value_matches(sort_value(candidate, field), pattern)
                for pattern in patterns
            )
            for field, patterns in filters.items()
        )

    selected: dict = {}
    for kind, template, segments, group, row in walk_status_map(instances):
        if not matches(kind, template, segments, group, row):
            continue
        templates = selected.setdefault(kind, {})
        if segments:
            templates.setdefault(template, {run_addressing.INSTANCES_MARKER: {}})[
                run_addressing.INSTANCES_MARKER
            ].setdefault(segments, {})[group] = row
        else:
            templates.setdefault(template, {})[group] = row
    return selected


def latest_child_revision(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
    action: str | None = None,
) -> dict | None:
    """The revision a SPAWNED child just committed.

    The child publishes its own committed pointer, so the parent reads it back
    rather than being told — the same record any later run would consult.
    """
    instance_dir, address = state_run_store.target_instance_dir_for_run(
        parent_run_dir, target_run, execution_context, action
    )
    # Read the GROUP this child published into. Defaulting to
    # `deployment` made a plan child invisible to its workflow, which then
    # committed with no child_revisions at all — a composition recording nothing.
    resolved_action = action or state_run_store.load_run_metadata(parent_run_dir).get("action")
    pointer = state_run_store.read_committed_pointer(instance_dir, run_actions.action_group(str(resolved_action)))
    if not pointer:
        return None
    return {
        "address": address,
        "run_id": pointer.get("run_id"),
        "snapshot_sha256": pointer.get("snapshot_sha256"),
        "status": pointer.get("status"),
    }


def up_to_date_child_revision(
    parent_run_dir: Path,
    target_run: dict,
    execution_context: dict[str, object],
    action: str | None = None,
) -> dict | None:
    """The published revision, only when reusing it is still correct.

    the ACTION is compared like every other identity field. Without it
    a workflow holding one target under two actions skips BOTH members — the six
    content fields match either way, because source and cfg are identical — so a
    run that destroyed and then failed to re-provision reports success while the
    instance is still destroyed. The target's resolved per-action policy is
    checked here as a final defensive gate, so no caller can reuse a committed
    result for an action the target classified as non-reusable."""

    if target_run.get("reuse_committed_result") is not True:
        return None
    if target_run.get("ref_policy") != "commit_required":
        return None
    if target_run.get("source_state") != "clean":
        return None
    source_commit = target_run.get("source_commit")
    cfg_source_commit = target_run.get("cfg_source_commit")
    target_definition_sha256 = target_run.get("target_definition_sha256")
    target_cfg_view_sha256 = target_run.get("target_cfg_view_sha256")
    if not all((
        source_commit,
        cfg_source_commit,
        target_definition_sha256,
        target_cfg_view_sha256,
    )):
        return None
    instance_dir, address = state_run_store.target_instance_dir_for_run(
        parent_run_dir, target_run, execution_context, action
    )
    resolved_action = action or state_run_store.load_run_metadata(parent_run_dir).get("action")
    pointer = state_run_store.read_committed_pointer(instance_dir, run_actions.action_group(str(resolved_action)))
    if not pointer or pointer.get("status") == state_run_store.RunStatus.OUTDATED or pointer.get("outdated"):
        return None
    expected = {
        "action": action or state_run_store.load_run_metadata(parent_run_dir).get("action"),
        "source_commit": source_commit,
        "cfg_source_commit": cfg_source_commit,
        "source_state": "clean",
        "ref_policy": "commit_required",
        "target_definition_sha256": target_definition_sha256,
        "target_cfg_view_sha256": target_cfg_view_sha256,
    }
    if any(pointer.get(key) != value for key, value in expected.items()):
        return None
    snapshot_path = (
        instance_dir / "runs" / str(pointer.get("run_id") or "") / state_run_store.RUN_METADATA_FILENAME
    )
    if not snapshot_path.is_file():
        return None
    snapshot = kernel_yaml_io.load_yaml(snapshot_path) or {}
    if not isinstance(snapshot, dict):
        return None
    canonical = json.dumps(
        snapshot, separators=(",", ":"), sort_keys=True, default=str
    )
    if hashlib.sha256(canonical.encode("utf-8")).hexdigest() != pointer.get(
        "snapshot_sha256"
    ):
        return None
    return {
        "address": address,
        "run_id": pointer.get("run_id"),
        "snapshot_sha256": pointer.get("snapshot_sha256"),
        "status": pointer.get("status"),
        "skipped_committed_rerun": True,
    }
