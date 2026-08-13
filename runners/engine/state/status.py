"""What the recorded state MEANS: fresh, stale, failed, absent.

A verdict is computed from run records, never stored, so it cannot drift from
them. Only the forward side of a paired action can go stale — a destroy record
describes an instance that is gone, and nothing can make that answer stale."""

import hashlib
import json
from enum import StrEnum
from pathlib import Path

from engine.kernel import ids as kernel_ids
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
    return f"{action} failed{mutated} under run {run_id}" + (f": {summary}" if summary else "")


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


def _freshness(pointer: dict | None, spec: dict) -> tuple[str, list[str]]:
    """Does the committed record still match what is declared?

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
        return {
            "status": None,
            "reasons": [],
            "mutation_started": False,
            "run_id": None,
            "time": None,
            "action": None,
            "label": None,
            "parent_workflow_instance": None,
            "parent_workflow_run_id": None,
        }
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


def compute_target_instance_status(namespace_root: Path, action: str, spec: dict) -> dict:
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
        result["superseded_by"] = run_addressing.qualified_address("target", superseded_by)

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


def compute_workflow_instance_status(namespace_root: Path, action: str, spec: dict) -> dict:
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
            / run_addressing.compose_state_relpath(
                "target", target_spec["key"], target_spec["segments"]
            ),
            group,
        )
        expected = recorded.get(target_spec["address"])
        if child.get("freshness") == Freshness.OUTDATED:
            drift.append(f"{target_spec['address']}: outdated")
        elif expected is None:
            drift.append(f"{target_spec['address']}: not recorded by workflow")
        elif expected.get("run_id") != (child_pointer or {}).get("run_id") or expected.get(
            "snapshot_sha256"
        ) != (child_pointer or {}).get("snapshot_sha256"):
            drift.append(f"{target_spec['address']}: committed revision changed")
        children.append(child)

    if pointer is not None:
        if pointer.get("workflow_definition_sha256") != spec["workflow_definition_sha256"]:
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
                if entry.get("standing") == Standing.SUPERSEDED and entry.get("superseded_by")
            }
            if not replaced_by:
                return Standing.ACTIVE, None
            for sibling in members:
                if sibling == address:
                    continue
                sibling_key, sibling_segments = run_addressing.split_target_instance_address(
                    sibling
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

    def _target_evidence(self, relations: dict, segments: list[str]) -> dict[str, dict]:
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
                        f"❌ target standing evidence for {member!r} must record a mutating action"
                    )
                if not isinstance(event.get("committed_at"), str) or not event["committed_at"]:
                    raise RuntimeError(
                        f"❌ target standing evidence for {member!r} must record committed_at"
                    )
                if not isinstance(event.get("run_id"), str) or not event["run_id"]:
                    raise RuntimeError(
                        f"❌ target standing evidence for {member!r} must record run_id"
                    )
            coordinates = [(event["committed_at"], event["run_id"]) for event in evidence.values()]
            if len(coordinates) != len(set(coordinates)):
                raise RuntimeError(
                    f"❌ target standing evidence for relation members {members} "
                    "must have distinct commit coordinates"
                )
            in_effect = max(
                evidence,
                key=lambda member: (evidence[member]["committed_at"], evidence[member]["run_id"]),
            )
            if evidence[in_effect]["action"] != run_actions.Action.PROVISION:
                return None, None
            if address == in_effect:
                return Standing.ACTIVE, None
            return Standing.SUPERSEDED, in_effect
        return None, None


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
        if previous is None or str(metadata["updated_at"]) > str(previous[1].get("updated_at")):
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

    row: dict = {"status": run_conclusion(metadata), "time": metadata.get("updated_at")}
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
            else {
                **entry,
                "instance": run_addressing.qualified_address("target", entry["instance"]),
            }
            for entry in target_instances
        ]
    return row


def run_conclusion(metadata: dict) -> str:
    """A run's outcome, in the vocabulary a target row already uses."""

    status = str(metadata.get("status") or "")
    if status == state_run_store.RunStatus.OK:
        return Verdict.PASSED
    if status == state_run_store.RunStatus.IN_PROGRESS:
        return Verdict.RUNNING
    return Verdict.FAILED if status else Verdict.PASSED


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
    pointer = state_run_store.read_committed_pointer(
        instance_dir, run_actions.action_group(str(resolved_action))
    )
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
    if not all(
        (
            source_commit,
            cfg_source_commit,
            target_definition_sha256,
            target_cfg_view_sha256,
        )
    ):
        return None
    instance_dir, address = state_run_store.target_instance_dir_for_run(
        parent_run_dir, target_run, execution_context, action
    )
    resolved_action = action or state_run_store.load_run_metadata(parent_run_dir).get("action")
    pointer = state_run_store.read_committed_pointer(
        instance_dir, run_actions.action_group(str(resolved_action))
    )
    if (
        not pointer
        or pointer.get("status") == state_run_store.RunStatus.OUTDATED
        or pointer.get("outdated")
    ):
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
        instance_dir
        / "runs"
        / str(pointer.get("run_id") or "")
        / state_run_store.RUN_METADATA_FILENAME
    )
    if not snapshot_path.is_file():
        return None
    snapshot = kernel_yaml_io.load_yaml(snapshot_path) or {}
    if not isinstance(snapshot, dict):
        return None
    canonical = json.dumps(snapshot, separators=(",", ":"), sort_keys=True, default=str)
    if hashlib.sha256(canonical.encode("utf-8")).hexdigest() != pointer.get("snapshot_sha256"):
        return None
    return {
        "address": address,
        "run_id": pointer.get("run_id"),
        "snapshot_sha256": pointer.get("snapshot_sha256"),
        "status": pointer.get("status"),
        "skipped_committed_rerun": True,
    }
