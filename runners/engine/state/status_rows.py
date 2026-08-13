"""Turning computed status into the rows a reader is shown.

One row per instance, per workflow, per maintenance result. It asks the
computation for each verdict and never performs one, which is the whole of
the split.
"""

from pathlib import Path

from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.state import run_store as state_run_store
from engine.state import status as state_status


def _targeted_workflow_status(namespace_root: Path, action: str, spec: dict) -> dict:
    """A workflow owns execution, so its `status` comes from its LAST RUN — and
    its `freshness` from its members, who each answer for themselves.

    Reading a committed pointer here returned an empty row once workflows stopped
    publishing one; a workflow still publishes none, so the run record is the
    only source for what it DID. What is DEPLOYED is the members' answer.
    """

    result = {"kind": "workflow", "key": spec["key"], "address": spec["address"]}
    last_run = state_status.workflow_last_run(
        namespace_root, spec["key"], spec.get("segments") or []
    )
    if last_run:
        result.update({k: v for k, v in last_run.items() if k not in ("target_instances",)})
    members = [target_spec["address"] for target_spec in (spec.get("target_specs") or [])] or list(
        (last_run or {}).get("target_instances") or []
    )
    freshness, member_rows, _ = workflow_member_freshness(
        namespace_root, action, members, spec.get("relations") or {}
    )
    standing, superseded_by = state_status.StandingResolver(namespace_root).workflow(
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
            state_status.compute_target_instance_status(namespace_root, action, spec)
            if spec["kind"] == "target"
            else _targeted_workflow_status(namespace_root, action, spec)
        )
        computed["selection"] = selection_label
        results.append(computed)
    return results


def _ordered_workflow_row(
    row: dict,
    *,
    standing: state_status.Standing | None,
    freshness: str | None,
    members: list[dict],
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
        ordered["superseded_by"] = run_addressing.qualified_address("workflow", superseded_by)
    if freshness is not None:
        ordered["freshness"] = freshness
    for key in (
        "last_operation",
        "actions",
        "time",
        "run_id",
        "selectors",
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
    namespace_root: Path,
    action: str,
    member_addresses: list[str],
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
        child = state_status.compute_target_instance_status(
            namespace_root,
            member_action,
            {
                "kind": "target",
                "key": key,
                "segments": segments,
                "address": unqualified,
                "relations": relations or {},
            },
        )
        entry = {"address": run_addressing.qualified_address("target", unqualified)}
        if isinstance(recorded, dict) and recorded.get("action"):
            entry["action"] = str(recorded["action"])
        # A nested workflow table is useful only when its members answer as
        # target rows rather than as address-only annotations. The child has
        # already computed these facts, so retain the status-report vocabulary
        # here instead of making the renderer read state or invent another report.
        for field in (
            "status",
            "last_action",
            "standing",
            "superseded_by",
            "freshness",
            "time",
            "label",
        ):
            if child.get(field) is not None:
                entry[field] = str(child[field])
        members.append(entry)

    verdicts = {entry["freshness"] for entry in members if "freshness" in entry}
    for value in (
        state_status.Freshness.OUTDATED,
        state_status.Freshness.UNDETERMINED,
        state_status.Freshness.UP_TO_DATE,
    ):
        if str(value) not in verdicts:
            continue
        if value == state_status.Freshness.UP_TO_DATE:
            return str(value), members, []
        return (
            str(value),
            members,
            [
                f"{entry['address']}: {entry['freshness']}"
                for entry in members
                if entry.get("freshness") == str(value)
            ],
        )
    return None, members, []


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
            for slot in namespace_root.rglob(f"{state}/{maintenance_group}/STATUS.yaml")
        )
    for instance_dir in target_paths:
        parsed = run_addressing.parse_state_relpath(namespace_root, instance_dir)
        if parsed is None or parsed["kind"] != run_actions.ResultKind.TARGET:
            continue
        target_instances.add((parsed["key"], tuple(parsed["instance_segments"])))
    for key, segments_tuple in target_instances:
        segments = list(segments_tuple)
        address = run_addressing.target_instance_address(key, segments)
        computed = state_status.compute_target_instance_status(
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
            parsed = run_addressing.parse_state_relpath(namespace_root, runs_dir.parent)
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
                    "status": state_status.run_conclusion(metadata),
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
                "scope",
                "selection",
                "candidate_run_ids",
                "object_keys",
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
            slot.parent.parent.parent for slot in namespace_root.rglob(f"{state}/*/STATUS.yaml")
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
                state_status.compute_target_instance_status(
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
        for effect, result in state_status.workflow_last_run_by_effect(
            namespace_root, key, segments
        ).items():
            state_group = result["state_group"]
            run_row = result["row"]
            row = {k: v for k, v in run_row.items() if k not in ("group", "target_instances")}
            members = list(run_row.get("target_instances") or [])
            default_action = (
                run_row.get("default_action") or (run_actions.STATUS_GROUP_ACTION[state_group])
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
            standing, superseded_by = state_status.StandingResolver(namespace_root).workflow(
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
    return {kind: dict(sorted(instances.items())) for kind, instances in rows.items() if instances}
