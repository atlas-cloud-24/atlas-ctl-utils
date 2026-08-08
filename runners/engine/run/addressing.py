"""How a thing is named, and where its record therefore lives.

An address is derived from the axes that identify an instance, and a state path
is derived from the address, so the two cannot be separated without letting them
disagree. Params ADDRESS an instance; a hash would only identify it."""

import re

from pathlib import Path

from engine.execution import references as execution_references
from engine.run import actions as run_actions

def workflow_target_run_signature(target_run_entry) -> tuple[str, str | None]:
    """Return `(target key, action)` for one workflow target_run entry.

    A bare string is a key with no declared action; a mapping carries both. The
    pair is the entry's IDENTITY: one key repeat when the actions
    differ, so neither half alone distinguishes two entries.
    """

    if isinstance(target_run_entry, str):
        return target_run_entry, None
    if isinstance(target_run_entry, dict):
        return target_run_entry.get("target"), target_run_entry.get("action")
    raise RuntimeError(f"❌ invalid workflow target_run entry: {target_run_entry!r}")


def workflow_target_run_key(target_run_entry) -> str | None:
    """Return the TARGET key an entry addresses.

    Distinct from `get_workflow_target_run_id`: `id` is the run's display
    identity, which the target-run builder may set independently of the target it
    points at. A placement anchor names a target, so it resolves against this.
    """

    return workflow_target_run_signature(target_run_entry)[0]


def get_workflow_target_run_id(target_run_entry) -> str:
    """

    return the target_run id from a workflow target_run entry."""

    if isinstance(target_run_entry, str):
        return target_run_entry
    if isinstance(target_run_entry, dict):
        target_run_id = target_run_entry.get("id")
        if isinstance(target_run_id, str) and target_run_id:
            return target_run_id
    raise RuntimeError(f"❌ invalid workflow target_run entry: {target_run_entry!r}")


def normalize_target_keys(values: list[str], *, label: str) -> list[str]:
    """Just the keys, in order — the shape the run action consumes.

    A key-only view, so it asks for no action: callers that need one go through
    `normalize_target_entries` with the list's declared default."""
    keys: list[str] = []
    for value in values if isinstance(values, list) else []:
        keys.append(run_actions.normalize_result_name(
            value.get("key") if isinstance(value, dict) else value, label=label
        ))
    return keys


def parse_result_dir(ctl_state_local_root: Path, result_dir: Path) -> dict | None:
    """Parse a result dir path into its identity (-aware).

    Layout: <root>/<locator...>/<action>/<run_type>/<result_name...>. The
    locator prefix has variable depth, so the boundary is found by scanning for
    the first <known action>/<known run_type> pair."""
    try:
        rel = Path(result_dir).resolve().relative_to(Path(ctl_state_local_root).resolve())
    except ValueError:
        return None
    parts = rel.parts
    for index in range(len(parts) - 2):
        if parts[index] in run_actions.RUN_ACTIONS and parts[index + 1] in run_actions.RUN_TYPES:
            action, run_type = parts[index], parts[index + 1]
            rest = list(parts[index + 2:])
            # Strip the optional instance layer from the key.
            instance_segments: list[str] = []
            if "instances" in rest:
                marker = rest.index("instances")
                after = rest[marker + 1:]
                instance_segments, _ = split_instance_segments(after)
                rest = rest[:marker]
            result_name = "/".join(rest)
            if not result_name:
                return None
            address = instance_address(result_name, instance_segments)
            return {
                "locator": list(parts[:index]),
                "action": action,
                "run_type": run_type,
                "result_name": result_name,
                "result_key": f"{action}/{run_type}/{result_name}",
                "instance": instance_segments,
                "address": address,
            }
    return None


def rendered_scope_target_dir(plt_rendered_dir: Path, target_path: str) -> Path:
    target_rel = target_path.lstrip("/")
    target_dir = (plt_rendered_dir / target_rel).resolve()
    try:
        target_dir.relative_to(plt_rendered_dir.resolve())
    except ValueError as exc:
        raise RuntimeError(f"Scope target_path escapes rendered cfg dir: {target_path}") from exc
    return target_dir


def split_target_instance_address(address: str) -> tuple[str, list[str]]:
    """

    split a path-form instance address into (key, segments): trailing
    components containing `=` are instance segments; key components never contain
    one (Q1j parse boundary). Workflow and target instances are both addressed by
    `param=value` segments, so neither needs a special case."""

    if not isinstance(address, str) or not address:
        raise RuntimeError("❌ target instance address must be a non-empty string")
    parts = address.split("/")
    idx = len(parts)
    while idx > 0 and "=" in parts[idx - 1]:
        idx -= 1
    segments = parts[idx:]
    # The marker separates key from segments and belongs to NEITHER.
    if idx > 0 and parts[idx - 1] == INSTANCES_MARKER:
        idx -= 1
    if idx == 0:
        raise RuntimeError(f"❌ malformed target instance address: {address!r}")
    target_key = "/".join(parts[:idx])
    return run_actions.normalize_result_name(target_key, label="target instance address"), segments


# One shape for every kind. `status` first because it is the fact a
# reader acts on, `freshness` second because it qualifies a published result,
# `time` after them because it dates the record rather than describing it.
# `committed_at`/`committed_run_id` stay OUT of the flat row: a field that
# appears only when a run failed after a success makes the common row harder to
# scan, and two timestamps invite misreading which is which. They remain on the
# detailed row, where a reader is already investigating.
# Outcome first (`last_action` qualifies `status`, so it stays adjacent), then
# which alternative is in effect, then whether it still matches its cfg, then
# when. `standing`/`superseded_by` sit AHEAD of `freshness` because a superseded
# row's freshness describes it against its OWN cfg, which reads as reassuring
# until you know it is not the one deployed.
# `label` is LAST: it names the invocation a row belonged to rather than saying
# anything about the row's own outcome, so it is the fact a reader reaches for
# after the ones they act on.
AXIS_ORDER = ("status", "last_action", "standing", "superseded_by", "freshness", "time",
              "parent_workflow", "parent_workflow_run_id", "label")


def order_axes(axes: dict[str, str]) -> dict[str, str]:
    """One canonical order for every emitted group.

    This also FILTERS: an axis absent from `AXIS_ORDER` is dropped, so a new one
    reaches a row only by being named here.
    """

    return {k: axes[k] for k in AXIS_ORDER if k in axes}


def _axis_row(computed: dict) -> dict[str, str]:
    """The axes only, for the flat namespace map — no reasons, no children.

    A row carrying no `status` describes nothing that happened, so it collapses to
    empty and its group is omitted. Freshness alone is not a fact about a run.
    """

    # One field order for every row: outcome first, then which alternative is in
    # effect, then whether it still matches its cfg, then when.
    row = {
        axis: computed[axis]
        for axis in ("status", "last_action", "standing", "superseded_by", "freshness")
        if computed.get(axis)
    }
    if not row.get("status"):
        return {}
    for field in ("time", "parent_workflow", "parent_workflow_run_id", "label"):
        if computed.get(field):
            row[field] = computed[field]
    return order_axes(row)


def _place_instance(
    rows: dict, kind: str, key: str, segments: list[str], groups: dict
) -> None:
    """File one instance under its TEMPLATE, mirroring the state dir layout.

    `kind -> template -> instances -> <segments> -> groups`, so every
    materialization of one declared key reads as a single block instead of
    scattered rows sorted by address.

    A singleton has no segments and therefore no instance to name: its groups sit
    directly under the template, exactly as `compose_state_relpath` omits the
    `instances/` layer on disk.
    """

    template = rows[kind].setdefault(key, {})
    if not segments:
        template.update(groups)
        return
    template.setdefault(INSTANCES_MARKER, {})["/".join(segments)] = groups


#/Q1j — target-instance identity path contract.
# Names and values in a Hive-style instance segment must match this charset
# verbatim (no percent-encoding, no sha fallback for targets); the whole
# instance suffix is capped so it stays well inside the S3 1024-byte key limit.
INSTANCE_TOKEN_RE = re.compile(r"[a-z0-9_.-]+")


INSTANCE_SUFFIX_MAX = 128


def resolve_target_instance_segments(
    target_instance_params, execution_context: dict[str, object], *, label: str
) -> list[str]:
    """Resolve declared target_instance_params to Hive-style path segments in
    declaration order: `["account=dev", "env_type=dev"]`.

    Empty/absent params => a singleton target (no instances/ layer, `[]`).
    Each param name and value must match INSTANCE_TOKEN_RE verbatim and be
    present in the execution context; the joined suffix is capped at
    INSTANCE_SUFFIX_MAX (hard error, never a sha fallback — Q1g)."""

    if target_instance_params is None:
        return []
    if not isinstance(target_instance_params, list):
        raise RuntimeError(f"❌ {label}: target_instance_params must be a list")
    segments: list[str] = []
    seen: set[str] = set()
    for param in target_instance_params:
        if not isinstance(param, str) or not INSTANCE_TOKEN_RE.fullmatch(param):
            raise RuntimeError(
                f"❌ {label}: target_instance_params name {param!r} must match [a-z0-9_.-]+"
            )
        if param in seen:
            raise RuntimeError(f"❌ {label}: target_instance_params lists {param!r} twice")
        seen.add(param)
        ref = f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.{param}"
        if ref not in execution_context:
            raise RuntimeError(
                f"❌ {label}: target_instance_params {param!r} is not in the execution context "
                "(instance identity params must be bound)"
            )
        value = str(execution_context[ref])
        if not INSTANCE_TOKEN_RE.fullmatch(value):
            raise RuntimeError(
                f"❌ {label}: instance value {param}={value!r} must match [a-z0-9_.-]+ "
                "(no percent-encoding; shorten or rename the value)"
            )
        segments.append(f"{param}={value}")
    suffix = "/".join(segments)
    if len(suffix) > INSTANCE_SUFFIX_MAX:
        raise RuntimeError(
            f"❌ {label}: instance suffix {suffix!r} exceeds {INSTANCE_SUFFIX_MAX} chars "
            "(shorten instance param values — no sha fallback for target instances, Q1g)"
        )
    return segments


INSTANCES_MARKER = "instances"


def qualified_address(kind: str, address: str) -> str:
    """`<kind>/<instance address>` — the form `--structure flat` already emits.

    A cross-reference between kinds must say WHICH kind it points at: without it
    `env/baseline` is ambiguous between a workflow and a target of the same name,
    and a reader cannot paste it back into a query.
    """

    return address if address.startswith(f"{kind}/") else f"{kind}/{address}"


def unqualified_address(address: str) -> str:
    """The inverse of `qualified_address` — strip a leading `<kind>/`.

    A row that CARRIES a kind (a workflow's member list is targets) does not
    repeat it per entry, while a cross-reference a reader pastes back into a
    query does.
    """

    for kind in run_actions.RESULT_KINDS:
        prefix = f"{kind}/"
        if address.startswith(prefix):
            return address[len(prefix):]
    return address


def instance_address(key: str, instance_segments: list[str]) -> str:
    """The canonical instance address: `<key>` for a singleton, otherwise
    `<key>/instances/<seg>/<seg>` — the SAME path form as the state dir layout,
    marker included, so an address can be read straight off a path and back.

    Segments contain `=`, key components never do, and the `instances` marker
    separates them explicitly rather than by inference.
    """

    if not instance_segments:
        return key
    return "/".join([key, INSTANCES_MARKER, *instance_segments])


def target_instance_address(target_key: str, instance_segments: list[str]) -> str:
    return instance_address(target_key, instance_segments)


def recorded_member_actions(facts: dict) -> list[str]:
    """The action each RECORDED member ran.

    Distinct from `workflow_member_actions`, which reads a workflow's CFG. This
    reads the run record, where a member is already resolved to an instance.

    A member is a bare address when it took the workflow's `default_action`, and
    `{instance, action}` when it differs. Both forms appear in one
    list, so both are read here rather than at every call site.
    """
    default_action = facts.get("default_action")
    actions: list[str] = []
    for member in facts.get("target_instances") or []:
        if isinstance(member, dict):
            action = member.get("action") or default_action
        else:
            action = default_action
        if action:
            actions.append(str(action))
    return actions


def workflow_group(facts: dict) -> str | None:
    """The group a workflow belongs to, derived from its members' actions.

    None when the composition records no action at all — an empty or malformed
    record answers nothing rather than defaulting into a group it may not be in.
    """

    groups = {run_actions.action_group(action) for action in recorded_member_actions(facts)}
    if not groups:
        return None
    for group in run_actions.GROUP_PRECEDENCE:
        if group in groups:
            return group
    raise RuntimeError(f"❌ workflow members resolve to unknown group(s): {sorted(groups)}")


STATE_STRUCTURAL_NAMES = frozenset({"runs", "committed.yaml", "identity.yaml", "locks"})


def compose_state_relpath(
    kind: str, key: str, instance_segments: list[str]
) -> Path:
    """The namespace-relative instance directory for a state owner:
    `<kind>/<key...>/instances/<seg>/<seg>` — or, for a singleton, `<kind>/<key...>`
    with no instances/ layer. The namespace root is prepended by the caller.

    There is no action segment. A key names a THING, and provision and destroy are
    two directions of one state, so they share an instance and differ only in the
    group file they publish."""

    if kind not in run_actions.RESULT_KINDS:
        raise RuntimeError(f"❌ unknown state kind {kind!r} (expected one of {run_actions.RESULT_KINDS})")
    key_parts = [p for p in key.split("/") if p]
    if not key_parts:
        raise RuntimeError("❌ state key must be non-empty")
    parts = [kind, *key_parts]
    if instance_segments:
        parts += ["instances", *instance_segments]
    return Path(*parts)


def parse_state_relpath(namespace_root: Path, state_dir: Path) -> dict | None:
    """Inverse of compose_state_relpath: parse an instance directory back to its
    identity. Returns None when the path is not under the namespace
    root or does not match the tree shape."""
    try:
        rel = Path(state_dir).resolve().relative_to(Path(namespace_root).resolve())
    except ValueError:
        return None
    parts = list(rel.parts)
    if len(parts) < 2 or parts[0] not in run_actions.RESULT_KINDS:
        return None
    kind, rest = parts[0], parts[1:]
    if "instances" in rest:
        idx = rest.index("instances")
        key_parts = rest[:idx]
        after = rest[idx + 1:]
        instance_segments, _ = split_instance_segments(after)
    else:
        # singleton: key runs until the first structural name
        key_parts = []
        for part in rest:
            if part in STATE_STRUCTURAL_NAMES:
                break
            key_parts.append(part)
        instance_segments = []
    if not key_parts:
        return None
    key = "/".join(key_parts)
    return {
        "kind": kind,
        "key": key,
        "instance_segments": instance_segments,
        "instance": "/".join(instance_segments),
        "address": "/".join([kind, instance_address(key, instance_segments)]),
    }


def instance_relpath(instance_segments: list[str]) -> str:
    """

    the `instances/<seg>/<seg>` relative path for a target instance, or ''
    for a singleton target (no instances/ layer,)."""

    if not instance_segments:
        return ""
    return "/".join(["instances", *instance_segments])


def split_instance_segments(parts: list[str]) -> tuple[list[str], list[str]]:
    """Split a path fragment that begins after `instances/` into
    (instance_segments, remaining) using the deterministic `=` boundary
: consume leading segments that contain `=`; the first
    segment without `=` is where structure (`runs`, `committed.yaml`,
    `identity.yaml`, `locks`) resumes. Structural names never contain `=`."""
    instance: list[str] = []
    for i, part in enumerate(parts):
        if "=" in part:
            instance.append(part)
        else:
            return instance, list(parts[i:])
    return instance, []


def workflow_instance_address(workflow_name: str, segments: list[str]) -> str:
    """`env/baseline` when a workflow varies by nothing; otherwise the Hive path.

    Same shape as a target instance address, so one addressing scheme covers both
    kinds and a reader can tell which environment a run belongs to by reading it.
    """

    return instance_address(workflow_name, segments)


def target_instance_display(
    target_run: dict, execution_context: dict[str, object]
) -> str:
    """The target-instance identity for a report row: Hive segments joined
    (`account=dev/env_type=dev`), `<singleton>` when the target has no instance
    layer, or `<unresolved>` if its instance params don't bind."""
    try:
        segments = resolve_target_instance_segments(
            target_run.get("target_instance_params"),
            execution_context,
            label="target instance",
        )
    except Exception:
        return "<unresolved>"
    return "/".join(segments) if segments else "<singleton>"
