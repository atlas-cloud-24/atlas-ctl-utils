"""Which member of a cfg block applies to this run.

Selectors are the one mechanism cfg uses to vary by axis, so every collection
that varies — scopes, overlays, targets, workflows, guard
declarations — is resolved through here rather than by each consumer."""

import collections
import json
import logging
import re

from engine.execution import references as execution_references


def selectors_to_map(items: list[tuple[str, str]], *, label: str) -> dict[str, str]:
    selectors: dict[str, str] = {}
    for key, value in items:
        if key in selectors:
            raise RuntimeError(f"❌ duplicate {label} selector {key!r}")
        selectors[key] = value
    return selectors


def require_selector(selectors: dict[str, str], key: str, *, label: str) -> str:
    value = selectors.get(key)
    if not value:
        raise RuntimeError(f"❌ missing required {label} selector {key!r}")
    return value


def selector_group_is_group(entry: object) -> bool:
    """A selector-membered group entry in a cfg collection
    (cfg_key_sets, refs.scoped) — same resolution semantics as execution
    identity groups: `members` select exactly one concrete value."""

    return isinstance(entry, dict) and "members" in entry


def resolve_selector_group_member(
    entry: dict,
    execution_context: dict[str, object],
    *,
    value_field: str,
    label: str,
    tolerate_none: bool = False,
) -> str | None:
    """Resolve a selector-membered group entry to its one matching member's
    `value_field`. Mirrors execution-identity group semantics:
    members are {<value_field>, selectors}; EXACTLY ONE member must match the
    frozen execution context. The returned value may still carry
    ${execution_context.*} placeholders — the caller renders them.

    With `tolerate_none=True`, ZERO matches returns None instead of raising (used
    for an inactive target whose selector axis isn't bound in this run); MORE than
    one match is always a hard error (a genuine cfg ambiguity)."""

    members = entry.get("members")
    if not isinstance(members, list) or not members:
        raise RuntimeError(f"❌ {label}: group members must be a non-empty list")
    for member in members:
        if not isinstance(member, dict) or set(member) - {value_field, "selectors"}:
            raise RuntimeError(f"❌ {label}: group member must be {{{value_field}, selectors}}")
        value = member.get(value_field)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"❌ {label}: group member {value_field} must be a non-empty string")
    reject_duplicate_selectors(
        {m.get(value_field): m.get("selectors") for m in members}, label=label
    )
    matches = [
        member
        for member in members
        if selector_matches(
            member.get("selectors"),
            execution_context,
            label=f"{label} member {member.get(value_field)}",
            structured_only=True,
        )
    ]
    if len(matches) != 1:
        if tolerate_none and not matches:
            return None
        member_values = [member.get(value_field) for member in members]
        raise RuntimeError(
            f"❌ {label}: exactly one group member must match the execution context, "
            f"matched {len(matches)} (members: {member_values})"
        )
    return matches[0][value_field].strip()


def _selector_param_axes(members: object) -> set[str]:
    """The params-namespace axes referenced by a member list's selectors

    (-axes guard input). Non-params refs (ctl.*) are ignored.
    (EXECUTION_CONTEXT_ROOT is defined later in the module — reference it at
    call time, never at import time.)
    """

    prefix = f"{execution_references.EXECUTION_CONTEXT_ROOT}.params."
    axes: set[str] = set()
    for member in members or []:
        if not isinstance(member, dict):
            continue
        try:
            requirements = selector_requirements(
                member.get("selectors"), label="consumed-axes scan", structured_only=True
            )
        except Exception:
            continue
        for ref in requirements:
            if ref.startswith(prefix):
                axes.add(ref[len(prefix) :])
    return axes


def _template_param_axes(value: object) -> set[str]:
    """${execution_context.params.X} variables in a raw template string."""

    if not isinstance(value, str):
        return set()
    pattern = rf"\$\{{{re.escape(execution_references.EXECUTION_CONTEXT_ROOT)}\.params\.([A-Za-z_][A-Za-z0-9_]*)\}}"
    return set(re.findall(pattern, value))


def collect_member_dispatch_axes(members: object, *, label: str) -> set[str]:
    """Uniqueness rule, field-agnostic: EVERY members-shaped
    dispatch in target resolution feeds the guard, and the violation test is
    per-AXIS, never per-field:

    - a params axis → returned (must be declared in target_instance_params
      unless path-encoded via the namespace);
    - `ctl.action` → safe: a target action determines the state group;
    - any OTHER ctl.* fact (e.g. ctl.profile) → hard error: it is neither
      path-encoded nor declarable, so two runs differing only in it would
      collapse onto one instance path and self-override."""
    params_prefix = f"{execution_references.EXECUTION_CONTEXT_ROOT}.params."
    safe_refs = {
        f"{execution_references.EXECUTION_CONTEXT_ROOT}.ctl.action",
    }
    axes: set[str] = set()
    for member in members or []:
        if not isinstance(member, dict):
            continue
        requirements = selector_requirements(
            member.get("selectors"), label=label, structured_only=True
        )
        for ref in requirements:
            if ref.startswith(params_prefix):
                axes.add(ref[len(params_prefix) :])
            elif ref not in safe_refs:
                raise RuntimeError(
                    f"❌ {label}: dispatch on {ref!r} is not allowed — target "
                    "resolution may dispatch only on declarable params axes or "
                    "the path-determining ctl.action"
                )
    return axes


def resolve_list_members(
    entry: dict,
    execution_context: dict[str, object] | None,
    *,
    value_field: str,
    label: str,
    allow_empty: bool = False,
    extra_fields: tuple[str, ...] = (),
) -> list | None:
    """Resolve a members-shaped LIST-valued declaration
    ({members: [{<value_field>: [...], selectors: {...}}, ...]}) to the ONE
    matching member's list ( schemas,-action
    target_keys). The scalar twin is resolve_selector_group_member.
    Returns None when no context is available or the dispatch axis is unbound
    (deferred — the caller decides whether that is an error)."""

    members = entry.get("members")
    if set(entry) != {"members"} or not isinstance(members, list) or not members:
        raise RuntimeError(f"❌ {label}: members-shaped declaration must be {{members: [...]}}")
    for member in members:
        allowed = {value_field, "selectors", *extra_fields}
        if (
            not isinstance(member, dict)
            or not {value_field, "selectors"} <= set(member)
            or not set(member) <= allowed
        ):
            raise RuntimeError(
                f"❌ {label}: each member must be {{{value_field}, selectors"
                + (", " + ", ".join(extra_fields) if extra_fields else "")
                + "}}"
            )
        if not isinstance(member[value_field], list) or (
            not member[value_field] and not allow_empty
        ):
            raise RuntimeError(f"❌ {label}: member {value_field} must be a non-empty list")
    if execution_context is None:
        return None
    matches = [
        member
        for member in members
        if selector_matches(
            member["selectors"],
            execution_context,
            label=f"{label} member",
            structured_only=True,
        )
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise RuntimeError(
            f"❌ {label}: exactly one member must match the execution context, matched {len(matches)}"
        )
    return list(matches[0][value_field])


def resolve_list_member(
    entry: dict,
    execution_context: dict[str, object] | None,
    *,
    value_field: str,
    label: str,
    extra_fields: tuple[str, ...] = (),
) -> dict | None:
    """The matching MEMBER itself, not just its list.

    a workflow member declares `default_action` for the whole list it
    carries, so the caller needs the member and not only its keys.
    """

    resolved = resolve_list_members(
        entry,
        execution_context,
        value_field=value_field,
        label=label,
        extra_fields=extra_fields,
    )
    if resolved is None:
        return None
    for member in entry.get("members") or []:
        if list(member.get(value_field) or []) == resolved and selector_matches(
            member["selectors"],
            execution_context or {},
            label=f"{label} member",
            structured_only=True,
        ):
            return member
    return None


def resolve_scalar_member(
    entry: object,
    execution_context: dict[str, object] | None,
    *,
    label: str,
) -> str | None:
    """Resolve a scalar or `{members: [{value, selectors}, ...]}` declaration."""

    if isinstance(entry, str):
        value = entry.strip()
        if not value or "\n" in value or "\r" in value:
            raise RuntimeError(f"❌ {label} must be a non-empty single-line string")
        return value
    if not isinstance(entry, dict) or set(entry) != {"members"}:
        raise RuntimeError(f"❌ {label} must be a non-empty string or {{members: [...]}}")
    members = entry.get("members")
    if not isinstance(members, list) or not members:
        raise RuntimeError(f"❌ {label} members must be a non-empty list")
    for member in members:
        if not isinstance(member, dict) or set(member) != {"value", "selectors"}:
            raise RuntimeError(f"❌ {label}: each member must be {{value, selectors}}")
        value = member.get("value")
        if not isinstance(value, str) or not value.strip() or "\n" in value or "\r" in value:
            raise RuntimeError(f"❌ {label}: member value must be a non-empty single-line string")
    if execution_context is None:
        return None
    matches = [
        member
        for member in members
        if selector_matches(
            member["selectors"],
            execution_context,
            label=f"{label} member",
            structured_only=True,
        )
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"❌ {label}: exactly one member must match the execution context, "
            f"matched {len(matches)}"
        )
    return str(matches[0]["value"]).strip()


def workflow_effective_selectors(action_workflows: dict, name: str, _stack: tuple = ()) -> dict:
    """A workflow's selectors intersected with all imported workflows' selectors
    (an import cannot widen availability)."""

    if name in _stack:
        return {}
    wf = action_workflows.get(name) or {}
    effective = selector_requirements(wf.get("selectors") or {}, label=f"workflow {name} selectors")
    for workflow_key in wf.get("import_workflows") or []:
        imported = selector_requirements(
            workflow_effective_selectors(action_workflows, workflow_key, (*_stack, name)),
            label=f"workflow {workflow_key} effective selectors",
        )
        for ref, values in imported.items():
            effective[ref] = effective[ref] & values if ref in effective else set(values)
            if not effective[ref]:
                raise RuntimeError(
                    f"❌ workflow {name!r} selectors have empty intersection for {ref!r} "
                    f"after importing {workflow_key!r}"
                )
    return selectors_to_in_shape(effective)


def resolve_default_action(
    declared: object, execution_context: dict[str, object] | None, *, label: str
) -> str | None:
    """A member's declared default action: a literal, or a context reference.

    A context reference resolves a consumer parameter explicitly. A literal says
    the list is one fixed direction whatever selected the workflow branch.
    """

    if declared is None:
        return None
    if not isinstance(declared, str) or not declared.strip():
        raise RuntimeError(f"❌ {label} default_action must be a non-empty string")
    declared = declared.strip()
    if declared.startswith("${") and declared.endswith("}"):
        reference = declared[2:-1].strip()
        value = (execution_context or {}).get(reference)
        if value is None:
            raise RuntimeError(
                f"❌ {label} default_action references {reference!r}, which is not "
                "bound in this execution context"
            )
        return str(value)
    return declared


def selector_expected_values(expected, *, label: str) -> list[str]:
    if isinstance(expected, str) and expected:
        return [expected]
    if isinstance(expected, list) and all(isinstance(item, str) and item for item in expected):
        return expected
    raise RuntimeError(f"❌ {label} must be a non-empty string or list of non-empty strings")


SELECTOR_STRUCTURED_KEYS = ("match", "in", "contains")


def selector_contains_requirements(selectors: object, *, label: str) -> dict[str, set[str]]:
    """The `contains` block: ref -> values that must be PRESENT IN the list-valued
    fact at ref.

    `match`/`in` ask "is this scalar fact one of these values?". `contains` asks
    the inverse: "is this value among the facts?" — needed for list-valued context
    facts such as the run's declared providers. Generic: nothing here is
    provider-specific; it applies to any list-valued fact.
    """

    if not isinstance(selectors, dict):
        return {}
    requirements: dict[str, set[str]] = {}

    # Structured form: {contains: {ref: values}}
    raw = selectors.get("contains") or {}
    if raw:
        if not isinstance(raw, dict):
            raise RuntimeError(f"❌ selectors.contains must be a mapping: {label}")
        for ref, expected in raw.items():
            ref = execution_references.validate_execution_context_ref(
                ref, label=f"{label}.contains"
            )
            requirements[ref] = set(
                selector_expected_values(expected, label=f"{label}.contains.{ref}")
            )

    # per-ref form: {ref: {contains: values}} — the shape constraint gates use,
    # where each entry is a direct match-mapping rather than a selectors block
    for ref, expected in selectors.items():
        if ref in ("match", "in", "contains") or not _is_contains_predicate(expected):
            continue
        ref = execution_references.validate_execution_context_ref(ref, label=label)
        requirements[ref] = set(
            selector_expected_values(expected["contains"], label=f"{label}.{ref}.contains")
        )
    return requirements


def _is_contains_predicate(value: object) -> bool:
    return isinstance(value, dict) and set(value) == {"contains"}


def _context_fact_members(value: object) -> set[str]:
    """A context fact as a set of members (a list fact -> its items; a scalar -> itself)."""

    if isinstance(value, (list, tuple, set)):
        return {str(item) for item in value}
    return {str(value)}


def selector_requirements(
    selectors: dict | None, *, label: str, structured_only: bool = False
) -> dict[str, set[str]]:
    """Normalize selector requirements to ref -> allowed string values.

    New selector metadata uses {match: {ref: scalar}, in: {ref: [values]}}.
    Constraint `when` maps still use the direct {ref: [values]} shape, so the
    legacy direct map is accepted only when structured_only is false.
    """

    if not selectors:
        return {}
    if not isinstance(selectors, dict):
        raise RuntimeError(f"❌ selectors must be a mapping: {label}")

    uses_structured = any(key in selectors for key in SELECTOR_STRUCTURED_KEYS)
    if uses_structured:
        unknown = sorted(set(selectors) - set(SELECTOR_STRUCTURED_KEYS))
        if unknown:
            raise RuntimeError(f"❌ selectors has unsupported keys {unknown}: {label}")
        requirements: dict[str, set[str]] = {}
        raw_match = selectors.get("match") or {}
        raw_in = selectors.get("in") or {}
        # `contains` is a different predicate KIND (membership in a list-valued
        # fact, not a scalar in a set) — see selector_contains_requirements.
        if not isinstance(raw_match, dict):
            raise RuntimeError(f"❌ selectors.match must be a mapping: {label}")
        if not isinstance(raw_in, dict):
            raise RuntimeError(f"❌ selectors.in must be a mapping: {label}")
        overlap = set(raw_match) & set(raw_in)
        if overlap:
            raise RuntimeError(
                f"❌ selector refs cannot appear in both match and in: {sorted(overlap)} ({label})"
            )
        for ref, expected in raw_match.items():
            ref = execution_references.validate_execution_context_ref(ref, label=f"{label}.match")
            values = selector_expected_values(expected, label=f"{label}.match.{ref}")
            if len(values) != 1:
                raise RuntimeError(f"❌ {label}.match.{ref} must be one exact value")
            requirements[ref] = set(values)
        for ref, expected in raw_in.items():
            ref = execution_references.validate_execution_context_ref(ref, label=f"{label}.in")
            requirements[ref] = set(selector_expected_values(expected, label=f"{label}.in.{ref}"))
        return requirements

    if structured_only:
        raise RuntimeError(f"❌ selectors must use match/in form: {label}")

    requirements = {}
    for ref, expected in selectors.items():
        if _is_contains_predicate(expected):
            continue  # handled by selector_contains_requirements
        ref = execution_references.validate_execution_context_ref(ref, label=label)
        requirements[ref] = set(selector_expected_values(expected, label=f"{label}.{ref}"))
    return requirements


def reject_duplicate_selectors(selectors_by_key: dict[str, dict | None], *, label: str) -> None:
    """Load-time guard for structures where selectors pick EXACTLY ONE entry
    (namespaces, selector groups): reject two entries whose selectors are
    byte-identical. This catches literal duplicates before any run, instead of
    waiting for the resolve-time 'matched 2' error on the first context that
    happens to hit both. It does NOT detect general selector OVERLAP (that is a
    predicate-disjointness problem left to the resolve-time exactly-one guard)."""
    seen: dict[str, str] = {}
    for key, selectors in selectors_by_key.items():
        requirements = selector_requirements(
            selectors, label=f"{label}.{key}", structured_only=True
        )
        canonical = json.dumps(
            {ref: sorted(vals) for ref, vals in requirements.items()}, sort_keys=True
        )
        if canonical in seen:
            raise RuntimeError(
                f"❌ {label}: {key!r} and {seen[canonical]!r} have identical selectors "
                f"— they can never resolve to exactly one; make them distinct"
            )
        seen[canonical] = key


def selector_matches(
    selectors: dict | None,
    execution_context: dict[str, object],
    *,
    label: str,
    structured_only: bool = False,
) -> bool:
    """Return whether selector constraints match the execution context.

    Uniform surface: any fully-qualified execution-context path is usable; a
    missing key means no match (the gated entry is simply inactive), never an
    error here. The miss is logged at DEBUG (a gated-inactive member is normal
    flow, so it must not flood INFO) with the available keys, so a typo'd
    execution input is still self-evident under --debug.
    """

    requirements = selector_requirements(selectors, label=label, structured_only=structured_only)
    for ref, allowed_values in requirements.items():
        if ref not in execution_context:
            logging.debug(
                "Selector %s: %s",
                label,
                execution_references.execution_context_miss_message(execution_context, ref),
            )
            return False
        if str(execution_context[ref]) not in allowed_values:
            return False
    for ref, required_members in selector_contains_requirements(selectors, label=label).items():
        if ref not in execution_context:
            logging.debug(
                "Selector %s: %s",
                label,
                execution_references.execution_context_miss_message(execution_context, ref),
            )
            return False
        if not required_members <= _context_fact_members(execution_context[ref]):
            return False
    return True


def selectors_to_in_shape(requirements: dict[str, set[str]]) -> dict:
    if not requirements:
        return {}
    return {"in": {ref: sorted(values) for ref, values in sorted(requirements.items())}}


def scope_prefix_matches(scope_id: str, prefix: str) -> bool:
    return scope_id == prefix or scope_id.startswith(prefix + "/")


def validate_scope_composition(active_scopes: list[dict], composition: dict[str, dict]) -> None:
    target_scopes: dict[str, list[dict]] = collections.defaultdict(list)
    for scope in active_scopes:
        target_scopes[scope["target_path"]].append(scope)

    for target_path, scopes in target_scopes.items():
        rule = composition.get(target_path)
        if rule is None:
            if len(scopes) > 1:
                rendered = ", ".join(str(scope["meta_path"]) for scope in scopes)
                raise RuntimeError(f"Duplicate active cfg target_path {target_path!r}: {rendered}")
            continue

        prefixes = rule["scopes"]
        seen_prefixes: dict[str, dict] = {}
        seen_match: dict[tuple[tuple[str, tuple[str, ...]], ...], dict] = {}
        for scope in scopes:
            matches = [
                prefix for prefix in prefixes if scope_prefix_matches(scope["scope_id"], prefix)
            ]
            if len(matches) != 1:
                raise RuntimeError(
                    f"❌ active cfg scope {scope['scope_id']} -> {target_path} must match exactly one "
                    f"scope_composition prefix {prefixes}; matched {matches}"
                )
            prefix = matches[0]
            previous = seen_prefixes.get(prefix)
            if previous is not None:
                raise RuntimeError(
                    f"❌ multiple active cfg scopes for target_path {target_path!r} and prefix {prefix!r}: "
                    f"{previous['meta_path']} and {scope['meta_path']}"
                )
            seen_prefixes[prefix] = scope

            match_req = selector_requirements(
                (scope.get("selectors") or {}).get("match")
                and {"match": (scope.get("selectors") or {}).get("match")},
                label=f"scope {scope['scope_id']} match",
            )
            match_key = tuple(
                sorted((ref, tuple(sorted(values))) for ref, values in match_req.items())
            )
            previous_match = seen_match.get(match_key)
            if match_key and previous_match is not None:
                raise RuntimeError(
                    f"❌ duplicate active cfg scope match for target_path {target_path!r}: "
                    f"{previous_match['meta_path']} and {scope['meta_path']}"
                )
            if match_key:
                seen_match[match_key] = scope


def _selector_branches(declared) -> list[tuple[list[str], dict | None]]:
    """A params declaration as `(params, selectors)` branches.

    A plain list is ONE branch with no condition — it applies always. A
    members-shaped declaration is one branch per member.
    """

    if declared is None:
        return [([], None)]
    if isinstance(declared, dict):
        return [
            (list(member.get("params") or []), member.get("selectors"))
            for member in (declared.get("members") or [])
        ]
    if isinstance(declared, list):
        return [(list(declared), None)]
    return []


def _selectors_can_both_hold(left: dict | None, right: dict | None, *, label: str) -> bool:
    """Whether one execution context could satisfy both selector blocks.

    Not subset: neither condition contains the other, they merely have to be
    SATISFIABLE together. Two blocks conflict only where they constrain the same
    reference to disjoint value sets; a reference only one of them names is free.
    """
    left_req = selector_requirements(left, label=label)
    right_req = selector_requirements(right, label=label)
    for ref, values in left_req.items():
        other = right_req.get(ref)
        if other is not None and not (values & other):
            return False
    return True


def collect_target_consumed_axes(
    target_key: str,
    action_target: dict,
    *,
    refs: dict,
    execution_identities: dict,
    execution_context: dict[str, object],
) -> set[str]:
    """Statically derive the params-namespace axes a target
    consumes at its three resolution chokepoints — ref template / ref group,
    domain + instance-schema group selectors (recorded at action load),
    and identity group selectors + account_key template."""
    consumed: set[str] = set(action_target.get("consumed_group_axes") or [])
    raw_ref = action_target.get("ref")
    consumed |= _template_param_axes(raw_ref)
    scoped_refs = (refs or {}).get("scoped") or {}
    ref_entry = scoped_refs.get(raw_ref)
    if selector_group_is_group(ref_entry):
        consumed |= _selector_param_axes(ref_entry.get("members"))
        try:
            member_template = resolve_selector_group_member(
                ref_entry,
                execution_context,
                value_field="ref_key",
                label=f"target {target_key!r} ref group",
                tolerate_none=True,
            )
        except Exception:
            member_template = None
        consumed |= _template_param_axes(member_template)
    # The target declares its execution inline, so the only axis it can
    # consume is the `account` template. The old walk over nested identity groups
    # (one dispatch axis per level) is gone with the groups themselves; per-action
    # variation is now `execution.roles`, keyed by authorization class, which
    # consumes no params axis.
    execution = action_target.get("execution_identities")
    if isinstance(execution, dict):
        consumed |= _template_param_axes(execution.get("account"))
    return consumed
