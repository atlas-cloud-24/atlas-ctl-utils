"""Assembling the execution context a run is carried out under.

The context is the run's identity: the axes, the profile, the params, the
providers. It is built once, validated against declared constraints, and then
only read — nothing downstream may add to it, or the constraints would be
checked against something other than what ran."""

import argparse
import re
import shutil

from pathlib import Path

from engine.cfg import resources as cfg_resources
from engine.execution import adapters as execution_adapters
from engine.execution import providers as execution_providers
from engine.execution import references as execution_references
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import selectors as run_selectors

EXECUTION_CONTEXT_FILENAME = "execution_context.yaml"


EXECUTION_CONTEXT_CONSTRAINT_FIELDS = frozenset(
    {"when_all", "when_any", "require_present", "allowed_values"}
)


def load_execution_context_constraints(ctl_cfg_root: Path) -> list[dict]:
    constraint_entries: list[tuple[dict, Path]] = []
    for path, section in kernel_yaml_io.collect_top_level_sections(ctl_cfg_root, "execution_context_constraints"):
        if not isinstance(section, list):
            raise RuntimeError(f"❌ execution_context_constraints must be a list: {path}")
        constraint_entries.extend((constraint, path) for constraint in section)

    constraints: list[dict] = []
    for idx, (constraint, path) in enumerate(constraint_entries, start=1):
        if not isinstance(constraint, dict):
            raise RuntimeError(f"❌ execution context constraint #{idx} must be a mapping: {path}")
        if "when" in constraint:
            raise RuntimeError(
                f"❌ execution context constraint #{idx} uses `when`, which is removed: {path}; "
                "use `when_all` (list of match-mappings, ALL must match) — a multi-path `when` "
                "mapping becomes N single-path entries — or `when_any` (ANY matches)"
            )
        unknown = sorted(set(constraint) - set(EXECUTION_CONTEXT_CONSTRAINT_FIELDS))
        if unknown:
            raise RuntimeError(
                f"❌ execution context constraint #{idx} has unknown fields {unknown}: {path}; "
                f"allowed: {sorted(EXECUTION_CONTEXT_CONSTRAINT_FIELDS)}"
            )
        for clause in ("when_all", "when_any"):
            if clause not in constraint:
                continue
            entries = constraint.get(clause)
            if not isinstance(entries, list) or not entries:
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} {clause} must be a non-empty list "
                    f"of match-mappings: {path}"
                )
            if not all(isinstance(entry, dict) and entry for entry in entries):
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} {clause} entries must be non-empty "
                    f"mappings: {path}"
                )
        require_present = constraint.get("require_present") or []
        allowed_values = constraint.get("allowed_values") or {}
        if not isinstance(require_present, list) or not all(isinstance(item, str) and item for item in require_present):
            raise RuntimeError(f"❌ execution context constraint #{idx} require_present must be a list of non-empty strings: {path}")
        if not isinstance(allowed_values, dict):
            raise RuntimeError(f"❌ execution context constraint #{idx} allowed_values must be a mapping: {path}")
        constraints.append(constraint)
    return constraints


def execution_context_constraint_applies(
    constraint: dict, execution_context: dict[str, object], *, idx: int
) -> bool:
    """Does this constraint's gate match?

    `when_all` — every entry must match (this is what the removed `when` did across
    the paths of one mapping). `when_any` — at least one entry must match (OR, which
    the old mapping-only form could not express, forcing duplicated rules). Both
    present → AND of the two. Neither present → the constraint always applies.
    """
    entries_all = constraint.get("when_all") or []
    if entries_all and not all(
        run_selectors.selector_matches(entry, execution_context, label=f"execution_context_constraints[{idx}].when_all[{n}]")
        for n, entry in enumerate(entries_all)
    ):
        return False

    entries_any = constraint.get("when_any") or []
    if entries_any and not any(
        run_selectors.selector_matches(entry, execution_context, label=f"execution_context_constraints[{idx}].when_any[{n}]")
        for n, entry in enumerate(entries_any)
    ):
        return False

    return True


def validate_execution_context_constraints(ctl_cfg_root: Path, execution_context: dict[str, object]) -> None:
    for idx, constraint in enumerate(load_execution_context_constraints(ctl_cfg_root), start=1):
        if not execution_context_constraint_applies(constraint, execution_context, idx=idx):
            continue
        when = {
            key: constraint[key]
            for key in ("when_all", "when_any")
            if key in constraint
        }

        for ref in constraint.get("require_present") or []:
            execution_references.validate_execution_context_ref(ref, label=f"execution_context_constraints[{idx}].require_present")
            if ref not in execution_context:
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} requires {ref!r} when {when} matches; "
                    f"{execution_references.execution_context_miss_message(execution_context, ref)}"
                )

        for ref, expected in (constraint.get("allowed_values") or {}).items():
            execution_references.validate_execution_context_ref(ref, label=f"execution_context_constraints[{idx}].allowed_values")
            allowed = run_selectors.selector_expected_values(expected, label=f"execution_context_constraints[{idx}].allowed_values.{ref}")
            if ref in execution_context and str(execution_context[ref]) not in allowed:
                raise RuntimeError(
                    f"❌ execution context constraint #{idx} allows {ref} only in {allowed}, got {execution_context[ref]!r}"
                )


def load_ctl_sources(ctl_cfg_root: Path) -> dict:
    """Load `ctl_sources.<key>`: the data ctl SOURCES, by key.

    Engine-generic on purpose. An entry declares WHAT the payload is (`type`),
    what a collision MEANS (`conflict_resolution`), and a LIST of sources. The
    engine validates that shape only — it learns no type or policy VALUES, and no
    provider vocabulary; `provider` sits on the SOURCE, so one input may be
    assembled from sources belonging to different providers and this collection
    cannot live inside any one provider's catalog. The consumer of an input
    validates the values and the source fields it implements.
    """

    entries = cfg_resources.collect_resource(ctl_cfg_root, "ctl_sources")
    for source_key, entry in entries.items():
        label = f"ctl_sources.{source_key}"
        if not isinstance(entry, dict):
            raise RuntimeError(f"❌ {label} must be a mapping: {ctl_cfg_root}")
        unknown = sorted(set(entry) - {"type", "conflict_resolution", "sources", "publish_under"})
        if unknown:
            raise RuntimeError(f"❌ {label} has unknown fields {unknown}: {ctl_cfg_root}")
        for field, why in (
            ("type", "the combine operation follows from it: a map is merged by key, "
                     "a list is concatenated"),
            ("conflict_resolution", "left implicit, source order would silently decide "
                                    "which value wins"),
        ):
            if not isinstance(entry.get(field), str) or not entry[field].strip():
                raise RuntimeError(
                    f"❌ {label}.{field} must be declared — {why}: {ctl_cfg_root}"
                )
        prefix = entry.get("publish_under")
        if prefix is not None and (not isinstance(prefix, str) or not prefix.strip()):
            raise RuntimeError(
                f"❌ {label}.publish_under must be a non-empty dotted prefix: {ctl_cfg_root}"
            )
        sources = entry.get("sources")
        if not isinstance(sources, list) or not sources:
            raise RuntimeError(f"❌ {label}.sources must be a non-empty list: {ctl_cfg_root}")
        for index, source in enumerate(sources):
            if not isinstance(source, dict):
                raise RuntimeError(f"❌ {label}.sources[{index}] must be a mapping: {ctl_cfg_root}")
            # Only these two are universal. Everything else is provider-shaped,
            # so the engine cannot know it and the consumer validates it.
            for field in ("provider", "format"):
                if not isinstance(source.get(field), str) or not source[field].strip():
                    raise RuntimeError(
                        f"❌ {label}.sources[{index}].{field} must be a non-empty string: "
                        f"{ctl_cfg_root}"
                    )
    return entries


def resolve_ref_context(target_ref: str, context: dict[str, object]) -> str:
    """

    resolve placeholders in a target ref into a refs.scoped context key."""

    return execution_references.resolve_runtime_scalar(
        target_ref,
        context,
        label="target ref_key",
    )


def resolve_input_params(
    input_params: object,
    input_param_set_keys: object,
    param_sets: dict,
    *,
    label: str,
    cfg_path: Path,
    _stack: tuple = (),
) -> list[str]:
    """Resolve a target's declared INPUT PARAMS.

    Set references and literal param names are separate fields, the same split
    `cfg_key_sets`/`cfg_keys` uses: one is a ctl catalog lookup, the other a param
    name, so they cannot collide. Nearly every target needs the same naming and
    placement axes, which is what the sets exist for.
    """
    resolved: list[str] = []
    if input_param_set_keys is not None:
        if not isinstance(input_param_set_keys, list) or not input_param_set_keys:
            raise RuntimeError(f"❌ {label} input_param_sets must be a non-empty list ({cfg_path})")
        for name in input_param_set_keys:
            if not isinstance(name, str) or not name.strip():
                raise RuntimeError(f"❌ {label} input_param_sets entries must be non-empty strings")
            name = name.strip()
            if name not in param_sets:
                available = ", ".join(sorted(param_sets)) or "none"
                raise RuntimeError(
                    f"❌ {label}: undefined param_set {name!r}; declared: {available} ({cfg_path})"
                )
            if name in _stack:
                raise RuntimeError(f"❌ param_set cycle: {' -> '.join([*_stack, name])} ({cfg_path})")
            member = param_sets[name]
            if not isinstance(member, dict):
                raise RuntimeError(f"❌ param_set {name!r} must be a mapping ({cfg_path})")
            unknown = sorted(set(member) - {"input_param_sets", "input_params"})
            if unknown:
                raise RuntimeError(f"❌ param_set {name!r} has unsupported keys {unknown} ({cfg_path})")
            resolved.extend(
                resolve_input_params(
                    member.get("input_params"), member.get("input_param_sets"), param_sets,
                    label=f"param_set {name!r}", cfg_path=cfg_path, _stack=(*_stack, name),
                )
            )
    if input_params is not None:
        if not isinstance(input_params, list) or not input_params:
            raise RuntimeError(f"❌ {label} input_params must be a non-empty list ({cfg_path})")
        for entry in input_params:
            if not isinstance(entry, str) or not execution_references.CONTEXT_KEY_RE.fullmatch(entry.strip()):
                raise RuntimeError(f"❌ {label} input_params entry {entry!r} must be a param name")
            resolved.append(entry.strip())
    seen: set[str] = set()
    return [e for e in resolved if not (e in seen or seen.add(e))]


def ref_context_to_result_path(ref_context: str) -> str:
    return ref_context.replace(".", "/")


def resolve_result_name(args: argparse.Namespace, run_type: str) -> str:
    """

    resolve the stable ctl result name for the selected runner mode."""

    if run_type == "workflow":
        if getattr(args, "target", None):
            raise RuntimeError("❌ workflow runner does not accept --target")
        raw_name = getattr(args, "workflow", None)
    elif run_type == "target":
        if getattr(args, "workflow", None):
            raise RuntimeError("❌ target runner does not accept --workflow")
        raw_name = getattr(args, "target", None)
    elif run_type == "procedure":
        if getattr(args, "workflow", None) or getattr(args, "target", None):
            raise RuntimeError("❌ procedure runner does not accept --workflow or --target")
        ref = getattr(args, "ref", None)
        ref_context = resolve_ref_context(ref, args.execution_params) if ref else "procedure"
        raw_name = f"{ref_context_to_result_path(ref_context)}/{getattr(args, 'source', None) or 'unknown'}/{getattr(args, 'procedure', None) or 'unknown'}"
    elif run_type == "maintenance":
        maintenance_target = getattr(args, "target", None) or getattr(args, "lock_id", None) or "unknown"
        raw_name = f"{getattr(args, 'maintenance_action', None) or 'maintenance'}/{maintenance_target}"
    elif run_type == "fan_out":
        raw_name = getattr(args, "fan_out", None)
    else:
        raise RuntimeError(f"❌ unknown runner run_type {run_type!r}")

    return run_actions.normalize_result_name(raw_name, label=f"{run_type} result name")


EXECUTION_PARAMS_KEY = "execution_params"


def load_execution_params(ctl_cfg_root: Path) -> dict[str, object]:
    """Read consumer param declarations discovered by the `execution_params`
    content key. Each value is a literal scalar or a whole-value fully-qualified
    reference into ctl/params (resolved against CLI params + promoted args)."""
    entries: dict[str, object] = {}
    origins: dict[str, Path] = {}
    for path, section in kernel_yaml_io.collect_top_level_sections(ctl_cfg_root, EXECUTION_PARAMS_KEY):
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ {EXECUTION_PARAMS_KEY} must be a mapping: {path}")
        # A namespaced family may be authored as a NESTED mapping and is flattened
        # to the dotted param keys the context uses — `ns: {key: x}` and
        # `ns.key: x` declare the same param. Params stay scalar-only; nesting is
        # an authoring shape only, so one namespace's params read as one block.
        # The engine names no namespace: the consumer chooses them.
        def walk(node, prefix="", path=path):
            for key, raw in node.items():
                if not isinstance(key, str) or not execution_references.CONTEXT_KEY_RE.fullmatch(key):
                    raise RuntimeError(
                        f"❌ {EXECUTION_PARAMS_KEY} key must be a valid identifier: {key!r}"
                    )
                dotted = f"{prefix}{key}"
                if isinstance(raw, dict):
                    if not raw:
                        raise RuntimeError(f"❌ {EXECUTION_PARAMS_KEY}.{dotted} must not be an empty map: {path}")
                    walk(raw, f"{dotted}.")
                    continue
                if dotted in entries:
                    raise RuntimeError(
                        f"❌ duplicate {EXECUTION_PARAMS_KEY}.{dotted}: {path} (also defined in {origins[dotted]})"
                    )
                entries[dotted] = raw
                origins[dotted] = path

        walk(section)
    return entries


def build_execution_context(
    ctl_cfg_root: Path,
    *,
    action: str | None,
    ctl_profile: str | None,
    execution_params: dict[str, str],
    execution_access_modes: dict[str, str] | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_runtime_mode: str,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
) -> dict[str, object]:
    """Build the flat dotted execution context: the closed, namespaced facts of
    this execution. Namespaces: `ctl` (promoted engine args), `params` (consumer
    values, merged from --execution-params CLI + the execution_params cfg block),
    and `sourced` (data ctl READ from a declared `ctl_sources` entry, so a
    consumer never keeps its own copy). Keys look like
    'execution_context.params.env.type'."""
    # An adapter is its own repository, so it has to be put on the
    # import path before it is called. Here because this is the OWNING construct:
    # Every entry point that reaches an adapter builds a context first, and it is
    # the one place that already holds the cfg root the declaration lives in.
    # Per-entry-point activation was tried and reverted — `validate_cfg.py` and
    # `regenerate_guardrails.py` had each shipped without it and could not run at
    # all, which is the duplication `Single Source Of Logic` exists to prevent.
    # `finalize_common_args` keeps its own call: it validates provider options
    # BEFORE any context exists, and that validation is done BY the adapter.
    execution_providers.activate_provider_adapters(ctl_cfg_root)
    context: dict[str, object] = {}

    def put_list(namespace: str, key: str, values, *, label: str) -> None:
        """A LIST-valued promoted fact. Params remain scalar-only; this exists for
        facts that are inherently plural (the run's declared providers), matched
        with the `contains` selector predicate."""

        if not execution_references.CONTEXT_KEY_RE.fullmatch(key):
            raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
        cleaned = [str(execution_references._context_scalar(v, label=label)) for v in (values or [])]
        context[f"{execution_references.EXECUTION_CONTEXT_ROOT}.{namespace}.{key}"] = cleaned

    def put(namespace: str, key: str, value, *, label: str) -> None:
        if not execution_references.CONTEXT_KEY_RE.fullmatch(key):
            raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
        context[f"{execution_references.EXECUTION_CONTEXT_ROOT}.{namespace}.{key}"] = execution_references._context_scalar(value, label=label)

    if action is not None:
        put("ctl", "action", action, label="promoted --action")
        # A WORKFLOW is invoked with an operation and its members carry
        # the actions, so member selectors gate on `ctl.operation`. It is published
        # alongside rather than instead of `ctl.action`: a target run still has one
        # action, and credential selection keys on that.
        put("ctl", "operation", action, label="promoted --operation")
    if ctl_profile is not None:
        put("ctl", "profile", ctl_profile, label="promoted --ctl-profile")
    # One fact per participating provider — the mode is a per-provider decision,
    # so cfg gates on `...ctl.execution_access_mode.<provider>`, never on a
    # single run-wide value. The engine names no provider and no mode here.
    for _provider, _mode in sorted((execution_access_modes or {}).items()):
        put(
            "ctl",
            f"execution_access_mode.{_provider}",
            _mode,
            label="promoted --execution-access-mode",
        )
    put("ctl", "agreed_defer_ctl_state_backend_sync", bool(agreed_defer_ctl_state_backend_sync), label="promoted --agreed-defer-ctl-state-backend-sync")
    put("ctl", "force_skip_ctl_state_backend_sync", bool(force_skip_ctl_state_backend_sync), label="promoted --force-skip-ctl-state-backend-sync")
    put("ctl", "force_skip_guardrails", bool(force_skip_guardrails), label="promoted --force-skip-guardrails")
    put(
        "ctl",
        "force_skip_full_cfg_validation_gate",
        bool(force_skip_full_cfg_validation_gate),
        label="promoted --force-skip-full-cfg-validation-gate",
    )
    put(
        "ctl",
        "force_skip_execution_identity_preflight_check",
        bool(force_skip_execution_identity_preflight_check),
        label="promoted --force-skip-execution-identity-preflight-check",
    )
    put("ctl", "execution_runtime_mode", execution_runtime_mode, label="promoted execution runtime")
    # The run DECLARES its participating providers; cfg gates on
    # membership (`contains`), and every target's provider must be in this list.
    put_list("ctl", "providers", providers, label="promoted --providers")

    # cfg-declared params are inserted first (so they lead the rendered
    # context), but CLI values are staged up front so cfg params may still
    # reference them. Collision semantics are unchanged (hard error).
    staged_cli: dict[str, str] = {}
    for key, value in execution_params.items():
        label = f"--execution-params {key}"
        if not execution_references.CONTEXT_KEY_RE.fullmatch(key):
            raise RuntimeError(f"❌ {label}: key {key!r} must be a valid identifier")
        staged_cli[key] = execution_references._context_scalar(value, label=label)
    lookup = dict(context)
    lookup.update({f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.{key}": value for key, value in staged_cli.items()})

    for key, raw in load_execution_params(ctl_cfg_root).items():
        label = f"{EXECUTION_PARAMS_KEY}.{key}"
        if key in staged_cli:
            raise RuntimeError(
                f"❌ {label} collides with a --execution-params CLI value; define it in one place"
            )
        if isinstance(raw, str):
            match = execution_references.EXECUTION_CONTEXT_PARAM_REF_RE.match(raw.strip())
            if match:
                ref = match.group(1)
                if ref not in lookup:
                    continue
                put("params", key, lookup[ref], label=label)
                lookup[f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.{key}"] = lookup[ref]
                continue
            if "${" in raw:
                raise RuntimeError(
                    f"❌ {label}: only a literal or a whole-value "
                    f"${{{execution_references.EXECUTION_CONTEXT_ROOT}.<ctl|params>.<key>}} reference is allowed, got {raw!r}"
                )
        put("params", key, raw, label=label)
        lookup[f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.{key}"] = context[f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.{key}"]

    for key, value in staged_cli.items():
        put("params", key, value, label=f"--execution-params {key}")

    # Provider-DERIVED params. A fact that is a property of a declared param
    # (not an independent choice) is resolved by the adapter that owns it,
    # instead of being restated at every call site. The engine names no key: it
    # hands each participating adapter the declared params and namespaces
    # whatever it returns. Derived facts never override a declared one.
    declared = {
        ref[len(f"{execution_references.EXECUTION_CONTEXT_ROOT}.params."):]: value
        for ref, value in context.items()
        if ref.startswith(f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.")
    }
    derived_param_keys: list[str] = []
    for provider in providers or ():
        adapter = execution_adapters.get_adapter(provider)
        derive = getattr(adapter, "derived_params", None)
        if derive is None:
            continue
        for key, value in (derive(ctl_cfg_root, dict(declared)) or {}).items():
            derived_param_keys.append(key)
            label = f"provider {provider!r} derived param {key}"
            if key in declared:
                raise RuntimeError(
                    f"❌ {label} collides with a declared execution param; "
                    "a derived fact must not be passed explicitly"
                )
            put("params", key, value, label=label)
    # A DERIVED param is not an input — no caller can supply it, so a
    # target never declares it. Recording which keys were derived lets the per-target
    # filter pass them through without treating them as declarable inputs.
    put_list("ctl", "derived_params", sorted(derived_param_keys), label="derived params")

    # Declared SOURCES. ctl reads each `ctl_sources` entry once, for the
    # frozen context, and publishes the payload here — so a consumer references
    # the fact instead of keeping a second copy of it. The engine names no input
    # and no key: it asks each participating adapter what it resolved and
    # flattens whatever comes back. Values are scalars because that is what a
    # cfg reference can carry.
    declared_sources = load_ctl_sources(ctl_cfg_root)
    for provider in providers or ():
        adapter = execution_adapters.get_adapter(provider)
        resolve_sources = getattr(adapter, "resolved_sources", None)
        if resolve_sources is None:
            continue
        for source_key, payload in (resolve_sources(ctl_cfg_root, dict(context)) or {}).items():
            # WHERE a payload lands is a cfg decision: `publish_under` names the
            # prefix and the payload keeps its own top-level key, so an input file
            # says what it holds and the declaration says where it belongs.
            prefix = (declared_sources.get(source_key) or {}).get("publish_under")
            for leaf_key, leaf_value in cfg_resources._flatten_sourced_payload(
                payload,
                prefix=str(prefix).strip() if prefix else source_key,
                label=f"provider {provider!r} source {source_key!r}",
            ).items():
                put("sourced", leaf_key, leaf_value, label=f"source {source_key!r}")
    return context


def scope_params_from_context(execution_context: dict[str, object]) -> dict[str, str]:
    """

    bare param map used for scope-identity activation (scope mechanism)."""

    prefix = f"{execution_references.EXECUTION_CONTEXT_ROOT}.params."
    return {ref[len(prefix):]: str(value) for ref, value in execution_context.items() if ref.startswith(prefix)}


def write_execution_context_artifact(run_dir: Path, execution_context: dict[str, object]) -> Path:
    path = run_dir / "execution" / EXECUTION_CONTEXT_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    kernel_yaml_io.write_yaml_file(path, execution_references.execution_context_nested(execution_context))
    return path


def execution_context_from_scope_params(scope_params: dict[str, str]) -> dict[str, object]:
    return {f"{execution_references.EXECUTION_CONTEXT_PARAMS_PREFIX}{key}": value for key, value in (scope_params or {}).items()}


def whole_tree_execution_context(
    ctl_cfg_root: Path, execution_context: dict[str, object]
) -> dict[str, object]:
    """/61: a context that activates EVERY declared domain's scopes.

    Scope activation is now the scope's own condition (`contains` over
    `target.domains`), which a real run supplies per target. Whole-tree tooling —
    `validate_cfg`, `regenerate_guardrails` — has no target, so it declares the
    full domain registry: it is validating the tree, not running one target.
    """

    context = dict(execution_context)
    context[f"{execution_references.EXECUTION_CONTEXT_ROOT}.target.domains"] = sorted(
        load_domain_registry(ctl_cfg_root)
    )
    return context


def build_target_execution_context(
    target_run_id: str,
    target_run: dict,
    run_execution_context: dict[str, object],
) -> dict[str, object]:
    """The execution context AS ONE TARGET SEES IT.

    - `ctl.*` passes through unchanged — it is a property of the INVOCATION.
    - `params.*` is FILTERED to the params this target declared. A target cannot
      read a coordinate it did not declare, so a param that is irrelevant to it is
      structurally unreachable rather than merely unused.
    - `target.*` carries what the target declares about itself: the domains it
      reads and its static vars. Never a coordinate, never a ctl-state segment.
    """

    declared = target_run.get("input_params")
    derived = set(
        run_execution_context.get(f"{execution_references.EXECUTION_CONTEXT_ROOT}.ctl.derived_params") or []
    )
    context: dict[str, object] = {}
    for ref, value in run_execution_context.items():
        _, namespace, key = ref.split(".", 2)
        if namespace != "params":
            context[ref] = value
            continue
        if declared is None or key in declared or key in derived:
            context[ref] = value

    if declared:
        missing = sorted(
            k for k in declared
            if f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.{k}" not in context
        )
        if missing:
            raise RuntimeError(
                f"❌ target_run {target_run_id!r} declares input params {missing} that this "
                "run does not supply"
            )

    domains = target_run.get("domains")
    if domains:
        context[f"{execution_references.EXECUTION_CONTEXT_ROOT}.target.domains"] = [str(d) for d in domains]
    for name, value in (target_run.get("static_vars") or {}).items():
        context[f"{execution_references.EXECUTION_CONTEXT_ROOT}.target.static_vars.{name}"] = execution_references._context_scalar(
            value, label=f"target_run {target_run_id!r} static_vars.{name}"
        )
    return context


def resolve_ctl_structure(value, execution_context: dict[str, object], *, label: str = "ctl cfg"):
    """

    deep-resolve every ${execution_context.<ns>.<key>} placeholder in a ctl
    cfg structure, leaving all other leaves untouched. Used to snapshot the ctl
    cfg that drove the run with its vars filled in (e.g. ref_key env/${…} →
    env/dev)."""

    if isinstance(value, dict):
        return {k: resolve_ctl_structure(v, execution_context, label=f"{label}.{k}") for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_ctl_structure(v, execution_context, label=f"{label}[{i}]") for i, v in enumerate(value)]
    if isinstance(value, str) and "${" in value:
        return execution_references.resolve_runtime_scalar(value, execution_context, label=label)
    return value


def ensure_repo_execution_context(repo_path: Path, execution_context_path: Path) -> bool:
    repo_execution_context_path = repo_path / EXECUTION_CONTEXT_FILENAME
    if execution_context_path.resolve() == repo_execution_context_path.resolve():
        return False
    shutil.copy2(execution_context_path, repo_execution_context_path)
    return True


def load_domain_registry(ctl_cfg_root: Path) -> dict:
    """

    the authored domain registry: bare conceptual
    declarations with flat keys. Every `domain` value appearing in cfg is
    validated against these keys, so a typo'd domain becomes a load error
    instead of a silent selector no-match."""

    return cfg_resources.collect_resource(ctl_cfg_root, "domains", entry_depth=1)


def validate_domain_value(domains: dict, value: object, *, label: str) -> None:
    if str(value) not in domains:
        available = ", ".join(sorted(domains)) or "none"
        raise RuntimeError(f"❌ {label}: unknown domain {value!r}; registry declares: {available}")


def credential_free_preflight_failure_reason(error: BaseException) -> str:
    detail = " ".join(str(error).split())
    detail = re.sub(
        r"(?i)((?:access[ _-]?key|secret|token|password)\s*[:=]\s*)\S+",
        r"\1<redacted>",
        detail,
    )
    # Report statuses carry the ❌ mark; the reason text stays plain
    detail = detail.lstrip("❌ ").strip()
    return detail or error.__class__.__name__
