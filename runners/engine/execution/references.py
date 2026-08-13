"""The `${execution_context.*}` reference language.

Cfg refers to run values through this one syntax, so this module owns the
grammar, the resolution and the error message for a miss. It knows nothing
about how a context is BUILT — that would make every reader of a reference
depend on the whole assembly."""

import fnmatch
import re
from enum import StrEnum
from pathlib import Path

RUNTIME_SCALAR_TOKEN_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")


def resolve_runtime_scalar(
    value, context: dict[str, object], *, label: str, tolerate_missing: bool = False
) -> str | None:
    """Resolve ${execution_context.<ns>.<key>} placeholders from the flat
    execution context (dotted keys).

    With `tolerate_missing`, an unbound reference yields None instead of raising —
    used where a domain-GENERIC declaration may simply not apply to this run.
    """

    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} must be a non-empty string")

    token_re = RUNTIME_SCALAR_TOKEN_RE
    if tolerate_missing and any(context.get(ref) in (None, "") for ref in token_re.findall(value)):
        return None

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        resolved = context.get(key)
        if resolved is None or str(resolved) == "":
            raise RuntimeError(f"❌ {label}: {execution_context_miss_message(context, key)}")
        return str(resolved)

    resolved = token_re.sub(replace, value.strip())
    if "${" in resolved:
        raise RuntimeError(
            f"❌ {label} contains an unsupported or unresolved placeholder: {value!r}"
        )
    if not resolved:
        raise RuntimeError(f"❌ {label} resolved to an empty string")
    return resolved


CFG_KEY_ENTRY_RE = re.compile(r"^[A-Za-z_*?\[][A-Za-z0-9_.*?\[\]-]*$")


def resolve_cfg_key_entries(
    cfg_keys: object,
    cfg_key_set_keys: object,
    cfg_key_sets: dict,
    *,
    label: str,
    cfg_path: Path,
    _stack: tuple = (),
) -> list[str]:
    """Resolve one consumer's contract into concrete key SELECTORS.

    Set REFERENCES and content KEYS are separate fields, exactly as the
    predecessor `cfg_file_sets` split `cfg_file_set_keys` from `cfg_files`. They
    are different things to the engine — one is a ctl catalog lookup, the other a
    plt content address — so they cannot collide, and a plt key is free to be
    named like a set.

    Referenced sets are spliced FIRST (they carry the axis/naming prefix), then
    the consumer's own keys. A key is an exact name, a dotted sub-path, or a glob
    over top-level names.
    """
    resolved: list[str] = []
    if cfg_key_set_keys is not None:
        if not isinstance(cfg_key_set_keys, list) or not cfg_key_set_keys:
            raise RuntimeError(f"❌ {label} cfg_key_sets must be a non-empty list ({cfg_path})")
        for name in cfg_key_set_keys:
            if not isinstance(name, str) or not name.strip():
                raise RuntimeError(
                    f"❌ {label} cfg_key_sets entries must be non-empty strings ({cfg_path})"
                )
            name = name.strip()
            if name not in cfg_key_sets:
                available = ", ".join(sorted(cfg_key_sets)) or "none"
                raise RuntimeError(
                    f"❌ {label}: undefined cfg_key_set {name!r}; declared: {available} ({cfg_path})"
                )
            if name in _stack:
                cycle = " -> ".join([*_stack, name])
                raise RuntimeError(f"❌ cfg_key_set cycle: {cycle} ({cfg_path})")
            member = cfg_key_sets[name]
            if not isinstance(member, dict):
                raise RuntimeError(
                    f"❌ cfg_key_set {name!r} must be a mapping of "
                    f"cfg_key_sets / cfg_keys ({cfg_path})"
                )
            unknown = sorted(set(member) - {"cfg_key_sets", "cfg_keys"})
            if unknown:
                raise RuntimeError(
                    f"❌ cfg_key_set {name!r} has unsupported keys {unknown} ({cfg_path})"
                )
            resolved.extend(
                resolve_cfg_key_entries(
                    member.get("cfg_keys"),
                    member.get("cfg_key_sets"),
                    cfg_key_sets,
                    label=f"cfg_key_set {name!r}",
                    cfg_path=cfg_path,
                    _stack=(*_stack, name),
                )
            )
    if cfg_keys is not None:
        if not isinstance(cfg_keys, list) or not cfg_keys:
            raise RuntimeError(f"❌ {label} cfg_keys must be a non-empty list ({cfg_path})")
        for entry in cfg_keys:
            if not isinstance(entry, str) or not entry.strip():
                raise RuntimeError(
                    f"❌ {label} cfg_keys entries must be non-empty strings ({cfg_path})"
                )
            entry = entry.strip()
            if not CFG_KEY_ENTRY_RE.fullmatch(entry):
                raise RuntimeError(
                    f"❌ {label}: {entry!r} is not a legal cfg key selector "
                    f"(key, key.path, or glob) ({cfg_path})"
                )
            resolved.append(entry)
    if not resolved:
        raise RuntimeError(f"❌ {label} declares neither cfg_key_sets nor cfg_keys ({cfg_path})")
    seen: set[str] = set()
    return [e for e in resolved if not (e in seen or seen.add(e))]


def project_cfg_keys(doc: dict, entries: list[str], *, label: str) -> dict:
    """Project the declared key selectors out of one domain's merged cfg doc.

    Assertion 1 (every declared key resolves in its domain) and assertion 5 (a
    glob matching nothing is a stale declaration) are enforced here — an entry
    that selects nothing is an ERROR, never a silent empty view.
    """
    projected: dict = {}
    for entry in entries:
        if entry == "*":
            matched = list(doc)
        elif any(ch in entry for ch in "*?["):
            matched = sorted(k for k in doc if fnmatch.fnmatchcase(k, entry))
        else:
            head, _, path = entry.partition(".")
            if head not in doc:
                raise RuntimeError(
                    f"❌ {label}: cfg key {entry!r} does not resolve in this domain; "
                    f"available: {', '.join(sorted(doc)) or 'none'}"
                )
            if not path:
                matched = [head]
            else:
                node = doc[head]
                for segment in path.split("."):
                    if not isinstance(node, dict) or segment not in node:
                        raise RuntimeError(
                            f"❌ {label}: cfg key path {entry!r} does not resolve in this domain"
                        )
                    node = node[segment]
                branch = projected.setdefault(head, {})
                if not isinstance(branch, dict):
                    raise RuntimeError(
                        f"❌ {label}: cfg key path {entry!r} conflicts with whole key {head!r}"
                    )
                cursor = branch
                segments = path.split(".")
                for segment in segments[:-1]:
                    cursor = cursor.setdefault(segment, {})
                cursor[segments[-1]] = node
                continue
        if not matched:
            raise RuntimeError(
                f"❌ {label}: cfg key selector {entry!r} matched no key in this domain "
                "(stale declaration)"
            )
        for key in matched:
            projected[key] = doc[key]
    return projected


EXECUTION_CONTEXT_ROOT = "execution_context"


class ContextNamespace(StrEnum):
    """The namespaces of the execution context, by KIND of fact.

    ctl     the invocation      action, profile, providers, runtime mode
    params  COORDINATES         identify an instance; become ctl-state path segments
    target  the target's own    what it reads (domains) + its constants (static_vars);
                                never a coordinate, never a path segment
    sourced pulled from elsewhere

    A namespace is the second segment of every reference, so the set is closed by
    the reference grammar itself: a name not here cannot be written down.
    """

    CTL = "ctl"
    PARAMS = "params"
    TARGET = "target"
    SOURCED = "sourced"


EXECUTION_CONTEXT_NAMESPACES = tuple(ContextNamespace)


EXECUTION_CONTEXT_PARAMS_PREFIX = f"{EXECUTION_CONTEXT_ROOT}.{ContextNamespace.PARAMS}."


# The key may itself be dotted — that is how provider-specific params are
# namespaced under their provider. Provider-neutral params keep a single segment
# (execution_context.params.env.type). Kept in sync with CONTEXT_KEY_RE, which
# validates the key on the way in.
EXECUTION_CONTEXT_REF_RE = re.compile(
    rf"^{EXECUTION_CONTEXT_ROOT}\.(?:{'|'.join(EXECUTION_CONTEXT_NAMESPACES)})"
    rf"\.[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)


def validate_execution_context_ref(ref: str, *, label: str) -> str:
    """Selector/constraint/interpolation references into the execution context are

    always fully-qualified paths starting at the root key.
    """

    if not isinstance(ref, str) or not ref.strip():
        raise RuntimeError(f"❌ {label}: execution-context reference must be a non-empty string")
    value = ref.strip()
    if not EXECUTION_CONTEXT_REF_RE.fullmatch(value):
        raise RuntimeError(
            f"❌ {label}: reference {value!r} must be a fully-qualified execution-context path "
            f"({EXECUTION_CONTEXT_ROOT}.<{'|'.join(EXECUTION_CONTEXT_NAMESPACES)}>.<key>)"
        )
    return value


def execution_context_miss_message(execution_context: dict[str, object], ref: str) -> str:
    available = ", ".join(sorted(execution_context)) or "none"
    return f"{ref!r} not found in execution context; available: {available}"


# An execution-context key is one or more identifier segments joined by dots.
# The dotted form is how provider-specific params are namespaced under their
# provider (`<provider>.<param>`), so one provider's vocabulary cannot collide
# with another's; provider-neutral params stay single-segment (`env_type`,
# `landing_zone`). The context is a flat dotted map, so a dotted key is simply a
# longer key — selectors already match full paths. The engine assigns no meaning
# to the leading segment; only the provider adapter interprets it.
CONTEXT_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")


# The key may be DOTTED — namespaced params are the normal case, so a
# whole-value reference must be able to name one.
EXECUTION_CONTEXT_PARAM_REF_RE = re.compile(
    rf"^\$\{{({EXECUTION_CONTEXT_ROOT}\.(?:ctl|params)\."
    rf"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\}}$"
)


def _context_scalar(value, *, label: str):
    if isinstance(value, (str, int, float, bool)):
        return value
    raise RuntimeError(f"❌ {label} must resolve to a scalar string/number/bool value")


def execution_context_nested(execution_context: dict[str, object]) -> dict[str, dict[str, object]]:
    """Nested {execution_context: {ctl: {...}, params: {...}}} view.

    Dotted keys nest fully (params.<provider>.x -> params: {<provider>: {x: ...}}),
    so the rendered artifact reads as structure, not as flat dotted strings. A
    scalar and a subtree cannot share a path — that collision fails loud.
    """
    nested: dict[str, dict[str, object]] = {ns: {} for ns in EXECUTION_CONTEXT_NAMESPACES}
    for ref, value in execution_context.items():
        _, namespace, key = ref.split(".", 2)
        node = nested[namespace]
        segments = key.split(".")
        for segment in segments[:-1]:
            child = node.setdefault(segment, {})
            if not isinstance(child, dict):
                raise RuntimeError(f"❌ execution context key {ref!r} nests under a scalar fact")
            node = child
        if isinstance(node.get(segments[-1]), dict):
            raise RuntimeError(f"❌ execution context key {ref!r} collides with a nested subtree")
        node[segments[-1]] = value
    return {EXECUTION_CONTEXT_ROOT: nested}
