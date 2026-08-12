"""Resolving a cfg cross-reference that STATES the collection it points into.

A bare key leaves the collection implied by the field name, which holds only
where the two match. They often do not: `input_param_sets` resolves against
`param_sets`, `providers` against `execution_providers`, `account` against
`accounts_registry`. So the value carries the path, and a path naming the wrong
collection is REFUSED — that check is what the extra text buys; without it the
path is a comment.

    source_key: target_sources.core
    #           |_ collection _| |key|

This module holds NO collection names. The caller passes the collection it is
about to look the value up in, which it already names one line later, so the
mechanism stays generic and each fact lives at the single site that knows it.
Provider-scoped collections are resolved by the provider ADAPTER for the same
reason: engine core never spells them.

SEPARATORS keep their meanings. `.` navigates cfg structure and `/` stays inside
a key, so the key is taken WHOLE after the declared prefix and may contain
slashes (`targets.env/core/baseline`). The prefix length is fixed by the
collection the caller names, so nothing has to guess where the split falls.

A path names an ENTRY and yields its KEY. It cannot reach into the entry's
fields — there is no `...target_roles.x.role_name` — which is what stops a value
being inlined across a boundary. The caller then resolves that key against the
collection and reads whatever fields it needs.

MAPPING KEYS ARE NOT QUALIFIED. A mapping key is an index, not a value: the
engine already holds the domain it looks `cfg_key_sets` up by, so a qualified key
would be built only to be stripped back to what it started with.
"""

# A path segment supplied by the declaration around the value, so one call covers
# every provider instead of one per provider.
WILDCARD = "*"


def _prefix_matches(expected: list[str], actual: list[str]) -> bool:
    return len(expected) == len(actual) and all(
        want in (WILDCARD, got) for want, got in zip(expected, actual, strict=False)
    )


def resolve(value, collection: str, *, label: str) -> str:
    """`<collection>.<key>` -> `<key>`, or refuse.

    `collection` may carry `*` for a segment the surrounding declaration
    supplies, e.g. `providers.*.target_roles`.
    """

    if not isinstance(value, str):
        raise RuntimeError(f"❌ {label}: expected a {collection} reference, got {value!r}")
    expected = collection.split(".")
    parts = value.split(".")
    if len(parts) <= len(expected) or not _prefix_matches(expected, parts[: len(expected)]):
        raise RuntimeError(
            f"❌ {label}: {value!r} does not name its collection — expected {collection}.<key>"
        )
    return ".".join(parts[len(expected) :])


def resolve_each(values, collection: str, *, label: str):
    """Resolve a list of references, or a mapping whose VALUES are references.

    A mapping's keys are left alone: `roles: {readwrite: <ref>}` names a role
    class on the left, which is an index rather than a reference.
    """

    if isinstance(values, list):
        return [resolve(item, collection, label=label) for item in values]
    if isinstance(values, dict):
        return {name: resolve(item, collection, label=label) for name, item in values.items()}
    return resolve(values, collection, label=label)


def resolve_fields(entry: dict, fields: dict, *, label: str) -> dict:
    """Resolve every declared field of one entry, leaving the rest untouched.

    `fields` maps a field name to the collection its values name. The caller owns
    that mapping because the caller owns the vocabulary: a target knows that
    `source_key` means `target_sources`, and nothing more general does.
    """

    resolved = dict(entry)
    for field, collection in fields.items():
        if field in resolved and resolved[field] is not None:
            resolved[field] = resolve_each(resolved[field], collection, label=f"{label}.{field}")
    return resolved
