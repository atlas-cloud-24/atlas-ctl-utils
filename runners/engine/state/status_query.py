"""Turning a computed status map into what a reader asked for.

Filtering, sorting and structuring only: nothing here reads or writes state,
which is why it can be split off `state/status.py` without a single import
back into it.
"""

from enum import StrEnum

from engine.kernel import paths as kernel_paths
from engine.run import addressing as run_addressing


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
            raise RuntimeError(f"❌ --filter must use FIELD=VALUE, got: {item!r}")
        if field not in FILTER_FIELDS:
            raise RuntimeError(
                f"❌ --filter field {field!r} unknown; expected one of {', '.join(FILTER_FIELDS)}"
            )
        if "*" in value and (value == "*" or value.count("*") != 1 or not value.endswith("*")):
            raise RuntimeError(
                f"❌ --filter supports one trailing * after a non-empty prefix, got: {value!r}"
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

    address = "/".join(
        [
            kind,
            run_addressing.instance_address(template, segments.split("/") if segments else []),
        ]
    )
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
            "instances": sort_rows([flat_row(*entry) for entry in walk_status_map(instances)], keys)
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
                _filter_value_matches(sort_value(candidate, field), pattern) for pattern in patterns
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
