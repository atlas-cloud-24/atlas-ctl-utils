"""Drawing a whole-namespace status map as a table.

Presentation only, over the SAME report object the YAML path prints. The two
renderings read one report, so they cannot disagree about facts — only about how
much of it they show.

YAML stays the default and the only machine-readable form. A table has to drop
what has no column: a workflow's `selectors` is a nested mapping and belongs to a
definition rather than to a result, and its `members` are objects, which the
nested shape renders as child rows and the flat shape can only count. That is why
the renderings are required to agree in TRUTH and not in completeness.

Separate from the map it draws for the same reason the preflight renderer is:
adding a fact to the report never means touching a branch here — the columns are
an allowlist, and a new fact reaches a table only when someone chooses one for it.
"""

from enum import StrEnum

from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.state import status as state_status


class StatusFormat(StrEnum):
    """How `status --all` renders the report it computed.

    Orthogonal to `StatusStructure`: the structure decides what the report SAYS,
    the format decides who it is written for. Every combination is meaningful, so
    neither is a mode of the other.
    """

    YAML = "yaml"
    TABLE = "table"


STATUS_FORMATS = tuple(StatusFormat)


# What the report says about ITSELF, in the order a header states it: WHERE the
# rows came from, then HOW they were shaped, then WHEN they were computed. A
# filter prints only when one was applied, for the same reason the report omits
# it — an absent filter is not a fact about the namespace.
HEADER_FIELDS = (
    "namespace", "scope", "structure", "sort", "filters", "computed_at",
)

# The paths `--write-cache` wrote. They are the one thing a reader needs back out
# of the command rather than out of the map, so they survive as a footer.
FOOTER_FIELDS = ("cache_written", "history_written")

# The columns a flat table carries, in order — the SORT VOCABULARY itself, not a
# second list beside it. A column and a sortable field are the same thing: the
# scalar facts a row carries, which is also what `--filter` matches on. Declaring
# them apart would let a column exist that could not be sorted by, or a sort field
# with no column to read the result in, with nothing to say which was intended.
FLAT_COLUMNS = state_status.SORT_FIELDS

# The nested table spends its first column on the tree, so `address` and `group`
# are POSITIONS there rather than cells, and members become child rows instead of
# a count.
NESTED_COLUMNS = (
    "status", "last_action", "last_operation", "standing", "superseded_by", "freshness", "time",
    "label",
)

# The columns EVERY row could carry, whatever kind it is and whatever the cfg
# declares. They are printed even when no row in view fills one, because their
# emptiness is a fact about these runs rather than about the vocabulary — a
# namespace where nothing was labelled has to be readable as exactly that, not as
# one where labelling does not exist.
#
# Every other column is CONDITIONAL, and is dropped when nothing in view has one:
#
#   last_action              a workflow row cannot have one; a workflow declares
#                            a default_action and its members carry the actions
#   standing, superseded_by  only exist inside an exclusive relation
#   freshness                only a record that can go stale has one — a destroy
#                            record describes an instance that is GONE
#   members                  only a workflow composes any
#
# The split decides only whether a column APPEARS when nothing in view fills it.
# A whole column of dashes would answer a question the vocabulary never posed —
# "did any of these have a standing?" is not a question in a namespace with no
# exclusive relations, while "was any of these labelled?" always is.
#
# Once a column IS shown, every reporting line takes the marker where it has no
# value, including a kind that could not have filled it (a workflow line under a
# LAST_ACTION column that a target line kept alive). Reading that per kind would
# need a second cell vocabulary inside one grid, and the grid is what makes the
# table scannable.
UNCONDITIONAL_COLUMNS = ("address", "group", "status", "time", "label")

# What an unconditional column shows when the row has no value.
#
# One character, so it never widens a column past its heading; conventional, so
# it is read as absence on sight; and unambiguous BY CONSTRUCTION — a run label
# may not start with `-` (it is passed on to child runs as argv, where argparse
# would read it as a flag), so a dash can never be mistaken for a real one.
# A blank was tried first and is worse than it looks: trailing cells are stripped,
# so an unset last column is indistinguishable from a row that simply ended.
EMPTY_CELL = "-"

# Two spaces: enough to separate columns without a rule, which would cost a line
# of vertical space per row on a surface read by scrolling.
COLUMN_GAP = "  "

# One indent level per nesting level the report already has: kind/template,
# instance, group, member.
INDENT = "  "

# A namespace with no instances prints this rather than a header with nothing
# under it, which reads as truncated output rather than as an answer.
EMPTY_MAP_LINE = "(no instances)"


def _value(row: dict, column: str) -> str:
    """One row's value for one column, as text, or `""` when it has none.

    `members` is the only COMPUTED value: a flat row has nowhere to nest a list
    of member objects, so it carries how many there were. Everything else is the
    report's own value, printed.
    """

    if column == "members":
        members = row.get("members")
        return str(len(members)) if isinstance(members, list) else ""
    value = row.get(column)
    return "" if value is None else str(value)


def _cell(row: dict | None, column: str, *, reports_a_run: bool) -> str:
    """What a printed cell shows.

    `reports_a_run` is what decides an absent value. A line that REPORTS one — a
    flat row, a nested group — shows the absence marker, because that run could
    have carried the fact and did not. A line that NAMES something instead shows
    blank: a kind/template or instance line has no run of its own, and a member
    line is a reference to a target row that appears elsewhere in this same map,
    so a dash on either would answer a question they were never asked.
    """

    if row is None:
        return ""
    value = _value(row, column)
    if value:
        return value
    return EMPTY_CELL if reports_a_run else ""


def _present_columns(rows: list[dict], columns: tuple[str, ...]) -> list[str]:
    """The columns this table prints.

    An unconditional column stays even when nothing fills it — its emptiness is
    a fact about these runs. A conditional one is dropped, because nothing in
    view could have carried it and a column of dashes would say otherwise.
    """

    return [
        column
        for column in columns
        if column in UNCONDITIONAL_COLUMNS
        or any(_value(row, column) for row in rows)
    ]


def _grid_lines(
    headings: list[str],
    rows: list[list[str]],
    *,
    first_column_width: int | None = None,
) -> list[str]:
    """Left-aligned columns, each sized to its widest cell, headings included.

    `first_column_width` lets a caller size column 0 from fewer rows than it
    prints. A cell wider than its column OVERFLOWS rather than being truncated:
    the row's later cells shift right and every other row stays where it was,
    which loses alignment on one line instead of on all of them.
    """

    grid = [headings, *rows]
    widths = [max(len(row[index]) for row in grid) for index in range(len(headings))]
    if first_column_width is not None:
        widths[0] = first_column_width
    return [
        COLUMN_GAP.join(
            cell.ljust(width) for cell, width in zip(row, widths)
        ).rstrip()
        for row in grid
    ]


def _field_text(value) -> str:
    """A report-level value as one line.

    A filter map is written back in the spelling that produced it, so a reader can
    copy the line into the next command rather than translate a mapping by hand.
    """

    if isinstance(value, dict):
        # One `field=value` per value, never `field=a,b`: the line is meant to be
        # copied back into the next command, and a comma in --filter separates
        # PAIRS, so the compact spelling would not parse.
        return "  ".join(
            f"{field}={item}" for field, values in value.items() for item in values
        )
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def _field_lines(report: dict, fields: tuple[str, ...]) -> list[str]:
    """Report-level fields as an aligned `name  value` block.

    Not a table: these describe the whole report rather than one row of it, so
    giving them column headings would claim they vary per row.
    """

    pairs = [(field, _field_text(value))
             for field in fields
             if (value := report.get(field)) is not None]
    if not pairs:
        return []
    width = max(len(name) for name, _ in pairs)
    return [f"{name.ljust(width)}{COLUMN_GAP}{value}" for name, value in pairs]


def _flat_lines(rows: list[dict]) -> list[str]:
    """One row per group, each carrying its own address — what flat already means."""

    # On the ROWS, not on the columns: an unconditional column is present
    # whatever the data, so an empty namespace would otherwise print a heading
    # with nothing under it — the truncated-looking output EMPTY_MAP_LINE exists
    # to replace.
    if not rows:
        return []
    columns = _present_columns(rows, FLAT_COLUMNS)
    return _grid_lines(
        [column.upper() for column in columns],
        [
            [_cell(row, column, reports_a_run=True) for column in columns]
            for row in rows
        ],
    )


def _group_entries(groups: dict, *, depth: int) -> list[tuple[str, dict | None, bool]]:
    """One entry per status group, followed by the members that group ran with."""

    entries: list[tuple[str, dict | None, bool]] = []
    for group, row in groups.items():
        entries.append((INDENT * depth + str(group), row, False))
        for member in row.get("members") or []:
            # A member row reports what THAT member did under the group above it,
            # so its `action` fills the same column a group's `last_action` does.
            # One column grid for the whole table is what makes it scannable; a
            # second name for one fact would break that for no gain.
            entries.append(
                (
                    INDENT * (depth + 1) + "└ " + str(member.get("address", "")),
                    {**member, "last_action": member.get("action")},
                    True,
                )
            )
    return entries


def _nested_entries(kinds: dict) -> list[tuple[str, dict | None, bool]]:
    """The nested map flattened to `(tree cell, row, is_member)` triples.

    Indentation carries the structure the YAML carries as nesting. A kind/template
    line and an instance line carry `None` rather than an empty row: they NAME
    what the rows below them belong to, so their cells stay blank — a dash there
    would claim a template has no status, when a template has no status to have.

    `is_member` marks the leaf annotations. A member is not a level of the tree —
    the state layout has no member directory, and a member's address names a
    target that lives elsewhere in this same map — so it is excluded from the
    tree column's width below.
    """

    entries: list[tuple[str, dict | None, bool]] = []
    for kind, templates in kinds.items():
        for template, body in templates.items():
            entries.append((f"{kind}{COLUMN_GAP}{template}", None, False))
            if run_addressing.INSTANCES_MARKER in body:
                for segments, groups in body[run_addressing.INSTANCES_MARKER].items():
                    entries.append((INDENT + str(segments), None, False))
                    entries.extend(_group_entries(groups, depth=2))
            else:
                # A singleton has no segments and therefore no instance line,
                # exactly as the state layout omits the `instances/` level.
                entries.extend(_group_entries(body, depth=1))
    return entries


def _nested_lines(kinds: dict) -> list[str]:
    """The kind/template/instance tree in the first column, groups as its rows."""

    entries = _nested_entries(kinds)
    if not entries:
        return []
    columns = _present_columns(
        [row for _, row, _ in entries if row is not None], NESTED_COLUMNS
    )
    # The tree column is sized by the TREE, not by the member addresses hanging
    # off it: a member sits at the deepest indent and carries the longest text, so
    # letting it set the width pushed every group's axes off to the right and made
    # the column a reader scans down unreadable. Members overflow instead.
    tree_width = max(len(tree) for tree, _, is_member in entries if not is_member)
    # The tree column gets no heading: it holds four different kinds of thing —
    # a template, an instance, a group, a member — and no one word is true of all
    # of them. The axis headings are what a reader needs to tell the columns apart.
    return _grid_lines(
        ["", *[column.upper() for column in columns]],
        [
            [
                tree,
                *[
                    _cell(row, column, reports_a_run=not is_member)
                    for column in columns
                ],
            ]
            for tree, row, is_member in entries
        ],
        first_column_width=tree_width,
    )


def render_status_map(report: dict) -> str:
    """A whole-namespace status report as text.

    Dispatches on the report's OWN `structure`, so `--format` and `--structure`
    stay orthogonal — each shape has a rendering, and neither argument narrows
    the other.
    """

    structure = str(report.get("structure") or "")
    if structure == state_status.StatusStructure.FLAT:
        body = _flat_lines(list(report.get("instances") or []))
    elif structure == state_status.StatusStructure.NESTED:
        body = _nested_lines(
            {kind: report[kind] for kind in run_actions.RESULT_KINDS if kind in report}
        )
    else:
        raise RuntimeError(
            f"❌ status report has structure {structure!r}; expected one of "
            f"{', '.join(state_status.STATUS_STRUCTURES)}"
        )
    sections = [
        _field_lines(report, HEADER_FIELDS),
        body or [EMPTY_MAP_LINE],
        _field_lines(report, FOOTER_FIELDS),
    ]
    return "\n\n".join("\n".join(section) for section in sections if section)
