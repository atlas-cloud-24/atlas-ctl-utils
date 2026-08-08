"""`--format` renders the status report; it never computes a different one.

`status --all` emitted YAML and nothing else, which is right for one instance and
for another program, and hard to scan across a namespace: `status`, `time` and
`freshness` sit four levels deep, so comparing them across instances means
reading down a page instead of across a column.

The rule the table lives under is that the two renderings agree in TRUTH and not
in completeness. A table drops what has no column — a workflow's `selectors` is a
nested mapping, its `members` are objects — but it may never show a value the
report does not hold, and rendering may never change the report. Those are the
claims worth guarding; the exact spacing is not.

`--format` is also ORTHOGONAL to `--structure`: both shapes have a table, and
today's YAML output is one of four cells rather than a special case.
"""

import argparse
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cli import args as cli_args
from engine.state import render as state_render
from engine.state import status as state_status

MEMBER = {
    "address": "target/env/seed/baseline/instances/env.type=dev/aws.account=dev",
    "action": "plan",
    "freshness": "undetermined",
}

NESTED_REPORT = {
    "namespace": "aws/nonprod",
    "scope": "local",
    "computed_at": "2026-08-08T09:12:04Z",
    "structure": "nested",
    "sort": "address",
    "target": {
        "env/seed/baseline": {
            "instances": {
                "env.type=dev/aws.account=dev": {
                    "mutative": {
                        "status": "passed",
                        "last_action": "provision",
                        "freshness": "undetermined",
                        "time": "2026-08-06T11:39Z",
                        "label": "release-2026.08",
                    }
                }
            }
        }
    },
    "workflow": {
        "env/seed": {
            "instances": {
                "env.type=dev/aws.account=dev": {
                    "plan": {
                        "status": "passed",
                        "freshness": "undetermined",
                        "time": "2026-08-05T18:21Z",
                        "selectors": {"match": {"execution_context.params.env.type": "dev"}},
                        "label": "release-2026.07",
                        "members": [MEMBER],
                    }
                }
            }
        }
    },
}

FLAT_REPORT = {
    "namespace": "aws/nonprod",
    "scope": "local",
    "computed_at": "2026-08-08T09:12:04Z",
    "structure": "flat",
    "sort": "time:desc",
    "instances": [
        {
            "address": "workflow/env/seed/instances/env.type=dev/aws.account=dev",
            "group": "plan",
            "status": "passed",
            "freshness": "undetermined",
            "time": "2026-08-05T18:21Z",
            "selectors": {"match": {"execution_context.params.env.type": "dev"}},
            "members": [MEMBER, MEMBER],
            "label": "release-2026.07",
        },
        {
            "address": "target/env/seed/baseline/instances/env.type=dev/aws.account=dev",
            "group": "mutative",
            "status": "failed",
            "time": "2026-08-04T07:07Z",
        },
    ],
}


def _status_args(**overrides) -> argparse.Namespace:
    """The slim status namespace, defaulted to a read that would otherwise pass."""

    values = {
        "execution_param": [],
        "scope": "local",
        "all": False,
        "structure": None,
        "output_format": None,
        "write_cache": False,
        "hydrate_to": None,
        "ctl_state_local_root": None,
        "provider_options": {},
        "action": "provision",
        "target": "env/seed/baseline",
        "workflow": None,
        "fan_out": None,
        "sort": "address",
        "filters": [],
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class FormatIsDeclaredAsAReadingChoiceTest(unittest.TestCase):
    def test_yaml_is_the_default(self):
        """Not passing `--format` must leave the machine-readable shape, or every
        tool reading `status` breaks on an engine upgrade."""

        parser = argparse.ArgumentParser()
        cli_args.add_status_args(parser)
        action = next(a for a in parser._actions if "--format" in a.option_strings)
        self.assertIsNone(action.default)
        self.assertEqual(list(action.choices), ["yaml", "table"])

    def test_it_renders_all_and_refuses_a_targeted_query(self):
        """A targeted query names ONE instance, which is what the YAML report is
        already right for; the table is defined over a namespace map."""

        with self.assertRaises(RuntimeError):
            cli_args.finalize_status_args(_status_args(output_format="table"))

    def test_every_structure_and_format_pairing_renders(self):
        """Orthogonal: neither argument narrows the other, so all four cells are
        reachable and today's output is one of them."""

        for report in (NESTED_REPORT, FLAT_REPORT):
            with self.subTest(structure=report["structure"]):
                self.assertTrue(state_render.render_status_map(report).strip())

    def test_an_unrecognised_structure_fails_loud(self):
        with self.assertRaises(RuntimeError):
            state_render.render_status_map({"structure": "tree"})


class TheTableShowsOnlyWhatTheReportHoldsTest(unittest.TestCase):
    def test_rendering_never_changes_the_report(self):
        """The YAML path prints the same object; a renderer that edited it would
        make the two disagree about facts, which is the one thing forbidden."""

        for report in (NESTED_REPORT, FLAT_REPORT):
            with self.subTest(structure=report["structure"]):
                import copy

                before = copy.deepcopy(report)
                state_render.render_status_map(report)
                self.assertEqual(report, before)

    def test_a_flat_row_carries_its_own_address_and_its_group(self):
        rendered = state_render.render_status_map(FLAT_REPORT)
        for row in FLAT_REPORT["instances"]:
            with self.subTest(address=row["address"]):
                self.assertIn(row["address"], rendered)
                self.assertIn(row["group"], rendered)

    def test_a_flat_row_counts_its_members_because_it_cannot_nest_them(self):
        """One row per group is what flat already means, so a member can only be
        a number — and must not become a row of its own."""

        rendered = state_render.render_status_map(FLAT_REPORT)
        headings, *rows = [
            line for line in rendered.splitlines() if line.startswith(("ADDRESS", "workflow/", "target/"))
        ]
        self.assertIn("MEMBERS", headings)
        self.assertEqual(len(rows), len(FLAT_REPORT["instances"]))
        members_cell = rows[0].split()[headings.split().index("MEMBERS")]
        self.assertEqual(members_cell, str(len(FLAT_REPORT["instances"][0]["members"])))

    def test_the_nested_table_renders_members_as_rows_under_their_group(self):
        """Nesting exists to show what a composition ran with, so the shape that
        has room for members uses it."""

        rendered = state_render.render_status_map(NESTED_REPORT)
        self.assertIn(MEMBER["address"], rendered)
        member_line = next(
            line for line in rendered.splitlines() if MEMBER["address"] in line
        )
        self.assertIn(MEMBER["freshness"], member_line)
        # A member's `action` fills the same column a group's `last_action` does:
        # one column grid for the whole table is what makes it scannable.
        self.assertIn(MEMBER["action"], member_line)

    def test_the_tree_column_is_sized_by_the_tree_and_not_by_its_members(self):
        """A member sits at the deepest indent and carries the longest text.
        Letting it set the width pushed every group's axes off to the right and
        made the one column a reader scans down unreadable."""

        rendered = state_render.render_status_map(NESTED_REPORT)
        group_line = next(
            line for line in rendered.splitlines() if line.startswith("    mutative")
        )
        self.assertLess(group_line.index("passed"), len(MEMBER["address"]))

    def test_a_nested_mapping_has_no_column_and_stays_in_the_yaml(self):
        """`selectors` belongs to a definition rather than to a result, so it is
        dropped rather than flattened into an unreadable cell."""

        for report in (NESTED_REPORT, FLAT_REPORT):
            with self.subTest(structure=report["structure"]):
                self.assertNotIn(
                    "execution_context.params.env.type",
                    state_render.render_status_map(report),
                )

    def test_columns_are_an_allowlist(self):
        """A fact added to the report reaches YAML immediately and a table only
        once someone chooses a column for it — the same rule as AXIS_ORDER."""

        invented = dict(FLAT_REPORT)
        invented["instances"] = [
            {**FLAT_REPORT["instances"][0], "cloud_cost_estimate": "17 USD"}
        ]
        self.assertNotIn("17 USD", state_render.render_status_map(invented))

    def test_a_conditional_column_no_row_fills_is_dropped(self):
        """`standing` exists only inside an exclusive relation. A namespace with
        none was never asked the question, so a column of dashes would answer
        one nobody posed."""

        self.assertNotIn("standing", state_render.UNCONDITIONAL_COLUMNS)
        self.assertNotIn("STANDING", state_render.render_status_map(FLAT_REPORT))

    def test_an_unconditional_column_stays_and_marks_the_absence(self):
        """Every run could carry a label, so a namespace where none does has to
        read as exactly that — not as one where labelling does not exist. That
        was the whole reported defect: `--format table` hid the column."""

        unlabelled = dict(FLAT_REPORT)
        unlabelled["instances"] = [
            {k: v for k, v in row.items() if k != "label"}
            for row in FLAT_REPORT["instances"]
        ]
        rendered = state_render.render_status_map(unlabelled)
        self.assertIn("LABEL", rendered)
        self.assertTrue(
            all(
                line.rstrip().endswith(state_render.EMPTY_CELL)
                for line in rendered.splitlines()
                if line.startswith(("workflow/", "target/"))
            )
        )

    def test_the_absence_marker_can_never_be_mistaken_for_a_label(self):
        """`-` is unambiguous BY CONSTRUCTION: a run label may not start with one,
        because it is passed on to child runs as argv."""

        with self.assertRaises(RuntimeError):
            cli_args.normalize_run_label(state_render.EMPTY_CELL)

    def test_a_line_that_names_something_stays_blank(self):
        """A template, an instance and a member NAME a thing rather than report a
        run. A member points at a target row that appears elsewhere in the same
        map, so a dash on either would answer a question they were never asked."""

        rendered = state_render.render_status_map(NESTED_REPORT)
        for prefix in ("target  ", "  env.type=", "      └ "):
            line = next(
                line for line in rendered.splitlines() if line.startswith(prefix)
            )
            with self.subTest(line=prefix):
                self.assertNotIn(
                    f"  {state_render.EMPTY_CELL} ", f"{line} "
                )

    def test_the_report_still_says_where_it_came_from(self):
        """Namespace and scope are what make the rows trustworthy — local and
        bucket history legitimately differ."""

        rendered = state_render.render_status_map(FLAT_REPORT)
        for field in ("namespace", "scope", "computed_at"):
            with self.subTest(field=field):
                self.assertIn(field, rendered)
                self.assertIn(str(FLAT_REPORT[field]), rendered)

    def test_written_cache_paths_survive_the_rendering(self):
        """They are the one thing a reader needs back out of the command rather
        than out of the map."""

        with_cache = {**FLAT_REPORT, "cache_written": "/root/ns/status_cache.yaml"}
        self.assertIn(
            "/root/ns/status_cache.yaml", state_render.render_status_map(with_cache)
        )

    def test_an_empty_namespace_says_so(self):
        """A header with nothing under it reads as truncated output rather than
        as an answer."""

        empty = {**FLAT_REPORT, "instances": []}
        self.assertIn(
            state_render.EMPTY_MAP_LINE, state_render.render_status_map(empty)
        )


class TheTableAgreesWithTheStructureItRendersTest(unittest.TestCase):
    def test_the_structure_names_come_from_one_place(self):
        """`--format` dispatches on the report's own `structure`, so the two
        vocabularies cannot drift into a shape with no rendering."""

        self.assertEqual(
            sorted(state_status.STATUS_STRUCTURES),
            sorted((NESTED_REPORT["structure"], FLAT_REPORT["structure"])),
        )


if __name__ == "__main__":
    unittest.main()
