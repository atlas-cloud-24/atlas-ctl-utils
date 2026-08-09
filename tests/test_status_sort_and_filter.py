"""Ordering and narrowing a namespace map, over one vocabulary.

A status row carries a handful of scalar facts. Every one of them is worth
ordering by and worth narrowing by, and both questions are asked of the SAME
fact — "which of these ran last" and "only the ones that failed" are the column
you are already reading, used two ways. So there is one field list, and
`--sort`/`--filter` both take it.

Two things here are easy to get subtly wrong and are guarded rather than assumed:

- Multi-key sort must be lexicographic with a PER-KEY direction. One composite
  key cannot do that — reversing it reverses every field at once — so the keys
  are applied last-to-first over a stable sort, and a test pins the result rather
  than the technique.
- The filter must match the row a reader can SEE. The nested map spells an
  address across three levels and never as one string, so filter and flat shape
  share one walk; two walks would drift and `--filter address=...` would answer
  about something the table never showed.
"""

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.run import addressing as run_addressing
from engine.state import render as state_render
from engine.state import status as state_status

# Two workflow templates and one target, so every axis under test varies: kind,
# group, status, freshness, time, member count and label.
MAP = {
    "workflow": {
        "env/seed": {
            "instances": {
                "env.type=dev": {
                    "mutative": {
                        "status": "passed",
                        "freshness": "undetermined",
                        "time": "2026-08-06T11:39Z",
                        "members": [1],
                    },
                    "non_mutative": {
                        "status": "failed",
                        "last_operation": "plan",
                        "actions": ["plan", "readonly"],
                        "standing": "superseded",
                        "superseded_by": "workflow/env/core/instances/env.type=dev",
                        "time": "2026-08-07T09:00Z",
                        "members": [1, 2, 3],
                    },
                }
            }
        },
        "env/core": {
            "instances": {
                "env.type=dev": {
                    "mutative": {
                        "status": "failed",
                        "last_operation": "provision",
                        "freshness": "outdated",
                        "time": "2026-08-05T10:00Z",
                        "members": [1, 2],
                        "label": "release-2026.07",
                    }
                }
            }
        },
    },
    "target": {
        "env/seed/baseline": {
            "instances": {
                "env.type=dev": {
                    "mutative": {
                        "status": "passed",
                        "last_action": "provision",
                        "freshness": "up_to_date",
                        "time": "2026-08-06T11:40Z",
                    }
                }
            }
        }
    },
}


def _flat(instances: dict, sort: str = "address") -> list[dict]:
    return state_status.structure_status_map(instances, "flat", sort)["instances"]


def _filtered(*pairs: str) -> dict:
    return state_status.filter_status_map(MAP, state_status.parse_filters(list(pairs)))


class OneVocabularyTest(unittest.TestCase):
    def test_the_table_columns_are_the_sort_fields(self):
        """A column you cannot sort by, or a sort field with no column to read
        the result in, would each be a defect with nothing to say which was
        intended. They are one tuple, so neither can happen."""

        self.assertEqual(state_render.FLAT_COLUMNS, state_status.SORT_FIELDS)

    def test_filtering_adds_kind_and_nothing_else(self):
        """`kind` is filterable without being a column: the flat shape carries it
        as the first segment of the address, so printing it would repeat what a
        reader can already see — but "only the workflows" is a real question."""

        self.assertEqual(
            state_status.FILTER_FIELDS, ("kind", *state_status.SORT_FIELDS)
        )

    def test_yaml_only_relation_detail_survives_report_structuring(self):
        row = next(item for item in _flat(MAP) if item.get("standing") == "superseded")
        self.assertEqual(
            "workflow/env/core/instances/env.type=dev", row["superseded_by"]
        )
        self.assertNotIn("superseded_by", state_status.SORT_FIELDS)

    def test_the_time_axis_and_the_time_sort_field_are_one_word(self):
        """They were `at` and `time`: one fact, two names, and `--sort at` reads
        as an unfinished sentence."""

        self.assertIn("time", run_addressing.AXIS_ORDER)
        self.assertNotIn("at", run_addressing.AXIS_ORDER)
        self.assertEqual(state_status.SortField.TIME, "time")


class SortKeysApplyInOrderTest(unittest.TestCase):
    def test_a_later_key_breaks_the_ties_an_earlier_one_leaves(self):
        rows = _flat(MAP, "status,time")
        self.assertEqual(
            [(row["status"], row["time"]) for row in rows],
            [
                ("failed", "2026-08-05T10:00Z"),
                ("failed", "2026-08-07T09:00Z"),
                ("passed", "2026-08-06T11:39Z"),
                ("passed", "2026-08-06T11:40Z"),
            ],
        )

    def test_each_key_carries_its_own_direction(self):
        """The reason one composite key cannot serve: reversing it would reverse
        every field at once, and `members:desc,time` means biggest first, oldest
        first — two directions in one order."""

        rows = _flat(MAP, "members:desc,time")
        self.assertEqual(
            [(state_status.sort_value(row, "members"), row["time"]) for row in rows],
            [
                (3, "2026-08-07T09:00Z"),
                (2, "2026-08-05T10:00Z"),
                (1, "2026-08-06T11:39Z"),
                (0, "2026-08-06T11:40Z"),
            ],
        )

    def test_members_orders_by_count_and_not_by_text(self):
        """Ordering the printed text would put 10 before 9."""

        many = {"members": list(range(10))}
        self.assertGreater(
            state_status.sort_value(many, "members"),
            state_status.sort_value({"members": [1, 2, 3]}, "members"),
        )

    def test_rows_agreeing_on_every_key_still_have_one_order(self):
        """An order that varies between two reads of the same namespace cannot be
        used to compare them, which is most of what a status table is for."""

        rows = _flat(MAP, "status")
        self.assertEqual(rows, _flat(MAP, "status"))
        failed = [row["address"] for row in rows if row["status"] == "failed"]
        self.assertEqual(failed, sorted(failed))

    def test_an_absent_value_does_not_scatter_its_rows(self):
        """Only one row is labelled; sorting by label must not interleave the
        rest around it."""

        labels = [row.get("label", "") for row in _flat(MAP, "label")]
        self.assertEqual(labels, ["", "", "", "release-2026.07"])


class SortRefusalsTest(unittest.TestCase):
    def test_an_unknown_field_is_refused(self):
        with self.assertRaises(RuntimeError):
            state_status.parse_sort("cost")

    def test_a_bad_direction_is_refused(self):
        with self.assertRaises(RuntimeError):
            state_status.parse_sort("time:newest")

    def test_naming_one_field_twice_is_refused(self):
        """It cannot break its own ties, so the second key is dead — and the
        spelling that hides it is `time:asc,time:desc`, which looks deliberate."""

        for raw in ("time,time", "time:asc,time:desc"):
            with self.subTest(raw=raw), self.assertRaises(RuntimeError):
                state_status.parse_sort(raw)

    def test_an_empty_sort_is_refused(self):
        with self.assertRaises(RuntimeError):
            state_status.parse_sort("  ")

    def test_a_nested_map_takes_only_the_fields_that_aggregate(self):
        """A template holds many rows. Its name is its own and its newest row is
        the one a reader wants; "the worst status under it" is not a question,
        and answering it deterministically would look considered."""

        for field in ("address", "time"):
            with self.subTest(field=field):
                state_status.parse_sort(field, structure="nested")
        for field in ("status", "members", "label", "group"):
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                state_status.parse_sort(field, structure="nested")


class FilterNarrowsTest(unittest.TestCase):
    def test_values_of_one_field_are_alternatives(self):
        rows = _flat(_filtered("group=non_mutative", "group=mutative"))
        self.assertEqual(len(rows), 4)

    def test_different_fields_all_have_to_hold(self):
        rows = _flat(_filtered("kind=workflow", "group=mutative"))
        self.assertEqual(
            [row["address"] for row in rows],
            [
                "workflow/env/core/instances/env.type=dev",
                "workflow/env/seed/instances/env.type=dev",
            ],
        )

    def test_adding_a_filter_never_widens_the_result(self):
        """The reason a repeated field collects rather than overwrites: a second
        `group=` that replaced the first would silently widen it back."""

        narrowed = _flat(_filtered("kind=workflow"))
        narrower = _flat(_filtered("kind=workflow", "status=failed"))
        self.assertLess(len(narrower), len(narrowed))
        self.assertTrue({row["address"] for row in narrower} <= {row["address"] for row in narrowed})

    def test_every_row_field_can_be_filtered_on(self):
        for pair, expected in (
            ("kind=target", 1),
            ("address=workflow/env/seed/instances/env.type=dev", 2),
            ("group=non_mutative", 1),
            ("status=failed", 2),
            ("last_action=provision", 1),
            ("last_operation=provision", 1),
            ("actions=plan+readonly", 1),
            ("standing=superseded", 1),
            ("freshness=outdated", 1),
            ("time=2026-08-06T11:40Z", 1),
            ("members=3", 1),
            ("label=release-2026.07", 1),
        ):
            with self.subTest(pair=pair):
                self.assertEqual(len(_flat(_filtered(pair))), expected)

    def test_a_trailing_star_matches_a_prefix_on_any_text_field(self):
        for pair, expected in (
            ("address=workflow/env/*", 3),
            ("address=target/env/*", 1),
            ("status=pass*", 2),
            ("label=release-*", 1),
        ):
            with self.subTest(pair=pair):
                self.assertEqual(len(_flat(_filtered(pair))), expected)

    def test_the_filter_sees_the_row_the_flat_shape_shows(self):
        """The nested map spells an address across three levels and never as one
        string. Filter and flat shape share one walk, so what you filter on is
        what the table prints."""

        address = "target/env/seed/baseline/instances/env.type=dev"
        rows = _flat(_filtered(f"address={address}"))
        self.assertEqual([row["address"] for row in rows], [address])

    def test_an_empty_filter_is_the_whole_map(self):
        self.assertEqual(MAP, state_status.filter_status_map(MAP, {}))
        self.assertEqual(MAP, state_status.filter_status_map(MAP, None))

    def test_a_template_left_with_no_row_is_dropped(self):
        """An empty template would read as "nothing happened here", which is a
        different claim from "you asked not to see it"."""

        kept = _filtered("label=release-2026.07")
        self.assertEqual(list(kept), ["workflow"])
        self.assertEqual(list(kept["workflow"]), ["env/core"])

    def test_a_filter_matching_nothing_is_an_empty_map(self):
        self.assertEqual({}, _filtered("status=running"))

    def test_the_kept_map_keeps_the_layout_it_was_given(self):
        """It is handed straight to `structure_status_map`, which walks the
        instances marker — a filter that flattened the tree would make `nested`
        silently lose its instances."""

        kept = _filtered("kind=workflow", "group=mutative")
        body = kept["workflow"]["env/seed"]
        self.assertIn(run_addressing.INSTANCES_MARKER, body)
        self.assertEqual(
            list(body[run_addressing.INSTANCES_MARKER]["env.type=dev"]), ["mutative"]
        )


class FilterRefusalsTest(unittest.TestCase):
    def test_an_unknown_field_is_refused(self):
        with self.assertRaises(RuntimeError):
            state_status.parse_filters(["cost=17"])

    def test_yaml_only_relation_detail_is_not_a_query_field(self):
        with self.assertRaises(RuntimeError):
            state_status.parse_filters(["superseded_by=workflow/env/core"])
        with self.assertRaises(RuntimeError):
            state_status.parse_sort("superseded_by")

    def test_a_pair_without_a_value_is_refused(self):
        for item in ("group", "group=", "=mutative"):
            with self.subTest(item=item), self.assertRaises(RuntimeError):
                state_status.parse_filters([item])

    def test_a_wildcard_is_only_a_single_trailing_prefix_marker(self):
        for value in ("*", "*failed", "fail*ed", "fail**"):
            with self.subTest(value=value), self.assertRaisesRegex(
                RuntimeError, "one trailing"
            ):
                state_status.parse_filters([f"status={value}"])

    def test_the_echoed_filters_parse_back(self):
        """The report prints its filters so a reader can copy the line into the
        next command; `field=a,b` would not parse, because a comma separates
        PAIRS."""

        filters = state_status.parse_filters(
            ["group=non_mutative", "group=mutative", "kind=workflow"]
        )
        echoed = state_render._field_text(filters)
        self.assertEqual(filters, state_status.parse_filters(echoed.split()))


class SortAndFilterComposeTest(unittest.TestCase):
    def test_filtering_then_sorting_is_the_sorted_subset(self):
        """They are independent: narrowing must not reorder, and ordering must
        not drop."""

        everything = _flat(MAP, "time:desc")
        subset = _flat(_filtered("kind=workflow"), "time:desc")
        self.assertEqual(
            subset,
            [row for row in everything if row["address"].startswith("workflow/")],
        )

    def test_a_nested_read_can_be_filtered_by_any_field(self):
        """Only SORTING is narrowed by the nested shape — a filter tests one row
        at a time, so it works the same in both."""

        kept = state_status.filter_status_map(
            MAP, state_status.parse_filters(["status=failed"])
        )
        nested = state_status.structure_status_map(kept, "nested", "time:desc")
        self.assertEqual(list(nested), ["workflow"])
        self.assertEqual(list(nested["workflow"]), ["env/seed", "env/core"])


if __name__ == "__main__":
    unittest.main()
