"""A composed workflow may PLACE a member, not only append.

`import_workflows` resolves imports in order and `targets` appends after them, so
a composition whose extra member belongs BETWEEN two imported ones cannot express
that by writing it — it does not author that list. `insert_targets` names the
position instead.

Two fields rather than one: in `targets`, list position IS the position, so an
anchor there would be a second way to say the same thing and the two could
disagree. `targets` appends and forbids an anchor; `insert_targets` requires one.

The anchor is a `{workflow, key}` mapping, never a bare key. With two imports a
bare key would not say which sequence it points into, and a slash-joined path is
unsplittable because target keys contain `/` themselves.

This is a splice index, not a DAG: the result is one linear order, exactly what
typing the entry at that position would produce.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.catalog import target_catalog  # noqa: E402
from engine.catalog import workflow as catalog_workflow  # noqa: E402

BASE = {"default_action": "provision", "targets": ["env/ops/ecr", "env/ops/dbs", "env/ops/app"]}


def _composed(insert_keys, *, imports=("base",), targets=()):
    return {
        "base": BASE,
        "other": {"default_action": "provision", "targets": ["other/one"]},
        "w": {
            "default_action": "provision",
            "import_workflows": list(imports),
            "targets": list(targets),
            "insert_targets": {"default_action": "provision", "keys": insert_keys},
        },
    }


def _expand(workflows, name="w"):
    return [
        run["target"] if isinstance(run, dict) else run
        for run in catalog_workflow.WorkflowImports.expand(workflows, name)
    ]


class PlacementTest(unittest.TestCase):
    def test_an_entry_lands_after_its_anchor(self):
        self.assertEqual(
            ["env/ops/ecr", "env/ops/dbs", "env/ops/populate", "env/ops/app"],
            _expand(
                _composed(
                    [
                        {
                            "key": "env/ops/populate",
                            "after": {"workflow": "base", "key": "env/ops/dbs"},
                        }
                    ]
                )
            ),
        )

    def test_an_entry_lands_before_its_anchor(self):
        """`before` is what `after` cannot express: insertion at the head."""

        self.assertEqual(
            ["env/ops/first", "env/ops/ecr", "env/ops/dbs", "env/ops/app"],
            _expand(
                _composed(
                    [{"key": "env/ops/first", "before": {"workflow": "base", "key": "env/ops/ecr"}}]
                )
            ),
        )

    def test_an_unanchored_member_still_appends(self):
        """`targets` keeps its meaning: imports first, then the workflow's own."""

        self.assertEqual(
            ["env/ops/ecr", "env/ops/dbs", "env/ops/app", "env/ops/smoke"],
            _expand(_composed([], targets=["env/ops/smoke"])),
        )

    def test_placed_and_appended_members_coexist(self):
        self.assertEqual(
            ["env/ops/ecr", "env/ops/dbs", "env/ops/populate", "env/ops/app", "env/ops/smoke"],
            _expand(
                _composed(
                    [
                        {
                            "key": "env/ops/populate",
                            "after": {"workflow": "base", "key": "env/ops/dbs"},
                        }
                    ],
                    targets=["env/ops/smoke"],
                )
            ),
        )

    def test_two_entries_at_one_anchor_keep_declaration_order(self):
        self.assertEqual(
            ["env/ops/ecr", "env/ops/dbs", "env/ops/one", "env/ops/two", "env/ops/app"],
            _expand(
                _composed(
                    [
                        {"key": "env/ops/one", "after": {"workflow": "base", "key": "env/ops/dbs"}},
                        {"key": "env/ops/two", "after": {"workflow": "base", "key": "env/ops/dbs"}},
                    ]
                )
            ),
        )

    def test_the_anchor_selects_among_several_imports(self):
        """What the qualified anchor is FOR: with two imports, a bare key would
        not say which sequence the position is relative to."""

        self.assertEqual(
            ["env/ops/ecr", "env/ops/dbs", "env/ops/app", "other/one", "env/ops/placed"],
            _expand(
                _composed(
                    [{"key": "env/ops/placed", "after": {"workflow": "other", "key": "other/one"}}],
                    imports=("base", "other"),
                )
            ),
        )

    def test_a_placed_entry_may_carry_its_own_action(self):
        workflows = _composed(
            [
                {
                    "key": "env/ops/wipe",
                    "action": "destroy",
                    "after": {"workflow": "base", "key": "env/ops/dbs"},
                }
            ]
        )
        runs = catalog_workflow.WorkflowImports.expand(workflows, "w")
        placed = next(r for r in runs if r["target"] == "env/ops/wipe")
        self.assertEqual("destroy", placed["action"])


class AnchorGuardsTest(unittest.TestCase):
    def test_an_anchor_naming_a_workflow_that_is_not_imported_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "does not import"):
            _expand(_composed([{"key": "x/y", "after": {"workflow": "other", "key": "other/one"}}]))

    def test_an_anchor_key_the_named_import_does_not_run_is_refused(self):
        """Scoped to THAT import: a key present elsewhere in the merged sequence
        must not satisfy an anchor pointed at `base`."""

        with self.assertRaisesRegex(RuntimeError, "does not run"):
            _expand(
                _composed(
                    [{"key": "x/y", "after": {"workflow": "base", "key": "other/one"}}],
                    imports=("base", "other"),
                )
            )

    def test_an_ambiguous_anchor_is_refused_rather_than_resolved(self):
        """A repeated key gives the anchor two positions. Picking one silently
        would place a target somewhere the author did not ask for."""

        workflows = _composed([{"key": "x/y", "after": {"workflow": "base", "key": "env/ops/dbs"}}])
        workflows["base"] = {
            "default_action": "provision",
            "targets": [
                {"key": "env/ops/dbs", "action": "destroy"},
                {"key": "env/ops/dbs", "action": "provision"},
            ],
        }
        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            _expand(workflows)

    def test_declaring_both_after_and_before_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            target_catalog.TargetEntries.normalize_inserts(
                [
                    {
                        "key": "x/y",
                        "after": {"workflow": "b", "key": "k"},
                        "before": {"workflow": "b", "key": "k"},
                    }
                ],
                label="wf",
                default_action="provision",
            )

    def test_an_entry_with_no_anchor_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "no anchor"):
            target_catalog.TargetEntries.normalize_inserts(
                [{"key": "x/y"}], label="wf", default_action="provision"
            )

    def test_a_bare_key_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "declares no anchor"):
            target_catalog.TargetEntries.normalize_inserts(
                ["x/y"], label="wf", default_action="provision"
            )

    def test_an_unqualified_anchor_is_refused(self):
        """A bare key cannot say which imported sequence it addresses."""

        with self.assertRaisesRegex(RuntimeError, "must be"):
            target_catalog.TargetEntries.normalize_inserts(
                [{"key": "x/y", "after": "env/ops/dbs"}],
                label="wf",
                default_action="provision",
            )

    def test_an_anchor_inside_targets_is_refused(self):
        """`targets` position IS the position, so an anchor there is a second,
        contradictory way to say it."""

        with self.assertRaisesRegex(RuntimeError, "unsupported keys"):
            target_catalog.TargetEntries.normalize(
                [{"key": "x/y", "after": {"workflow": "b", "key": "k"}}],
                label="wf",
                default_action="provision",
            )


class DeclaredShapeTest(unittest.TestCase):
    """`insert_targets` branches on a selector exactly as `targets` does."""

    DECLARED = {
        "insert_targets": {
            "members": [
                {
                    "default_action": "provision",
                    "keys": [
                        {
                            "key": "env/ops/populate",
                            "after": {"workflow": "base", "key": "env/ops/dbs"},
                        }
                    ],
                    "selectors": {"match": {"execution_context.params.operation": "provision"}},
                },
            ]
        }
    }

    def test_the_matching_branch_resolves(self):
        resolved = catalog_workflow.WorkflowImports.resolve_inserts(
            self.DECLARED, {"execution_context.params.operation": "provision"}, name="w"
        )
        self.assertEqual(["env/ops/populate"], [e["key"] for e in resolved["keys"]])

    def test_an_operation_with_no_branch_places_nothing(self):
        """Not an error: a composition that adds a member to provision only is
        the normal case."""

        self.assertEqual(
            {},
            catalog_workflow.WorkflowImports.resolve_inserts(
                self.DECLARED, {"execution_context.params.operation": "destroy"}, name="w"
            ),
        )

    def test_a_plain_list_declaration_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "must be a mapping"):
            catalog_workflow.WorkflowImports.resolve_inserts(
                {"insert_targets": [{"key": "a/b"}]}, {}, name="w"
            )


class TheRealCompositionTest(unittest.TestCase):
    """`env/baseline_with_artificial_data` runs `env/baseline` with the populator
    placed after the databases it populates, rather than restating every member."""

    def _workflows(self):
        import yaml

        root = Path(__file__).resolve().parents[3] / "cfg/oxygen/oxygen-ctl-cfg/workflows"
        workflows: dict = {}
        for path in sorted(root.rglob("*.yaml")):
            workflows.update((yaml.safe_load(path.read_text()) or {}).get("workflows") or {})
        # cfg states each reference's collection; the loader resolves it, and a
        # test reading cfg directly has to do the same or it reads paths as keys.
        from engine.cfg import references as cfg_references

        resolved = {}
        for name, entry in workflows.items():
            body = dict(entry)
            if body.get("import_workflows") is not None:
                body["import_workflows"] = cfg_references.resolve_each(
                    body["import_workflows"], "workflows", label=name
                )
            for field in ("targets", "insert_targets"):
                if field in body:
                    body[field] = catalog_workflow._resolve_member_block(body[field], label=name)
            resolved[name] = body
        return resolved

    def _resolved(self, workflows, operation):
        from engine.run import selectors as run_selectors

        context = {
            "execution_context.params.operation": operation,
            "execution_context.ctl.action": operation,
        }
        resolved = {}
        for name, wf in workflows.items():
            targets = wf.get("targets") or {}
            if "members" in targets:
                member = run_selectors.resolve_list_member(
                    targets,
                    context,
                    value_field="keys",
                    label=name,
                    extra_fields=("default_action",),
                )
                keys = list(member["keys"]) if member else []
                declared_default = (member or {}).get("default_action")
            else:
                keys = list(targets.get("keys") or [])
                declared_default = targets.get("default_action")
            resolved[name] = {
                **wf,
                "targets": keys,
                "default_action": run_selectors.resolve_default_action(
                    declared_default, context, label=name
                ),
                "insert_targets": catalog_workflow.WorkflowImports.resolve_inserts(
                    wf, context, name=name
                ),
            }
        return resolved

    def test_the_composition_expands_to_baseline_plus_the_populator(self):
        workflows = self._workflows()
        for operation in ("plan", "provision", "destroy"):
            with self.subTest(operation=operation):
                resolved = self._resolved(workflows, operation)
                baseline = _expand(resolved, "env/baseline")
                composed = _expand(resolved, "env/baseline_with_artificial_data")
                if operation == "provision":
                    index = baseline.index("env/database_setup") + 1
                    self.assertEqual(
                        baseline[:index] + ["env/artificial_data"] + baseline[index:],
                        composed,
                    )
                else:
                    self.assertEqual(baseline, composed)


if __name__ == "__main__":
    unittest.main()
