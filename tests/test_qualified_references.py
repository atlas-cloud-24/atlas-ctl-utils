"""A ctl cfg cross-reference states the COLLECTION it points into.

A bare key leaves the collection implied by the field NAME, which holds only
where the two match. In this cfg they often do not — `input_param_sets` resolves
against `param_sets`, `providers` against `execution_providers` — so the value carries
the path and a path naming the wrong collection is refused. That refusal is the
whole point: without it the extra text is a comment.

WHERE THE FACT LIVES is the design. `cfg/references.py` holds NO collection
names; the caller passes the collection it is about to look the value up in. The
ctl-model fields are declared by the catalog that owns them, and the fields
inside `execution_identities` are resolved by the provider ADAPTER — engine core
may not spell `providers.<provider>.target_roles`, and a test enforces that.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.catalog import target_catalog  # noqa: E402
from engine.cfg import references as cfg_references  # noqa: E402


class ResolveTest(unittest.TestCase):
    def test_a_qualified_value_yields_its_key(self):
        self.assertEqual(
            "core",
            cfg_references.resolve("target_sources.core", "target_sources", label="t"),
        )

    def test_a_key_containing_slashes_survives_whole(self):
        """`/` stays inside a key and `.` navigates, so the prefix length decides
        the split — a target key is not carved up by its own separators."""

        self.assertEqual(
            "env/core/baseline",
            cfg_references.resolve("targets.env/core/baseline", "targets", label="t"),
        )

    def test_a_key_containing_an_interpolation_survives_whole(self):
        self.assertEqual(
            "${execution_context.params.domain}",
            cfg_references.resolve(
                "domains.${execution_context.params.domain}", "domains", label="t"
            ),
        )

    def test_a_bare_key_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "does not name its collection"):
            cfg_references.resolve("core", "target_sources", label="t")

    def test_the_WRONG_collection_is_refused(self):
        """The check the qualification exists for. A path that resolves to a real
        key of another collection is still wrong here, and nothing else would
        notice: the reader would look the stripped key up in its own collection
        and find it."""

        with self.assertRaisesRegex(RuntimeError, "expected target_sources"):
            cfg_references.resolve("execution_providers.core", "target_sources", label="t")

    def test_a_wildcard_segment_is_supplied_by_the_declaration(self):
        """One call covers every provider, so the adapter needs one rule per
        field rather than one per provider."""

        self.assertEqual(
            "ctl_target_readonly",
            cfg_references.resolve(
                "providers.aws.target_roles.ctl_target_readonly",
                "providers.*.target_roles",
                label="t",
            ),
        )

    def test_a_wildcard_does_not_match_a_different_shape(self):
        with self.assertRaisesRegex(RuntimeError, "does not name its collection"):
            cfg_references.resolve(
                "providers.aws.ctl_state_roles.reader", "providers.*.target_roles", label="t"
            )

    def test_a_non_string_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "expected a targets reference"):
            cfg_references.resolve(None, "targets", label="t")


class ResolveEachTest(unittest.TestCase):
    def test_a_list_resolves_elementwise(self):
        self.assertEqual(
            ["env", "org"],
            cfg_references.resolve_each(["domains.env", "domains.org"], "domains", label="t"),
        )

    def test_a_mapping_resolves_its_VALUES_and_keeps_its_keys(self):
        """`roles: {readwrite: <ref>}` names a role class on the left, which is an
        index into the provider's vocabulary rather than a reference."""

        self.assertEqual(
            {"readonly": "a", "readwrite": "b"},
            cfg_references.resolve_each(
                {
                    "readonly": "providers.aws.target_roles.a",
                    "readwrite": "providers.aws.target_roles.b",
                },
                "providers.*.target_roles",
                label="t",
            ),
        )

    def test_resolve_fields_leaves_undeclared_fields_alone(self):
        entry = {"source_key": "target_sources.core", "procedure_key": "baseline"}
        self.assertEqual(
            {"source_key": "core", "procedure_key": "baseline"},
            cfg_references.resolve_fields(entry, {"source_key": "target_sources"}, label="t"),
        )


class TheMechanismNamesNoCollectionTest(unittest.TestCase):
    """The design constraint, asserted rather than trusted.

    A central table of field -> collection inside the mechanism is what puts cfg
    shape into engine core; the caller passing the collection is what keeps it
    out. If a table ever appears here, this fails.
    """

    def test_the_module_spells_no_ctl_collection(self):
        import ast

        source = (
            Path(__file__).resolve().parents[1] / "runners/engine/cfg/references.py"
        ).read_text()
        # Prose NAMES collections while explaining the rule, so docstrings and
        # comments are dropped and only executable code is scanned. Parsing and
        # re-emitting drops both, without a regex guessing where they end.
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            ) and ast.get_docstring(node):
                node.body = node.body[1:]
        code = ast.unparse(tree)
        for collection in (
            "target_sources",
            "param_sets",
            "execution_providers",
            "accounts_registry",
            "target_roles",
            "credential_sources",
        ):
            self.assertNotIn(collection, code, f"{collection} must be passed in, not held here")

    def test_the_scan_would_see_a_table_if_one_appeared(self):
        """Guards the check above: an empty scan would pass for the wrong reason."""

        import ast

        tree = ast.parse('FIELDS = {"source_key": "target_sources"}')
        self.assertIn("target_sources", ast.unparse(tree))


class TheCatalogOwnsItsVocabularyTest(unittest.TestCase):
    def test_the_target_map_covers_the_ctl_model_fields_only(self):
        """`execution_identities` is absent BY DESIGN: everything inside it names
        provider collections, which engine core may not spell."""

        self.assertEqual(
            {
                "source_key": "target_sources",
                "domains": "domains",
                "input_param_sets": "param_sets",
                "providers": "execution_providers",
            },
            target_catalog.TARGET_REFERENCE_FIELDS,
        )


if __name__ == "__main__":
    unittest.main()
