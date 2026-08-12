"""Preset composition — imports, params, and the assertions.

Each test names the assertion it pins, so a future edit that drops one fails
here rather than silently widening what a preset may do.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cfg import validate as cfg_validate
from engine.cfg import presets as cfg_presets


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class PresetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="atlas-preset-test-")
        self.root = Path(self._tmp.name) / "cfg"
        self.root.mkdir(parents=True)
        self.dest = Path(self._tmp.name) / "out"
        self.addCleanup(self._tmp.cleanup)

    def materialize(self, preset: str, **kwargs) -> dict:
        import yaml

        cfg_presets.materialize(self.root, preset, dest=self.dest, **kwargs)
        merged: dict = {}
        for path in sorted(self.dest.rglob("*.yaml")):
            doc = yaml.safe_load(path.read_text()) or {}
            merged.update(doc)
        return merged


class ImportTest(PresetTestCase):
    def test_import_selects_keys_not_files(self):
        """O1: `import` names cfg keys; a preset's file split is its own business."""
        write(self.root / "base" / "a.yaml", "kept: 1\ndropped: 2\n")
        write(self.root / "base" / "nested" / "b.yaml", "kept_too: 3\ndropped_too: 4\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            "imports:\n  - from: /base\n    import: [kept, kept_too]\n",
        )
        merged = self.materialize("/consumer")
        self.assertEqual(merged, {"kept": 1, "kept_too": 3})

    def test_import_keeps_relative_file_paths(self):
        """A key spread over many files stays spread: later stages merge in path order."""
        write(self.root / "base" / "one.yaml", "shared:\n  a: 1\n")
        write(self.root / "base" / "two.yaml", "shared:\n  b: 2\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        cfg_presets.materialize(self.root, "/consumer", dest=self.dest)
        produced = sorted(p.name for p in self.dest.rglob("*.yaml"))
        self.assertEqual(produced, ["one.yaml", "two.yaml"])

    def test_alias_nests_imported_keys(self):
        write(self.root / "base" / "a.yaml", "key: value\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n    as: grouped\n',
        )
        self.assertEqual(self.materialize("/consumer"), {"grouped": {"key": "value"}})

    def test_own_files_win_over_imported(self):
        write(self.root / "base" / "a.yaml", "key: imported\n")
        write(self.root / "consumer" / "a.yaml", "key: own\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        self.assertEqual(self.materialize("/consumer"), {"key": "own"})

    def test_nested_import_is_depth_first(self):
        """D10: a preset may import a preset."""
        write(self.root / "leaf" / "a.yaml", "leaf_key: leaf\n")
        write(
            self.root / "middle" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /leaf\n    import: "*"\n',
        )
        write(self.root / "middle" / "b.yaml", "middle_key: middle\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /middle\n    import: "*"\n',
        )
        self.assertEqual(
            self.materialize("/consumer"), {"leaf_key": "leaf", "middle_key": "middle"}
        )

    def test_cycle_is_an_error(self):
        """A2."""

        for name, other in (("a", "b"), ("b", "a")):
            write(
                self.root / name / cfg_presets.IMPORTS_FILENAME,
                f'imports:\n  - from: /{other}\n    import: "*"\n',
            )
            write(self.root / name / "v.yaml", f"{name}: 1\n")
        with self.assertRaisesRegex(cfg_presets.PresetError, "cfg import cycle"):
            self.materialize("/a")

    def test_duplicate_source_is_an_error(self):
        """A9 / D15: a preset is imported once, never instantiated twice."""
        write(self.root / "base" / "a.yaml", "key: 1\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            "imports:\n"
            '  - from: /base\n    import: "*"\n    as: one\n'
            '  - from: /base\n    import: "*"\n    as: two\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, "duplicate import from"):
            self.materialize("/consumer")

    def test_unknown_import_field_is_an_error(self):
        write(self.root / "base" / "a.yaml", "key: 1\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n    unless: nope\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, "unknown import field"):
            self.materialize("/consumer")

    def test_selecting_a_key_the_preset_does_not_produce_is_an_error(self):
        write(self.root / "base" / "a.yaml", "present: 1\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            "imports:\n  - from: /base\n    import: [absent]\n",
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, "is not produced by"):
            self.materialize("/consumer")


class ParamTest(PresetTestCase):
    def _base_with_param(self, body: str = "value: ${var.flavour}\n") -> None:
        write(self.root / "base" / cfg_presets.PARAMS_FILENAME, "params: [flavour]\n")
        write(self.root / "base" / "a.yaml", body)

    def test_param_is_bound_at_the_import_site(self):
        self._base_with_param()
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n    with:\n      flavour: spicy\n',
        )
        self.assertEqual(self.materialize("/consumer"), {"value": "spicy"})

    def test_missing_param_is_an_error(self):
        """A4."""
        self._base_with_param()
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, r"requires param\(s\) \['flavour'\]"):
            self.materialize("/consumer")

    def test_undeclared_binding_is_an_error(self):
        """A3."""
        self._base_with_param()
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n'
            "    with:\n      flavour: spicy\n      extra: nope\n",
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, r"given param\(s\) \['extra'\]"):
            self.materialize("/consumer")

    def test_unused_param_is_an_error(self):
        """A5 / O3."""
        write(self.root / "base" / cfg_presets.PARAMS_FILENAME, "params: [unused]\n")
        write(self.root / "base" / "a.yaml", "value: fixed\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n    with:\n      unused: x\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, "never references"):
            self.materialize("/consumer")

    def test_params_with_defaults_are_rejected(self):
        """D2: params is a bare list of names."""
        write(
            self.root / "base" / cfg_presets.PARAMS_FILENAME,
            "params:\n  flavour:\n    default: mild\n",
        )
        write(self.root / "base" / "a.yaml", "value: ${var.flavour}\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, "plain list of names"):
            self.materialize("/consumer")

    def test_bound_value_may_carry_a_reference(self):
        """O2: `with` takes derived values; the reference resolves later, whole-scope."""
        self._base_with_param()
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n'
            "    with:\n      flavour: ${main_tag}-spicy\n",
        )
        self.assertEqual(self.materialize("/consumer"), {"value": "${main_tag}-spicy"})

    def test_booleans_survive_binding(self):
        write(self.root / "base" / cfg_presets.PARAMS_FILENAME, "params: [enabled]\n")
        write(self.root / "base" / "a.yaml", "flag: ${var.enabled}\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n    with:\n      enabled: true\n',
        )
        self.assertEqual(self.materialize("/consumer"), {"flag": True})

    def test_params_thread_through_a_nested_import(self):
        """An intermediate preset forwards its own param inward."""
        write(self.root / "leaf" / cfg_presets.PARAMS_FILENAME, "params: [flavour]\n")
        write(self.root / "leaf" / "a.yaml", "value: ${var.flavour}\n")
        write(self.root / "middle" / cfg_presets.PARAMS_FILENAME, "params: [flavour]\n")
        write(
            self.root / "middle" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /leaf\n    import: "*"\n'
            "    with:\n      flavour: ${var.flavour}\n",
        )
        write(self.root / "middle" / "b.yaml", "echo: ${var.flavour}\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /middle\n    import: "*"\n    with:\n      flavour: sweet\n',
        )
        self.assertEqual(self.materialize("/consumer"), {"value": "sweet", "echo": "sweet"})

    def test_reserved_var_top_level_key_is_rejected(self):
        """A7."""
        write(self.root / "base" / "a.yaml", "var:\n  x: 1\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, "reserved top-level key"):
            self.materialize("/consumer")

    def test_declaration_files_are_never_payload(self):
        self._base_with_param()
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n    with:\n      flavour: spicy\n',
        )
        cfg_presets.materialize(self.root, "/consumer", dest=self.dest)
        produced = {p.name for p in self.dest.rglob("*")}
        self.assertNotIn(cfg_presets.PARAMS_FILENAME, produced)
        self.assertNotIn(cfg_presets.IMPORTS_FILENAME, produced)


class AliasTest(PresetTestCase):
    def test_alias_resolves_inside_the_preset(self):
        write(
            self.root / "base" / cfg_presets.ALIASES_FILENAME,
            "tag: ${execution_context.params.main_tag}\n",
        )
        write(self.root / "base" / "a.yaml", "role_name: ${tag}-runner\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        self.assertEqual(
            self.materialize("/consumer"),
            {"role_name": "${execution_context.params.main_tag}-runner"},
        )

    def test_alias_is_never_exported(self):
        """An alias is the preset's plumbing: importing it yields only what the
        preset is FOR."""
        write(self.root / "base" / cfg_presets.ALIASES_FILENAME, "tag: oxygen\n")
        write(self.root / "base" / "a.yaml", "boundary_name: ${tag}-boundary\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        merged = self.materialize("/consumer")
        self.assertEqual(merged, {"boundary_name": "oxygen-boundary"})
        self.assertNotIn("tag", merged)

    def test_alias_satisfies_the_self_containment_check(self):
        write(self.root / "base" / cfg_presets.ALIASES_FILENAME, "tag: oxygen\n")
        write(self.root / "base" / "a.yaml", "name: ${tag}-x\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        self.materialize("/consumer")  # must not raise


class SelfContainmentTest(PresetTestCase):
    def test_reaching_into_an_undefined_name_is_an_error(self):
        """A6: a name the preset defines nothing under is not ownership.

        The rule is exactly that — you must define SOMETHING under the name you
        reference — and no stronger. A top-level key IS a collection, so members
        filled by different layers is the co-definition case immediately below,
        and requiring sole ownership of a top-level namespace would reject it.
        """
        write(self.root / "base" / "a.yaml", "mine:\n  x: 1\n")
        write(self.root / "base" / "b.yaml", "value: ${theirs.y}\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, r"does not own.*theirs\.y"):
            self.materialize("/consumer")

    def test_filling_a_leaf_into_an_owned_collection_is_allowed(self):
        """A co-defined collection: the preset owns the collection, the importer
        fills member fields into it."""
        write(self.root / "base" / "a.yaml", "shared:\n  members:\n    one:\n      known: 1\n")
        write(self.root / "base" / "b.yaml", "value: ${shared.members.two.supplied}\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /base\n    import: "*"\n',
        )
        self.materialize("/consumer")  # must not raise


class CompositionTest(PresetTestCase):
    """A preset is configured ONCE per composition, however many paths reach it."""

    def _diamond(self, left_value: str, right_value: str) -> None:
        write(self.root / "leaf" / cfg_presets.PARAMS_FILENAME, "params: [flavour]\n")
        write(self.root / "leaf" / "a.yaml", "value: ${var.flavour}\n")
        for side, value in (("left", left_value), ("right", right_value)):
            write(
                self.root / side / cfg_presets.IMPORTS_FILENAME,
                f'imports:\n  - from: /leaf\n    import: "*"\n    with:\n      flavour: {value}\n',
            )
            write(self.root / side / f"{side}.yaml", f"{side}_key: 1\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /left\n    import: "*"\n  - from: /right\n    import: "*"\n',
        )

    def test_same_preset_same_bindings_is_fine(self):
        self._diamond("spicy", "spicy")
        merged = self.materialize("/consumer", composition={})
        self.assertEqual(merged["value"], "spicy")

    def test_same_preset_different_bindings_is_an_error(self):
        self._diamond("spicy", "mild")
        with self.assertRaisesRegex(cfg_presets.PresetError, "reached twice in one composition"):
            self.materialize("/consumer", composition={})

    def test_conflict_is_only_checked_within_one_composition(self):
        """Two scopes may configure one preset differently; they are separate
        compositions and never merge."""
        self._diamond("spicy", "mild")
        cfg_presets.materialize(self.root, "/left", dest=self.dest / "a", composition={})
        cfg_presets.materialize(self.root, "/right", dest=self.dest / "b", composition={})


class RedundantImportTest(PresetTestCase):
    """

    an import must state something the unit could not get without it."""

    def _chain(self) -> None:
        write(self.root / "leaf" / "a.yaml", "leaf_key:\n  value: 1\n")
        write(
            self.root / "middle" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /leaf\n    import: "*"\n',
        )
        write(self.root / "middle" / "b.yaml", "middle_key: ${leaf_key.value}\n")

    def test_import_also_reached_transitively_and_unused_is_an_error(self):
        self._chain()
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /leaf\n    import: "*"\n  - from: /middle\n    import: "*"\n',
        )
        with self.assertRaisesRegex(cfg_presets.PresetError, "already receives it through"):
            self.materialize("/consumer")

    def test_the_same_import_is_fine_when_the_unit_uses_it(self):
        """Declaring what you use is IWYU, not redundancy."""
        self._chain()
        write(self.root / "consumer" / "own.yaml", "mine: ${leaf_key.value}\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /leaf\n    import: "*"\n  - from: /middle\n    import: "*"\n',
        )
        self.materialize("/consumer")  # must not raise

    def test_unrelated_imports_are_fine(self):
        self._chain()
        write(self.root / "other" / "c.yaml", "other_key: 2\n")
        write(
            self.root / "consumer" / cfg_presets.IMPORTS_FILENAME,
            'imports:\n  - from: /other\n    import: "*"\n  - from: /middle\n    import: "*"\n',
        )
        self.materialize("/consumer")  # must not raise


class ReachabilityTest(unittest.TestCase):
    """

    payload outside every scope and preset is dead cfg, not an inert extra."""

    def _tree(self, extra: dict[str, str]) -> Path:
        tmp = tempfile.TemporaryDirectory(prefix="atlas-reach-test-")
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name) / "cfg"
        write(
            root / "env" / "dev" / "__meta__.yaml",
            "type: scope\ntarget_path: /env\nselectors:\n  match:\n"
            "    execution_context.params.env.type: dev\n",
        )
        write(root / "env" / "dev" / "value.yaml", "region: eu-west-2\n")
        for rel, body in extra.items():
            write(root / rel, body)
        return root

    def test_orphaned_payload_is_rejected(self):
        root = self._tree({"stray/forgotten.yaml": "key: value\n"})
        with self.assertRaisesRegex(RuntimeError, "outside every scope and preset"):
            cfg_validate.CfgTreeShape.require_all_payload_reachable(root.resolve())

    def test_payload_inside_a_scope_is_accepted(self):
        root = self._tree({"env/dev/nested/more.yaml": "key: value\n"})
        cfg_validate.CfgTreeShape.require_all_payload_reachable(root.resolve())


if __name__ == "__main__":
    unittest.main()
