"""§Phase 83 Q2: the action registry is the ONE place an action is described.

Actions stay engine-owned — every engine branch on an action name asks one of the
registry's facts, and the interesting one (provision and destroy are two
directions of one state) is the engine's state model, not a consumer value.

What this pins is that they are described ONCE. Before the registry the same five
names were written out eight times: two verbatim-duplicate tuples, a group map, a
group->actions inverse, a group->representative inverse, and three argparse lists
in a repository that cannot import any of them.
"""
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))
from utils import common  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_RUNTIME = (
    REPO_ROOT.parent / "atlas-ctl-orchestrator" / "runners" / "bootstrap" / "runtime.py"
)


class RegistryStatesTheFactsTest(unittest.TestCase):
    """What each action IS — claims about the domain, not restatements of the code.

    The first version of this class compared `MUTATING_ACTIONS` against a
    comprehension over `ACTIONS`, and both sides derive from `ACTIONS`. Deleting
    `mutating: True` from `destroy` changed both together and the test passed —
    a tautology, caught by mutation and replaced with the claim itself.
    """

    def test_provision_and_destroy_are_the_mutating_actions(self):
        """The set the mutation lock, the mutation mark and the destroy guard all
        turn on. Nothing else may enter it silently."""
        self.assertEqual({"provision", "destroy"}, set(common.MUTATING_ACTIONS))

    def test_plan_and_readonly_and_maintenance_do_not_mutate(self):
        for action in ("plan", "readonly", "maintenance"):
            with self.subTest(action=action):
                self.assertNotIn(action, common.MUTATING_ACTIONS)

    def test_provision_and_destroy_share_one_group(self):
        """Two directions of ONE state: a destroy is the instance ending, not a
        separate thing that happened to it."""
        self.assertEqual(
            common.GROUP_BY_ACTION["provision"], common.GROUP_BY_ACTION["destroy"]
        )

    def test_every_other_action_has_a_group_of_its_own(self):
        for action in ("plan", "readonly", "maintenance"):
            with self.subTest(action=action):
                others = {
                    common.GROUP_BY_ACTION[name]
                    for name in common.ACTIONS if name != action
                }
                self.assertNotIn(common.GROUP_BY_ACTION[action], others)

    def test_the_two_name_tuples_are_one_tuple(self):
        """`RUN_ACTIONS` and `KNOWN_ACTIONS` were verbatim copies consulted by
        different validators, so one could gain an action the other refused."""
        self.assertIs(common.RUN_ACTIONS, common.KNOWN_ACTIONS)
        self.assertEqual(tuple(common.ACTIONS), common.RUN_ACTIONS)

    def test_the_group_inverse_is_derived_not_written(self):
        """`STATUS_GROUPS` was a hand-written inverse of `GROUP_BY_ACTION`, which
        is a second place for one fact to be wrong — and it WAS wrong: it omitted
        `maintenance`, so `--group maintenance` was refused for rows the status
        computation produces."""
        rebuilt = {}
        for action, group in common.GROUP_BY_ACTION.items():
            rebuilt.setdefault(group, []).append(action)
        self.assertEqual(
            {g: sorted(a) for g, a in rebuilt.items()},
            {g: sorted(a) for g, a in common.STATUS_GROUPS.items()},
        )
        self.assertIn("maintenance", common.STATUS_GROUPS)

    def test_every_group_has_exactly_one_representative(self):
        for group in common.RESULT_GROUPS:
            with self.subTest(group=group):
                representative = common.group_representative_action(group)
                self.assertIn(representative, common.STATUS_GROUPS[group])
                self.assertEqual(group, common.GROUP_BY_ACTION[representative])

    def test_a_paired_group_is_represented_by_its_forward_action(self):
        """The state's existence is what a status answers about; the reverse is
        that state ending."""
        self.assertEqual("provision", common.group_representative_action("mutative"))

    def test_a_group_with_two_forward_actions_is_refused(self):
        """Silently picking one would make the status of every instance in that
        group depend on dict order."""
        original = dict(common.ACTIONS["destroy"])
        common.ACTIONS["destroy"] = {**original, "direction": "forward"}
        try:
            with self.assertRaises(RuntimeError):
                common.group_representative_action("mutative")
        finally:
            common.ACTIONS["destroy"] = original


class ActionFactsTest(unittest.TestCase):
    def test_only_the_forward_side_of_a_pair_can_go_stale(self):
        """A destroy record describes an instance that is gone; nothing can make
        that answer stale."""
        self.assertTrue(common.action_can_go_stale("provision"))
        for action in ("destroy", "plan", "readonly", "maintenance"):
            with self.subTest(action=action):
                self.assertFalse(common.action_can_go_stale(action))

    def test_an_unknown_action_can_not_go_stale(self):
        self.assertFalse(common.action_can_go_stale("nonsense"))

    def test_a_preview_resolves_the_action_it_previews(self):
        self.assertEqual("provision", common.action_previewed_by("plan"))

    def test_an_action_that_previews_nothing_answers_itself(self):
        for action in ("provision", "destroy", "readonly", "maintenance"):
            with self.subTest(action=action):
                self.assertEqual(action, common.action_previewed_by(action))


class BootstrapParserMirrorsTheRegistryTest(unittest.TestCase):
    """The bootstrap parser CANNOT import the registry — it exists so `--help`
    works before atlas-ctl-utils is fetched. That is the
    `Cross-Boundary Shared Value` case exactly: one value, two formulas, and the
    rule is that both are guarded against one baseline. This is that guard.
    """

    def _literal_action_lists(self) -> list[list[str]]:
        body = BOOTSTRAP_RUNTIME.read_text()
        return [
            re.findall(r'"([a-z-]+)"', match)
            for match in re.findall(r'choices=\[((?:\s*"[a-z-]+",?\s*)+)\]', body)
            if "provision" in match
        ]

    def test_the_lists_are_found(self):
        """Guards the test: a changed spelling would silently find nothing."""
        self.assertTrue(
            self._literal_action_lists(),
            f"no action choices= list found in {BOOTSTRAP_RUNTIME}",
        )

    def test_every_bootstrap_action_list_matches_the_registry(self):
        expected = list(common.ACTIONS)
        for found in self._literal_action_lists():
            self.assertEqual(
                expected, found,
                "the bootstrap parser accepts a different action set than the "
                "engine, so --help and the run disagree about what is valid",
            )


if __name__ == "__main__":
    unittest.main()
