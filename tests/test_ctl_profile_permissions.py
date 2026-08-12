"""A ctl profile permission is DECLARED, once.

The requirement this pins: adding a new ctl profile policy is one row. Before
`Permission` it was four separate edits — a bool accessor, a validator, a
refusal message that quoted the cfg key by hand, and the flag name written again
inside that message — so a renamed key left the refusal naming a field that no
longer existed, and nothing failed.
"""

import sys
import tempfile
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.run import policy as run_policy


def profile_root(tmp: str, **grants: bool) -> Path:
    root = Path(tmp)
    (root / "ctl_profiles.yaml").write_text(
        yaml.safe_dump(
            {
                "ctl_profiles": {
                    "p": {"description": "d", "ref_policy": "local_dirty_allowed", **grants}
                }
            }
        ),
        encoding="utf-8",
    )
    return root


def declared_permissions() -> list[run_policy.Permission]:
    return [
        value
        for name, value in vars(run_policy.Permissions).items()
        if isinstance(value, run_policy.Permission)
    ]


class EveryPermissionIsOneRowTest(unittest.TestCase):
    def test_permissions_are_declared(self):
        """Guards the suite: an empty catalog passes every assertion below."""
        self.assertTrue(declared_permissions())

    def test_every_permission_declares_a_cfg_key_and_a_flag(self):
        for permission in declared_permissions():
            with self.subTest(permission=permission.key):
                self.assertTrue(permission.key.startswith("allow_"))
                self.assertTrue(permission.flag.startswith("--"))

    def test_no_two_permissions_share_a_cfg_key(self):
        keys = [permission.key for permission in declared_permissions()]
        self.assertEqual(sorted(set(keys)), sorted(keys))

    def test_a_permission_is_not_granted_by_default(self):
        """An absent grant is a refusal. Defaulting to true would make forgetting
        to declare one indistinguishable from deciding to allow it."""
        with tempfile.TemporaryDirectory() as tmp:
            root = profile_root(tmp)
            for permission in declared_permissions():
                with self.subTest(permission=permission.key):
                    self.assertFalse(permission.granted(root, "p"))

    def test_a_declared_grant_is_granted(self):
        for permission in declared_permissions():
            with self.subTest(permission=permission.key), tempfile.TemporaryDirectory() as tmp:
                root = profile_root(tmp, **{permission.key: True})
                self.assertTrue(permission.granted(root, "p"))

    def test_a_non_bool_grant_is_refused(self):
        """A profile that says `allow_x: "yes"` must fail, not be truthy."""
        permission = declared_permissions()[0]
        with tempfile.TemporaryDirectory() as tmp:
            root = profile_root(tmp, **{permission.key: "yes"})
            with self.assertRaisesRegex(RuntimeError, "must be a bool"):
                permission.granted(root, "p")


class RefusalNamesWhatWasAskedAndWhatIsMissingTest(unittest.TestCase):
    def test_an_ungranted_request_is_refused(self):
        for permission in declared_permissions():
            with self.subTest(permission=permission.key), tempfile.TemporaryDirectory() as tmp:
                root = profile_root(tmp)
                with self.assertRaises(RuntimeError) as caught:
                    permission.require(root, "p", requested=True)
                message = str(caught.exception)
                self.assertIn(permission.flag, message)
                self.assertIn(permission.key, message)

    def test_a_request_that_was_not_made_needs_no_grant(self):
        """An ungranted permission costs nothing while unused, which is what lets
        a strict profile decline everything without declining every run."""
        with tempfile.TemporaryDirectory() as tmp:
            root = profile_root(tmp)
            for permission in declared_permissions():
                with self.subTest(permission=permission.key):
                    permission.require(root, "p", requested=False)

    def test_a_granted_request_passes(self):
        for permission in declared_permissions():
            with self.subTest(permission=permission.key), tempfile.TemporaryDirectory() as tmp:
                root = profile_root(tmp, **{permission.key: True})
                permission.require(root, "p", requested=True)

    def test_the_refusal_quotes_no_key_the_permission_does_not_declare(self):
        """The message is BUILT from the row, so it cannot outlive a rename — the
        failure that made this class necessary."""
        permission = run_policy.Permission("allow_invented_thing", "--invented-thing")
        with tempfile.TemporaryDirectory() as tmp:
            root = profile_root(tmp)
            with self.assertRaises(RuntimeError) as caught:
                permission.require(root, "p", requested=True)
            self.assertIn("allow_invented_thing", str(caught.exception))
            self.assertIn("--invented-thing", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
