"""A docstring opens with its summary, on the first line.

PEP 257, and the project rule that cites it. 93 docstrings across three repos
opened with a blank line instead — a form that reads as an unfinished sentence
and hides the summary from every tool that shows the first line.

Only the FORM is asserted. Length is not: the rule says to keep what the
signature cannot say, and this engine's longest docstrings are decision tables
and recorded defects, which are exactly that.
"""

import ast
import re
import unittest
from pathlib import Path

STACK = Path(__file__).resolve().parents[2]
REPOS = ("atlas-ctl-utils", "atlas-ctl-orchestrator", "atlas-ctl-adapter-aws")


class DocstringFormTests(unittest.TestCase):
    def documented_nodes(self):
        for repo in REPOS:
            for path in sorted((STACK / repo).rglob("*.py")):
                if path.name == "__init__.py" or "__pycache__" in str(path):
                    continue
                for node in ast.walk(ast.parse(path.read_text())):
                    if not isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef):
                        continue
                    doc = ast.get_docstring(node, clean=False)
                    if doc:
                        yield path, getattr(node, "name", "<module>"), doc

    def test_no_docstring_opens_with_a_blank_line(self):
        offenders = [
            f"{path.relative_to(STACK)}:{name}"
            for path, name, doc in self.documented_nodes()
            if doc.splitlines() and not doc.splitlines()[0].strip()
        ]
        self.assertEqual(offenders, [], "these docstrings hide their summary line")

    def test_every_summary_line_starts_with_a_capital(self):
        # unless it opens with a code token — an identifier map (`name -> ...`)
        # or a filename — where capitalising would misname the thing
        offenders = [
            f"{path.relative_to(STACK)}:{name}"
            for path, name, doc in self.documented_nodes()
            if doc.strip()
            and doc.strip()[0].islower()
            and not re.match(r"^[a-z_][a-z0-9_]*(\.py)?\s*(->|—|:)", doc.strip())
        ]
        self.assertEqual(offenders, [], "a summary line is a sentence")


if __name__ == "__main__":
    unittest.main()
