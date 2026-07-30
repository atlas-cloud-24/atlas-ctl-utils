"""No branch may read a local that only a SIBLING branch assigns.

`resolve_pipeline_selection` dispatches on selection kind — procedure, target,
workflow — and each branch assembles its cfg in a different ORDER. Threading a new
argument into one branch is exactly how you come to read a name a sibling assigns:
Python compiles it happily and it fails at run time, on that branch alone.

That shipped once, past 487 green tests and full cfg validation, because nothing
exercised the target branch. The runtime guard that does exercise it resolves real
cfg and takes minutes, so it is opt-in — and a guard you must remember to run is a
weak guard. This is the cheap one: the property is static, so it is checked
statically, on every run, in milliseconds.
"""

import ast
import builtins
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
COMMON = REPO_ROOT / "runners/utils/common.py"


def _bound_by(node: ast.AST) -> set[str]:
    """Names this statement binds, not descending into nested scopes."""
    bound: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        for child in ast.iter_child_nodes(current):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                                  ast.ClassDef)):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                      ast.ClassDef)):
                    bound.add(child.name)
                continue
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store):
                bound.add(child.id)
            elif isinstance(child, (ast.Import, ast.ImportFrom)):
                for alias in child.names:
                    bound.add((alias.asname or alias.name).split(".")[0])
            elif isinstance(child, ast.ExceptHandler) and child.name:
                bound.add(child.name)
            stack.append(child)
    return bound


def _loads(node: ast.AST) -> set[str]:
    """Names this statement reads, not descending into nested scopes."""
    reads: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        for child in ast.iter_child_nodes(current):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                                  ast.ClassDef)):
                continue
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
                reads.add(child.id)
            stack.append(child)
    return reads


def _scan(body: list[ast.stmt], bound: set[str], report: list[str], where: str) -> set[str]:
    """Walk statements in order, flagging a branch that reads a sibling's local.

    Returns the names bound after the block. A name bound in only SOME branches is
    conservatively treated as bound afterwards — this guard is about cross-branch
    reads, and claiming more would produce false positives on the common
    assign-in-every-branch shape.
    """
    bound = set(bound)
    for statement in body:
        if isinstance(statement, ast.If):
            branches: list[list[ast.stmt]] = []
            node: ast.stmt = statement
            while isinstance(node, ast.If):
                branches.append(node.body)
                if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
                    node = node.orelse[0]
                    continue
                if node.orelse:
                    branches.append(node.orelse)
                break
            per_branch = [set().union(*(_bound_by(s) for s in b)) if b else set()
                          for b in branches]
            for index, branch in enumerate(branches):
                siblings: set[str] = set()
                for other in range(len(branches)):
                    if other != index:
                        siblings |= per_branch[other]
                # NOT `- per_branch[index]`: a branch that binds the name LATER
                # still reads it unbound here, which is precisely the defect.
                # Ordering is handled per statement by `seen` below.
                siblings -= bound
                # only what this branch reads BEFORE binding it itself
                seen: set[str] = set()
                for statement_in_branch in branch:
                    risky = (_loads(statement_in_branch) & siblings) - seen
                    for name in sorted(risky):
                        report.append(
                            f"{where}: line {statement_in_branch.lineno} reads {name!r}, "
                            "which only a sibling branch assigns"
                        )
                    seen |= _bound_by(statement_in_branch)
                _scan(branch, bound | per_branch[index], report, where)
            for names in per_branch:
                bound |= names
        else:
            for field in ("body", "orelse", "finalbody"):
                nested = getattr(statement, field, None)
                if isinstance(nested, list) and nested and isinstance(nested[0], ast.stmt):
                    _scan(nested, bound, report, where)
            bound |= _bound_by(statement)
    return bound


class BranchesDoNotShareLocalsTest(unittest.TestCase):
    MODULE = COMMON

    def _violations(self) -> list[str]:
        tree = ast.parse(self.MODULE.read_text())
        module_names = set(dir(builtins))
        for node in tree.body:
            # A def contributes only its NAME. Descending into its body would
            # collect every local in the file as a module-level name, which is
            # what silently disarmed this guard the first time it was written.
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                module_names.add(node.name)
            else:
                module_names |= _bound_by(node)
        report: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            params = {a.arg for a in node.args.args + node.args.kwonlyargs
                      + node.args.posonlyargs}
            if node.args.vararg:
                params.add(node.args.vararg.arg)
            if node.args.kwarg:
                params.add(node.args.kwarg.arg)
            _scan(node.body, module_names | params, report, node.name)
        return report

    # The scan reports four other sites whose shapes it does not yet model
    # correctly (a name bound by a tuple unpack or inside a `with`, read later in
    # the same branch). They are not defects, so asserting on them would ship a
    # flaky guard; the assertion is scoped to the resolvers where this class of
    # bug actually bit, and widening it means fixing the scan first.
    WATCHED = (
        "resolve_pipeline_selection",
        "run_pipeline",
        "build_active_target_runs",
        "run_targets",
    )

    def test_selection_resolvers_have_no_cross_branch_reads(self):
        violations = [
            v for v in self._violations() if v.split(":", 1)[0] in self.WATCHED
        ]
        self.assertEqual([], violations, "\n" + "\n".join(violations))


if __name__ == "__main__":
    unittest.main()
