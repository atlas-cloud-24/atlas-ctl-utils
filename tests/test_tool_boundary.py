"""Engine core carries no TOOL vocabulary.

The engine has a PROVIDER seam — adapters, a registry, and
`test_provider_boundary.py` keeping AWS vocabulary out of engine core. It has no
TOOL seam, and Terraform leaked straight into engine core: a regex that greps
`step.sh` SOURCE for `./bin/tf.sh <dir> init <var>`, a function that extracts a
Terraform project and state-key variable from it, a literal `"terraform"`, and a
`TFSTATE_KEY_VAR` env var.

That regex is the one that matters. Everywhere else the engine treats a step as a
black box behind `step.yaml`; there it opens the shell script and pattern-matches
a tool invocation to guess where state lives. It is also silently fragile: rename
`bin/tf.sh`, wrap the call in a function, or split init across two steps, and
force-unlock stops working while the step itself still runs fine.

Every tool interaction belongs inside a step, behind a contract the engine already
has. This test is the ratchet that keeps it there.
"""


import re
import unittest
from pathlib import Path

# The same files test_provider_boundary.py guards, for the same reason — and by
# the same walk, IMPORTED rather than repeated here. Two walks are two places for
# the boundary to be wrong, and they drift apart silently: the loser keeps
# passing over whatever it still matches. `EngineCoreDiscoveryTests` over there
# guards the walk for both, so this file inherits the proof that it covers
# something.
from test_provider_boundary import engine_core_files

REPO_ROOT = Path(__file__).resolve().parents[1]

# A tool the engine must not name. `tf` alone is too common a fragment to match
# safely, so the patterns are anchored to how the tool actually appears.
FORBIDDEN_TOOL = re.compile(
    r"(?i)("
    r"\bterraform\b"
    r"|\btofu\b|\bopentofu\b"
    r"|tfstate"
    r"|tfvars"
    r"|tf\.sh"
    r"|\.tf\b"
    r"|\bhcl\b"
    r")"
)

# Prose that legitimately names the tool while describing a CONSUMER contract
# rather than engine behaviour would go here. Empty on purpose: an exemption is a
# decision, and an empty list makes adding one visible in review.
# `run_step.sh` and `src/step.sh` are the STEP CONTRACT — the engine locates and
# runs them, which is not tool knowledge. Only a specific tool's name is.
CONTRACT_FILENAMES = re.compile(r"run_step\.sh|src/step\.sh|step\.sh")

ALLOWED_LINES: frozenset[str] = frozenset()


def _hits(path: Path) -> list[str]:
    # Located by path, not by name: most of the engine's basenames repeat.
    return [
        f"{path.relative_to(REPO_ROOT)}:{number}: {line.strip()}"
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if FORBIDDEN_TOOL.search(CONTRACT_FILENAMES.sub("", line))
        and line.strip() not in ALLOWED_LINES
    ]


class ToolBoundaryTests(unittest.TestCase):
    def test_engine_core_has_no_tool_tokens(self):
        hits = [h for path in engine_core_files() for h in _hits(path)]
        self.assertEqual(
            [],
            hits,
            f"{len(hits)} engine-core tool tokens — a tool belongs inside a step, "
            "behind the step contract:\n" + "\n".join(hits),
        )

    def test_engine_core_never_reads_step_source(self):
        """The engine must not READ a step's script to infer what it does.

        `step.yaml` is the contract. Parsing `src/step.sh` to pattern-match a tool
        invocation makes the engine depend on a step's INTERNALS, which no other
        engine code does and which nothing declares.

        Naming the script is fine and unavoidable — the engine locates and runs
        it. What is forbidden is OPENING it.
        """

        offenders = []
        for path in engine_core_files():
            for number, line in enumerate(path.read_text().splitlines(), start=1):
                if "step.sh" in line and ".read_text" in line:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{number}: {line.strip()}")
        self.assertEqual([], offenders, "\n".join(offenders))


if __name__ == "__main__":
    unittest.main()
