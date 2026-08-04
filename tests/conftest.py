"""Test-time path setup, in ONE place.

 moved the AWS adapter into its own repository, so it is no longer a
subpackage of the engine. Its location is DECLARED — `ctl-adapter-aws` is a
tooling ref, and the dev cfg reflection resolves it to a local checkout in
`local_repos.yaml` — so the tests read that declaration instead of guessing at
the filesystem.

Falling back to the sibling checkout by NAME (never a glob) keeps the suite
runnable before the dev cfg has been generated; discovering adapters by scanning
what sits beside the engine is exactly what the declaration exists to replace.
"""


import sys
from pathlib import Path

import yaml

_UTILS = Path(__file__).resolve().parents[1]
_STACK = _UTILS.parent
_LOCAL_REPOS = _STACK.parent / "cfg/oxygen/oxygen-ctl-cfg-dev/local_repos.yaml"


def _declared_tooling_paths() -> list[Path]:
    if not _LOCAL_REPOS.is_file():
        return []
    tooling = (yaml.safe_load(_LOCAL_REPOS.read_text()) or {}).get("tooling") or {}
    return [
        Path(entry["repo_path"])
        for name, entry in tooling.items()
        if name.startswith("ctl-adapter-") and entry.get("repo_path")
    ]


_paths = [_UTILS / "runners", *_declared_tooling_paths()]
if not any(p.name.startswith("atlas-ctl-adapter-") for p in _paths):
    _paths.append(_STACK / "atlas-ctl-adapter-aws")

for _path in _paths:
    if _path.is_dir() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
