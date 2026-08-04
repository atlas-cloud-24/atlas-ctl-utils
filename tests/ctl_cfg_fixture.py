"""Minimal ctl cfg a test can point the engine at.

The engine holds NO list of providers — which ones exist is declared by the
consumer, in `ctl_providers.yaml`. That means a test exercising anything that
reaches an adapter has to declare one too, exactly as a real cfg tree does.

Stated per test rather than activated once for the suite. A suite-wide default
would make the missing declaration invisible, which is the failure the
declaration exists to prevent.
"""

import tempfile
from pathlib import Path

import yaml

from engine.execution import adapters


def declare_providers(root: Path, *names: str) -> Path:
    """Write `ctl_providers.yaml` into a cfg root, declaring these providers."""
    if not names:
        raise AssertionError("declare at least one provider, or the fixture proves nothing")
    (Path(root) / "ctl_providers.yaml").write_text(
        yaml.safe_dump({"ctl_providers": {name: {"implements": ["execution"]} for name in names}}),
        encoding="utf-8",
    )
    return Path(root)


def activate(test, root: Path) -> Path:
    """Make `root` the active cfg root for one test, and unset it afterwards.

    Unset via `addCleanup` because a leaked activation would let a LATER test
    pass on cfg it never declared — the failure would then surface somewhere
    unrelated, or not at all.
    """
    adapters.set_active_ctl_cfg_root(Path(root))
    test.addCleanup(adapters.set_active_ctl_cfg_root, None)
    return Path(root)


def cfg_root(test, *names: str) -> Path:
    """A throwaway cfg root declaring these providers, active for one test.

    For a test that reaches an adapter while holding no cfg tree of its own —
    the declaration still has to be somewhere for the engine to read.
    """
    temporary = tempfile.TemporaryDirectory()
    test.addCleanup(temporary.cleanup)
    return activate(test, declare_providers(Path(temporary.name), *names))
