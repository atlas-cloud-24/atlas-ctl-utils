"""The engine's surface, as the tests see it — discovered, never enumerated.

Every helper here walks the package. A test that names modules or files goes
stale the moment one moves, and it goes stale SILENTLY: a list that matches
nothing still passes every assertion made over it. That is what a
by-name list did to the provider boundary, and what a single-file source scan
did to the checks that read engine source.
"""

import contextlib
import importlib
import pkgutil
from pathlib import Path
from unittest import mock

import engine

PACKAGE_ROOT = Path(engine.__path__[0])


def engine_modules() -> dict[str, object]:
    """Every engine module, imported, keyed by dotted name."""
    modules = {}
    for info in pkgutil.walk_packages(engine.__path__, prefix="engine."):
        if not info.ispkg:
            modules[info.name] = importlib.import_module(info.name)
    if not modules:
        raise RuntimeError(
            f"walked {PACKAGE_ROOT} and found no engine modules — every check "
            f"built on this would pass vacuously"
        )
    return modules


def engine_source_files() -> list[Path]:
    """Every engine source file. Package markers carry no code and are skipped."""
    files = sorted(
        p for p in PACKAGE_ROOT.rglob("*.py")
        if p.name != "__init__.py" and "__pycache__" not in p.parts
    )
    if not files:
        raise RuntimeError(f"no engine source under {PACKAGE_ROOT}")
    return files


def engine_source() -> str:
    """All engine source as one string, for checks that grep the whole engine."""
    return "\n".join(p.read_text(encoding="utf-8") for p in engine_source_files())


def owning_modules() -> dict[str, object]:
    """name -> the engine object that DEFINES it, for patching.

    A bare name is owned by its module. A name grouped under a class is owned by
    the CLASS and is keyed `Class.method`, because that is the object a patch has
    to land on — patching the module would leave every caller reaching through
    the class untouched.

    A name defined in two modules is ambiguous, so it is dropped rather than
    resolved to whichever import happened last.
    """
    owners: dict[str, object] = {}
    duplicates: set[str] = set()

    def claim(key: str, owner: object) -> None:
        if key in owners:
            duplicates.add(key)
        owners[key] = owner

    for dotted, module in engine_modules().items():
        for name, value in vars(module).items():
            if name.startswith("__") or getattr(value, "__module__", None) != dotted:
                continue
            claim(name, module)
            if isinstance(value, type):
                for attr, member in vars(value).items():
                    if attr.startswith("__") or not callable(member):
                        continue
                    claim(f"{name}.{attr}", value)
    for name in duplicates:
        owners.pop(name, None)
    return owners


def engine_defines(name: str) -> bool:
    """Whether ANY engine module exports this name.

    The question a removal test asks: not "is it gone from the file it used to
    be in", which a move would answer wrongly, but "is it gone from the engine".
    """
    return any(hasattr(module, name) for module in engine_modules().values())


@contextlib.contextmanager
def patch_engine(*dotted, **names):
    """Stub engine functions BY NAME, on whichever object owns each one.

    A caller reaches a function through its owning module, so the patch has to
    land there — and one `with` block routinely stubs names that live in several
    modules, which `mock.patch.multiple` cannot express because it takes exactly
    one target. Yields {name: Mock}, like `patch.multiple`.

    A grouped name is `Class.method`, which is not a legal keyword, so those are
    passed as leading mappings: `patch_engine({"Cls.method": mock.DEFAULT}, ...)`.
    The patch lands on the class, which is what every caller reaching through it
    (including a module-level singleton) resolves against.
    """
    targets: dict[str, object] = {}
    for mapping in dotted:
        targets.update(mapping)
    targets.update(names)

    owners = owning_modules()
    unknown = sorted(n for n in targets if n not in owners)
    if unknown:
        raise AssertionError(
            f"no engine module defines {unknown} — stubbing a name that does not "
            f"exist would silently pass"
        )

    by_owner: dict[int, tuple[object, dict]] = {}
    attr_names: dict[str, str] = {}
    for name, value in targets.items():
        owner = owners[name]
        attr = name.rpartition(".")[2]
        attr_names[attr] = name
        by_owner.setdefault(id(owner), (owner, {}))[1][attr] = value

    with contextlib.ExitStack() as stack:
        patched: dict[str, mock.Mock] = {}
        for owner, group in by_owner.values():
            for attr, patched_mock in stack.enter_context(
                mock.patch.multiple(owner, **group)
            ).items():
                patched[attr_names[attr]] = patched_mock
        yield patched
