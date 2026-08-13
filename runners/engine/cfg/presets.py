"""Preset composition for platform cfg.

A PRESET is a directory of cfg payload that declares the values it must be
given. Every reference inside a preset resolves to a value it defines, a value
it imports, a parameter it declares, or the execution context — which is what
makes a preset readable on its own.

Three declaration files, all optional, none ever merged as payload:

    __imports__.yaml    imports: [{from, import, as?, with?}]
    __params__.yaml     params: [name, ...]
    __aliases__.yaml    <name>: <value>   PRIVATE to the unit

An alias is plumbing, not interface. A preset that needs `main_tag` to build a
name declares it privately; an importer asking for that preset receives the name
and nothing else. A value that genuinely belongs to a domain's published
contract is ordinary payload, declared by the unit that publishes it.

Imports are MATERIALIZED here: each import site produces its own directory of
payload with `${var.*}` already bound from that site's `with:`. The merge, the
whole-scope render, and the step-side projection are unchanged and never see a
preset — they see ordinary cfg directories. That keeps the mechanism confined to
this module.

Ordering is depth-first: a preset's own imports materialize first, then its own
files land on top, so a preset always wins over what it imports.
"""

from __future__ import annotations

import fnmatch
import re
import shutil
from pathlib import Path

from engine.kernel import yaml_io as kernel_yaml_io

IMPORTS_FILENAME = "__imports__.yaml"
PARAMS_FILENAME = "__params__.yaml"
ALIASES_FILENAME = "__aliases__.yaml"
DECLARATION_FILENAMES = (IMPORTS_FILENAME, PARAMS_FILENAME, ALIASES_FILENAME)

# `var` is a reserved top-level payload key: `${var.x}` would otherwise be
# ambiguous between a preset input and an ordinary collection lookup.
PARAM_NAMESPACE = "var"

_PARAM_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PARAM_REF_RE = re.compile(r"\$\{" + PARAM_NAMESPACE + r"\.([A-Za-z_][A-Za-z0-9_]*)(?::-.*?)?\}")
_ALIAS_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_IMPORT_FIELDS = {"from", "import", "as", "with"}


class PresetError(RuntimeError):
    """A preset declaration is malformed, incomplete, or unsatisfiable."""


def _load_yaml_mapping(path: Path) -> dict:
    try:
        return kernel_yaml_io.load_yaml(path)
    except RuntimeError as error:
        raise PresetError(f"❌ {path} must contain a mapping") from error


def _payload_files(preset_dir: Path) -> list[Path]:
    """Every payload file of a preset: yaml plus anything carried alongside it.

    Declaration files are excluded — they configure the mechanism and are never
    part of what an importer receives.
    """
    files: list[Path] = []
    for path in sorted(preset_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(preset_dir)
        if ".git" in rel.parts or "__guardrails__" in rel.parts:
            continue
        if path.name in DECLARATION_FILENAMES:
            continue
        files.append(path)
    return files


def declared_params(preset_dir: Path) -> list[str]:
    """Read `__params__.yaml`. Absent file means the preset takes no inputs."""
    params_path = preset_dir / PARAMS_FILENAME
    if not params_path.exists():
        return []
    doc = _load_yaml_mapping(params_path)
    unknown = set(doc) - {"params"}
    if unknown:
        raise PresetError(
            f"❌ unknown key(s) {sorted(unknown)} in {params_path}; only `params` is allowed"
        )
    raw = doc.get("params") or []
    if not isinstance(raw, list):
        # A mapping is the rejected form that carried per-param defaults.
        raise PresetError(f"❌ params must be a plain list of names (no defaults): {params_path}")
    names: list[str] = []
    for entry in raw:
        # A bare list of names. A mapping here is the rejected
        # form that carried defaults.
        if not isinstance(entry, str) or not _PARAM_NAME_RE.match(entry):
            raise PresetError(
                f"❌ params must be a plain list of names (no defaults): {params_path}; got {entry!r}"
            )
        if entry in names:
            raise PresetError(f"❌ duplicate param {entry!r}: {params_path}")
        names.append(entry)
    return names


def declared_aliases(preset_dir: Path) -> dict:
    """Read `__aliases__.yaml`: values bound inside the unit and never exported.

    Kept out of the payload so a preset's export surface is exactly what it is
    for. Importing a permissions boundary must not also hand over the deployment
    naming token the preset happened to need.
    """
    aliases_path = preset_dir / ALIASES_FILENAME
    if not aliases_path.exists():
        return {}
    doc = _load_yaml_mapping(aliases_path)
    for name in doc:
        if not isinstance(name, str) or not _PARAM_NAME_RE.match(name):
            raise PresetError(f"❌ alias name must be a simple name: {aliases_path}; got {name!r}")
    return doc


def declared_imports(preset_dir: Path) -> list[dict]:
    """Read and validate `__imports__.yaml`."""
    imports_path = preset_dir / IMPORTS_FILENAME
    if not imports_path.exists():
        return []
    doc = _load_yaml_mapping(imports_path)
    unknown = set(doc) - {"imports"}
    if unknown:
        raise PresetError(
            f"❌ unknown key(s) {sorted(unknown)} in {imports_path}; only `imports` is allowed"
        )
    raw = doc.get("imports") or []
    if not isinstance(raw, list):
        raise PresetError(f"❌ imports must be a list: {imports_path}")

    entries: list[dict] = []
    seen_sources: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise PresetError(f"❌ each import must be a mapping: {imports_path}")
        unknown = set(item) - _IMPORT_FIELDS
        if unknown:
            raise PresetError(
                f"❌ unknown import field(s) {sorted(unknown)} in {imports_path}; "
                f"allowed: {sorted(_IMPORT_FIELDS)}"
            )
        source = item.get("from")
        if not isinstance(source, str) or not source.startswith("/"):
            raise PresetError(
                f"❌ import `from` must be an absolute cfg path: {imports_path}; got {source!r}"
            )
        # A6/A9: one import per source. Materializing the same preset twice is
        # module-style instantiation, which belongs to the infrastructure layer.
        if source in seen_sources:
            raise PresetError(
                f"❌ duplicate import from {source!r} in {imports_path}; a preset is imported "
                "once per consuming scope"
            )
        seen_sources.add(source)

        selection = item.get("import")
        if selection is None:
            raise PresetError(f"❌ import from {source!r} must declare `import`: {imports_path}")
        if selection == "*":
            selected = "*"
        elif (
            isinstance(selection, list)
            and selection
            and all(isinstance(k, str) and k for k in selection)
        ):
            selected = list(selection)
        else:
            raise PresetError(
                f'❌ import `import` must be "*" or a non-empty list of cfg keys: {imports_path} ({source})'
            )

        alias = item.get("as")
        if alias is not None and (not isinstance(alias, str) or not _PARAM_NAME_RE.match(alias)):
            raise PresetError(f"❌ import `as` must be a simple name: {imports_path} ({source})")

        bindings = item.get("with") or {}
        if not isinstance(bindings, dict):
            raise PresetError(f"❌ import `with` must be a mapping: {imports_path} ({source})")
        for name in bindings:
            if not isinstance(name, str) or not _PARAM_NAME_RE.match(name):
                raise PresetError(
                    f"❌ `with` key must be a param name: {imports_path} ({source}); got {name!r}"
                )

        entries.append(
            {
                "from": source,
                "import": selected,
                "as": alias,
                "with": bindings,
                "declared_in": imports_path,
            }
        )
    return entries


def _referenced_params(preset_dir: Path) -> set[str]:
    """Where a declared param may legitimately be used.

    Payload is the obvious place. A `with:` binding is the other: an
    intermediate preset takes a param solely to FORWARD it to a preset it
    imports, which is a real use and must not read as a stale declaration.
    """
    found: set[str] = set()
    for path in _payload_files(preset_dir):
        if path.suffix != ".yaml":
            continue
        found.update(_PARAM_REF_RE.findall(path.read_text(encoding="utf-8")))
    imports_path = preset_dir / IMPORTS_FILENAME
    if imports_path.exists():
        for entry in declared_imports(preset_dir):
            for value in entry["with"].values():
                if isinstance(value, str):
                    found.update(_PARAM_REF_RE.findall(value))
    return found


def _bind_aliases(text: str, aliases: dict) -> str:
    """Substitute a unit's private aliases into its payload.

    An alias never reaches an importer, so the payload cannot carry `${main_tag}`
    and hope it resolves later — the name would be gone. Substituting here keeps
    the source readable (`${main_tag}-ctl-runner`) while what actually travels is
    the value the alias stands for, which is reachable from anywhere.
    """

    if not aliases:
        return text

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in aliases:
            return match.group(0)
        value = aliases[name]
        if isinstance(value, bool):
            return "true" if value else "false"
        return "null" if value is None else str(value)

    return _ALIAS_REF_RE.sub(replace, text)


def _bind_params(text: str, bindings: dict, *, label: str, params: list[str]) -> str:
    """Replace every `${var.x}` with its bound value.

    a bound value may itself contain `${y}`. It is substituted as
    text and resolves later against the consuming scope, exactly like any other
    reference — no new machinery, and an undefined `y` is still caught.
    """

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in params:
            raise PresetError(
                f"❌ ${{{PARAM_NAMESPACE}.{name}}} is not declared in params: {label}"
            )
        value = bindings[name]
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            return "null"
        return str(value)

    return _PARAM_REF_RE.sub(replace, text)


def _project_keys(doc: dict, selection, *, label: str) -> dict:
    """`import` names cfg KEYS, never files.

    A preset's internal file split is its own business; importers address the
    keys it produces. A selector that matches nothing is a stale declaration.
    """

    if selection == "*":
        return doc
    projected: dict = {}
    for key in selection:
        if key in doc:
            projected[key] = doc[key]
            continue
        matched = fnmatch.filter(list(doc), key) if any(c in key for c in "*?[") else []
        if not matched:
            raise PresetError(
                f"❌ imported key {key!r} is not produced by {label}; produced: "
                + (", ".join(sorted(doc)) or "nothing")
            )
        for name in matched:
            projected[name] = doc[name]
    return projected


def _merge(base: dict, incoming: dict) -> dict:
    merged = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _reachable(cfg_root: Path, preset_path: str, seen: tuple[str, ...] = ()) -> set[str]:
    """Every preset reached by following one preset's imports."""

    if preset_path in seen:
        return set()
    preset_dir = (cfg_root / preset_path.lstrip("/")).resolve()
    if not preset_dir.is_dir():
        return set()
    out: set[str] = set()
    for entry in declared_imports(preset_dir):
        out.add(entry["from"])
        out |= _reachable(cfg_root, entry["from"], (*seen, preset_path))
    return out


def assert_no_redundant_imports(cfg_root: Path, unit_dir: Path, unit_path: str) -> None:
    """An import must state something the unit could not get without it.

    An import whose content the unit never references, and which another of its
    own imports already brings, states nothing: dropping it changes no rendered
    value. It is not merely noise — it reads as a dependency the unit has, and
    the next person maintains it as one.

    Safe to forbid because A6 makes the transitive arrival dependable: whoever
    actually uses a value must declare it, so the value cannot quietly disappear
    from underneath a unit that receives it through another import.
    """

    entries = declared_imports(unit_dir)
    if len(entries) < 2:
        return

    own_references: set[str] = set()
    for path in _payload_files(unit_dir):
        if path.suffix == ".yaml":
            own_references |= _references_in(_load_yaml_mapping(path))

    direct = [entry["from"] for entry in entries]
    for source in direct:
        others = [other for other in direct if other != source]
        if not any(source in _reachable(cfg_root, other) for other in others):
            continue
        source_dir = (cfg_root / source.lstrip("/")).resolve()
        provided: set[str] = set()
        for path in _payload_files(source_dir):
            if path.suffix == ".yaml":
                provided |= _defined_paths(_load_yaml_mapping(path))
        deep = {key for key in provided if "." in key} or provided
        if any(ref == key or ref.startswith(key + ".") for key in deep for ref in own_references):
            continue
        via = next(other for other in others if source in _reachable(cfg_root, other))
        raise PresetError(
            f"❌ {unit_path} imports {source} without using it, and already receives it "
            f"through {via}; remove the import, or reference what it provides"
        )


def _binding_signature(bindings: dict) -> tuple:
    return tuple(sorted((name, repr(value)) for name, value in bindings.items()))


def materialize(
    cfg_root: Path,
    preset_path: str,
    *,
    dest: Path,
    bindings: dict | None = None,
    stack: tuple[str, ...] = (),
    composition: dict[str, tuple] | None = None,
) -> None:
    """Render one preset into `dest` as ordinary cfg payload.

    Depth-first: the preset's own imports land first, its own files on top.

    `composition` records how each preset was configured across ONE scope's whole
    import graph. A preset reached by two paths is fine while both configure it
    the same way — the copies are identical and merging is a no-op. Reached with
    DIFFERENT bindings it is an error: the two copies would merge silently and the
    last one would win, with nothing in either import saying so.
    """
    if preset_path in stack:
        chain = " -> ".join((*stack, preset_path))
        raise PresetError(f"❌ cfg import cycle: {chain}")

    preset_dir = (cfg_root / preset_path.lstrip("/")).resolve()
    try:
        preset_dir.relative_to(cfg_root.resolve())
    except ValueError as exc:
        raise PresetError(f"❌ import escapes the cfg root: {preset_path}") from exc
    if not preset_dir.is_dir():
        raise PresetError(f"❌ imported preset is not a directory: {preset_path}")

    params = declared_params(preset_dir)
    aliases = declared_aliases(preset_dir)
    bindings = dict(bindings or {})

    if composition is not None:
        signature = _binding_signature(bindings)
        previous = composition.get(preset_path)
        if previous is not None and previous != signature:
            raise PresetError(
                f"❌ preset {preset_path} is reached twice in one composition with different "
                f"params: {dict(previous)} and {dict(signature)}; a preset is configured once "
                "per composition"
            )
        composition[preset_path] = signature

    missing = [name for name in params if name not in bindings]
    if missing:
        raise PresetError(
            f"❌ preset {preset_path} requires param(s) {missing} that the importer did not supply"
        )
    undeclared = [name for name in bindings if name not in params]
    if undeclared:
        raise PresetError(
            f"❌ preset {preset_path} was given param(s) {undeclared} it does not declare"
        )
    if params:
        unused = sorted(set(params) - _referenced_params(preset_dir))
        if unused:
            raise PresetError(
                f"❌ preset {preset_path} declares param(s) {unused} that it never references "
                f"as ${{{PARAM_NAMESPACE}.<name>}}"
            )

    dest.mkdir(parents=True, exist_ok=True)
    assert_no_redundant_imports(cfg_root, preset_dir, preset_path)

    imported_keys: set[str] = set()
    for entry in declared_imports(preset_dir):
        nested_dest = dest
        nested = Path(str(dest) + ".__import__")
        if nested.exists():
            shutil.rmtree(nested)
        materialize(
            cfg_root,
            entry["from"],
            dest=nested,
            bindings=_bind_import_values(entry, params, bindings, preset_path),
            stack=(*stack, preset_path),
            composition=composition,
        )
        _absorb(
            nested, nested_dest, selection=entry["import"], alias=entry["as"], label=entry["from"]
        )
        shutil.rmtree(nested)

    for path in sorted(dest.rglob("*.yaml")):
        imported_keys |= _defined_paths(_load_yaml_mapping(path))
    _assert_self_contained(preset_dir, preset_path, imported_keys, params, aliases)

    label = f"preset {preset_path}"
    for path in _payload_files(preset_dir):
        rel = path.relative_to(preset_dir)
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix != ".yaml":
            shutil.copy2(path, target)
            continue
        text = path.read_text(encoding="utf-8")
        text = _bind_aliases(text, aliases)
        if params:
            text = _bind_params(text, bindings, label=label, params=params)
        try:
            doc = kernel_yaml_io.load_yaml_text(text, label=str(path))
        except RuntimeError as error:
            raise PresetError(f"❌ cfg payload must be a mapping: {path}") from error
        if PARAM_NAMESPACE in doc:
            raise PresetError(
                f"❌ plt payload must not define reserved top-level key {PARAM_NAMESPACE!r}: {path}"
            )
        if target.exists():
            doc = _merge(_load_yaml_mapping(target), doc)
        kernel_yaml_io.write_yaml_file(target, doc)


_REFERENCE_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)(?::-.*?)?\}")
_EXECUTION_CONTEXT_PREFIX = "execution_context."


def _defined_paths(value, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            paths.add(prefix + str(key))
            paths |= _defined_paths(child, prefix + str(key) + ".")
    return paths


def _references_in(value) -> set[str]:
    """References carried by cfg VALUES. Comments describe, they do not resolve."""
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            found |= _references_in(key) | _references_in(child)
    elif isinstance(value, list):
        for child in value:
            found |= _references_in(child)
    elif isinstance(value, str):
        found |= set(_REFERENCE_RE.findall(value))
    return found


def _owns(name: str, defined: set[str]) -> bool:
    """Does the unit own enough of `name` to be allowed to reference it?

    The unit must own the reference, or own a COLLECTION it sits inside. That is
    what lets a base layer and the layer above it co-define one structure: `all`
    owns `service_defaults`, so it may read a version field that the env layer
    fills into a member of it, whether or not `all` defines that member.

    A ONE-SEGMENT prefix counts. It did not use to: every helper was
    wrapped in a shared `_common` root, so owning one key under it proved nothing
    about `_common.permissions_boundaries.workload.name`, which belonged to a
    different unit entirely — the rule had to demand two segments to tell a real
    collection from that shared namespace. With the wrapper removed a top-level
    key IS the collection, owning it is a real claim, and requiring two segments
    would instead reject the co-definition case above.
    """

    if name in defined:
        return True
    segments = name.split(".")
    return any(".".join(segments[:size]) in defined for size in range(len(segments) - 1, 0, -1))


def _assert_self_contained(
    preset_dir: Path, preset_path: str, imported: set[str], params: list[str], aliases: dict
) -> None:
    """A6: every reference a preset makes resolves to something the preset has.

    Locally defined, imported, declared as a param, or the execution context —
    nothing else. A reference that only resolves once the preset lands in some
    particular scope is the defect this phase removes: it makes the preset
    unreadable on its own and silently unusable anywhere else.

    Judged on the preset's OWN files in their DECLARED form, before any `with:`
    binding is substituted. A bound value may itself carry a reference (O2), but
    that reference is the importer's to satisfy, not the preset's — and the
    importer is judged by this same rule.

    """

    defined = set(imported) | set(aliases)
    references: set[str] = set()
    for path in _payload_files(preset_dir):
        if path.suffix != ".yaml":
            continue
        doc = _load_yaml_mapping(path)
        defined |= _defined_paths(doc)
        references |= _references_in(doc)

    unresolved = sorted(
        name
        for name in references
        if not name.startswith(_EXECUTION_CONTEXT_PREFIX)
        and not (name.startswith(PARAM_NAMESPACE + ".") and name.split(".", 1)[1] in params)
        and not _owns(name, defined)
    )
    if unresolved:
        rendered = ", ".join(f"${{{name}}}" for name in unresolved)
        raise PresetError(
            f"❌ preset {preset_path} makes reference(s) it does not own: {rendered}; "
            "define them, import them, or declare them as params"
        )


def _bind_import_values(entry: dict, params: list[str], bindings: dict, preset_path: str) -> dict:
    """Bind an inner import's `with:`, threading this preset's own params through."""
    resolved: dict = {}
    for name, value in entry["with"].items():
        if isinstance(value, str) and params:
            value = _bind_params(value, bindings, label=f"preset {preset_path}", params=params)
        resolved[name] = value
    return resolved


def _absorb(source: Path, dest: Path, *, selection, alias: str | None, label: str) -> None:
    """Fold a materialized import into its importer's payload tree.

    Selection and aliasing act on KEYS, but the import keeps its RELATIVE FILE
    PATHS. Both later stages — the whole-scope render and the step-side
    projection — merge a scope's yaml files in sorted path order, so collapsing
    an import into one file would silently re-order which definition wins. A key
    spread across many files (`foundation` spans 42) is projected file by file
    and stays spread.
    """
    dest.mkdir(parents=True, exist_ok=True)
    yaml_paths = [p for p in sorted(source.rglob("*")) if p.is_file() and p.suffix == ".yaml"]

    if selection != "*":
        # "matched nothing" is judged over the WHOLE preset: a key legitimately
        # appears in only some of its files.
        whole: dict = {}
        for path in yaml_paths:
            whole = _merge(whole, _load_yaml_mapping(path))
        _project_keys(whole, selection, label=label)

    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(source)
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix != ".yaml":
            shutil.copy2(path, target)
            continue
        doc = _load_yaml_mapping(path)
        if selection != "*":
            doc = {key: value for key, value in doc.items() if _selected(key, selection)}
            if not doc:
                continue
        if alias:
            doc = {alias: doc}
        if target.exists():
            doc = _merge(_load_yaml_mapping(target), doc)
        kernel_yaml_io.write_yaml_file(target, doc)


def _selected(key: str, selection) -> bool:
    for pattern in selection:
        if key == pattern or (any(c in pattern for c in "*?[") and fnmatch.fnmatch(key, pattern)):
            return True
    return False
