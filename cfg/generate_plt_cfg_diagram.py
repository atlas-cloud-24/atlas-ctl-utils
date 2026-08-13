#!/usr/bin/env python3
"""Generate a Mermaid composition graph for platform cfg, and audit it.

The graph is the import structure: which scopes and presets compose which, what
each import carries, and where parameters are threaded. The audit reports what
the engine's own assertions cannot see from inside a single unit — units nobody
reaches, values a scope imports but never uses, and imports that carry more than
the importer asked for.

Read-only. Writes a `.mmd` file (and an SVG when Mermaid CLI is available).
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from engine.cfg import presets as cfg_presets

REFERENCE_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)(?::-.*?)?\}")
EXECUTION_CONTEXT_PREFIX = "execution_context."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plt-cfg-root", required=True, help="Platform cfg directory to read.")
    parser.add_argument(
        "--out", help="Output .mmd path (default: <plt-cfg-root>/diagrams/composition.mmd)."
    )
    parser.add_argument(
        "--audit-only", action="store_true", help="Report findings without writing a diagram."
    )
    parser.add_argument(
        "--mmdc", help="Path or command name for Mermaid CLI (default: mmdc from PATH)."
    )
    parser.add_argument(
        "--mmd-only", action="store_true", help="Write Mermaid source without rendering SVG."
    )
    return parser.parse_args()


def payload_files(unit_dir: Path) -> list[Path]:
    return [
        path
        for path in sorted(unit_dir.rglob("*.yaml"))
        if path.name not in cfg_presets.DECLARATION_FILENAMES
        and not path.name.startswith("__")
        and "__guardrails__" not in path.parts
        and ".git" not in path.parts
    ]


def defined_paths(value, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            paths.add(prefix + str(key))
            paths |= defined_paths(child, prefix + str(key) + ".")
    return paths


def references_in(value) -> set[str]:
    """References carried by cfg VALUES. Comments describe, they do not resolve."""
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            found |= references_in(key) | references_in(child)
    elif isinstance(value, list):
        for child in value:
            found |= references_in(child)
    elif isinstance(value, str):
        found |= set(REFERENCE_RE.findall(value))
    return found


class Graph:
    def __init__(self, cfg_root: Path) -> None:
        self.root = cfg_root.resolve()
        self.scopes: dict[str, dict] = {}
        self.units: set[str] = set()
        self.imports: dict[str, list[dict]] = {}
        self.params: dict[str, list[str]] = {}
        self.aliases: dict[str, dict] = {}
        self.defines: dict[str, set[str]] = {}
        self.uses: dict[str, set[str]] = {}
        self._load()

    def _rel(self, path: Path) -> str:
        rel = path.relative_to(self.root).as_posix()
        return "/" + rel if rel != "." else "/"

    def _load(self) -> None:
        for meta_path in sorted(
            self.root.rglob(cfg_presets.IMPORTS_FILENAME.replace("imports", "meta"))
        ):
            doc = yaml.safe_load(meta_path.read_text()) or {}
            if doc.get("type") == "overlay":
                continue
            unit = self._rel(meta_path.parent)
            self.scopes[unit] = doc
            self.units.add(unit)

        for imports_path in sorted(self.root.rglob(cfg_presets.IMPORTS_FILENAME)):
            unit = self._rel(imports_path.parent)
            self.units.add(unit)
            entries = cfg_presets.declared_imports(imports_path.parent)
            self.imports[unit] = entries
            for entry in entries:
                self.units.add(entry["from"])

        for unit in sorted(self.units):
            unit_dir = self.root / unit.lstrip("/")
            self.params[unit] = cfg_presets.declared_params(unit_dir)
            self.aliases[unit] = cfg_presets.declared_aliases(unit_dir)
            defined: set[str] = set()
            used: set[str] = set()
            for path in payload_files(unit_dir):
                doc = yaml.safe_load(path.read_text()) or {}
                defined |= defined_paths(doc)
                used |= references_in(doc)
            self.defines[unit] = defined
            self.uses[unit] = used

    @property
    def presets(self) -> list[str]:
        return sorted(self.units - set(self.scopes))

    def reachable_from(self, unit: str, seen: tuple[str, ...] = ()) -> list[str]:
        """Every unit reached by following this unit's imports, depth-first."""

        if unit in seen:
            return []
        out: list[str] = []
        for entry in self.imports.get(unit, []):
            out.append(entry["from"])
            out.extend(self.reachable_from(entry["from"], (*seen, unit)))
        return out

    @staticmethod
    def _touches(references: set[str], provided: set[str]) -> bool:
        return any(
            ref == key or ref.startswith(key + ".") for key in provided for ref in references
        )

    def importers_of(self, unit: str) -> list[str]:
        return sorted(
            u for u, entries in self.imports.items() if any(e["from"] == unit for e in entries)
        )

    def audit(self) -> list[tuple[str, str]]:
        """Findings the per-unit assertions structurally cannot see."""
        findings: list[tuple[str, str]] = []

        for preset in self.presets:
            if not self.importers_of(preset):
                findings.append(("unreachable", f"{preset} is imported by nothing"))

        for unit, entries in sorted(self.imports.items()):
            own_refs = self.uses[unit]
            own_defined = self.defines[unit]
            for entry in entries:
                source = entry["from"]
                provided = self.defines.get(source, set())
                if entry["import"] != "*":
                    provided = {
                        key
                        for key in provided
                        if any(key == sel or key.startswith(sel + ".") for sel in entry["import"])
                    }
                if entry["as"]:
                    continue  # aliased content is addressed under a name; skip surface analysis
                top_level = {key for key in provided if "." not in key}
                if not top_level:
                    continue
                touched = {
                    key
                    for key in top_level
                    if any(ref == key or ref.startswith(key + ".") for ref in own_refs)
                }
                # A scope may import a preset purely to PUBLISH what it provides
                republished = bool(top_level & own_defined) or unit in self.scopes
                if not touched and not republished:
                    findings.append(
                        (
                            "unused-import",
                            f"{unit} imports {source} but references none of {sorted(top_level)}",
                        )
                    )
                elif touched and len(top_level - touched) > 0 and entry["import"] == "*":
                    extra = sorted(top_level - touched)
                    if not republished:
                        findings.append(
                            (
                                "wide-import",
                                f'{unit} imports {source} with "*" but uses only {sorted(touched)}; '
                                f"also receives {extra}",
                            )
                        )

        # An import declared where it is not used, while a preset BELOW it does use
        # it, is the dynamic-scoping shape: the importer carries a dependency on its
        # child's behalf, and the child looks satisfied only because of that.
        # Matched on FULL paths. Comparing top-level keys collides on shared roots
        # like `_common`, which is the same mistake A6 originally made.
        # Scopes are checked too. A scope carrying an import on a preset's behalf is
        # precisely the defect this catches, and exempting scopes because they also
        # publish would exempt the only place the defect occurs.
        for unit, entries in sorted(self.imports.items()):
            for entry in entries:
                source = entry["from"]
                if entry["as"]:
                    continue
                provided = {key for key in self.defines.get(source, set()) if "." in key}
                if not provided:
                    continue
                if self._touches(self.uses[unit], provided):
                    continue
                consumers = sorted(
                    {
                        descendant
                        for descendant in self.reachable_from(unit)
                        if descendant != source
                        and source not in [e["from"] for e in self.imports.get(descendant, [])]
                        and self._touches(self.uses[descendant], provided)
                    }
                )
                if consumers:
                    findings.append(
                        (
                            "import-belongs-lower",
                            f"{unit} imports {source} but does not use it; "
                            f"{', '.join(consumers)} does — declare it there",
                        )
                    )

        # An import whose content the unit neither references nor would lose —
        # because another of its imports already brings it — states nothing. It
        # is safe to drop precisely because A6 forces whoever DOES use the content
        # to declare it, so the value cannot quietly disappear from underneath.
        for unit, entries in sorted(self.imports.items()):
            direct = [entry["from"] for entry in entries]
            for entry in entries:
                source = entry["from"]
                reached_via = [
                    other
                    for other in direct
                    if other != source and source in self.reachable_from(other)
                ]
                if not reached_via:
                    continue
                provided = {key for key in self.defines.get(source, set()) if "." in key}
                provided = provided or self.defines.get(source, set())
                if self._touches(self.uses[unit], provided):
                    continue
                findings.append(
                    (
                        "redundant-import",
                        f"{unit} imports {source} without using it, and already receives it "
                        f"via {reached_via[0]}",
                    )
                )

        for unit in sorted(self.units):
            for name in self.aliases[unit]:
                if not any(ref == name or ref.startswith(name + ".") for ref in self.uses[unit]):
                    findings.append(("unused-alias", f"{unit} binds alias {name!r} it never uses"))
            for name in self.params[unit]:
                bound_onward = any(
                    isinstance(value, str) and f"${{{cfg_presets.PARAM_NAMESPACE}.{name}}}" in value
                    for entry in self.imports.get(unit, [])
                    for value in entry["with"].values()
                )
                used_here = any(
                    ref == f"{cfg_presets.PARAM_NAMESPACE}.{name}" for ref in self.uses[unit]
                )
                if not used_here and not bound_onward:
                    findings.append(
                        ("unused-param", f"{unit} declares param {name!r} it never uses")
                    )

        return findings

    def composition_of(self, scope: str) -> list[str]:
        """Every unit in one scope's composition, the scope included."""

        return sorted({scope, *self.reachable_from(scope)})

    def _depth(self, scope: str) -> dict[str, int]:
        """Distance from the leaf presets, so a diagram can be layered.

        Longest path is deliberate: a unit reached by a short and a long route
        belongs on the deeper row, or its edges would point upward.
        """
        depth: dict[str, int] = {}

        def measure(unit: str, seen: tuple[str, ...] = ()) -> int:
            if unit in seen:
                return 0
            if unit in depth:
                return depth[unit]
            below = [measure(e["from"], (*seen, unit)) for e in self.imports.get(unit, [])]
            depth[unit] = 1 + max(below, default=-1)
            return depth[unit]

        for unit in self.composition_of(scope):
            measure(unit)
        return depth

    def _label(self, unit: str) -> str:
        """The unit's cfg path, verbatim.

        A path is what identifies a scope or a preset, and it is what the cfg
        itself writes in `from:`. Shortening it to a leaf name invents a second
        name for the same thing and stops the diagram being greppable against
        the files it describes.
        """

        return unit

    def scope_mermaid(self, scope: str) -> str:
        """One scope, its whole import tree, presets above and the scope below.

        Edges are drawn preset -> importer, which is the direction values flow and
        the direction merge order runs, so the diagram reads the way the cfg does.
        """

        units = self.composition_of(scope)
        depth = self._depth(scope)

        def node_id(unit: str) -> str:
            return "n_" + re.sub(r"[^A-Za-z0-9]", "_", unit.strip("/"))

        lines = [
            f"%% Composition of {scope}. Generated by cfg/generate_plt_cfg_diagram.py.",
            "graph TD",
            "  classDef scope fill:#1f6feb,stroke:#0b3d91,color:#ffffff,font-weight:bold;",
            "  classDef preset fill:#f6f8fa,stroke:#8b949e,color:#24292f;",
            "  classDef parametrised fill:#fff4e5,stroke:#d97706,color:#24292f;",
        ]
        by_level: dict[int, list[str]] = {}
        for unit in units:
            by_level.setdefault(depth[unit], []).append(unit)

        for level in sorted(by_level, reverse=True):
            title = "scope" if level == depth[scope] else f"level {level}"
            lines.append(f'  subgraph L{level}["{title}"]')
            lines.append("    direction LR")
            for unit in sorted(by_level[level]):
                shape = (
                    f'["{self._label(unit)}"]'
                    if unit in self.scopes
                    else f'("{self._label(unit)}")'
                )
                lines.append(f"    {node_id(unit)}{shape}")
            lines.append("  end")

        for unit in units:
            for entry in self.imports.get(unit, []):
                if entry["from"] not in units:
                    continue
                lines.append(f"  {node_id(entry['from'])} --> {node_id(unit)}")

        for unit in units:
            cls = (
                "scope"
                if unit in self.scopes
                else ("parametrised" if self.params[unit] else "preset")
            )
            lines.append(f"  class {node_id(unit)} {cls};")
        return "\n".join(lines) + "\n"

    def consumers_of(self, preset: str) -> list[str]:
        """Every unit that reaches this preset, directly or through another.

        The inverse of `composition_of`: that answers "what does this scope pull
        in", this answers "who breaks if I change this preset".
        """

        return sorted(
            unit
            for unit in list(self.scopes) + self.presets
            if unit != preset and preset in self.reachable_from(unit)
        )

    def preset_mermaid(self, preset: str) -> str:
        """One preset and everything that consumes it.

        Edges keep the composition direction — preset -> importer — so the arrow
        still reads as "values flow this way", and the diagram is the same graph
        seen from the other end.
        """

        consumers = self.consumers_of(preset)
        units = [preset, *consumers]

        def node_id(unit: str) -> str:
            return "n_" + re.sub(r"[^A-Za-z0-9]", "_", unit.strip("/"))

        lines = [
            f"%% Consumers of {preset}. Generated by cfg/generate_plt_cfg_diagram.py.",
            "graph TD",
            "  classDef subject fill:#8250df,stroke:#4c1d95,color:#ffffff,font-weight:bold;",
            "  classDef scope fill:#1f6feb,stroke:#0b3d91,color:#ffffff;",
            "  classDef preset fill:#f6f8fa,stroke:#8b949e,color:#24292f;",
            '  subgraph SUBJ["preset"]',
            "    direction LR",
            f'    {node_id(preset)}("{self._label(preset)}")',
            "  end",
        ]
        direct = [u for u in consumers if any(e["from"] == preset for e in self.imports.get(u, []))]
        indirect = [u for u in consumers if u not in direct]
        for title, group in (("direct importers", direct), ("reached through another", indirect)):
            if not group:
                continue
            lines.append(f'  subgraph G{title.split()[0]}["{title}"]')
            lines.append("    direction LR")
            for unit in group:
                shape = (
                    f'["{self._label(unit)}"]'
                    if unit in self.scopes
                    else f'("{self._label(unit)}")'
                )
                lines.append(f"    {node_id(unit)}{shape}")
            lines.append("  end")

        for unit in units:
            for entry in self.imports.get(unit, []):
                if entry["from"] in units:
                    lines.append(f"  {node_id(entry['from'])} --> {node_id(unit)}")

        lines.append(f"  class {node_id(preset)} subject;")
        for unit in consumers:
            lines.append(f"  class {node_id(unit)} {'scope' if unit in self.scopes else 'preset'};")
        return "\n".join(lines) + "\n"

    def preset_consumers_text(self, preset: str) -> str:
        """The same answer as a flat list, for reading in a terminal."""

        consumers = self.consumers_of(preset)
        lines = [preset, ""]
        if not consumers:
            lines.append("  (no consumer — this preset is unreachable)")
            return "\n".join(lines) + "\n"
        for unit in consumers:
            how = (
                "direct"
                if any(e["from"] == preset for e in self.imports.get(unit, []))
                else "indirect"
            )
            kind = "scope" if unit in self.scopes else "preset"
            lines.append(f"  {unit:58} {kind:6} {how}")
        return "\n".join(lines) + "\n"

    def scope_tree(self, scope: str) -> str:
        """The same composition as an indented text tree, for reading in a terminal."""

        lines = [scope]

        def walk(unit: str, prefix: str, seen: tuple[str, ...]) -> None:
            entries = self.imports.get(unit, [])
            for index, entry in enumerate(entries):
                last = index == len(entries) - 1
                source = entry["from"]
                marker = "└── " if last else "├── "
                if source in seen:
                    lines.append(f"{prefix}{marker}{source}   (already composed)")
                    continue
                lines.append(f"{prefix}{marker}{source}")
                walk(source, prefix + ("    " if last else "│   "), (*seen, source))

        walk(scope, "", (scope,))
        return "\n".join(lines) + "\n"


def render_svg(mmd_path: Path, svg_path: Path, *, mmdc: str | None = None) -> None:
    # `--mmdc` may be a bare executable or a full command line, so that Mermaid
    # can be run through a package runner without a global install.
    command = (mmdc or "mmdc").split()
    if not shutil.which(command[0]):
        print(f"  mmdc not found; wrote Mermaid source only: {mmd_path}")
        return
    result = subprocess.run(
        [*command, "-i", str(mmd_path), "-o", str(svg_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"mmdc failed: {result.stderr.strip() or result.stdout.strip()}")
    print(f"  wrote {svg_path}")


def main() -> int:
    args = parse_args()
    graph = Graph(Path(args.plt_cfg_root))

    print(f"units: {len(graph.units)}  scopes: {len(graph.scopes)}  presets: {len(graph.presets)}")
    print(
        f"imports: {sum(len(v) for v in graph.imports.values())}  "
        f"parametrised presets: {sum(1 for u in graph.units if graph.params[u])}"
    )

    findings = graph.audit()
    if findings:
        print(f"\naudit findings: {len(findings)}")
        for kind, message in findings:
            print(f"  [{kind}] {message}")
    else:
        print("\naudit: no findings")

    if args.audit_only:
        return 1 if findings else 0

    out_dir = Path(args.out) if args.out else Path(args.plt_cfg_root) / "diagrams" / "composition"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nwriting one diagram per scope into {out_dir}")

    for scope in sorted(graph.scopes):
        # One folder per scope, mirroring the cfg tree, holding that scope's three
        # renderings of the same graph.
        scope_dir = out_dir / scope.strip("/")
        scope_dir.mkdir(parents=True, exist_ok=True)
        mmd_path = scope_dir / "composition.mmd"
        mmd_path.write_text(graph.scope_mermaid(scope), encoding="utf-8")
        (scope_dir / "composition.txt").write_text(graph.scope_tree(scope), encoding="utf-8")
        units = len(graph.composition_of(scope))
        print(f"  {scope:58} {units:2d} units")
        if not args.mmd_only:
            render_svg(mmd_path, mmd_path.with_suffix(".svg"), mmdc=args.mmdc)

    # The same graph from the other end: one diagram per PRESET, showing who
    # consumes it. A scope diagram answers "what does this pull in"; this answers
    # "who breaks if I change this".
    presets_dir = out_dir.parent / "presets"
    presets_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nwriting one diagram per preset into {presets_dir}")
    for preset in graph.presets:
        preset_dir = presets_dir / preset.strip("/")
        preset_dir.mkdir(parents=True, exist_ok=True)
        mmd_path = preset_dir / "consumers.mmd"
        mmd_path.write_text(graph.preset_mermaid(preset), encoding="utf-8")
        (preset_dir / "consumers.txt").write_text(
            graph.preset_consumers_text(preset), encoding="utf-8"
        )
        count = len(graph.consumers_of(preset))
        print(f"  {preset:58} {count:2d} consumers")
        if not args.mmd_only:
            render_svg(mmd_path, mmd_path.with_suffix(".svg"), mmdc=args.mmdc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
