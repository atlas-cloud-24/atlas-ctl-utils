#!/usr/bin/env python3
"""Generate Mermaid and SVG dependency diagrams from Atlas ctl cfg."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


ACTION_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
RUNTIME_REF_RE = re.compile(r"\$\{[^{}]+\}")
DIAGRAM_VIEWS = (
    "general",
    "sources",
    "ctl_state_buckets",
    "execution_identities",
)
GROUPS = {
    "fan_out": ("Fan-outs", "fanout"),
    "workflow": ("Workflows", "workflow"),
    "target": ("Targets", "target"),
    "source": ("Sources", "source"),
    "identity": ("Execution identities", "identity"),
    "backend": ("Ctl-state namespaces", "backend"),
}
VIEW_GROUP_KINDS = {
    "general": ("fan_out", "workflow", "target"),
    "sources": ("target", "source"),
    "ctl_state_buckets": ("target", "backend"),
    "execution_identities": ("target", "identity"),
}


@dataclass(frozen=True, order=True)
class Edge:
    source: str
    target: str
    label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ctl-cfg-root", required=True, help="Ctl cfg directory to read and write into.")
    parser.add_argument(
        "--action",
        action="append",
        dest="actions",
        help="Generate this action; repeatable. By default each available action is generated.",
    )
    parser.add_argument(
        "--view",
        action="append",
        dest="views",
        choices=DIAGRAM_VIEWS,
        help="Generate this view; repeatable. By default all four views are generated.",
    )
    parser.add_argument(
        "--font-size",
        type=int,
        default=28,
        help="Diagram font size in pixels (default: 28).",
    )
    parser.add_argument("--mmdc", help="Path or command name for Mermaid CLI (default: mmdc from PATH).")
    parser.add_argument("--mmd-only", action="store_true", help="Write Mermaid source without rendering SVG.")
    return parser.parse_args()


def node_id(kind: str, key: str) -> str:
    digest = hashlib.sha1(f"{kind}:{key}".encode("utf-8")).hexdigest()[:12]
    return f"n_{kind}_{digest}"


def mermaid_label(*lines: object) -> str:
    return "<br/>".join(html.escape(str(line), quote=True) for line in lines if str(line))


def mermaid_edge_label(value: str) -> str:
    return html.escape(value, quote=True)


def wrap_label(value: object, *, max_length: int = 32) -> list[str]:
    text = str(value)
    if len(text) <= max_length:
        return [text]
    tokens = re.split(r"(?<=[/_-])", text)
    lines: list[str] = []
    current = ""
    for token in tokens:
        if current and len(current) + len(token) > max_length:
            lines.append(current)
            current = token
        else:
            current += token
    if current:
        lines.append(current)
    return lines


def reference_candidates(reference: str, keys: set[str], *, label: str) -> list[str]:
    if not isinstance(reference, str) or not reference.strip():
        raise RuntimeError(f"{label} must be a non-empty string")
    reference = reference.strip()
    if "${" not in reference:
        if reference not in keys:
            raise RuntimeError(f"{label} references missing key {reference!r}")
        return [reference]

    pattern_parts = []
    cursor = 0
    for match in RUNTIME_REF_RE.finditer(reference):
        pattern_parts.append(re.escape(reference[cursor : match.start()]))
        pattern_parts.append(r"[^/]+")
        cursor = match.end()
    pattern_parts.append(re.escape(reference[cursor:]))
    pattern = re.compile("^" + "".join(pattern_parts) + "$")
    matches = sorted(key for key in keys if pattern.fullmatch(key))
    if not matches:
        raise RuntimeError(f"{label} dynamic reference {reference!r} matches no keys")
    return matches


def selected_actions(
    workflows: dict,
    targets: dict,
    requested: list[str] | None,
) -> list[str]:
    available = set(workflows) | set(targets)
    if not requested:
        return sorted(available)
    duplicates = sorted({action for action in requested if requested.count(action) > 1})
    if duplicates:
        raise RuntimeError(f"--action values must be unique; duplicates: {duplicates}")
    unknown = sorted(set(requested) - available)
    if unknown:
        raise RuntimeError(f"unknown actions {unknown}; available: {sorted(available)}")
    return requested


def build_diagram(
    ctl_cfg_root: Path,
    *,
    action: str,
    view: str,
    font_size: int = 28,
) -> str:
    if view not in DIAGRAM_VIEWS:
        raise RuntimeError(f"unknown diagram view {view!r}; available: {list(DIAGRAM_VIEWS)}")
    fan_outs = common.collect_resource(ctl_cfg_root, "fan_outs")

    # §Phase 33: targets/workflows are declared once with an `actions:`
    # allowlist; rebuild the per-action view this diagram renders from it.
    def _by_action(flat: dict) -> dict:
        per_action: dict = {}
        for name, entry in flat.items():
            if not isinstance(entry, dict):
                continue
            for entry_action in entry.get("actions") or []:
                per_action.setdefault(entry_action, {})[name] = entry
        return per_action

    workflows = _by_action(
        common.collect_resource(ctl_cfg_root, "workflows", entry_depth=1)
    )
    targets = _by_action(
        common.collect_resource(ctl_cfg_root, "targets", entry_depth=1)
    )
    sources = common.collect_resource(ctl_cfg_root, "target_sources")
    identities = common.collect_resource(ctl_cfg_root, "execution_identities")
    backends = common.collect_resource(ctl_cfg_root, "ctl_state_backends")
    actions = selected_actions(workflows, targets, [action])
    focused = True

    nodes: dict[str, list[tuple[str, str]]] = {
        "fan_out": [],
        "workflow": [],
        "target": [],
        "source": [],
        "identity": [],
        "backend": [],
    }
    edges: set[Edge] = set()

    for key in sorted(fan_outs):
        nodes["fan_out"].append((node_id("fanout", key), mermaid_label(key)))
    for action in actions:
        for key in sorted((workflows.get(action) or {})):
            qualified = f"{action}:{key}"
            nodes["workflow"].append(
                (node_id("workflow", qualified), mermaid_label(action, key))
            )
        for key in sorted((targets.get(action) or {})):
            qualified = f"{action}:{key}"
            nodes["target"].append((node_id("target", qualified), mermaid_label(action, key)))
    for key, cfg in sorted(sources.items()):
        repo_url = cfg.get("repo_url", "") if isinstance(cfg, dict) else ""
        repo_name = str(repo_url).rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")
        nodes["source"].append((node_id("source", key), mermaid_label(key, repo_name)))
    for key, cfg in sorted(identities.items()):
        identity_kind = "group" if isinstance(cfg, dict) and "members" in cfg else "concrete"
        provider = cfg.get("provider", "") if isinstance(cfg, dict) else ""
        nodes["identity"].append(
            (
                node_id("identity", key),
                mermaid_label(*wrap_label(key), identity_kind, provider),
            )
        )
    for key, cfg in sorted(backends.items()):
        if not isinstance(cfg, dict):
            raise RuntimeError(f"ctl_state_backends.{key} must be a mapping")
        implementation = "/".join(
            str(value) for value in (cfg.get("provider"), cfg.get("backend_type")) if value
        )
        bucket_name = cfg.get("bucket_name", "")
        nodes["backend"].append(
            (
                node_id("backend", key),
                mermaid_label(key, implementation, *wrap_label(bucket_name)),
            )
        )

    workflow_keys_by_action = {
        action: set((workflows.get(action) or {})) for action in actions
    }
    target_keys_by_action = {action: set((targets.get(action) or {})) for action in actions}

    for fan_out_key, fan_out_cfg in sorted(fan_outs.items()):
        if not isinstance(fan_out_cfg, dict) or not isinstance(fan_out_cfg.get("runs"), list):
            raise RuntimeError(f"fan_outs.{fan_out_key}.runs must be a list")
        source_id = node_id("fanout", fan_out_key)
        for index, run in enumerate(fan_out_cfg["runs"]):
            if not isinstance(run, dict):
                raise RuntimeError(f"fan_outs.{fan_out_key}.runs[{index}] must be a mapping")
            has_workflow = "workflow_key" in run
            has_target = "target_key" in run
            if has_workflow == has_target:
                raise RuntimeError(
                    f"fan_outs.{fan_out_key}.runs[{index}] must declare exactly one workflow_key or target_key"
                )
            ref_field = "workflow_key" if has_workflow else "target_key"
            ref = run[ref_field]
            catalogs = workflow_keys_by_action if has_workflow else target_keys_by_action
            kind = "workflow" if has_workflow else "target"
            matches = [(action, ref) for action, keys in catalogs.items() if ref in keys]
            if not matches:
                if focused:
                    continue
                raise RuntimeError(f"fan_outs.{fan_out_key}.runs[{index}].{ref_field} references missing key {ref!r}")
            for action, key in matches:
                edges.add(Edge(source_id, node_id(kind, f"{action}:{key}"), kind))

    for action in actions:
        action_workflows = workflows.get(action) or {}
        action_targets = targets.get(action) or {}
        for workflow_key, workflow_cfg in sorted(action_workflows.items()):
            if not isinstance(workflow_cfg, dict):
                raise RuntimeError(f"workflows.{action}.{workflow_key} must be a mapping")
            source_id = node_id("workflow", f"{action}:{workflow_key}")
            for imported in workflow_cfg.get("import_workflow_keys") or []:
                if imported not in action_workflows:
                    raise RuntimeError(
                        f"workflows.{action}.{workflow_key}.import_workflow_keys references missing key {imported!r}"
                    )
                edges.add(
                    Edge(source_id, node_id("workflow", f"{action}:{imported}"), "imports")
                )
            for target_key in workflow_cfg.get("target_keys") or []:
                if target_key not in action_targets:
                    raise RuntimeError(
                        f"workflows.{action}.{workflow_key}.target_keys references missing key {target_key!r}"
                    )
                edges.add(Edge(source_id, node_id("target", f"{action}:{target_key}"), ""))

        for target_key, target_cfg in sorted(action_targets.items()):
            if not isinstance(target_cfg, dict):
                raise RuntimeError(f"targets.{action}.{target_key} must be a mapping")
            source_id = node_id("target", f"{action}:{target_key}")
            source_key = target_cfg.get("source_key")
            if source_key is not None:
                for match in reference_candidates(
                    source_key,
                    set(sources),
                    label=f"target {action}:{target_key} source_key",
                ):
                    edges.add(Edge(source_id, node_id("source", match), ""))
            identity_key = target_cfg.get("execution_identity_key")
            if identity_key is not None:
                for match in reference_candidates(
                    identity_key,
                    set(identities),
                    label=f"target {action}:{target_key} execution_identity_key",
                ):
                    edges.add(Edge(source_id, node_id("identity", match), ""))
            # Namespace membership is invocation-scoped, not a target property.
            # A static diagram therefore shows every namespace this target may
            # use; runtime selectors resolve exactly one before execution.
            for backend_key in sorted(backends):
                edges.add(
                    Edge(
                        source_id,
                        node_id("backend", backend_key),
                        "invocation-selected",
                    )
                )

    connected_node_ids = {identifier for edge in edges for identifier in (edge.source, edge.target)}
    backend_keys = {
        key
        for key in backends
        if not focused or node_id("backend", key) in connected_node_ids
    }
    for backend_key in sorted(backend_keys):
        backend_cfg = backends[backend_key]
        operation_identities = backend_cfg.get("execution_identity_keys") or {}
        if not isinstance(operation_identities, dict):
            raise RuntimeError(
                f"ctl_state_backends.{backend_key}.execution_identity_keys must be a mapping"
            )
        for operation, identity_key in sorted(operation_identities.items()):
            for match in reference_candidates(
                identity_key,
                set(identities),
                label=(
                    f"ctl_state_backends.{backend_key}.execution_identity_keys.{operation}"
                ),
            ):
                edges.add(
                    Edge(
                        node_id("backend", backend_key),
                        node_id("identity", match),
                        f"{operation} identity",
                    )
                )

    connected_node_ids = {identifier for edge in edges for identifier in (edge.source, edge.target)}
    pending_identity_keys = {
        key
        for key in identities
        if not focused or node_id("identity", key) in connected_node_ids
    }
    processed_identity_keys: set[str] = set()
    while pending_identity_keys:
        identity_key = min(pending_identity_keys)
        pending_identity_keys.remove(identity_key)
        if identity_key in processed_identity_keys:
            continue
        processed_identity_keys.add(identity_key)
        identity_cfg = identities[identity_key]
        if not isinstance(identity_cfg, dict) or "members" not in identity_cfg:
            continue
        members = identity_cfg.get("members")
        if not isinstance(members, list):
            raise RuntimeError(f"execution_identities.{identity_key}.members must be a list")
        for index, member in enumerate(members):
            member_key = member.get("identity_key") if isinstance(member, dict) else None
            for match in reference_candidates(
                member_key,
                set(identities),
                label=f"execution_identities.{identity_key}.members[{index}].identity_key",
            ):
                edges.add(
                    Edge(
                        node_id("identity", identity_key),
                        node_id("identity", match),
                        "member",
                    )
                )
                pending_identity_keys.add(match)

    if focused:
        retained_node_ids = {
            identifier for edge in edges for identifier in (edge.source, edge.target)
        }
        retained_node_ids.update(
            node_id(kind, f"{action}:{key}")
            for action in actions
            for kind, catalog in (
                ("workflow", workflows.get(action) or {}),
                ("target", targets.get(action) or {}),
            )
            for key in catalog
        )
        for kind in nodes:
            nodes[kind] = [
                node for node in nodes[kind] if node[0] in retained_node_ids
            ]

    node_kind_by_id = {
        identifier: kind
        for kind, kind_nodes in nodes.items()
        for identifier, _ in kind_nodes
    }
    view_group_kinds = VIEW_GROUP_KINDS[view]
    if view == "execution_identities":
        view_edges = {
            edge
            for edge in edges
            if node_kind_by_id.get(edge.source) == "target"
            and node_kind_by_id.get(edge.target) == "identity"
        }
        pending_identity_ids = {edge.target for edge in view_edges}
        processed_identity_ids: set[str] = set()
        while pending_identity_ids:
            identity_id = min(pending_identity_ids)
            pending_identity_ids.remove(identity_id)
            if identity_id in processed_identity_ids:
                continue
            processed_identity_ids.add(identity_id)
            member_edges = {
                edge
                for edge in edges
                if edge.source == identity_id
                and node_kind_by_id.get(edge.target) == "identity"
            }
            view_edges.update(member_edges)
            pending_identity_ids.update(edge.target for edge in member_edges)
    else:
        allowed_kinds = set(view_group_kinds)
        view_edges = {
            edge
            for edge in edges
            if node_kind_by_id.get(edge.source) in allowed_kinds
            and node_kind_by_id.get(edge.target) in allowed_kinds
        }

    retained_view_node_ids = {
        identifier
        for edge in view_edges
        for identifier in (edge.source, edge.target)
    }
    if view == "general":
        retained_view_node_ids.update(
            identifier
            for kind in view_group_kinds
            for identifier, _ in nodes[kind]
        )
    else:
        retained_view_node_ids.update(identifier for identifier, _ in nodes["target"])

    view_nodes = {
        kind: [
            node for node in nodes[kind] if node[0] in retained_view_node_ids
        ]
        for kind in view_group_kinds
    }
    lines = [
        "%% Generated by atlas-ctl-utils/cfg/generate_ctl_cfg_diagram.py; do not edit.",
        "%%{init: "
        + json.dumps(
            {
                "themeVariables": {"fontSize": f"{font_size}px"},
                "flowchart": {
                    "defaultRenderer": "elk",
                    "curve": "basis",
                    "nodeSpacing": 36,
                    "rankSpacing": 90,
                },
            },
            separators=(",", ":"),
        )
        + "}%%",
        "flowchart LR",
        "  classDef fanout fill:#e8f0fe,stroke:#345995,color:#111;",
        "  classDef workflow fill:#e7f6ec,stroke:#287a45,color:#111;",
        "  classDef target fill:#fff4d6,stroke:#9a6b00,color:#111;",
        "  classDef source fill:#f3f3f3,stroke:#555,color:#111;",
        "  classDef identity fill:#fde8e8,stroke:#a33a3a,color:#111;",
        "  classDef backend fill:#e8f7f5,stroke:#21756d,color:#111;",
    ]
    for kind in view_group_kinds:
        title, class_name = GROUPS[kind]
        lines.append(f'  subgraph sg_{kind}["{title}"]')
        lines.append("    direction TB")
        for identifier, label in view_nodes[kind]:
            lines.append(f'    {identifier}["{label}"]:::{class_name}')
        lines.append("  end")
    for edge in sorted(view_edges):
        if edge.label:
            lines.append(
                f'  {edge.source} -->|"{mermaid_edge_label(edge.label)}"| {edge.target}'
            )
        else:
            lines.append(f"  {edge.source} --> {edge.target}")
    return "\n".join(lines) + "\n"


def render_svg(mmd_path: Path, svg_path: Path, *, mmdc: str | None = None) -> None:
    executable = shutil.which(mmdc or "mmdc")
    if executable is None:
        raise RuntimeError(
            "Mermaid CLI 'mmdc' was not found; install @mermaid-js/mermaid-cli "
            "or pass --mmdc <path>. The .mmd file was still written."
        )
    with tempfile.NamedTemporaryFile(
        prefix=f".{svg_path.stem}.",
        suffix=".svg",
        dir=svg_path.parent,
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)
    temporary_path.unlink()
    try:
        result = subprocess.run(
            [
                executable,
                "--input",
                str(mmd_path),
                "--output",
                str(temporary_path),
                "--backgroundColor",
                "transparent",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"mmdc failed: {result.stderr.strip() or result.stdout.strip()}")
        if not temporary_path.is_file() or temporary_path.stat().st_size == 0:
            raise RuntimeError(f"mmdc produced no SVG: {svg_path}")
        temporary_path.replace(svg_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    ctl_cfg_root = Path(args.ctl_cfg_root).expanduser().resolve()
    if not ctl_cfg_root.is_dir():
        raise RuntimeError(f"ctl cfg root not found: {ctl_cfg_root}")
    if not 12 <= args.font_size <= 48:
        raise RuntimeError("--font-size must be between 12 and 48")
    # §Phase 33: available actions come from the flat entries' `actions:` allowlists.
    available_actions: set[str] = set()
    for kind in ("workflows", "targets"):
        for entry in common.collect_resource(ctl_cfg_root, kind, entry_depth=1).values():
            if isinstance(entry, dict):
                available_actions.update(entry.get("actions") or [])
    actions = selected_actions(
        {action: {} for action in available_actions}, {}, args.actions
    )
    unsafe_actions = [
        action for action in actions if not ACTION_KEY_RE.fullmatch(action)
    ]
    if unsafe_actions:
        raise RuntimeError(
            "diagram generation requires filename-safe action keys; "
            f"invalid: {unsafe_actions}"
        )
    views = args.views or list(DIAGRAM_VIEWS)
    duplicate_views = sorted({view for view in views if views.count(view) > 1})
    if duplicate_views:
        raise RuntimeError(f"--view values must be unique; duplicates: {duplicate_views}")

    diagrams_root = ctl_cfg_root / "diagrams"
    diagrams_root.mkdir(exist_ok=True)
    for action in actions:
        legacy_basenames = [
            f"ctl-cfg-architecture-{action}",
            *(
                f"ctl-cfg-architecture-{action}-{legacy_view}"
                for legacy_view in (
                    "flow",
                    "sources",
                    "ctl-state",
                    "execution-identities",
                )
            ),
        ]
        for legacy_basename in legacy_basenames:
            (diagrams_root / f"{legacy_basename}.mmd").unlink(missing_ok=True)
            (diagrams_root / f"{legacy_basename}.svg").unlink(missing_ok=True)
        for view in views:
            output_dir = diagrams_root / action / view
            output_dir.mkdir(parents=True, exist_ok=True)
            mmd_path = output_dir / "diagram.mmd"
            svg_path = output_dir / "diagram.svg"
            mmd_path.write_text(
                build_diagram(
                    ctl_cfg_root,
                    action=action,
                    view=view,
                    font_size=args.font_size,
                ),
                encoding="utf-8",
            )
            print(f"wrote {mmd_path}")
            if not args.mmd_only:
                render_svg(mmd_path, svg_path, mmdc=args.mmdc)
                print(f"wrote {svg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
