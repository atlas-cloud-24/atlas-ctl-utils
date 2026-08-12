"""Drawing a preflight report as a text tree.

Presentation only, and separate from the reports it draws so that adding a
check never means touching the renderer — the tree shape comes from the report,
not from a branch per check."""

from engine.catalog import target_catalog

PREFLIGHT_RESULT_STATUSES = {
    "passed",
    "failed",
    "force_skipped",
    "not_applicable",
    "not_evaluated",
}


PREFLIGHT_SKIPPED_STATUSES = {
    "bypassed",
    "force_skipped",
    "not_applicable",
    "skipped",
}


def _preflight_status_tag(status: str) -> str:
    if status in PREFLIGHT_SKIPPED_STATUSES:
        return "[ skipped ⏭ ]"
    if status == "not_evaluated":
        return "[ not evaluated ⚠️ ]"
    if status == "partial":
        return "[ partial ⚠️ ]"
    marks = {"passed": "✅", "failed": "❌"}
    mark = marks.get(status)
    return f"[ {status} {mark} ]" if mark else f"[ {status} ]"


# ── report rendering as a `tree`-style ASCII tree ──────────────────────────
# every report is converted to nodes {"label", "children"} then rendered with
# └──/├──/│ connectors. Node labels already carry the status tag.
def _node(label: str, children: list | None = None) -> dict:
    return {"label": label, "children": children or []}


def _tree_child_lines(children: list, prefix: str) -> list[str]:
    lines: list[str] = []
    for index, child in enumerate(children):
        last = index == len(children) - 1
        lines.append(prefix + ("└── " if last else "├── ") + child["label"])
        lines.extend(_tree_child_lines(child["children"], prefix + ("    " if last else "│   ")))
    return lines


def _render_tree(root: dict) -> list[str]:
    return [root["label"]] + _tree_child_lines(root["children"], "")


def _report_node_label(report: dict) -> str:
    selection = report["selection"]
    label = f"{selection['kind']}: {selection['key']}"
    params = report.get("params") or {}
    if params:
        label += "  (" + ", ".join(f"{k}={v}" for k, v in sorted(params.items())) + ")"
    return f"{label} {_preflight_status_tag(report['status'])}"


def _nested_report_node(report: dict, leaf_builder) -> dict:
    node = _node(_report_node_label(report))
    if report.get("failure_reason"):
        # A not_evaluated node did not fail here — it just could not be checked
        label = "not evaluated" if report.get("status") == "not_evaluated" else "error"
        node["children"].append(_node(f"{label}: {report['failure_reason']}"))
    for child in report.get("children", []):
        node["children"].append(_nested_report_node(child, leaf_builder))
    node["children"].extend(leaf_builder(report))
    return node


def _identity_result_nodes(report: dict) -> list[dict]:
    nodes: list[dict] = []
    for result in report.get("results", []):
        status = result["status"]
        tag = _preflight_status_tag
        if "ctl_state_backend" in result:
            children: list[dict] = []
            backend_execution = target_catalog.TargetExecutionIdentity.describe(
                result.get("execution_identities")
            )
            if backend_execution:
                children.append(_node(f"execution_identity: {backend_execution} {tag(status)}"))
            if result.get("reason"):
                children.append(_node(f"reason: {result['reason']}"))
            nodes.append(
                _node(
                    f"ctl_state_backend: {result.get('ctl_state_backend') or '<none>'} {tag(status)}",
                    children,
                )
            )
            continue
        # Container row carries only passed/failed/not_evaluated; the identity
        # row keeps the raw status (bypassed/skipped/not-applicable count as passed)
        row_status = status if status in ("failed", "not_evaluated") else "passed"
        identity_key = (
            target_catalog.TargetExecutionIdentity.describe(result.get("execution_identities"))
            or "<unresolved>"
        )
        identity_children: list[dict] = []
        for path_node in result.get("provider_path") or []:
            display = (
                path_node.get("display")
                or path_node.get("cfg_key")
                or path_node.get("node_type")
                or "path"
            )
            pchildren = (
                [_node(f"error: {path_node['failure_reason']}")]
                if path_node.get("failure_reason")
                else []
            )
            identity_children.append(
                _node(f"{display} {tag(path_node.get('status', status))}", pchildren)
            )
        if result.get("reason"):
            reason = result["reason"]
            if status in PREFLIGHT_SKIPPED_STATUSES:
                reason = "execution identity was skipped for this run"
            identity_children.append(_node(f"reason: {reason}"))
        if result.get("failure_reason"):
            identity_children.append(_node(f"error: {result['failure_reason']}"))
        if result.get("blocked"):
            identity_children.append(_node(f"blocked: {result['blocked']}"))
        identity_node = _node(
            f"execution_identity: {identity_key} {tag(status)}", identity_children
        )
        # The identity is checked FOR this instance, so nest it under the instance
        if result.get("instance"):
            target_children = [_node(f"instance: {result['instance']}", [identity_node])]
        else:
            target_children = [identity_node]
        nodes.append(_node(f"target: {result['target_key']} {tag(row_status)}", target_children))
    return nodes


def _preflight_text_lines(report: dict) -> list[str]:
    return _render_tree(_nested_report_node(report, _identity_result_nodes))


def _policy_check_nodes(report: dict) -> list[dict]:
    tag = _preflight_status_tag

    def check_node(check: dict) -> dict:
        children: list[dict] = []
        if check.get("failure_reason"):
            children.append(_node(f"error: {check['failure_reason']}"))
        # Detail is provider-authored render text: a flat list of
        # lines, or a mapping of provider -> lines for the per-provider check.
        detail = check.get("detail")
        if isinstance(detail, dict):
            for name in sorted(detail):
                children.append(_node(f"provider: {name}", [_node(line) for line in detail[name]]))
        elif detail:
            children.extend(_node(line) for line in detail)
        return _node(f"check: {check['name']} {tag(check['status'])}", children)

    nodes = [check_node(check) for check in report.get("checks", [])]
    for target in report.get("targets", []):
        nodes.append(
            _node(
                f"target: {target['target_key']} {tag(target['status'])}",
                [check_node(check) for check in target.get("checks", [])],
            )
        )
    return nodes


def _ctl_policy_preflight_text_lines(report: dict) -> list[str]:
    return _render_tree(_nested_report_node(report, _policy_check_nodes))


def _cfg_validation_text_lines(report: dict) -> list[str]:
    root = _node(f"cfg validation {_preflight_status_tag(report['status'])}")
    gate = report.get("gate") or {}
    if gate:
        gate_children = [_node(f"reason: {gate['reason']}")] if gate.get("reason") else []
        root["children"].append(
            _node(
                f"full cfg validation gate {_preflight_status_tag(gate['status'])}",
                gate_children,
            )
        )
    for finding in report.get("findings", []):
        children = [_node(f"error: {finding['error']}")] if finding.get("error") else []
        root["children"].append(
            _node(
                f"{finding['cfg_path']} {_preflight_status_tag(finding['status'])}",
                children,
            )
        )
    return _render_tree(root)


def _target_cfg_result_nodes(report: dict) -> list[dict]:
    tag = _preflight_status_tag
    nodes: list[dict] = []
    for result in report.get("results", []):
        children: list[dict] = []
        if result.get("instance"):
            children.append(_node(f"instance: {result['instance']}"))
        for row in result.get("rows", []):
            children.append(_node(f"{row['name']} {tag(row['status'])}"))
        if result.get("failure_reason"):
            children.append(_node(f"error: {result['failure_reason']}"))
        nodes.append(_node(f"target: {result['target_key']} {tag(result['status'])}", children))
    return nodes


def _target_cfg_validation_text_lines(report: dict) -> list[str]:
    return _render_tree(_nested_report_node(report, _target_cfg_result_nodes))
