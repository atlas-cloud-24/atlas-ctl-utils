"""Composing an ordered run out of declared targets.

A workflow is an ORDERED sequence, not a graph: it has no branching and no
concurrency, so a member that fails ends the run. A fan-out expands one
declaration into many runs."""

import hashlib
import json
import logging
import shutil
import sys
from pathlib import Path

import yaml

from engine.catalog import target_catalog
from engine.cfg import references as cfg_references
from engine.cfg import resources as cfg_resources
from engine.execution import adapters as execution_adapters
from engine.execution import run_context as execution_run_context
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.run import selectors as run_selectors
from engine.state import run_store as state_run_store


def _resolve_entry_references(entries: list, *, label: str) -> list:
    """A member list: a target reference, or a mapping carrying one.

    `{key, action}` appends and `{key, after|before}` places; both address a
    target, and an anchor names the WORKFLOW it points into as well as the target
    it sits beside.
    """

    resolved = []
    for value in entries or []:
        if not isinstance(value, dict):
            resolved.append(cfg_references.resolve(value, "targets", label=label))
            continue
        entry = dict(value)
        entry["key"] = cfg_references.resolve(entry["key"], "targets", label=f"{label}.key")
        for position in ("after", "before"):
            anchor = entry.get(position)
            if isinstance(anchor, dict):
                entry[position] = {
                    "workflow": cfg_references.resolve(
                        anchor["workflow"], "workflows", label=f"{label}.{position}.workflow"
                    ),
                    "key": cfg_references.resolve(
                        anchor["key"], "targets", label=f"{label}.{position}.key"
                    ),
                }
        resolved.append(entry)
    return resolved


def _resolve_member_block(block, *, label: str):
    if not isinstance(block, dict):
        return block
    if "members" in block:
        return {
            **block,
            "members": [
                {
                    **member,
                    "keys": _resolve_entry_references(member.get("keys"), label=f"{label}.members"),
                }
                for member in block["members"]
            ],
        }
    return {**block, "keys": _resolve_entry_references(block.get("keys"), label=label)}


class WorkflowCatalog:
    """Declared workflows, and the cfg one run composes from them."""

    @staticmethod
    def load(ctl_cfg_root: Path) -> dict:
        """Every declared workflow, with its cross-references resolved to bare keys."""

        workflows = cfg_resources.collect_resource(ctl_cfg_root, "workflows", entry_depth=1)
        resolved: dict = {}
        for name, entry in workflows.items():
            if not isinstance(entry, dict):
                resolved[name] = entry
                continue
            body = dict(entry)
            if body.get("import_workflows") is not None:
                body["import_workflows"] = cfg_references.resolve_each(
                    body["import_workflows"],
                    "workflows",
                    label=f"workflow {name!r} import_workflows",
                )
            for field in ("targets", "insert_targets"):
                if field in body:
                    body[field] = _resolve_member_block(
                        body[field], label=f"workflow {name!r} {field}"
                    )
            resolved[name] = body
        return resolved

    @staticmethod
    def workflow_cfg(
        ctl_cfg_root: Path,
        ctl_profile: str,
        action: str | None,
        workflow_name: str,
        execution_context: dict[str, object],
    ) -> dict:
        """Load a content-key workflow: `workflows.<name>` (imports + selectors).

        workflows are declared ONCE with a required `actions:` allowlist;
        the action gates availability. `targets` may be members-shaped (dispatch
        by `execution_context.ctl.action`) when the apply-family composition differs
        per action. Expands `import_workflows` (ordered, recursive) then the
        workflow's own `targets`; applies `selectors` (intersected through
        imports). The workflow name is an opaque key (slashes are cosmetic).

        Resolution COLLAPSES the declaration to the one branch this run selected, so
        the returned workflow carries a flat `targets` list with `default_action`
        beside it. Cfg declares branches; a resolved run has exactly one.
        """

        workflows = WorkflowCatalog.load(ctl_cfg_root)
        if workflow_name not in workflows:
            raise RuntimeError(f"❌ workflow {workflow_name!r} not found")
        WorkflowCatalog.validate_actions_declared(workflows)
        # WHICH workflows are resolved against this run's context, and which are only
        # checked for shape. The two checks depend on different things:
        #
        #   structural            operation-INDEPENDENT — a malformed declaration is
        #                         broken whatever runs, so every workflow is checked
        #   branch resolution     operation-DEPENDENT — `None` means "this workflow
        #                         does not apply to this operation", which is an
        #                         error only for the one that was ASKED for
        #
        # Resolving the whole catalog conflated them: one selector value on
        # `env/seed` failed on `env/tech_jobs`, which declares plan and provision and
        # no destroy. That is not a defect — member selectors are exactly how a
        # workflow says which operations it applies to — and it is not reachable by
        # --force-skip-full-cfg-validation-gate either, because this runs before any
        # gate exists.
        run_closure = WorkflowImports.closure(workflows, workflow_name)
        resolved_workflows: dict = {}
        for name, wf in workflows.items():
            if not isinstance(wf, dict):
                raise RuntimeError(f"❌ workflow {name!r} must be a mapping")
            # A workflow declares no allowlist. Its member selectors decide
            # when it applies, and each member's declared action decides what runs —
            # an `operations:` list restated the selectors and could contradict them.
            # `workflow_target_branches` is the one place that reads the declared
            # shape, so the workflow-level `default_action` refusal reaches every
            # caller rather than only the ones that remembered to check.
            WorkflowCatalog.target_branches(wf, name=name)
            # Validate every operation declaration's shape even when this run does
            # not use it. A workflow may omit operation when it has nothing useful
            # to add to status beyond its resolved target actions.
            if "operation" in wf:
                run_selectors.resolve_scalar_member(
                    wf["operation"], None, label=f"workflow {name!r} operation"
                )
            if name not in run_closure:
                # Shape-checked above; nothing else about it is this run's business.
                continue
            operation = (
                run_selectors.resolve_scalar_member(
                    wf["operation"],
                    execution_context,
                    label=f"workflow {name!r} operation",
                )
                if name == workflow_name and "operation" in wf
                else None
            )
            targets = wf.get("targets")
            if isinstance(targets, dict) and "members" in targets:
                member = run_selectors.resolve_list_member(
                    targets,
                    execution_context,
                    value_field="keys",
                    label=f"workflow {name!r} targets",
                    extra_fields=("default_action",),
                )
                if member is None:
                    raise RuntimeError(
                        f"❌ workflow {name!r} members-shaped targets did not "
                        "resolve for this execution context"
                    )
                # The member's declared default reaches every entry it
                # carries, so a list going one direction states its verb once.
                wf = {
                    **wf,
                    "targets": list(member["keys"]),
                    "default_action": run_selectors.resolve_default_action(
                        member.get("default_action"),
                        execution_context,
                        label=f"workflow {name!r} member",
                    ),
                    "member_selectors": member.get("selectors"),
                }
            else:
                wf = {
                    **wf,
                    "targets": list((targets or {}).get("keys") or []),
                    "default_action": run_selectors.resolve_default_action(
                        (targets or {}).get("default_action"),
                        execution_context,
                        label=f"workflow {name!r}",
                    ),
                }
            wf = {
                **wf,
                "insert_targets": WorkflowImports.resolve_inserts(wf, execution_context, name=name),
            }
            if operation is not None:
                wf["operation"] = operation
            resolved_workflows[name] = wf
        if workflow_name not in resolved_workflows:
            raise RuntimeError(
                f"❌ workflow {workflow_name!r} does not resolve for this execution context"
            )

        effective_selectors = run_selectors.workflow_effective_selectors(
            resolved_workflows, workflow_name
        )
        if not run_selectors.selector_matches(
            effective_selectors,
            execution_context,
            label=f"workflow {workflow_name}",
        ):
            raise RuntimeError(
                f"❌ workflow {workflow_name} is not available for "
                f"runtime selectors {execution_context} (selectors {effective_selectors})"
            )

        target_runs = WorkflowImports.expand(resolved_workflows, workflow_name)
        resolved = resolved_workflows[workflow_name]
        cfg = {
            "meta": {"name": workflow_name},
            "target_runs": target_runs,
        }
        if resolved.get("operation"):
            cfg["operation"] = resolved["operation"]
        # The matched member's declared default and its selector block travel
        # with the resolved workflow — the run record carries them, and returning only
        # meta + target_runs silently dropped both. the instance params
        # for the same reason: a field the caller needs must survive resolution.
        for field in ("default_action", "member_selectors", "workflow_instance_params"):
            if resolved.get(field):
                cfg[field] = resolved[field]
        return cfg

    @staticmethod
    def procedure_cfg(
        ctl_cfg_root: Path,
        action: str,
        *,
        source: str,
        ref: str,
        domain_name: str,
        procedure: str,
        execution_provider: str | None = None,
        execution_account: str | None = None,
        execution_role: str | None = None,
        action_role_class: str | None = None,
    ) -> tuple[dict, dict]:
        """Build a one-target cfg for a synthetic repo-local procedure run.

        The synthetic target is composed directly from CLI args and need not exist
        in targets/<action>/. Synthetic runs are local-only and do not publish ctl state.
        """
        target_sources = cfg_resources.collect_resource(ctl_cfg_root, "target_sources")
        # A synthetic procedure target names a DOMAIN and takes the
        # whole of it — the operator is debugging one step, not authoring a contract.
        domain = str(domain_name).strip().strip("/")
        execution_run_context.validate_domain_value(
            execution_run_context.load_domain_registry(ctl_cfg_root),
            domain,
            label="synthetic procedure target domain",
        )
        resolved = {
            "source": source,
            "ref": ref,
            "procedure": procedure,
            "domains": [domain],
            "cfg_keys": {domain: ["*"]},
            "allowed_actions": [action],
            # Synthetic procedures never publish reusable target state, but they use
            # the same resolved target shape and therefore state that policy explicitly.
            "committed_result_reuse": {action: False},
        }
        if execution_provider:
            # The synthetic target gets the same execution_identity block a declared
            # target has; a single --execution-role is bound under the key that
            # provider uses for this action — the engine asks, it does not decide.
            role_class = action_role_class or execution_adapters.get_adapter(
                execution_provider
            )._action_role_key(action, label="synthetic procedure target")
            resolved["execution_identities"] = target_catalog.TargetExecutionIdentity.validate_all(
                {
                    "provider": execution_provider,
                    "account": execution_account,
                    "roles": {role_class: execution_role},
                },
                label="synthetic procedure target",
            )
        name = "procedure"
        action_cfg = {"target_sources": target_sources, "targets": {name: resolved}}
        workflow_cfg = {
            "meta": {"name": f"procedure/{source}/{procedure}", "action": action},
            "target_runs": [name],
        }
        return workflow_cfg, action_cfg

    @staticmethod
    def validate_actions_declared(workflows: dict) -> None:
        """Every workflow entry must be able to resolve an ACTION.

        A list with no `default_action` whose entries carry no `action:` is not
        runnable — the engine cannot know what to do with those targets. This is a cfg
        gate rather than a run-time surprise, so a workflow that could never run is
        refused when the configuration is validated.
        """

        for name, workflow in (workflows or {}).items():
            if not isinstance(workflow, dict):
                continue
            for entries, default_action in WorkflowCatalog.target_branches(workflow, name=name):
                if default_action:
                    is_reference = (
                        isinstance(default_action, str)
                        and default_action.startswith("${")
                        and default_action.endswith("}")
                    )
                    if not is_reference and default_action not in run_actions.WORKFLOW_ACTIONS:
                        raise RuntimeError(
                            f"❌ workflow {name!r} default_action {default_action!r} must "
                            f"be one of {sorted(run_actions.WORKFLOW_ACTIONS)}; "
                            "maintenance uses the maintenance runner"
                        )
                for entry in entries:
                    entry_action = entry.get("action") if isinstance(entry, dict) else None
                    if (
                        entry_action is not None
                        and entry_action not in run_actions.WORKFLOW_ACTIONS
                    ):
                        raise RuntimeError(
                            f"❌ workflow {name!r} member action {entry_action!r} must be "
                            f"one of {sorted(run_actions.WORKFLOW_ACTIONS)}; maintenance "
                            "uses the maintenance runner"
                        )
                if default_action:
                    continue
                bare = [
                    entry
                    for entry in entries
                    if not (isinstance(entry, dict) and entry.get("action"))
                ]
                if bare:
                    raise RuntimeError(
                        f"❌ workflow {name!r}: {bare} have no action and the list "
                        "declares no default_action, so the engine cannot know how to "
                        "run them. Declare `default_action:` for the list — a literal "
                        "action, a consumer execution-param reference, or `action:` "
                        "beneath each key"
                    )

    @staticmethod
    def member_actions(workflow_cfg: dict) -> set[str]:
        """Every action a workflow's member entries ask of their targets."""
        actions: set[str] = set()
        for entry in (workflow_cfg or {}).get("target_runs") or []:
            if isinstance(entry, dict) and entry.get("action"):
                actions.add(str(entry["action"]))
        return actions

    @staticmethod
    def representative_action(workflow_cfg: dict) -> str:
        """Internal action used by state APIs after a workflow composition resolves.

        Public workflow status reports effect + exact actions. This representative
        exists only to select the run's internal state channel before its recorded
        composition is available.
        """

        actions = WorkflowCatalog.member_actions(workflow_cfg)
        if workflow_cfg.get("default_action"):
            actions.add(str(workflow_cfg["default_action"]))
        groups = {run_actions.action_group(action) for action in actions}
        if not groups:
            raise RuntimeError("❌ workflow resolves no target actions")
        if run_actions.Group.MAINTENANCE in groups:
            raise RuntimeError(
                "❌ workflow members cannot use maintenance; use the maintenance runner"
            )
        group = next(
            group for group in run_actions.WORKFLOW_STATE_GROUP_PRECEDENCE if group in groups
        )
        return str(run_actions.group_representative_action(group))

    @staticmethod
    def target_branches(workflow: dict, *, name: str) -> list[tuple[list, object]]:
        """The `(keys, default_action)` pairs a workflow declares, one per branch.

        `targets` is ALWAYS a mapping, and `default_action` lives INSIDE it — beside
        the `keys` list it governs in the single-branch form, and inside each member
        when the list branches on a selector:

            targets: {default_action, keys}                    one branch
            targets: {members: [{default_action, keys, selectors}, ...]}

        `default_action` has no meaning apart from the list it governs, so it never
        sits at workflow level, where it would stand beside `workflow_instance_params`
        — a genuinely workflow-scoped field — and read as a property of the workflow.
        Declared there it is REFUSED, because a branched list resolves its action from
        the matched member and a workflow-level one could only be lost.
        """

        if "default_action" in workflow:
            raise RuntimeError(
                f"❌ workflow {name!r} declares `default_action` at workflow level. It "
                "belongs inside `targets`, beside the `keys` list it governs (or inside "
                "each member when `targets` branches)"
            )
        targets = workflow.get("targets")
        if targets is None:
            return []
        if not isinstance(targets, dict):
            raise RuntimeError(
                f"❌ workflow {name!r} `targets` must be a mapping of "
                "{default_action, keys} or {members: [...]}"
            )
        if "members" in targets:
            return [
                (member.get("keys") or [], member.get("default_action"))
                for member in targets.get("members") or []
                if isinstance(member, dict)
            ]
        return [(targets.get("keys") or [], targets.get("default_action"))]

    @staticmethod
    def target_key_entries(
        workflow: dict, workflows: dict, *, label: str, _seen: tuple = ()
    ) -> list[str]:
        """Every target key a workflow can run, across ALL its member branches.

        Static: no execution context, so every branch counts rather than the one a
        run would select — a misdeclaration in an unselected branch is still a
        misdeclaration. Imports are followed, since an imported workflow's members
        are this workflow's members too.
        """

        name = str(workflow.get("__name__", ""))
        keys: list[str] = []
        for imported in workflow.get("import_workflows") or []:
            if imported in _seen or imported not in workflows:
                continue
            keys += WorkflowCatalog.target_key_entries(
                {**workflows[imported], "__name__": imported},
                workflows,
                label=label,
                _seen=(*_seen, name, imported),
            )
        branches = [entries for entries, _ in WorkflowCatalog.target_branches(workflow, name=name)]
        # A placed member is a member: leaving it out would exempt it from the
        # instance-param gate that every other target key passes.
        inserts = workflow.get("insert_targets")
        if isinstance(inserts, dict):
            branches += (
                [member.get("keys") or [] for member in inserts.get("members") or []]
                if "members" in inserts
                else [inserts.get("keys") or []]
            )
        for branch in branches:
            for entry in branch:
                key = entry.get("key") if isinstance(entry, dict) else entry
                if isinstance(key, str) and key and key not in keys:
                    keys.append(key)
        return keys


class WorkflowImports:
    """One workflow's reach into others: what it imports, and what it splices in."""

    @staticmethod
    def expand(action_workflows: dict, name: str, _stack: tuple = ()) -> list:
        """Resolve import_workflows in order, then append the workflow targets."""

        if name in _stack:
            raise RuntimeError(f"❌ workflow import cycle: {' -> '.join([*_stack, name])}")
        wf = action_workflows.get(name)
        if wf is None:
            raise RuntimeError(f"❌ workflow {name!r} not found (imported)")
        if not isinstance(wf, dict):
            raise RuntimeError(f"❌ workflow {name!r} must be a mapping")
        import_keys = wf.get("import_workflows") or []
        if not isinstance(import_keys, list) or not all(
            isinstance(value, str) and value for value in import_keys
        ):
            raise RuntimeError(
                f"❌ workflow {name!r} import_workflows must be a list of non-empty strings"
            )
        # An entry is a bare key, or a key with its OWN action; an entry without one
        # takes the branch's `default_action`. `targets` is FLAT here — load_workflow
        # has already collapsed the declared branches to the one this run selected.
        entries = target_catalog.TargetEntries.normalize(
            wf.get("targets") or [],
            label=f"workflow {name!r} targets",
            default_action=wf.get("default_action"),
        )
        target_runs: list = []
        # Where each import's contribution starts, so an anchor resolves inside the
        # sequence it NAMES rather than anywhere in the merged list.
        import_spans: dict[str, tuple[int, int]] = {}
        for workflow_key in import_keys:
            start = len(target_runs)
            target_runs.extend(
                WorkflowImports.expand(action_workflows, workflow_key, (*_stack, name))
            )
            import_spans[workflow_key] = (start, len(target_runs))
        for key, action in entries:
            target_runs.append({"id": key, "target": key, "action": action} if action else key)
        target_runs = WorkflowImports.splice_inserts(
            target_runs, wf, import_spans, name=name, label=f"workflow {name!r} insert_targets"
        )
        # A key MAY repeat when the actions differ — that is a composition doing two
        # things to one instance, and order decides the final state. The same key with
        # the same action twice is still a mistake.
        seen: set = set()
        for entry in target_runs:
            signature = run_addressing.workflow_target_run_signature(entry)
            if signature in seen:
                raise RuntimeError(
                    f"❌ workflow {name!r} has duplicate target key {signature[0]!r} after "
                    "import expansion" + (f" (action {signature[1]})" if signature[1] else "")
                )
            seen.add(signature)
        return target_runs

    @staticmethod
    def closure(workflows: dict, name: str, _seen: tuple = ()) -> set[str]:
        """The selected workflow plus every workflow it imports, transitively.

        What a run actually COMPOSES. `expand_workflow_imports` walks the same edges
        to build the target sequence, so this is the set whose declarations the run
        depends on — and therefore the only set whose branches have to resolve
        against this run's context.
        """

        if name in _seen or name not in workflows:
            return set(_seen)
        closure = {name}
        for imported in workflows[name].get("import_workflows") or []:
            closure |= WorkflowImports.closure(workflows, imported, (*_seen, name))
        return closure

    @staticmethod
    def resolve_inserts(workflow: dict, execution_context: dict[str, object], *, name: str) -> dict:
        """Collapse a declared `insert_targets` block to the branch this run selects.

        Same shape as `targets` — `{default_action, keys}` or `{members: [...]}` —
        because a placed entry needs the operation selector as much as an appended
        one does. A branched block that matches nothing places nothing: an operation
        the composition adds no member to is the normal case, not an error.
        """

        declared = workflow.get("insert_targets")
        if declared is None:
            return {}
        if not isinstance(declared, dict):
            raise RuntimeError(
                f"❌ workflow {name!r} `insert_targets` must be a mapping of "
                "{default_action, keys} or {members: [...]}"
            )
        if "members" not in declared:
            return {
                "keys": list(declared.get("keys") or []),
                "default_action": run_selectors.resolve_default_action(
                    declared.get("default_action"),
                    execution_context,
                    label=f"workflow {name!r} insert_targets",
                ),
            }
        member = run_selectors.resolve_list_member(
            declared,
            execution_context,
            value_field="keys",
            label=f"workflow {name!r} insert_targets",
            extra_fields=("default_action",),
        )
        if member is None:
            return {}
        return {
            "keys": list(member["keys"]),
            "default_action": run_selectors.resolve_default_action(
                member.get("default_action"),
                execution_context,
                label=f"workflow {name!r} insert_targets member",
            ),
        }

    @staticmethod
    def splice_inserts(
        target_runs: list,
        wf: dict,
        import_spans: dict[str, tuple[int, int]],
        *,
        name: str,
        label: str,
    ) -> list:
        """Place `insert_targets` entries INSIDE an imported sequence.

        `targets` appends, so its order is the order it is written in. A composed
        workflow that needs a member BETWEEN two imported ones cannot say that by
        writing it, because it does not author that list — so `insert_targets` names
        the position instead, and an anchor is legal only against an import.

        This is a splice index, not a graph: the result is one linear order, exactly
        as if the entry had been typed at that position.

        Entries anchored at the same point keep their declaration order.
        """

        declared = wf.get("insert_targets") or {}
        inserts = target_catalog.TargetEntries.normalize_inserts(
            declared.get("keys") or [],
            label=label,
            default_action=declared.get("default_action") or wf.get("default_action"),
        )
        if not inserts:
            return target_runs
        placements: list[tuple[int, int, object]] = []
        for order, (key, action, (position, anchor_workflow, anchor_key)) in enumerate(inserts):
            if anchor_workflow not in import_spans:
                raise RuntimeError(
                    f"❌ {label} entry {key!r} anchors in workflow {anchor_workflow!r}, "
                    f"which {name!r} does not import"
                )
            start, end = import_spans[anchor_workflow]
            matches = [
                index
                for index in range(start, end)
                if run_addressing.workflow_target_run_signature(target_runs[index])[0] == anchor_key
            ]
            if not matches:
                raise RuntimeError(
                    f"❌ {label} entry {key!r} anchors on {anchor_key!r}, which workflow "
                    f"{anchor_workflow!r} does not run"
                )
            if len(matches) > 1:
                raise RuntimeError(
                    f"❌ {label} entry {key!r} anchors on {anchor_key!r}, which workflow "
                    f"{anchor_workflow!r} runs {len(matches)} times — the position is "
                    "ambiguous, so it is refused rather than resolved by picking one"
                )
            at = matches[0] + (1 if position == "after" else 0)
            placements.append((at, order, {"id": key, "target": key, "action": action}))
        spliced = list(target_runs)
        for offset, (at, _, run) in enumerate(sorted(placements, key=lambda p: (p[0], p[1]))):
            spliced.insert(at + offset, run)
        return spliced


class WorkflowInstanceParams:
    """The parameters that make one workflow declaration many instances."""

    @staticmethod
    def validate_all(workflows: dict, targets: dict) -> None:
        """Every workflow's declared instance params, checked STATICALLY.

        The per-run guard is exact but only ever sees the workflow being run, so a
        misdeclaration elsewhere stays silent until someone runs it. This is the
        whole-cfg pass: it compares each workflow BRANCH against the target params
        that apply under that branch's condition, with no execution context and no
        resolution.

        A target's plain list applies to every branch; a members-shaped target
        contributes only the branches whose selectors could hold together with the
        workflow branch's. If SEVERAL branches of one target could hold and they
        declare different params, the workflow branch does not pin the axis that
        target dispatches on, and no single declaration can be correct — that is an
        error rather than a guess.
        """

        for name, workflow in sorted(workflows.items()):
            if not isinstance(workflow, dict):
                continue
            label = f"workflow {name!r}"
            member_keys = WorkflowCatalog.target_key_entries(
                {**workflow, "__name__": name}, workflows, label=label
            )
            for params, selectors in run_selectors._selector_branches(
                workflow.get("workflow_instance_params")
            ):
                union: list[str] = []
                for key in member_keys:
                    target = targets.get(key)
                    if not isinstance(target, dict):
                        continue  # not in this cfg's action; the per-run guard sees it
                    applicable = [
                        branch
                        for branch in run_selectors._selector_branches(
                            target.get("target_instance_params")
                        )
                        if run_selectors._selectors_can_both_hold(selectors, branch[1], label=label)
                    ]
                    distinct = {tuple(branch[0]) for branch in applicable}
                    if len(distinct) > 1:
                        raise RuntimeError(
                            f"❌ {label}: a branch selected by {selectors!r} matches "
                            f"{len(distinct)} different instance-param branches of target "
                            f"{key!r} ({sorted(distinct)}). The workflow branch does not pin "
                            "the axis that target dispatches on, so no single declaration is "
                            "correct — split the workflow branch the way the target is split"
                        )
                    for branch_params in applicable:
                        for param in branch_params[0]:
                            if param not in union:
                                union.append(param)
                missing = sorted(set(union) - set(params))
                extra = sorted(set(params) - set(union))
                if missing:
                    raise RuntimeError(
                        f"❌ {label}: workflow_instance_params branch {params} is missing "
                        f"{missing}, which its members instance over. Two target instances "
                        "would share one workflow address and their histories would merge"
                    )
                if extra:
                    raise RuntimeError(
                        f"❌ {label}: workflow_instance_params branch {params} declares "
                        f"{extra}, which no member instances over. That address can never "
                        "differ, and it tells a reader the workflow varies by an axis it "
                        "does not"
                    )

    @staticmethod
    def member_params(workflow_cfg: dict, targets: dict, *, label: str) -> tuple[list[str], bool]:
        """The union of the instance params of every target this workflow runs.

        UNION, not intersection: members may instance on different axes — one over
        two params, another over just one of them — and a workflow must partition by
        everything ANY member varies over. Otherwise two target instances collapse
        into one workflow address and their histories merge.

        Returns the union and whether it is COMPLETE — a member missing from this
        action's action contributes unknown axes, so the caller must not conclude
        that a declared param is spare.
        """
        union: list[str] = []
        complete = True
        for entry in workflow_cfg.get("target_runs") or []:
            name = entry if isinstance(entry, str) else entry.get("target")
            if not name:
                continue
            # A member absent from THIS action's action contributes no axes. The
            # sibling resolver is tolerant the same way (`targets.get(name) or {}`);
            # raising here would turn "this workflow is not runnable for this action"
            # Into a hard failure at identity resolution, which is not this guard's
            # job — the action allowlist already answers that.
            target_def = targets.get(name)
            if target_def is None:
                # Absent from THIS action's action, so its axes are unknown here
                # and the union is incomplete. Raising would turn "not runnable for
                # this action" into a hard failure, which the action allowlist
                # already answers.
                complete = False
                continue
            for param in target_def.get("target_instance_params") or []:
                if param not in union:
                    union.append(param)
        return union, complete

    @staticmethod
    def resolve_declared(
        declared, execution_context: dict[str, object] | None, *, label: str
    ) -> list[str]:
        """A workflow's declared instance params as a flat list.

        Shared by the run side, which then VALIDATES the list against its members'
        union, and by the status side, which only needs to ADDRESS an instance the
        run already validated. One definition because the two must produce the same
        address — they did not, and a targeted status query looked in a prefix no run
        writes.
        """

        if declared is None:
            return []
        if isinstance(declared, dict):
            # Members-shaped, exactly as a target's own instance params may be: a
            # member whose instance axes DISPATCH on a param (a domain, a profile)
            # Has a different union per context, so the workflow above it needs the
            # same dispatch rather than one list that can only ever be right once.
            resolved = run_selectors.resolve_list_members(
                declared, execution_context, value_field="params", label=label
            )
            return list(resolved or [])
        if isinstance(declared, list):
            return list(declared)
        raise RuntimeError(
            f"❌ {label}: workflow_instance_params must be a list, or members-shaped"
        )

    @staticmethod
    def validate(
        declared,
        workflow_cfg: dict,
        targets: dict,
        *,
        label: str,
        execution_context: dict[str, object] | None = None,
    ) -> list[str]:
        """A workflow's declared instance params must EQUAL its members' union.

        Declared rather than derived because declared params ARE identity,
        and guarded because the value has exactly one correct answer:

            declared < union   two target instances collapse into one workflow
                               address, so their histories merge and `last_run`
                               answers for the wrong one
            declared > union   an address that can never differ, and one that LIES:
                               a reader concludes the workflow varies by an axis
                               whose value is the same in every instance

        Both are errors. This is stricter than the target rule (over-declaration warns) on purpose: a target's params describe a thing that
        exists, so a spare axis is only slack; a workflow's params are DERIVED from
        its members, so a spare axis contradicts them.
        """
        union, union_is_complete = WorkflowInstanceParams.member_params(
            workflow_cfg, targets, label=label
        )
        declared_list = WorkflowInstanceParams.resolve_declared(
            declared, execution_context, label=label
        )
        missing = [p for p in union if p not in declared_list]
        extra = [p for p in declared_list if p not in union]
        if missing:
            raise RuntimeError(
                f"❌ {label}: workflow_instance_params is missing {sorted(missing)}, "
                f"which its members instance over. Two target instances would share one "
                f"workflow address and their histories would merge"
            )
        if extra and union_is_complete:
            raise RuntimeError(
                f"❌ {label}: workflow_instance_params declares {sorted(extra)}, which no "
                f"member instances over. That address can never differ, and it tells a "
                f"reader the workflow varies by an axis it does not"
            )
        # Declaration order is the ADDRESS order, so it is preserved as written.
        return declared_list


class WorkflowArtifacts:
    """What a workflow run writes for a reader: flow documents and identity."""

    @staticmethod
    def write_target_run_flow(
        path: Path, workflow_meta: dict | None, active_target_runs: dict
    ) -> None:
        """Write a compact ordered target_run-flow artifact."""
        target_run_flow = {
            "meta": workflow_meta,
            "target_runs": [
                {
                    "id": target_run_id,
                    "target": target_run.get("target"),
                    "source": target_run.get("source"),
                    "workflow": target_run.get("workflow"),
                    "execution_identities": target_run.get("execution_identities"),
                    "branch": target_run.get("branch"),
                    "commit": target_run.get("commit"),
                }
                for target_run_id, target_run in active_target_runs.items()
            ],
        }
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(target_run_flow, f, sort_keys=False)

    @staticmethod
    def write_target_flow(
        ctl_cfg_root: Path,
        artifacts_dir: Path,
        *,
        ctl_profile: str,
        execution_context: dict[str, object],
        action: str,
        workflow_name: str | None,
        target_repo_key: str,
        require_target_ref: bool,
        require_commit_refs: bool,
        refs: dict | None,
    ) -> None:
        """For plan runs, write the matching create-flow preview artifact."""

        if action != "plan" or not workflow_name:
            return

        target_action = "provision"
        try:
            target_workflow_cfg = WorkflowCatalog.workflow_cfg(
                ctl_cfg_root,
                ctl_profile,
                target_action,
                workflow_name,
                execution_context,
            )
            target_action_cfg = target_catalog.TargetCatalog.action_cfg(
                ctl_cfg_root, target_action, execution_context
            )
            target_catalog.TargetEntries.validate_selectors(
                target_workflow_cfg, target_action_cfg, execution_context
            )
            target_active_target_runs = target_catalog.ActiveTargetRuns.build(
                target_workflow_cfg,
                target_action_cfg,
                repo_key=target_repo_key,
                require_branch_or_commit=require_target_ref,
                refs=refs,
                execution_context=execution_context,
                require_commit_refs=require_commit_refs,
            )
        except Exception as exc:
            logging.warning(
                "Skipping target_runs_by_key_flow.yaml generation for plan/%s: %s",
                workflow_name,
                exc,
            )
            return

        WorkflowArtifacts.write_target_run_flow(
            artifacts_dir / "target_runs_by_key_flow.yaml",
            target_workflow_cfg.get("meta"),
            target_active_target_runs,
        )

    @staticmethod
    def identity_doc(
        workflow_key: str, target_instance_addresses: list[str], resolved_params: dict[str, str]
    ) -> dict:
        """The authoritative workflow-instance identity manifest:
        facts only — the digest is never the only identity source."""
        # The composition sha is the instance DIR NAME — not duplicated here
        return {
            "workflow_instance": {
                "workflow": workflow_key,
                "targets": list(target_instance_addresses),
                "resolved_params": dict(resolved_params),
            }
        }

    @staticmethod
    def selection_state_spec(selection: dict) -> dict:
        context = selection.execution_context
        target_specs: list[dict] = []
        for target_run in selection.active_target_runs.values():
            target_key = run_actions.normalize_result_name(
                target_run["target"], label="status target key"
            )
            segments = run_addressing.resolve_target_instance_segments(
                target_run.get("target_instance_params"),
                context,
                label=f"target {target_key}",
            )
            target_specs.append(
                {
                    "kind": "target",
                    "key": target_key,
                    "target_definition_sha256": kernel_paths.canonical_sha256(
                        target_catalog.ActiveTargetRuns.definition_document(target_run)
                    ),
                    **(
                        {"target_cfg_view_sha256": target_run["target_cfg_view_sha256"]}
                        if target_run.get("target_cfg_view_sha256") is not None
                        else {}
                    ),
                    "segments": segments,
                    "address": run_addressing.target_instance_address(target_key, segments),
                    "prefix": run_addressing.compose_state_relpath(
                        "target", target_key, segments
                    ).as_posix(),
                }
            )
        if selection.kind == "target":
            if len(target_specs) != 1:
                raise RuntimeError("❌ target status selection must resolve one target instance")
            return target_specs[0]
        if selection.kind != "workflow":
            raise RuntimeError(
                f"❌ status does not support selection kind {selection['selection_kind']!r}"
            )
        key = run_actions.normalize_result_name(selection.key, label="status workflow key")
        # The SAME addressing a run writes — declared instance params, not a
        # composition digest. the run side to params and left this one
        # on the hash, so a targeted status query hydrated `instances/sha256=<digest>`,
        # A prefix nothing ever writes, and reported no state. Invisible under `--all`,
        # which parses the tree instead of composing a prefix.
        segments = run_addressing.resolve_target_instance_segments(
            WorkflowInstanceParams.resolve_declared(
                selection.workflow_cfg.get("workflow_instance_params"),
                selection.execution_context,
                label=f"workflow {key!r}",
            ),
            selection.execution_context,
            label=f"workflow {key!r}",
        )
        definition_canonical = json.dumps(
            selection.workflow_cfg, separators=(",", ":"), sort_keys=True
        )
        return {
            "kind": "workflow",
            "key": key,
            "segments": segments,
            "address": run_addressing.instance_address(key, segments),
            "prefix": run_addressing.compose_state_relpath("workflow", key, segments).as_posix(),
            "target_specs": target_specs,
            "workflow_definition_sha256": hashlib.sha256(
                definition_canonical.encode("utf-8")
            ).hexdigest(),
        }


class WorkflowChildren:
    """The child runs a workflow spawns, and the slice each one receives."""

    @staticmethod
    def populate_slice(
        child_run_dir: Path,
        target_run: dict,
        target_run_id: str,
        plt_targets_dir_path: Path,
        execution_context: dict[str, object],
    ) -> None:
        """Make a workflow-child target run self-contained AT THE TARGET
        LEVEL — its own rendered cfg (input + resolved), frozen execution context,
        and the source refs it ran against — so a target result is independently
        inspectable without walking to the parent workflow run. Workflow-WIDE
        artifacts (whole-workflow plan, resolved flow, orchestrator logs) stay under
        the parent run, which the child references by `parent_workflow_run_id`.
        Additive: it only writes into the child run dir, never the workflow run."""
        # (b3): the child owns its WHOLE cfg derivation, not two views of a
        # tree the workflow keeps. The workflow builds it up front (fail-fast for every
        # target before any runs — §b2) and hands the complete tree to the target it
        # describes; `run_pipeline` then drops the workflow-side copy.
        cfg_dst = child_run_dir / "cfg"
        src_root = plt_targets_dir_path / target_run_id
        for view in ("merged", "rendered", "input", "resolved"):
            src = src_root / view
            if src.is_dir():
                shutil.copytree(src, cfg_dst / "plt" / view, dirs_exist_ok=True)
        # The target's OWN execution context — params filtered to what it declared,
        # plus target.* — not the run-wide one)
        target_context_path = (
            src_root / "execution" / execution_run_context.EXECUTION_CONTEXT_FILENAME
        )
        if target_context_path.is_file():
            (child_run_dir / "execution").mkdir(parents=True, exist_ok=True)
            shutil.copy2(
                target_context_path,
                child_run_dir / "execution" / execution_run_context.EXECUTION_CONTEXT_FILENAME,
            )
        else:
            execution_run_context.write_execution_context_artifact(child_run_dir, execution_context)
        if target_run.get("target_definition") is not None:
            kernel_yaml_io.write_yaml_file(
                cfg_dst / "ctl" / "target_definition.yaml", target_run["target_definition"]
            )
        source_refs = {
            key: target_run[key]
            for key in ("source", "ref", "branch", "commit", "procedure")
            if target_run.get(key) is not None
        }
        if source_refs:
            # A RECORD (what this run ran), not workspace scratch — it no
            # longer shares a name with the build workspace.
            kernel_yaml_io.write_yaml_file(child_run_dir / "source_refs.yaml", source_refs)

    @staticmethod
    def build_command(
        spec: dict,
        target_key: str,
        *,
        parent_run_dir: Path,
        parent_run_id: str,
        action: str | None = None,
    ) -> list[str]:
        """The argv for one workflow child, derived from ONE frozen spec.

        A child must run with exactly its parent's settings. Dropping a flag would not
        fail — the child would silently run DIFFERENTLY — so the argv is built from a
        single object captured in `run_pipeline`, never assembled from scattered
        locals.

        the ACTION is the exception, and deliberately so. It comes from the
        member entry when that entry declares one, because a workflow is the one level
        that may hold members going different directions. Everything else still comes
        from the frozen spec.
        """

        argv = [
            sys.executable,
            str(spec["ctl_entrypoint"]),
            "target",
            "--ctl-cfg",
            str(spec["ctl_cfg_root"]),
            "--ctl-profile",
            spec["ctl_profile"],
            "--ctl-state-local-root",
            str(spec["ctl_state_local_root"]),
            "--execution-runtime-mode",
            spec["execution_runtime_mode"],
            "--action",
            action or spec["action"],
            "--target",
            target_key,
            # The child runs UNDER the parent's ctl-state lock. Authorisation is a
            # single-use grant passed by ENVIRONMENT (see CHILD_LOCK_GRANT_ENV); the
            # run id below is provenance only, and is deliberately not a credential.
            "--parent-workflow-run-id",
            parent_run_id,
        ]
        # Read from the parent's own record rather than the frozen spec: the spec is
        # about how to INVOKE a child, while the instance address and the label are
        # facts ABOUT the parent, and taking them from the record they are written in
        # leaves nothing to drift.
        parent_metadata = state_run_store.load_run_metadata(parent_run_dir)
        parent_instance_address = parent_metadata.get("instance_address")
        if parent_instance_address:
            argv += ["--parent-workflow-instance-address", str(parent_instance_address)]
        # One invocation, one label: a child carries its parent's rather than being
        # given one of its own, which is what makes a label group a deployment
        # instead of restating the run it sits on.
        parent_label = parent_metadata.get("label")
        if parent_label:
            argv += ["--label", str(parent_label)]
        if spec.get("providers"):
            argv += ["--providers", ",".join(spec["providers"])]
        # The cadence has no default, so a child cannot inherit one: the parent's
        # choice travels in the frozen spec like every other run-shaping argument.
        if spec.get("credential_refresh_modes"):
            argv += [
                "--credential-refresh-mode",
                ",".join(f"{k}={v}" for k, v in sorted(spec["credential_refresh_modes"].items())),
            ]
        for key, value in (spec.get("execution_params") or {}).items():
            argv += ["--execution-params", f"{key}={value}"]
        for key, value in (spec.get("provider_options") or {}).items():
            argv += ["--provider-options", f"{key}={value}"]
        for provider, mode in (spec.get("execution_access_modes") or {}).items():
            argv += ["--execution-access-mode", f"{provider}={mode}"]
        for provider in spec.get("force_skip_execution_identity_preflight_check") or []:
            argv += ["--force-skip-execution-identity-preflight-check", provider]
        for flag, enabled in (
            (
                "--agreed-defer-ctl-state-backend-sync",
                spec.get("agreed_defer_ctl_state_backend_sync"),
            ),
            ("--force-skip-ctl-state-backend-sync", spec.get("force_skip_ctl_state_backend_sync")),
            ("--force-skip-guardrails", spec.get("force_skip_guardrails")),
            (
                "--force-skip-full-cfg-validation-gate",
                spec.get("force_skip_full_cfg_validation_gate"),
            ),
            ("--skip-children-precheck", spec.get("skip_children_precheck")),
        ):
            if enabled:
                argv.append(flag)
        return argv
