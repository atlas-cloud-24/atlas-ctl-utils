"""The fan-out a run expands into children."""

from dataclasses import dataclass
from pathlib import Path

from engine.catalog import workflow as catalog_workflow
from engine.cfg import resources as cfg_resources
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.run import selectors as run_selectors


@dataclass(frozen=True, kw_only=True)
class FanOut:
    """A fan-out and the child runs it expands into."""

    key: str = ""
    children: tuple[dict, ...] = ()

    @classmethod
    def from_cfg(cls, key: str, fan_out_cfg: dict) -> "FanOut":
        """Build from the mapping the catalog produced."""

        return cls(key=key, children=tuple(fan_out_cfg.get("children") or ()))

    @property
    def child_count(self) -> int:
        """How many children this fan-out drives."""

        return len(self.children)

    def to_document(self) -> dict:
        """Render for the run record."""

        return {"fan_out": self.key, "children": self.child_count}

    @staticmethod
    def expand(ctl_cfg_root: Path, fan_out_key: str, execution_context: dict[str, object]) -> dict:
        """Expand a fan_out into concrete child runs — pure cfg logic, no execution and
        no state. Each child retains its optional parameter-set and entry keys so
        reports never conflate one declared workflow with its concrete expansions.
        Each child is one existing workflow/target run; the driver loops the runners.

        a param-set member is {params, selectors?}. A member whose
        selectors do not match the frozen execution context is DROPPED before
        children are built — one fan-out serves every zone, the per-zone member
        set is resolved, not hardcoded. `domain` params are validated against the
        domain registry."""
        fan_outs = cfg_resources.collect_resource(ctl_cfg_root, "fan_outs", entry_depth=1)
        fan_out = fan_outs.get(fan_out_key)
        if not isinstance(fan_out, dict):
            available = ", ".join(sorted(fan_outs)) or "none"
            raise RuntimeError(f"❌ fan-out {fan_out_key!r} not found; available: {available}")
        runs = fan_out.get("runs")
        if not isinstance(runs, list) or not runs:
            raise RuntimeError(f"❌ fan-out {fan_out_key!r} has no runs")
        param_sets = cfg_resources.collect_resource(
            ctl_cfg_root, "fan_out_param_sets", entry_depth=1
        )
        domains = execution_run_context.load_domain_registry(ctl_cfg_root)
        children: list[dict] = []
        for i, run in enumerate(runs):
            # A fan-out expands WORKFLOWS. To fan a target, wrap it in a
            # workflow, exactly as a step is only reachable through a procedure. One
            # child kind leaves ONE place in cfg where a target's action is declared
            # — the workflow member — so the two mechanisms cannot disagree about
            # what a target does.
            workflow_key = run.get("workflow_key")
            if not workflow_key:
                raise RuntimeError(f"❌ fan-out {fan_out_key!r} run[{i}] must set workflow_key")
            if run.get("target_key"):
                raise RuntimeError(
                    f"❌ fan-out {fan_out_key!r} run[{i}] sets target_key; a fan-out "
                    "expands workflows only. Wrap the target in a workflow and name that"
                )
            kind = "workflow"
            key = workflow_key
            param_set_key = run.get("fan_out_param_set_key")
            # `extra_params` adds the SAME param to every member of the
            # referenced set, so one account list can serve several domains instead
            # of being copied per domain. Additive only — a key already declared by
            # A member is a hard error, never a silent override.
            extra_params = run.get("extra_params")
            if extra_params is not None:
                run_label = f"fan-out {fan_out_key!r} run[{i}] extra_params"
                if not isinstance(extra_params, dict) or not extra_params:
                    raise RuntimeError(f"❌ {run_label} must be a non-empty map")
                if param_set_key is None:
                    raise RuntimeError(
                        f"❌ {run_label} requires fan_out_param_set_key "
                        "(there are no members to add the params to)"
                    )
                for extra_key, extra_value in extra_params.items():
                    if not isinstance(
                        extra_key, str
                    ) or not execution_references.CONTEXT_KEY_RE.fullmatch(extra_key):
                        raise RuntimeError(
                            f"❌ {run_label}: key {extra_key!r} must be a valid identifier"
                        )
                    if isinstance(extra_value, (dict, list)):
                        raise RuntimeError(f"❌ {run_label}.{extra_key} must be a scalar")
                if "domain" in extra_params:
                    execution_run_context.validate_domain_value(
                        domains, extra_params["domain"], label=run_label
                    )
            if param_set_key is None:
                children.append(
                    {
                        "kind": kind,
                        "key": key,
                        "params": {},
                        "label": key,
                        "fan_out_param_set_key": None,
                        "fan_out_param_entry_key": None,
                    }
                )
                continue
            param_set = param_sets.get(param_set_key)
            if not isinstance(param_set, dict) or not param_set:
                raise RuntimeError(
                    f"❌ fan-out {fan_out_key!r} run[{i}] references unknown fan_out_param_set {param_set_key!r}"
                )
            matched_members = 0
            for entry_name, member in param_set.items():
                member_label = f"fan_out_param_set {param_set_key!r}.{entry_name}"
                if not isinstance(member, dict):
                    raise RuntimeError(f"❌ {member_label} must be a mapping")
                unknown = set(member) - {"params", "selectors"}
                if unknown:
                    raise RuntimeError(
                        f"❌ {member_label} has unsupported keys {sorted(unknown)} "
                        "(a member is params + optional selectors; selectors must NOT "
                        "sit inside params)"
                    )
                params = member.get("params")
                if not isinstance(params, dict) or not params:
                    raise RuntimeError(f"❌ {member_label} params must be a non-empty map")
                if "selectors" in params:
                    raise RuntimeError(
                        f"❌ {member_label}: selectors must be a member field, not a param"
                    )
                if "domain" in params:
                    execution_run_context.validate_domain_value(
                        domains, params["domain"], label=member_label
                    )
                if extra_params:
                    collisions = sorted(set(params) & set(extra_params))
                    if collisions:
                        raise RuntimeError(
                            f"❌ {member_label} already declares {collisions} also set by "
                            f"fan-out {fan_out_key!r} run[{i}] extra_params; define each param "
                            "in one place"
                        )
                if not run_selectors.selector_matches(
                    member.get("selectors"),
                    execution_context,
                    label=member_label,
                    structured_only=True,
                ):
                    continue
                children.append(
                    {
                        "kind": kind,
                        "key": key,
                        "params": {**params, **(extra_params or {})},
                        # One param set may serve several runs of the same
                        # workflow (each pinned by different extra_params), so the
                        # member name alone does not identify a child.
                        "label": (
                            f"{key}[{'+'.join(str(v) for v in extra_params.values())}:{entry_name}]"
                            if extra_params
                            else f"{key}[{entry_name}]"
                        ),
                        "fan_out_param_set_key": param_set_key,
                        "fan_out_param_entry_key": entry_name,
                    }
                )
                matched_members += 1
            if matched_members == 0:
                raise RuntimeError(
                    f"❌ fan-out {fan_out_key!r} run[{i}]: no member of fan_out_param_set "
                    f"{param_set_key!r} matches the execution context (a run entry must "
                    "contribute at least one child)"
                )
        # Fan-out children run SEQUENTIALLY. Each child acquires the ctl-state lock,
        # which is exclusive and non-blocking over the whole local root, so a second
        # concurrent child fails outright. `max_parallel` therefore described a knob
        # that could not be turned; it is removed rather than left as a trap. Running
        # disjoint children in parallel needs a finer-grained lock (per namespace or
        # per instance) — recorded as tech debt, not a cfg setting.
        if "max_parallel" in fan_out:
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} declares max_parallel, which is removed: "
                "children run sequentially because each acquires the exclusive "
                "ctl-state lock. Delete the key."
            )
        failure_mode = fan_out.get("failure_mode", "stop")
        if failure_mode not in ("stop", "continue"):
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} failure_mode must be 'stop' or 'continue'"
            )
        return {"failure_mode": failure_mode, "children": children}

    @staticmethod
    def validate_param_collisions(
        ctl_cfg_root: Path,
        children: list[dict],
        cli_execution_params: dict[str, str],
    ) -> None:
        """Reject fan-out params that would override an existing run param."""
        cfg_param_keys = set(execution_run_context.load_execution_params(ctl_cfg_root))
        cli_param_keys = set(cli_execution_params)
        occupied_param_keys = cfg_param_keys | cli_param_keys
        collision_rows: list[str] = []
        for child in children:
            for key in sorted(occupied_param_keys & set(child.get("params") or {})):
                sources: list[str] = []
                if key in cli_param_keys:
                    sources.append("--execution-params")
                if key in cfg_param_keys:
                    sources.append("ctl execution_params")
                source = " and ".join(sources)
                collision_rows.append(f"{child['label']}: {key} ({source})")
        if collision_rows:
            raise RuntimeError(
                "❌ fan-out child params collide with existing execution params; "
                "fan-out params cannot override CLI or ctl cfg values: " + "; ".join(collision_rows)
            )

    @staticmethod
    def validate_unique_materializations(
        child_selections: list[dict],
    ) -> list[dict]:
        specs = [
            catalog_workflow.WorkflowArtifacts.selection_state_spec(selection)
            for selection in child_selections
        ]
        seen: dict[str, int] = {}
        duplicates: list[str] = []
        for index, spec in enumerate(specs):
            address = f"{spec['kind']}:{spec['address']}"
            if address in seen:
                duplicates.append(address)
            else:
                seen[address] = index
        if duplicates:
            raise RuntimeError(
                "❌ fan-out materializes duplicate state owners: "
                + ", ".join(sorted(set(duplicates)))
            )
        return specs

    @staticmethod
    def wrap_preflight_child(
        report: dict,
        child: dict,
        *,
        effective_params: dict[str, str] | None = None,
    ) -> dict:
        """

        fold the child's own (per-member) params onto its workflow/target node,
        and wrap it in a parameter-set node when one was expanded. Run-constant params
        (provider, landing_zone, …) live on the fan-out header, not here."""

        del effective_params  # per-member params are child["params"]; constants hoist
        per_member = dict(child.get("params") or {})
        param_set_key = child.get("fan_out_param_set_key")
        entry_key = child.get("fan_out_param_entry_key")
        if param_set_key is None:
            if not per_member:
                return report
            return {**report, "params": per_member}
        report_node = dict(report)
        if per_member:
            report_node["params"] = per_member
        return {
            "selection": {
                "kind": "fan_out_param_set",
                "key": f"{param_set_key}.{entry_key}",
            },
            "status": report["status"],
            "children": [report_node],
        }

    def run(self, *, context) -> list[int]:
        """Run one child per expansion."""

        return [context.spawn(child) for child in self.children]
