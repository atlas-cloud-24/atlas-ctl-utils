"""Composing an ordered run out of declared targets.

A workflow is an ORDERED sequence, not a graph: it has no branching and no
concurrency, so a member that fails ends the run. Variants place extra targets
against an anchor, and a fan-out expands one declaration into many runs."""

import hashlib
import json
import logging
import shutil
import sys

from pathlib import Path
import yaml

from engine.catalog import targets as catalog_targets
from engine.cfg import resources as cfg_resources
from engine.execution import providers as execution_providers
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.kernel import paths as kernel_paths
from engine.kernel import scalars as kernel_scalars
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.run import selectors as run_selectors
from engine.state import run_store as state_run_store

def validate_workflow_actions_declared(workflows: dict) -> None:
    """Every workflow entry must be able to resolve an ACTION.

    A list with no `default_action` whose entries carry no `action:` is not
    runnable — the engine cannot know what to do with those targets. This is a cfg
    gate rather than a run-time surprise, so a workflow that could never run is
    refused when the configuration is validated.
    """

    for name, workflow in (workflows or {}).items():
        if not isinstance(workflow, dict):
            continue
        declaration = workflow.get("target_keys")
        lists: list[tuple[list, object]] = []
        if isinstance(declaration, list):
            lists.append((declaration, workflow.get("default_action")))
        elif isinstance(declaration, dict):
            for member in declaration.get("members") or []:
                if isinstance(member, dict):
                    lists.append(
                        (member.get("target_keys") or [], member.get("default_action"))
                    )
        for entries, default_action in lists:
            if default_action:
                continue
            bare = [
                entry for entry in entries
                if not (isinstance(entry, dict) and entry.get("action"))
            ]
            if bare:
                raise RuntimeError(
                    f"❌ workflow {name!r}: {bare} have no action and the list "
                    "declares no default_action, so the engine cannot know how to "
                    "run them. Declare `default_action:` for the list — a literal "
                    "action, or ${execution_context.ctl.operation} to follow the "
                    "invocation — or `action:` beneath each key"
                )


def parse_ctl_variants_arg(value: str) -> list[str]:
    """

    parse comma-separated ctl variant paths under variants/."""

    return kernel_scalars.parse_relative_paths_arg(
        value,
        root_dir_name="variants",
        item_label="Ctl variant",
    )


def workflow_member_actions(workflow_cfg: dict) -> set[str]:
    """Every action a workflow's member entries ask of their targets."""
    actions: set[str] = set()
    for entry in (workflow_cfg or {}).get("target_runs") or []:
        if isinstance(entry, dict) and entry.get("action"):
            actions.add(str(entry["action"]))
    return actions


def expand_workflow_imports(action_workflows: dict, name: str, _stack: tuple = ()) -> list:
    """

    resolve import_workflow_keys in order, then append the workflow target_keys."""

    if name in _stack:
        raise RuntimeError(f"❌ workflow import cycle: {' -> '.join([*_stack, name])}")
    wf = action_workflows.get(name)
    if wf is None:
        raise RuntimeError(f"❌ workflow {name!r} not found (imported)")
    if not isinstance(wf, dict):
        raise RuntimeError(f"❌ workflow {name!r} must be a mapping")
    import_keys = wf.get("import_workflow_keys") or []
    if not isinstance(import_keys, list) or not all(
        isinstance(value, str) and value for value in import_keys
    ):
        raise RuntimeError(
            f"❌ workflow {name!r} import_workflow_keys must be a list of non-empty strings"
        )
    # An entry is a bare key, or a key with its OWN action. A member
    # without one inherits the invoked operation, which is what keeps every
    # existing workflow working unchanged.
    entries = catalog_targets.normalize_target_entries(
        wf.get("target_keys") or [],
        label=f"workflow {name!r} target_keys",
        default_action=wf.get("default_action"),
    )
    target_runs: list = []
    for workflow_key in import_keys:
        target_runs.extend(expand_workflow_imports(action_workflows, workflow_key, (*_stack, name)))
    for key, action in entries:
        target_runs.append(
            {"id": key, "target": key, "action": action} if action else key
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
                "import expansion"
                + (f" (action {signature[1]})" if signature[1] else "")
            )
        seen.add(signature)
    return target_runs


def load_workflow_cfg(
    ctl_cfg_root: Path,
    ctl_profile: str,
    action: str,
    workflow_name: str,
    execution_context: dict[str, object],
) -> dict:
    """Load a content-key workflow: `workflows.<name>` (imports + selectors).

    workflows are declared ONCE with a required `actions:` allowlist;
    the action gates availability. `target_keys` may be members-shaped (dispatch
    by `execution_context.ctl.action`) when the apply-family composition differs
    per action. Expands `import_workflow_keys` (ordered, recursive) then the
    workflow's own `target_keys`; applies `selectors` (intersected through
    imports). The workflow name is an opaque key (slashes are cosmetic).
    """

    workflows = cfg_resources.collect_resource(ctl_cfg_root, "workflows", entry_depth=1)
    if workflow_name not in workflows:
        raise RuntimeError(f"❌ workflow {workflow_name!r} not found")
    validate_workflow_actions_declared(workflows)
    resolved_workflows: dict = {}
    for name, wf in workflows.items():
        if not isinstance(wf, dict):
            raise RuntimeError(f"❌ workflow {name!r} must be a mapping")
        # A workflow declares no allowlist. Its member selectors decide
        # when it applies, and each member's declared action decides what runs —
        # an `operations:` list restated the selectors and could contradict them.
        target_keys = wf.get("target_keys")
        if isinstance(target_keys, dict):
            member = run_selectors.resolve_list_member(
                target_keys,
                execution_context,
                value_field="target_keys",
                label=f"workflow {name!r} target_keys",
                extra_fields=("default_action",),
            )
            if member is None:
                raise RuntimeError(
                    f"❌ workflow {name!r} members-shaped target_keys did not "
                    "resolve for this execution context"
                )
            # The member's declared default reaches every entry it
            # carries, so a list going one direction states its verb once.
            wf = {
                **wf,
                "target_keys": list(member["target_keys"]),
                "default_action": run_selectors.resolve_default_action(
                    member.get("default_action"), execution_context,
                    label=f"workflow {name!r} member",
                ),
                "member_selectors": member.get("selectors"),
            }
        else:
            wf = {
                **wf,
                "default_action": run_selectors.resolve_default_action(
                    wf.get("default_action"), execution_context,
                    label=f"workflow {name!r}",
                ),
            }
        resolved_workflows[name] = wf
    if workflow_name not in resolved_workflows:
        raise RuntimeError(
            f"❌ workflow {workflow_name!r} does not allow action {action!r}"
        )

    effective_selectors = run_selectors.workflow_effective_selectors(resolved_workflows, workflow_name)
    if not run_selectors.selector_matches(
        effective_selectors,
        execution_context,
        label=f"workflow {action}/{workflow_name}",
    ):
        raise RuntimeError(
            f"❌ workflow {action}/{workflow_name} is not available for "
            f"runtime selectors {execution_context} (selectors {effective_selectors})"
        )

    target_runs = expand_workflow_imports(resolved_workflows, workflow_name)
    resolved = resolved_workflows[workflow_name]
    cfg = {
        "meta": {"name": f"{action}/{workflow_name}", "action": action},
        "target_runs": target_runs,
    }
    # The matched member's declared default and its selector block travel
    # with the resolved workflow — the run record carries them, and returning only
    # meta + target_runs silently dropped both. the instance params
    # for the same reason: a field the caller needs must survive resolution.
    for field in ("default_action", "member_selectors", "workflow_instance_params"):
        if resolved.get(field):
            cfg[field] = resolved[field]
    return cfg


def find_workflow_anchor_index(target_runs: list, anchor: str, *, label: str) -> int | None:
    """Locate the single entry an anchor target key names; None when absent.

    Ambiguity is REFUSED rather than resolved by position. Since key
    may appear twice with different actions, and picking the first would place a
    variant before or after an arbitrary one of them — a silent choice about
    ordering, which is exactly what a placement declares.
    """

    matches = [
        index
        for index, entry in enumerate(target_runs)
        if run_addressing.workflow_target_run_key(entry) == anchor
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise RuntimeError(
            f"❌ {label} anchor {anchor!r} matches {len(matches)} entries; a "
            "placement cannot choose between them. Qualify the anchor or make "
            "the key unique in this workflow"
        )
    return matches[0]


def variant_source_action(action: str) -> str:
    """

    which action's variants apply. `plan` previews `provision`, so a plan run
    resolves variants against `provision` rather than a separate `plan` block."""

    return run_actions.action_previewed_by(action)


def variant_anchor(variant: dict, *, label: str) -> tuple[str, bool]:
    """Return `(anchor target key, insert_before)` for one placement.

    The two anchor fields are one choice with two spellings, so exactly one is
    required and both together is a contradiction rather than a precedence rule.
    """
    before, after = variant.get("before_target_key"), variant.get("after_target_key")
    if before and after:
        raise RuntimeError(
            f"❌ {label} cannot set both 'before_target_key' and 'after_target_key'"
        )
    if not (before or after):
        raise RuntimeError(
            f"❌ {label} must define 'after_target_key' or 'before_target_key'"
        )
    return (before, True) if before else (after, False)


def variant_target_run_entry(
    variant: dict, target_name: str, workflow_cfg: dict, *, label: str
) -> dict:
    """Build the target_run entry a placement inserts.

 every entry to carry a DECLARED action, so a placement
    resolves one the same way a member list does: its own `action:`, else the
    workflow's resolved default. Inserting a bare key would slip past that gate,
    because normalization has already run by the time a variant applies.
    """

    action = variant.get("action") or workflow_cfg.get("default_action")
    if not action:
        raise RuntimeError(
            f"❌ {label} inserts {target_name!r} with no action: the workflow "
            "declares no `default_action`, so the placement must declare `action:`"
        )
    if action not in run_actions.RUN_ACTIONS:
        raise RuntimeError(
            f"❌ {label} declares action {action!r}; expected one of {sorted(run_actions.RUN_ACTIONS)}"
        )
    return {"id": target_name, "target": target_name, "action": action}


def apply_ctl_variants_to_workflow_cfg(
    ctl_cfg_root: Path,
    workflow_cfg: dict,
    action_cfg: dict,
    *,
    execution_context: dict[str, object],
    action: str,
    workflow_name: str,
    ctl_variants: list[str],
) -> dict:
    """Apply selected variant placements to a loaded workflow cfg.

    A variant is `variants.<action>.<name> = {target_key, workflow_key, after_target_key|before_target_key, [selectors]}`.
    For each selected variant whose `workflow_key` matches the running one: validate its target
    exists and `variant.selectors ⊆ target.selectors`, gate through runtime selectors (the target
    ceiling AND the variant subset), then insert the target name at the after/before anchor
    (skip + log if the anchor is absent in this workflow).
    """

    if not ctl_variants:
        return workflow_cfg

    variant_action = variant_source_action(action)
    variants = execution_run_context.load_variants_cfg(ctl_cfg_root).get(variant_action, {})
    targets = action_cfg.get("targets", {})
    target_runs = list(workflow_cfg.get("target_runs") or [])

    for name in ctl_variants:
        v = variants.get(name)
        if v is None:
            raise RuntimeError(
                f"❌ variant {name!r} not found under action {variant_action!r} in variant config"
            )
        if not isinstance(v, dict):
            raise RuntimeError(f"❌ variant {name!r} must be a mapping")

        if v.get("workflow_key") != workflow_name:
            logging.info(
                "Variant '%s' targets workflow '%s', not the running '%s' — skipped",
                name, v.get("workflow_key"), workflow_name,
            )
            continue

        target_name = v.get("target_key")
        target = targets.get(target_name)
        if target is None:
            raise RuntimeError(
                f"❌ variant {name!r} references missing target {target_name!r} (action {action!r})"
            )

        ok, why = run_selectors._selectors_subset(v.get("selectors"), target.get("selectors"))
        if not ok:
            raise RuntimeError(f"❌ variant {name!r} selectors exceed target {target_name!r}: {why}")

        if not run_selectors.selector_matches(
            target.get("selectors"),
            execution_context,
            label=f"target {target_name}",
        ):
            logging.info("Variant '%s' target is not available for selectors %s — skipped", name, execution_context)
            continue
        if not run_selectors.selector_matches(
            v.get("selectors"),
            execution_context,
            label=f"variant {name}",
        ):
            logging.info("Variant '%s' placement gated off for selectors %s — skipped", name, execution_context)
            continue

        anchor, before = variant_anchor(v, label=f"variant {name!r}")
        index = find_workflow_anchor_index(
            target_runs, anchor, label=f"variant {name!r}"
        )
        if index is None:
            logging.info("Variant '%s' anchor '%s' absent from '%s' — skipped", name, anchor, workflow_name)
            continue

        entry = variant_target_run_entry(
            v, target_name, workflow_cfg, label=f"variant {name!r}"
        )
        signature = run_addressing.workflow_target_run_signature(entry)
        if signature in {run_addressing.workflow_target_run_signature(e) for e in target_runs}:
            raise RuntimeError(
                f"❌ variant {name!r} inserts duplicate target {target_name!r}"
                + (f" (action {signature[1]})" if signature[1] else "")
            )
        target_runs.insert(index if before else index + 1, entry)
        logging.info(
            "Applied variant '%s': inserted '%s' (action %s) %s '%s'",
            name, target_name, signature[1], "before" if before else "after", anchor,
        )

    patched = dict(workflow_cfg)
    patched["target_runs"] = target_runs
    return patched


def write_target_run_flow_artifact(path: Path, workflow_meta: dict | None, active_target_runs: dict) -> None:
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


def write_target_flow_artifact(
    ctl_cfg_root: Path,
    artifacts_dir: Path,
    *,
    ctl_profile: str,
    execution_context: dict[str, object],
    action: str,
    workflow_name: str | None,
    ctl_variants: list[str],
    plt_overlays: list[str],
    target_repo_key: str,
    require_target_ref: bool,
    require_commit_refs: bool,
    refs: dict | None,
) -> None:
    """

    for plan runs, write the matching create-flow preview artifact."""

    if action != "plan" or not workflow_name:
        return

    target_action = "provision"
    try:
        target_workflow_cfg = load_workflow_cfg(
            ctl_cfg_root,
            ctl_profile,
            target_action,
            workflow_name,
            execution_context,
        )
        target_action_cfg = catalog_targets.load_action_cfg(ctl_cfg_root, target_action, execution_context)
        target_workflow_cfg = apply_ctl_variants_to_workflow_cfg(
            ctl_cfg_root,
            target_workflow_cfg,
            target_action_cfg,
            execution_context=execution_context,
            action=target_action,
            workflow_name=workflow_name,
            ctl_variants=ctl_variants,
        )
        catalog_targets.validate_workflow_target_selectors(target_workflow_cfg, target_action_cfg, execution_context)
        target_active_target_runs = catalog_targets.build_active_target_runs(
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

    write_target_run_flow_artifact(
        artifacts_dir / "target_runs_by_key_flow.yaml",
        target_workflow_cfg.get("meta"),
        target_active_target_runs,
    )


def selection_state_spec(selection: dict) -> dict:
    context = selection["execution_context"]
    target_specs: list[dict] = []
    for target_run in selection["active_target_runs"].values():
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
                    catalog_targets.target_definition_document(target_run)
                ),
                **(
                    {"target_cfg_view_sha256": target_run["target_cfg_view_sha256"]}
                    if target_run.get("target_cfg_view_sha256") is not None
                    else {}
                ),
                "segments": segments,
                "address": run_addressing.target_instance_address(target_key, segments),
                "prefix": run_addressing.compose_state_relpath("target", target_key, segments
                ).as_posix(),
            }
        )
    if selection["selection_kind"] == "target":
        if len(target_specs) != 1:
            raise RuntimeError("❌ target status selection must resolve one target instance")
        return target_specs[0]
    if selection["selection_kind"] != "workflow":
        raise RuntimeError(
            f"❌ status does not support selection kind {selection['selection_kind']!r}"
        )
    key = run_actions.normalize_result_name(selection["selection_key"], label="status workflow key")
    # The SAME addressing a run writes — declared instance params, not a
    # composition digest. the run side to params and left this one
    # on the hash, so a targeted status query hydrated `instances/sha256=<digest>`,
    # A prefix nothing ever writes, and reported no state. Invisible under `--all`,
    # which parses the tree instead of composing a prefix.
    segments = run_addressing.resolve_target_instance_segments(
        resolve_declared_workflow_instance_params(
            selection["workflow_cfg"].get("workflow_instance_params"),
            selection["execution_context"],
            label=f"workflow {key!r}",
        ),
        selection["execution_context"],
        label=f"workflow {key!r}",
    )
    definition_canonical = json.dumps(
        selection["workflow_cfg"], separators=(",", ":"), sort_keys=True
    )
    return {
        "kind": "workflow",
        "key": key,
        "segments": segments,
        "address": run_addressing.instance_address(key, segments),
        "prefix": run_addressing.compose_state_relpath("workflow", key, segments
        ).as_posix(),
        "target_specs": target_specs,
        "workflow_definition_sha256": hashlib.sha256(
            definition_canonical.encode("utf-8")
        ).hexdigest(),
    }


def validate_unique_fan_out_materializations(
    child_selections: list[dict],
) -> list[dict]:
    specs = [selection_state_spec(selection) for selection in child_selections]
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


def _recorded_target_specs(pointer: dict | None) -> list[dict]:
    """The members a workflow pointer recorded, as target specs."""
    specs: list[dict] = []
    for item in (pointer or {}).get("child_revisions") or []:
        if not isinstance(item, dict):
            continue
        child_key, child_segments = run_addressing.split_target_instance_address(
            str(item.get("address"))
        )
        specs.append(
            {
                "kind": "target",
                "key": child_key,
                "segments": child_segments,
                "address": str(item.get("address")),
            }
        )
    return specs


def populate_workflow_child_slice(
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
    #(b3): the child owns its WHOLE cfg derivation, not two views of a
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
    target_context_path = src_root / "execution" / execution_run_context.EXECUTION_CONTEXT_FILENAME
    if target_context_path.is_file():
        (child_run_dir / "execution").mkdir(parents=True, exist_ok=True)
        shutil.copy2(target_context_path, child_run_dir / "execution" / execution_run_context.EXECUTION_CONTEXT_FILENAME)
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


def build_child_target_command(
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
        sys.executable, str(spec["ctl_entrypoint"]), "target",
        "--ctl-cfg", str(spec["ctl_cfg_root"]),
        "--ctl-profile", spec["ctl_profile"],
        "--ctl-state-local-root", str(spec["ctl_state_local_root"]),
        "--execution-runtime-mode", spec["execution_runtime_mode"],
        "--action", action or spec["action"],
        "--target", target_key,
        # The child runs UNDER the parent's ctl-state lock. Authorisation is a
        # single-use grant passed by ENVIRONMENT (see CHILD_LOCK_GRANT_ENV); the
        # run id below is provenance only, and is deliberately not a credential.
        "--parent-workflow-run-id", parent_run_id,
    ]
    # Read from the parent's own record rather than the frozen spec: the spec is
    # about how to INVOKE a child, the instance address is a fact about the parent,
    # and taking it from the record it is written in leaves nothing to drift.
    parent_instance_address = state_run_store.load_run_metadata(parent_run_dir).get("instance_address")
    if parent_instance_address:
        argv += ["--parent-workflow-instance-address", str(parent_instance_address)]
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
    for overlay in (spec.get("plt_overlays") or []):
        argv += ["--plt-overlays", overlay]
    for provider in (spec.get("force_skip_execution_identity_preflight_check") or []):
        argv += ["--force-skip-execution-identity-preflight-check", provider]
    for flag, enabled in (
        ("--agreed-defer-ctl-state-backend-sync", spec.get("agreed_defer_ctl_state_backend_sync")),
        ("--force-skip-ctl-state-backend-sync", spec.get("force_skip_ctl_state_backend_sync")),
        ("--force-skip-guardrails", spec.get("force_skip_guardrails")),
        ("--force-skip-full-cfg-validation-gate", spec.get("force_skip_full_cfg_validation_gate")),
        ("--skip-children-precheck", spec.get("skip_children_precheck")),
    ):
        if enabled:
            argv.append(flag)
    return argv


def build_workflow_identity_doc(
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


def workflow_target_key_entries(
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
    for imported in workflow.get("import_workflow_keys") or []:
        if imported in _seen or imported not in workflows:
            continue
        keys += workflow_target_key_entries(
            {**workflows[imported], "__name__": imported},
            workflows,
            label=label,
            _seen=(*_seen, name, imported),
        )
    target_keys = workflow.get("target_keys")
    branches = (
        [member.get("target_keys") or [] for member in (target_keys.get("members") or [])]
        if isinstance(target_keys, dict)
        else [target_keys or []]
    )
    for branch in branches:
        for entry in branch:
            key = entry.get("key") if isinstance(entry, dict) else entry
            if isinstance(key, str) and key and key not in keys:
                keys.append(key)
    return keys


def validate_all_workflow_instance_params(workflows: dict, targets: dict) -> None:
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
        member_keys = workflow_target_key_entries(
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
                    for branch in run_selectors._selector_branches(target.get("target_instance_params"))
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


def workflow_member_instance_params(
    workflow_cfg: dict, targets: dict, *, label: str
) -> tuple[list[str], bool]:
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


def resolve_declared_workflow_instance_params(
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


def validate_workflow_instance_params(
    declared, workflow_cfg: dict, targets: dict, *, label: str,
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
    union, union_is_complete = workflow_member_instance_params(
        workflow_cfg, targets, label=label
    )
    declared_list = resolve_declared_workflow_instance_params(
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


def build_procedure_cfg(
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
        execution_run_context.load_domain_registry(ctl_cfg_root), domain, label="synthetic procedure target domain"
    )
    resolved = {
        "source": source,
        "ref": ref,
        "procedure": procedure,
        "domains": [domain],
        "cfg_keys": {domain: ["*"]},
    }
    if execution_provider:
        # The synthetic target gets the same execution_identity block a declared
        # target has; a single --execution-role is bound under the key that
        # provider uses for this action — the engine asks, it does not decide.
        role_class = action_role_class or execution_providers.get_provider_adapter(
            execution_provider
        )._action_role_key(action, label="synthetic procedure target")
        resolved["execution_identities"] = catalog_targets.validate_target_execution_identities(
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


def validate_fan_out_param_collisions(
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
            "fan-out params cannot override CLI or ctl cfg values: "
            + "; ".join(collision_rows)
        )


def expand_fan_out(
    ctl_cfg_root: Path, fan_out_key: str, execution_context: dict[str, object]
) -> dict:
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
    param_sets = cfg_resources.collect_resource(ctl_cfg_root, "fan_out_param_sets", entry_depth=1)
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
            raise RuntimeError(
                f"❌ fan-out {fan_out_key!r} run[{i}] must set workflow_key"
            )
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
                if not isinstance(extra_key, str) or not execution_references.CONTEXT_KEY_RE.fullmatch(extra_key):
                    raise RuntimeError(f"❌ {run_label}: key {extra_key!r} must be a valid identifier")
                if isinstance(extra_value, (dict, list)):
                    raise RuntimeError(f"❌ {run_label}.{extra_key} must be a scalar")
            if "domain" in extra_params:
                execution_run_context.validate_domain_value(domains, extra_params["domain"], label=run_label)
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
                raise RuntimeError(f"❌ {member_label}: selectors must be a member field, not a param")
            if "domain" in params:
                execution_run_context.validate_domain_value(domains, params["domain"], label=member_label)
            if extra_params:
                collisions = sorted(set(params) & set(extra_params))
                if collisions:
                    raise RuntimeError(
                        f"❌ {member_label} already declares {collisions} also set by "
                        f"fan-out {fan_out_key!r} run[{i}] extra_params; define each param "
                        "in one place"
                    )
            if not run_selectors.selector_matches(
                member.get("selectors"), execution_context,
                label=member_label, structured_only=True,
            ):
                continue
            children.append(
                {
                    "kind": kind,
                    "key": key,
                    "params": {**params, **(extra_params or {})},
                    # One param set may serve several runs of the same
                    # workflow (each pinned by different extra_params), so the
                    # member name alone no longer identifies a child.
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
        raise RuntimeError(f"❌ fan-out {fan_out_key!r} failure_mode must be 'stop' or 'continue'")
    return {"failure_mode": failure_mode, "children": children}


def wrap_fan_out_preflight_child(
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
