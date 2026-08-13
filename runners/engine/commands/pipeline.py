"""The entry point that runs targets.

Everything else in the engine exists to be composed here. This module holds no
logic of its own beyond ordering and error handling — when a rule turns out to
live here, it belongs in the module that owns the concept."""

import hashlib
import json
import logging
import shutil
from pathlib import Path

from engine.catalog import target_catalog
from engine.catalog import workflow as catalog_workflow
from engine.cfg import materialize as cfg_materialize
from engine.cfg import secrets as cfg_secrets
from engine.cfg import tooling as cfg_tooling
from engine.cfg import validate as cfg_validate
from engine.cfg import views as cfg_views
from engine.commands import maintenance as commands_maintenance
from engine.commands import selection as commands_selection
from engine.commands import target_runner as commands_target_runner
from engine.execution import providers as execution_providers
from engine.execution import run_context as execution_run_context
from engine.guardrails import verify as guardrails_verify
from engine.kernel import git as kernel_git
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.plt import dispatch as plt_dispatch
from engine.run import actions as run_actions
from engine.run import policy as run_policy
from engine.run import request as run_request
from engine.run import selection as run_selection
from engine.state import lifecycle as state_lifecycle
from engine.state import run_store as state_run_store
from engine.state import status_outdating as state_status_outdating
from engine.state import sync as state_sync


def run_targets(
    request: run_request.RunRequest,
    selection: run_selection.RunSelection,
    *,
    active_target_runs: dict,
    plt_targets_dir_path: Path,
    execution_context_path: Path,
    tooling_refs: dict,
    credential_refresh_modes: dict | None,
    child_command_spec: dict | None,
    secret_store,
    plt_provider_dispatch: plt_dispatch.ProviderDispatch,
) -> None:
    """Clone and run all active target runs.

    `secret_store` is passed rather than built here so the secrets registry is
    read once for the whole run, not once per target."""

    commands_target_runner.TargetRunner(
        active_target_runs=active_target_runs,
        run_dir=request.run_dir,
        plt_targets_dir_path=plt_targets_dir_path,
        execution_context_path=execution_context_path,
        action=request.action,
        execution_context=selection.execution_context,
        run_id=request.run_id,
        tooling_refs=tooling_refs,
        use_local_tooling_cfg=request.use_local_tooling_cfg,
        provider_adapter=selection.provider_adapter,
        provider_catalogs=selection.provider_catalogs,
        credential_acquisition=request.credential_acquisition,
        execution_runtime_mode=request.execution_runtime_mode,
        execution_access_modes=request.execution_access_modes,
        provider_options=request.provider_options,
        skip_up_to_date=request.skip_up_to_date,
        child_command_spec=child_command_spec,
        credential_refresh_modes=credential_refresh_modes,
        secret_store=secret_store,
        plt_provider_dispatch=plt_provider_dispatch,
    ).run()


def _arm_publication(
    request: run_request.RunRequest, selection: run_selection.RunSelection
) -> Path:
    """Freeze the backend-readiness fact, verify ctl guardrails, and arm publication.

    Nothing may publish before this: the defer gate decides whether the run is
    ALLOWED to proceed without a backend, and that answer has to be frozen before
    anything writes.
    """

    execution_context = selection.execution_context
    workflow_cfg, action_cfg = selection.workflow_cfg, selection.action_cfg
    selected_graph_provisions_backend = request.parent_graph_provisions_ctl_state_backend
    backend_absence_confirmed = request.parent_ctl_state_backend_absence_confirmed
    if request.agreed_defer_ctl_state_backend_sync and not selected_graph_provisions_backend:
        graph_probe = commands_maintenance.CtlStateMaintenance.inspect_selected_graph_backend(
            [selection],
            request.ctl_cfg_root,
            credential_acquisition=request.credential_acquisition,
            execution_access_modes=request.execution_access_modes,
            provider_options=request.provider_options,
        )
        selected_graph_provisions_backend = True
        backend_absence_confirmed = graph_probe["status"] == "absent"
        if not backend_absence_confirmed:
            raise RuntimeError(
                "❌ --agreed-defer-ctl-state-backend-sync is not applicable: "
                "the selected backend already exists"
            )
    state_run_store.update_run_metadata(
        request.run_dir,
        {
            "selected_graph_provisions_ctl_state_backend": selected_graph_provisions_backend,
            "ctl_state_backend_absence_confirmed_at_start": backend_absence_confirmed,
        },
    )

    # Resolve the run's namespace and arm publication only after the graph-level
    # defer gate has frozen its provider-classified readiness fact.
    ctl_state_namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(
        request.ctl_cfg_root, execution_context
    )
    guardrails_verify.verify_ctl_guardrails(
        request.ctl_cfg_root,
        request.guardrails_cfg_root,
        execution_context,
    )
    state_sync.PUBLICATION.configure(
        request.ctl_cfg_root,
        request.ctl_profile,
        ctl_state_namespace_key,
        execution_context,
        request.run_dir,
        agreed_defer_ctl_state_backend_sync=request.agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=request.force_skip_ctl_state_backend_sync,
        provisions_ctl_state_backend=state_sync.run_provisions_ctl_state_backend(
            workflow_cfg, action_cfg
        ),
        selected_graph_provisions_ctl_state_backend=selected_graph_provisions_backend,
        backend_absence_confirmed=backend_absence_confirmed,
        execution_access_modes=request.execution_access_modes,
        provider_options=request.provider_options,
        credential_acquisition=request.credential_acquisition,
    )
    execution_context_path = execution_run_context.write_execution_context_artifact(
        request.run_dir, execution_context
    )
    return execution_context_path


def _precheck_target_cfg(
    request: run_request.RunRequest,
    selection: run_selection.RunSelection,
    *,
    active_target_runs: dict,
    provider_dispatch: plt_dispatch.ProviderDispatch,
    run_type_now: str,
) -> None:
    """Render and guard-verify every selected target's cfg before any of them runs."""

    execution_context = selection.execution_context
    precheck_runs = active_target_runs
    if (
        request.skip_children_precheck
        and state_run_store.load_run_metadata(request.run_dir).get("run_type") == "workflow"
    ):
        logging.info(
            "Skipping the child pre-check (--skip-children-precheck); each target "
            "renders and validates its own cfg when it runs"
        )
        precheck_runs = {}
        state_run_store.update_run_metadata(request.run_dir, {"skipped_children_precheck": True})
    for target_run_id, target_run in precheck_runs.items():
        if not target_run.get("domains"):
            continue
        target_context = execution_run_context.build_target_execution_context(
            target_run_id, target_run, execution_context
        )
        target_cfg_dir = cfg_views.target_cfg_view_dir(request.run_dir, run_type_now, target_run_id)
        if provider_dispatch.enabled:
            target_rendered_dir, provider_selection = provider_dispatch.prepare_target_view(
                target_run_id,
                target_run,
                execution_context=target_context,
                target_cfg_dir=target_cfg_dir,
                scope_params=execution_run_context.scope_params_from_context(target_context),
            )
            target_run["plt_provider"] = provider_selection
        else:
            target_rendered_dir = cfg_views.prepare_target_cfg_view(
                target_run_id,
                target_run,
                plt_cfg_root=request.plt_cfg_root,
                target_cfg_dir=target_cfg_dir,
                ctl_profile=request.ctl_profile,
                scope_params=execution_run_context.scope_params_from_context(target_context),
                execution_context=target_context,
            )
        guardrails_verify.verify_guardrails(
            request.ctl_cfg_root,
            request.plt_cfg_root,
            request.guardrails_cfg_root,
            target_rendered_dir,
            target_context,
            execution_run_context.scope_params_from_context(target_context),
        )


def _child_command_spec(
    request: run_request.RunRequest,
    selection: run_selection.RunSelection,
    *,
    final_plt_overlays: list | None,
) -> dict:
    """ONE frozen spec describing this invocation, from which every child's argv derives.

    Built here because this is the only place holding all of it.
    """

    execution_context = selection.execution_context
    run_metadata_now = state_run_store.load_run_metadata(request.run_dir)
    run_metadata_now = state_run_store.load_run_metadata(request.run_dir)
    child_command_spec = {
        "ctl_entrypoint": kernel_paths.ctl_utils_root().parent
        / "atlas-ctl-orchestrator"
        / "ctl.py",
        "ctl_cfg_root": request.ctl_cfg_root,
        "ctl_profile": request.ctl_profile,
        "ctl_state_local_root": run_metadata_now.get("ctl_state_local_root"),
        "execution_runtime_mode": request.execution_runtime_mode,
        "action": request.action,
        "providers": list(execution_providers.run_providers(execution_context)),
        "execution_params": dict(request.execution_params),
        "provider_options": dict(request.provider_options or {}),
        "execution_access_modes": dict(request.execution_access_modes or {}),
        "plt_overlays": list(final_plt_overlays or []),
        "force_skip_execution_identity_preflight_check": list(
            request.force_skip_execution_identity_preflight_check or []
        ),
        "agreed_defer_ctl_state_backend_sync": request.agreed_defer_ctl_state_backend_sync,
        "force_skip_ctl_state_backend_sync": request.force_skip_ctl_state_backend_sync,
        "force_skip_guardrails": request.force_skip_guardrails,
        "force_skip_full_cfg_validation_gate": request.force_skip_full_cfg_validation_gate,
        "skip_children_precheck": request.skip_children_precheck,
        "credential_refresh_modes": request.credential_refresh_modes,
    }
    kernel_yaml_io.write_yaml_file(
        request.artifacts_dir / "child_command_spec.yaml",
        {k: (str(v) if isinstance(v, Path) else v) for k, v in child_command_spec.items()},
    )
    return child_command_spec


def _write_run_artifacts(
    request: run_request.RunRequest,
    selection: run_selection.RunSelection,
    *,
    active_target_runs: dict,
    pipeline_run_cfg_path: Path,
    plt_targets_dir_path: Path,
    run_type_now: str,
) -> Path:
    """Write everything that makes the run self-describing, and distribute target views.

    Answers with the distribution root, which `run_cfg_distribution` may move.
    """

    execution_context = selection.execution_context
    workflow_cfg, action_cfg, refs = selection.workflow_cfg, selection.action_cfg, selection.refs
    require_commit_refs = selection.require_commit_refs
    catalog_workflow.WorkflowArtifacts.write_target_flow(
        request.ctl_cfg_root,
        request.artifacts_dir,
        ctl_profile=request.ctl_profile,
        execution_context=execution_context,
        action=request.action,
        workflow_name=request.workflow_name,
        target_repo_key=request.target_repo_key,
        require_target_ref=request.require_target_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )

    # Write git metas
    cfg_materialize.write_git_metas(
        request.ctl_cfg_root,
        request.plt_cfg_root,
        request.guardrails_cfg_root,
        request.artifacts_dir,
    )

    # Resolved ctl cfg snapshot (self-describing run, next to cfg/plt/)
    cfg_materialize.write_ctl_cfg_snapshot(
        request.run_dir,
        ctl_profile=request.ctl_profile,
        ctl_profile_policy_cfg=run_policy.ctl_profile_policy(
            request.ctl_cfg_root, request.ctl_profile
        ),
        action=request.action,
        workflow_cfg=workflow_cfg,
        action_cfg=action_cfg,
        active_target_runs=active_target_runs,
        refs=refs,
        execution_context=execution_context,
    )

    pipeline_cfg_now = kernel_yaml_io.load_yaml(pipeline_run_cfg_path) or {}
    pipeline_cfg_now["target_runs"] = active_target_runs
    kernel_yaml_io.write_yaml_file(pipeline_run_cfg_path, pipeline_cfg_now)

    # Distribute target_run input views from the rendered tree
    plt_targets_dir_path = cfg_materialize.run_cfg_distribution(
        pipeline_run_cfg_path, plt_targets_dir_path, run_type_now
    )
    cfg_views.finalize_target_cfg_view_facts(
        active_target_runs, plt_targets_dir_path, pipeline_run_cfg_path
    )
    if state_run_store.load_run_metadata(request.run_dir).get("run_type") == "target":
        only_target = next(iter(active_target_runs.values()), None)
        if only_target and only_target.get("target_definition") is not None:
            kernel_yaml_io.write_yaml_file(
                request.run_dir / "cfg" / "ctl" / "target_definition.yaml",
                only_target["target_definition"],
            )
        if only_target:
            state_run_store.update_run_metadata(
                request.run_dir,
                {
                    key: only_target[key]
                    for key in ("target_definition_sha256", "target_cfg_view_sha256")
                },
            )
    return plt_targets_dir_path


def _freeze_source_facts(request: run_request.RunRequest, active_target_runs: dict) -> None:
    """Freeze the commit facts the opt-in committed-rerun gate reads."""

    # Freeze the commit facts used by the opt-in committed-rerun gate.
    cfg_source_commit, cfg_source_state = kernel_git.git_source_facts(request.plt_cfg_root)
    for target_run in active_target_runs.values():
        source_commit, target_source_state = state_run_store.target_run_source_facts(target_run)
        target_run["source_commit"] = source_commit
        target_run["cfg_source_commit"] = cfg_source_commit
        target_run["source_state"] = (
            "clean" if target_source_state == "clean" and cfg_source_state == "clean" else "dirty"
        )
        target_run["ref_policy"] = request.ctl_ref_policy
    if state_run_store.load_run_metadata(request.run_dir).get("run_type") == "target":
        only_target = next(iter(active_target_runs.values()), None)
        if only_target:
            state_run_store.update_run_metadata(
                request.run_dir,
                {
                    key: only_target[key]
                    for key in ("source_commit", "cfg_source_commit", "source_state", "ref_policy")
                },
            )


def run_pipeline(
    request: run_request.RunRequest,
    *,
    preflight_selection: run_selection.RunSelection | None = None,
) -> None:
    """Run a declared workflow, declared target, or synthetic repo-local procedure.

    The caller passes target_run repo settings and pre-created run/log directories.

    """

    if preflight_selection is None:
        selection, _ = commands_selection.resolve_and_preflight_execution_identities(
            request,
        )
    else:
        selection = preflight_selection
    execution_context = selection.execution_context
    scope_params = selection.scope_params
    if selection.is_workflow:
        definition_canonical = json.dumps(
            selection.workflow_cfg, separators=(",", ":"), sort_keys=True
        )
        state_run_store.update_run_metadata(
            request.run_dir,
            {
                "workflow_definition_sha256": hashlib.sha256(
                    definition_canonical.encode("utf-8")
                ).hexdigest()
            },
        )
    require_commit_refs = selection.require_commit_refs
    workflow_cfg = selection.workflow_cfg
    action_cfg = selection.action_cfg
    refs = selection.refs
    active_target_runs = selection.active_target_runs
    # Recorded as soon as the composition is RESOLVED, not after the
    # cfg and guardrail phases. Those take tens of seconds, and a status read
    # during them showed a running workflow with no members — the composition
    # was known the whole time and simply had not been written down.
    if state_run_store.load_run_metadata(request.run_dir).get("run_type") == "workflow":
        state_lifecycle.record_workflow_members(request.run_dir, active_target_runs, workflow_cfg)
    provider_adapter = selection.provider_adapter
    provider_catalogs = selection.provider_catalogs

    # Preserve the runtime binding contract after the live gate passes.
    adapter_access_mode, adapter_options = execution_providers.provider_inputs(
        execution_providers.run_provider(execution_context),
        request.execution_access_modes,
        request.provider_options,
    )
    provider_adapter.validate_active_target_access(
        active_target_runs,
        provider_catalogs,
        execution_context=execution_context,
        credential_acquisition=request.credential_acquisition,
        execution_access_mode=adapter_access_mode,
        provider_options=adapter_options,
    )

    execution_context_path = _arm_publication(request, selection)

    if request.use_local_tooling_cfg:
        tooling_refs = cfg_tooling.load_local_tooling_cfg(request.ctl_cfg_root)
    else:
        tooling_refs = refs.get("global") or {}
        cfg_validate.CommitPinning(request.ctl_ref_policy).check_tooling_refs(tooling_refs)

    logging.info(f"Selector policy validation passed: ctl_profile={request.ctl_profile}")

    provider_dispatch = plt_dispatch.ProviderDispatch(request.ctl_cfg_root, request.plt_cfg_root)

    # Prepare pipeline config
    active_target_runs, pipeline_run_cfg_path, final_plt_overlays = (
        commands_selection.prepare_pipeline_cfg(
            request.plt_cfg_root,
            workflow_cfg,
            action_cfg,
            request.artifacts_dir,
            request.ctl_profile,
            scope_params=scope_params,
            execution_context=execution_context,
            target_repo_key=request.target_repo_key,
            require_target_ref=request.require_target_ref,
            require_commit_refs=require_commit_refs,
            refs=refs,
            active_target_runs=active_target_runs,
        )
    )
    state_run_store.update_run_metadata(request.run_dir, {"plt_overlays": final_plt_overlays})
    # chain, PER TARGET: each target_run merges its own
    # scopes with its own overlays, renders them, is guard-verified against its own
    # rendered values, and receives its projected key view. Nothing is shared, so a
    # target's cfg cannot be reshaped by another target's declarations.
    run_type_now = str(state_run_store.load_run_metadata(request.run_dir).get("run_type"))
    plt_targets_dir_path = cfg_views.target_cfg_views_root(request.run_dir, run_type_now)
    # For a WORKFLOW this loop is a PRE-CHECK of its CHILDREN — each
    # spawned target re-derives and re-validates its own cfg when it runs. Skipping
    # trades fail-fast (catch a bad target before target #1 mutates anything) for not
    # doing the work twice. A target run has no children, so the flag is a no-op.
    _precheck_target_cfg(
        request,
        selection,
        active_target_runs=active_target_runs,
        provider_dispatch=provider_dispatch,
        run_type_now=run_type_now,
    )

    if request.procedure_run:
        target_keys = request.procedure_run.get("affected_target_keys") or []
        if request.action in run_actions.MUTATING_ACTIONS and not target_keys:
            raise RuntimeError("❌ mutating procedure runs require affected_target_keys")
    else:
        target_keys = target_catalog.ActiveTargetRuns.target_keys(active_target_runs)
    state_run_store.record_run_target_keys(request.run_dir, target_keys)
    run_metadata = state_run_store.load_run_metadata(request.run_dir)
    ctl_state_local_root_value = run_metadata.get("ctl_state_local_root")
    if isinstance(ctl_state_local_root_value, str) and ctl_state_local_root_value:
        state_status_outdating.mark_removed_definitions_outdated(
            Path(ctl_state_local_root_value), request.ctl_cfg_root
        )

    plt_targets_dir_path = _write_run_artifacts(
        request,
        selection,
        active_target_runs=active_target_runs,
        pipeline_run_cfg_path=pipeline_run_cfg_path,
        plt_targets_dir_path=plt_targets_dir_path,
        run_type_now=run_type_now,
    )
    # Prepared snapshot: cfg layers + run-level metadata are immutable from here.
    state_sync.PUBLICATION.push("preparation complete")

    _freeze_source_facts(request, active_target_runs)

    # ONE frozen spec describing this invocation, from which every
    # child target's argv is derived. Built here because run_pipeline is the only
    # place that holds all of it; passing scattered locals into run_targets is how
    # A flag gets forgotten and a child silently runs differently.
    child_command_spec = _child_command_spec(
        request, selection, final_plt_overlays=final_plt_overlays
    )

    # Run target runs
    credential_refresh_modes = run_policy.validate_credential_refresh_modes(
        request.ctl_cfg_root,
        request.ctl_profile,
        request.credential_refresh_modes,
        request.providers,
        request.execution_access_modes,
    )
    run_targets(
        request,
        selection,
        active_target_runs=active_target_runs,
        plt_targets_dir_path=plt_targets_dir_path,
        execution_context_path=execution_context_path,
        tooling_refs=tooling_refs,
        credential_refresh_modes=credential_refresh_modes,
        child_command_spec=child_command_spec,
        secret_store=cfg_secrets.SecretStore(
            request.ctl_cfg_root,
            execution_context=execution_context,
            credential_acquisition=request.credential_acquisition,
            execution_access_modes=request.execution_access_modes,
            provider_options=request.provider_options,
        ),
        plt_provider_dispatch=provider_dispatch,
    )

    # (b3): a WORKFLOW owns ordering, policy and the run verdict — not cfg.
    # Each child has received its complete derivation, so the workflow-side copy is
    # dropped rather than published twice.
    if state_run_store.load_run_metadata(request.run_dir).get("run_type") == "workflow":
        workflow_plt_dir = request.run_dir / "cfg" / "plt"
        if workflow_plt_dir.exists():
            shutil.rmtree(workflow_plt_dir)

    state_lifecycle.print_run_summary(request.run_id, request.log_file)
