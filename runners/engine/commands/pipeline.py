"""The entry point that runs targets.

Everything else in the engine exists to be composed here. This module holds no
logic of its own beyond ordering and error handling — when a rule turns out to
live here, it belongs in the module that owns the concept."""

import hashlib
import json
import logging
import os
import shutil
from pathlib import Path

from engine.catalog import targets as catalog_targets
from engine.catalog import workflow as catalog_workflow
from engine.cfg import materialize as cfg_materialize
from engine.cfg import secrets as cfg_secrets
from engine.cfg import tooling as cfg_tooling
from engine.cfg import validate as cfg_validate
from engine.cfg import views as cfg_views
from engine.commands import maintenance as commands_maintenance
from engine.commands import selection as commands_selection
from engine.execution import providers as execution_providers
from engine.execution import run_context as execution_run_context
from engine.guardrails import verify as guardrails_verify
from engine.kernel import git as kernel_git
from engine.kernel import paths as kernel_paths
from engine.kernel import process as kernel_process
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import policy as run_policy
from engine.state import lifecycle as state_lifecycle
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine.state import sync as state_sync


def run_targets(
    active_target_runs: dict,
    run_dir: Path,
    plt_targets_dir_path: Path,
    execution_context_path: Path,
    action: str,
    execution_context: dict[str, object],
    run_id: str,
    tooling_refs: dict,
    use_local_tooling_cfg: bool,
    provider_adapter,
    provider_catalogs: dict,
    provider_implementation_key: str,
    execution_runtime_mode: str,  # required, no default — the CLI (--execution-runtime-mode) supplies it
    execution_access_modes: dict[str, str] | None = None,
    provider_options: dict[str, str] | None = None,
    skip_up_to_date: bool = False,
    child_command_spec: dict | None = None,
    credential_refresh_modes: dict | None = None,
    secret_store=None,
) -> None:
    """Clone and run all active target runs.

    `secret_store` is passed rather than built here so the secrets registry is
    read once for the whole run, not once per target."""
    os.chdir(run_dir)
    tooling_env = cfg_tooling.build_tooling_env(tooling_refs)
    # CTL owns the execution box. It invokes the ctl-owned runtime
    # dispatcher (run_step.sh) — never a per-target_run run script — passing the box
    # spec the target_run declared (image / docker_build) plus the active runtime and
    # tooling source. The target_run carries only src/step.sh + step.yaml.
    runtime_dispatcher = str(cfg_materialize.materialize_step_utils(run_dir) / "run_step.sh")
    tooling_mode = "repo_path" if use_local_tooling_cfg else "repo_url"
    mutation_marked = False
    child_revisions: list[dict] = []
    for target_run_id, target_run in active_target_runs.items():
        # A member may declare its OWN action, and the child is spawned
        # with it. Everything that reads back what the child did must use the same
        # action — `action` is the RUN's, so a `plan` member inside a
        # `provision` run publishes to committed/plan.yaml while the parent looks
        # in committed/mutative.yaml and finds a composition that recorded nothing.
        member_action = target_run.get("action") or action
        state_lifecycle.log_target_run_banner(f"[{member_action}] [{target_run_id}]")
        if skip_up_to_date:
            revision = state_status.up_to_date_child_revision(
                run_dir, target_run, execution_context, member_action
            )
            if revision is not None:
                logging.info(
                    "Skipping committed target instance %s (published result is reusable)",
                    revision["address"],
                )
                child_revisions.append(revision)
                continue

        # A WORKFLOW spawns `ctl.py target` per child, so a target
        # runs by exactly the same path standalone and inside a workflow. The child
        # builds its own cfg, context and log; `run_and_log` streams its output into
        # this run's log, so the workflow keeps the aggregate. It executes under this
        # run's ctl-state lock — flock is exclusive and non-blocking, so acquiring it
        # again would fail outright.
        if child_command_spec is not None and state_run_store.load_run_metadata(run_dir).get(
            "run_type"
        ) == "workflow":
            target_key = target_run.get("target")
            argv = catalog_workflow.build_child_target_command(
                child_command_spec, target_key,
                parent_run_dir=run_dir, parent_run_id=run_id,
                action=target_run.get("action"),
            )
            logging.info("Spawning child target run: %s", target_key)
            child_env = dict(os.environ)
            child_env[state_run_store.CHILD_LOCK_GRANT_ENV] = state_run_store.mint_child_lock_grant(
                Path(child_command_spec["ctl_state_local_root"]),
                child_kind="target", child_key=target_key,
            )
            # The workflow's OWN slot has to record the mutation. This branch
            # spawns and `continue`s, so it never reaches the inline mark below
            # — a workflow run therefore reported `mutation_started: false` no
            # matter how much its children changed, and `partial` could not
            # surface on the composition row at all. Marked BEFORE the child
            # runs, on the same conservative rule as the inline path: from here
            # resources may change, and claiming possible damage beats denying it.
            if member_action in run_actions.MUTATING_ACTIONS and not mutation_marked:
                state_lifecycle.mark_mutation_started(run_dir, target_run_id)
                mutation_marked = True
            kernel_process.run_and_log(argv, cwd=str(run_dir), env=child_env)
            revision = state_status.latest_child_revision(
                run_dir, target_run, execution_context, member_action
            )
            if revision is not None:
                child_revisions.append(revision)
            continue

        repo_path, target_env = cfg_materialize.prepare_target_repo(
            target_run_id,
            target_run,
            run_dir,
            tooling_env,
            secret_store=secret_store,
            provider_adapter=provider_adapter,
            provider_catalogs=provider_catalogs,
            execution_context=execution_context,
            provider_implementation_key=provider_implementation_key,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            )

        procedure_key = target_run.get("procedure")
        if not isinstance(procedure_key, str) or not procedure_key:
            raise RuntimeError(f"❌ target run {target_run_id!r} must define a non-empty procedure")
        target_view_dir = (
            plt_targets_dir_path
            if (plt_targets_dir_path / "input").is_dir()
            else plt_targets_dir_path / target_run_id
        )
        origin_cfg_path = target_view_dir / "input"
        if not origin_cfg_path.is_dir():
            raise RuntimeError(f"❌ target_run input cfg dir not found for target_run {target_run_id!r}: {origin_cfg_path}")
        target_cfg_dir = target_view_dir / "resolved"
        os.makedirs(target_cfg_dir, exist_ok=True)
        target_state_run_dir, target_instance_address = state_lifecycle.begin_workflow_target_run(
            run_dir, target_run, execution_context
        )
        target_artifacts_dir = (
            target_state_run_dir / "artifacts"
            if target_instance_address is not None
            else run_dir / "artifacts" / "targets" / target_run_id
        )
        os.makedirs(target_artifacts_dir, exist_ok=True)

        copied_execution_context = execution_run_context.ensure_repo_execution_context(repo_path, execution_context_path)
        # Everything this target emits also lands in its own log.
        target_log = state_run_store.target_run_log(
            target_state_run_dir if target_instance_address is not None else None
        )
        target_log.__enter__()
        try:
            repo_step_ids, repo_steps = cfg_materialize.get_repo_local_steps(repo_path, member_action, procedure_key)
            run_manifest = {
                "run_id": run_id,
                "branch": target_run.get("branch"),
                "commit": target_run.get("commit"),
                "action": member_action,
                "procedure": procedure_key,
                "active_steps": repo_step_ids,
                "origin_cfg": str(origin_cfg_path),
                "execution_context_file": str(execution_context_path),
                "execution_context_keys": sorted(execution_context),
            }
            logging.info(json.dumps(run_manifest, indent=4))

            if member_action in run_actions.MUTATING_ACTIONS and not mutation_marked:
                state_lifecycle.mark_mutation_started(run_dir, target_run_id)
                mutation_marked = True

            for repo_step in repo_steps:
                repo_step_id = repo_step["id"]
                repo_step_path = repo_step["path"]
                state_lifecycle.log_target_run_banner(f"[{member_action}] [{target_run_id}] [{repo_step_id}]", ch="-")
                repo_step_runtime = repo_step.get("runtime", {})
                supported = set(repo_step_runtime.get("supported_execution_runtime_modes", run_policy.EXECUTION_RUNTIME_MODES))
                if execution_runtime_mode not in supported:
                    raise RuntimeError(
                        f"❌ execution runtime {execution_runtime_mode!r} not supported by target_run "
                        f"{target_run_id}/{repo_step_id} (supported: {sorted(supported)})"
                    )
                step_run_cmd = [runtime_dispatcher]
                repo_step_env = dict(target_env)
                repo_step_env["ATLAS_EXECUTION_CONTEXT_FILE"] = execution_run_context.EXECUTION_CONTEXT_FILENAME
                repo_step_env["cfg_keys"] = json.dumps(repo_step.get("cfg_keys") or {})
                repo_step_env["STEP_WRITE_VALUES_JSON"] = (
                    "true" if repo_step_runtime.get("values_json", True) else "false"
                )
                repo_step_env["STEP_WRITE_ENV_SH"] = (
                    "true" if repo_step_runtime.get("env_sh", True) else "false"
                )
                repo_step_env["origin_cfg_base_dir_path"] = str(origin_cfg_path)
                repo_step_env["TARGET_CFG_DIR"] = str(target_cfg_dir)
                repo_step_env["TARGET_ARTIFACTS_DIR"] = str(target_artifacts_dir)
                # CTL owns the box; hand the dispatcher the runtime + the
                # target_run's declared box spec. step_dir locates src/step.sh in the repo.
                repo_step_env["ATLAS_EXECUTION_RUNTIME_MODE"] = execution_runtime_mode
                repo_step_env["ATLAS_STEP_NAME"] = kernel_process._step_box_name(target_run_id, repo_step_id)
                repo_step_env["ATLAS_STEP_IMAGE"] = repo_step_runtime["image"]
                repo_step_env["ATLAS_STEP_DOCKER_BUILD"] = (
                    "true" if repo_step_runtime.get("docker_build", False) else "false"
                )
                repo_step_env["step_dir"] = repo_step_path
                repo_step_env["local_step_tooling_mode"] = tooling_mode
                if (credential_refresh_modes or {}).get(
                    getattr(provider_adapter, "PROVIDER_NAME", "")
                ) == "per_step":
                    cfg_materialize.rebind_step_credentials(
                        list(repo_step.get("providers") or []),
                        target_run_id=target_run_id,
                        target_run=target_run,
                        step_env=repo_step_env,
                        provider_adapter=provider_adapter,
                        provider_catalogs=provider_catalogs,
                        execution_context=execution_context,
                        provider_implementation_key=provider_implementation_key,
                        execution_access_modes=execution_access_modes,
                        provider_options=provider_options,
                    )

                logging.info(" ".join(step_run_cmd))
                kernel_process.run_and_log(
                    step_run_cmd,
                    cwd=repo_path,
                    env=repo_step_env,
                )
            state_sync.PUBLICATION.push(f"target_run {target_run_id} completed")
            if target_instance_address is not None:
                # Fill the child's target-level slice (cfg, execution
                # context, source refs) now that resolved cfg exists — before the
                # child pointer is published.
                catalog_workflow.populate_workflow_child_slice(
                    target_state_run_dir,
                    target_run,
                    target_run_id,
                    plt_targets_dir_path,
                    execution_context,
                )
                revision = state_lifecycle.finish_workflow_target_run(target_state_run_dir)
                if revision is not None:
                    child_revisions.append(revision)
        except BaseException as error:
            if target_instance_address is not None:
                state_lifecycle.finish_workflow_target_run(target_state_run_dir, error=error)
            raise
        finally:
            target_log.__exit__(None, None, None)
            repo_execution_context_path = repo_path / execution_run_context.EXECUTION_CONTEXT_FILENAME
            if copied_execution_context and repo_execution_context_path.is_file():
                repo_execution_context_path.unlink()

    if child_revisions:
        state_run_store.update_run_metadata(run_dir, {"child_revisions": child_revisions})


def run_pipeline(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    ctl_profile: str,
    execution_params: dict[str, str],
    ctl_ref_policy: str,
    action: str,
    workflow_name: str | None,
    run_id: str,
    target_repo_key: str,
    require_target_ref: bool,
    use_local_tooling_cfg: bool,
    provider_implementation_key: str,
    run_dir: Path,
    artifacts_dir: Path,
    log_file: Path,
    provider_options: dict[str, str] | None,
    execution_runtime_mode: str,  # required, no default — the CLI (--execution-runtime-mode) supplies it
    target_name: str | None = None,
    procedure_run: dict | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
    force_skip_guardrails: bool = False,
    force_skip_full_cfg_validation_gate: bool = False,
    execution_access_modes: dict[str, str] | None = None,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
    providers: list[str] | tuple[str, ...] = (),
    skip_up_to_date: bool = False,
    credential_refresh_modes: dict | None = None,
    skip_children_precheck: bool = False,
    parent_graph_provisions_ctl_state_backend: bool = False,
    parent_ctl_state_backend_absence_confirmed: bool = False,
    preflight_selection: dict | None = None,
) -> None:
    """

    run a declared workflow, declared target, or synthetic repo-local procedure.

    The caller passes target_run repo settings and pre-created run/log directories.
    """

    if preflight_selection is None:
        selection, _ = commands_selection.resolve_and_preflight_execution_identities(
            ctl_cfg_root,
            ctl_profile,
            execution_params,
            ctl_ref_policy,
            action,
            workflow_name,
            # A run DECLARES its providers; without this the selection resolves
            # with none and every provider lookup fails ("no providers declared")
            providers=providers,
            target_repo_key=target_repo_key,
            require_target_ref=require_target_ref,
            provider_implementation_key=provider_implementation_key,
            execution_runtime_mode=execution_runtime_mode,
            provider_options=provider_options,
            execution_access_modes=execution_access_modes,
            artifacts_dir=artifacts_dir,
            gates_dir=cfg_materialize.run_gates_dir(run_dir),
            target_name=target_name,
            procedure_run=procedure_run,
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            force_skip_guardrails=force_skip_guardrails,
            force_skip_full_cfg_validation_gate=(
                force_skip_full_cfg_validation_gate
            ),
            force_skip_execution_identity_preflight_check=(
                force_skip_execution_identity_preflight_check
            ),
        )
    else:
        selection = preflight_selection
    execution_context = selection["execution_context"]
    scope_params = selection["scope_params"]
    if selection.get("selection_kind") == "workflow":
        definition_canonical = json.dumps(
            selection["workflow_cfg"], separators=(",", ":"), sort_keys=True
        )
        state_run_store.update_run_metadata(
            run_dir,
            {
                "workflow_definition_sha256": hashlib.sha256(
                    definition_canonical.encode("utf-8")
                ).hexdigest()
            },
        )
    require_commit_refs = selection["require_commit_refs"]
    workflow_cfg = selection["workflow_cfg"]
    action_cfg = selection["action_cfg"]
    refs = selection["refs"]
    active_target_runs = selection["active_target_runs"]
    # Recorded as soon as the composition is RESOLVED, not after the
    # cfg and guardrail phases. Those take tens of seconds, and a status read
    # during them showed a running workflow with no members — the composition
    # was known the whole time and simply had not been written down.
    if state_run_store.load_run_metadata(run_dir).get("run_type") == "workflow":
        state_lifecycle.record_workflow_members(run_dir, active_target_runs, workflow_cfg)
    provider_adapter = selection["provider_adapter"]
    provider_catalogs = selection["provider_catalogs"]

    # Preserve the runtime binding contract after the live gate passes.
    adapter_access_mode, adapter_options = execution_providers.provider_inputs(
        execution_providers.run_provider(execution_context), execution_access_modes, provider_options
    )
    provider_adapter.validate_active_target_access(
        active_target_runs,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=adapter_access_mode,
        provider_options=adapter_options,
    )

    selected_graph_provisions_backend = parent_graph_provisions_ctl_state_backend
    backend_absence_confirmed = parent_ctl_state_backend_absence_confirmed
    if agreed_defer_ctl_state_backend_sync and not selected_graph_provisions_backend:
        graph_probe = commands_maintenance.inspect_selected_graph_ctl_state_backend(
            [selection],
            ctl_cfg_root,
            implementation_key=provider_implementation_key,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
        )
        selected_graph_provisions_backend = True
        backend_absence_confirmed = graph_probe["status"] == "absent"
        if not backend_absence_confirmed:
            raise RuntimeError(
                "❌ --agreed-defer-ctl-state-backend-sync is not applicable: "
                "the selected backend already exists"
            )
    state_run_store.update_run_metadata(
        run_dir,
        {
            "selected_graph_provisions_ctl_state_backend": selected_graph_provisions_backend,
            "ctl_state_backend_absence_confirmed_at_start": backend_absence_confirmed,
        },
    )

    # Resolve the run's namespace and arm publication only after the graph-level
    # defer gate has frozen its provider-classified readiness fact.
    ctl_state_namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(
        ctl_cfg_root, execution_context
    )
    guardrails_verify.verify_ctl_guardrails(
        ctl_cfg_root,
        guardrails_cfg_root,
        execution_context,
    )
    state_sync.PUBLICATION.configure(
        ctl_cfg_root,
        ctl_profile,
        ctl_state_namespace_key,
        execution_context,
        run_dir,
        agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
        force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
        provisions_ctl_state_backend=state_sync.run_provisions_ctl_state_backend(workflow_cfg, action_cfg),
        selected_graph_provisions_ctl_state_backend=selected_graph_provisions_backend,
        backend_absence_confirmed=backend_absence_confirmed,
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
        provider_implementation_key=provider_implementation_key,
    )
    execution_context_path = execution_run_context.write_execution_context_artifact(run_dir, execution_context)

    if use_local_tooling_cfg:
        tooling_refs = cfg_tooling.load_local_tooling_cfg(ctl_cfg_root)
    else:
        tooling_refs = refs.get("global") or {}
        cfg_validate.CommitPinning(ctl_ref_policy).check_tooling_refs(tooling_refs)

    logging.info(f"Selector policy validation passed: ctl_profile={ctl_profile}")

    # Prepare pipeline config
    active_target_runs, pipeline_run_cfg_path, final_plt_overlays = commands_selection.prepare_pipeline_cfg(
        plt_cfg_root,
        workflow_cfg,
        action_cfg,
        artifacts_dir,
        ctl_profile,
        scope_params=scope_params,
        execution_context=execution_context,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
        active_target_runs=active_target_runs,
    )
    state_run_store.update_run_metadata(run_dir, {"plt_overlays": final_plt_overlays})
    # chain, PER TARGET: each target_run merges its own
    # scopes with its own overlays, renders them, is guard-verified against its own
    # rendered values, and receives its projected key view. Nothing is shared, so a
    # target's cfg cannot be reshaped by another target's declarations.
    run_type_now = str(state_run_store.load_run_metadata(run_dir).get("run_type"))
    plt_targets_dir_path = cfg_views.target_cfg_views_root(run_dir, run_type_now)
    # For a WORKFLOW this loop is a PRE-CHECK of its CHILDREN — each
    # spawned target re-derives and re-validates its own cfg when it runs. Skipping
    # trades fail-fast (catch a bad target before target #1 mutates anything) for not
    # doing the work twice. A target run has no children, so the flag is a no-op.
    precheck_runs = active_target_runs
    if skip_children_precheck and state_run_store.load_run_metadata(run_dir).get("run_type") == "workflow":
        logging.info(
            "Skipping the child pre-check (--skip-children-precheck); each target "
            "renders and validates its own cfg when it runs"
        )
        precheck_runs = {}
        state_run_store.update_run_metadata(run_dir, {"skipped_children_precheck": True})
    for target_run_id, target_run in precheck_runs.items():
        if not target_run.get("domains"):
            continue
        target_context = execution_run_context.build_target_execution_context(
            target_run_id, target_run, execution_context
        )
        target_rendered_dir = cfg_views.prepare_target_cfg_view(
            target_run_id, target_run,
            plt_cfg_root=plt_cfg_root,
            target_cfg_dir=cfg_views.target_cfg_view_dir(run_dir, run_type_now, target_run_id),
            ctl_profile=ctl_profile,
            scope_params=execution_run_context.scope_params_from_context(target_context),
            execution_context=target_context,
        )
        guardrails_verify.verify_guardrails(
            ctl_cfg_root,
            plt_cfg_root,
            guardrails_cfg_root,
            target_rendered_dir,
            target_context,
            execution_run_context.scope_params_from_context(target_context),
        )

    if procedure_run:
        target_keys = procedure_run.get("affected_target_keys") or []
        if action in run_actions.MUTATING_ACTIONS and not target_keys:
            raise RuntimeError("❌ mutating procedure runs require affected_target_keys")
    else:
        target_keys = catalog_targets.target_keys_from_active_target_runs(active_target_runs)
    state_run_store.record_run_target_keys(run_dir, target_keys)
    run_metadata = state_run_store.load_run_metadata(run_dir)
    ctl_state_local_root_value = run_metadata.get("ctl_state_local_root")
    if isinstance(ctl_state_local_root_value, str) and ctl_state_local_root_value:
        state_status.mark_removed_definitions_outdated(Path(ctl_state_local_root_value), ctl_cfg_root)

    catalog_workflow.write_target_flow_artifact(
        ctl_cfg_root,
        artifacts_dir,
        ctl_profile=ctl_profile,
        execution_context=execution_context,
        action=action,
        workflow_name=workflow_name,
        target_repo_key=target_repo_key,
        require_target_ref=require_target_ref,
        require_commit_refs=require_commit_refs,
        refs=refs,
    )

    # Write git metas
    cfg_materialize.write_git_metas(ctl_cfg_root, plt_cfg_root, guardrails_cfg_root, artifacts_dir)

    # Resolved ctl cfg snapshot (self-describing run, next to cfg/plt/)
    cfg_materialize.write_ctl_cfg_snapshot(
        run_dir,
        ctl_profile=ctl_profile,
        ctl_profile_policy_cfg=run_policy.ctl_profile_policy(ctl_cfg_root, ctl_profile),
        action=action,
        workflow_cfg=workflow_cfg,
        action_cfg=action_cfg,
        active_target_runs=active_target_runs,
        refs=refs,
        execution_context=execution_context,
    )

    # Distribute target_run input views from the rendered tree
    plt_targets_dir_path = cfg_materialize.run_cfg_distribution(
        pipeline_run_cfg_path, plt_targets_dir_path, run_type_now
    )
    cfg_views.finalize_target_cfg_view_facts(
        active_target_runs, plt_targets_dir_path, pipeline_run_cfg_path
    )
    if state_run_store.load_run_metadata(run_dir).get("run_type") == "target":
        only_target = next(iter(active_target_runs.values()), None)
        if only_target and only_target.get("target_definition") is not None:
            kernel_yaml_io.write_yaml_file(
                run_dir / "cfg" / "ctl" / "target_definition.yaml",
                only_target["target_definition"],
            )
        if only_target:
            state_run_store.update_run_metadata(
                run_dir,
                {
                    key: only_target[key]
                    for key in (
                        "target_definition_sha256", "target_cfg_view_sha256"
                    )
                },
            )
    # Prepared snapshot: cfg layers + run-level metadata are immutable from here.
    state_sync.PUBLICATION.push("preparation complete")

    # Freeze the commit facts used by the opt-in committed-rerun gate.
    cfg_source_commit, cfg_source_state = kernel_git.git_source_facts(plt_cfg_root)
    for target_run in active_target_runs.values():
        source_commit, target_source_state = state_run_store.target_run_source_facts(target_run)
        target_run["source_commit"] = source_commit
        target_run["cfg_source_commit"] = cfg_source_commit
        target_run["source_state"] = (
            "clean"
            if target_source_state == "clean" and cfg_source_state == "clean"
            else "dirty"
        )
        target_run["ref_policy"] = ctl_ref_policy
    if state_run_store.load_run_metadata(run_dir).get("run_type") == "target":
        only_target = next(iter(active_target_runs.values()), None)
        if only_target:
            state_run_store.update_run_metadata(
                run_dir,
                {
                    key: only_target[key]
                    for key in (
                        "source_commit", "cfg_source_commit", "source_state", "ref_policy"
                    )
                },
            )

    # ONE frozen spec describing this invocation, from which every
    # child target's argv is derived. Built here because run_pipeline is the only
    # place that holds all of it; passing scattered locals into run_targets is how
    # A flag gets forgotten and a child silently runs differently.
    run_metadata_now = state_run_store.load_run_metadata(run_dir)
    child_command_spec = {
        "ctl_entrypoint": kernel_paths.ctl_utils_root().parent
            / "atlas-ctl-orchestrator" / "ctl.py",
        "ctl_cfg_root": ctl_cfg_root,
        "ctl_profile": ctl_profile,
        "ctl_state_local_root": run_metadata_now.get("ctl_state_local_root"),
        "execution_runtime_mode": execution_runtime_mode,
        "action": action,
        "providers": list(execution_providers.run_providers(execution_context)),
        "execution_params": dict(execution_params),
        "provider_options": dict(provider_options or {}),
        "execution_access_modes": dict(execution_access_modes or {}),
        "plt_overlays": list(final_plt_overlays or []),
        "force_skip_execution_identity_preflight_check":
            list(force_skip_execution_identity_preflight_check or []),
        "agreed_defer_ctl_state_backend_sync": agreed_defer_ctl_state_backend_sync,
        "force_skip_ctl_state_backend_sync": force_skip_ctl_state_backend_sync,
        "force_skip_guardrails": force_skip_guardrails,
        "force_skip_full_cfg_validation_gate": force_skip_full_cfg_validation_gate,
        "skip_children_precheck": skip_children_precheck,
        "credential_refresh_modes": credential_refresh_modes,
    }
    kernel_yaml_io.write_yaml_file(
        artifacts_dir / "child_command_spec.yaml",
        {k: (str(v) if isinstance(v, Path) else v) for k, v in child_command_spec.items()},
    )

    # Run target runs
    credential_refresh_modes = run_policy.validate_credential_refresh_modes(
        ctl_cfg_root, ctl_profile, credential_refresh_modes, providers,
        execution_access_modes,
    )
    run_targets(
        active_target_runs, run_dir, plt_targets_dir_path, execution_context_path,
        action, execution_context, run_id,
        secret_store=cfg_secrets.SecretStore(
            ctl_cfg_root,
            execution_context=execution_context,
            implementation_key=provider_implementation_key,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
        ),
        child_command_spec=child_command_spec,
        tooling_refs=tooling_refs,
        use_local_tooling_cfg=use_local_tooling_cfg,
        provider_adapter=provider_adapter,
        provider_catalogs=provider_catalogs,
        provider_implementation_key=provider_implementation_key,
        execution_access_modes=execution_access_modes,
        provider_options=provider_options,
        execution_runtime_mode=execution_runtime_mode,
        skip_up_to_date=skip_up_to_date,
    )

    #(b3): a WORKFLOW owns ordering, policy and the run verdict — not cfg.
    # Each child has received its complete derivation, so the workflow-side copy is
    # dropped rather than published twice.
    if state_run_store.load_run_metadata(run_dir).get("run_type") == "workflow":
        workflow_plt_dir = run_dir / "cfg" / "plt"
        if workflow_plt_dir.exists():
            shutil.rmtree(workflow_plt_dir)

    state_lifecycle.print_run_summary(run_id, log_file)
