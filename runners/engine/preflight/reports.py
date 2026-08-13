"""Building the reports the pre-run checks produce.

A check produces a REPORT rather than raising, because an operator needs to see
everything that is wrong, not the first thing. What a report MEANS for the run —
and which check owns it — belongs to `checks.py`."""

from pathlib import Path

from engine.cfg import validate as cfg_validate
from engine.execution import adapters as execution_adapters
from engine.execution import providers as execution_providers
from engine.execution import run_context as execution_run_context
from engine.preflight import render as preflight_render
from engine.run import addressing as run_addressing
from engine.run import policy as run_policy
from engine.run import selectors as run_selectors
from engine.state import sync as state_sync


def collect_provider_cfg_findings(
    ctl_cfg_root: Path, execution_context: dict[str, object]
) -> list[dict]:
    """Stage-1 provider cfg well-formedness findings, once per participating provider."""
    findings: list[dict] = []
    for _name, adapter in execution_providers.run_provider_adapters(execution_context):
        findings.extend(
            adapter.collect_provider_cfg_findings(ctl_cfg_root, execution_context=execution_context)
        )
    return findings


def load_selection_provider_catalogs(selection, ctl_cfg_root: Path):
    """The selection, bound to the provider adapter and its runtime catalogs.

    Answers with a new selection rather than filling the given one in: a
    selection is frozen, so a caller keeping the unbound one still holds what it
    resolved.

    Runtime catalogs are structurally validated but permit unresolved concrete
    provider values. The adapter validates concrete values reachable from the
    selected target runs during target-cfg and execution-identity preflight.
    """
    execution_context = selection.execution_context
    provider_adapter = execution_providers.run_provider_adapter(execution_context)
    return selection.with_provider(
        provider_adapter,
        provider_adapter.load_runtime_catalogs(ctl_cfg_root, execution_context=execution_context),
    )


def aggregate_execution_identity_preflight_status(statuses: list[str]) -> str:
    """Container (fan-out/workflow/target) status rolls up its children without
    ever a false green OR a false 'nothing checked':

    - any `failed` child                         → failed;
    - else no `not_evaluated` child              → passed;
    - else a block is present, and:
        - at least one GENUINE `passed` child    → partial (honest mixed state);
        - no genuine pass (only blocks + neutral
          non-checks)                            → not_evaluated (fully blocked).

    A `not_evaluated` child is blocked upstream (e.g. a malformed account id) so
    it could not be checked. Deliberate non-checks (bypassed, force-skipped,
    not-applicable, skipped) are NEUTRAL — they neither block nor count as a
    verification: a container of blocked-plus-skipped is `not_evaluated`, not
    `partial` (only a real `passed` sibling makes it partial). Neither `partial`
    nor `not_evaluated` fails the run — only `failed` gates; these are honest
    summaries. The per-identity rows keep their own raw statuses."""

    if any(status == "failed" for status in statuses):
        return "failed"
    if not any(status == "not_evaluated" for status in statuses):
        return "passed"
    # A block is present; `partial` requires a GENUINE pass alongside it —
    # deliberate non-checks (skipped/force_skipped/not_applicable/bypassed) are
    # neutral, NOT verifications, so blocked + only-neutral is fully not_evaluated.
    if any(status == "passed" for status in statuses):
        return "partial"
    return "not_evaluated"


def build_ctl_policy_preflight_report(
    selection: dict,
    *,
    ctl_cfg_root: Path,
    ctl_profile: str,
    ctl_ref_policy: str,
    execution_runtime_mode: str,
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
    force_skip_execution_identity_preflight_check: list[str],
) -> dict:
    """Evaluate run policy independently from provider identity reachability."""
    checks: list[dict] = []

    def check(name: str, validator) -> None:
        try:
            detail = validator()
            entry = {"name": name, "status": "passed"}
            # A validator MAY return render lines (provider-authored strings the
            # engine prints verbatim); it never composes provider vocabulary itself.
            if detail:
                entry["detail"] = list(detail) if not isinstance(detail, dict) else detail
            checks.append(entry)
        except Exception as error:
            checks.append(
                {
                    "name": name,
                    "status": "failed",
                    "failure_reason": execution_run_context.credential_free_preflight_failure_reason(
                        error
                    ),
                }
            )

    workflow_cfg = selection.workflow_cfg
    action_cfg = selection.action_cfg
    execution_context = selection.execution_context
    check(
        "execution_context_constraints",
        lambda: execution_run_context.validate_execution_context_constraints(
            ctl_cfg_root, execution_context
        ),
    )

    def _providers_check() -> list[str]:
        declared = sorted(execution_access_modes or {})
        run_policy.validate_ctl_allowed_providers(ctl_cfg_root, ctl_profile, declared)
        return [f"declared: {', '.join(declared) or '(none)'}"]

    check("allowed_providers", _providers_check)

    # per-provider access authorization, each provider's own policy block. The
    # ADAPTER returns the render lines; the engine nests them, naming no mode.
    def _provider_access_detail() -> dict[str, list[str]]:
        rows: dict[str, list[str]] = {}
        for provider, mode in sorted((execution_access_modes or {}).items()):
            rows[provider] = execution_adapters.get_adapter(provider).authorize_run(
                run_policy.ctl_profile_provider_policy(ctl_cfg_root, ctl_profile, provider),
                execution_access_mode=mode,
                provider_options=execution_providers.provider_options_for(
                    provider_options, provider
                ),
                label=f"ctl profile {ctl_profile!r} policy for {provider!r}",
            )
        return rows

    check("provider_access_policy", _provider_access_detail)

    check(
        "execution_access_policy",
        lambda: run_policy.validate_execution_access(
            ctl_cfg_root,
            ctl_profile,
            workflow_cfg,
            action_cfg,
            execution_context=execution_context,
            agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
            force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            force_skip_execution_identity_preflight_check=(
                force_skip_execution_identity_preflight_check
            ),
        ),
    )
    check(
        "execution_runtime_mode_policy",
        lambda: run_policy.validate_execution_runtime_mode(
            ctl_cfg_root, ctl_profile, execution_runtime_mode
        ),
    )
    check(
        "ref_policy",
        lambda: cfg_validate.CommitPinning(ctl_ref_policy).check_target_runs(
            selection.active_target_runs
        ),
    )
    # Per-target policy checks (hybrid: selection-scoped checks above, target-
    # scoped checks here).
    targets: list[dict] = []
    for target_key in sorted(run_policy.active_target_names(workflow_cfg)):
        target_checks: list[dict] = []

        def target_check(name: str, validator, target_checks=target_checks) -> None:
            try:
                validator()
                target_checks.append({"name": name, "status": "passed"})
            except Exception as error:
                target_checks.append(
                    {
                        "name": name,
                        "status": "failed",
                        "failure_reason": execution_run_context.credential_free_preflight_failure_reason(
                            error
                        ),
                    }
                )

        target_check(
            "target_policy_constraints",
            lambda tk=target_key: run_policy.validate_target_policy_constraints_for_target(
                ctl_cfg_root, ctl_profile, tk
            ),
        )
        targets.append(
            {
                "target_key": target_key,
                "status": (
                    "failed" if any(c["status"] == "failed" for c in target_checks) else "passed"
                ),
                "checks": target_checks,
            }
        )
    status = (
        "failed"
        if any(item["status"] == "failed" for item in checks)
        or any(target["status"] == "failed" for target in targets)
        else "passed"
    )
    return {
        "selection": {
            "kind": selection.kind,
            "key": selection.key,
        },
        "status": status,
        "checks": checks,
        "targets": targets,
    }


def build_ctl_state_backend_preflight_result(
    selection: dict,
    *,
    ctl_cfg_root: Path,
    credential_acquisition: str,
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    force_skip_providers: list[str],
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
) -> dict:
    """Preflight the run's ctl-state backend synchronizer identity.

    Mirrors the sync semantics: force-skip-sync and namespace-less runs are
    not_applicable (sync will not happen); agreed-skip still CHECKS the identity
    (only a missing bucket is tolerated, and only for a provisioning run — noted
    here, never failed; the syncer re-checks the bucket at every sync point)."""

    buckets = state_sync.CtlStateBackends.load(ctl_cfg_root)
    result: dict = {
        "ctl_state_backend": None,
        "execution_identities": None,
        "provider": None,
        "access_mode": None,
        "status": "not_applicable",
        "provider_path": [],
    }
    if not buckets:
        result["reason"] = "no ctl-state backend registry"
        return result
    try:
        namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(
            ctl_cfg_root, selection.execution_context
        )
    except Exception as error:
        result["status"] = "failed"
        result["failure_reason"] = execution_run_context.credential_free_preflight_failure_reason(
            error
        )
        return result
    result["ctl_state_backend"] = namespace_key
    if force_skip_ctl_state_backend_sync:
        result["reason"] = "ctl-state sync force-skipped for this run"
        return result
    if agreed_defer_ctl_state_backend_sync:
        result["reason"] = (
            "ctl-state sync readiness is validated once for the complete selected graph"
        )
        return result
    entry = buckets[namespace_key]
    result["provider"] = entry.get("provider")
    # The backend's own provider decides the mode and options here — it is not
    # necessarily the provider the targets run against.
    namespace_provider = str(entry["provider"])
    namespace_adapter = execution_adapters.get_adapter(namespace_provider)
    namespace_access_mode, namespace_adapter_options = execution_providers.provider_inputs(
        namespace_provider, execution_access_modes, provider_options
    )
    operation_execution = state_sync.CtlStateBackends.operation_execution(
        entry, "sync", namespace_key=namespace_key, required=False
    )
    if operation_execution is None:
        if not namespace_adapter.resolves_execution_identity(namespace_access_mode):
            result["reason"] = "identity bypass: synchronizer uses the substitute credential"
            return result
        result["status"] = "failed"
        result["failure_reason"] = (
            f"ctl_state_backends.{namespace_key} declares no execution_identity.operations.sync"
        )
        return result
    # Ctl-state publication always uses its normal operation role path.
    sync_access_mode = state_sync.CtlStateAccess.publication_access_mode(
        namespace_adapter, namespace_access_mode
    )
    provider_adapter = namespace_adapter
    try:
        checked = provider_adapter.preflight_execution_identity(
            f"ctl_state_backend/{namespace_key}",
            {"execution_identities": operation_execution},
            selection.provider_catalogs,
            execution_context=selection.execution_context,
            credential_acquisition=credential_acquisition,
            execution_access_mode=sync_access_mode,
            provider_options=namespace_adapter_options,
            live_check=namespace_provider not in force_skip_providers,
        )
        if (
            not isinstance(checked, dict)
            or checked.get("status") not in preflight_render.PREFLIGHT_RESULT_STATUSES
        ):
            raise RuntimeError("provider preflight returned an invalid result")
    except Exception as error:
        result["status"] = "failed"
        result["execution_identities"] = operation_execution
        result["failure_reason"] = execution_run_context.credential_free_preflight_failure_reason(
            error
        )
        return result
    checked = dict(checked)
    checked["ctl_state_backend"] = namespace_key
    return checked


def build_execution_identity_preflight_report(
    selection: dict,
    *,
    credential_acquisition: str,
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    force_skip_providers: list[str],
    ctl_cfg_root: Path | None = None,
    agreed_defer_ctl_state_backend_sync: bool = False,
    force_skip_ctl_state_backend_sync: bool = False,
) -> dict:
    """Run one adapter preflight per selected target and aggregate every result.

    When `ctl_cfg_root` is provided, the run's ctl-state backend synchronizer is
    checked as one more result row (same aggregate rules)."""
    active_target_runs = selection.active_target_runs
    provider_adapter = selection.provider_adapter
    catalogs = selection.provider_catalogs
    execution_context = selection.execution_context

    target_runs_by_key: dict[str, tuple[str, dict]] = {}
    for target_run_id, target_run in active_target_runs.items():
        target_key = target_run.get("target") or target_run_id
        target_runs_by_key.setdefault(target_key, (target_run_id, target_run))

    results: list[dict] = []
    for target_key, (target_run_id, target_run) in target_runs_by_key.items():
        try:
            adapter_access_mode, adapter_options = execution_providers.provider_inputs(
                provider_adapter.PROVIDER_NAME,
                execution_access_modes,
                provider_options,
            )
            result = provider_adapter.preflight_execution_identity(
                target_run_id,
                target_run,
                catalogs,
                execution_context=execution_context,
                credential_acquisition=credential_acquisition,
                execution_access_mode=adapter_access_mode,
                provider_options=adapter_options,
                live_check=provider_adapter.PROVIDER_NAME not in force_skip_providers,
            )
            if not isinstance(result, dict):
                raise RuntimeError("provider preflight returned a non-mapping result")
        except Exception as error:
            result = {
                "execution_identities": target_run.get("execution_identities"),
                # This adapter's own view: it preflights the identity it owns
                "provider": provider_adapter.PROVIDER_NAME,
                "access_mode": execution_providers.execution_access_mode_for(
                    execution_access_modes, provider_adapter.PROVIDER_NAME
                )
                if (target_run.get("execution_identities") or {})
                else None,
                "status": "failed",
                "provider_path": [],
                "failure_reason": execution_run_context.credential_free_preflight_failure_reason(
                    error
                ),
            }
        status = result.get("status")
        if status not in preflight_render.PREFLIGHT_RESULT_STATUSES:
            result = {
                "execution_identities": result.get("execution_identities"),
                "provider": result.get("provider"),
                "access_mode": result.get("access_mode"),
                "status": "failed",
                "provider_path": [],
                "failure_reason": f"provider preflight returned invalid status {status!r}",
            }
        result = dict(result)
        result["target_key"] = target_key
        result["instance"] = run_addressing.target_instance_display(target_run, execution_context)
        results.append(result)

    if ctl_cfg_root is not None:
        results.append(
            build_ctl_state_backend_preflight_result(
                selection,
                ctl_cfg_root=ctl_cfg_root,
                credential_acquisition=credential_acquisition,
                execution_access_modes=execution_access_modes,
                provider_options=provider_options,
                force_skip_providers=force_skip_providers,
                agreed_defer_ctl_state_backend_sync=agreed_defer_ctl_state_backend_sync,
                force_skip_ctl_state_backend_sync=force_skip_ctl_state_backend_sync,
            )
        )

    status = aggregate_execution_identity_preflight_status(
        [str(result["status"]) for result in results]
    )
    return {
        "selection": {
            "kind": selection.kind,
            "key": selection.key,
        },
        "status": status,
        "results": results,
    }


def instance_axis_exclusions(ctl_cfg_root: Path | None) -> set[str]:
    """Axes that are NEVER instance axes: the provider dispatch key and the

    namespace axes (already encoded in the ctl-state bucket choice, e.g.
    landing_zone) — derived from the ctl_state_backends selectors, not a
    hand-list.
    """

    excluded = {"provider"}
    if ctl_cfg_root is None:
        return excluded
    try:
        for entry in state_sync.CtlStateBackends.load(ctl_cfg_root).values():
            if isinstance(entry, dict):
                excluded |= run_selectors._selector_param_axes([entry])
    except Exception:
        pass
    return excluded


def build_target_cfg_validation_report(
    selection: dict,
    *,
    credential_acquisition: str,
    execution_access_modes: dict[str, str],
    provider_options: dict[str, str] | None,
    ctl_cfg_root: Path | None = None,
) -> dict:
    """Per-target cfg resolution requires every selected identity/account binding
    to be concrete. Whole-cfg health remains non-blocking for unrelated values.
    Includes the-axes guard: declared < consumed →
    ERROR (self-override risk); declared > consumed → WARN (unused axis)."""
    active_target_runs = selection.active_target_runs
    provider_adapter = selection.provider_adapter
    catalogs = selection.provider_catalogs
    execution_context = selection.execution_context
    by_key: dict[str, tuple[str, dict]] = {}
    for target_run_id, target_run in active_target_runs.items():
        by_key.setdefault(target_run.get("target") or target_run_id, (target_run_id, target_run))
    results: list[dict] = []
    for target_key, (target_run_id, target_run) in by_key.items():
        try:
            adapter_access_mode, adapter_options = execution_providers.provider_inputs(
                provider_adapter.PROVIDER_NAME,
                execution_access_modes,
                provider_options,
            )
            result = provider_adapter.resolve_target_cfg_references(
                target_run_id,
                target_run,
                catalogs,
                execution_context=execution_context,
                credential_acquisition=credential_acquisition,
                execution_access_mode=adapter_access_mode,
                provider_options=adapter_options,
            )
        except Exception as error:
            result = {
                "status": "failed",
                "rows": [],
                "failure_reason": execution_run_context.credential_free_preflight_failure_reason(
                    error
                ),
            }
        result = dict(result)
        result["target_key"] = target_key
        result["instance"] = run_addressing.target_instance_display(target_run, execution_context)
        # Axes guard
        action_target = (selection.action_cfg.get("targets") or {}).get(target_key)
        if isinstance(action_target, dict):
            consumed = run_selectors.collect_target_consumed_axes(
                target_key,
                action_target,
                refs=selection.refs or {},
                execution_identities=(catalogs or {}).get("execution_identities") or {},
                execution_context=execution_context,
            ) - instance_axis_exclusions(ctl_cfg_root)
            declared = set(target_run.get("target_instance_params") or [])
            rows = result.setdefault("rows", [])
            for axis in sorted(consumed - declared):
                rows.append(
                    {
                        "name": f"instance_axis {axis}: consumed but not declared",
                        "status": "failed",
                    }
                )
                result["status"] = "failed"
                result.setdefault(
                    "failure_reason",
                    "target varies by an undeclared axis — add it to "
                    "target_instance_params or runs will self-override",
                )
            for axis in sorted(declared - consumed):
                rows.append(
                    {
                        "name": f"instance_axis {axis}: declared but not consumed",
                        "status": "warning",
                    }
                )
        results.append(result)
    status = aggregate_execution_identity_preflight_status(
        [str(result["status"]) for result in results]
    )
    return {
        "selection": {
            "kind": selection.kind,
            "key": selection.key,
        },
        "status": status,
        "results": results,
    }
