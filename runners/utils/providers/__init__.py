"""Provider adapters for the Atlas ctl engine.

The engine core owns only `execution_identity_key`, provider selection,
lifecycle, and error propagation. Each adapter owns its identity schema,
catalogs, credential acquisition, target resolution, runtime binding, access
assertion, and derived provider facts.

Adapter contract (module-level callables):

    validate_catalog(ctl_cfg_root)
    validate_target_execution_identity(execution, ctl_cfg_root, *, label)
    describe() -> {execution_access_modes, identity_preflight,
        modes_resolving_execution_identity, provider_options}
    supported_execution_access_modes() -> set[str]      (CSI-style advertisement)
    supports_identity_preflight() -> bool
    normal_execution_access_mode() -> str          (its own non-escalated mode)
    resolves_execution_identity(execution_access_mode) -> bool
    target_consent(execution_access_mode) -> {opt_in_field, execution_field} | None
    execution_access_mode_from_options(provider_options) -> mode | None
    validate_provider_options(options) -> options       (unknown key -> hard error)
    required_profile_grants(options) -> [ctl-profile permission keys]
    load_runtime_catalogs(ctl_cfg_root, *, execution_context) ->
        structurally valid opaque catalogs bundle; concrete bindings may remain unresolved
    validate_active_target_access(active_target_runs, catalogs, *, execution_context,
        implementation_key, execution_access_mode, provider_options)
    preflight_execution_identity(target_run_id, target_run, catalogs, *, execution_context,
        implementation_key, execution_access_mode, provider_options, live_check) -> result
    materialize_target_binding(target_run_id, target_run, target_env, catalogs, *,
        execution_context, implementation_key, execution_access_mode, provider_options)
        -> binds credentials via ephemeral private temp state only; NEVER writes
           credential material into the run's ctl-state dir
    target_assertion_argv(step_utils_dir) -> argv | None
    validate_state_backend_entry(namespace_key, entry, path)
    resolve_ctl_state_credential(operation_execution, ctl_cfg_root, *,
        execution_context, implementation_key, execution_access_mode, provider_options)
    create_state_syncer(results_root, bucket_name, bucket_region, credential, *,
        required)

The engine narrows both provider-specific inputs at this boundary: it passes ONE
provider's `execution_access_mode` (a name only that adapter defines) and ONE
provider's `provider_options` (its own keys, prefix stripped). No adapter ever
sees another provider's mode or options.
"""


REGISTERED_PROVIDERS = ("aws",)


def describe_all() -> dict[str, dict]:
    """Every registered adapter's self-description, for the discovery command.

    The engine names no mode, capability or option key — it only asks each
    adapter to describe itself.
    """
    return {name: get_adapter(name).describe() for name in REGISTERED_PROVIDERS}


def get_adapter(provider: str):
    """Return the adapter module for a provider; unknown providers are a hard error."""
    if provider == "aws":
        from utils.providers import aws
        return aws
    raise RuntimeError(f"❌ no provider adapter registered for provider {provider!r}")
