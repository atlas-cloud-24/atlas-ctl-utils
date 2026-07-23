"""Provider adapters for the Atlas ctl engine.

The engine core owns only `execution_identity_key`, provider selection,
lifecycle, and error propagation. Each adapter owns its identity schema,
catalogs, credential acquisition, target resolution, runtime binding, access
assertion, and derived provider facts.

Adapter contract (module-level callables):

    validate_catalog(ctl_cfg_root)
    validate_execution_identity(identity_key, identity_cfg, ctl_cfg_root)
    load_runtime_catalogs(ctl_cfg_root, *, execution_context) ->
        structurally valid opaque catalogs bundle; concrete bindings may remain unresolved
    validate_active_target_access(active_target_runs, catalogs, *, execution_context,
        implementation_key, execution_access_mode, provider_credential)
    preflight_execution_identity(target_run_id, target_run, catalogs, *, execution_context,
        implementation_key, execution_access_mode, provider_credential, live_check) -> result
    materialize_target_binding(target_run_id, target_run, target_env, catalogs, *,
        execution_context, implementation_key, execution_access_mode, provider_credential)
    target_assertion_argv(step_utils_dir) -> argv | None
    validate_state_backend_entry(namespace_key, entry, path)
    resolve_ctl_state_credential(identity_key, ctl_cfg_root, *,
        execution_context, implementation_key, execution_access_mode, provider_credential)
    create_state_syncer(results_root, bucket_name, bucket_region, credential, *,
        required)
    normalize_provider_credential(value)
"""


def get_adapter(provider: str):
    """Return the adapter module for a provider; unknown providers are a hard error."""
    if provider == "aws":
        from utils.providers import aws
        return aws
    raise RuntimeError(f"❌ no provider adapter registered for provider {provider!r}")
