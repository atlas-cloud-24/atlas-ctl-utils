"""Provider adapters for the Atlas ctl engine.

The engine core owns only `execution_identity_key`, provider selection,
lifecycle, and error propagation. Each adapter owns its identity schema,
catalogs, credential acquisition, target resolution, runtime binding, access
assertion, and derived provider facts.

Adapter contract (module-level callables):

    validate_catalog(ctl_cfg_root)
    validate_execution_identity(identity_key, identity_cfg, ctl_cfg_root)
    load_runtime_catalogs(ctl_cfg_root) -> opaque catalogs bundle
    validate_active_stage_access(active_stages, catalogs, *, execution_context,
        implementation_key, execution_access_mode, provider_credential)
    preflight_execution_identity(stage_id, stage, catalogs, *, execution_context,
        implementation_key, execution_access_mode, provider_credential, live_check) -> result
    materialize_stage_binding(stage_id, stage, stage_env, catalogs, *,
        execution_context, implementation_key, execution_access_mode, provider_credential)
    stage_assertion_argv(stage_utils_dir) -> argv | None
    validate_state_backend_entry(domain, entry, path)
    ctl_state_backend_locator(domain, entry, execution_context) -> [segments]
        (canonical local-mirror path segments, unique within the provider's
        namespace; e.g. aws -> ["aws", "s3", <bucket>])
    resolve_synchronizer_credential(identity_key, ctl_cfg_root, *,
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
