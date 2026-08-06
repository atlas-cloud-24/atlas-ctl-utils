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


# An adapter lives in its OWN repository — the engine depends on it, never the
# other way round — so it is imported by PACKAGE NAME rather than by reaching into
# a subdirectory of this one. Where that package comes from is DECLARED, not
# discovered: `ctl-adapter-{provider}` is a tooling ref in `refs.global`,
# materialized by the same path as the rest of the engine's tooling. The engine
# never globs the filesystem for adapters — that would make whatever happens to
# sit beside the checkout into the registry.

# The cfg root of the run in progress, so a provider lookup can reach the
# declaration without every call site threading it through. Set once, by the same
# code that puts the declared adapter repositories on the import path.
from pathlib import Path

from engine.kernel import yaml_io as kernel_yaml_io

# A provider adapter is TOOLING and its ref goes where refs already go
# — DECLARED in `refs.global`, materialized by the same path as ctl-utils, never
# discovered by looking at what sits beside the engine on disk.
# The names are DERIVED from the provider, so engine core never spells a provider
# out; `ctl_providers.yaml` is the only place a provider is named.
PROVIDER_ADAPTER_TOOLING_TEMPLATE = "ctl-adapter-{provider}"


def provider_adapter_tooling_name(provider: str) -> str:
    """

    the tooling ref name a provider's adapter is pinned under."""

    return PROVIDER_ADAPTER_TOOLING_TEMPLATE.format(provider=provider)


def load_ctl_providers(ctl_cfg_root) -> dict:
    """The declared providers: what each implements, and where its adapter lives.

    Read from cfg rather than carried as a constant, so adding a provider — or
    replacing one with your own implementation — is a declaration and a
    repository, never an edit to the engine.
    """

    if not ctl_cfg_root:
        return {}
    path = Path(ctl_cfg_root) / "ctl_providers.yaml"
    if not path.is_file():
        return {}
    return (kernel_yaml_io.load_yaml(path) or {}).get("ctl_providers") or {}


def provider_adapter_package(provider: str, ctl_cfg_root=None) -> str:
    """The importable package a provider's adapter provides.

    Declared, with the convention as a fallback: a consumer's adapter is THEIR
    package, and naming it by our convention would assume they forked ours.
    """

    declared = (load_ctl_providers(ctl_cfg_root).get(provider) or {}).get("package")
    return declared or f"atlas_ctl_adapter_{provider}"


_ACTIVE_CTL_CFG_ROOT = None


def set_active_ctl_cfg_root(root) -> None:
    global _ACTIVE_CTL_CFG_ROOT
    _ACTIVE_CTL_CFG_ROOT = root


def registered_providers(ctl_cfg_root=None) -> tuple[str, ...]:
    """The providers this cfg tree declares.

    There is no engine-side list, because a list in code is a SECOND registry:
    a second place for the answer to be wrong, and one that cannot name a
    consumer's own provider without the engine already knowing it exists. The
    declaration in `ctl_providers.yaml` is the only registry, and an entry whose
    adapter will not import fails at `get_adapter` with a message that says so.
    """

    root = ctl_cfg_root or _ACTIVE_CTL_CFG_ROOT
    if root is None:
        raise RuntimeError(
            "❌ no ctl cfg root is active, so the declared providers cannot be read"
        )
    declared = load_ctl_providers(root)
    if not declared:
        raise RuntimeError(f"❌ {root} declares no providers in ctl_providers.yaml")
    return tuple(declared)


def describe_all(ctl_cfg_root=None) -> dict[str, dict]:
    """Every declared adapter's self-description, for the discovery command.

    The engine names no provider, mode, capability or option key — it reads which
    providers a cfg tree declares, then asks each adapter to describe itself.
    """

    return {
        name: get_adapter(name, ctl_cfg_root).describe()
        for name in registered_providers(ctl_cfg_root)
    }


def get_adapter(provider: str, ctl_cfg_root=None):
    """Return the adapter module for a provider; unknown providers are a hard error.

    The package name comes from `ctl_providers.yaml` when a cfg root is known —
    a consumer's adapter is THEIR package — falling back to the convention only
    when nothing declared one.
    """

    root = ctl_cfg_root or _ACTIVE_CTL_CFG_ROOT
    if provider not in registered_providers(root):
        raise RuntimeError(
            f"❌ provider {provider!r} is not declared in ctl_providers.yaml; "
            f"declared: {list(registered_providers(root))}"
        )
    package = provider_adapter_package(provider, root)
    import importlib

    try:
        return importlib.import_module(package)
    except ImportError as exc:
        # The adapter is a declared tooling ref; if its package is not importable
        # the checkout was never materialized, and saying so beats an ImportError
        # that looks like a missing dependency.
        tooling = provider_adapter_tooling_name(provider)
        raise RuntimeError(
            f"❌ provider {provider!r} is registered but its adapter package "
            f"{package!r} is not importable: {exc}. It is declared as the tooling "
            f"ref {tooling!r} in refs.global — check that ref materialized."
        ) from exc
