"""Provider adapters, reached by package name.

The engine owns provider selection and lifecycle; an adapter owns its identity
schema, catalogs, credentials and access assertion. The contract is the set of
module-level callables this module looks up.
"""


# An adapter is imported by package name, from a DECLARED tooling ref
# (`execution-provider-{provider}`) — never found by globbing the filesystem.

# The cfg root of the run in progress, so a lookup need not thread it through.
from pathlib import Path

from engine.kernel import yaml_io as kernel_yaml_io

# A provider adapter is TOOLING and its ref goes where refs already go
# — DECLARED in `refs.global`, materialized by the same path as ctl-utils, never
# discovered by looking at what sits beside the engine on disk.
# The names are DERIVED from the provider, so engine core never spells a provider
# out; `execution_providers.yaml` is the only place a provider is named.
PROVIDER_ADAPTER_TOOLING_TEMPLATE = "execution-provider-{provider}"


def provider_adapter_tooling_name(provider: str) -> str:
    """The tooling ref name a provider's adapter is pinned under."""

    return PROVIDER_ADAPTER_TOOLING_TEMPLATE.format(provider=provider)


def load_execution_providers(ctl_cfg_root) -> dict:
    """The declared providers: what each implements, and where its adapter lives.

    Read from cfg rather than carried as a constant, so adding a provider — or
    replacing one with your own implementation — is a declaration and a
    repository, never an edit to the engine.
    """

    if not ctl_cfg_root:
        return {}
    path = Path(ctl_cfg_root) / "execution_providers.yaml"
    if not path.is_file():
        return {}
    return (kernel_yaml_io.load_yaml(path) or {}).get("execution_providers") or {}


def provider_adapter_package(provider: str, ctl_cfg_root=None) -> str:
    """The importable package a provider's adapter provides.

    Declared, with the convention as a fallback: a consumer's adapter is THEIR
    package, and naming it by our convention would assume they forked ours.
    """

    declared = (load_execution_providers(ctl_cfg_root).get(provider) or {}).get("package")
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
    declaration in `execution_providers.yaml` is the only registry, and an entry whose
    adapter will not import fails at `get_adapter` with a message that says so.
    """

    root = ctl_cfg_root or _ACTIVE_CTL_CFG_ROOT
    if root is None:
        raise RuntimeError("❌ no ctl cfg root is active, so the declared providers cannot be read")
    declared = load_execution_providers(root)
    if not declared:
        raise RuntimeError(f"❌ {root} declares no providers in execution_providers.yaml")
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

    The package name comes from `execution_providers.yaml` when a cfg root is known —
    a consumer's adapter is THEIR package — falling back to the convention only
    when nothing declared one.
    """

    root = ctl_cfg_root or _ACTIVE_CTL_CFG_ROOT
    if provider not in registered_providers(root):
        raise RuntimeError(
            f"❌ provider {provider!r} is not declared in execution_providers.yaml; "
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
