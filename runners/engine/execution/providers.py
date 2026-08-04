"""The provider seam: what an adapter implements, and how it is called.

The engine names no provider. Which providers exist is cfg, which package
implements one is derived from its name, and what a provider can do is asked of
the adapter — so adding a provider touches cfg and a new package, never this
module."""

import argparse
import sys

from pathlib import Path

from engine.cfg import resources as cfg_resources
from engine.cfg import tooling as cfg_tooling
from engine.execution import references as execution_references
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


def validate_target_providers(declared: object, identities: dict, *, label: str) -> list[str]:
    """A target DECLARES the providers it runs against, and the declaration must
    agree with the identities it carries.

    Declared rather than inferred from `execution_identities`, for the same reason
    `--providers` is declared for a run: the provider set is a statement about what
    this target is allowed to reach, and a drifted identity block should fail
    rather than quietly widen it.
    """

    if not isinstance(declared, list) or not declared or not all(
        isinstance(item, str) and item.strip() for item in declared
    ):
        raise RuntimeError(f"❌ {label} providers must be a non-empty list of provider names")
    if sorted(set(declared)) != sorted(identities):
        raise RuntimeError(
            f"❌ {label} declares providers {sorted(set(declared))} but carries "
            f"execution_identities for {sorted(identities)}; they must agree"
        )
    return sorted(set(declared))


def activate_provider_adapters(ctl_cfg_root) -> list[str]:
    """Put every DECLARED provider-adapter repository on the import path.

    an adapter is its own repository, so the engine must be told where
    it is — and it is told by the same declaration that pins every other piece of
    tooling. Local dev resolves `local_repos.yaml`; a strict run resolves the
    materialized checkout. Neither scans the filesystem: globbing for
    `atlas-ctl-adapter-*` beside the engine would make whatever happens to sit
    there the registry, which is the thing the declaration replaces.

    Returns the paths added, so a caller can report what a run actually loaded.
    """

    if not ctl_cfg_root:
        return []
    from engine.execution.adapters import set_active_ctl_cfg_root

    set_active_ctl_cfg_root(Path(ctl_cfg_root))
    try:
        tooling = cfg_tooling.load_local_tooling_cfg(Path(ctl_cfg_root))
    except Exception:
        return []                       # strict runs materialize refs elsewhere
    added = []
    for name, entry in (tooling or {}).items():
        if not name.startswith("ctl-adapter-"):
            continue
        repo_path = (entry or {}).get("repo_path")
        if repo_path and Path(repo_path).is_dir() and repo_path not in sys.path:
            sys.path.insert(0, repo_path)
            added.append(repo_path)
    return added


def execution_access_mode_for(modes: dict[str, str] | str | None, provider: str) -> str:
    """

    one provider's mode from the per-provider map."""

    if isinstance(modes, str):          # already narrowed by a caller
        return modes
    try:
        return (modes or {})[provider]
    except KeyError:
        raise RuntimeError(
            f"❌ no execution access mode resolved for provider {provider!r} "
            f"(have: {sorted(modes or {})})"
        ) from None


def get_provider_adapter(provider: str):
    """

    dispatch to the provider adapter (utils.providers); unknown = hard error."""

    from engine.execution.adapters import get_adapter
    return get_adapter(provider)


def run_providers(execution_context: dict[str, object]) -> list[str]:
    """

    the providers this run DECLARED (--providers), in order."""

    declared = execution_context.get(f"{execution_references.EXECUTION_CONTEXT_ROOT}.ctl.providers") or []
    if isinstance(declared, str):
        declared = [declared]
    providers = [str(p).strip() for p in declared if str(p).strip()]
    if not providers:
        raise RuntimeError(
            "❌ no providers declared for this run; pass --providers <name>[,<name>...]"
        )
    return providers


def run_provider_adapters(execution_context: dict[str, object]) -> list[tuple[str, object]]:
    """(name, adapter) for every participating provider."""

    return [(name, get_provider_adapter(name)) for name in run_providers(execution_context)]


def run_provider_adapter(execution_context: dict[str, object]):
    """The single participating provider's adapter.

    The run-level catalog/preflight path still assumes ONE adapter per run (it keeps
    one `provider_catalogs` bundle). Declaring several providers is accepted by the
    CLI, gating and coverage guard, but this path is not wired for it yet — so fail
    loud here rather than silently picking the first.
    """

    return get_provider_adapter(run_provider(execution_context))


def run_provider(execution_context: dict[str, object]) -> str:
    """

    the single participating provider's name (same single-provider fence)."""

    providers = run_providers(execution_context)
    if len(providers) > 1:
        raise RuntimeError(
            f"❌ this run declares multiple providers {providers}, but the run-level "
            "provider catalog/preflight path is single-provider today; run them as "
            "separate invocations until per-target adapter dispatch lands"
        )
    return providers[0]


def target_run_providers(target_run: dict) -> list[str]:
    """Every provider a target_run executes against, from its execution_identities.

    A target may declare more than one: a root touching two clouds needs
    credentials for both before a single plan.
    """

    identities = (target_run or {}).get("execution_identities") or {}
    if not identities:
        raise RuntimeError("❌ target_run execution_identities declares no provider")
    return sorted(identities)


def provider_inputs(
    provider: str,
    execution_access_modes: dict[str, str] | str | None,
    provider_options: dict[str, str] | None,
) -> tuple[str, dict[str, str]]:
    """Narrow the run's per-provider inputs to ONE provider, for an adapter call.

    An adapter is never handed another provider's mode or options: the mode names
    and option keys are its own vocabulary and mean nothing outside it.
    """

    return (
        execution_access_mode_for(execution_access_modes, provider),
        provider_options_for(provider_options, provider),
    )


def load_provider_catalogs(ctl_cfg_root: Path) -> dict:
    """Load the `providers` collection: providers.<name>.<section>.<entry>.

    One collection for all provider-owned catalogs, indexed by provider name —
    never by assembling key names from prefixes ( provider-catalog
    end-state). Entries collide at depth 3, so multiple files may contribute to
    one provider section. This loader is engine-generic: it validates structure
    only and knows no provider names or section vocabularies — each provider
    implementation validates its OWN subtree.
    """

    providers = cfg_resources.collect_resource(ctl_cfg_root, "providers", entry_depth=3)
    for provider_name in providers:
        if not isinstance(provider_name, str) or not provider_name.strip():
            raise RuntimeError(f"❌ providers keys must be non-empty strings: {ctl_cfg_root}")
    return providers


def parse_provider_options(value: str) -> dict[str, str]:
    """Parse `--provider-options` into a flat map of provider-namespaced keys.

    ONE generic engine arg instead of per-adapter flags (which would explode the
    CLI as providers are added). The engine parses key=value and NEVER interprets
    either side; a key's leading segment routes it to that provider's adapter,
    which owns and validates its own option vocabulary.
    """
    options: dict[str, str] = {}
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                f"provider option must use <provider>.<key>=<value>, got: {item!r}"
            )
        key, raw = item.split("=", 1)
        key, raw = key.strip(), raw.strip()
        if not key or not raw:
            raise argparse.ArgumentTypeError(
                f"provider option must use non-empty <provider>.<key>=<value>, got: {item!r}"
            )
        if "." not in key:
            raise argparse.ArgumentTypeError(
                f"provider option key must be provider-namespaced (<provider>.<key>), got: {key!r}"
            )
        if key in options:
            raise argparse.ArgumentTypeError(f"duplicate provider option {key!r}")
        options[key] = raw
    if not options:
        raise argparse.ArgumentTypeError(f"expected <provider>.<key>=<value>, got: {value!r}")
    return options


class ProviderOptionsAction(argparse.Action):
    """

    merge repeated/comma-separated --provider-options into one flat map."""

    def __call__(self, parser, namespace, values, option_string=None):
        merged = dict(getattr(namespace, self.dest, None) or {})
        try:
            for key, value in parse_provider_options(values).items():
                if key in merged:
                    raise argparse.ArgumentTypeError(f"duplicate provider option {key!r}")
                merged[key] = value
        except argparse.ArgumentTypeError as exc:
            raise argparse.ArgumentError(self, str(exc)) from exc
        setattr(namespace, self.dest, merged)


def provider_options_for(options: dict[str, str] | None, provider: str) -> dict[str, str]:
    """

    the subset of options addressed to one provider, with its prefix stripped."""

    prefix = f"{provider}."
    return {
        key[len(prefix):]: value
        for key, value in (options or {}).items()
        if key.startswith(prefix)
    }


def validate_provider_options(
    options: dict[str, str] | None, providers: list[str] | tuple[str, ...]
) -> None:
    """Validate provider options: addressed to a declared provider, and a key
    that provider actually offers. The engine checks the ADDRESS; each adapter
    checks its own KEYS — the engine knows none of them."""
    validate_provider_options_addressing(options, providers)
    from engine.execution.adapters import get_adapter

    for provider in providers:
        get_adapter(provider).validate_provider_options(
            provider_options_for(options, provider)
        )


def validate_provider_options_addressing(
    options: dict[str, str] | None, providers: list[str] | tuple[str, ...]
) -> None:
    """

    every option must address a provider that is actually participating."""

    declared = set(providers or ())
    stray = sorted(key for key in (options or {}) if key.split(".", 1)[0] not in declared)
    if stray:
        raise RuntimeError(
            "❌ --provider-options address providers not declared in --providers "
            f"{sorted(declared)}: {stray}"
        )


def resolve_provider_implementation_key(
    provider_options: dict[str, str] | None, provider: str
) -> str:
    """The credential implementation this run wants from one provider.

    WHERE a run executes (execution_runtime_mode) and HOW it authenticates are
    INDEPENDENT axes; this reads only the latter, from the provider's own
    options. REQUIRED — the engine has no implementation to default to, and it
    does not interpret the value: the adapter does.
    """

    options = provider_options_for(provider_options, provider)
    implementation_key = options.get("credential_implementation")
    if not implementation_key:
        raise RuntimeError(
            f"❌ no credential implementation declared for provider {provider!r}; "
            f"pass --provider-options {provider}.credential_implementation=... "
            f"(see `ctl.py providers`)"
        )
    return implementation_key


def run_provider_implementation_key(args: argparse.Namespace) -> str:
    """

    the single declared provider's credential implementation, from CLI args."""

    providers = list(getattr(args, "providers", ()) or ())
    if len(providers) != 1:
        raise RuntimeError(
            f"❌ this run declares providers {providers}, but the run-level "
            "provider catalog/preflight path is single-provider today; run them as "
            "separate invocations until per-target adapter dispatch lands"
        )
    return resolve_provider_implementation_key(
        getattr(args, "provider_options", None), providers[0]
    )


def validate_target_provider_coverage(
    active_target_runs: dict, providers: list[str] | tuple[str, ...]
) -> None:
    """Every provider a selected target declares must be among the run's.

    A target may declare several identities, so its provider SET must be a subset
    of `--providers`.

    The provider set is DECLARED (--providers), never inferred from the targets, so
    a target reaching for an undeclared provider fails loud instead of silently
    widening the run.
    """

    declared = set(providers or ())
    offenders = []
    for target_run_id, target_run in active_target_runs.items():
        undeclared = sorted(
            set(target_run.get("execution_identities") or {}) - declared
        )
        if undeclared:
            offenders.append(f"{target_run_id} (providers {undeclared})")
    offenders = sorted(offenders)
    if offenders:
        raise RuntimeError(
            "❌ selected target_runs use providers not declared in --providers "
            f"{sorted(declared)}: " + ", ".join(offenders)
        )
