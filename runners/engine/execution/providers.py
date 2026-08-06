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
from engine.execution import adapters
from engine.execution import references as execution_references

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
    adapters.set_active_ctl_cfg_root(Path(ctl_cfg_root))
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

    return [(name, adapters.get_adapter(name)) for name in run_providers(execution_context)]


def run_provider_adapter(execution_context: dict[str, object]):
    """The single participating provider's adapter.

    The run-level catalog/preflight path still assumes ONE adapter per run (it keeps
    one `provider_catalogs` bundle). Declaring several providers is accepted by the
    CLI, gating and coverage guard, but this path is not wired for it yet — so fail
    loud here rather than silently picking the first.
    """

    return adapters.get_adapter(run_provider(execution_context))


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
    for provider in providers:
        adapters.get_adapter(provider).validate_provider_options(
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

# What a declared contract OBLIGES an adapter to expose. `implements:` was
# accepted and read by nobody, so a provider could claim a contract its package
# did not satisfy and the mismatch surfaced mid-run as an AttributeError — which
# reads as an engine bug rather than as a cfg error. Checking it before the run
# is the registry's whole value.
CONTRACT_CALLABLES: dict[str, tuple[str, ...]] = {
    "execution": (
        "preflight_execution_identity",
        "materialize_target_binding",
        "validate_active_target_access",
        "supported_execution_access_modes",
        "normal_execution_access_mode",
        "resolves_execution_identity",
        "target_consent",
        "execution_access_mode_from_options",
        "target_assertion_argv",
        "load_runtime_catalogs",
    ),
    "ctl_state": (
        "resolve_ctl_state_credential",
        "create_state_syncer",
        "validate_state_backend_entry",
    ),
    "secrets": (
        "resolve_secret",
    ),
}


def validate_declared_contracts(ctl_cfg_root) -> None:
    """Every `implements:` entry is backed by the callables it promises.

    A declared contract with nothing behind it is a registration accepted but not
    honoured — the silent acceptance this registry exists to prevent.
    """

    declared = adapters.load_ctl_providers(ctl_cfg_root) or {}
    for provider, spec in declared.items():
        contracts = (spec or {}).get("implements")
        if not isinstance(contracts, list) or not contracts:
            raise RuntimeError(
                f"❌ ctl_providers.{provider} must declare a non-empty `implements` "
                f"list; there is no default"
            )
        unknown = sorted(set(contracts) - set(CONTRACT_CALLABLES))
        if unknown:
            raise RuntimeError(
                f"❌ ctl_providers.{provider} declares unknown contracts {unknown}; "
                f"known: {sorted(CONTRACT_CALLABLES)}"
            )
        adapter = adapters.get_adapter(provider, ctl_cfg_root)
        for contract in contracts:
            missing = [
                name for name in CONTRACT_CALLABLES[contract]
                if not callable(getattr(adapter, name, None))
            ]
            if missing:
                raise RuntimeError(
                    f"❌ ctl_providers.{provider} declares contract {contract!r} but "
                    f"its adapter does not implement {missing}"
                )
