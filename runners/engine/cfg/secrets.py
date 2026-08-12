"""Resolving a declared secret to its value.

A secret is a VALUE a provider produces on request, not specifically a
credential: a registry password, a signing key and a database password are the
same kind of thing. `ctl_secrets` declares which secrets exist; every consumer
references one BY KEY and never inlines its resolution, so rotating a secret is
one edit and the answer to "what secrets does this tree need" is one file.

`env` and `file` are resolved HERE rather than by an adapter, and that is not an
exception carved out for convenience. An adapter is a FETCHED package and
fetching needs a secret, so a secret provider that must itself be fetched can
never resolve the first one. They are the primitives that break that cycle.
Every other provider is delegated, and the engine names none of them.
"""

import os
from pathlib import Path

from engine.cfg import resources as cfg_resources
from engine.execution import adapters as execution_adapters
from engine.plt import providers as plt_providers

RESOURCE_KEY = "ctl_secrets"

# Resolvable without a credential, and therefore the only sources that can
# produce the secret an adapter is fetched with.
PRIMITIVE_PROVIDERS = ("env", "file")


class SecretStore:
    """The declared secrets of one cfg tree.

    An instance because every lookup reads the same registry from the same cfg
    root; passing the root per call would repeat the parameter the class exists
    to hold, and re-read the tree for each secret a run needs.
    """

    def __init__(
        self,
        ctl_cfg_root: Path,
        *,
        execution_context: dict[str, object] | None = None,
        implementation_key: str | None = None,
        execution_access_modes: dict[str, str] | None = None,
        provider_options: dict[str, str] | None = None,
    ):
        self.ctl_cfg_root = Path(ctl_cfg_root)
        self.execution_context = execution_context or {}
        self.implementation_key = implementation_key
        self.execution_access_modes = execution_access_modes or {}
        self.provider_options = provider_options or {}
        self._declared: dict | None = None

    @property
    def declared(self) -> dict:
        if self._declared is None:
            self._declared = cfg_resources.collect_resource(
                self.ctl_cfg_root, RESOURCE_KEY
            ) or {}
        return self._declared

    def entry(self, secret_key: str, *, label: str) -> dict:
        """The declaration for one key; an undeclared key is a hard error.

        Refused here rather than resolved to nothing, because a secret that
        silently resolves to `None` fails later as an authentication error, which
        reads as a wrong credential rather than a missing declaration.
        """

        if not isinstance(secret_key, str) or not secret_key.strip():
            raise RuntimeError(f"❌ {label}: secret_key must be a non-empty string")
        entry = self.declared.get(secret_key)
        if entry is None:
            known = ", ".join(sorted(self.declared)) or "none"
            raise RuntimeError(
                f"❌ {label}: secret_key {secret_key!r} is not declared in "
                f"{RESOURCE_KEY}; declared: {known}"
            )
        if not isinstance(entry, dict):
            raise RuntimeError(f"❌ {RESOURCE_KEY}.{secret_key} must be a mapping")
        return entry

    def resolve(self, secret_key: str, *, label: str) -> str:
        entry = self.entry(secret_key, label=label)
        provider = entry.get("provider")
        if not isinstance(provider, str) or not provider.strip():
            raise RuntimeError(
                f"❌ {RESOURCE_KEY}.{secret_key} must declare a provider"
            )
        provider = provider.strip()
        where = f"{RESOURCE_KEY}.{secret_key}"

        if provider == "env":
            return self._from_env(entry, where=where)
        if provider == "file":
            return self._from_file(entry, where=where)
        return self._from_provider(provider, entry, where=where)

    def _from_env(self, entry: dict, *, where: str) -> str:
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            raise RuntimeError(f"❌ {where}: provider 'env' requires a `name`")
        value = os.getenv(name.strip())
        if value is None:
            raise RuntimeError(
                f"❌ {where}: environment variable {name.strip()!r} is not set"
            )
        return value

    def _from_file(self, entry: dict, *, where: str) -> str:
        raw = entry.get("path")
        if not isinstance(raw, str) or not raw.strip():
            raise RuntimeError(f"❌ {where}: provider 'file' requires a `path`")
        path = Path(raw.strip()).expanduser()
        if not path.is_file():
            raise RuntimeError(f"❌ {where}: secret file not found: {path}")
        # trailing newline is how an editor saves a one-line secret, never part of it
        return path.read_text(encoding="utf-8").rstrip("\n")

    def _from_provider(self, provider: str, entry: dict, *, where: str) -> str:
        adapter = execution_adapters.get_adapter(provider, self.ctl_cfg_root)
        resolve = getattr(adapter, "resolve_secret", None)
        if resolve is None:
            raise RuntimeError(
                f"❌ {where}: provider {provider!r} does not implement the "
                f"'secrets' contract"
            )
        value = resolve(
            {key: value for key, value in entry.items() if key != "provider"},
            ctl_cfg_root=self.ctl_cfg_root,
            execution_context=self.execution_context,
            implementation_key=self.implementation_key,
            execution_access_mode=self.execution_access_modes.get(provider, "standard"),
            provider_options=self.provider_options,
        )
        if not isinstance(value, str) or not value:
            raise RuntimeError(f"❌ {where}: provider {provider!r} returned no value")
        return value


def validate_declared_secrets(ctl_cfg_root: Path) -> None:
    """Every declared secret names a provider that could resolve it.

    A provider-backed secret whose adapter is itself fetched using a secret is a
    cycle. It cannot be detected by following one entry, because the fetch chain
    runs through a provider registry, so the check is the invariant that makes the
    cycle impossible: the secret that fetches an adapter must be primitive.
    """

    store = SecretStore(ctl_cfg_root)
    declared = store.declared
    provider_registries = (
        (
            "execution_providers",
            execution_adapters.load_execution_providers(ctl_cfg_root) or {},
        ),
        ("plt_providers", plt_providers.ProviderRegistry(ctl_cfg_root).entries),
    )
    for registry_key, providers in provider_registries:
        for provider, spec in providers.items():
            secret_key = ((spec or {}).get("source") or {}).get("secret_key")
            if secret_key is None:
                continue
            entry = store.entry(secret_key, label=f"{registry_key}.{provider}.source")
            entry_provider = entry.get("provider")
            if entry_provider not in PRIMITIVE_PROVIDERS:
                raise RuntimeError(
                    f"❌ {registry_key}.{provider}.source uses secret {secret_key!r}, "
                    f"which resolves through provider {entry_provider!r}. The secret "
                    f"that FETCHES an adapter must resolve without one — declare it "
                    f"with a primitive provider ({', '.join(PRIMITIVE_PROVIDERS)})"
                )
    for secret_key, entry in declared.items():
        if not isinstance(entry, dict) or not entry.get("provider"):
            raise RuntimeError(f"❌ {RESOURCE_KEY}.{secret_key} must declare a provider")
