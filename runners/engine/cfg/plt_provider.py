"""Optional provider-owned configuration on selectable cfg units."""

import re

_PROVIDER_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


class ProviderBinding:
    """Validate provider identity while leaving provider-owned cfg opaque."""

    @staticmethod
    def selectable_unit(owner: object, *, label: str) -> dict[str, object]:
        """Return a scope or overlay binding with optional provider cfg."""

        binding = ProviderBinding._binding(owner, label=label, opaque_key="provider_cfg")
        plt_cfg = owner.get("plt") if isinstance(owner, dict) else None
        provider = plt_cfg.get("provider") if isinstance(plt_cfg, dict) else None
        if provider is not None:
            if not isinstance(provider, str) or not _PROVIDER_KEY_RE.fullmatch(provider):
                raise RuntimeError(f"❌ {label}.plt.provider has an invalid shape")
            binding["provider"] = provider
        return binding

    @staticmethod
    def source_step(owner: object, *, label: str) -> dict[str, object]:
        """Return a source-step binding with an optional provider request."""

        return ProviderBinding._binding(owner, label=label, opaque_key="request")

    @staticmethod
    def _binding(
        owner: object,
        *,
        label: str,
        opaque_key: str,
    ) -> dict[str, object]:
        if not isinstance(owner, dict):
            raise RuntimeError(f"❌ {label} must be a mapping")

        plt_cfg = owner.get("plt")
        if plt_cfg is None:
            return {}
        if not isinstance(plt_cfg, dict) or not plt_cfg:
            raise RuntimeError(f"❌ {label}.plt must be a non-empty mapping")

        allowed = {opaque_key, "provider"} if opaque_key == "provider_cfg" else {opaque_key}
        unknown = set(plt_cfg) - allowed
        if unknown:
            raise RuntimeError(f"❌ {label}.plt has unsupported keys {sorted(unknown)}")

        binding: dict[str, object] = {}
        if opaque_key in plt_cfg:
            opaque_cfg = plt_cfg[opaque_key]
            if not isinstance(opaque_cfg, dict) or not opaque_cfg:
                raise RuntimeError(
                    f"❌ {label}.plt.{opaque_key} must be a non-empty mapping when declared"
                )
            binding[opaque_key] = dict(opaque_cfg)
        return binding
