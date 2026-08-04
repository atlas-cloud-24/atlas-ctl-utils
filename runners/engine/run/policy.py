"""What a ctl profile PERMITS.

A policy answers "is this allowed?" and never "which one is this?". Nothing here
may be used to select or resolve anything; the moment a permission is read as a
choice, a guard has become a router and stops guarding."""

from dataclasses import dataclass
from pathlib import Path

from engine.execution import providers as execution_providers
from engine.execution import references as execution_references
from engine.kernel import yaml_io as kernel_yaml_io

def load_ctl_profiles(ctl_cfg_root: Path) -> dict[str, dict]:
    """Load the ctl profile catalog (content key: ctl_profiles) — named policy
    bundles governing ctl behavior.

    TWO LEVELS, split by who DEFINES the concept: the top level holds engine
    policies (ref_policy, execution runtime, ctl-state sync skips, guardrail and
    cfg-gate skips) plus `allowed_providers`; a `<provider>:` block holds the
    policies whose vocabulary belongs to that provider. The engine never reads
    inside a provider block — it hands it to the adapter."""
    profiles: dict[str, dict] = {}
    for path, section in kernel_yaml_io.collect_top_level_sections(ctl_cfg_root, "ctl_profiles"):
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ ctl_profiles must be a mapping: {path}")
        for profile_name, policy in section.items():
            if profile_name in profiles:
                raise RuntimeError(f"❌ duplicate ctl profile {profile_name!r}: {path}")
            if not isinstance(profile_name, str) or not profile_name.strip():
                raise RuntimeError(f"❌ ctl profile names must be non-empty strings: {path}")
            if policy is not None and not isinstance(policy, dict):
                raise RuntimeError(f"❌ ctl profile {profile_name!r} policy must be a mapping: {path}")
            profiles[profile_name] = policy or {}
    return profiles


def ctl_profile_policy(ctl_cfg_root: Path, ctl_profile: str) -> dict:
    profiles = load_ctl_profiles(ctl_cfg_root)
    if ctl_profile not in profiles:
        known = ", ".join(sorted(profiles)) or "none"
        raise RuntimeError(f"❌ unknown ctl profile {ctl_profile!r}; known profiles: {known}")
    return profiles[ctl_profile]


# The closed set of ref_policy values. The engine branches strict-vs-permissive
# on `commit_required` (ref_policy_requires_commits); every other value is the
# permissive path. Validating against this set at load makes a typo fail loud
# instead of silently degrading to permissive (the unsafe direction).
REF_POLICY_COMMIT_REQUIRED = "commit_required"


REF_POLICY_LOCAL_DIRTY_ALLOWED = "local_dirty_allowed"


REF_POLICIES = frozenset({REF_POLICY_COMMIT_REQUIRED, REF_POLICY_LOCAL_DIRTY_ALLOWED})


def ctl_ref_policy(ctl_cfg_root: Path, ctl_profile: str) -> str:
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    ref_policy = policy.get("ref_policy")
    if not isinstance(ref_policy, str) or not ref_policy.strip():
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} must define non-empty ref_policy")
    ref_policy = ref_policy.strip()
    if ref_policy not in REF_POLICIES:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} has unknown ref_policy {ref_policy!r}; "
            f"expected one of {sorted(REF_POLICIES)}"
        )
    return ref_policy


def ctl_profile_bool(ctl_cfg_root: Path, ctl_profile: str, key: str) -> bool:
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    value = policy.get(key, False)
    if not isinstance(value, bool):
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} {key} must be a bool: {value!r}")
    return value


@dataclass(frozen=True)
class Permission:
    """One thing a ctl profile can GRANT.

    A permission answers "is this allowed?" and never "which one is this?" — it
    is a guard, and a guard used to select something has stopped guarding.

    Adding a permission is one row in `Permissions`. It used to be a bool
    accessor, a validator, a refusal message and an argparse flag written out
    separately, which is four places for one grant to drift: the message quoted
    the cfg key by hand, so a renamed key left the refusal naming a field that no
    longer existed.
    """

    key: str
    flag: str

    def granted(self, ctl_cfg_root: Path, ctl_profile: str) -> bool:
        return ctl_profile_bool(ctl_cfg_root, ctl_profile, self.key)

    def require(self, ctl_cfg_root: Path, ctl_profile: str, *, requested: bool) -> None:
        """Refuse a request this profile does not grant. A request that was not
        made needs no grant, so an ungranted permission costs nothing unused."""

        if requested and not self.granted(ctl_cfg_root, ctl_profile):
            raise RuntimeError(
                f"❌ {self.flag} was requested, but ctl profile {ctl_profile!r} "
                f"does not grant {self.key}"
            )


class Permissions:
    """Every grant a ctl profile can make."""

    AGREED_DEFER_CTL_STATE_BACKEND_SYNC = Permission(
        "allow_agreed_defer_ctl_state_backend_sync",
        "--agreed-defer-ctl-state-backend-sync",
    )
    FORCE_SKIP_CTL_STATE_BACKEND_SYNC = Permission(
        "allow_force_skip_ctl_state_backend_sync",
        "--force-skip-ctl-state-backend-sync",
    )
    FORCE_SKIP_GUARDRAILS = Permission(
        "allow_force_skip_guardrails", "--force-skip-guardrails"
    )
    FORCE_SKIP_FULL_CFG_VALIDATION_GATE = Permission(
        "allow_force_skip_full_cfg_validation_gate",
        "--force-skip-full-cfg-validation-gate",
    )
    FORCE_SKIP_EXECUTION_IDENTITY_PREFLIGHT_CHECK = Permission(
        "allow_force_skip_execution_identity_preflight_check",
        "--force-skip-execution-identity-preflight-check",
    )
    SKIP_CHILDREN_PRECHECK = Permission(
        "allow_skip_children_precheck", "--skip-children-precheck"
    )
    # Erasing the only evidence that something is provisioned is a different
    # power from aging out history, so it is a different grant.
    CTL_STATE_FORGET = Permission("allow_ctl_state_forget", "--forget")
    CTL_STATE_HISTORY_MAINTENANCE = Permission(
        "allow_ctl_state_history_maintenance", "--history-prune"
    )


# execution runtime (WHERE a target_run's box is produced). CTL selects one
# runtime for the whole run (always explicit, no default); the target_run declares
# which it can run in (a constraint).
EXECUTION_RUNTIME_MODES = ("local", "ci")


def step_supported_execution_runtime_modes(runtime_cfg: dict, *, label: str) -> set[str]:
    """

    runtimes a target_run can run in; absent = all EXECUTION_RUNTIME_MODES."""

    raw = runtime_cfg.get("supported_execution_runtime_modes")
    if raw is None:
        return set(EXECUTION_RUNTIME_MODES)
    if not isinstance(raw, list) or not all(isinstance(r, str) for r in raw):
        raise RuntimeError(f"❌ target_run runtime.supported_execution_runtime_modes must be a list of strings: {label}")
    runtimes = set(raw)
    unknown = runtimes - set(EXECUTION_RUNTIME_MODES)
    if unknown:
        raise RuntimeError(f"❌ target_run runtime.supported_execution_runtime_modes has unknown runtimes {sorted(unknown)}: {label}")
    if not runtimes:
        raise RuntimeError(f"❌ target_run runtime.supported_execution_runtime_modes must not be empty: {label}")
    return runtimes


def ctl_allowed_execution_runtime_modes(ctl_cfg_root: Path, ctl_profile: str) -> set[str]:
    """

    runtimes the ctl profile authorizes. Absent = all EXECUTION_RUNTIME_MODES."""

    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    raw = policy.get("allowed_execution_runtime_modes")
    if raw is None:
        return set(EXECUTION_RUNTIME_MODES)
    if not isinstance(raw, list) or not all(isinstance(r, str) for r in raw):
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} allowed_execution_runtime_modes must be a list of strings")
    runtimes = set(raw)
    unknown = runtimes - set(EXECUTION_RUNTIME_MODES)
    if unknown:
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} allowed_execution_runtime_modes has unknown runtimes {sorted(unknown)}")
    if not runtimes:
        raise RuntimeError(f"❌ ctl profile {ctl_profile!r} allowed_execution_runtime_modes must not be empty")
    return runtimes


def validate_execution_runtime_mode(ctl_cfg_root: Path, ctl_profile: str, execution_runtime_mode: str) -> None:
    """

    reconcile the selected runtime against the ctl profile: a known
    runtime, allowed by the profile. Per-target_run `supported_execution_runtime_modes` is enforced in
    run_targets, where the repo-local target_run manifest is loaded."""

    if execution_runtime_mode not in EXECUTION_RUNTIME_MODES:
        raise RuntimeError(f"❌ unknown execution runtime {execution_runtime_mode!r} (known: {sorted(EXECUTION_RUNTIME_MODES)})")
    allowed = ctl_allowed_execution_runtime_modes(ctl_cfg_root, ctl_profile)
    if execution_runtime_mode not in allowed:
        raise RuntimeError(
            f"❌ execution runtime {execution_runtime_mode!r} is not allowed by ctl profile {ctl_profile!r} (allowed: {sorted(allowed)})"
        )


def ctl_allowed_providers(ctl_cfg_root: Path, ctl_profile: str) -> list[str]:
    """Providers this ctl profile may run.

    Declared, never defaulted: a profile states which providers it authorizes,
    and each one must carry its own policy block. Provider IDENTITY is engine
    vocabulary; everything inside a provider's block is not.
    """

    from engine.execution.adapters import registered_providers

    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile)
    raw = policy.get("allowed_providers")
    if not isinstance(raw, list) or not raw or not all(isinstance(p, str) for p in raw):
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} must declare allowed_providers as a "
            "non-empty list of provider names"
        )
    declared = registered_providers(ctl_cfg_root)
    unknown = sorted(set(raw) - set(declared))
    if unknown:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} allowed_providers names undeclared "
            f"providers {unknown}; declared: {list(declared)}"
        )
    missing = sorted(p for p in raw if not isinstance(policy.get(p), dict))
    if missing:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} allows providers {missing} but declares no "
            f"policy block for them (expected a `{missing[0]}:` mapping in the profile)"
        )
    return list(raw)


def validate_ctl_allowed_providers(
    ctl_cfg_root: Path, ctl_profile: str, providers: list[str] | tuple[str, ...]
) -> None:
    allowed = ctl_allowed_providers(ctl_cfg_root, ctl_profile)
    stray = sorted(set(providers) - set(allowed))
    if stray:
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} does not allow providers {stray} "
            f"(allowed: {allowed})"
        )


def ctl_profile_provider_policy(
    ctl_cfg_root: Path, ctl_profile: str, provider: str
) -> dict:
    """One provider's policy block, opaque to the engine — the adapter reads it."""
    ctl_allowed_providers(ctl_cfg_root, ctl_profile)
    policy = ctl_profile_policy(ctl_cfg_root, ctl_profile).get(provider)
    if not isinstance(policy, dict):
        raise RuntimeError(
            f"❌ ctl profile {ctl_profile!r} declares no {provider!r} policy block"
        )
    return policy


def validate_cadence_supported(provider: str, mode: str) -> None:
    """The ADAPTER decides which cadences exist for it; the engine only asks.

    Without this the set of legal modes lived in engine strings, so adding a
    provider that refreshes differently meant editing the engine.
    """

    supported = execution_providers.get_provider_adapter(provider).supported_credential_refresh_modes()
    if mode not in supported:
        raise RuntimeError(
            f"❌ --credential-refresh-mode {provider}={mode} is not supported by "
            f"{provider} (supported: {', '.join(sorted(supported)) or 'none'})"
        )


def validate_credential_refresh_modes(
    ctl_cfg_root: Path,
    ctl_profile: str,
    requested: dict[str, str],
    providers,
    execution_access_modes: dict[str, str] | None = None,
) -> dict[str, str]:
    """Every participating provider states its cadence, and the profile allows it.

    Per provider because only some have expiring sessions at all — the engine
    holds an opaque provider->value map and hands each adapter its own, exactly
    as it does for execution access modes.

    There is no inherited default: an unnamed provider is a caller who skipped
    the choice, not one who wanted the cheap option.
    """

    selected = dict(requested or {})
    missing = [p for p in providers if p not in selected]
    if missing:
        raise RuntimeError(
            "❌ --credential-refresh-mode must name every provider given to "
            f"--providers; missing: {', '.join(sorted(missing))}"
        )
    unknown = [p for p in selected if p not in providers]
    if unknown:
        raise RuntimeError(
            "❌ --credential-refresh-mode names providers not in --providers: "
            + ", ".join(sorted(unknown))
        )
    for provider, mode in sorted(selected.items()):
        validate_cadence_supported(provider, mode)
        validate_cadence_against_access_mode(
            {provider: mode}, execution_access_modes
        )
        allowed = ctl_profile_provider_policy(
            ctl_cfg_root, ctl_profile, provider
        ).get("allowed_credential_refresh_modes") or []
        if mode not in allowed:
            raise RuntimeError(
                f"❌ ctl profile {ctl_profile!r} does not allow credential refresh "
                f"mode {mode!r} for provider {provider!r} "
                f"(allowed: {', '.join(allowed) or 'none'})"
            )
    return selected


def validate_skip_children_precheck(
    ctl_cfg_root: Path, ctl_profile: str, requested: bool
) -> None:
    Permissions.SKIP_CHILDREN_PRECHECK.require(
        ctl_cfg_root, ctl_profile, requested=requested
    )


def validate_force_skip_full_cfg_validation_gate_policy(
    ctl_cfg_root: Path, ctl_profile: str, requested: bool
) -> None:
    Permissions.FORCE_SKIP_FULL_CFG_VALIDATION_GATE.require(
        ctl_cfg_root, ctl_profile, requested=requested
    )


def ref_policy_requires_commits(ref_policy: str) -> bool:
    return ref_policy == REF_POLICY_COMMIT_REQUIRED


def validate_skip_up_to_date_ref_policy(
    skip_up_to_date: bool | None, ref_policy: str, ctl_profile: str
) -> None:
    """Fail loud on a --skip-up-to-date=true that can never reuse.

    The reuse gate only reuses a committed result when ref_policy is
    commit_required (a clean, commit-pinned source). Under any other policy
    (e.g. local_dirty_allowed) reuse is structurally impossible, so
    --skip-up-to-date true would be a silent no-op — every run re-executes.
    Reject it instead of silently ignoring the flag."""

    if skip_up_to_date and not ref_policy_requires_commits(ref_policy):
        raise RuntimeError(
            f"❌ --skip-up-to-date true cannot reuse under ctl profile {ctl_profile!r} "
            f"(ref_policy {ref_policy!r}): reuse requires ref_policy 'commit_required' "
            "(a clean, commit-pinned source). Pass --skip-up-to-date false, or use a "
            "commit_required profile."
        )


def validate_cadence_against_access_mode(
    refresh_modes: dict[str, str], access_modes: dict[str, str] | None
) -> None:
    """Refuse a cadence that cannot do anything on the chosen access path.

    Checked during ARGUMENT finalization, not inside the pipeline: it needs no
    cfg, and catching it after identity preflight would mean resolving
    credentials before telling the caller the request was meaningless.

    WHICH cadence works on WHICH access path is the ADAPTER's knowledge — whether
    a fresh session can be minted depends on how that provider authenticates, not
    on the engine's step loop. The engine names no cadence and no access mode of
    its own here; it asks and enforces the answer.
    """

    for provider, mode in sorted((refresh_modes or {}).items()):
        access = (access_modes or {}).get(provider)
        if not access:
            continue
        usable = execution_providers.get_provider_adapter(provider).credential_refresh_mode_access_modes(
            mode
        )
        if usable and access not in usable:
            raise RuntimeError(
                f"❌ --credential-refresh-mode {provider}={mode} has no effect "
                f"under --execution-access-mode {provider}={access}: that path "
                "re-uses the credential already resolved rather than acquiring a "
                f"new one. {provider} honours {mode} only under "
                f"{', '.join(usable)}"
            )


def active_target_names(workflow_cfg: dict) -> list[str]:
    names: list[str] = []
    for entry in workflow_cfg.get("target_runs", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        if isinstance(target_name, str) and target_name:
            names.append(target_name)
    return names


def active_targets_missing_key(workflow_cfg: dict, action_cfg: dict, skip_key: str) -> list[str]:
    """

    targets that do NOT declare the given skip_* key (presence = capability)."""

    targets = action_cfg.get("targets", {})
    missing: list[str] = []
    for target_name in active_target_names(workflow_cfg):
        target_cfg = targets.get(target_name) or {}
        if skip_key not in target_cfg:
            missing.append(target_name)
    return missing


def validate_execution_access(
    ctl_cfg_root: Path,
    ctl_profile: str,
    workflow_cfg: dict,
    action_cfg: dict,
    *,
    execution_context: dict[str, object],
    execution_access_modes: dict[str, str],
    agreed_defer_ctl_state_backend_sync: bool,
    force_skip_ctl_state_backend_sync: bool,
    provider_options: dict[str, str] | None,
    force_skip_execution_identity_preflight_check: list[str] | None = None,
) -> None:
    """Validate the PROVIDER-NEUTRAL execution access policy: per-target mode
    consent, and the ctl-state sync / guardrail / cfg-gate skip permissions.

    Provider gating and each provider's own policy (mode, credential
    implementation, option grants) are validated separately — the
    `allowed_providers` and `provider_access_policy` checks — so this function
    names no provider vocabulary."""
    # consent: a mode may require each active target of that provider to have
    # declared, up front, that it accepts being run this way and with what
    #. The ADAPTER says which of its modes need consent and which
    # target fields carry it; declaring the sources is not the same as opting in.
    targets = action_cfg.get("targets", {})
    for target_name in active_target_names(workflow_cfg):
        target_cfg = targets.get(target_name) or {}
        identities = target_cfg.get("execution_identities")
        if identities is None:
            continue  # coverage is validated separately
        # A target may declare several identities; consent is asked of EACH
        # provider that this run actually activates.
        for provider, execution in sorted((identities or {}).items()):
            if provider not in (execution_access_modes or {}):
                continue  # provider coverage is validated separately
            mode = execution_access_modes[str(provider)]
            consent = execution_providers.get_provider_adapter(str(provider)).target_consent(mode)
            if not consent:
                continue
            opt_in_field = consent["opt_in_field"]
            opt_in = target_cfg.get(opt_in_field, False)
            if not isinstance(opt_in, bool):
                raise RuntimeError(f"❌ target {opt_in_field} must be a boolean")
            if not opt_in:
                raise RuntimeError(
                    f"❌ execution access mode {mode!r} was requested, but target "
                    f"{target_name!r} does not set {opt_in_field}: true"
                )
            execution_field = consent["execution_field"]
            if not ((execution or {}).get(execution_field) or []):
                raise RuntimeError(
                    f"❌ execution access mode {mode!r} was requested, but target "
                    f"{target_name!r} declares no execution_identities.{provider}.{execution_field}"
                )

    # ctl-state sync skip (an operation, orthogonal to access mode)
    for permission, requested in (
        (Permissions.AGREED_DEFER_CTL_STATE_BACKEND_SYNC, agreed_defer_ctl_state_backend_sync),
        (Permissions.FORCE_SKIP_CTL_STATE_BACKEND_SYNC, force_skip_ctl_state_backend_sync),
    ):
        permission.require(ctl_cfg_root, ctl_profile, requested=requested)
    ctl_flags = f"{execution_references.EXECUTION_CONTEXT_ROOT}.ctl"
    Permissions.FORCE_SKIP_GUARDRAILS.require(
        ctl_cfg_root,
        ctl_profile,
        requested=bool(execution_context.get(f"{ctl_flags}.force_skip_guardrails")),
    )
    Permissions.FORCE_SKIP_FULL_CFG_VALIDATION_GATE.require(
        ctl_cfg_root,
        ctl_profile,
        requested=bool(
            execution_context.get(f"{ctl_flags}.force_skip_full_cfg_validation_gate")
        ),
    )
    Permissions.FORCE_SKIP_EXECUTION_IDENTITY_PREFLIGHT_CHECK.require(
        ctl_cfg_root,
        ctl_profile,
        requested=bool(force_skip_execution_identity_preflight_check),
    )
    if agreed_defer_ctl_state_backend_sync:
        missing = active_targets_missing_key(workflow_cfg, action_cfg, "allow_agreed_defer_ctl_state_backend_sync")
        if missing:
            raise RuntimeError(
                "❌ --agreed-defer-ctl-state-backend-sync was requested, but active targets do not "
                "declare allow_agreed_defer_ctl_state_backend_sync: true: " + ", ".join(sorted(missing))
            )


def load_target_policy_constraints(ctl_cfg_root: Path) -> list[dict]:
    constraints: list[dict] = []
    for path, section in kernel_yaml_io.collect_top_level_sections(ctl_cfg_root, "target_policy_constraints"):
        if not isinstance(section, list):
            raise RuntimeError(f"❌ target_policy_constraints must be a list: {path}")
        for idx, raw in enumerate(section, start=1):
            if not isinstance(raw, dict):
                raise RuntimeError(f"❌ target_policy_constraints entry #{idx} must be a mapping: {path}")
            target_prefix = raw.get("target_prefix")
            required_ref_policy = raw.get("required_ref_policy")
            if not isinstance(target_prefix, str) or not target_prefix.strip():
                raise RuntimeError(f"❌ target_policy_constraints entry #{idx} target_prefix must be a non-empty string: {path}")
            if not isinstance(required_ref_policy, str) or not required_ref_policy.strip():
                raise RuntimeError(f"❌ target_policy_constraints entry #{idx} required_ref_policy must be a non-empty string: {path}")
            constraints.append({
                "target_prefix": target_prefix.strip(),
                "required_ref_policy": required_ref_policy.strip(),
            })
    return constraints


def validate_target_policy_constraints(
    ctl_cfg_root: Path,
    ctl_profile: str,
    workflow_cfg: dict,
    action_cfg: dict,
) -> None:
    del action_cfg  # reserved for future target-policy dimensions
    active = active_target_names(workflow_cfg)
    if not active:
        return
    selected_ref_policy = ctl_ref_policy(ctl_cfg_root, ctl_profile)
    for constraint in load_target_policy_constraints(ctl_cfg_root):
        prefix = constraint["target_prefix"]
        matching = sorted(target for target in active if target.startswith(prefix))
        if not matching:
            continue
        required_ref_policy = constraint["required_ref_policy"]
        if selected_ref_policy != required_ref_policy:
            raise RuntimeError(
                f"❌ active targets under {prefix!r} require ref_policy {required_ref_policy!r}; "
                f"ctl profile {ctl_profile!r} uses {selected_ref_policy!r}. Targets: {', '.join(matching)}"
            )


def validate_target_policy_constraints_for_target(
    ctl_cfg_root: Path, ctl_profile: str, target_key: str
) -> None:
    """Per-target variant of validate_target_policy_constraints — one target's
    ref-policy requirement, so the ctl-policy report can attribute it per target."""
    selected_ref_policy = ctl_ref_policy(ctl_cfg_root, ctl_profile)
    for constraint in load_target_policy_constraints(ctl_cfg_root):
        prefix = constraint["target_prefix"]
        if not target_key.startswith(prefix):
            continue
        required_ref_policy = constraint["required_ref_policy"]
        if selected_ref_policy != required_ref_policy:
            raise RuntimeError(
                f"❌ target {target_key!r} under {prefix!r} requires ref_policy "
                f"{required_ref_policy!r}; ctl profile {ctl_profile!r} uses {selected_ref_policy!r}"
            )
