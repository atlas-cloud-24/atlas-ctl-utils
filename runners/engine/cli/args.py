"""The CLI surface of a runner entrypoint.

Argument declaration and normalisation only: what a runner accepts, and how a
raw namespace becomes the validated values the engine works with. Nothing here
decides what a run DOES.

Split out of the engine module because nothing in the engine calls it — it is a
pure leaf, invoked by entrypoints alone.

Borrowed engine names are reached through `common.<name>` rather than imported
by name: a `from ... import` binds at import time, so a test patching
`common.<name>` would not reach a call made from here.
"""

from __future__ import annotations

import argparse
import collections
from pathlib import Path

from engine.catalog import workflow as catalog_workflow
from engine.execution import providers as execution_providers
from engine.kernel import ids as kernel_ids
from engine.kernel import scalars as kernel_scalars
from engine.run import actions as run_actions
from engine.run import policy as run_policy
from engine.run import selectors as run_selectors
from engine.state import status as state_status


def parse_selector_pairs(value: str) -> list[tuple[str, str]]:
    """Parse one --execution-params value into (key, value) pairs.

    Accepts a comma-separated list (`a=1,b=2`); a single pair is the one-element
    case. Combined with ExecutionParamsAction (which EXTENDS the dest list), both
    `--execution-params a=1,b=2` and the repeated `--execution-params a=1
    --execution-params b=2` produce the same flat list of pairs.
    """
    pairs: list[tuple[str, str]] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Selector must use key=value format, got: {item!r}")
        key, selector_value = item.split("=", 1)
        key = key.strip()
        selector_value = selector_value.strip()
        if not key or not selector_value:
            raise ValueError(f"Selector must use non-empty key=value, got: {item!r}")
        pairs.append((key, selector_value))
    if not pairs:
        raise ValueError(f"Selector must use key=value format, got: {value!r}")
    return pairs


class ExecutionParamsAction(argparse.Action):
    """Collect execution params as a FLAT list of (key, value) pairs.

    `type=` is deliberately not used: an append action with a list-returning type
    would nest to list[list[tuple]], while every downstream consumer
    (common.selectors_to_map, the arg normalizers, the tests) expects a flat
    list[tuple[str, str]].
    """

    def __call__(self, parser, namespace, values, option_string=None):
        current = list(getattr(namespace, self.dest, None) or [])
        try:
            current.extend(parse_selector_pairs(values))
        except ValueError as exc:
            raise argparse.ArgumentError(self, str(exc)) from exc
        setattr(namespace, self.dest, current)


def normalize_ctl_state_local_root(value: str) -> Path:
    """

    normalize the operator-provided local ctl-state root directory."""

    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("❌ --ctl-state-local-root must be a non-empty directory path")
    root = Path(value.strip()).expanduser().resolve()
    if root.exists() and not root.is_dir():
        raise RuntimeError(f"❌ --ctl-state-local-root exists but is not a directory: {root}")
    return root


def finalize_common_args(args: argparse.Namespace) -> None:
    """Normalize execution-params CLI args into a map and common values."""
    args.execution_params = run_selectors.selectors_to_map(args.execution_param, label="execution param")
    args.ctl_state_local_root = normalize_ctl_state_local_root(args.ctl_state_local_root)
    # BEFORE the first adapter call: provider options are validated by the adapter
    # itself, so its repository has to be importable by then.
    execution_providers.activate_provider_adapters(getattr(args, "ctl_cfg", None))
    execution_providers.validate_provider_options(
        getattr(args, "provider_options", None), getattr(args, "providers", ()) or ()
    )
    args.execution_access_modes = normalize_execution_access_modes(args)
    args.credential_refresh_modes = run_selectors.selectors_to_map(
        getattr(args, "credential_refresh_modes", []) or [],
        label="credential refresh mode",
    )
    run_policy.validate_cadence_against_access_mode(
        args.credential_refresh_modes, args.execution_access_modes
    )
    args.force_skip_execution_identity_preflight_check = (
        normalize_force_skip_execution_identity_preflight_check(args)
    )
    # --status is gone from the run parsers (status is standalone).
    if (
        getattr(args, "execution_identity_preflight_check_only", False)
        and getattr(args, "force_skip_execution_identity_preflight_check", None)
    ):
        raise RuntimeError(
            "❌ --execution-identity-preflight-check-only and "
            "--force-skip-execution-identity-preflight-check are mutually exclusive"
        )
    # --skip-up-to-date is an explicit true/false with no default. A normal run
    # reaches the reuse-vs-rerun decision and must state intent; the exit-early
    # preflight-only mode never does, so it stays optional there.
    if hasattr(args, "skip_up_to_date"):
        exits_before_execution = getattr(
            args, "execution_identity_preflight_check_only", False
        )
        if args.skip_up_to_date is None and not exits_before_execution:
            raise RuntimeError(
                "❌ --skip-up-to-date is required (true or false) for a normal run; "
                "omit it only with --execution-identity-preflight-check-only"
            )
    args.run_id = kernel_ids.generate_uuid7()


def normalize_execution_access_modes(args: argparse.Namespace) -> dict[str, str]:
    """Resolve the per-provider access mode map.

    `--execution-access-mode <provider>=<mode>,...` — one mode per participating
    provider.
    A provider left unnamed takes the engine default; the engine validates only
    that every named provider was declared, because the MODE NAMES belong to the
    adapters (validated against supported_execution_access_modes()).
    """

    declared = list(getattr(args, "providers", ()) or ())
    modes = dict(getattr(args, "execution_access_modes", None) or {})

    stray = sorted(set(modes) - set(declared))
    if stray:
        raise RuntimeError(
            f"❌ --execution-access-mode names providers not declared in --providers "
            f"{declared}: {stray}"
        )
    missing = sorted(set(declared) - set(modes))
    if missing:
        raise RuntimeError(
            "❌ --execution-access-mode must state the mode for every declared "
            f"provider (no default): missing {missing}"
        )

    from engine.execution.adapters import get_adapter

    options = getattr(args, "provider_options", None)
    for provider, mode in modes.items():
        adapter = get_adapter(provider)
        supported = adapter.supported_execution_access_modes()
        if mode not in supported:
            raise RuntimeError(
                f"❌ provider {provider!r} does not support execution access mode "
                f"{mode!r}; it advertises {sorted(supported)} (see `ctl.py providers`)"
            )
        # An option may only be meaningful in one mode (a substitute credential
        # means nothing unless the run stops resolving identities). Passing it in
        # another mode is a contradiction, not a silent no-op.
        implied = adapter.execution_access_mode_from_options(
            execution_providers.provider_options_for(options, provider)
        )
        if implied is not None and implied != mode:
            raise RuntimeError(
                f"❌ the --provider-options given for {provider!r} are only valid in "
                f"execution access mode {implied!r}, but it runs {mode!r}"
            )
    return modes


def normalize_force_skip_execution_identity_preflight_check(
    args: argparse.Namespace,
) -> list[str]:
    """Resolve the providers whose live identity check is skipped.

    A subset of --providers. Skipping is meaningless for a provider that has no
    live check to begin with, and for one already running without a resolved
    identity — both are named rather than silently ignored.
    """

    requested = list(getattr(args, "force_skip_execution_identity_preflight_check", ()) or [])
    if not requested:
        return []
    declared = list(getattr(args, "providers", ()) or ())
    stray = sorted(set(requested) - set(declared))
    if stray:
        raise RuntimeError(
            "❌ --force-skip-execution-identity-preflight-check names providers not "
            f"declared in --providers {declared}: {stray}"
        )

    from engine.execution.adapters import get_adapter

    modes = getattr(args, "execution_access_modes", None) or {}
    for provider in requested:
        adapter = get_adapter(provider)
        if not adapter.supports_identity_preflight():
            raise RuntimeError(
                f"❌ provider {provider!r} declares no live execution-identity check, "
                "so there is nothing to skip (see `ctl.py providers`)"
            )
        if not adapter.resolves_execution_identity(
            execution_providers.execution_access_mode_for(modes, provider)
        ):
            raise RuntimeError(
                f"❌ provider {provider!r} runs mode "
                f"{execution_providers.execution_access_mode_for(modes, provider)!r} without resolving an "
                "execution identity, so its preflight check does not run and cannot "
                "be skipped"
            )
    return sorted(set(requested))


def parse_overlays_arg(value: str) -> list[str]:
    """

    parse comma-separated plt overlay names."""

    if value is None:
        return []

    raw = [v.strip() for v in value.split(",") if v.strip()]
    if not raw:
        return []
    if len(raw) == 1 and raw[0].lower() in ("none", "null", "-"):
        return []

    for item in raw:
        if "/" in item or "\\" in item:
            raise argparse.ArgumentTypeError(
                f"Overlay must be a metadata name, not a path: {item}"
            )
        if item in (".", ".."):
            raise argparse.ArgumentTypeError(f"Overlay name is invalid: {item}")

    duplicates = [item for item, count in collections.Counter(raw).items() if count > 1]
    if duplicates:
        raise argparse.ArgumentTypeError(
            f"Overlay names must be unique; duplicates: {', '.join(sorted(duplicates))}"
        )

    return raw


class ExecutionAccessModesAction(argparse.Action):
    """Merge repeated/comma-separated --execution-access-mode into one map.

    Parses shape only (`provider=mode`); the MODE NAMES are the adapters', so
    they are validated later against each adapter's advertised set.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        merged = dict(getattr(namespace, self.dest, None) or {})
        for pair in kernel_scalars.parse_comma_list(values):
            if "=" not in pair:
                raise argparse.ArgumentError(
                    self, f"expected PROVIDER=MODE, got {pair!r}"
                )
            provider, mode = (part.strip() for part in pair.split("=", 1))
            if not provider or not mode:
                raise argparse.ArgumentError(
                    self, f"expected PROVIDER=MODE, got {pair!r}"
                )
            if provider in merged and merged[provider] != mode:
                raise argparse.ArgumentError(
                    self, f"conflicting execution access modes for provider {provider!r}"
                )
            merged[provider] = mode
        setattr(namespace, self.dest, merged)


def _add_workflow_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--workflow",
        required=True,
        help="declared ctl workflow name",
    )


def _add_target_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--target",
        required=True,
        help="declared target name",
    )


def _add_fan_out_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--fan-out",
        required=True,
        dest="fan_out",
        help="declared fan_out key to expand and run",
    )


def _add_maintenance_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--maintenance-action",
        required=True,
        choices=list(run_actions.MAINTENANCE_ACTIONS),
        help="maintenance action",
    )
    parser.add_argument(
        "--older-than",
        default=None,
        metavar="ISO-DATE|any",
        help="forget only: age filter; `any` ignores age. Always required — a "
        "forget states both of its dimensions",
    )
    parser.add_argument(
        "--address",
        dest="forget_address",
        default=None,
        type=kernel_scalars.parse_comma_list,
        metavar="ADDRESS[,ADDRESS...]|all",
        help="forget only: what to forget; depth decides scope, so a template "
        "address takes every instance under it. `all` is the explicit wide value",
    )
    parser.add_argument(
        "--apply",
        dest="apply_forget",
        action="store_true",
        help="forget only: actually remove. Without it, a forget is a dry run "
        "that lists what would go",
    )
    parser.add_argument(
        "--accept-orphaned-resources",
        action="store_true",
        help="forget only: forget records whose state is `provisioned` or "
        "`partial`. The infrastructure outlives the record naming it, so what "
        "is accepted is orphaned resources — that is what the flag says",
    )
    parser.add_argument(
        "--accept-forget-everything",
        action="store_true",
        help="forget only: required when BOTH filters are wide, so forgetting "
        "everything cannot be reached by widening one flag at a time",
    )
    parser.add_argument(
        "--scope",
        dest="unlock_scope",
        default=None,
        choices=("local", "remote", "both"),
        help="unlock-ctl-state only: which lock to release. `local` is this "
        "machine's working tree, `remote` is the namespace, `both` (the default) "
        "is the remote lock and THIS machine's local one",
    )
    parser.add_argument(
        "--lock-id",
        help="the run ID holding the lock, as named in the error",
    )
    parser.add_argument(
        "--target",
        help="declared target to operate on",
    )
    parser.add_argument(
        "--prune-run-id",
        action="append",
        default=[],
        help="run UUIDv7 to include in a history-prune selection; repeatable",
    )
    parser.add_argument(
        "--prune-before",
        help="prune run history older than this ISO-8601 timestamp",
    )
    parser.add_argument(
        "--prune-kind",
        choices=["target", "workflow"],
        help="limit history-prune to one state-owner kind",
    )
    parser.add_argument(
        "--cascade",
        action="store_true",
        help="history-prune and forget: also take retained workflow runs that reference the selected records, which would otherwise be left naming a member that no longer exists",
    )
    parser.add_argument(
        "--apply-history-prune",
        action="store_true",
        help="apply the reported history deletion set",
    )
    parser.add_argument(
        "--agree-history-prune",
        action="store_true",
        help="explicitly acknowledge deletion of the reported unversioned object keys",
    )


def _add_procedure_args(parser: argparse._ActionsContainer) -> None:
    parser.add_argument(
        "--source",
        required=True,
        help="source repo for a synthetic target",
    )
    parser.add_argument(
        "--ref",
        required=True,
        help="ref context (a key in refs.scoped, e.g. env/${env_type} or org) for a synthetic target",
    )
    parser.add_argument(
        "--domain",
        required=True,
        dest="domain",
        help="plt domain a synthetic target reads (it takes the whole domain)",
    )
    parser.add_argument(
        "--procedure",
        required=True,
        dest="procedure",
        help="repo-local procedure to run",
    )
    # A synthetic target declares the same execution axes a declared
    # target does — provider, account and the role to assume. All three or none.
    parser.add_argument(
        "--execution-provider",
        dest="execution_provider",
        default=None,
        help="synthetic target: provider whose adapter runs it",
    )
    parser.add_argument(
        "--execution-account",
        dest="execution_account",
        default=None,
        help="synthetic target: account to run in",
    )
    parser.add_argument(
        "--execution-role",
        dest="execution_role",
        default=None,
        help="synthetic target: provider role key to assume",
    )
    parser.add_argument(
        "--affected-target-key",
        dest="affected_target_keys",
        action="append",
        default=[],
        help="affected declared target key; repeatable and required for mutating synthetic runs",
    )


def add_common_args(parser: argparse.ArgumentParser, *, run_type: str) -> None:
    """Add shared and runner-specific arguments for local runner entrypoints.

    Arguments are placed into titled argparse groups that drive --help
    presentation: ctl -> execution -> action & selector -> defer / force
    overrides -> cfg variation -> run modes -> misc, followed by suppressed
    internal args. Group creation order is the --help section order (it no
    longer depends on add order). Keep add_bootstrap_common_args (the
    pre-fetch --help duplicate) with the SAME groups in the SAME order."""
    ctl_group = parser.add_argument_group(
        "ctl",
        "control-plane authority & context: cfg/policy source, governing "
        "profile, state root — the profile declares what this run is allowed to do",
    )
    execution_group = parser.add_argument_group(
        "execution",
        "concrete execution choices (access, runtime, params) — the values this "
        "run actually uses, each honored only within what the ctl profile permits",
    )
    selector_group = parser.add_argument_group(
        "run",
        "the actual run: lifecycle action, the runner selector, and (for "
        "workflow/fan-out) whether to reuse committed children",
    )
    override_group = parser.add_argument_group(
        "defer / skip overrides",
        "authorized escalations; each also requires ctl-profile allowance",
    )
    variation_group = parser.add_argument_group(
        "cfg variation", "optional ctl variants and plt overlays"
    )
    mode_group = parser.add_argument_group(
        "checks & previews",
        "inspect or preview only; exit without executing targets",
    )
    parser.add_argument_group("misc")
    # 1) ctl
    ctl_group.add_argument(
        "--ctl-cfg",
        required=True,
        help="git URL@ref or local path to the ctl cfg",
    )
    ctl_group.add_argument(
        "--ctl-profile",
        required=True,
        help="Ctl profile name (named policy bundle from the ctl_profiles catalog)",
    )
    ctl_group.add_argument(
        "--ctl-state-local-root",
        required=True,
        help="Local ctl-state root (run results tree); runner appends <action>/<run_type>/<name>",
    )
    # 2) execution access mode, then runtime.
    # Execution access is a PER-PROVIDER decision: a run that spans
    # providers may need normal access to one and escalated access to another,
    # and the mode NAMES belong to the adapters — the engine owns no mode
    # vocabulary of its own. Hence a provider=mode map, required and complete:
    # The operator states intent for every provider the run declares.
    execution_group.add_argument(
        "--execution-access-mode",
        required=True,
        dest="execution_access_modes",
        action=ExecutionAccessModesAction,
        metavar="PROVIDER=MODE[,PROVIDER=MODE...]",
        help="Execution access mode per provider, comma-separated and/or repeatable "
        "(required, no default). Must name every provider given to --providers, and "
        "each mode must be one the adapter advertises — run `ctl.py providers` to see "
        "what each supports, and what provider options a mode needs "
        "(passed with --provider-options).",
    )
    # Execution runtime: WHERE CTL produces each target_run's clean box.
    execution_group.add_argument(
        "--execution-runtime-mode",
        choices=run_policy.EXECUTION_RUNTIME_MODES,
        required=True,
        help="Execution runtime (required, no default): 'local' builds a fresh Docker "
        "box per target_run on this machine; 'ci' runs each target_run on the CI "
        "runner (no Docker-in-Docker). Must be allowed by the ctl profile "
        "(allowed_execution_runtime_modes) and supported by every active target_run "
        "(step.yaml runtime.supported_execution_runtime_modes).",
    )
    # 3) execution params
    execution_group.add_argument(
        "--credential-refresh-mode",
        dest="credential_refresh_modes",
        action=ExecutionParamsAction,
        required=True,
        default=[],
        metavar="PROVIDER=MODE[,PROVIDER=MODE...]",
        help="when credentials are acquired, PER PROVIDER: `per_target` once "
        "while the target repo is prepared, or `per_step` freshly before every "
        "step. Required, no default — when a run acquires credentials is a "
        "choice, not something to inherit. Per provider because only some have "
        "expiring sessions at all, and a run may span several. Each mode must be "
        "one the provider's policy block allows",
    )
    execution_group.add_argument(
        "--execution-params",
        dest="execution_param",
        action=ExecutionParamsAction,
        default=[],
        metavar="KEY=VALUE[,KEY=VALUE...]",
        help="Execution params in key=value form; comma-separated and/or repeatable; "
        "lands in execution_context.params.*",
    )
    execution_group.add_argument(
        "--providers",
        dest="providers",
        required=True,
        type=kernel_scalars.parse_comma_list,
        metavar="NAME[,NAME...]",
        help="Providers participating in this run; every selected target's provider "
        "must be one of these. Lands in execution_context.ctl.providers",
    )
    execution_group.add_argument(
        "--provider-options",
        dest="provider_options",
        action=execution_providers.ProviderOptionsAction,
        default={},
        metavar="PROVIDER.KEY=VALUE[,...]",
        help="Provider-namespaced options, comma-separated and/or repeatable. The "
        "engine only routes them; each provider owns its option vocabulary",
    )
    # 4) what to do — spelled for the run type
    if run_type in ("workflow", "fan_out"):
        # A workflow is invoked with an OPERATION: it says what was asked for,
        # while each member target carries the ACTION it performs. The two differ
        # only here, because a workflow is the one level that may mix directions.
        # A fan-out shares the spelling because it expands workflows and passes the
        # operation through unchanged — it varies the address, never the verb.
        selector_group.add_argument(
            "--operation",
            dest="action",
            required=True,
            choices=list(run_actions.KNOWN_ACTIONS),
            help="What to do to this thing. Each member target performs its own\n"
            "declared action, or inherits this one when it declares none",
        )
    else:
        selector_group.add_argument(
            "--action",
            required=True,
            choices=list(run_actions.KNOWN_ACTIONS),
            help="Lifecycle action. `maintenance` operates on a target WITHOUT\n"
            "provisioning, planning or destroying it — releasing a tool's state\n"
            "lock is the case it exists for",
        )
    # 5) run-type selector (--workflow / --fan-out / --target / ... and its
    #    run-specific siblings)
    if run_type == "workflow":
        _add_workflow_args(selector_group)
    elif run_type == "target":
        _add_target_args(selector_group)
    elif run_type == "maintenance":
        _add_maintenance_args(selector_group)
    elif run_type == "procedure":
        _add_procedure_args(selector_group)
    elif run_type == "fan_out":
        _add_fan_out_args(selector_group)
    else:
        raise RuntimeError(f"❌ unknown runner run_type {run_type!r}")
    # --skip-up-to-date parametrizes the actual run (reuse committed children vs
    # re-run), so it lives in the run group — not with the non-executing checks.
    if run_type in {"workflow", "fan_out"}:
        selector_group.add_argument(
            "--skip-up-to-date",
            default=None,
            type=kernel_scalars.str2bool,
            metavar="{true,false}",
            help="Explicit true/false (no default; required for a normal run, "
            "omit only with --status or --execution-identity-preflight-check-only): "
            "when true, reuse a workflow child's committed result (skip re-running "
            "it) only when its committed target instance is current, commit-pinned, "
            "clean, and matches the current source/cfg commits and effective target definition/cfg view; when false, always re-run",
        )
    # 6) --agreed-* / --force-* overrides
    override_group.add_argument(
        "--agreed-defer-ctl-state-backend-sync",
        action="store_true",
        dest="agreed_defer_ctl_state_backend_sync",
        help="Agree to defer ctl-state publication while the selected namespace backend is "
        "absent during bootstrap; requires profile allow_agreed_defer_ctl_state_backend_sync "
        "and every active target to declare allow_agreed_defer_ctl_state_backend_sync: true",
    )
    override_group.add_argument(
        "--force-skip-ctl-state-backend-sync",
        action="store_true",
        dest="force_skip_ctl_state_backend_sync",
        help="Blanket override: skip ctl-state backend sync for EVERY active target, "
        "ignoring target keys; requires profile allow_force_skip_ctl_state_backend_sync",
    )
    override_group.add_argument(
        "--skip-children-precheck",
        action="store_true",
        dest="skip_children_precheck",
        help="Do not pre-check this run's CHILDREN before starting them: a fan-out "
        "skips pre-checking its workflows, a workflow skips pre-checking its targets. "
        "Each child still renders and validates its own cfg when it runs, so this "
        "changes WHEN a bad child is found, not whether it is. A target run has no "
        "children, so it is a no-op there. Requires profile "
        "allow_skip_children_precheck",
    )
    override_group.add_argument(
        "--force-skip-guardrails",
        action="store_true",
        dest="force_skip_guardrails",
        help="Skip ctl + plt guardrail verification for this run; requires profile "
        "allow_force_skip_guardrails",
    )
    override_group.add_argument(
        "--force-skip-full-cfg-validation-gate",
        action="store_true",
        dest="force_skip_full_cfg_validation_gate",
        help="Keep the full cfg-validation report but do not let unrelated failed "
        "findings block this run; complete cfg structure and every selected-run "
        "dependency remain mandatory; requires profile "
        "allow_force_skip_full_cfg_validation_gate",
    )
    if run_type in {"workflow", "target", "fan_out"}:
        override_group.add_argument(
            "--force-skip-execution-identity-preflight-check",
            dest="force_skip_execution_identity_preflight_check",
            default=[],
            type=kernel_scalars.parse_comma_list,
            metavar="PROVIDER[,PROVIDER...]",
            help="Providers whose live execution-identity check to skip (a subset of "
            "--providers; the identity is still RESOLVED for all of them). Per provider "
            "because only some adapters have a live check at all, and a mixed run may "
            "need to skip one provider's probe while keeping another's. You accept the "
            "risk that a skipped provider's target fails MID-RUN on an execution "
            "identity the preflight would have caught up front; requires ctl-profile "
            "authorization.",
        )
    # 7) cfg variation
    if run_type in {"workflow", "fan_out"}:
        variation_group.add_argument(
            "--ctl-variants",
            required=False,
            default=[],
            dest="ctl_variants",
            type=catalog_workflow.parse_ctl_variants_arg,
            help="Optional comma-separated ctl variant paths under variants/",
        )
    variation_group.add_argument(
        "--plt-overlays",
        required=False,
        default=[],
        dest="plt_overlays",
        type=parse_overlays_arg,
        help="Optional comma-separated plt overlay names",
    )
    # 8) run modes
    if run_type == "fan_out":
        mode_group.add_argument(
            "--dry-run",
            action="store_true",
            help="print expanded child runner commands and exit",
        )
    # Status is no longer a mode on the run runners — it is the
    # standalone read-only status.py (its own slim parser). Removed here.
    if run_type in {"workflow", "target", "fan_out"}:
        mode_group.add_argument(
            "--execution-identity-preflight-check-only",
            action="store_true",
            help="Resolve and live-check every selected execution identity, write the "
            "preflight artifacts, and exit without state, guardrails, or target_runs",
        )
    # internal, engine-set (hidden from --help) — last
    parser.add_argument(
        "--parent-graph-provisions-ctl-state-backend",
        action="store_true",
        dest="parent_graph_provisions_ctl_state_backend",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--parent-ctl-state-backend-absence-confirmed",
        action="store_true",
        dest="parent_ctl_state_backend_absence_confirmed",
        help=argparse.SUPPRESS,
    )
    # Set by the fan-out runner on its child runs: the fan-out run id recorded
    # in child run metadata as the batch audit record (— the fan-out
    # itself is stateless and owns no run history).
    parser.add_argument(
        "--parent-fan-out-run-id",
        dest="parent_fan_out_run_id",
        default=None,
        help=argparse.SUPPRESS,
    )
    # A workflow-spawned child target run executes under the lock its
    # parent already holds. Internal wiring, not an operator flag.
    parser.add_argument(
        "--parent-workflow-run-id",
        dest="parent_workflow_run_id",
        default=None,
        help=argparse.SUPPRESS,
    )
    # The parent's INSTANCE address, not just its run id. A spawned
    # child cannot derive it — it knows neither the workflow key nor the axes the
    # workflow partitions by — so the parent hands it over like every other fact
    # the child cannot compute. The in-process path already recorded it.
    parser.add_argument(
        "--parent-workflow-instance-address",
        dest="parent_workflow_instance_address",
        default=None,
        help=argparse.SUPPRESS,
    )


def add_status_args(parser: argparse.ArgumentParser) -> None:
    """The slim, read-only status parser. Only what a read needs —
    namespace + breadth + scope (+ dir for local, dev substitute for remote).
    No runtime-mode, no access-mode enum, no reuse/force/defer. --ctl-profile
    stays: a read still consults its ref_policy. Titled groups
    drive --help, mirroring the run runners; keep add_bootstrap_status_args (the
    pre-fetch --help duplicate) in sync with the SAME groups in the SAME order."""
    ctl_group = parser.add_argument_group(
        "ctl",
        "cfg/policy source and governing profile; a read consults only the "
        "profile's ref_policy (tooling/cfg pinning + dirty-vs-committed gate)",
    )
    query_group = parser.add_argument_group(
        "query",
        "what to read: the namespace/instance selectors, the breadth (whole "
        "namespace or one owner), and the optional lifecycle view",
    )
    scope_group = parser.add_argument_group(
        "scope",
        "where to read from: the local tree (offline, no credentials) or the "
        "authoritative bucket (hydrated into an auto temp). NEITHER mutates "
        "local ctl-state",
    )
    # 1) ctl
    ctl_group.add_argument(
        "--ctl-cfg", required=True, help="git URL@ref or local path to the ctl cfg"
    )
    ctl_group.add_argument(
        "--ctl-profile",
        required=True,
        help="Ctl profile name; a read consults its ref_policy only",
    )
    # 2) query — selectors, breadth, lifecycle view
    query_group.add_argument(
        "--execution-params",
        dest="execution_param",
        action=ExecutionParamsAction,
        default=[],
        metavar="KEY=VALUE[,KEY=VALUE...]",
        help="Execution params key=value; comma-separated and/or repeatable. Namespace selectors "
        "(provider, landing_zone) always; instance selectors (account, env_type, "
        "region) only for a targeted query",
    )
    breadth = query_group.add_mutually_exclusive_group(required=True)
    breadth.add_argument(
        "--all",
        action="store_true",
        help="whole-namespace: every target and workflow instance",
    )
    breadth.add_argument("--target", metavar="NAME", help="one declared target instance")
    breadth.add_argument(
        "--workflow", metavar="NAME", help="one declared workflow instance"
    )
    breadth.add_argument(
        "--fan-out",
        dest="fan_out",
        metavar="NAME",
        help="status of the targets/workflows a fan-out expands to",
    )
    query_group.add_argument(
        "--action",
        default=None,
        choices=list(run_actions.KNOWN_ACTIONS),
        help="which status GROUP a targeted target query reports (an action names "
        "its group: provision/destroy -> deployment); ignored by --all",
    )
    # Filters, not selectors: they narrow what --all PRINTS. A breadth argument
    # names ONE instance, so a filter needs its own word rather than a valueless
    # --target/--workflow.
    query_group.add_argument(
        "--kind",
        dest="kinds",
        default=None,
        type=kernel_scalars.parse_comma_list,
        metavar="KIND[,KIND...]",
        help="--all only: show these row kinds (target, workflow); default both. "
        "Same vocabulary as the row prefix and as --prune-kind",
    )
    query_group.add_argument(
        "--structure",
        default=None,
        choices=("nested", "flat"),
        help="--all only: `nested` keeps the kind/template/instance tree; `flat` "
        "emits one row per group carrying its own address, the only shape a "
        "strictly chronological order can take. Required with --all — the shape "
        "is a choice, not a default to inherit",
    )
    query_group.add_argument(
        "--sort",
        default="address",
        metavar="FIELD[:asc|desc]",
        help="--all only: order by `time` or `address`; direction defaults to asc. "
        "Nested sorts on two levels — templates by their newest instance, "
        "instances by their newest group",
    )
    query_group.add_argument(
        "--group",
        dest="groups",
        default=None,
        type=kernel_scalars.parse_comma_list,
        metavar="GROUP[,GROUP...]",
        help="--all only: show these status groups (plan, readonly, deployment); "
        "default all. A row whose every group is filtered out is dropped",
    )
    # 3) scope — where to read, and the local dir / dev substitute
    scope_group.add_argument(
        "--hydrate-to",
        dest="hydrate_to",
        default=None,
        metavar="DIR",
        help="--scope remote only: keep the hydrated namespace here instead of "
        "discarding it. The result is directly usable as --ctl-state-local-root "
        "— a full copy of the namespace IS a valid local root",
    )
    scope_group.add_argument(
        "--scope",
        required=True,
        choices=("local", "remote"),
        help="'local' reads the dir offline (no remote ctl-state backend, no "
        "credentials) — the only way to see force-skipped runs; 'remote' is the "
        "authoritative ctl-state backend view (hydrated into an auto temp, discarded)",
    )
    scope_group.add_argument(
        "--ctl-state-local-root",
        default=None,
        metavar="DIR",
        help="required for --scope local (the ctl-state tree to read); not "
        "valid for --scope remote (remote uses an auto temp)",
    )
    scope_group.add_argument(
        "--provider-options",
        dest="provider_options",
        action=execution_providers.ProviderOptionsAction,
        default={},
        metavar="PROVIDER.KEY=VALUE[,...]",
        help="Provider-namespaced options for --scope remote, comma-separated "
        "and/or repeatable — including a substitute credential when no ctl-state "
        "read chain exists yet, which makes the read skip identity resolution. "
        "Run `ctl.py providers` for each provider's option keys.",
    )
    scope_group.add_argument(
        "--write-cache",
        action="store_true",
        help="also persist the computed map as an advisory, self-dated "
        "status_cache.yaml at the namespace root under --ctl-state-local-root, "
        "and a dated copy under _local/status_history/ so past answers stay "
        "readable "
        "(requires --all — the cache is a whole-namespace map). LOCAL ONLY: "
        "status never writes to ctl-state itself. Default: print only, write "
        "nothing. Never touches committed pointers.",
    )


def finalize_status_args(args: argparse.Namespace) -> None:
    """Normalize + validate the slim status args, and synthesize the internal
    values the shared resolvers still expect (a read has ONE operation, so the
    access mode collapses to standard-chain, or the dev substitute)."""
    args.execution_params = run_selectors.selectors_to_map(
        args.execution_param, label="execution param"
    )
    args.status = args.scope
    write_cache = getattr(args, "write_cache", False)
    if write_cache and not args.all:
        raise RuntimeError(
            "❌ --write-cache requires --all: the status cache is a "
            "whole-namespace map"
        )
    if getattr(args, "hydrate_to", None) and args.scope != "remote":
        raise RuntimeError("❌ --hydrate-to keeps a REMOTE hydration; use --scope remote")
    if args.all and not getattr(args, "structure", None):
        raise RuntimeError("❌ --all requires --structure nested|flat")
    if getattr(args, "structure", None) and not args.all:
        raise RuntimeError("❌ --structure shapes --all; a targeted query names one instance")
    if args.all:
        state_status.parse_sort(args.sort)
    for flag, dest, allowed in (
        ("--kind", "kinds", run_actions.RESULT_KINDS),
        ("--group", "groups", tuple(run_actions.STATUS_GROUPS)),
    ):
        selected = getattr(args, dest, None)
        if selected is None:
            continue
        if not args.all:
            raise RuntimeError(f"❌ {flag} filters --all; a targeted query names one instance")
        unknown = sorted(set(selected) - set(allowed))
        if unknown:
            raise RuntimeError(
                f"❌ {flag} got unknown {', '.join(unknown)}; expected one of {', '.join(allowed)}"
            )
    if args.scope == "local":
        if not args.ctl_state_local_root:
            raise RuntimeError("❌ --scope local requires --ctl-state-local-root")
        if args.provider_options:
            raise RuntimeError(
                "❌ --provider-options is not valid with --scope local "
                "(local reads the dir — no remote ctl-state backend, no credentials)"
            )
        args.ctl_state_local_root = normalize_ctl_state_local_root(
            args.ctl_state_local_root
        )
    else:
        # Remote reads pointers from the bucket into a throwaway temp, so it
        # needs no local root — UNLESS --write-cache, where the local root is the
        # cache write target (the derived map lands there; local pointers are
        # never read or clobbered).
        if write_cache:
            if not args.ctl_state_local_root:
                raise RuntimeError(
                    "❌ --write-cache with --scope remote requires "
                    "--ctl-state-local-root (the cache write target)"
                )
            args.ctl_state_local_root = normalize_ctl_state_local_root(
                args.ctl_state_local_root
            )
        elif args.ctl_state_local_root:
            raise RuntimeError(
                "❌ --ctl-state-local-root is not valid with --scope remote "
                "(remote hydrates into an auto temp; only --write-cache uses it)"
            )
        else:
            args.ctl_state_local_root = None
    # A read has ONE operation against ONE provider — the namespace selector
    # names it — so the run-level per-provider inputs collapse to that provider.
    # The mode is the provider's normal path, unless its options imply another
    # (a substitute credential IS the request to skip identity resolution). The
    # engine picks no mode name here; the adapter does.
    read_provider = args.execution_params.get("provider")
    if args.scope == "remote" and not read_provider:
        raise RuntimeError(
            "❌ --scope remote requires --execution-params provider=<name>: the read "
            "acquires a credential, and only the provider knows how"
        )
    # Scope local reads the dir offline: no credential is acquired, so the
    # provider (if given as a query selector) implies no options and no mode.
    args.providers = [read_provider] if read_provider and args.scope == "remote" else []
    args.execution_access_modes = {}
    if args.providers:
        execution_providers.validate_provider_options(args.provider_options, args.providers)
        adapter = execution_providers.get_provider_adapter(read_provider)
        adapter_options = execution_providers.provider_options_for(args.provider_options, read_provider)
        args.execution_access_modes = {
            read_provider: (
                adapter.execution_access_mode_from_options(adapter_options)
                or adapter.normal_execution_access_mode()
            )
        }
    # Inert for a read (no box is built) but required by the shared context
    # builders / selection resolvers.
    args.execution_runtime_mode = "local"
    args.ctl_variants = []
    if not args.all and args.action is None:
        raise RuntimeError(
            "❌ a targeted status (--target/--workflow/--fan-out) requires "
            "--action provision|destroy"
        )
