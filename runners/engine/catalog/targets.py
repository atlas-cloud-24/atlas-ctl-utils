"""The target catalog: what is declared, and what may run it.

A target's definition and its execution identity resolve together because a
target that cannot say who runs it is not runnable, and finding that out at
execution time is finding it out too late."""

import collections
from pathlib import Path

from engine.cfg import references as cfg_references
from engine.cfg import resources as cfg_resources
from engine.execution import adapters as execution_adapters
from engine.execution import providers as execution_providers
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.kernel import paths as kernel_paths
from engine.run import actions as run_actions
from engine.run import selectors as run_selectors

TARGET_EXECUTION_IDENTITY_FIELDS = frozenset(
    {
        "account",
        "roles",
        "allowed_agreed_direct_credential_sources",
        "allowed_accounts",
    }
)


def _resolve_instance_params_members(
    entry: dict, execution_context: dict[str, object] | None, *, target_name: str
) -> list[str] | None:
    return run_selectors.resolve_list_members(
        entry,
        execution_context,
        value_field="params",
        label=f"target {target_name!r} target_instance_params",
    )


def validate_distinct_target_signatures(definitions: dict) -> None:
    """Two targets with the same INPUT SIGNATURE are one target twice.

    They would run identical procedures against identical resources under two
    separate committed pointers, neither aware of the other — the same hazard the
    dependency registries exist to catch, but between declarations rather than
    between resources.

    The signature must be COMPLETE. Comparing only the procedure and the instance
    params gives false positives: two targets can share a source, a procedure and
    their params while consuming different cfg, and then legitimately produce
    different resources.

    A static cfg check. It never observes resources and never decides which
    declaration is authoritative — it answers only whether a pair may coexist.
    """

    def signature(definition: dict) -> tuple:
        def frozen(value):
            if isinstance(value, dict):
                return tuple(sorted((k, frozen(v)) for k, v in value.items()))
            if isinstance(value, list):
                return tuple(frozen(v) for v in value)
            return value

        return (
            definition.get("source_key"),
            definition.get("ref_key"),
            definition.get("procedure_key"),
            frozen(definition.get("domains")),
            frozen(definition.get("cfg_keys")),
            frozen(definition.get("cfg_key_sets")),
            frozen(definition.get("input_params")),
            frozen(definition.get("input_param_sets")),
            frozen(definition.get("target_instance_params")),
            # The overlays a target requires are part of its INPUT: two targets
            # over one procedure that render different cfg are alternatives, not
            # a duplicate. That is exactly what a mode group declares.
            frozen(definition.get("required_plt_overlay_keys")),
        )

    seen: dict[tuple, str] = {}
    for key, definition in sorted((definitions or {}).items()):
        if not isinstance(definition, dict):
            continue
        sig = signature(definition)
        if None in sig[:3]:
            continue
        first = seen.get(sig)
        if first is not None:
            raise RuntimeError(
                f"❌ targets {first!r} and {key!r} declare the same input signature "
                "(source, ref, procedure, domains, cfg keys, input params, instance "
                "params, required overlays). They would run identical work against "
                "identical resources under two committed pointers; declare one "
                "target, or make them differ"
            )
        seen[sig] = key


def validate_target_execution_identities(entries: object, *, label: str) -> dict:
    """Validate a target's `execution_identities:` — identities KEYED BY PROVIDER.

    A run may span providers, and so may a target: a root that touches two clouds
    needs credentials for both before a single plan. The provider is the key, so
    the engine picks N adapters instead of one and everything below each key stays
    opaque.
    """

    if not isinstance(entries, dict) or not entries:
        raise RuntimeError(
            f"❌ {label} execution_identities must be a non-empty mapping keyed by provider"
        )
    resolved: dict[str, dict] = {}
    for provider, execution in entries.items():
        if not isinstance(provider, str) or not provider.strip():
            raise RuntimeError(f"❌ {label} execution_identities keys must be provider names")
        resolved[provider] = validate_target_execution_identity(
            execution, label=f"{label} [{provider}]"
        )
    return resolved


# What a target's fields REFERENCE. Declared here because a target's vocabulary
# is this module's to own: nothing more general knows that `source_key` names a
# `target_sources` entry while `input_param_sets` names a `param_sets` one.
# `execution_identities` is absent by design — the engine reads its shape and
# treats every value inside as opaque, so its references are the ADAPTER's.
TARGET_REFERENCE_FIELDS = {
    "source_key": "target_sources",
    "domains": "domains",
    "input_param_sets": "param_sets",
    "providers": "ctl_providers",
}


def load_target_catalog(ctl_cfg_root: Path) -> dict:
    """Every declared target, with its cross-references resolved to bare keys.

    Resolution happens at the LOAD point rather than at each read, so a qualified
    value never reaches a reader that did not ask for one.
    """

    targets = cfg_resources.collect_resource(ctl_cfg_root, "targets", entry_depth=1)
    resolved: dict = {}
    for name, entry in targets.items():
        if not isinstance(entry, dict):
            resolved[name] = entry
            continue
        body = cfg_references.resolve_fields(
            entry, TARGET_REFERENCE_FIELDS, label=f"target {name!r}"
        )
        identities = body.get("execution_identities")
        if isinstance(identities, dict):
            # Everything inside is the provider's vocabulary, so the ADAPTER
            # resolves it — engine core never names those collections.
            body["execution_identities"] = {
                provider: (
                    execution_adapters.get_adapter(
                        provider, ctl_cfg_root
                    ).resolve_execution_identity_references(
                        identity, provider=provider, label=f"target {name!r} [{provider}]")
                    if isinstance(identity, dict) else identity
                )
                for provider, identity in identities.items()
            }
        if isinstance(body.get("cfg_key_sets"), dict):
            # The mapping KEY is a domain and stays an index; only the sets it
            # points at are references.
            body["cfg_key_sets"] = {
                domain: cfg_references.resolve_each(
                    sets, "cfg_key_sets", label=f"target {name!r} cfg_key_sets.{domain}")
                for domain, sets in body["cfg_key_sets"].items()
            }
        resolved[name] = body
    return resolved


def validate_target_execution_identity(execution: object, *, label: str) -> dict:
    """Validate ONE entry of a target's `execution_identities:` block.

    The provider is the KEY, so one target may declare several. The engine reads
    the generic shape only; every value inside (`account`, the role keys,
    credential source keys) is opaque here and interpreted by that adapter.
    """

    if not isinstance(execution, dict) or not execution:
        raise RuntimeError(f"❌ {label} execution must be a non-empty mapping")

    unknown = sorted(set(execution) - TARGET_EXECUTION_IDENTITY_FIELDS)
    if unknown:
        raise RuntimeError(
            f"❌ {label} execution has unknown fields {unknown}; "
            f"allowed: {sorted(TARGET_EXECUTION_IDENTITY_FIELDS)}"
        )

    value = execution.get("account")
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"❌ {label} execution.account must be a non-empty string")

    roles = execution.get("roles")
    if not isinstance(roles, dict) or not roles:
        raise RuntimeError(
            f"❌ {label} execution.roles must be a non-empty mapping; its KEYS are the "
            "provider's vocabulary, not the engine's"
        )
    for role_class, role_key in roles.items():
        if not isinstance(role_key, str) or not role_key.strip():
            raise RuntimeError(
                f"❌ {label} execution.roles.{role_class} must be a non-empty string"
            )

    for field in ("allowed_agreed_direct_credential_sources", "allowed_accounts"):
        if field not in execution:
            continue
        values = execution.get(field)
        if not isinstance(values, list) or not values or not all(
            isinstance(item, str) and item.strip() for item in values
        ):
            raise RuntimeError(
                f"❌ {label} execution.{field} must be a non-empty list of non-empty strings"
            )

    return execution


def target_consent_opt_in_fields(ctl_cfg_root=None) -> set[str]:
    """Every per-target opt-in field any declared adapter asks for."""

    fields: set[str] = set()
    for provider in execution_adapters.registered_providers(ctl_cfg_root):
        adapter = execution_adapters.get_adapter(provider, ctl_cfg_root)
        for mode in adapter.supported_execution_access_modes():
            consent = adapter.target_consent(mode)
            if consent:
                fields.add(consent["opt_in_field"])
    return fields


def build_active_target_runs(
    workflow_cfg: dict,
    action_cfg: dict,
    repo_key: str = "repo_url",
    require_branch_or_commit: bool = True,
    refs: dict | None = None,
    execution_context: dict[str, object] | None = None,
    require_commit_refs: bool = False,
) -> dict:
    action_target_sources = action_cfg.get("target_sources", {})
    if not isinstance(action_target_sources, dict):
        raise RuntimeError("'target_sources' in action must be a mapping: source -> meta")

    action_targets = action_cfg.get("targets", {})
    if not isinstance(action_targets, dict):
        raise RuntimeError("'targets' in action must be a mapping: target -> meta")

    refs = refs or {}
    scoped_refs = refs.get("scoped") or {}
    ref_context_values = execution_context or {}
    active = {}

    def normalize_cfg_root(raw_value, *, target_key: str) -> str:
        value = raw_value if raw_value is not None else "/"
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must be a non-empty string")
        value = value.strip()
        if "\\" in value:
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must use forward slashes: {value}")
        if not value.startswith("/"):
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must start with /: {value}")
        parts = [part for part in value.split("/") if part]
        if any(part in (".", "..") for part in parts):
            raise RuntimeError(f"Target run target {target_key!r} cfg_root must not contain . or ..: {value}")
        # cfg_root is freed: any safe path incl. "/" (root) and multi-segment; it no
        # longer must be a single scope segment and is independent of the target's ref/context.
        return "/" + "/".join(parts)

    for st in workflow_cfg.get("target_runs", []):
        if isinstance(st, str):
            target_run_id = st
            target_key = st
            target_override = {}
        else:
            target_run_id = st.get("id")
            if not target_run_id:
                raise RuntimeError("Target run entry missing required field 'id'")
            target_key = st.get("target")
            if not target_key:
                raise RuntimeError(f"Target run {target_run_id!r} has empty 'target'")
            target_override = st

        if target_key not in action_targets:
            raise RuntimeError(
                f"Target run target {target_key!r} (target_run id={target_run_id!r}) not found in action {workflow_cfg.get('action')!r}"
            )

        target_cfg = action_targets[target_key]
        if not isinstance(target_cfg, dict):
            raise RuntimeError(
                f"Target run target {target_key!r} metadata must be a mapping, got: {type(target_cfg).__name__}"
            )

        target_source = target_cfg.get("source")
        if not isinstance(target_source, str) or not target_source:
            raise RuntimeError(f"Target run target {target_key!r} must define non-empty 'source'")
        if target_source not in action_target_sources:
            raise RuntimeError(
                f"Target run target {target_key!r} references missing source {target_source!r} in action {workflow_cfg.get('action')!r}"
            )

        source_cfg = action_target_sources[target_source]
        if not isinstance(source_cfg, dict):
            raise RuntimeError(
                f"Target run source {target_source!r} metadata must be a mapping, got: {type(source_cfg).__name__}"
            )

        # resolve this target's ref context → per-context source/module pins.
        target_ref = target_cfg.get("ref")
        ctx_target_source_refs: dict = {}
        ctx_module_refs: dict = {}
        if scoped_refs and target_ref:
            ctx = execution_run_context.resolve_ref_context(target_ref, ref_context_values)
            ctx_block = scoped_refs.get(ctx)
            if ctx_block is None:
                raise RuntimeError(
                    f"Target run target {target_key!r} ref context {ctx!r} not found in refs.scoped"
                )
            # A scoped-ref group resolves to one concrete scoped
            # entry (the member ref_key may carry ${execution_context.*}
            # placeholders, rendered here before the second lookup).
            if run_selectors.selector_group_is_group(ctx_block):
                member_ref = run_selectors.resolve_selector_group_member(
                    ctx_block, ref_context_values,
                    value_field="ref_key",
                    label=f"refs.scoped group {ctx!r}",
                )
                concrete_ctx = execution_run_context.resolve_ref_context(member_ref, ref_context_values)
                ctx_block = scoped_refs.get(concrete_ctx)
                if ctx_block is None:
                    raise RuntimeError(
                        f"Target run target {target_key!r} refs.scoped group {ctx!r} member "
                        f"resolved to {concrete_ctx!r}, which is not in refs.scoped"
                    )
                if run_selectors.selector_group_is_group(ctx_block):
                    raise RuntimeError(
                        f"Target run target {target_key!r} refs.scoped group {ctx!r} member "
                        f"{concrete_ctx!r} is itself a group (no nested groups)"
                    )
            ctx_target_source_refs = ctx_block.get("target_sources") or {}
            ctx_module_refs = ctx_block.get("modules") or {}

        target_source_ref = ctx_target_source_refs.get(target_source) or {}
        if not isinstance(target_source_ref, dict):
            raise RuntimeError(
                f"Target run source refs for {target_source!r} must be a mapping, got: {type(target_source_ref).__name__}"
            )

        branch = target_override.get("branch") or target_source_ref.get("branch")
        commit = target_override.get("commit") or target_source_ref.get("commit")
        # Fat target carries the repo-local procedure; a dict target_run entry may still override
        child_procedure = target_override.get("procedure") or target_cfg.get("procedure")

        if branch and commit:
            raise RuntimeError(
                f"Target run {target_run_id!r} resolved both branch={branch!r} and commit={commit!r}. "
                "Only one ref type may be set."
            )

        if require_branch_or_commit and not branch and not commit:
            raise RuntimeError(f"Target run {target_run_id!r} source {target_source!r} has neither branch nor commit configured")
        if require_branch_or_commit and require_commit_refs and not commit:
            raise RuntimeError(
                f"Target run {target_run_id!r} ref {target_ref!r} requires an explicit commit (not a branch) for reproducibility"
            )

        repo_value = source_cfg.get(repo_key)
        if not repo_value:
            raise RuntimeError(
                f"Target run {target_run_id!r} (target={target_key!r}, source={target_source!r}) missing {repo_key!r} in action {workflow_cfg.get('action')!r}"
            )

        # A target_run carries its declared domains + per-domain key
        # contract. A domain-generic target whose axis is unbound arrives with
        # domains=None and is not materializable.
        target_domains = target_cfg.get("domains")
        target_cfg_keys = target_cfg.get("cfg_keys")
        if target_domains is not None and not isinstance(target_domains, list):
            raise RuntimeError(f"Target run target {target_key!r} domains must be a list")
        if target_cfg_keys is not None and not isinstance(target_cfg_keys, dict):
            raise RuntimeError(f"Target run target {target_key!r} cfg_keys must be a map")

        # A member entry may declare the ACTION this target performs.
        # Carried onto the run record so the spawn hands the child its own verb
        # rather than the parent's, and validated against the target's own
        # allowlist — the workflow may choose, never widen.
        member_action = target_override.get("action")
        allowed = target_cfg.get("allowed_actions") or []
        if member_action is not None and member_action not in allowed:
            raise RuntimeError(
                f"❌ workflow member {target_key!r} declares action "
                f"{member_action!r}, which that target does not allow "
                f"(allowed: {', '.join(sorted(allowed)) or 'none'})"
            )
        effective_action = member_action or (workflow_cfg.get("meta") or {}).get("action")
        reuse_policy = target_cfg.get("committed_result_reuse")
        if not isinstance(reuse_policy, dict) or effective_action not in reuse_policy:
            raise RuntimeError(
                f"❌ target run {target_run_id!r} cannot resolve committed-result reuse "
                f"for action {effective_action!r}"
            )
        reuse_committed_result = reuse_policy[effective_action]
        if not isinstance(reuse_committed_result, bool):
            raise RuntimeError(
                f"❌ target run {target_run_id!r} committed-result reuse for action "
                f"{effective_action!r} must be boolean"
            )

        active_target_run = {
            "target": target_key,
            "source": target_source,
            "ref": target_ref,
            "branch": branch,
            "commit": commit,
            "procedure": child_procedure,
            "domains": target_domains,
            "cfg_keys": target_cfg_keys,
            "reuse_committed_result": reuse_committed_result,
        }
        if member_action is not None:
            active_target_run["action"] = member_action
        required_overlays = target_cfg.get("requires_plt_overlays")
        if required_overlays:
            active_target_run["requires_plt_overlays"] = list(required_overlays)
        for behavior_field in (
            "provisions_ctl_state_backend",
            "allow_agreed_defer_ctl_state_backend_sync",
            *sorted(target_consent_opt_in_fields()),
        ):
            if target_cfg.get(behavior_field) is not None:
                active_target_run[behavior_field] = target_cfg[behavior_field]

        target_execution_identity = target_override.get("execution_identities") or target_cfg.get("execution_identities")
        if target_execution_identity is not None:
            active_target_run["execution_identities"] = validate_target_execution_identities(
                target_execution_identity, label=f"target run {target_run_id!r}"
            )
            active_target_run["providers"] = execution_providers.validate_target_providers(
                target_override.get("providers") or target_cfg.get("providers"),
                active_target_run["execution_identities"],
                label=f"target run {target_run_id!r}",
            )

        #/32: the declared instance identity must ride on the target_run
        # so per-target reports and the target-instance locator see it (the
        # workflow-composition identity reads it from the action separately).
        if target_cfg.get("target_instance_params_unresolved"):
            raise RuntimeError(
                f"Target run target {target_key!r} has a members-shaped "
                "target_instance_params whose dispatch axis is unbound in this run"
            )
        instance_params = target_cfg.get("target_instance_params")
        if instance_params is not None:
            active_target_run["target_instance_params"] = instance_params

        if repo_key == "repo_path":
            repo_path = Path(repo_value).expanduser()
            if not repo_path.is_absolute():
                raise RuntimeError(
                    f"Target run {target_run_id!r} source {target_source!r} repo_path must be absolute, got: {repo_value}"
                )
            active_target_run["repo_path"] = str(repo_path.resolve())
        else:
            active_target_run["repo_url"] = repo_value
            active_target_run["secret_key"] = source_cfg.get("secret_key")

        raw_modules = source_cfg.get("modules") or {}
        if raw_modules and not isinstance(raw_modules, dict):
            raise RuntimeError(
                f"Target run {target_run_id!r} source {target_source!r} modules must be a mapping, got: {type(raw_modules).__name__}"
            )

        resolved_modules = {}
        for module_name, module_meta in raw_modules.items():
            if not isinstance(module_name, str):
                raise RuntimeError(
                    f"Target run {target_run_id!r} module names must be strings, got: {type(module_name).__name__}"
                )
            if module_meta is None:
                module_meta = {}
            if not isinstance(module_meta, dict):
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} metadata must be a mapping, got: {type(module_meta).__name__}"
                )

            module_ref = ctx_module_refs.get(module_name) or {}
            if not isinstance(module_ref, dict):
                raise RuntimeError(
                    f"Module refs for {module_name!r} must be a mapping, got: {type(module_ref).__name__}"
                )

            module_branch = module_ref.get("branch")
            module_commit = module_ref.get("commit")
            if module_branch and module_commit:
                raise RuntimeError(
                    f"Module {module_name!r} resolved both branch={module_branch!r} and commit={module_commit!r}. "
                    "Only one ref type may be set."
                )
            if require_branch_or_commit and not module_branch and not module_commit:
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} has neither branch nor commit configured"
                )
            if require_branch_or_commit and require_commit_refs and not module_commit:
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} ref {target_ref!r} requires an explicit commit"
                )

            dest = module_meta.get("dest")
            if not isinstance(dest, str) or not dest.strip():
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} must define non-empty 'dest'"
                )
            dest_path = Path(dest)
            if dest_path.is_absolute() or ".." in dest_path.parts:
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} dest must stay within the target_run repo: {dest}"
                )

            module_repo_value = module_meta.get(repo_key)
            if not module_repo_value:
                raise RuntimeError(
                    f"Target run {target_run_id!r} module {module_name!r} missing {repo_key!r} in action {workflow_cfg.get('action')!r}"
                )

            resolved_module = {
                "dest": dest,
                "branch": module_branch,
                "commit": module_commit,
            }
            if repo_key == "repo_path":
                module_repo_path = Path(module_repo_value).expanduser()
                if not module_repo_path.is_absolute():
                    raise RuntimeError(
                        f"Target run {target_run_id!r} module {module_name!r} repo_path must be absolute, got: {module_repo_value}"
                    )
                resolved_module["repo_path"] = str(module_repo_path.resolve())
            else:
                resolved_module["repo_url"] = module_repo_value
                resolved_module["secret_key"] = module_meta.get("secret_key")

            resolved_modules[module_name] = resolved_module

        if resolved_modules:
            active_target_run["modules"] = resolved_modules

        active[target_run_id] = active_target_run

    return active


def validate_target_execution_identity_coverage(
    active_target_runs: dict,
    *,
    execution_access_modes: dict[str, str] | None = None,
) -> None:
    """Every target_run declares its `execution_identity:` block, always.

    The one exception is a run where NO provider resolves an execution identity
    (every provider runs on a substitute credential) — then there is nothing for
    the block to feed. A mixed run still requires it everywhere: a target_run
    without a block does not say which provider it belongs to, so it cannot be
    matched to the provider that would have excused it.
    """

    modes = execution_access_modes or {}
    if modes and not any(
        execution_adapters.get_adapter(provider).resolves_execution_identity(mode)
        for provider, mode in modes.items()
    ):
        return
    stages_without_execution = sorted(
        target_run_id for target_run_id, target_run in active_target_runs.items()
        if target_run.get("execution_identities") is None
    )
    if stages_without_execution:
        raise RuntimeError(
            "❌ selected target_runs have no execution_identity block: "
            + ", ".join(stages_without_execution)
            + "; declare it, or run every provider in a mode that resolves no "
            "execution identity (see `ctl.py providers`)"
        )


class TargetActionPolicy:
    """Validate one target declaration's action-related policy.

    The declaration and its label are shared by both checks, so the responsibility
    is one object rather than separate validators that repeat the same inputs.
    """

    def __init__(self, entry: dict, *, label: str):
        self.entry = entry
        self.label = label

    def actions(self) -> list[str]:
        """Return the required target-action allowlist."""

        actions = self.entry.get("actions")
        if "operations" in self.entry:
            raise RuntimeError(
                f"❌ {self.label} target action policy does not accept 'operations'"
            )
        if (
            not isinstance(actions, list)
            or not actions
            or not all(
                isinstance(action, str) and action in run_actions.KNOWN_ACTIONS
                for action in actions
            )
            or len(set(actions)) != len(actions)
        ):
            raise RuntimeError(
                f"❌ {self.label} must declare 'actions': a non-empty, duplicate-free "
                f"subset of {list(run_actions.KNOWN_ACTIONS)} "
                "(availability is default-closed)"
            )
        return actions

    def committed_result_reuse(self) -> dict[str, bool]:
        """Return the target's action-exact committed-result reuse policy."""

        actions = self.actions()
        policy = self.entry.get("committed_result_reuse")
        if not isinstance(policy, dict):
            raise RuntimeError(
                f"❌ {self.label} must declare 'committed_result_reuse': a mapping with "
                "one boolean for every declared action"
            )
        missing = sorted(set(actions) - set(policy))
        extra = sorted(set(policy) - set(actions))
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing actions: {missing}")
            if extra:
                details.append(f"undeclared actions: {extra}")
            raise RuntimeError(
                f"❌ {self.label} committed_result_reuse must match its actions exactly "
                f"({'; '.join(details)})"
            )
        non_boolean = sorted(
            action for action, value in policy.items() if not isinstance(value, bool)
        )
        if non_boolean:
            raise RuntimeError(
                f"❌ {self.label} committed_result_reuse values must be booleans "
                f"(invalid actions: {non_boolean})"
            )
        return policy


def load_action_cfg(
    ctl_cfg_root: Path,
    action: str,
    execution_context: dict[str, object] | None = None,
    member_actions: set[str] | None = None,
) -> dict:
    """Compose action cfg from target_sources, cfg key sets, and targets/*.yaml.

    `action` is the action (provision/plan/destroy/readonly). Layout:
      - target_sources.yaml  source repos: source key -> meta
      - cfg_key_sets.yaml        named CONTENT-KEY bundles: set key -> [key selectors]
      - targets/*.yaml  fat targets. Each file is a flat `targets:` map; all
            files merge (duplicate names rejected). A target is self-contained:
              {actions, committed_result_reuse, source_key, ref_key,
               procedure_key, domains, cfg_keys, [execution], [selectors],
               [required_plt_overlay_keys]}.

    Returns the flat shape build_active_target_runs consumes ({target_sources,
    targets}), where each target carries source + domains + cfg_keys
    (its declared domains + per-domain cfg_keys) + procedure + execution identity requirement
    (+ selectors /
    requires_plt_overlays when present).
    """
    # Global resources + targets are content-key (collected by top-level key)
    target_sources = cfg_resources.collect_resource(ctl_cfg_root, "target_sources")
    cfg_key_sets = cfg_resources.collect_resource(ctl_cfg_root, "cfg_key_sets")
    param_sets = cfg_resources.collect_resource(ctl_cfg_root, "param_sets")
    cfg_key_sets_path = ctl_cfg_root  # label for include/error messages
    domain_registry = execution_run_context.load_domain_registry(ctl_cfg_root)
    if not target_sources:
        raise RuntimeError(f"❌ no 'target_sources' defined under: {ctl_cfg_root}")
    if not domain_registry:
        raise RuntimeError(f"❌ no 'domains' registry defined under: {ctl_cfg_root}")

    # Targets are declared ONCE (no action level); each declares a
    # REQUIRED `actions:` allowlist (default-closed) and the action for a run
    # is the subset allowing this action.
    all_targets = load_target_catalog(ctl_cfg_root)
    targets = {}
    target_actions: dict[str, list[str]] = {}
    target_reuse_policies: dict[str, dict[str, bool]] = {}
    # A workflow member may declare its own action, so the action
    # admits a target that allows the INVOKED action or any action a member asked
    # of it. Without this a member naming `destroy` on a destroy-only target could
    # not resolve inside a provision workflow, which is the whole point of letting
    # A composition mix directions.
    admitted = {action, *(member_actions or ())}
    for target_name, target_def in all_targets.items():
        if not isinstance(target_def, dict):
            raise RuntimeError(f"❌ target {target_name!r} must be a mapping")
        label = f"target {target_name!r}"
        action_policy = TargetActionPolicy(target_def, label=label)
        allowed_actions = action_policy.actions()
        reuse_policy = action_policy.committed_result_reuse()
        target_actions[target_name] = allowed_actions
        target_reuse_policies[target_name] = reuse_policy
        if admitted & set(allowed_actions):
            targets[target_name] = target_def
    if not targets:
        raise RuntimeError(f"❌ no targets allow action {action!r}")

    validate_distinct_target_signatures(all_targets)

    resolved_targets: dict = {}
    for target_name, target_def in targets.items():
        consumed_group_axes: set[str] = set()

        source = target_def.get("source_key")
        if not isinstance(source, str) or not source:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'source_key'")

        target_ref = target_def.get("ref_key")
        if not isinstance(target_ref, str) or not target_ref.strip():
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'ref_key'")

        # The target DECLARES the domains it reads and, per domain, the
        # content keys it consumes. `domains` is identity (which namespaces this
        # target subscribes to); `cfg_keys` is the contract (what it takes out of
        # them). Neither names a plt FILE, so plt owns its own layout.
        declared_domains = target_def.get("domains")
        if not isinstance(declared_domains, list) or not declared_domains:
            raise RuntimeError(
                f"❌ target {target_name!r} must define a non-empty 'domains' list"
            )
        # A domain-GENERIC target (the state-backend one) takes its domain from the
        # execution context. If that axis is not bound — a generic target in a
        # shared action this run does not activate — resolution is deferred
        # (None) rather than failing an unrelated run.
        domains: list[str] | None = []
        for raw_domain in declared_domains:
            if not isinstance(raw_domain, str) or not raw_domain.strip():
                raise RuntimeError(
                    f"❌ target {target_name!r} domains entries must be non-empty strings"
                )
            raw_domain = raw_domain.strip()
            if "${" not in raw_domain:
                domains.append(raw_domain)
                continue
            if execution_context is None:
                domains = None
                break
            consumed_group_axes.update(
                ref[len(f"{execution_references.EXECUTION_CONTEXT_ROOT}.params."):]
                for ref in execution_references.RUNTIME_SCALAR_TOKEN_RE.findall(raw_domain)
                if ref.startswith(f"{execution_references.EXECUTION_CONTEXT_ROOT}.params.")
            )
            resolved_domain = execution_references.resolve_runtime_scalar(
                raw_domain, execution_context,
                label=f"target {target_name!r} domains entry",
                tolerate_missing=True,
            )
            if resolved_domain is None:
                domains = None
                break
            domains.append(str(resolved_domain))
        if domains is not None:
            if len(set(domains)) != len(domains):
                raise RuntimeError(f"❌ target {target_name!r} lists a domain twice: {domains}")
            for domain in domains:
                execution_run_context.validate_domain_value(
                    domain_registry, domain, label=f"target {target_name!r} domains"
                )

        raw_cfg_keys = target_def.get("cfg_keys") or {}
        raw_cfg_key_sets = target_def.get("cfg_key_sets") or {}
        for field, value in (("cfg_keys", raw_cfg_keys), ("cfg_key_sets", raw_cfg_key_sets)):
            if not isinstance(value, dict):
                raise RuntimeError(f"❌ target {target_name!r} {field} must be a map (domain -> list)")
        if not raw_cfg_keys and not raw_cfg_key_sets:
            raise RuntimeError(
                f"❌ target {target_name!r} must define cfg_key_sets and/or cfg_keys "
                "(domain -> what it consumes)"
            )
        cfg_keys: dict[str, list[str]] | None = None
        if domains is not None:
            def _domain_key(raw, target_name=target_name):
                key = str(raw).strip()
                if "${" in key:
                    key = str(execution_references.resolve_runtime_scalar(
                        key, execution_context,
                        label=f"target {target_name!r} cfg_keys domain"))
                return key
            per_domain: dict[str, dict] = {}
            for raw_key, entries in raw_cfg_key_sets.items():
                per_domain.setdefault(_domain_key(raw_key), {})["cfg_key_sets"] = entries
            for raw_key, entries in raw_cfg_keys.items():
                per_domain.setdefault(_domain_key(raw_key), {})["cfg_keys"] = entries
            cfg_keys = {
                domain_key: execution_references.resolve_cfg_key_entries(
                    spec.get("cfg_keys"), spec.get("cfg_key_sets"), cfg_key_sets,
                    label=f"target {target_name!r} [{domain_key}]",
                    cfg_path=cfg_key_sets_path,
                )
                for domain_key, spec in per_domain.items()
            }
            # Assertion 2: cfg_keys for a domain this target does not read
            extra = sorted(set(cfg_keys) - set(domains))
            if extra:
                raise RuntimeError(
                    f"❌ target {target_name!r} declares cfg_keys for {extra}, "
                    f"which are not in its domains {domains}"
                )
            # Assertion 3: a declared domain this target takes nothing from
            missing = sorted(set(domains) - set(cfg_keys))
            if missing:
                raise RuntimeError(
                    f"❌ target {target_name!r} declares domains {missing} with no "
                    "cfg_keys entry — a subscription that consumes nothing"
                )

        procedure = target_def.get("procedure_key")
        if not isinstance(procedure, str) or not procedure:
            raise RuntimeError(f"❌ target {target_name!r} must define a non-empty 'procedure_key'")

        # The target declares its execution axes inline. The old
        # `execution_identity_key` (a bundle of account + role + credential,
        # dispatched through named identity groups) is gone: per-action variation
        # is now `execution.roles`, keyed by authorization class.
        if "execution_identity_key" in target_def:
            raise RuntimeError(
                f"❌ target {target_name!r} uses `execution_identity_key`, which is removed; "
                "declare an `execution_identity:` block (provider, account, roles) instead"
            )
        target_execution_identity = target_def.get("execution_identities")
        target_providers = None
        if target_execution_identity is not None:
            target_execution_identity = validate_target_execution_identities(
                target_execution_identity, label=f"target {target_name!r}"
            )
            target_providers = execution_providers.validate_target_providers(
                target_def.get("providers"),
                target_execution_identity,
                label=f"target {target_name!r}",
            )

        # A target declares the COORDINATES it consumes and the
        # CONSTANTS it always uses. The two must not intersect — a name is either
        # A coordinate or a constant, never both.
        declared_input_params = execution_run_context.resolve_input_params(
            target_def.get("input_params"),
            target_def.get("input_param_sets"),
            param_sets,
            label=f"target {target_name!r}",
            cfg_path=cfg_key_sets_path,
        )
        static_vars = target_def.get("static_vars") or {}
        if not isinstance(static_vars, dict):
            raise RuntimeError(f"❌ target {target_name!r} static_vars must be a map")
        for var_name, var_value in static_vars.items():
            if not isinstance(var_name, str) or not execution_references.CONTEXT_KEY_RE.fullmatch(var_name):
                raise RuntimeError(
                    f"❌ target {target_name!r} static_vars key {var_name!r} must be an identifier"
                )
            if isinstance(var_value, (dict, list)):
                raise RuntimeError(
                    f"❌ target {target_name!r} static_vars.{var_name} must be a literal scalar; "
                    "a selector-dependent constant varies per instance, which makes it a coordinate"
                )
        overlap = sorted(set(declared_input_params) & set(static_vars))
        if overlap:
            raise RuntimeError(
                f"❌ target {target_name!r} declares {overlap} as BOTH an input param and a "
                "static var; a name is either a coordinate or a constant"
            )

        resolved = {
            "source": source,
            "ref": target_ref.strip(),
            "procedure": procedure,
            "domains": domains,
            "cfg_keys": cfg_keys,
            "input_params": declared_input_params,
            "static_vars": dict(static_vars),
            # The allowlist is carried onto the action entry, not
            # consumed by the filter that built it. A workflow member may name an
            # action, and the check that it is permitted needs the list at the
            # point the member is resolved.
            "allowed_actions": target_actions[target_name],
            "committed_result_reuse": target_reuse_policies[target_name],
        }
        if domains is None:
            # Domain-generic target whose domain axis is not bound in this run.
            # Record the AXIS NAMES it needs, never the raw `${...}` template:
            # The ctl cfg snapshot resolves every scalar it walks, so an
            # unresolved placeholder stored here would fail the whole run.
            resolved["domains_unresolved"] = sorted(
                {
                    ref
                    for raw in declared_domains
                    for ref in execution_references.RUNTIME_SCALAR_TOKEN_RE.findall(str(raw))
                }
            )
        # Declared instance identity flows through to the resolved
        # target (consumed by resolve_run_instance_identity).
        # A GENERIC target whose instance axes vary by another axis
        # dispatches its schema by `members` ({params: [...], selectors: {...}}),
        # the same pattern as its ref groups. Exactly one member
        # matches; an unbound dispatch axis defers (hard error only if the
        # target is actually activated in a run).
        instance_params = target_def.get("target_instance_params")
        # Every coordinate must be a declared input — checked on ALL
        # members branches, not just the one this run selects, so an unreachable
        # branch cannot hide a typo until some later execution context picks it.
        for branch in (
            [m.get("params") for m in (instance_params.get("members") or [])]
            if isinstance(instance_params, dict)
            else [instance_params]
        ):
            if not isinstance(branch, list):
                continue
            undeclared = sorted(
                {str(x).strip() for x in branch if isinstance(x, str)}
                - set(declared_input_params)
            )
            if undeclared:
                raise RuntimeError(
                    f"❌ target {target_name!r} target_instance_params {undeclared} are not "
                    f"declared input params (declared: {sorted(declared_input_params) or 'none'}); "
                    "a target cannot be identified by a coordinate it does not read"
                )
        if isinstance(instance_params, dict):
            # The dispatch axes of the schema itself are consumed axes too
            consumed_group_axes.update(
                run_selectors.collect_member_dispatch_axes(
                    instance_params.get("members"),
                    label=f"target {target_name!r} target_instance_params members",
                )
            )
            instance_params = _resolve_instance_params_members(
                instance_params, execution_context, target_name=target_name
            )
            if instance_params is None:
                resolved["target_instance_params_unresolved"] = True
        if consumed_group_axes:
            resolved["consumed_group_axes"] = sorted(consumed_group_axes)
        if instance_params is not None:
            if not isinstance(instance_params, list) or not all(
                isinstance(p, str) and p.strip() for p in instance_params
            ):
                raise RuntimeError(
                    f"❌ target {target_name!r} target_instance_params must be a list of non-empty strings"
                )
            resolved["target_instance_params"] = [p.strip() for p in instance_params]
        if target_execution_identity is not None:
            resolved["execution_identities"] = target_execution_identity
            resolved["providers"] = target_providers
        if "provisions_ctl_state_bucket" in target_def:
            raise RuntimeError(
                f"❌ target {target_name!r} uses deprecated provisions_ctl_state_bucket; "
                "use provisions_ctl_state_backend"
            )
        if target_def.get("provisions_ctl_state_backend") is True:
            resolved["provisions_ctl_state_backend"] = True
        # per-target consent to a mode that requires it (§12); the adapter names
        # the field, the engine only carries it through. Default: not granted.
        for consent_field in target_consent_opt_in_fields():
            if consent_field in target_def:
                if not isinstance(target_def[consent_field], bool):
                    raise RuntimeError(f"❌ target {consent_field} must be a boolean")
                resolved[consent_field] = target_def[consent_field]
        for legacy_flag in (  # removed keys
            "allow_skip_ctl_entry",
            "allow_skip_ctl_state_sync",
            "skip_ctl_role_chain",  # removed
            "execution_access_mode",
        ):
            if legacy_flag in target_def:
                raise RuntimeError(
                    f"❌ target {target_name!r} uses removed {legacy_flag}; "
                    "use the execution_identity block's consent fields (§12) for access, "
                    "allow_agreed_defer_ctl_state_backend_sync for deferred sync"
                )
        # Static policy: the target may participate in an explicitly agreed
        # deferred-sync bootstrap graph. Runtime agreement is a separate CLI fact.
        if "allow_agreed_defer_ctl_state_backend_sync" in target_def:
            value = target_def["allow_agreed_defer_ctl_state_backend_sync"]
            if value is not True:
                raise RuntimeError(
                    f"❌ target {target_name!r} allow_agreed_defer_ctl_state_backend_sync "
                    "must be literal true when present"
                )
            resolved["allow_agreed_defer_ctl_state_backend_sync"] = True
        if "selectors" in target_def:
            resolved["selectors"] = target_def["selectors"]
        if "required_plt_overlay_keys" in target_def:
            overlay_keys = target_def["required_plt_overlay_keys"]
            if not isinstance(overlay_keys, list) or not all(
                isinstance(key, str) and key for key in overlay_keys
            ):
                raise RuntimeError(
                    f"❌ target {target_name!r} required_plt_overlay_keys must be "
                    "a list of non-empty strings"
                )
            duplicate_overlay_keys = [
                key
                for key, count in collections.Counter(overlay_keys).items()
                if count > 1
            ]
            if duplicate_overlay_keys:
                raise RuntimeError(
                    f"❌ target {target_name!r} required_plt_overlay_keys must be unique; "
                    f"duplicates: {', '.join(sorted(duplicate_overlay_keys))}"
                )
            resolved["requires_plt_overlays"] = overlay_keys
        resolved_targets[target_name] = resolved

    return {
        "target_sources": target_sources,
        "targets": resolved_targets,
    }


def normalize_target_entries(
    values: list, *, label: str, default_action: str | None = None
) -> list[tuple[str, str]]:
    """A workflow member entry is a KEY or a key with its own ACTION.

        - key: env/ops/ecr
          action: provision

    The action belongs to the TARGET, never to the member list: a member is a list
    plus a selector deciding WHEN the list applies, so putting the action on the
    member would force one member per target purely to vary a verb.

    Every entry states its action; nothing inherits. A key MAY repeat with
    differing actions. Both entries address one instance, so
    its pointer is written twice in one run and the last write wins — correct,
    because after destroy-then-provision the instance is provisioned. Order is
    therefore load-bearing: with a repeated key it decides the final state, not
    merely the execution sequence.
    """

    if not isinstance(values, list):
        raise RuntimeError(f"❌ {label} must be a list")
    entries: list[tuple[str, str | None]] = []
    seen: set[tuple[str, str | None]] = set()
    for value in values:
        if isinstance(value, dict):
            unknown = sorted(set(value) - {"key", "action"})
            if unknown:
                raise RuntimeError(
                    f"❌ {label} entry has unsupported keys {unknown}; a member "
                    "entry is a key, or a key with its own action"
                )
            key = run_actions.normalize_result_name(value.get("key"), label=label)
            action = value.get("action")
            if action is not None and (
                not isinstance(action, str)
                or action not in run_actions.WORKFLOW_ACTIONS
            ):
                raise RuntimeError(
                    f"❌ {label} entry {key!r} declares action {action!r}; expected "
                    f"one of {sorted(run_actions.WORKFLOW_ACTIONS)}; maintenance "
                    "uses the maintenance runner"
                )
        else:
            key, action = run_actions.normalize_result_name(value, label=label), None
        # Every entry ends up with a DECLARED action — its own, or the
        # one its member declares for the whole list. There is no fallback: without
        # an action the engine does not know how to run the target, and a silent
        # guess is the one thing a cfg gate exists to prevent.
        action = action or default_action
        if action is None:
            raise RuntimeError(
                f"❌ {label} entry {key!r} has no action, so it is not runnable. "
                "Declare `default_action:` for the list, or `action:` beneath the key"
            )
        if action not in run_actions.WORKFLOW_ACTIONS:
            raise RuntimeError(
                f"❌ {label} entry {key!r} resolves action {action!r}; expected one "
                f"of {sorted(run_actions.WORKFLOW_ACTIONS)}; maintenance uses the "
                "maintenance runner"
            )
        if (key, action) in seen:
            raise RuntimeError(
                f"❌ duplicate target entry in {label}: {key}"
                + (f" (action {action})" if action else "")
            )
        seen.add((key, action))
        entries.append((key, action))
    return entries


def normalize_insert_entries(
    values: list, *, label: str, default_action: str | None = None
) -> list[tuple[str, str, tuple[str, str, str]]]:
    """A placed entry: a key, its action, and the ANCHOR saying where it goes.

        - key: env/ops/db_artificial_populator
          after:
            workflow: env/baseline
            key: env/ops/dbs

    An anchor addresses a sequence this workflow did not author, so it names the
    import it points into: with two imports a bare key would not say which one.
    Qualified, it reads as "in `env/baseline`, after `env/ops/dbs`".

    `after` and `before` are mutually exclusive — `before` exists because `after`
    alone cannot express insertion at the head of a sequence.

    Returns `(key, action, (position, anchor_workflow, anchor_key))`.
    """

    if not isinstance(values, list):
        raise RuntimeError(f"❌ {label} must be a list")
    entries: list[tuple[str, str, tuple[str, str, str]]] = []
    for value in values:
        if not isinstance(value, dict):
            raise RuntimeError(
                f"❌ {label} entry {value!r} declares no anchor; every entry here "
                "states `after:` or `before:`"
            )
        unknown = sorted(set(value) - {"key", "action", "after", "before"})
        if unknown:
            raise RuntimeError(
                f"❌ {label} entry has unsupported keys {unknown}; a placed entry is "
                "a key, an optional action, and one anchor"
            )
        key = run_actions.normalize_result_name(value.get("key"), label=label)
        positions = [name for name in ("after", "before") if name in value]
        if len(positions) != 1:
            raise RuntimeError(
                f"❌ {label} entry {key!r} declares {positions or 'no anchor'}; state "
                "exactly one of `after:` or `before:`"
            )
        position = positions[0]
        anchor = value[position]
        if not isinstance(anchor, dict) or sorted(anchor) != ["key", "workflow"]:
            raise RuntimeError(
                f"❌ {label} entry {key!r} anchor must be "
                "{workflow: <imported workflow>, key: <target key>}"
            )
        anchor_workflow = run_actions.normalize_result_name(anchor["workflow"], label=label)
        anchor_key = run_actions.normalize_result_name(anchor["key"], label=label)
        action = value.get("action") or default_action
        if action is None:
            raise RuntimeError(
                f"❌ {label} entry {key!r} has no action, so it is not runnable. "
                "Declare `default_action:` for the list, or `action:` beneath the key"
            )
        entries.append((key, action, (position, anchor_workflow, anchor_key)))
    return entries


def target_keys_from_active_target_runs(active_target_runs: dict) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for target_run in active_target_runs.values():
        target_key = target_run.get("target")
        if not isinstance(target_key, str) or not target_key:
            continue
        target_key = run_actions.normalize_result_name(target_key, label="resolved target key")
        if target_key not in seen:
            seen.add(target_key)
            keys.append(target_key)
    return keys


def target_definition_document(target_run: dict) -> dict:
    """Return the stable, resolved target definition used for execution."""
    stable_keys = (
        "target",
        "source",
        "ref",
        "branch",
        "commit",
        "procedure",
        "domains",
        "cfg_keys",
        "reuse_committed_result",
        "target_instance_params",
        "requires_plt_overlays",
        "execution_identities",
        "provisions_ctl_state_backend",
        "allow_agreed_defer_ctl_state_backend_sync",
        *sorted(target_consent_opt_in_fields()),
    )
    definition = {
        key: target_run[key]
        for key in stable_keys
        if target_run.get(key) is not None
    }
    modules = {}
    for module_name, module in (target_run.get("modules") or {}).items():
        stable_module = {
            key: module[key]
            for key in ("dest", "branch", "commit")
            if module.get(key) is not None
        }
        modules[module_name] = stable_module
    if modules:
        definition["modules"] = modules
    return definition


def attach_target_definition_facts(active_target_runs: dict) -> None:
    for target_run in active_target_runs.values():
        definition = target_definition_document(target_run)
        target_run["target_definition"] = definition
        target_run["target_definition_sha256"] = kernel_paths.canonical_sha256(definition)


def describe_target_execution_identity(execution: object) -> str | None:
    """Compact report label for execution identities: provider:account:role.

    Accepts either the keyed block (`{provider: {...}}`, one entry per provider)
    or one already-selected entry. Several providers render one line each, joined
    by `, ` — a target with two identities reads as two, not as a merged one.

    Provider-neutral: the engine only joins the declared fields; it does not
    interpret them.
    """

    if not isinstance(execution, dict) or not execution:
        return None

    def _entry(provider: str | None, entry: dict) -> str:
        parts = [str(provider or entry.get("provider") or "?"), str(entry.get("account") or "?")]
        roles = entry.get("roles")
        if isinstance(roles, dict) and roles:
            parts.append("/".join(f"{cls}={key}" for cls, key in sorted(roles.items())))
        elif entry.get("role"):
            parts.append(str(entry["role"]))
        return ":".join(parts)

    if all(isinstance(value, dict) for value in execution.values()):
        return ", ".join(_entry(provider, entry) for provider, entry in sorted(execution.items()))
    return _entry(None, execution)


def validate_workflow_target_selectors(
    workflow_cfg: dict,
    action_cfg: dict,
    execution_context: dict[str, object],
) -> None:
    targets = action_cfg.get("targets", {})
    for entry in workflow_cfg.get("target_runs", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        target_cfg = targets.get(target_name)
        if target_cfg is None:
            continue
        selectors = target_cfg.get("selectors")
        if not run_selectors.selector_matches(selectors, execution_context, label=f"target {target_name}"):
            raise RuntimeError(
                f"❌ target {target_name!r} is not available for runtime selectors {execution_context}; "
                f"selectors={selectors}"
            )
