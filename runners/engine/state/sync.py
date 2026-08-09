"""Publishing local run records to the ctl-state backend.

Publication can be deferred or skipped, which makes the LOCAL tree a mirror and
the backend the truth. A run that cannot publish queues rather than failing, so
a backend outage does not cost the record of what ran.

Four subjects, so four classes. `CtlStateBackends` and `CtlStateAccess` read cfg
and hold nothing, so their methods are static. `PendingSyncQueue` is the on-disk
manifest format, likewise static. `CtlStatePublication` is the one ARMED session
a run publishes through — it is an instance because the syncer, the two configs
and the note are one coherent state that a cluster of operations both reads and
replaces.
"""

import contextlib
import fcntl
import logging
import re
from pathlib import Path

from engine.execution import adapters as execution_adapters
from engine.execution import providers as execution_providers
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.kernel import ids as kernel_ids
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import addressing as run_addressing
from engine.run import selectors as run_selectors
from engine.state import run_store as state_run_store


def release_remote_mutation_lock(syncer, lock_id: str) -> str:
    """Release the NAMESPACE lock, refusing to release someone else's.

    `--lock-id` names the holder from the error rather than releasing whatever is
    present: the hazard is not a stale lock, it is releasing one whose holder is
    still ALIVE. Only this lock protects the namespace from another machine, so a
    wrong release yields two concurrent mutating runs on one namespace.
    """

    existing = syncer.read_mutation_lock()
    if not existing:
        return "not present — skipped"
    holder = str(existing.get("run_id") or "")
    if holder != lock_id:
        raise RuntimeError(
            f"❌ the namespace lock is held by run {holder!r}, not {lock_id!r}; "
            "pass the id named in the error"
        )
    syncer.delete_mutation_lock()
    return "released"


def ctl_state_target_address_prefix(action: str, address: str) -> str:
    target_key, segments = run_addressing.split_target_instance_address(address)
    return run_addressing.compose_state_relpath("target", target_key, segments).as_posix()


def run_provisions_ctl_state_backend(workflow_cfg: dict, action_cfg: dict) -> bool:
    """

    whether any target in this run is the ctl-state bucket-creating target
    (declares provisions_ctl_state_backend: true). Such a run may legitimately start
    before its results bucket exists; every other run must find it already there
    under a `required` sync policy."""

    targets = action_cfg.get("targets", {})
    for entry in workflow_cfg.get("target_runs", []):
        target_name = entry if isinstance(entry, str) else entry.get("target")
        target_cfg = targets.get(target_name) or {}
        if target_cfg.get("provisions_ctl_state_backend") is True:
            return True
    return False


class CtlStateBackends:
    """The `ctl_state_backends` registry: what backends exist and what each declares.

    Stateless: every method reads a cfg tree or one registry entry and answers
    about that alone. The registry is re-read per call because a namespace is
    resolved from many places in a run, each with its own cfg root.
    """

    OPERATIONS = ("read", "sync", "maintenance")

    # A namespace may declare its own: a backend holding a slow estate needs longer
    # than one holding a small one, and that is known where the namespace is declared.
    MUTATION_LOCK_TTL_FIELD = "mutation_lock_ttl_seconds"

    @staticmethod
    def validate_execution(execution: object, *, label: str, path: Path) -> dict:
        """Validate a ctl-state backend's `execution_identity:` block.

        Same shape as a target's, with one structural difference: a backend has a
        role per ctl-state OPERATION (read / sync / maintenance — least privilege),
        where a target has one per authorization class. The account is fixed, so the
        direct credential source is SINGULAR per operation rather than a list.
        """

        if not isinstance(execution, dict) or not execution:
            raise RuntimeError(f"❌ {label}.execution must be a non-empty mapping: {path}")
        unknown = sorted(set(execution) - {"account", "operations"})
        if unknown:
            raise RuntimeError(
                f"❌ {label}.execution has unknown fields {unknown}; allowed: ['account', 'operations']: {path}"
            )
        account = execution.get("account")
        if not isinstance(account, str) or not account.strip():
            raise RuntimeError(f"❌ {label}.execution.account must be a non-empty string: {path}")

        operations = execution.get("operations")
        if not isinstance(operations, dict) or not operations:
            raise RuntimeError(
                f"❌ {label}.execution_identity.operations must be a non-empty mapping keyed by "
                f"{list(CtlStateBackends.OPERATIONS)}: {path}"
            )
        unknown_ops = sorted(set(operations) - set(CtlStateBackends.OPERATIONS))
        if unknown_ops:
            raise RuntimeError(
                f"❌ {label}.execution_identity.operations has unknown operations {unknown_ops}; "
                f"allowed: {list(CtlStateBackends.OPERATIONS)}: {path}"
            )

        cleaned_ops: dict[str, dict] = {}
        for operation, spec in operations.items():
            op_label = f"{label}.execution_identity.operations.{operation}"
            if not isinstance(spec, dict) or not spec:
                raise RuntimeError(f"❌ {op_label} must be a non-empty mapping: {path}")
            unknown_fields = sorted(set(spec) - {"role", "agreed_direct_credential_source_key"})
            if unknown_fields:
                raise RuntimeError(f"❌ {op_label} has unknown fields {unknown_fields}: {path}")
            role = spec.get("role")
            if not isinstance(role, str) or not role.strip():
                raise RuntimeError(f"❌ {op_label}.role must be a non-empty string: {path}")
            cleaned = {"role": role.strip()}
            direct_key = spec.get("agreed_direct_credential_source_key")
            if direct_key is not None:
                if not isinstance(direct_key, str) or not direct_key.strip():
                    raise RuntimeError(
                        f"❌ {op_label}.agreed_direct_credential_source_key must be a non-empty string: {path}"
                    )
                cleaned["agreed_direct_credential_source_key"] = direct_key.strip()
            cleaned_ops[operation] = cleaned

        return {"account": account.strip(), "operations": cleaned_ops}

    @staticmethod
    def operation_execution(
        entry: dict, operation: str, *, namespace_key: str, required: bool = True
    ) -> dict | None:
        """A ctl-state backend's execution narrowed to ONE operation.

        Returns the account plus that operation's role and optional direct credential
        source; the adapter turns those into a credential.
        """

        execution = entry.get("execution_identity") or {}
        spec = (execution.get("operations") or {}).get(operation)
        if spec is None:
            if required:
                raise RuntimeError(
                    f"❌ ctl_state_backends.{namespace_key} declares no "
                    f"execution_identity.operations.{operation}"
                )
            return None
        return {"account": execution.get("account"), "operation": operation, **spec}

    @staticmethod
    def mutation_lock_ttl(entry: dict | None) -> int:
        """

        the lock TTL a ctl-state backend entry declares, else the default."""

        field = CtlStateBackends.MUTATION_LOCK_TTL_FIELD
        declared = (entry or {}).get(field)
        if declared is None:
            return state_run_store.MUTATION_LOCK_TTL_SECONDS
        if not isinstance(declared, int) or isinstance(declared, bool) or declared <= 0:
            raise RuntimeError(
                f"❌ {field} must be a positive integer of seconds, "
                f"got {declared!r}"
            )
        return declared

    @staticmethod
    def load(ctl_cfg_root: Path) -> dict | None:
        """Load the optional ctl-state backend registry.

        Schema is ``ctl_state_backends``:
        {namespace: {selectors, provider, backend_type, bucket_name,
        bucket_region, execution}}. The backend declares its own `execution_identity:` block
     — one account, and a role per ctl-state OPERATION.
        """
        merged: dict = {}
        seen_sources: dict[str, Path] = {}
        section_name = "ctl_state_backends"
        entries = list(kernel_yaml_io.collect_top_level_sections(ctl_cfg_root, section_name))
        for path, section in entries:
            if not isinstance(section, dict):
                raise RuntimeError(f"❌ {section_name} must be a mapping: {path}")
            for namespace_key, entry in section.items():
                # Namespaces are consumer-defined vocabulary (the engine stays cfg-shape
                # agnostic): any non-empty snake_case key is a valid state namespace.
                if not isinstance(namespace_key, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", namespace_key):
                    raise RuntimeError(f"❌ {section_name} namespace must be a snake_case key: {namespace_key!r} in {path}")
                if namespace_key in merged:
                    raise RuntimeError(f"❌ duplicate {section_name} namespace {namespace_key!r}: {path} (first: {seen_sources[namespace_key]})")
                if not isinstance(entry, dict):
                    raise RuntimeError(f"❌ {section_name}.{namespace_key} must be a mapping: {path}")
                allowed = {
                    "provider", "backend_type", "bucket_name", "bucket_region",
                    "execution_identity", "selectors",
                    # Optional — how long a mutation may hold this
                    # namespace before its lock becomes breakable.
                    CtlStateBackends.MUTATION_LOCK_TTL_FIELD,
                }
                unknown = set(entry) - allowed
                if unknown:
                    raise RuntimeError(f"❌ {section_name}.{namespace_key} has unsupported keys {sorted(unknown)}: {path}")
                provider = entry.get("provider")
                backend_type = entry.get("backend_type")
                for field, value in (("provider", provider), ("backend_type", backend_type)):
                    if not isinstance(value, str) or not value.strip():
                        raise RuntimeError(f"❌ {section_name}.{namespace_key}.{field} must be a non-empty string: {path}")
                for field in ("bucket_name", "bucket_region"):
                    if not isinstance(entry.get(field), str) or not entry[field].strip():
                        raise RuntimeError(f"❌ {section_name}.{namespace_key}.{field} must be a non-empty string: {path}")
                resolved = {
                    "provider": provider.strip(),
                    "backend_type": backend_type.strip(),
                    "bucket_name": entry["bucket_name"].strip(),
                    "bucket_region": entry["bucket_region"].strip(),
                }
                # Carried and CHECKED here, because the loader builds
                # `resolved` field by field — a key added to the allowlist but not to
                # this block passes validation and is then silently dropped, which
                # looks exactly like a declaration that had no effect.
                if CtlStateBackends.MUTATION_LOCK_TTL_FIELD in entry:
                    resolved[CtlStateBackends.MUTATION_LOCK_TTL_FIELD] = CtlStateBackends.mutation_lock_ttl(entry)
                execution = entry.get("execution_identity")
                if execution is not None:
                    resolved["execution_identity"] = CtlStateBackends.validate_execution(
                        execution, label=f"{section_name}.{namespace_key}", path=path
                    )
                selectors = entry.get("selectors")
                if selectors is not None:
                    # A backend entry IS the namespace — its selectors
                    # resolve exactly one entry per invocation (item 13c collapse).
                    run_selectors.selector_requirements(
                        selectors, label=f"{section_name}.{namespace_key}.selectors", structured_only=True
                    )
                    resolved["selectors"] = selectors
                merged[namespace_key] = resolved
                seen_sources[namespace_key] = path
        # Namespaces resolve exactly one entry by selectors — reject
        # byte-identical selectors at load (before the resolve-time exactly-one guard).
        run_selectors.reject_duplicate_selectors(
            {k: v.get("selectors") for k, v in merged.items() if v.get("selectors") is not None},
            label=section_name,
        )
        return merged or None

    @staticmethod
    def resolve_namespace(
        ctl_cfg_root: Path, execution_context: dict[str, object]
    ) -> tuple[str, dict]:
        """Resolve EXACTLY ONE ctl-state namespace from the frozen execution
        context. A namespace IS a ctl_state_backends entry (item
        13c collapse): its `selectors` select it. Zero or multiple matches are hard
        errors; the selection is immutable for the whole top-level invocation and is
        recorded in run metadata by the caller. Returns (namespace_key,
        backend_entry)."""

        backends = CtlStateBackends.load(ctl_cfg_root) or {}
        if not backends:
            raise RuntimeError(f"❌ no 'ctl_state_backends' defined under: {ctl_cfg_root}")
        matches = [
            key for key, entry in backends.items()
            if entry.get("selectors") is not None
            and run_selectors.selector_matches(
                entry.get("selectors"), execution_context,
                label=f"ctl_state_backends.{key}.selectors", structured_only=True,
            )
        ]
        if len(matches) != 1:
            selectable = sorted(k for k, e in backends.items() if e.get("selectors") is not None)
            raise RuntimeError(
                f"❌ exactly one ctl-state namespace (backend with selectors) must match the "
                f"execution context, matched {len(matches)} of {selectable}"
            )
        return matches[0], backends[matches[0]]


class CtlStateAccess:
    """Reaching a ctl-state backend for ONE operation, outside a run's publication.

    Stateless: the read and maintenance paths arm a throwaway syncer per command
    and drop it, so there is nothing to hold. Publication is the opposite case and
    lives on `CtlStatePublication`.
    """

    @staticmethod
    def publication_access_mode(adapter, execution_access_mode: str) -> str:
        """The mode ctl-state publication runs under, given the run's mode.

        Publication always takes the provider's NORMAL path — escalated target access
        is about reaching the target, not about writing ctl-state. The one exception
        is a mode that resolves no execution identity at all: there is no normal path
        to fall back to, so it carries over. Both mode names come from the adapter.
        """

        if not adapter.resolves_execution_identity(execution_access_mode):
            return execution_access_mode
        return adapter.normal_execution_access_mode()

    @staticmethod
    def arm_operation(
        ctl_cfg_root: Path,
        execution_context: dict[str, object],
        ctl_state_local_root: Path,
        *,
        operation: str,
        provider_implementation_key: str,
        execution_access_modes: dict[str, str],
        provider_options: dict[str, str] | None,
        object_keys: list[str] | tuple[str, ...] = (),
        object_prefixes: list[str] | tuple[str, ...] = (),
    ):
        namespace_key, entry = CtlStateBackends.resolve_namespace(
            ctl_cfg_root, execution_context
        )
        adapter = execution_adapters.get_adapter(entry["provider"])
        adapter.validate_state_backend_entry(namespace_key, entry, ctl_cfg_root)
        bucket_name = str(
            execution_references.resolve_runtime_scalar(
                entry["bucket_name"],
                execution_context,
                label=f"ctl_state_backends.{namespace_key}.bucket_name",
            )
        )
        run_access_mode, adapter_options = execution_providers.provider_inputs(
            entry["provider"], execution_access_modes, provider_options
        )
        operation_execution = CtlStateBackends.operation_execution(
            entry,
            operation,
            namespace_key=namespace_key,
            required=adapter.resolves_execution_identity(run_access_mode),
        )
        operation_access_mode = CtlStateAccess.publication_access_mode(adapter, run_access_mode)
        credential = adapter.resolve_ctl_state_credential(
            operation_execution,
            ctl_cfg_root,
            execution_context=execution_context,
            implementation_key=provider_implementation_key,
            operation=operation,
            bucket_name=bucket_name,
            object_keys=object_keys,
            object_prefixes=object_prefixes,
            execution_access_mode=operation_access_mode,
            provider_options=adapter_options,
        )
        namespace_root = Path(ctl_state_local_root) / namespace_key
        syncer = adapter.create_state_syncer(
            namespace_root,
            bucket_name,
            str(entry["bucket_region"]),
            credential,
            namespace_root,
            required=True,
        )
        if not syncer.ensure_ready(operation):
            raise RuntimeError(f"❌ ctl-state backend {namespace_key!r} is not ready")
        # No mutation-lock check here. This is the READ path, and a
        # read neither takes the lock nor may be denied by it — checking cost an
        # object GET per query to answer a question whose only possible outcome was
        # to refuse the caller.
        return namespace_key, namespace_root, syncer

    @staticmethod
    def arm_reader(
        ctl_cfg_root: Path,
        selection: dict,
        ctl_state_local_root: Path,
        *,
        provider_implementation_key: str,
        execution_access_modes: dict[str, str],
        provider_options: dict[str, str] | None,
    ):
        return CtlStateAccess.arm_operation(
            ctl_cfg_root,
            selection["execution_context"],
            ctl_state_local_root,
            operation="read",
            provider_implementation_key=provider_implementation_key,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
        )

    @staticmethod
    def local_scope(
        ctl_cfg_root: Path, execution_context: dict[str, object], ctl_state_local_root: Path
    ) -> tuple[str, Path]:
        """`local` scope: the namespace resolves from cfg alone, so the
        local view needs no credentials and makes no bucket calls. Reads the tree
        exactly as it is — the ONLY way to see a force-skipped run, which exists
        locally and can never reach the bucket."""
        namespace_key, _ = CtlStateBackends.resolve_namespace(ctl_cfg_root, execution_context)
        return namespace_key, Path(ctl_state_local_root) / namespace_key


class PendingSyncQueue:
    """The on-disk manifest a run writes when the backend cannot take its record.

    Stateless: a manifest is addressed by the run dir or namespace root handed in,
    and the file itself carries everything a later drain needs. The queue survives
    the process, so holding any of it in memory would be a second source of truth.
    """

    @staticmethod
    def manifest_path(run_dir: Path) -> tuple[Path, Path]:
        metadata = state_run_store.load_run_metadata(run_dir)
        local_root = Path(metadata["ctl_state_local_root"])
        locator = list(metadata.get("ctl_state_locator") or [])
        namespace_root = local_root.joinpath(*locator)
        top_level_run_id = (
            metadata.get("fan_out_run_id")
            or metadata.get("parent_workflow_run_id")
            or metadata.get("run_id")
            or Path(run_dir).name
        )
        manifest_dir = namespace_root / "_pending_sync" / str(top_level_run_id)
        return namespace_root, manifest_dir / "manifest.yaml"

    @staticmethod
    def enqueue(
        run_dir: Path,
        pointer_path: Path | None,
        *,
        dependencies: list[str] | None = None,
    ) -> Path:
        namespace_root, manifest_path = PendingSyncQueue.manifest_path(run_dir)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = manifest_path.parent / ".lock"
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            manifest = kernel_yaml_io.load_yaml(manifest_path) if manifest_path.is_file() else {}
            if not isinstance(manifest, dict):
                raise RuntimeError(f"❌ pending-sync manifest must be a mapping: {manifest_path}")
            metadata = state_run_store.load_run_metadata(run_dir)
            run_rel = Path(run_dir).resolve().relative_to(namespace_root.resolve()).as_posix()
            instance_dir = state_run_store.ctl_state_dir_from_run_dir(run_dir)
            identity_path = instance_dir / "identity.yaml"
            object_paths = [
                path
                for path in sorted(Path(run_dir).rglob("*"))
                if path.is_file()
            ]
            if identity_path.is_file():
                object_paths.append(identity_path)
            if pointer_path is not None:
                object_paths.append(pointer_path)
            hashes = {
                path.resolve().relative_to(namespace_root.resolve()).as_posix(): kernel_paths._sha256_file(path)
                for path in dict.fromkeys(object_paths)
            }
            entry = {
                "run_id": Path(run_dir).name,
                "run_type": metadata.get("run_type"),
                "owner_address": metadata.get("instance_address") or metadata.get("result_name"),
                "run_path": run_rel,
                "identity_path": (
                    identity_path.resolve().relative_to(namespace_root.resolve()).as_posix()
                    if identity_path.is_file()
                    else None
                ),
                "pointer_path": (
                    pointer_path.resolve().relative_to(namespace_root.resolve()).as_posix()
                    if pointer_path is not None
                    else None
                ),
                "dependencies": list(dependencies or []),
                "objects": hashes,
                "status": "pending",
            }
            entries = [
                item for item in (manifest.get("entries") or [])
                if item.get("run_path") != run_rel
            ]
            entries.append(entry)
            kernel_yaml_io.write_yaml_file(
                manifest_path,
                {
                    "version": 1,
                    "namespace": (metadata.get("ctl_state_namespace") or (metadata.get("ctl_state_locator") or [None])[0]),
                    "top_level_run_id": manifest_path.parent.name,
                    "status": "pending",
                    "updated_at": kernel_ids.utc_timestamp(),
                    "entries": entries,
                },
            )
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        return manifest_path

    @staticmethod
    def validate_entry(namespace_root: Path, entry: dict) -> None:
        for relative_path, expected_sha in (entry.get("objects") or {}).items():
            path = namespace_root / relative_path
            if not path.is_file():
                raise RuntimeError(f"❌ pending ctl-state object is missing: {path}")
            actual = kernel_paths._sha256_file(path)
            if actual != expected_sha:
                raise RuntimeError(
                    f"❌ pending ctl-state object hash changed: {relative_path}"
                )

    @staticmethod
    def manifest_paths(local_root: Path, namespace_key: str) -> list[Path]:
        root = Path(local_root) / namespace_key / "_pending_sync"
        return sorted(root.glob("*/manifest.yaml")) if root.is_dir() else []


class CtlStatePublication:
    """The one armed publication a run publishes its records through.

    An INSTANCE, because arming is a state transition that a whole cluster of
    operations then depends on: `syncer` (armed or not), `sync_config` (what to
    re-arm from), `defer_config` (whether queuing is even allowed) and `note` (what
    the run records about its own publication) are read and replaced together, and
    every one of them belongs to the run dir the cluster is re-arming.

    Re-arming per run dir is deliberate, not incidental: a credential is scoped to
    the exact objects one run publishes, so publishing a different run needs a
    different credential.
    """

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.syncer = None
        self.sync_config: dict | None = None
        self.defer_config: dict | None = None
        self.note: dict[str, str] = {"mode": "disabled"}

    def _build_config(
        self,
        ctl_cfg_root: Path,
        namespace_key: str,
        entry: dict,
        execution_context: dict[str, object],
        run_dir: Path,
        *,
        execution_access_modes: dict[str, str],
        provider_options: dict[str, str] | None,
        provider_implementation_key: str,
    ) -> dict:
        metadata = state_run_store.load_run_metadata(run_dir)
        results_root_value = metadata.get("ctl_state_local_root")
        if not isinstance(results_root_value, str) or not results_root_value:
            raise RuntimeError("❌ run metadata is missing ctl_state_local_root")
        locator = [namespace_key]
        if metadata.get("ctl_state_locator") != locator:
            raise RuntimeError(
                f"❌ run dirs use locator {metadata.get('ctl_state_locator')!r}, "
                f"but ctl-state namespace resolves to {locator!r}"
            )
        bucket_name = str(
            execution_references.resolve_runtime_scalar(
                entry["bucket_name"],
                execution_context,
                label=f"ctl_state_backends.{namespace_key}.bucket_name",
            )
        )
        return {
            "ctl_cfg_root": Path(ctl_cfg_root),
            "namespace_key": namespace_key,
            "entry": entry,
            "execution_context": execution_context,
            "run_dir": Path(run_dir),
            "results_root": Path(results_root_value).joinpath(*locator),
            "bucket_name": bucket_name,
            "bucket_region": str(entry["bucket_region"]),
            "execution_access_modes": execution_access_modes,
            "provider_options": provider_options,
            "provider_implementation_key": provider_implementation_key,
        }

    @staticmethod
    def _run_access_scope(config: dict) -> tuple[list[str], list[str]]:
        """Return exact object keys and immutable run prefixes for one publication."""
        results_root = Path(config["results_root"]).resolve()
        run_dir = Path(config["run_dir"]).resolve()
        run_prefix = run_dir.relative_to(results_root).as_posix()
        instance_dir = state_run_store.ctl_state_dir_from_run_dir(run_dir).resolve()
        instance_prefix = instance_dir.relative_to(results_root).as_posix()
        metadata = state_run_store.load_run_metadata(run_dir)
        action = str(metadata.get("action") or "")
        instance_prefixes = [instance_prefix]
        for address in metadata.get("target_addresses") or []:
            instance_prefixes.append(
                ctl_state_target_address_prefix(action, str(address))
            )
        keys = [
            key
            for prefix in dict.fromkeys(instance_prefixes)
            for key in (
                f"{prefix}/identity.yaml",
                *(
                    f"{prefix}/committed/{group}.yaml"
                    for group in run_actions.RESULT_GROUPS
                ),
            )
        ]
        return sorted(set(keys)), [run_prefix]

    def _arm(self, config: dict, *, tolerate_not_ready: bool) -> bool:
        entry = config["entry"]
        adapter = execution_adapters.get_adapter(entry["provider"])
        run_access_mode, adapter_options = execution_providers.provider_inputs(
            entry["provider"], config["execution_access_modes"], config["provider_options"]
        )
        operation_execution = CtlStateBackends.operation_execution(
            entry,
            "sync",
            namespace_key=config["namespace_key"],
            required=adapter.resolves_execution_identity(run_access_mode),
        )
        # Escalated target access may be needed to reach the target, but ctl-state
        # publication takes the provider's normal path as soon as it can.
        sync_access_mode = CtlStateAccess.publication_access_mode(adapter, run_access_mode)
        try:
            object_keys, object_prefixes = self._run_access_scope(config)
            credential = adapter.resolve_ctl_state_credential(
                operation_execution,
                config["ctl_cfg_root"],
                execution_context=config["execution_context"],
                implementation_key=config["provider_implementation_key"],
                operation="sync",
                bucket_name=config["bucket_name"],
                object_keys=object_keys,
                object_prefixes=object_prefixes,
                execution_access_mode=sync_access_mode,
                provider_options=adapter_options,
            )
        except Exception as error:
            if not tolerate_not_ready:
                raise
            self.note = {
                "mode": "deferred",
                "reason": "synchronizer_not_ready",
                "detail": execution_run_context.credential_free_preflight_failure_reason(error),
            }
            return False
        syncer = adapter.create_state_syncer(
            config["results_root"],
            config["bucket_name"],
            config["bucket_region"],
            credential,
            config["run_dir"],
            required=not tolerate_not_ready,
        )
        if not syncer.ensure_ready("ctl-state publication readiness"):
            if not tolerate_not_ready:
                raise RuntimeError(
                    f"❌ ctl-state backend {config['bucket_name']!r} is not ready"
                )
            self.note = {
                "mode": "deferred",
                "reason": "backend_absent",
            }
            return False
        self.syncer = syncer
        self.note = syncer.summary()
        return True

    def configure(
        self,
        ctl_cfg_root: Path,
        ctl_profile: str,
        namespace_key: str | None,
        execution_context: dict[str, object],
        run_dir: Path,
        *,
        agreed_defer_ctl_state_backend_sync: bool = False,
        force_skip_ctl_state_backend_sync: bool = False,
        provisions_ctl_state_backend: bool = False,
        selected_graph_provisions_ctl_state_backend: bool = False,
        backend_absence_confirmed: bool = False,
        execution_access_modes: dict[str, str] | None = None,
        provider_options: dict[str, str] | None = None,
        provider_implementation_key: str = "local",
    ) -> dict[str, str] | None:
        """

        arm namespace publication or establish an explicitly proven defer queue."""

        del ctl_profile, provisions_ctl_state_backend
        self.reset()

        backends = CtlStateBackends.load(ctl_cfg_root)
        if backends is None:
            if agreed_defer_ctl_state_backend_sync or force_skip_ctl_state_backend_sync:
                logging.info("ctl-state sync option has no effect: no backend registry")
            return None
        if force_skip_ctl_state_backend_sync:
            self.note = {"mode": "skipped", "reason": "force_skip"}
            return None

        if namespace_key is None:
            namespace_key, _ = CtlStateBackends.resolve_namespace(ctl_cfg_root, execution_context)
        entry = backends[namespace_key]
        adapter = execution_adapters.get_adapter(entry["provider"])
        adapter.validate_state_backend_entry(namespace_key, entry, ctl_cfg_root)
        config = self._build_config(
            ctl_cfg_root,
            namespace_key,
            entry,
            execution_context,
            run_dir,
            execution_access_modes=execution_access_modes,
            provider_options=provider_options,
            provider_implementation_key=provider_implementation_key,
        )
        self.sync_config = config

        if agreed_defer_ctl_state_backend_sync:
            if not selected_graph_provisions_ctl_state_backend:
                raise RuntimeError(
                    "❌ agreed ctl-state defer is valid only when the complete selected "
                    "graph contains exactly one backend provisioner"
                )
            if not backend_absence_confirmed:
                raise RuntimeError(
                    "❌ agreed ctl-state defer is not applicable: the provider did not "
                    "confirm that the selected backend was absent at invocation start"
                )
            self.defer_config = config
            if not self._arm(config, tolerate_not_ready=True):
                return {
                    "namespace": namespace_key,
                    "bucket_name": config["bucket_name"],
                    "bucket_region": config["bucket_region"],
                }
        else:
            self._arm(config, tolerate_not_ready=False)

        syncer = self.syncer
        if syncer is None:
            raise RuntimeError("❌ ctl-state syncer was not armed")
        instance_prefix = state_run_store.ctl_state_dir_from_run_dir(run_dir).resolve().relative_to(
            syncer.results_root
        ).as_posix()
        metadata = state_run_store.load_run_metadata(run_dir)
        child_prefixes = [
            ctl_state_target_address_prefix(
                str(metadata.get("action") or ""), str(address)
            )
            for address in (metadata.get("target_addresses") or [])
        ]
        syncer.hydrate_instance(
            instance_prefix,
            child_prefixes,
            committed_groups=run_actions.RESULT_GROUPS,
        )
        state_run_store.enforce_mutation_lock(
            syncer,
            action=str(metadata.get("action") or ""),
            run_id=str(metadata.get("run_id") or Path(run_dir).name),
            parent_run_id=metadata.get("parent_workflow_run_id"),
            # The namespace's own TTL, because how long a mutation can
            # run is a property of the estate that namespace holds.
            ttl_seconds=CtlStateBackends.mutation_lock_ttl(config.get("entry")),
        )
        syncer.push("run started")
        self.note = syncer.summary()
        return {
            "namespace_key": namespace_key,
            "bucket_name": config["bucket_name"],
            "bucket_region": config["bucket_region"],
        }

    def drain_pending(self) -> int:
        """

        drain pending manifests with one fresh, run-scoped credential per entry."""

        syncer = self.syncer
        if syncer is None:
            return 0
        base_config = self.defer_config or self.sync_config
        pending_root = syncer.results_root / "_pending_sync"
        if not pending_root.is_dir():
            return 0
        drained = 0
        for manifest_path in sorted(pending_root.glob("*/manifest.yaml")):
            lock_path = manifest_path.parent / ".lock"
            with lock_path.open("a+", encoding="utf-8") as lock_handle:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                manifest = kernel_yaml_io.load_yaml(manifest_path) or {}
                entries = list(manifest.get("entries") or [])
                for entry in entries:
                    active_syncer = self.syncer
                    if active_syncer is None:
                        raise RuntimeError("❌ ctl-state syncer disappeared during catch-up")
                    PendingSyncQueue.validate_entry(active_syncer.results_root, entry)
                    if base_config is not None:
                        entry_config = dict(base_config)
                        entry_config["run_dir"] = active_syncer.results_root / entry["run_path"]
                        self._arm(entry_config, tolerate_not_ready=False)
                        active_syncer = self.syncer
                    identity_rel = entry.get("identity_path")
                    if identity_rel:
                        active_syncer.publish_identity(active_syncer.results_root / identity_rel)
                    active_syncer.push_run(
                        active_syncer.results_root / entry["run_path"],
                        f"deferred catch-up {entry['run_id']}",
                    )
                priority = {"target": 0, "workflow": 1}
                for entry in sorted(
                    entries,
                    key=lambda item: (
                        priority.get(str(item.get("run_type")), 2),
                        str(item.get("run_id")),
                    ),
                ):
                    pointer_rel = entry.get("pointer_path")
                    if not pointer_rel:
                        continue
                    active_syncer = self.syncer
                    if base_config is not None:
                        entry_config = dict(base_config)
                        entry_config["run_dir"] = active_syncer.results_root / entry["run_path"]
                        self._arm(entry_config, tolerate_not_ready=False)
                        active_syncer = self.syncer
                    active_syncer.publish_committed_pointer(
                        active_syncer.results_root / pointer_rel
                    )
                manifest_path.unlink()
                drained += len(entries)
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            lock_path.unlink(missing_ok=True)
            with contextlib.suppress(OSError):
                manifest_path.parent.rmdir()
        with contextlib.suppress(OSError):
            pending_root.rmdir()
        if base_config is not None:
            self._arm(base_config, tolerate_not_ready=False)
        return drained

    def retry_deferred(self) -> bool:
        if self.defer_config is None:
            return self.syncer is not None
        if self.syncer is None and not self._arm(
            self.defer_config, tolerate_not_ready=True
        ):
            return False
        drained = self.drain_pending()
        self.note = self.syncer.summary()
        if drained:
            logging.info("drained %d deferred ctl-state run(s)", drained)
        return True

    def publish_or_queue_run(
        self,
        run_dir: Path,
        pointer_path: Path | None,
        *,
        reason: str,
        dependencies: list[str] | None = None,
    ) -> None:
        if self.sync_config is not None and self.syncer is not None:
            publication_config = dict(self.sync_config)
            publication_config["run_dir"] = Path(run_dir)
            self._arm(publication_config, tolerate_not_ready=False)
        if self.syncer is not None:
            instance_identity = state_run_store.ctl_state_dir_from_run_dir(run_dir) / "identity.yaml"
            if instance_identity.is_file():
                self.syncer.publish_identity(instance_identity)
            self.syncer.push_run(run_dir, reason)
            if pointer_path is not None:
                self.syncer.publish_committed_pointer(pointer_path)
            return
        if self.defer_config is None:
            return
        PendingSyncQueue.enqueue(run_dir, pointer_path, dependencies=dependencies)
        self.retry_deferred()

    def push(self, reason: str) -> None:
        if self.syncer is not None:
            self.syncer.push(reason)

    def publish_committed(self, pointer_path: Path) -> None:
        if self.syncer is not None:
            self.syncer.publish_committed_pointer(pointer_path)

    def summary(self) -> dict[str, str]:
        if self.syncer is not None:
            return self.syncer.summary()
        return dict(self.note)


# One armed publication per top-level invocation, so the callers that publish
# (pipeline, lifecycle, maintenance) reach it by name rather than threading it
# through every frame between the run's start and the record it writes.
PUBLICATION = CtlStatePublication()
