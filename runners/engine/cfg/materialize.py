"""Fetching declared sources and staging them for a run.

Everything a run executes is materialized from a pin — cfg, step utilities,
target modules, tooling — so that what ran is reconstructible from the record
rather than from whatever the working copy held."""

import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

from engine.cfg import layout as cfg_layout
from engine.cfg import resources as cfg_resources
from engine.cfg import validate as cfg_validate
from engine.execution import providers as execution_providers
from engine.execution import references as execution_references
from engine.execution import run_context as execution_run_context
from engine.kernel import git as kernel_git
from engine.kernel import ids as kernel_ids
from engine.kernel import paths as kernel_paths
from engine.kernel import yaml_io as kernel_yaml_io
from engine.kernel.git import write_git_meta_to_file
from engine.run import policy as run_policy
from engine.state import run_store as state_run_store
from engine.units import step as units_step

ADAPTER_DIR = "atlas_ctl_adapter"


# Target run box images (step.yaml runtime.image). CTL owns how each is built/run.
STEP_IMAGES = ("infra", "ops")


def load_cfg_sources(ctl_cfg_root: Path) -> dict[str, dict[str, object]]:
    """

    load the ctl-owned one-to-one plt and guardrail source bindings."""

    entries = cfg_resources.collect_resource(ctl_cfg_root, "cfg_sources")
    expected = set(cfg_layout.CFG_SOURCE_KEYS)
    actual = set(entries)
    if actual != expected:
        raise RuntimeError(
            f"❌ cfg_sources must define exactly {sorted(expected)}; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    normalized: dict[str, dict[str, object]] = {}
    for key in cfg_layout.CFG_SOURCE_KEYS:
        raw = entries[key]
        label = f"cfg_sources.{key}"
        if not isinstance(raw, dict) or not raw:
            raise RuntimeError(f"❌ {label} must be a non-empty mapping")
        keys = set(raw)
        provider = raw.get("provider") if key == "plt" else None
        source_keys = keys - ({"provider"} if key == "plt" else set())
        if provider is not None and (not isinstance(provider, str) or not provider.strip()):
            raise RuntimeError(f"❌ {label}.provider must be a non-empty string when declared")
        if source_keys == {"repo_path"}:
            value = raw["repo_path"]
            if not isinstance(value, str) or not value.strip():
                raise RuntimeError(f"❌ {label}.repo_path must be a non-empty string")
            normalized[key] = {"repo_path": value.strip()}
            if provider is not None:
                normalized[key]["provider"] = provider.strip()
            continue
        if source_keys != {"repo_url", "ref"}:
            raise RuntimeError(
                f"❌ {label} must contain either repo_path only or exactly repo_url + ref"
            )
        url, ref = raw["repo_url"], raw["ref"]
        if not isinstance(url, str) or not url.strip():
            raise RuntimeError(f"❌ {label}.repo_url must be a non-empty string")
        if not isinstance(ref, dict) or len(ref) != 1:
            raise RuntimeError(f"❌ {label}.ref must contain exactly one of branch or commit")
        kind, value = next(iter(ref.items()))
        if kind not in {"branch", "commit"} or not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"❌ {label}.ref must contain a non-empty branch or commit")
        normalized[key] = {"repo_url": url.strip(), "ref": {kind: value.strip()}}
        if provider is not None:
            normalized[key]["provider"] = provider.strip()
    return normalized


def materialize_cfg_sources(
    ctl_cfg_root: Path,
    *,
    ref_policy: str,
    run_cfg_dir: Path,
    token: str | None = None,
) -> dict[str, Path]:
    """

    resolve local companion roots or clone their ctl-bound remote refs."""

    sources = load_cfg_sources(ctl_cfg_root)
    cfg_validate.CommitPinning(ref_policy).check_cfg_sources(sources)
    run_cfg_dir.mkdir(parents=True, exist_ok=True)
    roots = {}
    for key in cfg_layout.CFG_SOURCE_KEYS:
        entry = sources[key]
        if "repo_path" in entry:
            root = Path(str(entry["repo_path"])).expanduser()
            root = (ctl_cfg_root / root).resolve() if not root.is_absolute() else root.resolve()
            if not root.is_dir():
                raise RuntimeError(f"❌ cfg_sources.{key}.repo_path not found: {root}")
            roots[key] = root
            continue
        ref = entry["ref"]
        assert isinstance(ref, dict)
        root = (run_cfg_dir / f"{key}_cfg").resolve()
        try:
            root.relative_to(run_cfg_dir.resolve())
        except ValueError as exc:
            raise RuntimeError(f"❌ cfg source destination escapes run cfg dir: {root}") from exc
        if root.exists():
            shutil.rmtree(root)
        kernel_git.git_clone(
            str(entry["repo_url"]), ref.get("branch"), ref.get("commit"), root, token
        )
        roots[key] = root
    return roots


# Materialized imports live for exactly one discovery pass. The next
# pass frees the previous one, so a long-lived process holds at most one run's
# worth rather than accumulating them.
_MATERIALIZED_IMPORT_DIRS: list[tempfile.TemporaryDirectory] = []


def _fresh_materialization_workspace() -> Path:
    workspace = tempfile.TemporaryDirectory(prefix="atlas-cfg-preset-")
    _MATERIALIZED_IMPORT_DIRS.append(workspace)
    return Path(workspace.name)


def _release_materialized_imports() -> None:
    while _MATERIALIZED_IMPORT_DIRS:
        _MATERIALIZED_IMPORT_DIRS.pop().cleanup()
    kernel_paths._MATERIALIZED_IMPORT_LABELS.clear()


def write_ctl_cfg_snapshot(
    run_dir: Path,
    *,
    ctl_profile: str,
    ctl_profile_policy_cfg: dict,
    action: str,
    workflow_cfg: dict,
    action_cfg: dict,
    active_target_runs: dict,
    refs: dict,
    execution_context: dict[str, object],
) -> Path:
    """Write a resolved snapshot of the ctl cfg that drove the run to
    run_dir/cfg/ctl/, so the run is self-describing next to cfg/plt/. Vars are
    resolved against the execution context; active_target_runs is already resolved."""
    ctl_dir = run_dir / "cfg" / "ctl"
    if ctl_dir.exists():
        shutil.rmtree(ctl_dir)
    ctl_dir.mkdir(parents=True)
    kernel_yaml_io.write_yaml_file(
        ctl_dir / "profile.yaml", {"ctl_profile": ctl_profile, "policy": ctl_profile_policy_cfg}
    )
    kernel_yaml_io.write_yaml_file(
        ctl_dir / "workflow.yaml",
        execution_run_context.resolve_ctl_structure(
            workflow_cfg, execution_context, label="workflow"
        ),
    )
    kernel_yaml_io.write_yaml_file(
        ctl_dir / "action.yaml",
        execution_run_context.resolve_ctl_structure(
            action_cfg, execution_context, label=f"action.{action}"
        ),
    )
    kernel_yaml_io.write_yaml_file(ctl_dir / "active_target_runs.yaml", active_target_runs)
    kernel_yaml_io.write_yaml_file(
        ctl_dir / "refs.yaml",
        execution_run_context.resolve_ctl_structure(refs, execution_context, label="refs"),
    )
    logging.info("Wrote resolved ctl cfg snapshot: %s", ctl_dir)
    return ctl_dir


def write_git_metas(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    artifacts_dir: Path,
) -> None:
    """Write ctl, plt, guardrail, and orchestrator git metadata."""
    # ctl_cfg_git_meta
    write_git_meta_to_file(
        git_dir=ctl_cfg_root,
        dest_dir=artifacts_dir,
        filename="piepeline_orchestrator_cfg_git_meta.yaml",
        generator=kernel_ids.SERVICE_ID,
    )

    # orchestrator_git_meta
    write_git_meta_to_file(
        git_dir=os.getcwd(),
        dest_dir=artifacts_dir,
        filename="piepeline_orchestrator_git_meta.yaml",
        generator=kernel_ids.SERVICE_ID,
    )

    # plt_cfg_git_meta
    write_git_meta_to_file(
        git_dir=plt_cfg_root,
        dest_dir=artifacts_dir,
        filename="plt_cfg_git_meta.yaml",
        generator=kernel_ids.SERVICE_ID,
    )
    write_git_meta_to_file(
        git_dir=guardrails_cfg_root,
        dest_dir=artifacts_dir,
        filename="guardrails_cfg_git_meta.yaml",
        generator=kernel_ids.SERVICE_ID,
    )


def _step_utils_module(name: str):
    """

    import a step_utils/ctl python module by file path (shared primitives:
    Resolver, merge_values, cfg-entry refs). The module stays self-contained in
    step_utils because it also executes inside target_run containers."""

    import importlib.util

    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, source_step_utils_dir() / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def run_cfg_distribution(
    pipeline_run_cfg_path: Path, plt_targets_dir_path: Path, run_type: str = "workflow"
) -> Path:
    """Project each target_run's declared cfg keys out of ITS OWN rendered tree.

    the rendered tree lives under the target
    (`plt/targets/<target>/rendered`), not once per run, so this reads only what
    that target derived for itself.
    """

    cfg = kernel_yaml_io.load_yaml(pipeline_run_cfg_path) or {}
    target_runs = cfg.get("target_runs") or {}
    if not isinstance(target_runs, dict):
        raise RuntimeError("pipeline_run_cfg.yaml target_runs must be a mapping")
    plt_targets_dir_path.mkdir(parents=True, exist_ok=True)

    brc = _step_utils_module("build_runtime_cfg")

    for target_run_name, target_run_cfg in target_runs.items():
        if not isinstance(target_run_cfg, dict):
            raise RuntimeError(f"Target run {target_run_name!r} config must be a mapping")
        domains = target_run_cfg.get("domains") or []
        cfg_keys = target_run_cfg.get("cfg_keys") or {}
        if not domains:
            continue
        if not isinstance(cfg_keys, dict):
            raise RuntimeError(f"Target run {target_run_name!r} cfg_keys must be a mapping")

        view_dir = (
            plt_targets_dir_path if run_type == "target" else plt_targets_dir_path / target_run_name
        )
        rendered_root = view_dir / "rendered"
        target_input_dir = view_dir / "input"
        target_input_dir.mkdir(parents=True, exist_ok=True)

        for domain in domains:
            domain = str(domain).strip().strip("/")
            scope_root = rendered_root / domain
            # Assertion 4: a declared domain that matches NO active scope would
            # otherwise deliver a silently empty view.
            if not scope_root.is_dir():
                raise RuntimeError(
                    f"❌ target_run {target_run_name!r} declares domain {domain!r}, but no active "
                    f"cfg scope publishes into /{domain} for this execution context"
                )
            merged: dict = {}
            for path in sorted(scope_root.rglob("*.yaml")):
                merged = brc.merge_values(merged, brc.load_yaml_mapping(path))
            entries = cfg_keys.get(domain)
            if not entries:
                raise RuntimeError(
                    f"❌ target_run {target_run_name!r} declares domain {domain!r} with no cfg_keys"
                )
            projected = execution_references.project_cfg_keys(
                merged,
                list(entries),
                label=f"target_run {target_run_name!r} domain {domain!r}",
            )
            kernel_yaml_io.write_yaml_file(target_input_dir / f"{domain}.yaml", projected)

    logging.info("Prepared target_run input cfg views under %s", plt_targets_dir_path)
    return plt_targets_dir_path


def materialize_target_modules(
    target_run_id: str, target_run: dict, repo_path: Path, secret_store=None
) -> None:
    """

    populate target_run-local child modules before setup runs."""

    modules = target_run.get("modules") or {}
    if not modules:
        return

    for module_name, module_cfg in modules.items():
        dest_path = repo_path / module_cfg["dest"]
        try:
            dest_path.relative_to(repo_path)
        except ValueError as exc:
            raise RuntimeError(
                f"Target run '{target_run_id}' module '{module_name}' dest escapes the target_run repo: {module_cfg['dest']}"
            ) from exc

        if dest_path.exists() or dest_path.is_symlink():
            kernel_paths._remove_path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if "repo_path" in module_cfg:
            module_src = Path(module_cfg["repo_path"]).expanduser()
            if not module_src.is_dir():
                raise RuntimeError(
                    f"Target run '{target_run_id}' module '{module_name}' repo_path not found: {module_src}"
                )
            # Copy the local working tree snapshot so Dockerized target_run runners can read it.
            shutil.copytree(module_src, dest_path, symlinks=True)
        else:
            kernel_git.git_clone(
                repo_url=module_cfg["repo_url"],
                branch=module_cfg["branch"],
                commit=module_cfg["commit"],
                dest=dest_path,
                token=(
                    secret_store.resolve(
                        module_cfg["secret_key"],
                        label=f"target run {target_run_id!r} module {module_name!r}",
                    )
                    if secret_store is not None and module_cfg.get("secret_key")
                    else None
                ),
            )


def source_step_utils_dir() -> Path:
    utils_dir = kernel_paths.ctl_utils_root() / "step_utils"
    if not utils_dir.is_dir():
        raise RuntimeError(f"❌ step utils source dir not found: {utils_dir}")
    return utils_dir


def materialize_step_utils(run_dir: Path) -> Path:
    """Copy the ctl-owned target_run support scripts into this run's step_utils area.

    Rule: step_utils/ctl holds only files consumed by target_runs (host wrappers,
    in-container setup, the per-target_run resolver, access assert, dockerfiles).
    """
    utils_dir = run_dir / "step_utils" / "ctl"
    if utils_dir.is_dir():
        return utils_dir
    utils_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        source_step_utils_dir(),
        utils_dir,
        symlinks=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return utils_dir


def rebind_step_credentials(
    step_providers: list[str],
    *,
    target_run_id: str,
    target_run: dict,
    step_env: dict[str, str],
    provider_adapter,
    provider_catalogs: dict,
    execution_context: dict,
    provider_implementation_key: str,
    execution_access_modes: dict[str, str] | None,
    provider_options: dict[str, str] | None,
) -> None:
    """Acquire a fresh session for ONE step (`--credential-refresh per_step`).

    The CADENCE is the engine's: it owns the step loop and is the only thing that
    knows a sequence is long. The ACQUISITION stays the provider's — this calls
    the same binding the target repo was prepared with, so the engine never
    learns what a session is.

    Scoped by the step's declared `providers`, so a step re-acquires only what it
    said it uses. A step declaring none is left alone.

    The step contract is untouched: a step still RECEIVES environment values and
    never obtains them. Each step is a new container, so a fresh binding is simply
    a different environment — nothing reaches into a running step.
    """

    if provider_adapter is None or not step_providers:
        return
    if provider_adapter.PROVIDER_NAME not in step_providers:
        return
    adapter_access_mode, adapter_options = execution_providers.provider_inputs(
        provider_adapter.PROVIDER_NAME, execution_access_modes, provider_options
    )
    provider_adapter.materialize_target_binding(
        target_run_id,
        target_run,
        step_env,
        provider_catalogs,
        execution_context=execution_context,
        implementation_key=provider_implementation_key,
        execution_access_mode=adapter_access_mode,
        provider_options=adapter_options,
    )


def prepare_target_repo(
    target_run_id: str,
    target_run: dict,
    run_dir: Path,
    tooling_env: dict[str, str],
    provider_adapter=None,
    provider_catalogs: dict | None = None,
    execution_context: dict[str, object] | None = None,
    provider_implementation_key: str | None = None,
    execution_access_modes: dict[str, str] | None = None,
    provider_options: dict[str, str] | None = None,
    secret_store=None,
) -> tuple[Path, dict[str, str]]:
    """Clone/copy a target_run repo, materialize child modules, prepare its env.

    `secret_store` resolves the declared secret that READS each repository. It is
    passed rather than built here so the registry is read once per run, and so a
    local-path target run needs no store at all."""

    workspace = state_run_store.run_workspace_dir(run_dir)
    if workspace is None:
        raise RuntimeError(
            "❌ cannot materialize a target_run workspace: the run records no ctl_state_local_root"
        )
    repo_path = workspace / target_run_id
    if os.path.exists(repo_path):
        shutil.rmtree(repo_path)

    if "repo_path" in target_run:
        repo_path_value = target_run["repo_path"]
        if not repo_path_value:
            raise RuntimeError(f"Target run '{target_run_id}' has empty repo_path")
        repo_src = Path(repo_path_value).expanduser()
        if not repo_src.is_dir():
            raise RuntimeError(f"Target run '{target_run_id}' repo_path not found: {repo_src}")
        shutil.copytree(repo_src, repo_path, symlinks=True)
    else:
        kernel_git.git_clone(
            repo_url=target_run["repo_url"],
            branch=target_run["branch"],
            commit=target_run["commit"],
            dest=repo_path,
            token=secret_store.resolve(
                target_run["secret_key"],
                label=f"target run {target_run_id!r}",
            ),
        )

    materialize_target_modules(target_run_id, target_run, repo_path, secret_store)

    target_env = os.environ.copy()
    target_env.update(tooling_env)
    target_env["ATLAS_STEP_UTILS_DIR"] = str(materialize_step_utils(run_dir).parent)
    if provider_adapter is not None:
        if (
            provider_catalogs is None
            or execution_context is None
            or provider_implementation_key is None
        ):
            raise RuntimeError("❌ incomplete provider inputs for target_run preparation")
        adapter_access_mode, adapter_options = execution_providers.provider_inputs(
            provider_adapter.PROVIDER_NAME, execution_access_modes, provider_options
        )
        provider_adapter.materialize_target_binding(
            target_run_id,
            target_run,
            target_env,
            provider_catalogs,
            execution_context=execution_context,
            implementation_key=provider_implementation_key,
            execution_access_mode=adapter_access_mode,
            provider_options=adapter_options,
        )
    return repo_path, target_env


def _repo_local_active_steps(
    action_manifest: dict, active_ids: list[str], repo_root: Path, action: str | None = None
) -> list["units_step.Step"]:
    active: list[units_step.Step] = []
    for step_id in active_ids:
        entry = action_manifest.get(step_id)
        if not isinstance(entry, dict):
            raise RuntimeError(f"Step {step_id!r} not declared in manifest")
        step_path = entry.get("path")
        if not isinstance(step_path, str) or not step_path:
            raise RuntimeError(f"Step {step_id!r} manifest entry must define a non-empty path")

        step_meta_path = repo_root / step_path / "step.yaml"
        if not step_meta_path.is_file():
            raise RuntimeError(f"Step metadata not found: {step_meta_path}")
        # The manifest's action grouping is the single source for what a
        # step does, so nothing re-declares it. What the grouping cannot state is
        # that the PATH agrees with it — an entry filed under one action may point
        # at another action's directory — so the path is checked against the
        # action that reached it.
        if action is not None:
            expected_prefix = f"steps/{action}/"
            if expected_prefix not in f"{step_path}/":
                raise RuntimeError(
                    f"❌ step {step_id!r} is declared under action {action!r} but its "
                    f"path is {step_path!r}; a step's path must sit under "
                    f"{expected_prefix!r}"
                )
        step_meta = kernel_yaml_io.load_yaml(step_meta_path) or {}
        if "plt" in step_meta:
            raise RuntimeError(
                f"❌ source step metadata must not declare plt; providers materialize "
                f"the target source before universal step execution: {step_meta_path}"
            )
        runtime_cfg = step_meta.get("runtime") or {}
        if not isinstance(runtime_cfg, dict):
            raise RuntimeError(f"Step metadata runtime must be a mapping: {step_meta_path}")
        values_json = runtime_cfg.get("values_json", True)
        env_sh = runtime_cfg.get("env_sh", True)
        if not isinstance(values_json, bool) or not isinstance(env_sh, bool):
            raise RuntimeError(f"Step metadata runtime flags must be booleans: {step_meta_path}")
        # the step declares its BOX (image + docker capability), CTL owns
        # how the box is run. image is required; docker_build defaults false.
        image = runtime_cfg.get("image")
        if not isinstance(image, str) or image not in STEP_IMAGES:
            raise RuntimeError(
                f"Step metadata runtime.image must be one of {sorted(STEP_IMAGES)}: {step_meta_path}"
            )
        docker_build = runtime_cfg.get("docker_build", False)
        if not isinstance(docker_build, bool):
            raise RuntimeError(
                f"Step metadata runtime.docker_build must be a boolean: {step_meta_path}"
            )
        supported_execution_runtime_modes = run_policy.step_supported_execution_runtime_modes(
            runtime_cfg, label=str(step_meta_path)
        )
        # The STEP is the true consumer (its root declares the
        # variables), so it declares content keys, not files.
        # A step declares PURE CONTENT KEYS. `cfg_key_sets` is a
        # CTL-cfg authoring convenience; a source repo cannot resolve a ctl
        # catalog entry, and depending on one would recreate the cross-repo
        # coupling this phase removed. The step's contract is its own root's
        # variables, spelled out.
        if step_meta.get("cfg_key_sets"):
            raise RuntimeError(
                f"Step metadata must not use cfg_key_sets — a step declares content keys, "
                f"and cfg_key_sets is a CTL-cfg catalog the source repo cannot resolve: {step_meta_path}"
            )
        # A step declares the providers it uses. The target is the CEILING, the
        # step is the SIGNATURE: without this, every step in a multi-provider
        # target receives every credential set — a silent over-grant on the one
        # path that must not widen by default.
        step_providers = step_meta.get("providers")
        if (
            not isinstance(step_providers, list)
            or not step_providers
            or not all(isinstance(item, str) and item.strip() for item in step_providers)
        ):
            raise RuntimeError(
                f"❌ Step metadata providers must be a non-empty list of provider names: "
                f"{step_meta_path}"
            )

        step_contract = step_meta.get("cfg_keys") or {}
        if not isinstance(step_contract, dict):
            raise RuntimeError(f"Step metadata cfg_keys must be a mapping: {step_meta_path}")
        for domain, bindings in step_contract.items():
            # A step is a SIGNATURE: `<local name>: <cfg key>`. It binds by name,
            # so a glob — which can expand to a key the step never named — is not
            # expressible here by construction.
            if not isinstance(bindings, dict) or not bindings:
                raise RuntimeError(
                    f"Step metadata cfg_keys[{domain!r}] must be a non-empty mapping of "
                    f"local name -> cfg key: {step_meta_path}"
                )
            for local_name, cfg_key in bindings.items():
                if not isinstance(local_name, str) or not local_name:
                    raise RuntimeError(
                        f"Step metadata cfg_keys[{domain!r}] local names must be "
                        f"non-empty strings: {step_meta_path}"
                    )
                if not isinstance(cfg_key, str) or not cfg_key:
                    raise RuntimeError(
                        f"Step metadata cfg_keys[{domain!r}][{local_name!r}] must bind a "
                        f"non-empty cfg key: {step_meta_path}"
                    )

        active.append(
            units_step.Step(
                id=step_id,
                path=step_path,
                providers=tuple(sorted(set(step_providers))),
                cfg_keys=step_contract,
                runtime=units_step.StepRuntime(
                    image=units_step.StepImage(image),
                    docker_build=docker_build,
                    values_json=values_json,
                    env_sh=env_sh,
                    supported_execution_runtime_modes=tuple(
                        sorted(supported_execution_runtime_modes)
                    ),
                ),
                env_vars={"action": {}, "step": step_meta.get("env_vars", {})},
            )
        )
    return active


def get_repo_local_steps(
    repo_path: Path, action: str, procedure_key: str
) -> tuple[list[str], list["units_step.Step"]]:
    manifest_file = repo_path / ADAPTER_DIR / "manifest.yaml"
    if not manifest_file.is_file():
        raise RuntimeError(f"❌ manifest file not found: {manifest_file}")
    procedures_file = repo_path / ADAPTER_DIR / "procedures.yaml"
    if not procedures_file.is_file():
        raise RuntimeError(f"❌ procedures file not found: {procedures_file}")

    manifest = (kernel_yaml_io.load_yaml(manifest_file) or {}).get("manifest", {})
    procedures = (kernel_yaml_io.load_yaml(procedures_file) or {}).get("procedures", {})

    action_manifest = manifest.get(action)
    if not isinstance(action_manifest, dict) or not action_manifest:
        raise RuntimeError(f"manifest {manifest_file} declares no steps for action {action!r}")

    action_procedures = procedures.get(action)
    if not isinstance(action_procedures, dict) or procedure_key not in action_procedures:
        raise RuntimeError(f"procedure {action}/{procedure_key} not found in {procedures_file}")
    procedure = action_procedures[procedure_key]
    if not isinstance(procedure, dict) or "steps" not in procedure:
        raise RuntimeError(f"procedure {action}/{procedure_key} must define steps")

    active_ids: list[str] = []
    for step_id in procedure.get("steps", []):
        if step_id not in action_manifest:
            raise RuntimeError(f"Step {step_id!r} not declared in manifest for action {action!r}")
        active_ids.append(step_id)

    return active_ids, _repo_local_active_steps(action_manifest, active_ids, repo_path, action)


def run_gates_dir(run_dir: Path) -> Path:
    """The run's GATES dir — a top-level sibling of artifacts/ and logs/.

    Holds the verdicts that decide whether the run may proceed (cfg validation,
    target-cfg validation, ctl-policy, execution-identity preflight), kept apart
    from artifacts/ (what the run PRODUCES) and logs/ (what it SAID). Published
    with the run record (RUN_RECORD_MEMBERS).
    """

    return Path(run_dir) / "gates"
