"""Refusals that read a cfg tree and answer yes or no.

Separate from the loaders because a validation runs at a moment of the run's
choosing — some at start-up, some per target, some only under a gate — while a
loader runs whenever its value is needed.

Two responsibilities, so two classes. `CommitPinning` is everything one ref
policy refuses, and it HOLDS that policy: `if not requires_commits(...)` used to
be the first line of four separate functions, each re-deriving the same answer.
`CfgTreeShape` is what a cfg tree must look like regardless of any policy, so it
carries no state and its checks are static.
"""

from pathlib import Path

from engine.cfg import layout as cfg_layout
from engine.cfg import presets as cfg_presets
from engine.cfg import resources as cfg_resources
from engine.cfg import tooling as cfg_tooling
from engine.run import policy as run_policy


class CommitPinning:
    """What `ref_policy: commit_required` refuses.

    Reproducibility is the whole point: a branch reference means the code that
    ran cannot be recovered from the record. Under any other policy every check
    here is a no-op, which is why the policy is held rather than passed.
    """

    def __init__(self, ref_policy: str):
        self.ref_policy = ref_policy

    @property
    def required(self) -> bool:
        return run_policy.ref_policy_requires_commits(self.ref_policy)

    def check_target_runs(self, active_target_runs: dict) -> None:
        """Every resolved target run and module carries an explicit commit.

        Runs AFTER workflow patches and refs have resolved, so it sees what will
        actually execute rather than what was declared.
        """

        if not self.required:
            return

        target_runs_without_commit = []
        modules_without_commit = []
        for target_run_id, target_run_cfg in active_target_runs.items():
            if not target_run_cfg.get("commit"):
                target_runs_without_commit.append(target_run_id)

            raw_modules = target_run_cfg.get("modules") or {}
            if not isinstance(raw_modules, dict):
                modules_without_commit.append(f"{target_run_id}:<invalid-modules>")
                continue

            for module_name, module_cfg in raw_modules.items():
                if not module_cfg.get("commit"):
                    modules_without_commit.append(f"{target_run_id}:{module_name}")

        if target_runs_without_commit or modules_without_commit:
            details = []
            if target_runs_without_commit:
                details.append(f"Target runs missing 'commit': {target_runs_without_commit}")
            if modules_without_commit:
                details.append(f"Modules missing 'commit': {modules_without_commit}")
            raise RuntimeError(
                "❌ ref_policy=commit_required requires all target_runs and modules to have explicit 'commit' specified.\n"
                f"   {'; '.join(details)}\n"
                "   Using branch references is not allowed for reproducibility."
            )

    def check_ctl_cfg_ref(self, ctl_cfg_branch: str | None, ctl_cfg_commit: str | None) -> None:
        """The CLI-selected ctl cfg ref is commit-pinned."""

        if self.required and ctl_cfg_branch and not ctl_cfg_commit:
            raise RuntimeError(
                "❌ ref_policy=commit_required requires --ctl-cfg to use @commit=sha "
                f"(not branch={ctl_cfg_branch!r})"
            )

    def check_tooling_refs(self, tooling_refs: dict) -> None:
        """Every required tooling ref is commit-pinned."""

        if not self.required:
            return

        errors = []
        for tooling_name in cfg_tooling.REQUIRED_TOOLING_REFS:
            tooling_ref = tooling_refs.get(tooling_name) or {}
            if not isinstance(tooling_ref, dict):
                errors.append(f"tooling '{tooling_name}' ref must be a mapping")
                continue

            if tooling_ref.get("commit"):
                continue

            if tooling_ref.get("branch"):
                errors.append(
                    f"tooling '{tooling_name}' uses branch='{tooling_ref['branch']}' but commit is required"
                )
            else:
                errors.append(f"tooling '{tooling_name}' is missing commit")

        if errors:
            raise RuntimeError(
                "❌ ref_policy=commit_required requires tooling refs to use commits:\n"
                f"   {'; '.join(errors)}"
            )

    def check_cfg_sources(self, sources: dict[str, dict[str, object]]) -> None:
        """Companion cfg sources are commit-pinned, and none is a local path."""

        if not self.required:
            return
        errors = []
        for key in cfg_layout.CFG_SOURCE_KEYS:
            entry = sources[key]
            ref = entry.get("ref") or {}
            if "repo_path" in entry:
                errors.append(f"{key} uses repo_path")
            elif not isinstance(ref, dict) or not ref.get("commit"):
                errors.append(f"{key} is not commit-pinned")
        if errors:
            raise RuntimeError(
                "❌ ref_policy=commit_required requires commit-pinned cfg sources: "
                + ", ".join(errors)
            )


class CfgTreeShape:
    """What a cfg tree must look like, under every policy.

    Stateless: these read a tree and answer about that tree alone, so there is
    nothing to hold between calls.
    """

    @staticmethod
    def reject_meta_inside_data_dir(src: Path, *, import_path: str, meta_path: Path) -> None:
        """An import points at a data directory, not at another owned tree."""
        nested_meta = cfg_resources.find_nested_cfg_meta(src)
        if nested_meta is not None:
            raise RuntimeError(
                f"Import path must be a data directory, not a tree containing {cfg_layout.SCOPE_META_FILENAME}: "
                f"{import_path} ({meta_path}); found {nested_meta}"
            )

    @staticmethod
    def require_all_payload_reachable(cfg_root: Path) -> None:
        """Every payload file sits inside a scope or an imported preset.

        A file outside both is never merged and never renders — it is silently
        dead cfg, which reads as configuration and behaves as nothing.
        Composition is the only way payload reaches a target, so a file that no
        unit contains is an authoring error rather than an inert extra.
        """
        units: list[str] = []
        for meta_path in cfg_resources.discover_cfg_meta_paths(cfg_root):
            if cfg_resources.load_cfg_meta(meta_path)["type"] == "overlay":
                continue
            units.append("/" + meta_path.parent.relative_to(cfg_root).as_posix())
        for imports_path in sorted(cfg_root.rglob(cfg_presets.IMPORTS_FILENAME)):
            for entry in cfg_presets.declared_imports(imports_path.parent):
                units.append(entry["from"])
        prefixes = tuple(unit.rstrip("/") + "/" for unit in units)

        orphans: list[str] = []
        for path in sorted(cfg_root.rglob("*.yaml")):
            parts = path.relative_to(cfg_root).parts
            if (
                parts[0] in (".git", "_overlays", "diagrams")
                or cfg_layout.PLT_GUARDRAILS_DIRNAME in parts
            ):
                continue
            if path.name in cfg_layout.SCOPE_META_SKIP_FILENAMES:
                continue
            rel = "/" + path.relative_to(cfg_root).as_posix()
            if not rel.startswith(prefixes):
                orphans.append(rel)
        if orphans:
            raise RuntimeError(
                "❌ cfg payload outside every scope and preset, so it can never be merged: "
                + ", ".join(orphans)
            )
