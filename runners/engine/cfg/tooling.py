"""Where the engine's own tooling and cfg sources come from.

A provider adapter is TOOLING, so its pin lives beside ctl-utils and plt-utils
rather than being discovered from what happens to sit next to the checkout on
disk. The filesystem is not a registry."""

import functools
import logging
from pathlib import Path

from engine.cfg import merge as cfg_merge
from engine.cfg import resources as cfg_resources
from engine.kernel import yaml_io as kernel_yaml_io

REQUIRED_TOOLING_REFS = ("ctl-utils", "plt-utils")


TOOLING_ENV_PREFIXES = {
    "ctl-utils": "ATLAS_CTL_UTILS",
    "plt-utils": "ATLAS_PLT_UTILS",
}


TOOLING_DEFAULT_REPO_URLS = {
    "ctl-utils": "https://github.com/atlas-cloud-24/atlas-ctl-utils.git",
    "plt-utils": "https://github.com/atlas-cloud-24/atlas-plt-utils.git",
}


def load_refs_cfg(ctl_cfg_root: Path) -> dict:
    """Collect the content-key `refs` resource (deep tree-merge).

    Returns `{global: {tooling...}, scoped: {<ctx>: {target_sources, modules}}}`.
    `global` = run-level shared pins (the engine/tooling, one version everywhere).
    `scoped` = per-context pins keyed by a flat dotted context. Both optional; in
    dev the refs may be absent entirely → `{}`.
    """
    merged: dict = {}
    for yf in sorted(ctl_cfg_root.rglob("*.yaml")):
        data = kernel_yaml_io.load_yaml(yf) or {}
        if not isinstance(data, dict):
            continue
        section = data.get("refs")
        if section is None:
            continue
        if not isinstance(section, dict):
            raise RuntimeError(f"❌ 'refs' must be a mapping: {yf}")
        cfg_merge._deep_merge_refs(merged, section, yf)
    return merged


def load_local_tooling_cfg(ctl_cfg_root: Path) -> dict:
    """Local tooling repo paths, discovered by content key.

    Cached per cfg root. A run resolves this three times — once per
    `build_execution_context` (preflight, then the pipeline) and once more for
    the pipeline's own tooling env — and a materialized cfg root does not change
    while a run reads it, so the second and third answers can only agree with the
    first. Without the cache the tree was walked three times AND the operator saw
    the same line three times, which reads as three different decisions.
    """

    return dict(_local_tooling_cfg(str(Path(ctl_cfg_root).resolve())))


@functools.cache
def _local_tooling_cfg(resolved_root: str) -> dict:
    ctl_cfg_root = Path(resolved_root)
    raw_tooling_cfg = cfg_resources.collect_resource(ctl_cfg_root, "tooling")
    tooling_path = ctl_cfg_root
    if not raw_tooling_cfg:
        logging.info("No local tooling config found")
        return {}

    tooling_refs = {}
    for tooling_name, tooling_entry in raw_tooling_cfg.items():
        if not isinstance(tooling_name, str):
            raise RuntimeError(f"❌ local tooling keys must be strings: {tooling_path}")
        if tooling_entry is None:
            tooling_entry = {}
        if not isinstance(tooling_entry, dict):
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must be a mapping: {tooling_path}"
            )

        if tooling_entry.get("repo_url"):
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must use repo_path, not repo_url: {tooling_path}"
            )

        repo_path = tooling_entry.get("repo_path")
        if not repo_path:
            continue
        if not isinstance(repo_path, str):
            raise RuntimeError(
                f"❌ local tooling repo path for '{tooling_name}' must be a string: {tooling_path}"
            )

        branch = tooling_entry.get("branch")
        commit = tooling_entry.get("commit")
        if branch or commit:
            raise RuntimeError(
                f"❌ local tooling entry for '{tooling_name}' must not define branch or commit: {tooling_path}"
            )

        repo_path_obj = Path(repo_path).expanduser()
        if not repo_path_obj.is_absolute():
            repo_path_obj = (ctl_cfg_root / repo_path_obj).resolve()

        tooling_refs[tooling_name] = {"repo_path": str(repo_path_obj)}

    logging.info("Using local tooling config discovered by content key")
    return tooling_refs


def build_tooling_env(tooling_refs: dict) -> dict[str, str]:
    """Translate tooling refs into environment variables for setup scripts."""
    env_updates: dict[str, str] = {}

    for tooling_name, env_prefix in TOOLING_ENV_PREFIXES.items():
        tooling_ref = tooling_refs.get(tooling_name) or {}
        if not isinstance(tooling_ref, dict):
            continue

        repo_path = tooling_ref.get("repo_path")
        repo_url = tooling_ref.get("repo_url") or (
            None if repo_path else TOOLING_DEFAULT_REPO_URLS.get(tooling_name)
        )
        branch = tooling_ref.get("branch")
        commit = tooling_ref.get("commit")

        if repo_path:
            env_updates[f"{env_prefix}_REPO_PATH"] = repo_path
        if repo_url:
            env_updates[f"{env_prefix}_REPO_URL"] = repo_url
        if branch:
            env_updates[f"{env_prefix}_BRANCH"] = branch
        if commit:
            env_updates[f"{env_prefix}_COMMIT"] = commit

    return env_updates
