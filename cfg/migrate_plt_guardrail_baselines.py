#!/usr/bin/env python3
"""Hard-cut plt guardrails from scope/axes baselines to rendered-target instances."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "runners"))

from utils import common  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plt-cfg-root", required=True)
    parser.add_argument("--guardrails-cfg-root", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    return parser.parse_args()


def load_legacy_policy_files(plt_root: Path) -> tuple[dict[str, dict], list[Path]]:
    policies = {}
    catalog_paths = []
    for path in common.plt_guardrail_policy_sources(plt_root):
        data = common.load_yaml(path) or {}
        if not isinstance(data, dict):
            raise RuntimeError(f"invalid legacy guardrail cfg: {path}")
        if set(data) == {"baseline_axes"}:
            catalog_paths.append(path)
            continue
        if set(data) - {"declare", "baseline_axes_key"}:
            raise RuntimeError(f"unsupported legacy guardrail cfg: {path}")
        declarations = data.get("declare") or []
        if not declarations:
            raise RuntimeError(f"legacy guardrail file has no declarations: {path}")
        name = path.stem.removeprefix("__guardrails__") or "default"
        if name in policies:
            raise RuntimeError(f"duplicate migrated policy name {name!r}: {path}")
        target_paths = {entry.get("match_target_path") for entry in declarations}
        selectors = [entry.get("selectors") or {} for entry in declarations]
        if len(target_paths) != 1 or any(item != selectors[0] for item in selectors):
            raise RuntimeError(f"legacy file cannot become one cohesive policy: {path}")
        protected_vars = []
        for entry in declarations:
            if set(entry) - {"var", "match_target_path", "selectors"}:
                raise RuntimeError(f"unsupported legacy declaration in {path}: {entry}")
            var = entry.get("var")
            if not isinstance(var, str) or not var:
                raise RuntimeError(f"invalid legacy declaration var in {path}")
            protected_vars.append(var)
        policy = {
            "match_target_path": next(iter(target_paths)),
        }
        if selectors[0]:
            policy["selectors"] = selectors[0]
        policy["protected_vars"] = protected_vars
        policies[name] = {"path": path, "policy": policy}
    return policies, catalog_paths


def load_legacy_baselines(root: Path) -> dict[tuple, dict]:
    result = {}
    for path in sorted((root / "invariants" / "plt").rglob("*.yaml")):
        data = common.load_yaml(path) or {}
        entries = data.get(common.PLT_GUARDRAIL_BASELINES_KEY) if isinstance(data, dict) else None
        if not isinstance(entries, list):
            raise RuntimeError(f"invalid legacy baseline file: {path}")
        for index, raw in enumerate(entries):
            if not isinstance(raw, dict) or "scope_path" not in raw or "guarded_vars" not in raw:
                raise RuntimeError(f"not a legacy scope baseline at {path}[{index}]")
            axes = raw.get("axes") or {}
            identity = common.guard_baseline_identity(raw["scope_path"], axes)
            if identity in result:
                raise RuntimeError(f"duplicate legacy baseline identity {identity!r}")
            guarded = {}
            common.merge_guarded_vars(guarded, raw["guarded_vars"], origin=path)
            result[identity] = {
                "scope_path": raw["scope_path"],
                "axes": {str(key): str(value) for key, value in axes.items()},
                "values": {name: entry["value"] for name, entry in guarded.items()},
                "origin": path,
            }
    return result


def exact_selector_context(selectors: dict) -> dict[str, str] | None:
    requirements = common.selector_requirements(selectors, label="legacy scope", structured_only=True)
    if any(len(values) != 1 for values in requirements.values()):
        return None
    return {ref: next(iter(values)) for ref, values in requirements.items()}


def legacy_for_scope(entries: dict[tuple, dict], scope_path: str, context: dict[str, object]) -> tuple[tuple, dict] | None:
    matches = []
    for identity, entry in entries.items():
        if entry["scope_path"] != scope_path:
            continue
        if all(str(context.get(ref)) == value for ref, value in entry["axes"].items()):
            matches.append((identity, entry))
    if len(matches) > 1:
        raise RuntimeError(f"ambiguous legacy baselines for {scope_path}: {[item[0] for item in matches]}")
    return matches[0] if matches else None


def build_migration(plt_root: Path, legacy: dict[tuple, dict]) -> tuple[dict[tuple, dict], set[tuple]]:
    meta_by_scope = {}
    for meta_path in common.discover_cfg_meta_paths(plt_root):
        meta = common.load_cfg_meta(meta_path)
        if meta["type"] != "scope":
            continue
        rel = meta_path.parent.relative_to(plt_root).as_posix()
        scope_path = "/" + rel if rel != "." else "/"
        meta_by_scope[scope_path] = meta

    composition = common.load_scope_composition(plt_root)
    migrated = {}
    consumed = set()
    for seed_identity, seed in legacy.items():
        meta = meta_by_scope.get(seed["scope_path"])
        if meta is None:
            missing_scope_path = seed["scope_path"]
            raise RuntimeError(f"legacy baseline has no scope metadata: {missing_scope_path}")
        context = exact_selector_context(meta.get("selectors") or {})
        if context is None:
            continue
        context.update(seed["axes"])
        try:
            active_scopes = common.discover_active_cfg_scopes(
                plt_root,
                scope_params=common.scope_params_from_context(context),
                execution_context=context,
            )
        except RuntimeError:
            continue
        target_path = common.normalize_cfg_absolute_path(
            meta["target_path"],
            label="legacy target_path",
        )
        target_scopes = [scope for scope in active_scopes if scope["target_path"] == target_path]
        try:
            instance = common.build_plt_guardrail_instance(
                target_path,
                target_scopes,
                composition,
                context,
            )
        except RuntimeError:
            continue
        values = {}
        used = set()
        for scope in target_scopes:
            match = legacy_for_scope(legacy, scope["scope_path"], context)
            if match is None:
                continue
            old_identity, old_entry = match
            overlap = set(values) & set(old_entry["values"])
            if overlap:
                raise RuntimeError(f"duplicate protected vars while composing {target_path}: {sorted(overlap)}")
            values.update(old_entry["values"])
            used.add(old_identity)
        if not values:
            continue
        new_identity = common.plt_guardrail_instance_identity(instance)
        previous = migrated.get(new_identity)
        candidate = {"instance": instance, "protected_values": dict(sorted(values.items()))}
        if previous is not None and previous != candidate:
            raise RuntimeError(f"conflicting migration for target instance {instance}")
        migrated[new_identity] = candidate
        consumed.update(used)

    missing = sorted(set(legacy) - consumed)
    if missing:
        raise RuntimeError(f"legacy baselines were not consumed by target instances: {missing}")
    return migrated, consumed


def apply_migration(
    plt_root: Path,
    guardrails_root: Path,
    policy_files: dict[str, dict],
    catalog_paths: list[Path],
    legacy: dict[tuple, dict],
    migrated: dict[tuple, dict],
) -> None:
    for entry in migrated.values():
        common.write_plt_guardrail_baseline(
            guardrails_root,
            instance=entry["instance"],
            protected_values=entry["protected_values"],
        )
    new_paths = {
        common.plt_guardrail_baseline_file(guardrails_root, entry["instance"]["target_path"]).resolve()
        for entry in migrated.values()
    }
    old_paths = {entry["origin"].resolve() for entry in legacy.values()}
    for path in sorted(old_paths - new_paths):
        path.unlink()
    for directory in sorted((guardrails_root / "invariants" / "plt").rglob("*"), reverse=True):
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()

    for name, entry in sorted(policy_files.items()):
        common.write_yaml_file(
            entry["path"],
            {common.PLT_GUARDRAIL_POLICIES_KEY: {name: entry["policy"]}},
        )
    for path in catalog_paths:
        path.unlink()


def main() -> int:
    args = parse_args()
    plt_root = Path(args.plt_cfg_root).expanduser().resolve()
    guardrails_root = Path(args.guardrails_cfg_root).expanduser().resolve()
    policy_files, catalog_paths = load_legacy_policy_files(plt_root)
    legacy = load_legacy_baselines(guardrails_root)
    migrated, consumed = build_migration(plt_root, legacy)
    if args.apply:
        apply_migration(plt_root, guardrails_root, policy_files, catalog_paths, legacy, migrated)
        loaded = common.load_plt_guardrail_baselines(guardrails_root)
        comparable = {
            identity: {
                "instance": entry["instance"],
                "protected_values": entry["protected_values"],
            }
            for identity, entry in loaded.items()
        }
        if comparable != migrated:
            raise RuntimeError("post-migration baseline parity failed")
        common.load_plt_guardrail_policies(plt_root)
    print(f"OK: {len(legacy)} scope baselines -> {len(migrated)} target instances; consumed={len(consumed)}")
    for entry in migrated.values():
        print(yaml.safe_dump(entry["instance"], sort_keys=False).strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
