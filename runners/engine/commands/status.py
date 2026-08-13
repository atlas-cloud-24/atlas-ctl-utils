"""The status entry points.

Read-only, always: a status command never writes a run record and never takes a
mutation lock, which is what makes it safe to point at a live backend."""

import argparse
import contextlib
import tempfile
from pathlib import Path

import yaml

from engine.catalog import fan_out as catalog_fan_out
from engine.catalog import workflow as catalog_workflow
from engine.cfg import resources as cfg_resources
from engine.commands import selection as commands_selection
from engine.execution import run_context as execution_run_context
from engine.kernel import ids as kernel_ids
from engine.kernel import yaml_io as kernel_yaml_io
from engine.run import actions as run_actions
from engine.run import request as run_request
from engine.state import render as state_render
from engine.state import run_store as state_run_store
from engine.state import status as state_status
from engine.state import status_query as state_status_query
from engine.state import status_rows as state_status_rows
from engine.state import sync as state_sync


def _read_namespace(
    ctl_cfg_root: Path,
    args: argparse.Namespace,
    execution_context: dict[str, object],
    reader,
    *,
    credential_acquisition: str,
    include_maintenance_manifests: bool = False,
):
    """Read one complete namespace locally or from an isolated hydration."""

    namespace_key, _ = state_sync.CtlStateBackends.resolve_namespace(
        ctl_cfg_root, execution_context
    )
    if args.status == "local":
        namespace_root = Path(args.ctl_state_local_root) / namespace_key
        return namespace_key, reader(namespace_root)

    keep = getattr(args, "hydrate_to", None)
    scratch = (
        contextlib.nullcontext(str(Path(keep).expanduser()))
        if keep
        else tempfile.TemporaryDirectory(prefix="atlas-ctl-state-remote-all-")
    )
    with scratch as scratch_root:
        _, namespace_root, syncer = state_sync.CtlStateAccess.arm_operation(
            ctl_cfg_root,
            execution_context,
            Path(scratch_root),
            operation="read",
            credential_acquisition=credential_acquisition,
            execution_access_modes=args.execution_access_modes,
            provider_options=args.provider_options,
        )
        state_run_store.hydrate_ctl_state_index(
            syncer,
            include_maintenance_manifests=include_maintenance_manifests,
        )
        result = reader(namespace_root)
        if keep:
            kernel_yaml_io.write_yaml_file(
                Path(scratch_root) / "hydrated_from.yaml",
                {
                    "namespace": namespace_key,
                    "hydrated_at": kernel_ids.utc_timestamp(),
                    "source": "remote ctl-state backend",
                },
            )
        return namespace_key, result


class StatusCommand:
    """What one invocation reports about a ctl-state namespace."""

    @staticmethod
    def run(
        ctl_cfg_root: Path,
        args: argparse.Namespace,
        *,
        credential_acquisition: str = "local",
    ) -> dict:
        """Dispatch whole-namespace, maintenance-audit, or targeted status."""

        if args.all:
            return StatusCommand.all_run_types(
                ctl_cfg_root, args, credential_acquisition=credential_acquisition
            )
        if getattr(args, "maintenance", False):
            return StatusCommand.maintenance(
                ctl_cfg_root, args, credential_acquisition=credential_acquisition
            )
        run_type = "workflow" if args.workflow else "fan_out" if args.fan_out else "target"
        return StatusCommand.for_run_type(
            ctl_cfg_root,
            args,
            run_type=run_type,
            credential_acquisition=credential_acquisition,
        )

    @staticmethod
    def for_run_type(
        ctl_cfg_root: Path,
        args: argparse.Namespace,
        *,
        run_type: str,
        credential_acquisition: str = "local",
    ) -> dict:
        # a status read resolves the same selections a run would, so it asks for
        # them the same way — the narrowing per child is the only difference
        base_request = run_request.RunRequest.from_args(
            args,
            ctl_cfg_root=ctl_cfg_root,
            target_repo_key="repo_path",
            require_target_ref=False,
            credential_acquisition=credential_acquisition,
        )
        if run_type == "fan_out":
            expansion_context = execution_run_context.build_execution_context(
                ctl_cfg_root,
                action=args.action,
                ctl_profile=args.ctl_profile,
                execution_params=args.execution_params,
                providers=getattr(args, "providers", ()),
                execution_runtime_mode=args.execution_runtime_mode,
            )
            plan = catalog_fan_out.FanOutCatalog.expand(
                ctl_cfg_root, args.fan_out, expansion_context
            )
            catalog_fan_out.FanOutCatalog.validate_param_collisions(
                ctl_cfg_root, plan["children"], args.execution_params
            )
            commands_selection.require_unique_fan_out_namespace(
                ctl_cfg_root,
                plan["children"],
                action=args.action,
                ctl_profile=args.ctl_profile,
                execution_params=args.execution_params,
                providers=getattr(args, "providers", ()),
                execution_runtime_mode=args.execution_runtime_mode,
            )
            selections = []
            # The fan-out child's display name, not a run label — see
            # _compute_status_results.
            selection_labels = []
            for child in plan["children"]:
                params = dict(args.execution_params)
                params.update(child["params"])
                selections.append(
                    commands_selection.resolve_pipeline_selection(
                        base_request.for_child(
                            action=args.action,
                            execution_params=params,
                            workflow_name=(child["key"] if child["kind"] == "workflow" else None),
                            target_name=(child["key"] if child["kind"] == "target" else None),
                        ),
                        # A status read needs only the cfg-level state spec
                        # (prefix/segments); it enforces no mutate policy and loads
                        # no provider catalogs, which would validate account ids a
                        # read never uses.
                        enforce_ctl_policy=False,
                        load_provider_catalogs=False,
                    )
                )
                selection_labels.append(child["label"])
            specs = catalog_fan_out.FanOutCatalog.validate_unique_materializations(selections)
        else:
            selection = commands_selection.resolve_pipeline_selection(
                base_request.for_child(
                    action=args.action,
                    execution_params=args.execution_params,
                    workflow_name=(args.workflow if run_type == "workflow" else None),
                    target_name=(args.target if run_type == "target" else None),
                ),
                enforce_ctl_policy=False,
                load_provider_catalogs=False,
            )
            selections = [selection]
            specs = [catalog_workflow.WorkflowArtifacts.selection_state_spec(selection)]
            selection_labels = [selection.key]

        # A query must NEVER mutate local ctl-state. `remote` hydrates
        # into an auto-generated throwaway root (an implementation detail — never a
        # CLI argument) so pull_object's unconditional overwrite lands there instead
        # of clobbering a local-only pointer; `local` never touches the bucket.
        if args.status == "local":
            namespace_key, namespace_root = state_sync.CtlStateAccess.local_scope(
                ctl_cfg_root,
                selections[0]["execution_context"],
                args.ctl_state_local_root,
            )
            results = state_status_rows._compute_status_results(
                namespace_root, args.action, selection_labels, specs
            )
        else:
            with tempfile.TemporaryDirectory(prefix="atlas-ctl-state-remote-") as scratch_root:
                namespace_key, namespace_root, syncer = state_sync.CtlStateAccess.arm_reader(
                    ctl_cfg_root,
                    selections[0],
                    Path(scratch_root),
                    credential_acquisition=credential_acquisition,
                    execution_access_modes=args.execution_access_modes,
                    provider_options=args.provider_options,
                )
                for spec in specs:
                    child_prefixes = [target["prefix"] for target in spec.get("target_specs", [])]
                    syncer.hydrate_instance(
                        spec["prefix"],
                        child_prefixes,
                        committed_groups=run_actions.RESULT_GROUPS,
                    )
                results = state_status_rows._compute_status_results(
                    namespace_root, args.action, selection_labels, specs
                )
        report = {
            "selection": {
                "kind": run_type,
                "key": (
                    args.fan_out
                    if run_type == "fan_out"
                    else args.workflow
                    if run_type == "workflow"
                    else args.target
                ),
            },
            "namespace": namespace_key,
            # Which scope produced this view — local and bucket history legitimately
            # differ (a force-skipped run is local-only, permanently).
            "scope": args.status,
            # One roll-up PER AXIS. A single summary would reintroduce exactly what
            # the axes exist to remove: a live child hiding a stale one.
            "status": (
                state_status.Verdict.RUNNING
                if any(item["status"] == state_status.Verdict.RUNNING for item in results)
                else state_status.Verdict.FAILED
                if any(item["status"] == state_status.Verdict.FAILED for item in results)
                else state_status.Verdict.PASSED
            ),
            **(
                {
                    "state": (
                        "partial"
                        if any(item.get("state") == "partial" for item in results)
                        else "destroyed"
                        if all(
                            item.get("state") == "destroyed"
                            for item in results
                            if item.get("state")
                        )
                        else "provisioned"
                    ),
                    "freshness": (
                        "outdated"
                        if any(item.get("freshness") == "outdated" for item in results)
                        else "current"
                    ),
                }
                if any(item.get("state") for item in results)
                else {}
            ),
            "results": results,
        }
        print(yaml.safe_dump(report, sort_keys=False).rstrip())
        return report

    @staticmethod
    def all_run_types(
        ctl_cfg_root: Path,
        args: argparse.Namespace,
        *,
        credential_acquisition: str = "local",
    ) -> dict:
        """Namespace status: resolve the namespace from the axes,
        then read every instance — local walks the dir offline; remote hydrates the
        whole namespace into a throwaway temp (never the local tree) and reads that.
        Prints a flat map. Read-only by default; --write-cache additionally persists
        the map as an advisory, self-dated status_cache.yaml at the namespace root
        (an additive file — it never touches committed pointers)."""
        execution_context = execution_run_context.build_execution_context(
            ctl_cfg_root,
            action=args.action,
            ctl_profile=args.ctl_profile,
            execution_params=args.execution_params,
            providers=getattr(args, "providers", ()),
            execution_access_modes=args.execution_access_modes,
            execution_runtime_mode=args.execution_runtime_mode,
        )
        # An exclusive relation names targets that are ALTERNATIVES over one
        # deployment. ctl
        # cannot derive that — a target names a procedure, a procedure names steps —
        # so it is read from cfg, and an empty registry leaves every row unchanged.
        exclusive_target_relations = cfg_resources.collect_resource(
            ctl_cfg_root, "exclusive_target_relations"
        )
        exclusive_workflow_relations = cfg_resources.collect_resource(
            ctl_cfg_root, "exclusive_workflow_relations"
        )
        namespace_key, instances = _read_namespace(
            ctl_cfg_root,
            args,
            execution_context,
            lambda namespace_root: state_status_rows.compute_namespace_status_map(
                namespace_root,
                exclusive_target_relations,
                exclusive_workflow_relations,
            ),
            credential_acquisition=credential_acquisition,
        )
        filters = getattr(args, "filters", None) or {}
        instances = state_status_query.filter_status_map(instances, filters)
        instances = state_status_query.structure_status_map(instances, args.structure, args.sort)
        # Kinds sit at the TOP level, not under an `instances:` wrapper: the wrapper
        # said nothing the kind keys do not, and cost a level of nesting on every read.
        report = {
            "namespace": namespace_key,
            "scope": args.status,
            "computed_at": kernel_ids.utc_timestamp(),
            "structure": args.structure,
            "sort": args.sort,
            # Only when one was applied: an absent filter is not a fact about the
            # namespace, and a cached map that states its filters cannot be mistaken
            # for a whole-namespace one.
            **({"filters": filters} if filters else {}),
            **instances,
        }
        if getattr(args, "write_cache", False):
            # `report` already carries `filters` when one was applied, so a filtered
            # cache states which view produced it and cannot be mistaken for a
            # whole-namespace map.
            queried_at = kernel_ids.utc_timestamp()
            cache = {
                "advisory": True,
                "source": "status runner",
                "queried_at": queried_at,
                **report,
            }
            namespace_dir = Path(args.ctl_state_local_root) / namespace_key
            cache_path = namespace_dir / "status_cache.yaml"
            kernel_yaml_io.write_yaml_file(cache_path, cache)

            # Every query is also kept, dated, so a reader can see what
            # status SAID at a past moment rather than only what it says now.
            # LOCAL ONLY, and that is the point. `status` is read-only against
            # ctl-state; writing a query record into the synced tree would make a read
            # command mutate, fail under read-only credentials, and add sync churn for
            # something no other run consumes. The local root is already an advisory
            # mirror that is never truth, which is exactly what a query log is.
            # The LATEST stays at the namespace root under its stable name, so the one
            # path a tool reads does not move as history accumulates.
            # `_local` is a SIBLING of the namespaces, not a child of one — the same
            # place run workspaces live. Nesting it under the namespace would put a
            # never-synced directory inside a tree that IS synced.
            history_path = (
                Path(args.ctl_state_local_root).joinpath(*state_run_store.LOCAL_ONLY_LOCATOR)
                / "status_history"
                / namespace_key
                / f"{queried_at.replace(':', '-')}.yaml"
            )
            kernel_yaml_io.write_yaml_file(history_path, cache)
            report = {
                **report,
                "cache_written": cache_path.as_posix(),
                "history_written": history_path.as_posix(),
            }
        # ONE report, rendered two ways. `--format` is deliberately absent from the
        # report itself: how it was printed is not a fact about the namespace, and a
        # cached map claiming a format would be a claim about a file nobody kept.
        print(
            state_render.render_status_map(
                report,
                hide_members=getattr(args, "hide_members", False),
            )
            if getattr(args, "output_format", None) == state_render.StatusFormat.TABLE
            else yaml.safe_dump(report, sort_keys=False).rstrip()
        )
        return report

    @staticmethod
    def maintenance(
        ctl_cfg_root: Path,
        args: argparse.Namespace,
        *,
        credential_acquisition: str = "local",
    ) -> dict:
        """Report durable maintenance activity outside normal instance status."""

        execution_context = execution_run_context.build_execution_context(
            ctl_cfg_root,
            action=args.action,
            ctl_profile=args.ctl_profile,
            execution_params=args.execution_params,
            providers=getattr(args, "providers", ()),
            execution_access_modes=args.execution_access_modes,
            execution_runtime_mode=args.execution_runtime_mode,
        )
        namespace_key, rows = _read_namespace(
            ctl_cfg_root,
            args,
            execution_context,
            state_status_rows.maintenance_status_rows,
            credential_acquisition=credential_acquisition,
            include_maintenance_manifests=True,
        )
        report = {
            "namespace": namespace_key,
            "scope": args.status,
            "computed_at": kernel_ids.utc_timestamp(),
            "maintenance": rows,
        }
        print(
            state_render.render_maintenance_status(report)
            if getattr(args, "output_format", None) == state_render.StatusFormat.TABLE
            else yaml.safe_dump(report, sort_keys=False).rstrip()
        )
        return report
