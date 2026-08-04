"""What a pre-run check IS.

A check owns everything that happens once its report exists: it names itself,
draws the report, writes its artifact, and says whether the run may proceed.
One class per check and one registry, so declaring a check is all it takes —
no caller names a check, and none can be wired up by halves.

Each check is asserted SEPARATELY, so the refusal names which one refused. A
skippable check ASKS the policy whether it may be skipped rather than answering
it: `CfgValidationCheck.apply_gate` only records the decision the caller already
obtained from the ctl profile.

Building a report belongs to `SelectionPreflightCheck` and not to the root,
because the checks do not build at one moment: whole-cfg validation is built
ONCE per run from provider findings, the selection-scoped ones build per
selection and after their own catalog load, and the fan-out asserts an aggregate
of child reports that no check built at all. What every check shares begins at
the finished report.
"""

import abc
import dataclasses
from pathlib import Path

from engine.execution import run_context as execution_run_context
from engine.preflight import render as preflight_render
from engine.preflight import reports as preflight_reports


@dataclasses.dataclass(frozen=True)
class PreflightInputs:
    """Everything one run's selection-scoped checks resolve against.

    One value object rather than a keyword per check, because these arrive
    together from a single command line and every check reads a subset."""

    ctl_cfg_root: Path
    ctl_profile: str
    ctl_ref_policy: str
    execution_runtime_mode: str
    execution_access_modes: dict[str, str]
    provider_options: dict[str, str] | None
    implementation_key: str
    force_skip_execution_identity_preflight_check: list[str]
    agreed_defer_ctl_state_backend_sync: bool
    force_skip_ctl_state_backend_sync: bool


class PreflightCheck(abc.ABC):
    """One pre-run check, from its finished report onwards."""

    # how a caller finds this check's report, and the file it renders into
    name: str
    artifact_name: str
    # what the refusal says failed, and what it names when nothing named itself
    refusal: str
    unknown_item: str

    @abc.abstractmethod
    def render_lines(self, report: dict) -> list[str]:
        """The report, drawn."""

    @abc.abstractmethod
    def failed_items(self, report: dict) -> list[str]:
        """Every part of the report that failed — what the refusal names."""

    def gate_status(self, report: dict) -> str | None:
        """Whether this report stops the run. A skippable check overrides this
        to read the recorded decision instead of the raw status."""
        return report.get("status")

    def write_artifacts(self, gates_dir: Path, report: dict) -> None:
        text_path = Path(gates_dir) / self.artifact_name
        text_path.parent.mkdir(parents=True, exist_ok=True)
        text_path.write_text(
            "\n".join(self.render_lines(report)) + "\n", encoding="utf-8"
        )

    def assert_accepted(self, report: dict) -> None:
        if self.gate_status(report) != "failed":
            return
        raise RuntimeError(
            f"❌ {self.refusal} failed for: "
            + ", ".join(self.failed_items(report) or [self.unknown_item])
        )


class SelectionPreflightCheck(PreflightCheck):
    """A check scoped to ONE selection.

    Its report nests: the fan-out wraps one child report per fanned-out run
    under a parent, so a failed child is named by its own selection key."""

    unknown_item = "selected run"
    # whether the provider catalogs must be loaded before this check can build
    requires_provider_catalogs: bool = True
    # where the report keeps its per-item rows, and what names a row
    row_container: str
    row_key: str

    @abc.abstractmethod
    def build(self, selection: dict, inputs: PreflightInputs) -> dict:
        """This check's report for one selection."""

    def unresolved_report(self, selection_ref: dict, reason: str) -> dict:
        """The report of a check that never ran because the selection's provider
        catalogs did not load."""
        return {
            "selection": selection_ref,
            "status": "failed",
            self.row_container: [],
            "failure_reason": reason,
        }

    def failed_items(self, report: dict) -> list[str]:
        items = [
            str(row.get(self.row_key, "<unknown>"))
            for row in report.get(self.row_container, [])
            if row.get("status") == "failed"
        ]
        items.extend(
            str(child.get("selection", {}).get("key", "<unknown>"))
            for child in report.get("children", [])
            if child.get("status") == "failed"
        )
        return items


class CfgValidationCheck(PreflightCheck):
    """Whole-cfg well-formedness, built once per run and not from a selection.

    The only skippable check. An authorized force flag accepts failed concrete
    bindings OUTSIDE the selected run; structural and unclassified findings are
    never skippable, and the selected run's own bindings are enforced by
    target_cfg_validation regardless."""

    name = "cfg_validation"
    artifact_name = "cfg_validation.txt"
    refusal = "full cfg validation"
    unknown_item = "unknown cfg path"

    def build(self, findings: list[dict]) -> dict:
        """A flat list of cfg-path-keyed findings. Failed if any finding failed."""
        status = (
            "failed"
            if any(finding.get("status") == "failed" for finding in findings)
            else "passed"
        )
        return {"kind": "cfg_validation", "status": status, "findings": list(findings)}

    def apply_gate(self, report: dict, *, force_skip: bool) -> dict:
        """Record whether the run's profile accepted these findings."""
        unskippable_failure = any(
            finding.get("status") == "failed" and finding.get("structural") is not False
            for finding in report.get("findings", [])
        )
        if force_skip and report.get("status") == "failed" and not unskippable_failure:
            report["gate"] = {
                "status": "force_skipped",
                "reason": "unrelated full-cfg failures were accepted for this run",
            }
        else:
            report["gate"] = {"status": report.get("status", "failed")}
        return report

    def gate_status(self, report: dict) -> str:
        return (report.get("gate") or {}).get(
            "status", report.get("status", "failed")
        )

    def render_lines(self, report: dict) -> list[str]:
        return preflight_render._cfg_validation_text_lines(report)

    def failed_items(self, report: dict) -> list[str]:
        return [
            str(finding.get("cfg_path", "<unknown>"))
            for finding in report.get("findings", [])
            if finding.get("status") == "failed"
        ]


class TargetCfgValidationCheck(SelectionPreflightCheck):
    """Every cfg reference the SELECTED targets reach must be concrete."""

    name = "target_cfg_validation"
    artifact_name = "target_cfg_validation.txt"
    refusal = "target cfg validation"
    row_container = "results"
    row_key = "target_key"

    def build(self, selection: dict, inputs: PreflightInputs) -> dict:
        return preflight_reports.build_target_cfg_validation_report(
            selection,
            implementation_key=inputs.implementation_key,
            execution_access_modes=inputs.execution_access_modes,
            provider_options=inputs.provider_options,
            ctl_cfg_root=inputs.ctl_cfg_root,
        )

    def render_lines(self, report: dict) -> list[str]:
        return preflight_render._target_cfg_validation_text_lines(report)


class CtlPolicyPreflightCheck(SelectionPreflightCheck):
    """Run policy, evaluated independently of provider identity reachability —
    which is why it builds BEFORE the catalogs load: a catalog failure must not
    be able to masquerade as a policy failure."""

    name = "ctl_policy_preflight"
    artifact_name = "ctl_policy_validation.txt"
    refusal = "ctl policy preflight"
    requires_provider_catalogs = False
    row_container = "checks"
    row_key = "name"

    def build(self, selection: dict, inputs: PreflightInputs) -> dict:
        return preflight_reports.build_ctl_policy_preflight_report(
            selection,
            ctl_cfg_root=inputs.ctl_cfg_root,
            ctl_profile=inputs.ctl_profile,
            ctl_ref_policy=inputs.ctl_ref_policy,
            execution_runtime_mode=inputs.execution_runtime_mode,
            execution_access_modes=inputs.execution_access_modes,
            provider_options=inputs.provider_options,
            agreed_defer_ctl_state_backend_sync=(
                inputs.agreed_defer_ctl_state_backend_sync
            ),
            force_skip_ctl_state_backend_sync=(
                inputs.force_skip_ctl_state_backend_sync
            ),
            force_skip_execution_identity_preflight_check=(
                inputs.force_skip_execution_identity_preflight_check
            ),
        )

    def render_lines(self, report: dict) -> list[str]:
        return preflight_render._ctl_policy_preflight_text_lines(report)


class ExecutionIdentityPreflightCheck(SelectionPreflightCheck):
    """Whether the identity each selected target runs as is actually reachable."""

    name = "execution_identity_preflight"
    artifact_name = "execution_identity_preflight.txt"
    refusal = "execution identity preflight"
    row_container = "results"
    row_key = "target_key"

    def build(self, selection: dict, inputs: PreflightInputs) -> dict:
        return preflight_reports.build_execution_identity_preflight_report(
            selection,
            implementation_key=inputs.implementation_key,
            execution_access_modes=inputs.execution_access_modes,
            provider_options=inputs.provider_options,
            force_skip_providers=(
                inputs.force_skip_execution_identity_preflight_check
            ),
            ctl_cfg_root=inputs.ctl_cfg_root,
            agreed_defer_ctl_state_backend_sync=(
                inputs.agreed_defer_ctl_state_backend_sync
            ),
            force_skip_ctl_state_backend_sync=(
                inputs.force_skip_ctl_state_backend_sync
            ),
        )

    def render_lines(self, report: dict) -> list[str]:
        return preflight_render._preflight_text_lines(report)


CFG_VALIDATION = CfgValidationCheck()
TARGET_CFG_VALIDATION = TargetCfgValidationCheck()
CTL_POLICY_PREFLIGHT = CtlPolicyPreflightCheck()
EXECUTION_IDENTITY_PREFLIGHT = ExecutionIdentityPreflightCheck()

# The order a run is written out and refused in: whole-cfg health, then the
# selected run's cfg, then policy, then reachability.
PREFLIGHT_CHECKS: tuple[PreflightCheck, ...] = (
    CFG_VALIDATION,
    TARGET_CFG_VALIDATION,
    CTL_POLICY_PREFLIGHT,
    EXECUTION_IDENTITY_PREFLIGHT,
)

# Derived, so a new selection-scoped check is still only one registry row.
SELECTION_PREFLIGHT_CHECKS: tuple[SelectionPreflightCheck, ...] = tuple(
    check for check in PREFLIGHT_CHECKS if isinstance(check, SelectionPreflightCheck)
)


def build_selection_validation_reports(
    selection: dict, inputs: PreflightInputs
) -> dict:
    """Build every selection-scoped report for a selection resolved with
    `load_provider_catalogs=False`, keyed by check name.

    Catalogs load LENIENTLY, so a placeholder account id becomes a per-target
    'blocked' row rather than a crash. Shared by the single runners and the
    fan-out so they stay in lockstep. (cfg_validation is not per-selection: the
    caller builds it once.)"""
    selection_ref = {
        "kind": selection["selection_kind"],
        "key": selection["selection_key"],
    }
    reports: dict[str, dict] = {
        check.name: check.build(selection, inputs)
        for check in SELECTION_PREFLIGHT_CHECKS
        if not check.requires_provider_catalogs
    }
    catalog_checks = [
        check for check in SELECTION_PREFLIGHT_CHECKS if check.requires_provider_catalogs
    ]
    try:
        selection = preflight_reports.load_selection_provider_catalogs(
            selection, inputs.ctl_cfg_root
        )
        # One try for all of them: a catalog load that fails leaves NONE of these
        # checked, so none may report a result it did not actually reach.
        catalog_reports = {
            check.name: check.build(selection, inputs) for check in catalog_checks
        }
    except Exception as error:
        reason = execution_run_context.credential_free_preflight_failure_reason(error)
        catalog_reports = {
            check.name: check.unresolved_report(selection_ref, reason)
            for check in catalog_checks
        }
    reports.update(catalog_reports)
    return {"selection": selection, "reports": reports}
