"""Verifying rendered cfg against recorded baselines.

A baseline stores value AND hash, and the loader self-checks one against the
other, so a hand-edited value with a stale hash is refused rather than believed.

The entry points live here rather than on `Verifier` because they answer a
question about a RUN — may this run proceed — while `Verifier` answers about a
tree. That is also why the skip is checked here: `Verifier` has no opinion about
ctl profiles."""

import logging
from pathlib import Path

from engine.execution import references as execution_references
from engine.guardrails import policies as guardrails_policies


def verify_ctl_guardrails(
    ctl_cfg_root: Path,
    guardrails_cfg_root: Path,
    execution_context: dict[str, object],
) -> None:
    guardrails_policies.Verifier(guardrails_cfg_root).check_ctl(
        ctl_cfg_root,
        execution_context,
    )


def verify_plt_guardrails(
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
) -> None:
    guardrails_policies.Verifier(guardrails_cfg_root).check_plt(
        plt_cfg_root,
        plt_cfg_root,
        plt_rendered_dir,
        execution_context,
        scope_params,
    )


def verify_guardrails(
    ctl_cfg_root: Path,
    plt_cfg_root: Path,
    guardrails_cfg_root: Path,
    plt_rendered_dir: Path,
    execution_context: dict[str, object],
    scope_params: dict[str, str],
) -> None:
    if execution_context.get(
        f"{execution_references.EXECUTION_CONTEXT_ROOT}.ctl.force_skip_guardrails"
    ):
        logging.info("guardrails: force-skipped")
        return

    verifier = guardrails_policies.Verifier(guardrails_cfg_root)
    verifier.check_ctl(ctl_cfg_root, execution_context)
    logging.info("ctl guardrails: passed")
    verifier.check_plt(
        ctl_cfg_root,
        plt_cfg_root,
        plt_rendered_dir,
        execution_context,
        scope_params,
    )
    logging.info("plt guardrails: passed")
