"""The step a source repository declares."""

import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Protocol


class StepImage(StrEnum):
    """The box a step runs in."""

    INFRA = "infra"
    OPS = "ops"


class StepContext(Protocol):
    """What a step is given, so it reaches for nothing.

    A protocol rather than a record of `Callable`s: an argument name is part of
    a port's contract, and `Callable[..., None]` cannot state one.
    """

    execution_runtime_mode: str
    execution_context_filename: str
    dispatcher: str
    tooling_mode: str
    origin_cfg_path: Path
    target_cfg_dir: Path
    target_artifacts_dir: Path

    def box_name(self, step_id: str) -> str:
        """The name of the box this step runs in."""

    def launch(self, argv: list[str], env: dict[str, str]) -> None:
        """Run the dispatcher, streaming its output into this run's log."""

    def rebind_credentials(self, step: "Step", env: dict[str, str]) -> None:
        """Refresh the step's provider credentials into `env`, if this run refreshes per step."""


@dataclass(frozen=True, kw_only=True)
class StepRuntime:
    """How a step is run, as the step declares it."""

    image: StepImage
    docker_build: bool = False
    values_json: bool = True
    env_sh: bool = True
    supported_execution_runtime_modes: tuple[str, ...] = ()

    def to_document(self) -> dict:
        """Render for the run record."""

        return {
            "image": str(self.image),
            "docker_build": self.docker_build,
            "values_json": self.values_json,
            "env_sh": self.env_sh,
            "supported_execution_runtime_modes": sorted(self.supported_execution_runtime_modes),
        }


@dataclass(frozen=True, kw_only=True)
class Step:
    """One step of a procedure: its signature, its box, and where it lives.

    Built once from validated cfg, so a caller holding one never re-checks it.
    """

    id: str
    path: str
    providers: tuple[str, ...]
    cfg_keys: dict[str, dict[str, str]]
    runtime: StepRuntime
    env_vars: dict[str, dict] = field(default_factory=dict)

    def environment(self, base: dict[str, str], *, context: StepContext) -> dict[str, str]:
        """The environment this step runs with."""

        env = dict(base)
        env["cfg_keys"] = json.dumps(self.cfg_keys)
        env["step_dir"] = self.path
        env["STEP_WRITE_VALUES_JSON"] = "true" if self.runtime.values_json else "false"
        env["STEP_WRITE_ENV_SH"] = "true" if self.runtime.env_sh else "false"
        env["ATLAS_STEP_IMAGE"] = str(self.runtime.image)
        env["ATLAS_STEP_DOCKER_BUILD"] = "true" if self.runtime.docker_build else "false"
        env["ATLAS_EXECUTION_CONTEXT_FILE"] = context.execution_context_filename
        env["ATLAS_EXECUTION_RUNTIME_MODE"] = context.execution_runtime_mode
        env["ATLAS_STEP_NAME"] = context.box_name(self.id)
        env["origin_cfg_base_dir_path"] = str(context.origin_cfg_path)
        env["TARGET_CFG_DIR"] = str(context.target_cfg_dir)
        env["TARGET_ARTIFACTS_DIR"] = str(context.target_artifacts_dir)
        env["local_step_tooling_mode"] = context.tooling_mode
        return env

    def run(self, base_env: dict[str, str], *, context: StepContext) -> None:
        """Run this step, refusing a runtime it does not declare."""

        if not self.supports(context.execution_runtime_mode):
            raise RuntimeError(
                f"❌ execution runtime {context.execution_runtime_mode!r} not supported by "
                f"step {self.id!r} (supported: "
                f"{sorted(self.runtime.supported_execution_runtime_modes)})"
            )
        env = self.environment(base_env, context=context)
        context.rebind_credentials(self, env)
        context.launch([context.dispatcher], env)

    def supports(self, execution_runtime_mode: str) -> bool:
        """Whether this step declares it can run in that mode."""

        return execution_runtime_mode in self.runtime.supported_execution_runtime_modes

    def to_document(self) -> dict:
        """Render for the run record."""

        return {
            "id": self.id,
            "path": self.path,
            "providers": sorted(self.providers),
            "cfg_keys": self.cfg_keys,
            "runtime": self.runtime.to_document(),
            "env_vars": self.env_vars,
        }
