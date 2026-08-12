"""The workflow a run performs."""

from dataclasses import dataclass

from engine.run import actions as run_actions


@dataclass(frozen=True, kw_only=True)
class Workflow:
    """One workflow of one run: its members, and what they act as.

    Built from a resolved workflow cfg, so a caller holding one has a workflow
    the catalog already accepted.
    """

    key: str
    action: str
    members: tuple[dict, ...] = ()
    instance_params: dict[str, str] | None = None

    @classmethod
    def from_cfg(cls, key: str, workflow_cfg: dict, *, action: str) -> "Workflow":
        """Build from the mapping the catalog produced."""

        return cls(
            key=key,
            action=action,
            members=tuple(workflow_cfg.get("members") or ()),
            instance_params=workflow_cfg.get("instance_params"),
        )

    @property
    def member_actions(self) -> tuple[str, ...]:
        """Every action its members perform, in declaration order."""

        seen: dict[str, None] = {}
        for member in self.members:
            seen.setdefault(str(member.get("action") or self.action), None)
        return tuple(seen)

    @property
    def is_mutating(self) -> bool:
        """Whether any member changes anything."""

        return any(action in run_actions.MUTATING_ACTIONS for action in self.member_actions)

    def to_document(self) -> dict:
        """Render for the run record."""

        document: dict = {"workflow": self.key, "action": self.action}
        if self.instance_params:
            document["workflow_instance_params"] = self.instance_params
        return document

    def run(self, *, context) -> list[int]:
        """Run one child per member, in declaration order."""

        results = []
        for member in self.members:
            context.banner(f"[{self.action}] [{self.key}]")
            results.append(context.spawn(member))
        return results
