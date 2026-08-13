"""The fan-out a run expands into children."""

from dataclasses import dataclass
from typing import Protocol


class ChildRunContext(Protocol):
    """What a fan-out needs to run one of its children."""

    def spawn(self, child: dict) -> tuple[str, int]:
        """Run the child, and answer with its label and its exit code."""


@dataclass(frozen=True, kw_only=True)
class FanOut:
    """A fan-out and the child runs it expands into.

    Built from an expansion the catalog already resolved, so a caller holding
    one has children that exist and a failure mode that is legal.
    """

    key: str
    children: tuple[dict, ...] = ()
    failure_mode: str = "stop"

    @classmethod
    def from_plan(cls, key: str, plan: dict) -> "FanOut":
        """Build from the expansion the catalog produced."""

        return cls(
            key=key,
            children=tuple(plan.get("children") or ()),
            failure_mode=str(plan.get("failure_mode") or "stop"),
        )

    @property
    def child_count(self) -> int:
        """How many children this fan-out drives."""

        return len(self.children)

    def to_document(self) -> dict:
        """Render for the run record."""

        return {"fan_out": self.key, "children": self.child_count}

    def run(self, *, context: ChildRunContext) -> list[str]:
        """Run every child in turn, and return the labels that failed.

        Sequential by construction: each child takes the exclusive ctl-state
        lock, so running them at once would simply fail the second.
        """

        failures: list[str] = []
        for child in self.children:
            label, returncode = context.spawn(child)
            if returncode != 0:
                failures.append(label)
                if self.failure_mode == "stop":
                    break
        return failures
