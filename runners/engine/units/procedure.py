"""The repo-local procedure a run performs."""

from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class Procedure:
    """A synthetic target declared by a source repo rather than by ctl cfg."""

    key: str
    action: str
    source: str = ""
    ref: str = ""
    domain: str = ""
    affected_target_keys: tuple[str, ...] = ()

    @classmethod
    def from_args(cls, args) -> "Procedure":
        """Build from the arguments a procedure run was invoked with."""

        return cls(
            key=str(getattr(args, "procedure", "") or ""),
            action=str(getattr(args, "action", "") or ""),
            source=str(getattr(args, "source", "") or ""),
            ref=str(getattr(args, "ref", "") or ""),
            domain=str(getattr(args, "domain", "") or ""),
            affected_target_keys=tuple(getattr(args, "affected_target_keys", ()) or ()),
        )

    @property
    def affects_targets(self) -> bool:
        """Whether this procedure names targets it may change."""

        return bool(self.affected_target_keys)

    def to_document(self) -> dict:
        """Render for the run record."""

        document = {"procedure": self.key, "action": self.action}
        for name, value in (("source", self.source), ("ref", self.ref), ("domain", self.domain)):
            if value:
                document[name] = value
        if self.affected_target_keys:
            document["affected_target_keys"] = list(self.affected_target_keys)
        return document

    def run(self, *, context) -> int:
        """Run this repo-local procedure as one synthetic target."""

        context.banner(f"[{self.action}] [{self.key}]")
        return context.spawn(self.to_document())
