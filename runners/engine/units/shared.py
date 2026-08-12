"""What more than one unit is given."""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class SpawnContext:
    """Ports a unit needs to run children rather than steps."""

    spawn: Callable[[dict], int]
    banner: Callable[[str], None] = lambda _message: None
