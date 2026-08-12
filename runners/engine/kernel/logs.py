"""Engine logging setup.

One function, deliberately alone: how the engine logs is a process-wide
decision made once at start-up, and burying it in a module with other
responsibilities makes it look like something a caller may do repeatedly."""

import logging
import logging.handlers
import sys

from engine.kernel import process as kernel_process


def setup_logging() -> logging.handlers.MemoryHandler:
    """Setup logging with memory handler to capture early logs."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    memory_handler = logging.handlers.MemoryHandler(capacity=1000, flushLevel=logging.CRITICAL)
    logging.getLogger().addHandler(memory_handler)
    logging.info("Command: %s", " ".join(kernel_process.redact_command_argv(sys.argv)))
    return memory_handler
