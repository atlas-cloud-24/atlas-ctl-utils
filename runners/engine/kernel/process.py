"""Running a child process and reporting what it did.

Output handling belongs with execution, not with logging: the redaction, the
ANSI stripping and the error extraction all exist because a child process
writes for a human terminal and the engine has to store it."""

import logging
import logging.handlers
import re
import subprocess
from pathlib import Path

# ANSI escape code pattern
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    """Remove ANSI color codes from text."""

    return ANSI_ESCAPE.sub("", text)


def run_and_log(cmd, shell=False, cwd=None, env=None, check=True):
    """Run subprocess and log all output in real-time."""

    process = subprocess.Popen(
        cmd,
        shell=shell,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,  # Line buffered
    )

    # Stream output in real-time
    for line in process.stdout:
        line_stripped = line.rstrip()
        # Print colored output to terminal
        print(f"  {line_stripped}", flush=True)
        # Log clean output to file (strip ANSI codes)
        clean_line = strip_ansi(line_stripped)
        # Only log to file handlers, not console
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                handler.emit(
                    logging.LogRecord(
                        name=logging.getLogger().name,
                        level=logging.INFO,
                        pathname="",
                        lineno=0,
                        msg=f"  {clean_line}",
                        args=(),
                        exc_info=None,
                    )
                )

    # Wait for process to complete
    returncode = process.wait()

    if check and returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)

    return returncode


def redact_command_argv(argv: list[str]) -> list[str]:
    """Redact opaque credential selectors before command lines reach logs.

    Provider options are the one place a credential selector can be typed, and
    the engine cannot tell which of an adapter's keys is sensitive — so the
    VALUE of every provider option is redacted, whatever the key.
    """
    redacted: list[str] = []
    hide_next = False
    for value in argv:
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        if value == "--provider-options":
            redacted.append(value)
            hide_next = True
            continue
        if value.startswith("--provider-options="):
            redacted.append("--provider-options=<redacted>")
            continue
        redacted.append(value)
    return redacted


def tail_log_lines(log_path: str | None, limit: int = 40) -> list[str]:
    if not log_path:
        return []
    path = Path(log_path)
    if not path.is_file():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    return lines[-limit:]


def extract_error_summary(log_path: str | None, fallback: str) -> dict:
    tail = tail_log_lines(log_path)
    summary = fallback
    for line in reversed(tail):
        stripped = line.strip()
        if not stripped:
            continue
        if "Error:" in stripped or "CalledProcessError" in stripped or "failed" in stripped.lower():
            summary = stripped
            break
    return {"summary": summary, "tail_lines": tail}


def step_box_name(target_run_id: str, repo_step_id: str) -> str:
    """A valid, unique-per-run Docker tag / box name for a target_run."""

    raw = f"atlas-{target_run_id}-{repo_step_id}-target_run-local"
    name = re.sub(r"[^a-z0-9._-]+", "-", raw.lower())
    return re.sub(r"-{2,}", "-", name).strip("-")
