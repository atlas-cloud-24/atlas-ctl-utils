"""Errors the engine raises that a caller is expected to CATCH.

An error class lives here only when something outside the raising module
handles it; an error raised and never caught stays a plain `RuntimeError` at
the site that knows the message."""


class ProviderConfigBlockedError(RuntimeError):
    """A live check could not be evaluated because an upstream cfg defect (e.g. a
    malformed account id) blocks it. Surfaced per target as 'not_evaluated' with
    the exact blocking reason — never as a genuine identity failure."""
