"""Exception types shared by all joons_allocator components.

The package is independent of joons-agent-guard: it carries its own small
error hierarchy instead of importing the guard's. ``AllocatorError`` is the
base class for every error this package raises; ``InvalidIdentifier`` is
also a ``ValueError`` so plain ``except ValueError`` keeps working.
"""


class AllocatorError(Exception):
    """Base class for every error raised by joons_allocator."""


class InvalidIdentifier(AllocatorError, ValueError):
    """An identifier (task id, worker id, resource, principal, request id, ...)
    is empty or not a string."""


#: Upper bound for every identifier (task id, worker id, resource, principal,
#: request id, digest). Keeps a buggy or hostile caller from growing the
#: shared database without limit.
MAX_IDENTIFIER_CHARS = 1024


def require_ids(**values):
    """Raise InvalidIdentifier unless every value is a non-empty str of at
    most MAX_IDENTIFIER_CHARS."""
    for name, value in values.items():
        if not isinstance(value, str) or not value:
            raise InvalidIdentifier("%s must be a non-empty string" % name)
        if len(value) > MAX_IDENTIFIER_CHARS:
            raise InvalidIdentifier(
                "%s must be at most %d characters" % (name, MAX_IDENTIFIER_CHARS))
