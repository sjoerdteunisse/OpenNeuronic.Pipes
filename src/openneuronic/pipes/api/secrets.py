"""In-memory secret store and API-key authentication helpers for the API server.

Secrets are write-only from the outside: values are stored in memory, never
returned in API responses, and never serialised to disk.  Only the key *names*
are enumerable.

API-key authentication uses :func:`hmac.compare_digest` for constant-time
comparison to prevent timing-based enumeration of the key.
"""
from __future__ import annotations

import hmac
import re
from typing import Any

# Allowed key format: 1–128 characters, alphanumeric / underscore / hyphen.
_KEY_RE = re.compile(r"^[a-zA-Z0-9_\-]{1,128}$")


class SecretKeyError(ValueError):
    """Raised when a secret key name does not meet the naming constraints."""


class SecretNotFoundError(KeyError):
    """Raised when a secret referenced in a pipe spec cannot be resolved."""


class SecretStore:
    """Thread-unsafe in-memory store for sensitive configuration values.

    Values are **never** returned via the API — only their key names are
    exposed.  Use :meth:`resolve` inside the factory layer to substitute
    ``{"$secret": "key"}`` references with their real values at pipe-build time.

    Example::

        store = SecretStore()
        store.set("sqlserver_conn", "DRIVER=...;Server=...;PWD=secret")

        # In a pipe spec, reference the secret by name:
        # {"type": "sqlserver", "connection": {"$secret": "sqlserver_conn"}, ...}
        conn = store.resolve({"$secret": "sqlserver_conn"})
        assert conn == "DRIVER=...;Server=...;PWD=secret"
    """

    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def set(self, key: str, value: str) -> None:
        """Store *value* under *key*.

        Raises :class:`SecretKeyError` if *key* does not match
        ``[a-zA-Z0-9_\\-]{1,128}``.
        """
        if not _KEY_RE.fullmatch(key):
            raise SecretKeyError(
                f"Secret key {key!r} is invalid. "
                "Use 1–128 alphanumeric characters, underscores, or hyphens."
            )
        if not isinstance(value, str):
            raise TypeError("Secret values must be plain strings.")
        self._store[key] = value

    def delete(self, key: str) -> None:
        """Remove *key* from the store (no-op if absent)."""
        self._store.pop(key, None)

    # ------------------------------------------------------------------
    # Access
    # ------------------------------------------------------------------

    def keys(self) -> list[str]:
        """Return all stored key names in sorted order.

        Values are intentionally NOT exposed.
        """
        return sorted(self._store.keys())

    def resolve(self, value: Any) -> Any:
        """Resolve a secret reference in *value*.

        If *value* is exactly ``{"$secret": "<key>"}`` the stored secret is
        returned.  Any other value (string, int, nested dict, …) is returned
        unchanged.

        Raises :class:`SecretNotFoundError` when the referenced key is absent.
        """
        if isinstance(value, dict) and list(value) == ["$secret"]:
            key = value["$secret"]
            secret = self._store.get(key)
            if secret is None:
                raise SecretNotFoundError(
                    f"Secret '{key}' is not in the store. "
                    "Set it first via POST /secrets/{key}."
                )
            return secret
        return value

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def __len__(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# API-key verification
# ---------------------------------------------------------------------------


def verify_api_key(provided: str, expected: str) -> bool:
    """Constant-time string comparison to prevent timing-based attacks.

    Both strings are encoded to bytes before passing to
    :func:`hmac.compare_digest`.
    """
    return hmac.compare_digest(
        provided.encode("utf-8"),
        expected.encode("utf-8"),
    )
