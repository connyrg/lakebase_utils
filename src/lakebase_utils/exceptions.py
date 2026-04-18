"""Exceptions raised by lakebase-utils."""


class LakebaseError(Exception):
    """Base exception for all lakebase-utils errors."""


class LakebaseConnectionError(LakebaseError):
    """Failed to connect to the Lakebase instance (control plane or data plane)."""


class LakebaseAuthError(LakebaseError):
    """Authentication or authorisation failure."""


class LakebaseNotFoundError(LakebaseError):
    """Requested resource (instance, database, schema, or table) does not exist."""


class LakebaseAlreadyExistsError(LakebaseError):
    """Resource already exists and the operation does not allow overwriting."""


class LakebaseOperationError(LakebaseError):
    """A control-plane or data-plane operation failed for a non-auth, non-existence reason."""
