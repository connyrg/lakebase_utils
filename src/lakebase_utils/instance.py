"""Read-only operations for Lakebase Autoscaling instances (control plane).

All methods use the Databricks SDK WorkspaceClient and do not modify
any instance or project configuration — the platform team owns that.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from .exceptions import LakebaseNotFoundError, LakebaseOperationError
from .models import InstanceInfo

if TYPE_CHECKING:
    from .client import LakebaseClient


class InstanceManager:
    """Read-only view of Lakebase Autoscaling instances.

    Obtained via ``client.instance``.

    Example::

        client = LakebaseClient(host="...", token="...")
        info = client.instance.get("my-lakebase")
        print(info.state, info.pg_host)

        instances = client.instance.list()
    """

    def __init__(self, client: "LakebaseClient") -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, instance_name: str) -> InstanceInfo:
        """Return metadata for a single Lakebase instance by name.

        Parameters
        ----------
        instance_name:
            The name of the Lakebase Autoscaling instance.

        Returns
        -------
        InstanceInfo
            Metadata including state, PostgreSQL endpoint, and capacity.

        Raises
        ------
        LakebaseNotFoundError
            If no instance with that name exists in the workspace.
        LakebaseOperationError
            If the Databricks SDK call fails for any other reason.
        """
        raise NotImplementedError

    def list(self) -> list[InstanceInfo]:
        """Return metadata for all Lakebase instances visible in the workspace.

        Returns
        -------
        list[InstanceInfo]
            May be empty if no instances exist or the caller has no access.

        Raises
        ------
        LakebaseOperationError
            If the Databricks SDK call fails.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_instance_info(self, raw: object) -> InstanceInfo:
        """Convert a raw Databricks SDK response object to :class:`InstanceInfo`.

        Parameters
        ----------
        raw:
            The SDK response object returned by the lakebase API.
        """
        raise NotImplementedError
