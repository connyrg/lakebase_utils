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
        try:
            raw = self._client._ws.api_client.do(
                "GET", f"/api/2.0/lakebase/instances/{instance_name}"
            )
        except Exception as exc:
            msg = str(exc).upper()
            if any(k in msg for k in ("NOT_FOUND", "404", "RESOURCE_DOES_NOT_EXIST", "DOES_NOT_EXIST")):
                raise LakebaseNotFoundError(
                    f"Lakebase instance {instance_name!r} not found."
                ) from exc
            raise LakebaseOperationError(
                f"Failed to fetch instance {instance_name!r}: {exc}"
            ) from exc
        return self._to_instance_info(raw)

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
        try:
            resp = self._client._ws.api_client.do("GET", "/api/2.0/lakebase/instances")
        except Exception as exc:
            raise LakebaseOperationError(f"Failed to list instances: {exc}") from exc
        return [self._to_instance_info(r) for r in resp.get("instances", [])]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_instance_info(self, raw: object) -> InstanceInfo:
        """Convert a raw Databricks SDK response object to :class:`InstanceInfo`."""
        endpoint = raw.get("endpoint") or {}
        capacity = raw.get("capacity") or {}
        return InstanceInfo(
            instance_id=raw.get("instance_id") or raw.get("id", ""),
            name=raw.get("name", ""),
            state=raw.get("state", "UNKNOWN"),
            pg_host=endpoint.get("host", ""),
            pg_port=int(endpoint.get("port", 5432)),
            capacity_min=capacity.get("min"),
            capacity_max=capacity.get("max"),
            creator=raw.get("creator_user_name") or raw.get("creator"),
            tags=raw.get("custom_tags") or {},
        )
