"""Read-only operations for Lakebase Autoscaling instances (control plane).

All methods use the Databricks SDK WorkspaceClient and do not modify
any instance or project configuration — the platform team owns that.

API base path: /api/2.0/postgres/
Resource hierarchy: projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from .exceptions import LakebaseNotFoundError, LakebaseOperationError
from .models import InstanceInfo

if TYPE_CHECKING:
    from .client import LakebaseClient


class InstanceManager:
    """Read-only view of Lakebase Autoscaling projects.

    Obtained via ``client.instance``.

    Example::

        client = LakebaseClient(host="...", token="...")
        info = client.instance.get("my-project")
        print(info.state, info.creator)

        instances = client.instance.list()
    """

    def __init__(self, client: "LakebaseClient") -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, instance_name: str) -> InstanceInfo:
        """Return metadata for a single Lakebase project by name/ID.

        Parameters
        ----------
        instance_name:
            The project ID (e.g. ``"datascience-foundation-stream"``).

        Returns
        -------
        InstanceInfo
            Metadata including state and creator.

        Raises
        ------
        LakebaseNotFoundError
            If no project with that ID exists in the workspace.
        LakebaseOperationError
            If the Databricks API call fails for any other reason.
        """
        try:
            raw = self._client._ws.api_client.do(
                "GET", f"/api/2.0/postgres/projects/{instance_name}"
            )
        except Exception as exc:
            msg = str(exc).upper()
            if any(k in msg for k in ("NOT_FOUND", "404", "RESOURCE_DOES_NOT_EXIST", "DOES_NOT_EXIST")):
                raise LakebaseNotFoundError(
                    f"Lakebase project {instance_name!r} not found."
                ) from exc
            raise LakebaseOperationError(
                f"Failed to fetch project {instance_name!r}: {exc}"
            ) from exc
        return self._to_instance_info(raw)

    def list(self) -> list[InstanceInfo]:
        """Return metadata for all Lakebase projects visible in the workspace.

        Returns
        -------
        list[InstanceInfo]
            May be empty if no projects exist or the caller has no access.

        Raises
        ------
        LakebaseOperationError
            If the Databricks API call fails.
        """
        try:
            resp = self._client._ws.api_client.do("GET", "/api/2.0/postgres/projects")
        except Exception as exc:
            raise LakebaseOperationError(f"Failed to list projects: {exc}") from exc
        return [self._to_instance_info(r) for r in resp.get("projects", [])]

    def list_branches(self, project_id: str) -> list[dict]:
        """Return all branches for a project.

        Parameters
        ----------
        project_id:
            The project ID.
        """
        try:
            resp = self._client._ws.api_client.do(
                "GET", f"/api/2.0/postgres/projects/{project_id}/branches"
            )
        except Exception as exc:
            raise LakebaseOperationError(
                f"Failed to list branches for project {project_id!r}: {exc}"
            ) from exc
        return resp.get("branches", [])

    def list_endpoints(self, project_id: str, branch_id: str) -> list[dict]:
        """Return all endpoints for a project branch.

        Each endpoint has ``status.host`` and ``status.port`` for connecting.

        Parameters
        ----------
        project_id:
            The project ID.
        branch_id:
            The branch ID (e.g. ``"main"``).
        """
        try:
            resp = self._client._ws.api_client.do(
                "GET",
                f"/api/2.0/postgres/projects/{project_id}/branches/{branch_id}/endpoints",
            )
        except Exception as exc:
            raise LakebaseOperationError(
                f"Failed to list endpoints for {project_id!r}/{branch_id!r}: {exc}"
            ) from exc
        return resp.get("endpoints", [])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_instance_info(self, raw: object) -> InstanceInfo:
        """Convert a raw Databricks API response to :class:`InstanceInfo`.

        Response shape (GET /api/2.0/postgres/projects/{project_id})::

            {
              "name": "projects/{project_id}",
              "status": {
                "display_name": "...",
                "state": "READY",
                "creator": "user@example.com",
                "pg_version": 17
              }
            }
        """
        status = raw.get("status") or {}
        full_name = raw.get("name", "")
        # "projects/{project_id}" → project_id
        project_id = full_name.split("/")[-1] if "/" in full_name else full_name
        return InstanceInfo(
            instance_id=project_id,
            name=status.get("display_name") or project_id,
            state=status.get("state", "UNKNOWN"),
            creator=status.get("creator"),
        )
