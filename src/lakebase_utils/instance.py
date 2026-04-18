"""Read-only operations for Lakebase Autoscaling instances (control plane).

All methods use the Databricks SDK ``WorkspaceClient.postgres`` service.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from databricks.sdk.service.postgres import Branch, Endpoint, Project

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

        projects = client.instance.list()
    """

    def __init__(self, client: "LakebaseClient") -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, instance_name: str) -> InstanceInfo:
        """Return metadata for a single Lakebase project by ID.

        Parameters
        ----------
        instance_name:
            The project ID (e.g. ``"datascience-foundation-stream"``).
            Internally resolved to the resource name ``projects/{instance_name}``.

        Returns
        -------
        InstanceInfo

        Raises
        ------
        LakebaseNotFoundError
        LakebaseOperationError
        """
        try:
            project = self._client._ws.postgres.get_project(
                name=f"projects/{instance_name}"
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
        return self._to_instance_info(project)

    def list(self) -> list[InstanceInfo]:
        """Return metadata for all Lakebase projects visible in the workspace.

        Raises
        ------
        LakebaseOperationError
        """
        try:
            projects = list(self._client._ws.postgres.list_projects())
        except Exception as exc:
            raise LakebaseOperationError(f"Failed to list projects: {exc}") from exc
        return [self._to_instance_info(p) for p in projects]

    def list_branches(self, project_id: str) -> list[Branch]:
        """Return all branches for a project.

        Parameters
        ----------
        project_id:
            The project ID. Resolved to ``projects/{project_id}``.
        """
        try:
            return list(self._client._ws.postgres.list_branches(
                parent=f"projects/{project_id}"
            ))
        except Exception as exc:
            raise LakebaseOperationError(
                f"Failed to list branches for project {project_id!r}: {exc}"
            ) from exc

    def list_endpoints(self, project_id: str, branch_id: str) -> list[Endpoint]:
        """Return all endpoints for a project branch.

        Each endpoint's ``status.hosts.host`` is the PostgreSQL hostname,
        and ``status.current_state`` shows whether it is ACTIVE or IDLE.

        Parameters
        ----------
        project_id:
            The project ID.
        branch_id:
            The branch ID (e.g. ``"main"``).
        """
        try:
            return list(self._client._ws.postgres.list_endpoints(
                parent=f"projects/{project_id}/branches/{branch_id}"
            ))
        except Exception as exc:
            raise LakebaseOperationError(
                f"Failed to list endpoints for {project_id!r}/{branch_id!r}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_instance_info(self, project: Project) -> InstanceInfo:
        """Convert a :class:`~databricks.sdk.service.postgres.Project` to :class:`InstanceInfo`."""
        status = project.status
        full_name = project.name or ""
        # "projects/{project_id}" → project_id
        project_id = full_name.split("/")[-1] if "/" in full_name else full_name

        tags: dict[str, str] = {}
        if status and status.custom_tags:
            tags = {t.key: t.value for t in status.custom_tags if t.key}

        return InstanceInfo(
            instance_id=project_id,
            name=(status.display_name if status else None) or project_id,
            state="UNKNOWN",  # state lives at endpoint level, not project level
            creator=status.owner if status else None,
            tags=tags,
        )
