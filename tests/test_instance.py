"""Tests for InstanceManager (control-plane, read-only)."""

import pytest
from unittest.mock import MagicMock

from lakebase_utils.instance import InstanceManager
from lakebase_utils.exceptions import LakebaseNotFoundError, LakebaseOperationError


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def manager(mock_client):
    return InstanceManager(mock_client)


def _make_project(project_id="datascience-foundation-stream", display_name="DataScience Foundation Stream", owner="admin@example.com"):
    project = MagicMock()
    project.name = f"projects/{project_id}"
    project.status.display_name = display_name
    project.status.owner = owner
    project.status.custom_tags = []
    return project


class TestInstanceGet:
    def test_get_returns_instance_info(self, manager, mock_client):
        mock_client._ws.postgres.get_project.return_value = _make_project()
        info = manager.get("datascience-foundation-stream")
        assert info.instance_id == "datascience-foundation-stream"
        assert info.name == "DataScience Foundation Stream"
        assert info.creator == "admin@example.com"

    def test_get_raises_not_found(self, manager, mock_client):
        mock_client._ws.postgres.get_project.side_effect = Exception("NOT_FOUND: project missing")
        with pytest.raises(LakebaseNotFoundError):
            manager.get("missing-project")

    def test_get_raises_operation_error_on_unexpected_failure(self, manager, mock_client):
        mock_client._ws.postgres.get_project.side_effect = Exception("500 internal error")
        with pytest.raises(LakebaseOperationError):
            manager.get("my-project")

    def test_get_calls_sdk_with_correct_name(self, manager, mock_client):
        mock_client._ws.postgres.get_project.return_value = _make_project()
        manager.get("datascience-foundation-stream")
        mock_client._ws.postgres.get_project.assert_called_once_with(
            name="projects/datascience-foundation-stream"
        )

    def test_get_state_is_unknown(self, manager, mock_client):
        mock_client._ws.postgres.get_project.return_value = _make_project()
        info = manager.get("datascience-foundation-stream")
        assert info.state == "UNKNOWN"

    def test_get_tags_populated(self, manager, mock_client):
        project = _make_project()
        tag = MagicMock()
        tag.key = "env"
        tag.value = "prod"
        project.status.custom_tags = [tag]
        mock_client._ws.postgres.get_project.return_value = project
        info = manager.get("datascience-foundation-stream")
        assert info.tags == {"env": "prod"}

    def test_get_no_status(self, manager, mock_client):
        project = MagicMock()
        project.name = "projects/my-project"
        project.status = None
        mock_client._ws.postgres.get_project.return_value = project
        info = manager.get("my-project")
        assert info.instance_id == "my-project"
        assert info.name == "my-project"
        assert info.creator is None


class TestInstanceList:
    def test_list_returns_instances(self, manager, mock_client):
        mock_client._ws.postgres.list_projects.return_value = iter([_make_project()])
        result = manager.list()
        assert len(result) == 1
        assert result[0].name == "DataScience Foundation Stream"

    def test_list_returns_empty(self, manager, mock_client):
        mock_client._ws.postgres.list_projects.return_value = iter([])
        assert manager.list() == []

    def test_list_calls_sdk(self, manager, mock_client):
        mock_client._ws.postgres.list_projects.return_value = iter([])
        manager.list()
        mock_client._ws.postgres.list_projects.assert_called_once_with()

    def test_list_raises_operation_error_on_failure(self, manager, mock_client):
        mock_client._ws.postgres.list_projects.side_effect = Exception("SDK error")
        with pytest.raises(LakebaseOperationError):
            manager.list()


class TestInstanceListBranches:
    def test_list_branches_returns_branches(self, manager, mock_client):
        branch = MagicMock()
        branch.name = "projects/my-project/branches/main"
        mock_client._ws.postgres.list_branches.return_value = iter([branch])
        result = manager.list_branches("my-project")
        assert len(result) == 1
        assert result[0].name == "projects/my-project/branches/main"

    def test_list_branches_calls_sdk_with_parent(self, manager, mock_client):
        mock_client._ws.postgres.list_branches.return_value = iter([])
        manager.list_branches("my-project")
        mock_client._ws.postgres.list_branches.assert_called_once_with(
            parent="projects/my-project"
        )

    def test_list_branches_raises_operation_error_on_failure(self, manager, mock_client):
        mock_client._ws.postgres.list_branches.side_effect = Exception("fail")
        with pytest.raises(LakebaseOperationError):
            manager.list_branches("my-project")


class TestInstanceListEndpoints:
    def test_list_endpoints_returns_endpoints(self, manager, mock_client):
        ep = MagicMock()
        ep.name = "projects/my-project/branches/main/endpoints/primary"
        ep.status.hosts.host = "ep.database.azure.com"
        mock_client._ws.postgres.list_endpoints.return_value = iter([ep])
        result = manager.list_endpoints("my-project", "main")
        assert len(result) == 1
        assert result[0].status.hosts.host == "ep.database.azure.com"

    def test_list_endpoints_calls_sdk_with_parent(self, manager, mock_client):
        mock_client._ws.postgres.list_endpoints.return_value = iter([])
        manager.list_endpoints("my-project", "main")
        mock_client._ws.postgres.list_endpoints.assert_called_once_with(
            parent="projects/my-project/branches/main"
        )

    def test_list_endpoints_raises_operation_error_on_failure(self, manager, mock_client):
        mock_client._ws.postgres.list_endpoints.side_effect = Exception("fail")
        with pytest.raises(LakebaseOperationError):
            manager.list_endpoints("my-project", "main")
