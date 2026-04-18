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


_RAW_PROJECT = {
    "name": "projects/datascience-foundation-stream",
    "status": {
        "display_name": "DataScience Foundation Stream",
        "state": "READY",
        "creator": "admin@example.com",
        "pg_version": 17,
    },
}


class TestInstanceGet:
    def test_get_returns_instance_info(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = _RAW_PROJECT
        info = manager.get("datascience-foundation-stream")
        assert info.instance_id == "datascience-foundation-stream"
        assert info.name == "DataScience Foundation Stream"
        assert info.state == "READY"
        assert info.creator == "admin@example.com"

    def test_get_raises_not_found(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("NOT_FOUND: project missing")
        with pytest.raises(LakebaseNotFoundError):
            manager.get("missing-project")

    def test_get_raises_operation_error_on_unexpected_failure(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("500 internal error")
        with pytest.raises(LakebaseOperationError):
            manager.get("my-project")

    def test_get_calls_correct_endpoint(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = _RAW_PROJECT
        manager.get("datascience-foundation-stream")
        mock_client._ws.api_client.do.assert_called_once_with(
            "GET", "/api/2.0/postgres/projects/datascience-foundation-stream"
        )


class TestInstanceList:
    def test_list_returns_instances(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {"projects": [_RAW_PROJECT]}
        result = manager.list()
        assert len(result) == 1
        assert result[0].name == "DataScience Foundation Stream"

    def test_list_returns_empty(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {"projects": []}
        assert manager.list() == []

    def test_list_returns_empty_when_key_missing(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {}
        assert manager.list() == []

    def test_list_raises_operation_error_on_failure(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("SDK error")
        with pytest.raises(LakebaseOperationError):
            manager.list()

    def test_list_calls_correct_endpoint(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {"projects": []}
        manager.list()
        mock_client._ws.api_client.do.assert_called_once_with(
            "GET", "/api/2.0/postgres/projects"
        )


class TestInstanceListBranches:
    def test_list_branches_returns_branches(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {
            "branches": [{"name": "projects/my-project/branches/main"}]
        }
        result = manager.list_branches("my-project")
        assert len(result) == 1

    def test_list_branches_raises_operation_error_on_failure(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("fail")
        with pytest.raises(LakebaseOperationError):
            manager.list_branches("my-project")


class TestInstanceListEndpoints:
    def test_list_endpoints_returns_endpoints(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {
            "endpoints": [
                {
                    "name": "projects/my-project/branches/main/endpoints/primary",
                    "status": {"host": "ep.database.azure.com", "port": 5432},
                }
            ]
        }
        result = manager.list_endpoints("my-project", "main")
        assert len(result) == 1
        assert result[0]["status"]["host"] == "ep.database.azure.com"

    def test_list_endpoints_raises_operation_error_on_failure(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("fail")
        with pytest.raises(LakebaseOperationError):
            manager.list_endpoints("my-project", "main")
