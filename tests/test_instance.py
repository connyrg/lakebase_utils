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


_RAW_INSTANCE = {
    "instance_id": "inst-123",
    "name": "my-lakebase",
    "state": "RUNNING",
    "endpoint": {"host": "lb.example.com", "port": 5432},
    "capacity": {"min": 1, "max": 4},
    "creator_user_name": "admin@example.com",
    "custom_tags": {"env": "prod"},
}


class TestInstanceGet:
    def test_get_returns_instance_info(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = _RAW_INSTANCE
        info = manager.get("my-lakebase")
        assert info.instance_id == "inst-123"
        assert info.name == "my-lakebase"
        assert info.state == "RUNNING"
        assert info.pg_host == "lb.example.com"
        assert info.pg_port == 5432
        assert info.capacity_min == 1
        assert info.capacity_max == 4
        assert info.creator == "admin@example.com"
        assert info.tags == {"env": "prod"}

    def test_get_raises_not_found(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("NOT_FOUND: instance missing")
        with pytest.raises(LakebaseNotFoundError):
            manager.get("missing")

    def test_get_raises_operation_error_on_unexpected_failure(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("500 internal error")
        with pytest.raises(LakebaseOperationError):
            manager.get("my-lakebase")

    def test_get_calls_correct_endpoint(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = _RAW_INSTANCE
        manager.get("my-lakebase")
        mock_client._ws.api_client.do.assert_called_once_with(
            "GET", "/api/2.0/lakebase/instances/my-lakebase"
        )


class TestInstanceList:
    def test_list_returns_instances(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {"instances": [_RAW_INSTANCE]}
        result = manager.list()
        assert len(result) == 1
        assert result[0].name == "my-lakebase"

    def test_list_returns_empty(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {"instances": []}
        assert manager.list() == []

    def test_list_returns_empty_when_key_missing(self, manager, mock_client):
        mock_client._ws.api_client.do.return_value = {}
        assert manager.list() == []

    def test_list_raises_operation_error_on_failure(self, manager, mock_client):
        mock_client._ws.api_client.do.side_effect = Exception("SDK error")
        with pytest.raises(LakebaseOperationError):
            manager.list()
