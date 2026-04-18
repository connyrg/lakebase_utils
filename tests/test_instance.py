"""Tests for InstanceManager (control-plane, read-only)."""

import pytest
from unittest.mock import patch

from lakebase_utils.instance import InstanceManager


@pytest.fixture
def manager(mock_client):
    return InstanceManager(mock_client)


@pytest.fixture
def mock_client():
    from unittest.mock import MagicMock
    return MagicMock()


class TestInstanceGet:
    def test_get_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.get("my-lakebase")


class TestInstanceList:
    def test_list_raises_not_implemented(self, manager):
        with pytest.raises(NotImplementedError):
            manager.list()
