"""Tests for LakebaseClient auth and initialisation."""

import pytest
from unittest.mock import patch, MagicMock

from lakebase_utils import LakebaseClient, LakebaseAuthError, LakebaseConnectionError


@pytest.fixture
def mock_ws_client():
    with patch("lakebase_utils.client.WorkspaceClient"), \
         patch("lakebase_utils.client.Config"):
        yield


class TestLakebaseClientInit:
    def test_raises_when_no_host(self):
        with pytest.raises(LakebaseAuthError, match="host"):
            LakebaseClient()

    def test_accepts_token(self, mock_ws_client):
        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        assert client is not None

    def test_accepts_service_principal(self, mock_ws_client):
        client = LakebaseClient(
            host="https://host.azuredatabricks.net",
            client_id="sp-id",
            client_secret="sp-secret",
        )
        assert client is not None

    def test_host_from_env(self, mock_ws_client, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "https://host.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-env-token")
        client = LakebaseClient()
        assert client is not None


class TestPgConnection:
    def test_raises_when_pg_host_not_set(self, mock_ws_client):
        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        with pytest.raises(LakebaseConnectionError, match="pg_host"):
            with client.pg_connection():
                pass

    def test_set_pg_endpoint(self, mock_ws_client):
        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        client.set_pg_endpoint("pg.lakebase.example.com", port=5432)
        assert client._pg_host == "pg.lakebase.example.com"
        assert client._pg_port == 5432


class TestSubManagerAccess:
    def test_instance_property(self, mock_ws_client):
        from lakebase_utils.instance import InstanceManager

        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        assert isinstance(client.instance, InstanceManager)

    def test_databases_property(self, mock_ws_client):
        from lakebase_utils.database import DatabaseManager

        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        assert isinstance(client.databases, DatabaseManager)

    def test_schemas_property(self, mock_ws_client):
        from lakebase_utils.schema import SchemaManager

        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        assert isinstance(client.schemas, SchemaManager)

    def test_tables_property(self, mock_ws_client):
        from lakebase_utils.table import TableManager

        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        assert isinstance(client.tables, TableManager)
