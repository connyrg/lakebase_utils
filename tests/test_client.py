"""Tests for LakebaseClient auth and initialisation."""

import pytest
import psycopg2
from unittest.mock import patch, MagicMock, call

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

    def test_no_static_pg_credentials_stored(self, mock_ws_client):
        client = LakebaseClient(host="https://host.azuredatabricks.net", token="dapi123")
        assert not hasattr(client, "_pg_user")
        assert not hasattr(client, "_pg_password")


_PG_ENDPOINT = "projects/my-project/branches/main/endpoints/primary"


class TestGeneratePgCredentials:
    def test_returns_user_and_token(self, mock_ws_client):
        client = LakebaseClient(
            host="https://host.azuredatabricks.net", token="dapi123",
            pg_host="pg.example.com", pg_endpoint=_PG_ENDPOINT,
        )
        mock_cred = MagicMock()
        mock_cred.token = "short-lived-tok"
        client._ws.postgres.generate_database_credential.return_value = mock_cred
        user, password = client._generate_pg_credentials()
        assert user == "oauth2"
        assert password == "short-lived-tok"

    def test_calls_sdk_with_endpoint(self, mock_ws_client):
        client = LakebaseClient(
            host="https://host.azuredatabricks.net", token="dapi123",
            pg_host="pg.example.com", pg_endpoint=_PG_ENDPOINT,
        )
        mock_cred = MagicMock()
        mock_cred.token = "tok"
        client._ws.postgres.generate_database_credential.return_value = mock_cred
        client._generate_pg_credentials()
        client._ws.postgres.generate_database_credential.assert_called_once_with(
            endpoint=_PG_ENDPOINT
        )

    def test_raises_auth_error_when_pg_endpoint_not_set(self, mock_ws_client):
        client = LakebaseClient(
            host="https://host.azuredatabricks.net", token="dapi123", pg_host="pg.example.com"
        )
        with pytest.raises(LakebaseAuthError, match="pg_endpoint"):
            client._generate_pg_credentials()

    def test_raises_auth_error_on_sdk_failure(self, mock_ws_client):
        client = LakebaseClient(
            host="https://host.azuredatabricks.net", token="dapi123",
            pg_host="pg.example.com", pg_endpoint=_PG_ENDPOINT,
        )
        client._ws.postgres.generate_database_credential.side_effect = Exception("403 Forbidden")
        with pytest.raises(LakebaseAuthError, match="generate database credentials"):
            client._generate_pg_credentials()

    def test_fresh_credentials_per_connection(self, mock_ws_client):
        """Each pg_connection() call generates new credentials — never cached."""
        client = LakebaseClient(
            host="https://host.azuredatabricks.net", token="dapi123",
            pg_host="pg.example.com", pg_endpoint=_PG_ENDPOINT,
        )
        mock_cred = MagicMock()
        mock_cred.token = "tok"
        client._ws.postgres.generate_database_credential.return_value = mock_cred

        with patch("psycopg2.connect") as mock_connect:
            mock_connect.return_value.autocommit = True
            with client.pg_connection():
                pass
            with client.pg_connection():
                pass

        assert client._ws.postgres.generate_database_credential.call_count == 2


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

    def test_uses_generated_credentials(self, mock_ws_client):
        client = LakebaseClient(
            host="https://host.azuredatabricks.net", token="dapi123",
            pg_host="pg.example.com", pg_endpoint=_PG_ENDPOINT,
        )
        mock_cred = MagicMock()
        mock_cred.token = "db-token-xyz"
        client._ws.postgres.generate_database_credential.return_value = mock_cred

        with patch("psycopg2.connect") as mock_connect:
            mock_connect.return_value.autocommit = True
            with client.pg_connection(database="analytics"):
                pass

        mock_connect.assert_called_once_with(
            host="pg.example.com",
            port=5432,
            dbname="analytics",
            user="oauth2",
            password="db-token-xyz",
            connect_timeout=10,
        )


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
