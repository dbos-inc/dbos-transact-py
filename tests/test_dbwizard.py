import os
from unittest.mock import mock_open, patch

import pytest
import yaml

from dbos._db_wizard import db_wizard
from dbos._dbos_config import ConfigFile
from dbos._error import DBOSInitializationError


class TestDbWizardIntegration:
    """Integration test suite for the db_wizard function with a real PostgreSQL database."""

    @pytest.fixture
    def standard_config(self) -> ConfigFile:
        """Standard configuration with default database settings."""
        return {
            "name": "test-app",
            "database": {
                "hostname": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": os.environ.get("PGPASSWORD", "dbos"),
                "app_db_name": "postgres",
            },
        }

    @pytest.fixture
    def non_default_config(self) -> ConfigFile:
        """Configuration with non-default database settings."""
        return {
            "name": "test-app",
            "database": {
                "hostname": "non-existent-host",
                "port": 5433,
                "username": "non-existent-user",
                "password": "wrong-password",
                "app_db_name": "postgres",
            },
        }

    @pytest.fixture
    def wrong_password_config(self) -> ConfigFile:
        """Configuration with correct host/port but wrong password."""
        return {
            "name": "test-app",
            "database": {
                "hostname": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": "wrong",
                "app_db_name": "postgres",
            },
        }

    def test_successful_connection_postgres_db(
        self, standard_config: ConfigFile
    ) -> None:
        """Test when connection to postgres database is successful."""
        result = db_wizard(standard_config)
        assert result == standard_config

    '''
    Unfortunately psycopg gets a timeout error, even w/ the wrong password...
    def test_wrong_password(self, wrong_password_config):
        """Test with wrong password."""
        with pytest.raises(DBOSInitializationError) as exc_info:
            db_wizard(wrong_password_config, "config.yaml")
        assert "password authentication failed" in str(exc_info.value)
    '''

    def test_non_default_config_settings(self, non_default_config: ConfigFile) -> None:
        """Test with non-default database configuration."""
        mock_file_content = yaml.dump(
            {"name": "test-app"}
        )  # no db config in config file
        with patch("builtins.open", mock_open(read_data=mock_file_content)):
            # Should raise error because config has non-default database settings
            with pytest.raises(DBOSInitializationError) as exc_info:
                db_wizard(non_default_config)

            # Verify error message
            assert "Could not connect to the database" in str(exc_info.value)
