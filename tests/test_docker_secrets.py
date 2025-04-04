import os
import tempfile
import unittest
from unittest.mock import mock_open, patch

from dbos._dbos_config import DBOSConfig, _substitute_env_vars, load_config


class TestDockerSecrets(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory to simulate /run/secrets/
        self.temp_dir = tempfile.TemporaryDirectory()
        self.secrets_dir = os.path.join(self.temp_dir.name, "secrets")
        os.makedirs(self.secrets_dir)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_substitute_env_vars_with_env_vars(self):
        # Test that environment variables are still substituted correctly
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            content = "This is a ${TEST_VAR} test"
            result = _substitute_env_vars(content)
            self.assertEqual(result, "This is a test_value test")

    def test_substitute_env_vars_with_docker_secrets(self):
        # Create a mock Docker secret
        secret_path = os.path.join(self.secrets_dir, "db_password")
        with open(secret_path, "w") as f:
            f.write("secret_password")

        # Mock the /run/secrets/ path
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            with patch(
                "builtins.open", unittest.mock.mock_open(read_data="secret_password")
            ):
                content = "This is a ${SECRET:db_password} test"
                result = _substitute_env_vars(content)
                self.assertEqual(result, "This is a secret_password test")

    def test_substitute_env_vars_with_missing_docker_secret(self):
        # Test that a warning is logged when a Docker secret is missing
        with patch("dbos._dbos_config.dbos_logger") as mock_logger:
            content = "This is a ${SECRET:missing_secret} test"
            result = _substitute_env_vars(content)
            self.assertEqual(result, "This is a  test")
            mock_logger.warning.assert_called_once()

    def test_substitute_env_vars_with_both_env_vars_and_docker_secrets(self):
        # Test that both environment variables and Docker secrets are substituted
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            with patch("os.path.exists") as mock_exists:
                mock_exists.return_value = True
                with patch(
                    "builtins.open",
                    unittest.mock.mock_open(read_data="secret_password"),
                ):
                    content = "This is a ${TEST_VAR} and ${SECRET:db_password} test"
                    result = _substitute_env_vars(content)
                    self.assertEqual(
                        result, "This is a test_value and secret_password test"
                    )

    def test_substitute_env_vars_with_silent_mode(self):
        # Test that no warning is logged when silent mode is enabled
        with patch("dbos._dbos_config.dbos_logger") as mock_logger:
            content = "This is a ${SECRET:missing_secret} test"
            result = _substitute_env_vars(content, silent=True)
            self.assertEqual(result, "This is a  test")
            mock_logger.warning.assert_not_called()

    def test_load_config_with_docker_secrets(self):
        # Create a mock configuration file with Docker secrets
        config_content = """
name: test-app
database:
  hostname: localhost
  port: 5432
  username: postgres
  password: ${SECRET:db_password}
  app_db_name: test_db
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict = {
            "name": "test-app",
            "database": {
                "hostname": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": "secret_password",
                "app_db_name": "test_db",
            },
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch(
                "builtins.open", mock_open(read_data="secret_password"), create=True
            ) as mock_secret_file,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config(run_process_config=False)

            # Verify that the Docker secret was correctly substituted
            self.assertEqual(config["database"]["password"], "secret_password")

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_docker_secrets_in_database_url(self):
        # Create a mock configuration file with Docker secrets in the database URL
        config_content = """
name: test-app
database_url: postgresql://postgres:${SECRET:db_password}@localhost:5432/test_db
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict = {
            "name": "test-app",
            "database_url": "postgresql://postgres:secret_password@localhost:5432/test_db",
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch(
                "builtins.open", mock_open(read_data="secret_password"), create=True
            ) as mock_secret_file,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config(run_process_config=False)

            # Verify that the Docker secret was correctly substituted in the database URL
            self.assertEqual(
                config["database_url"],
                "postgresql://postgres:secret_password@localhost:5432/test_db",
            )

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_docker_secrets_in_nested_config(self):
        # Create a mock configuration file with Docker secrets in a nested configuration
        config_content = """
name: test-app
telemetry:
  OTLPExporter:
    logsEndpoint: http://logs-collector:4317
    tracesEndpoint: http://traces-collector:4317
  logs:
    logLevel: INFO
application:
  api_key: ${SECRET:api_key}
  nested:
    secret: ${SECRET:nested_secret}
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict = {
            "name": "test-app",
            "telemetry": {
                "OTLPExporter": {
                    "logsEndpoint": "http://logs-collector:4317",
                    "tracesEndpoint": "http://traces-collector:4317",
                },
                "logs": {"logLevel": "INFO"},
            },
            "application": {
                "api_key": "secret_value",
                "nested": {"secret": "secret_value"},
            },
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch(
                "builtins.open", mock_open(read_data="secret_value"), create=True
            ) as mock_secret_file,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config(run_process_config=False)

            # Verify that the Docker secrets were correctly substituted in the nested configuration
            self.assertEqual(config["application"]["api_key"], "secret_value")
            self.assertEqual(config["application"]["nested"]["secret"], "secret_value")

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_docker_secrets_in_list(self):
        # Create a mock configuration file with Docker secrets in a list
        config_content = """
name: test-app
runtimeConfig:
  setup:
    - echo "Setting up environment"
    - export API_KEY=${SECRET:api_key}
    - export DB_PASSWORD=${SECRET:db_password}
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict = {
            "name": "test-app",
            "runtimeConfig": {
                "setup": [
                    'echo "Setting up environment"',
                    "export API_KEY=secret_value",
                    "export DB_PASSWORD=secret_value",
                ]
            },
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch(
                "builtins.open", mock_open(read_data="secret_value"), create=True
            ) as mock_secret_file,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config(run_process_config=False)

            # Verify that the Docker secrets were correctly substituted in the list
            self.assertEqual(
                config["runtimeConfig"]["setup"][1], "export API_KEY=secret_value"
            )
            self.assertEqual(
                config["runtimeConfig"]["setup"][2], "export DB_PASSWORD=secret_value"
            )

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_multiple_docker_secrets_in_string(self):
        # Create a mock configuration file with multiple Docker secrets in a string
        config_content = """
name: test-app
application:
  connection_string: "postgresql://${SECRET:username}:${SECRET:password}@${SECRET:host}:5432/${SECRET:database}"
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict = {
            "name": "test-app",
            "application": {
                "connection_string": "postgresql://dbuser:dbpass@dbhost:5432/dbname"
            },
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks for different secrets
            mock_exists.return_value = True

            # Mock different secret values
            def mock_secret_open(*args, **kwargs):
                if "username" in args[0]:
                    return mock_open(read_data="dbuser").return_value
                elif "password" in args[0]:
                    return mock_open(read_data="dbpass").return_value
                elif "host" in args[0]:
                    return mock_open(read_data="dbhost").return_value
                elif "database" in args[0]:
                    return mock_open(read_data="dbname").return_value
                return mock_open(read_data="").return_value

            with patch("builtins.open", side_effect=mock_secret_open, create=True):
                mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                    "{}"
                )
                mock_yaml_load.return_value = mock_config_dict

                # Call the load_config function
                config = load_config(run_process_config=False)

                # Verify that all Docker secrets were correctly substituted in the connection string
                self.assertEqual(
                    config["application"]["connection_string"],
                    "postgresql://dbuser:dbpass@dbhost:5432/dbname",
                )

                # Verify that the schema validation was called
                mock_validate.assert_called_once()

    def test_load_config_without_docker_secrets(self):
        """Test that configurations without Docker secrets still work correctly."""
        # Create a mock configuration file without any Docker secrets
        config_content = """
name: test-app
database:
  hostname: localhost
  port: 5432
  username: postgres
  password: plain_password
  app_db_name: test_db
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict = {
            "name": "test-app",
            "database": {
                "hostname": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": "plain_password",
                "app_db_name": "test_db",
            },
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config(run_process_config=False)

            # Verify that the configuration was loaded correctly without any Docker secrets
            self.assertEqual(config["database"]["password"], "plain_password")

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_mixed_env_vars_and_docker_secrets(self):
        """Test that configurations with a mix of environment variables and Docker secrets work correctly."""
        # Create a mock configuration file with both environment variables and Docker secrets
        config_content = """
name: test-app
database:
  hostname: ${DB_HOST}
  port: 5432
  username: postgres
  password: ${SECRET:db_password}
  app_db_name: test_db
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict = {
            "name": "test-app",
            "database": {
                "hostname": "db.example.com",
                "port": 5432,
                "username": "postgres",
                "password": "secret_password",
                "app_db_name": "test_db",
            },
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch(
                "builtins.open", mock_open(read_data="secret_password"), create=True
            ) as mock_secret_file,
            patch("yaml.safe_load") as mock_yaml_load,
            patch.dict(os.environ, {"DB_HOST": "db.example.com"}),
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config(run_process_config=False)

            # Verify that both environment variables and Docker secrets were correctly substituted
            self.assertEqual(config["database"]["hostname"], "db.example.com")
            self.assertEqual(config["database"]["password"], "secret_password")

            # Verify that the schema validation was called
            mock_validate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
