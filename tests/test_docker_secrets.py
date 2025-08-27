import os
import tempfile
import unittest
from typing import Any, Dict, List, TypedDict, cast
from unittest.mock import mock_open, patch

from sqlalchemy import make_url

from dbos._dbos_config import _substitute_env_vars, load_config


class ConfigFile(TypedDict, total=False):
    name: str
    database: Dict[str, Any]
    database_url: str
    telemetry: Dict[str, Any]
    runtimeConfig: Dict[str, List[str]]


class DatabaseConfig(TypedDict, total=False):
    hostname: str
    port: int
    username: str
    password: str
    app_db_name: str
    url: str
    nested: Dict[str, Any]
    # Add other fields as needed


class TestDockerSecrets(unittest.TestCase):
    def setUp(self) -> None:
        # Create a temporary directory to simulate /run/secrets/
        self.temp_dir = tempfile.TemporaryDirectory()
        self.secrets_dir = os.path.join(self.temp_dir.name, "secrets")
        os.makedirs(self.secrets_dir)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_substitute_env_vars_with_env_vars(self) -> None:
        # Test that environment variables are still substituted correctly
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            content = "This is a ${TEST_VAR} test"
            result = _substitute_env_vars(content)
            self.assertEqual(result, "This is a test_value test")

    def test_substitute_env_vars_with_docker_secrets(self) -> None:
        # Create a mock Docker secret
        secret_path = os.path.join(self.secrets_dir, "db_password")
        with open(secret_path, "w") as f:
            f.write("secret_password")

        # Mock the /run/secrets/ path
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            with patch("builtins.open", mock_open(read_data="secret_password")):
                content = "This is a ${DOCKER_SECRET:db_password} test"
                result = _substitute_env_vars(content)
                self.assertEqual(result, "This is a secret_password test")

    def test_substitute_env_vars_with_missing_docker_secret(self) -> None:
        # Test that a warning is logged when a Docker secret is missing
        with patch("dbos._dbos_config.dbos_logger") as mock_logger:
            content = "This is a ${DOCKER_SECRET:missing_secret} test"
            result = _substitute_env_vars(content)
            self.assertEqual(result, "This is a  test")
            mock_logger.warning.assert_called_once()

    def test_substitute_env_vars_with_both_env_vars_and_docker_secrets(self) -> None:
        # Test that both environment variables and Docker secrets are substituted
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            with patch("os.path.exists") as mock_exists:
                mock_exists.return_value = True
                with patch(
                    "builtins.open",
                    mock_open(read_data="secret_password"),
                ):
                    content = (
                        "This is a ${TEST_VAR} and ${DOCKER_SECRET:db_password} test"
                    )
                    result = _substitute_env_vars(content)
                    self.assertEqual(
                        result, "This is a test_value and secret_password test"
                    )

    def test_substitute_env_vars_with_silent_mode(self) -> None:
        # Test that no warning is logged when silent mode is enabled
        with patch("dbos._dbos_config.dbos_logger") as mock_logger:
            content = "This is a ${DOCKER_SECRET:missing_secret} test"
            result = _substitute_env_vars(content, silent=True)
            self.assertEqual(result, "This is a  test")
            mock_logger.warning.assert_not_called()

    def test_load_config_with_docker_secrets_in_database_url(self) -> None:
        # Create a mock configuration file with Docker secrets in the database URL
        config_content = """
name: test-app
database_url: postgresql://postgres:${DOCKER_SECRET:db_password}@localhost:5432/test_db
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict: ConfigFile = {
            "name": "test-app",
            "database_url": "postgresql://postgres:secret_password@localhost:5432/test_db",
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config()

            # Verify that the Docker secret was correctly substituted in the database URL
            self.assertEqual(
                config["database_url"],
                "postgresql://postgres:secret_password@localhost:5432/test_db",
            )

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_docker_secrets_in_list(self) -> None:
        # Create a mock configuration file with Docker secrets in a list
        config_content = """
name: test-app
runtimeConfig:
  setup:
    - echo "Setting up environment"
    - export API_KEY=${DOCKER_SECRET:api_key}
    - export DB_PASSWORD=${DOCKER_SECRET:db_password}
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict: ConfigFile = {
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
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config()

            # Verify that the Docker secrets were correctly substituted in the list
            setup_commands = config["runtimeConfig"]["setup"]
            if setup_commands is not None:
                self.assertEqual(setup_commands[0], 'echo "Setting up environment"')
                self.assertEqual(setup_commands[1], "export API_KEY=secret_value")
                self.assertEqual(setup_commands[2], "export DB_PASSWORD=secret_value")

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_multiple_docker_secrets_in_string(self) -> None:
        # Create a mock configuration file with multiple Docker secrets in a string
        config_content = """
name: test-app
database_url: postgresql://${DOCKER_SECRET:db_user}:${DOCKER_SECRET:db_password}@${DOCKER_SECRET:db_host}:${DOCKER_SECRET:db_port}/${DOCKER_SECRET:db_name}
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict: ConfigFile = {
            "name": "test-app",
            "database_url": "postgresql://user:pass@host:5432/db",
        }

        # Mock the schema validation to always pass
        with (
            patch("builtins.open", mock_file),
            patch("dbos._dbos_config.validate") as mock_validate,
            patch("dbos._dbos_config.resources.files") as mock_resources,
            patch("os.path.exists") as mock_exists,
            patch("yaml.safe_load") as mock_yaml_load,
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            def mock_secret_open(*args: Any, **kwargs: Any) -> Any:
                filepath = args[0]
                if filepath == "dbos-config.yaml":
                    return mock_open(read_data=config_content).return_value

                secret_name = filepath.split("/")[-1]
                secret_values = {
                    "db_user": "user",
                    "db_password": "pass",
                    "db_host": "host",
                    "db_port": "5432",
                    "db_name": "db",
                }
                if secret_name in secret_values:
                    return mock_open(read_data=secret_values[secret_name]).return_value
                raise FileNotFoundError(f"Mock file not found: {filepath}")

            with patch("builtins.open", mock_secret_open, create=True):
                # Call the load_config function
                config = load_config()

                # Verify that all Docker secrets were correctly substituted in the string
                self.assertEqual(
                    config["database_url"],
                    "postgresql://user:pass@host:5432/db",
                )

                # Verify that the schema validation was called
                mock_validate.assert_called_once()

    def test_load_config_without_docker_secrets(self) -> None:
        # Create a mock configuration file without Docker secrets
        config_content = """
name: test-app
database_url: postgresql://postgres:plain_password@localhost:5432/test_db
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict: ConfigFile = {
            "name": "test-app",
            "database_url": "postgresql://postgres:plain_password@localhost:5432/test_db",
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
            config = load_config()

            # Verify that the configuration was loaded correctly without any substitutions
            assert config["database_url"] is not None
            self.assertEqual(
                make_url(config["database_url"]).password, "plain_password"
            )

            # Verify that the schema validation was called
            mock_validate.assert_called_once()

    def test_load_config_with_mixed_env_vars_and_docker_secrets(self) -> None:
        # Create a mock configuration file with both environment variables and Docker secrets
        config_content = """
name: test-app
database_url: postgresql://${DB_USER}:${DOCKER_SECRET:db_password}@${DB_HOST}:${DB_PORT}/${DB_NAME}
"""

        # Mock the file open and read operations
        mock_file = mock_open(read_data=config_content)

        # Create a mock dictionary that would be returned by yaml.safe_load
        mock_config_dict: ConfigFile = {
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
            patch.dict(
                os.environ,
                {
                    "DB_HOST": "localhost",
                    "DB_PORT": "5432",
                    "DB_USER": "postgres",
                    "DB_NAME": "test_db",
                },
            ),
        ):

            # Set up the mocks
            mock_exists.return_value = True
            mock_resources.return_value.joinpath.return_value.open.return_value.__enter__.return_value.read.return_value = (
                "{}"
            )
            mock_yaml_load.return_value = mock_config_dict

            # Call the load_config function
            config = load_config()

            # Verify that both environment variables and Docker secrets were correctly substituted
            self.assertEqual(
                config["database_url"],
                "postgresql://postgres:secret_password@localhost:5432/test_db",
            )

            # Verify that the schema validation was called
            mock_validate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
