import os
import subprocess
import tempfile

import yaml


def test_init_with_app_name() -> None:
    """Test that when an app name is provided, it is used as the project name."""
    app_name = "my-test-app"

    with tempfile.TemporaryDirectory() as temp_path:
        # Run the init command with app name and template
        subprocess.check_call(
            ["dbos", "init", app_name, "-t", "dbos-toolbox"],
            cwd=temp_path,
        )

        # Verify dbos-config.yaml
        config_path = os.path.join(temp_path, "dbos-config.yaml")
        assert os.path.exists(config_path)

        with open(config_path) as f:
            config = yaml.safe_load(f)
            assert config["name"] == app_name

        # Verify main.py
        main_path = os.path.join(temp_path, "main.py")
        assert os.path.exists(main_path)

        with open(main_path) as f:
            main_content = f.read()
            # Check for the DBOSConfig dictionary with the correct name
            assert f'"name": "{app_name}"' in main_content


def test_init_without_app_name() -> None:
    """Test that when no app name is provided, the template name is used as the project name."""
    template = "dbos-toolbox"

    with tempfile.TemporaryDirectory() as temp_path:
        # Run the init command with only template
        subprocess.check_call(
            ["dbos", "init", "-t", template],
            cwd=temp_path,
        )

        # Verify dbos-config.yaml
        config_path = os.path.join(temp_path, "dbos-config.yaml")
        assert os.path.exists(config_path)

        with open(config_path) as f:
            config = yaml.safe_load(f)
            assert config["name"] == template

        # Verify main.py
        main_path = os.path.join(temp_path, "main.py")
        assert os.path.exists(main_path)

        with open(main_path) as f:
            main_content = f.read()
            # Check for the DBOSConfig dictionary with the template name
            assert f'"name": "{template}"' in main_content
