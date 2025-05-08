from dbos.cli._template_init import get_templates_directory
from dbos.cli.cli import _resolve_project_name_and_template


def test_resolve_project_name_and_template() -> None:
    git_templates = ["dbos-toolbox", "dbos-app-starter", "dbos-cron-starter"]
    templates_dir = get_templates_directory()

    # dbos init my-app -t dbos-toolbox
    project_name, template = _resolve_project_name_and_template(
        project_name="my-app",
        template="dbos-toolbox",
        config=False,
        git_templates=git_templates,
        templates_dir=templates_dir,
    )
    assert project_name == "my-app"
    assert template == "dbos-toolbox"

    # dbos init -t dbos-toolbox
    project_name, template = _resolve_project_name_and_template(
        project_name=None,
        template="dbos-toolbox",
        config=False,
        git_templates=git_templates,
        templates_dir=templates_dir,
    )
    assert project_name == "dbos-toolbox"
    assert template == "dbos-toolbox"
