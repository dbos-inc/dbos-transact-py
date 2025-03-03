from dbos._debug import PythonModule, parse_start_command


def test_parse_fast_api_command() -> None:
    command = "fastapi run app/main.py"
    expected = "app/main.py"
    actual = parse_start_command(command)
    assert actual == expected


def test_parse_python_command() -> None:
    command = "python app/main.py"
    expected = "app/main.py"
    actual = parse_start_command(command)
    assert actual == expected


def test_parse_python3_command() -> None:
    command = "python3 app/main.py"
    expected = "app/main.py"
    actual = parse_start_command(command)
    assert actual == expected


def test_parse_python_module_command() -> None:
    command = "python -m some_module"
    actual = parse_start_command(command)
    assert isinstance(actual, PythonModule)
    assert actual.module_name == "some_module"


def test_parse_python3_module_command() -> None:
    command = "python3 -m some_module"
    actual = parse_start_command(command)
    assert isinstance(actual, PythonModule)
    assert actual.module_name == "some_module"
