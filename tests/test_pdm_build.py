from pdm_build import enterprise_requirement


def test_release_pins_exactly() -> None:
    assert enterprise_requirement("3.5.2") == "dbos-enterprise==3.5.2"


def test_preview_accepts_the_same_minor() -> None:
    assert enterprise_requirement("3.6.0a12") == "dbos-enterprise>=3.6.0a0,<3.7"
    assert enterprise_requirement("3.6.0a12+gabc123") == "dbos-enterprise>=3.6.0a0,<3.7"
    assert enterprise_requirement("3.9.0a0") == "dbos-enterprise>=3.9.0a0,<3.10"
