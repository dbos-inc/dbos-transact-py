"""Build hook: pin the `enterprise` extra to the dbos-enterprise version matching this build.

Releases pin exactly; previews accept any preview or release in the same minor,
since preview numbers count commits and never coincide across the two repos.
"""

import re

from pdm.backend.hooks.base import Context


def enterprise_requirement(version: str) -> str:
    if re.fullmatch(r"\d+\.\d+\.\d+", version):
        return f"dbos-enterprise=={version}"
    match = re.match(r"(\d+)\.(\d+)", version)
    assert match is not None, f"unparseable version {version!r}"
    major, minor = match.groups()
    return f"dbos-enterprise>={major}.{minor}.0a0,<{major}.{int(minor) + 1}"


def pdm_build_initialize(context: Context) -> None:
    metadata = context.config.metadata
    metadata.setdefault("optional-dependencies", {})["enterprise"] = [
        enterprise_requirement(metadata["version"])
    ]
