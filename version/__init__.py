from datetime import datetime, timezone

from pdm.backend.hooks.version import SCMVersion
from pdm.backend.hooks.version.scm import guess_next_version


def format_version(git_version: SCMVersion) -> str:
    if git_version.distance is None:
        version = str(git_version.version)
    else:
        guessed = guess_next_version(git_version.version)
        version = f"{guessed}a{git_version.distance}"

    if (
        git_version.branch is not None
        and git_version.branch is not "main"
        and "release" not in git_version.branch
    ):
        version += f"+{git_version.node}"

    return version
