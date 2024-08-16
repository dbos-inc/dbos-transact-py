from datetime import datetime, timezone

from pdm.backend.hooks.version import SCMVersion
from pdm.backend.hooks.version.scm import guess_next_version


def format_version(git_version: SCMVersion) -> str:
    assert git_version.branch is not None
    is_release = "release" in git_version.branch
    is_preview = git_version.branch is "main"

    if git_version.distance is None:
        if is_release:
            version = git_version.version
        elif is_preview:
            version = f"{git_version.version}a0"
        else:
            raise Exception(
                "Tagged releases may not be published from feature branches"
            )
    else:
        guessed = guess_next_version(git_version.version)
        if is_release:
            raise Exception("Release branches may only publish tagged releases")
        elif is_preview:
            version = f"{guessed}a{git_version.distance}"
        else:
            version = f"{guessed}a{git_version.distance}+{git_version.node}"

    return version
