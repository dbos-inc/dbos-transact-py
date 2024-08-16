from datetime import datetime, timezone

from pdm.backend.hooks.version import SCMVersion
from pdm.backend.hooks.version.scm import guess_next_version


def format_version(git_version: SCMVersion) -> str:
    """
    1. Release versions may only be published from release branches. Their version is a git tag.
    2. Preview versions are published from main. They are PEP440 alpha releases whose version is the
    next release version number followed by "a" followed by the number of commits since the last release.
    If the last release was 1.2.3 and there have been ten commits since, the preview version is 1.2.3a10
    3. Test versions are published from feature branches. They are PEP440 local versions tagged with a git hash.
    """
    assert git_version.branch is not None
    is_release = "release" in git_version.branch
    is_preview = git_version.branch == "main"

    if git_version.distance is None:
        if is_release:
            version = str(git_version.version)
        elif is_preview:
            version = f"{git_version.version}a0"
        else:
            raise Exception(
                "Tagged releases may not be published from feature branches"
            )
    else:
        guessed = guess_next_version(git_version.version)
        if is_release:
            raise Exception(
                f"Release branches may only publish tagged releases. Distance: {git_version.distance}"
            )
        elif is_preview:
            version = f"{guessed}a{git_version.distance}"
        else:
            version = f"{guessed}a{git_version.distance}+{git_version.node}"

    return version
