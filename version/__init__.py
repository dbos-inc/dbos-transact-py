from datetime import datetime, timezone

from pdm.backend.hooks.version import SCMVersion
from pdm.backend.hooks.version.scm import guess_next_version


def format_version(version: SCMVersion) -> str:
    if version.distance is None:
        main_version = str(version.version)
    else:
        guessed = guess_next_version(version.version)
        main_version = f"{guessed}.dev{version.distance}"

    if version.distance is None or version.node is None:
        fmt = ""
    else:
        fmt = "+{node}"
    local_version = fmt.format(node=version.node, time=datetime.now(tz=timezone.utc))
    return main_version + local_version
