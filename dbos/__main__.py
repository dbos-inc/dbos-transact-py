import re
import sys
from typing import NoReturn

from dbos.cli.cli import app


def main() -> NoReturn:
    # Modify sys.argv[0] to remove script or executable extensions
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])

    retval: str | int | None = 1
    try:
        app()
        retval = None
    except SystemExit as e:
        retval = e.code
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        retval = 1
    finally:
        sys.exit(retval)


if __name__ == "__main__":
    main()
