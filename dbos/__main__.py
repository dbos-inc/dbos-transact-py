import re
import sys
from typing import NoReturn, Optional, Union

from dbos.cli.cli import app

# This is used by the debugger to execute DBOS as a module.
# Never used otherwise.


def main() -> NoReturn:
    # Modify sys.argv[0] to remove script or executable extensions
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])

    retval: Optional[Union[str, int]] = 1
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
