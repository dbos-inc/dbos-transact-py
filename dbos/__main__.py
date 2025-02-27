import re
import sys

from dbos.cli.cli import app


def main():
    # Modify sys.argv[0] to remove script or executable extensions
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])

    retval = 1
    try:
        app()
        retval = 0
    except SystemExit as e:
        retval = e.code
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        retval = 1
    finally:
        sys.exit(retval)


if __name__ == "__main__":
    main()
