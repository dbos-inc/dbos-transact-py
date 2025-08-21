import logging
import os
import re

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from ._logger import dbos_logger


def run_alembic_migrations(engine: sa.Engine):
    """Run system database schema migrations with Alembic.
    This is DEPRECATED in favor of DBOS-managed migrations.
    It is retained only for backwards compatibility and
    will be removed in the next major version."""
    # Run a schema migration for the system database
    migration_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "_alembic_migrations"
    )
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", migration_dir)
    logging.getLogger("alembic").setLevel(logging.WARNING)
    # Alembic requires the % in URL-escaped parameters to itself be escaped to %%.
    escaped_conn_string = re.sub(
        r"%(?=[0-9A-Fa-f]{2})",
        "%%",
        engine.url.render_as_string(hide_password=False),
    )
    alembic_cfg.set_main_option("sqlalchemy.url", escaped_conn_string)
    try:
        command.upgrade(alembic_cfg, "head")
    except Exception as e:
        dbos_logger.warning(
            f"Exception during system database construction. This is most likely because the system database was configured using a later version of DBOS: {e}"
        )
