"""
Add triggers.

Revision ID: a3b18ad34abe
Revises: 5c361fc04708
Create Date: 2024-07-21 13:07:09.814989
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a3b18ad34abe"
down_revision: Union[str, None] = "5c361fc04708"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.execute(
        """
      CREATE OR REPLACE FUNCTION dbos.notifications_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.destination_uuid || '::' || NEW.topic;
      BEGIN
          PERFORM pg_notify('dbos_notifications_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
               """
    )
    op.execute(
        """
      CREATE TRIGGER dbos_notifications_trigger
      AFTER INSERT ON dbos.notifications
      FOR EACH ROW EXECUTE FUNCTION dbos.notifications_function();
               """
    )
    op.execute(
        """
      CREATE OR REPLACE FUNCTION dbos.workflow_events_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.workflow_uuid || '::' || NEW.key;
      BEGIN
          PERFORM pg_notify('dbos_workflow_events_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
               """
    )
    op.execute(
        """
    CREATE TRIGGER dbos_workflow_events_trigger
      AFTER INSERT ON dbos.workflow_events
      FOR EACH ROW EXECUTE FUNCTION dbos.workflow_events_function();
               """
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS dbos_notifications_trigger ON dbos.notifications;"
    )
    op.execute("DROP FUNCTION IF EXISTS dbos.notifications_function;")
    op.execute(
        "DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON dbos.workflow_events;"
    )
    op.execute("DROP FUNCTION IF EXISTS dbos.workflow_events_function;")
    # ### end Alembic commands ###
