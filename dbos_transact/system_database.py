import os
from enum import Enum
from typing import Any, Literal, Optional, TypedDict

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from alembic import command
from alembic.config import Config

import dbos_transact.utils as utils
from dbos_transact.error import DBOSWorkflowConflictUUIDError

from .dbos_config import ConfigFile
from .schemas.system_database import SystemSchema


class WorkflowStatusString(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    RETRIES_EXCEEDED = "RETRIES_EXCEEDED"
    CANCELLED = "CANCELLED"


WorkflowStatuses = Literal[
    "PENDING", "SUCCESS", "ERROR", "RETRIES_EXCEEDED", "CANCELLED"
]


class WorkflowInputs(TypedDict):
    args: Any
    kwargs: Any


class WorkflowStatusInternal(TypedDict):
    workflow_uuid: str
    status: WorkflowStatuses
    name: str
    output: Optional[str]  # Base64-encoded pickle
    error: Optional[str]  # Base64-encoded pickle


class RecordedResult(TypedDict):
    output: Optional[str]  # Base64-encoded pickle
    error: Optional[str]  # Base64-encoded pickle


class OperationResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # Base64-encoded pickle
    error: Optional[str]  # Base64-encoded pickle


class SystemDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config

        sysdb_name = (
            config["database"]["sys_db_name"]
            if "sys_db_name" in config["database"] and config["database"]["sys_db_name"]
            else config["database"]["app_db_name"] + SystemSchema.sysdb_suffix
        )

        # If the system database does not already exist, create it
        postgres_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database="postgres",
        )
        engine = sa.create_engine(postgres_db_url)
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            if not conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                parameters={"db_name": sysdb_name},
            ).scalar():
                conn.execute(sa.text(f"CREATE DATABASE {sysdb_name}"))
        engine.dispose()

        system_db_url = sa.URL.create(
            "postgresql",
            username=config["database"]["username"],
            password=config["database"]["password"],
            host=config["database"]["hostname"],
            port=config["database"]["port"],
            database=sysdb_name,
        )

        # Create a connection pool for the system database
        self.engine = sa.create_engine(system_db_url, pool_size=10, pool_timeout=30)

        # Run a schema migration for the system database
        migration_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "migrations"
        )
        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", migration_dir)
        alembic_cfg.set_main_option(
            "sqlalchemy.url", self.engine.url.render_as_string(hide_password=False)
        )
        command.upgrade(alembic_cfg, "head")

    # Destroy the pool when finished
    def destroy(self) -> None:
        self.engine.dispose()

    def update_workflow_status(self, status: WorkflowStatusInternal) -> None:
        with self.engine.begin() as c:
            c.execute(
                pg.insert(SystemSchema.workflow_status)
                .values(
                    workflow_uuid=status["workflow_uuid"],
                    status=status["status"],
                    name=status["name"],
                    output=status["output"],
                    error=status["error"],
                )
                .on_conflict_do_update(
                    index_elements=["workflow_uuid"],
                    set_=dict(
                        status=status["status"],
                        output=status["output"],
                        error=status["error"],
                    ),
                )
            )

    def get_workflow_status(
        self, workflow_uuid: str
    ) -> Optional[WorkflowStatusInternal]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(
                    SystemSchema.workflow_status.c.status,
                    SystemSchema.workflow_status.c.name,
                ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_uuid)
            ).fetchone()
            if row is None:
                return None
            status: WorkflowStatusInternal = {
                "workflow_uuid": workflow_uuid,
                "status": row[0],
                "name": row[1],
                "output": None,
                "error": None,
            }
            return status

    def update_workflow_inputs(self, workflow_uuid: str, inputs: str) -> None:
        with self.engine.begin() as c:
            c.execute(
                pg.insert(SystemSchema.workflow_inputs)
                .values(
                    workflow_uuid=workflow_uuid,
                    inputs=inputs,
                )
                .on_conflict_do_nothing()
            )

    def get_workflow_inputs(self, workflow_uuid: str) -> Optional[WorkflowInputs]:
        with self.engine.begin() as c:
            row = c.execute(
                sa.select(SystemSchema.workflow_inputs.c.inputs).where(
                    SystemSchema.workflow_inputs.c.workflow_uuid == workflow_uuid
                )
            ).fetchone()
            if row is None:
                return None
            inputs: WorkflowInputs = utils.deserialize(row[0])
            return inputs

    def get_pending_workflows(self) -> list[str]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(SystemSchema.workflow_status.c.workflow_uuid).where(
                    SystemSchema.workflow_status.c.status
                    == WorkflowStatusString.PENDING.value
                )
            ).fetchall()
            return [row[0] for row in rows]

    def record_operation_result(self, result: OperationResultInternal) -> None:
        error = result["error"]
        output = result["output"]
        assert error is None or output is None, "Only one of error or output can be set"
        with self.engine.begin() as c:
            try:
                c.execute(
                    pg.insert(SystemSchema.operation_outputs).values(
                        workflow_uuid=result["workflow_uuid"],
                        function_id=result["function_id"],
                        output=output,
                        error=error,
                    )
                )
            except sa.exc.IntegrityError:
                raise DBOSWorkflowConflictUUIDError(result["workflow_uuid"])
            except Exception as e:
                raise e

    def check_operation_execution(
        self, workflow_uuid: str, function_id: int
    ) -> Optional[RecordedResult]:
        with self.engine.begin() as c:
            rows = c.execute(
                sa.select(
                    SystemSchema.operation_outputs.c.output,
                    SystemSchema.operation_outputs.c.error,
                ).where(
                    SystemSchema.operation_outputs.c.workflow_uuid == workflow_uuid,
                    SystemSchema.operation_outputs.c.function_id == function_id,
                )
            ).all()
            if len(rows) == 0:
                return None
            result: RecordedResult = {
                "output": rows[0][0],
                "error": rows[0][1],
            }
            return result
