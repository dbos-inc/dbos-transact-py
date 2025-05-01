# Public API
import os
from urllib.parse import quote

from dbos import DBOS, DBOSConfig, Queue, SetWorkflowID


def default_config() -> DBOSConfig:
    return {
        "name": "test-app",
        "database_url": f"postgresql://postgres:{quote(os.environ.get('PGPASSWORD', 'dbos'), safe='')}@localhost:5432/dbostestpy",
    }


q = Queue("testq", concurrency=1, limiter={"limit": 1, "period": 1})


@DBOS.dbos_class()
class WF:
    @staticmethod
    @DBOS.workflow()
    def queued_task() -> int:
        DBOS.sleep(0.1)
        return 1

    @staticmethod
    @DBOS.workflow()
    def enqueue_5_tasks() -> int:
        for i in range(5):
            print(f"Iteration {i + 1}")
            wfh = DBOS.start_workflow(WF.queued_task)
            wfh.get_result()
            DBOS.sleep(0.9)

            if i == 3 and "DIE_ON_PURPOSE" in os.environ:
                print("CRASH")
                os._exit(1)
        return 5

    x = 5


def main() -> None:
    DBOS(config=default_config())
    DBOS.launch()
    DBOS._recover_pending_workflows()

    with SetWorkflowID("testqueuedwfcrash"):
        WF.enqueue_5_tasks()

    DBOS.destroy()
    os._exit(0)


if __name__ == "__main__":
    main()
