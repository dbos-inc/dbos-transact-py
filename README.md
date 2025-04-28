
<div align="center">

# DBOS Transact: Lightweight Durable Workflows

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

## What is DBOS?

DBOS provides lightweight durable workflows built on top of Postgres.
Instead of managing your own workflow orchestrator or task queue system, you can use DBOS to add durable workflows and queues to your program in just a few lines of code.

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install this open-source library and connect it to a Postgres database.
Then, annotate workflows and steps in your program to make it durable!
That's all you need to do&mdash;DBOS is entirely contained in this open-source library, there's no additional infrastructure for you to configure or manage.

## When Should I Use DBOS?

You should consider using DBOS if your application needs to **reliably handle failures**.
For example, you might be building a payments service that must reliably process transactions even if servers crash mid-operation, or a long-running data pipeline that needs to resume seamlessly from checkpoints rather than restart from the beginning when interrupted.

Handling failures is costly and complicated, requiring complex state management and recovery logic as well as heavyweight tools like external orchestration services.
DBOS makes it simpler: annotate your code to checkpoint it in Postgres and automatically recover from any failure.
DBOS also provides powerful Postgres-backed primitives that makes it easier to write and operate reliable code, including durable queues, notifications, scheduling, event processing, and programmatic workflow management.

## Features

<details open><summary><strong>💾 Durable Workflows</strong></summary>

####

DBOS workflows make your program **durable** by checkpointing its state in Postgres.
If your program ever fails, when it restarts all your workflows will automatically resume from the last completed step.

You add durable workflows to your existing Python program by annotating ordinary functions as workflows and steps:

```python
from dbos import DBOS

@DBOS.step()
def step_one():
    ...

@DBOS.step()
def step_two():
    ...

@DBOS.workflow()
def workflow()
    step_one()
    step_two()
```

Workflows are particularly useful for 

- Orchestrating business processes so they seamlessly recover from any failure.
- Building observable and fault-tolerant data pipelines.
- Operating an AI agent, or any application that relies on unreliable or non-deterministic APIs.

[Read more ↗️](https://docs.dbos.dev/python/tutorials/workflow-tutorial)

</details>

<details><summary><strong>📒 Durable Queues</strong></summary>

####

DBOS queues help you **durably** run tasks in the background.
You can enqueue a task (which can be a single step or an entire workflow) from a durable workflow and one of your processes will pick it up for execution.
DBOS manages the execution of your tasks: it guarantees that tasks complete, and that their callers get their results without needing to resubmit them, even if your application is interrupted.

Queues also provide flow control, so you can limit the concurrency of your tasks on a per-queue or per-process basis.
You can also set timeouts for tasks, rate limit how often queued tasks are executed, deduplicate tasks, or prioritize tasks.

You can add queues to your workflows in just a couple lines of code.
They don't require a separate queueing service or message broker&mdash;just Postgres.

```python
from dbos import DBOS, Queue

queue = Queue("example_queue")

@DBOS.step()
def process_task(task):
  ...

@DBOS.workflow()
def process_tasks(tasks):
  task_handles = []
  # Enqueue each task so all tasks are processed concurrently.
  for task in tasks:
    handle = queue.enqueue(process_task, task)
    task_handles.append(handle)
  # Wait for each task to complete and retrieve its result.
  # Return the results of all tasks.
  return [handle.get_result() for handle in task_handles]
```

[Read more ↗️](https://docs.dbos.dev/python/tutorials/queue-tutorial)

</details>

<details><summary><strong>⚙️ Programmatic Workflow Management</strong></summary>

####

Your workflows are stored as rows in a Postgres table, so you have full programmatic control over them.
Write scripts to query workflow executions, batch pause or resume workflows, or even restart failed workflows from a specific step.
Handle bugs or failures that affect thousands of workflows with power and flexibility.

```python
# Create a DBOS client connected to your Postgres database.
client = DBOSClient(database_url)
# Find all workflows that errored between 3:00 and 5:00 AM UTC on 2025-04-22.
workflows = client.list_workflows(status="ERROR", 
  start_time="2025-04-22T03:00:00Z", end_time="2025-04-22T05:00:00Z")
for workflow in workflows:
    # Check which workflows failed due to an outage in a service called from Step 2.
    steps = client.list_workflow_steps(workflow)
    if len(steps) >= 3 and isinstance(steps[2]["error"], ServiceOutage):
        # To recover from the outage, restart those workflows from Step 2.
        DBOS.fork_workflow(workflow.workflow_id, 2)
```

[Read more ↗️](https://docs.dbos.dev/python/reference/client)

</details>

<details><summary><strong>🎫 Exactly-Once Event Processing</strong></summary>

####

Use DBOS to build reliable webhooks, event listeners, or Kafka consumers by starting a workflow exactly-once in response to an event.
Acknowledge the event immediately while reliably processing it in the background.

For example:

```python
def handle_message(request: Request) -> None:
  event_id = request.body["event_id"]
  # Use the event ID as an idempotency key to start the workflow exactly-once
  with SetWorkflowID(event_id):
    # Start the workflow in the background, then acknowledge the event
    DBOS.start_workflow(message_workflow, request.body["event"])
```

Or with Kafka:

```python
@DBOS.kafka_consumer(config,["alerts-topic"])
@DBOS.workflow()
def process_kafka_alerts(msg):
    # This workflow runs exactly-once for each message sent to the topic
    alerts = msg.value.decode()
    for alert in alerts:
        respond_to_alert(alert)
```

[Read more ↗️](https://docs.dbos.dev/python/tutorials/workflow-tutorial)

</details>

<details><summary><strong>📅 Durable Scheduling</strong></summary>

####

Schedule workflows using cron syntax, or use durable sleep to pause workflows for as long as you like (even days or weeks) before executing.

You can schedule a workflow using a single annotation:

```python
@DBOS.scheduled('* * * * *') # crontab syntax to run once every minute
@DBOS.workflow()
def example_scheduled_workflow(scheduled_time: datetime, actual_time: datetime):
    DBOS.logger.info("I am a workflow scheduled to run once a minute.")
```

You can add a durable sleep to any workflow with a single line of code.
It stores its wakeup time in Postgres so the workflow sleeps through any interruption or restart, then always resumes on schedule.

```python
@DBOS.workflow()
def reminder_workflow(email: str, time_to_sleep: int):
    send_confirmation_email(email)
    DBOS.sleep(time_to_sleep)
    send_reminder_email(email)
```

[Read more ↗️](https://docs.dbos.dev/python/tutorials/scheduled-workflows)

</details>

<details><summary><strong>📫 Durable Notifications</strong></summary>

####

Pause your workflow executions until a notification is received, or emit events from your workflow to send progress updates to external clients.
All notifications are stored in Postgres, so they can be sent and received with exactly-once semantics.
Set durable timeouts when waiting for events, so you can wait for as long as you like (even days or weeks) through interruptions or restarts, then resume once a notification arrives or the timeout is reached.

For example, build a reliable billing workflow that durably waits for a notification from a payments service, processing it exactly-once:

```python
@DBOS.workflow()
def billing_workflow():
  ... # Calculate the charge, then submit the bill to a payments service
  payment_status = DBOS.recv(PAYMENT_STATUS, timeout=payment_service_timeout)
  if payment_status is not None and payment_status == "paid":
      ... # Handle a successful payment.
  else:
      ... # Handle a failed payment or timeout.
```

</details>


## Getting Started

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install this open-source library and connect it to a Postgres database.
Then, check out the [programming guide](https://docs.dbos.dev/python/programming-guide) to learn how to build with durable workflows and queues.

## Documentation

[https://docs.dbos.dev](https://docs.dbos.dev)

## Examples

[https://docs.dbos.dev/examples](https://docs.dbos.dev/examples)

## DBOS vs. Other Systems

<details><summary><strong>DBOS vs. Temporal</strong></summary>

####

Both DBOS and Temporal provide durable execution, but DBOS is implemented in a lightweight Postgres-backed library whereas Temporal is implemented in an externally orchestrated server.

You can add DBOS to your program by installing this open-source library, connecting it to Postgres, and annotating workflows and steps.
By contrast, to add Temporal to your program, you must rearchitect your program to move your workflows and steps (activities) to a Temporal worker, configure a Temporal server to orchestrate those workflows, and access your workflows only through a Temporal client.
[This blog post](https://www.dbos.dev/blog/durable-execution-coding-comparison) makes the comparison in more detail.

**When to use DBOS:** You need to add durable workflows to your applications with minimal rearchitecting, or you are using Postgres.

**When to use Temporal:** You don't want to add Postgres to your stack, or you need a language DBOS doesn't support yet.

</details>

<details><summary><strong>DBOS vs. Airflow</strong></summary>

####

DBOS and Airflow both provide workflow abstractions.
Airflow is targeted at data science use cases, providing many out-of-the-box connectors but requiring workflows be written as explicit DAGs and externally orchestrating them from an Airflow cluster.
Airflow is designed for batch operations and does not provide good performance for streaming or real-time use cases.
DBOS is general-purpose, but is often used for data pipelines, allowing developers to write workflows as code and requiring no infrastructure except Postgres.

**When to use DBOS:** You need the flexibility of writing workflows as code, or you need higher performance than Airflow is capable of (particularly for streaming or real-time use cases).

**When to use Airflow:** You need Airflow's ecosystem of connectors.

</details>

<details><summary><strong>DBOS vs. Celery/BullMQ</strong></summary>

####

DBOS provides a similar queue abstraction to dedicated queueing systems like Celery or BullMQ: you can declare queues, submit tasks to them, and control their flow with concurrency limits, rate limits, timeouts, prioritization, etc.
However, DBOS queues are **durable and Postgres-backed** and integrate with durable workflows.
For example, in DBOS you can write a durable workflow that enqueues a thousand tasks and waits for their results.
DBOS checkpoints the workflow and each of its tasks in Postgres, guaranteeing that even if failures or interruptions occur, the tasks will complete and the workflow will collect their results.
By contrast, Celery/BullMQ are Redis-backed and don't provide workflows, so they provide fewer guarantees but better performance.

**When to use DBOS:** You need the reliability of enqueueing tasks from durable workflows.

**When to use Celery/BullMQ**: You don't need durability, or you need very high throughput beyond what your Postgres server can support.
</details>

## Community

If you want to ask questions or hang out with the community, join us on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
