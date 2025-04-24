
<div align="center">

# DBOS Transact: Lightweight Durable Python Workflows

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

## What is DBOS?

DBOS provides lightweight durable workflows built on top of Postgres.
Instead of managing your own workflow orchestrator or task queue system, you can use DBOS to add durable workflows and queues to your program in just a few lines of code.

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install this open-source library and connect it to a Postgres database.
Then, annotate workflows and steps in your program to make it durable!
That's all you need to do&mdash;there's no additional infrastructure for you to configure or manage.

## Features

<details open><summary><strong>💾 Durable Workflows</strong></summary>

####

DBOS workflows make your program **durable** by checkpointing its state in Postgres.
If your program ever fails, when it restarts all your workflows will automatically resume from the last completed step.

You add durable workflows to your existing Python program by annotating ordinary functions as workflows and steps:

```python
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

- Orchestrating complex business processes so they seamlessly recover from any failure.
- Processing incoming events (e.g. from Kafka) exactly once.
- Operating an AI agent or data pipeline, or anything that connects to an unreliable or non-deterministic API.

[Read more ↗️](https://docs.dbos.dev/python/tutorials/workflow-tutorial)

</details>

<details><summary><strong>📒 Durable Queues</strong></summary>

####

DBOS queues help you **durably** run tasks or workflows in the background.
You can enqueue a task as part of a durable workflow and one of your processes will pick it up for execution.
DBOS manages the execution of your task, guaranteeing that it gets completed (and that your workflow gets the result without having to resubmit the task) if your application is interrupted.

Queues also provide flow control, so you can limit the concurrency of your tasks on a per-queue or per-process basis.
You can also set timeouts for tasks, rate limit how often queued tasks are executed, deduplicate tasks, or prioritize critical tasks.

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
client = DBOSClient(database_url)
# Find all workflows that errored between 3:00 and 5:00 AM UTC on 2025-04-22.
workflows = client.list_workflows(status="ERROR", 
  start_time="2025-04-22T03:00:00Z", end_time="2025-04-22T05:00:00Z")
for workflow in workflows:
    # Check which workflow failed due to an outage in a service called from Step 2
    steps = client.list_workflow_steps(workflow)
    if len(steps) >= 3 and isinstance(steps[2]["error"], ServiceOutage):
        # To recover from the outage, restart those workflows from step 2
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
    # This workflow runs exactly-once for each message on the topic
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
    DBOS.logger.info("I am a workflow scheduled to run once a minute. ")
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
All notifications are stored in Postgres, so they can be send and received with exactly-once semantics.
Set durable timeouts when waiting for events, so you can wait for as long as you like (even days or weeks) through interruptions or restarts, then resume once the notification arrives or the timeout is reached.

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

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install the open-source library and connect it to a Postgres database.
Then, check out the [programming guide](https://docs.dbos.dev/python/programming-guide) to learn how to build with durable workflows and queues.

## Documentation

[https://docs.dbos.dev](https://docs.dbos.dev)

## Examples

[https://docs.dbos.dev/examples](https://docs.dbos.dev/examples)!

## Community

If you want to ask questions or hang out with the community, join us on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
