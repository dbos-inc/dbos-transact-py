
<div align="center">

# DBOS Transact: A Lightweight Durable Execution Library Built on Postgres

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

DBOS Transact is a Python library for **ultra-lightweight durable execution**.
For example:

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

Durable execution means your program is **resilient to any failure**.
If it is ever interrupted or crashes, all your workflows will automatically resume from the last completed step.
Durable execution helps solve many common problems:

- Orchestrating long-running or business-critical workflows so they seamlessly recover from any failure.
- Running reliable background jobs with no timeouts.
- Processing incoming events (e.g. from Kafka) exactly once.
- Running a fault-tolerant distributed task queue.
- Running a reliable cron scheduler.
- Operating an AI agent, or anything that connects to an unreliable or non-deterministic API.

What’s unique about DBOS's implementation of durable execution is that it’s implemented in a **lightweight library** that’s **totally backed by Postgres**.
To use DBOS, just `pip install` it and annotate your program with DBOS decorators.
Under the hood, those decorators store your program's execution state (which workflows are currently executing and which steps they've completed) in a Postgres database.
If your program crashes or is interrupted, they automatically recover its workflows from their stored state.
So all you need to use DBOS is Postgres&mdash;there are no other dependencies you have to manage, no separate workflow server.

One big advantage of this approach is that you can add DBOS to **any** Python application&mdash;**it’s just a library**.
You can use DBOS to add reliable background jobs or cron scheduling or queues to your app with no external dependencies except Postgres.

## Getting Started

Install and configure with:

```shell
pip install dbos
dbos init --config
```

Then, try it out with this simple program:

```python
from fastapi import FastAPI
from dbos import DBOS

app = FastAPI()
DBOS(fastapi=app)

@DBOS.step()
def step_one():
    print("Step one completed!")

@DBOS.step()
def step_two():
    print("Step two completed!")

@DBOS.workflow()
def dbos_workflow():
    step_one()
    for _ in range(5):
        print("Press Control + \ to stop the app...")
        DBOS.sleep(1)
    step_two()

@app.get("/")
def fastapi_endpoint():
    dbos_workflow()
```

Save the program into `main.py` and start it with `fastapi run`.
Visit `localhost:8000` in your browser to start the workflow.
When prompted, press `Control + \` to force quit your application.
It should crash midway through the workflow, having completed step one but not step two.
Then, restart your app with `fastapi run`.
It should resume the workflow from where it left off, completing step two without re-executing step one.

To learn how to build more complex workflows, see the [programming guide](https://docs.dbos.dev/python/programming-guide) or [examples](https://docs.dbos.dev/examples).

## Documentation

[https://docs.dbos.dev](https://docs.dbos.dev)

## Examples


- [**AI-Powered Slackbot**](https://docs.dbos.dev/python/examples/rag-slackbot) &mdash; A Slackbot that answers questions about previous Slack conversations, using DBOS to durably orchestrate its RAG pipeline.
- [**Widget Store**](https://docs.dbos.dev/python/examples/widget-store) &mdash; An online storefront that uses DBOS durable workflows to be resilient to any failure.
- [**Scheduled Reminders**](https://docs.dbos.dev/python/examples/scheduled-reminders) &mdash; In just three lines of code, schedule an email to send days, weeks, or months in the future.

More examples [here](https://docs.dbos.dev/examples)!

## Community

If you're interested in building with us, please star our repository and join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
