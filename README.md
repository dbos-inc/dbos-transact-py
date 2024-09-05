## 🚀 DBOS Transact - Ultra-Lightweight Durable Execution in Python 🚀

---

📚 **Documentation**: Under Construction 🚧

💬 **Join the Discussion**: [Discord Community](https://discord.gg/fMwQjeW5zg)

---


**DBOS Python is under construction! 🚧🚧🚧 Check back regularly for updates, release coming in mid-September!**

DBOS Transact is a **Python library** providing ultra-lightweight durable execution.
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
If you want to see durable execution in action, check out [this demo app](https://demo-widget-store.cloud.dbos.dev/) (source code [here](https://github.com/dbos-inc/dbos-demo-apps/tree/main/python/widget-store)).
No matter how many times you try to crash it, it always resumes from exactly where it left off!

Under the hood, DBOS Transact works by storing your program's execution state (which workflows are currently executing and which steps they've completed) in a Postgres database.
So all you need to use it is a Postgres database to connect to&mdash;there's no need for a "workflow server."
This approach is also incredibly fast, for example [25x faster than AWS Step Functions](https://www.dbos.dev/blog/dbos-vs-aws-step-functions-benchmark).

Some more cool features include:

- Scheduled jobs&mdash;run your workflows exactly-once per time interval.
- Exactly-once event processing&mdash;use workflows to process incoming events (for example, from a Kafka topic) exactly-once.
- Observability&mdash;all workflows automatically emit [OpenTelemetry](https://opentelemetry.io/) traces.

## Getting Started

To try out the latest pre-release version, install and configure with:

```shell
pip install --pre dbos
dbos init --config
```

Try it out with this simple program (requires Postgres):

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
def workflow():
    step_one()
    for _ in range(5):
        print("Press Control + \ to stop the app...")
        DBOS.sleep(1)
    step_two()

@app.get("/")
def endpoint():
    workflow()
```

Save the program into `main.py`, tell it your local Postgres password via `export PGPASSWORD=<your password>` and start it with `fastapi run`.
Visit `localhost:8000` in your browser (or curl it) to start the workflow.
When prompted, press `Control + \` to force quit your application.
It should crash midway through the workflow, having completed step one but not step two.
Then, restart your app with `fastapi run`.
It should resume the workflow from where it left off, completing step two without re-executing step one.

To learn how to build more complex examples, see our programming guide (coming soon).

## Documentation

Coming soon! 🚧

## Examples

Check out some cool demo apps here: [https://github.com/dbos-inc/dbos-demo-apps/tree/main/python](https://github.com/dbos-inc/dbos-demo-apps/tree/main/python)

## Community

If you're interested in building with us, please star our repository and join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
