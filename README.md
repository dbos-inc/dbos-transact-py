## 🚀 DBOS Transact - Ultra-Lightweight Durable Execution in Python 🚀

---

📚 **Documentation**: [Explore the Docs 📖](https://docs.dbos.dev)

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

Under the hood, DBOS Transact works by storing your program's execution state (which workflows are currently executing and which steps they've completed) in a Postgres database.
So all you need to use it is a Postgres database to connect to&mdash;there's no need for a "workflow server."
This approach is also incredibly fast, for example [25x faster than AWS Step Functions](https://www.dbos.dev/blog/dbos-vs-aws-step-functions-benchmark).

Some more cool features include:

- Scheduled jobs&mdash;run your workflows exactly-once per time interval.
- Exactly-once event processing&mdash;use workflows to process incoming events (for example, from a Kafka topic) exactly-once.
- Observability&mdash;all workflows automatically emit [OpenTelemetry](https://opentelemetry.io/) traces.

## Getting Started

To try out the latest pre-release version, install with:

```shell
pip install --pre dbos
```

## Documentation

Coming soon! 🚧

But we have some cool demo apps for you to check out: [https://github.com/dbos-inc/dbos-demo-apps/tree/main/python](https://github.com/dbos-inc/dbos-demo-apps/tree/main/python)

## Community

If you're interested in building with us, please star our repository and join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
