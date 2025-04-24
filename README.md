
<div align="center">

# DBOS Transact: Lightweight Durable Python Workflows

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

## What is DBOS?

DBOS provides lightweight durable workflows built on top of Postgres.
Instead of managing your own workflow orchestrator or task queue system, you can use DBOS to add durable workflows to your program in just a few lines of code.

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install this open-source library and connect it to a Postgres database.
Then, annotate workflows and steps in your program to make it durable!
That's all you need to do&mdash;there's no additional infrastructure for you to configure or manage.

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


## Features

<details open><summary><strong>💾 Durable Workflows</strong></summary>

####

DBOS workflows make your program **durable** by checkpointing its state in Postgres.
If your program ever fails, when it restarts all your workflows will automatically resume from the last completed step.

You add durable workflows to your existing Python program by annotating workflows and steps:

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
