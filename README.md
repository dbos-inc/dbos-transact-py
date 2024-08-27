# DBOS Transact Python

**DBOS Python is under construction! 🚧🚧🚧 Check back regularly for updates, release coming in mid-September!**

DBOS Transact is a **Python library** for building durable and scalable applications.

You want to use DBOS Transact in your application because you need:

- **Resilience to any failure**.  If your app is interrupted for any reason, it automatically resumes from where it left off.  Reliable message delivery is built in. Idempotency is built in.
- **Reliable event processing**. Need to consume Kafka events exactly-once? Just add one line of code to your app. Need to run a task exactly once per hour, day, or month? Just one more line of code.
- **Built-in observability**. Automatically emit [OpenTelemetry](https://opentelemetry.io/)-compatible logs and traces from any application. Query your app's history from the command line or with SQL.
- **Blazing-fast, developer-friendly serverless**.  Develop your project locally and run it anywhere. When you're ready, [deploy it for free to DBOS Cloud](https://docs.dbos.dev/getting-started/quickstart#deploying-to-dbos-cloud) and we'll host it for you, [25x faster](https://www.dbos.dev/blog/dbos-vs-aws-step-functions-benchmark) and [15x cheaper](https://www.dbos.dev/blog/dbos-vs-lambda-cost) than AWS Lambda.

## Getting Started

To try out the latest pre-release version, install with:

```shell
pip install --pre dbos
```

## Documentation

Coming soon! 🚧

But we have some cool demo apps for you to check out: [https://github.com/dbos-inc/dbos-demo-apps/tree/main/python](https://github.com/dbos-inc/dbos-demo-apps/tree/main/python)

## Main Features

Here are some of the core features of DBOS Transact:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Transactions](https://www.dbos.dev/dbos-transact-python)                                      | Easily and safely query your application database using [SQLAlchemy](https://www.sqlalchemy.org/) or raw SQL.
| [Workflows](https://www.dbos.dev/dbos-transact-python)                                         | Reliable workflow orchestration&#8212;resume your program after any failure.
| [Idempotency](https://www.dbos.dev/dbos-transact-python)                                       | Automatically make any request idempotent, so your requests happen exactly once.
| [Authentication and Authorization](https://www.dbos.dev/dbos-transact-python)                  | Secure your HTTP endpoints so only authorized users can access them.
| [Kafka Integration](https://www.dbos.dev/dbos-transact-python)                                 | Consume Kafka messages exactly-once with transactions or workflows.
| [Scheduled Workflows](https://www.dbos.dev/dbos-transact-python)                               | Schedule your workflows to run exactly-once per time interval with cron-like syntax.
| [Self-Hosting](https://www.dbos.dev/dbos-transact-python)                                      | Host your applications anywhere, as long as they have a Postgres database to connect to.

And DBOS Cloud:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Serverless App Deployment](https://docs.dbos.dev/cloud-tutorials/application-management)      | Deploy apps to DBOS Cloud in minutes.
| [Interactive Time Travel](https://docs.dbos.dev/cloud-tutorials/interactive-timetravel)        | Query your application database as of any past point in time.
| [Cloud Database Management](https://docs.dbos.dev/cloud-tutorials/database-management)         | Provision cloud Postgres instances for your applications. Alternatively, [bring your own database](https://docs.dbos.dev/cloud-tutorials/byod-management).
| [Built-in Observability](https://docs.dbos.dev/cloud-tutorials/monitoring-dashboard)           | Built-in log capture, request tracing, and dashboards.

## Community

If you're interested in building with us, please star our repository and join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
