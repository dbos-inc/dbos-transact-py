# Welcome to DBOS!

This is a sample app built with DBOS and FastAPI.
It displays greetings to visitors and keeps track of how many times each visitor has been greeted.
Check out the source code in `<app-name>/main.py`!

### Running Locally

To run this app locally, you need a Postgres database.
If you have Docker, you can start one with:

```shell
export PGPASSWORD=dbos
python3 start_postgres_docker.py
```

Then run:

```shell
dbos migrate
dbos start
```

Visit [`http://localhost:8000`](http://localhost:8000) to see your app!

### Deploying to the Cloud

To deploy this app to DBOS Cloud, first install the DBOS Cloud CLI (requires Node):

```shell
npm i -g @dbos-inc/dbos-cloud
```

Then, run this command to deploy your app:

```shell
dbos-cloud app deploy
```

This command outputs a URL--visit it to see your app!
You can also visit the [DBOS Cloud Console](https://console.dbos.dev/) to see your app's status and logs.