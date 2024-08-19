# DBOS Transact Python

**DBOS Python is under construction! 🚧🚧🚧 Check back regularly for updates, release coming in mid-September!**

This package uses [`pdm`](https://pdm-project.org/en/latest/) for package and virtual environment management.
To install `pdm`, run:

```
curl -sSL https://pdm-project.org/install-pdm.py | python3 -
```

On Ubuntu, it may be necessary to do the following:
```
apt install python3.10-venv
```

To install dependencies:

```
pdm install
pdm run pre-commit install
```

To run unit tests:

```
pdm run pytest
```

To check types:

```
pdm run mypy .
```

We use alembic to manage system table schema migrations.
To generate a new migration, run:
```
pdm run alembic revision -m "<new migration name>"
```

This command will add a new file under the `dbos/migrations/versions/` folder.
For more information, read [alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html).
