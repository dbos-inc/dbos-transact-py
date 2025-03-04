### Setting Up for Development

#### Installing `pdm` and `venv`

This package uses [`pdm`](https://pdm-project.org/en/latest/) for package and
virtual environment management.
To install `pdm`, run:

```
curl -sSL https://pdm-project.org/install-pdm.py | python3 -
```

`pdm` is installed in `~/.local/bin`. You might have to add this directory to 
the `PATH` variable.

On Ubuntu, it may be necessary to install the following for Python 3.10 before 
installing `pdm`:

```
apt install python3.10-venv
```

For Python 3.12 run instead:

```
apt install python3.12-venv
```

#### Installing Python dependencies with `pdm`

NOTE: If you already have a virtual environment for this project, activate
it so that the dependencies are installed into your existing virtual
environment. If you do not have a virtual environment, `pdm` creates one
in `.venv`.

To install dependencies:

```
pdm install
pdm run pre-commit install
```

#### Executing unit tests, checking types, manage system table schema migrations

To run unit tests:

```
pdm run pytest
```

NOTE: The tests need a Postgres database running on `localhost:5432`. To start
one, run:

```bash
export PGPASSWORD=dbos
python3 dbos/_templates/dbos-db-starter/start_postgres_docker.py
```

A successful test run results in the following output:

```
=============================== warnings summary ===============================
<frozen importlib._bootstrap>:488
  <frozen importlib._bootstrap>:488: DeprecationWarning: Type google._upb._message.MessageMapContainer uses PyType_Spec with a metaclass that has custom tp_new. This is deprecated and will no longer be allowed in Python 3.14.

<frozen importlib._bootstrap>:488
  <frozen importlib._bootstrap>:488: DeprecationWarning: Type google._upb._message.ScalarMapContainer uses PyType_Spec with a metaclass that has custom tp_new. This is deprecated and will no longer be allowed in Python 3.14.

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
============ 182 passed, 2 skipped, 2 warnings in 254.62s (0:04:14) ============
```

The two skipped test cases verify the interaction with Kafka and if Kafka is not available,
the test cases are skipped.

To check types:

```
pdm run mypy .
```

A successful check of types results in (the number of files might be different depending
on the changes in the project since this was written):

```
Success: no issues found in 64 source files
```

We use alembic to manage system table schema migrations.
To generate a new migration, run:

```
pdm run alembic revision -m "<new migration name>"
```

This command will add a new file under the `dbos/migrations/versions/` folder.
For more information,
read [alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

### Creating a Release

To cut a new release, run:

```shell
python3 make_release.py [--version_number <version>]
```

Version numbers follow [semver](https://semver.org/).
This command tags the latest commit with the version number and creates a
release branch for it.
If a version number is not supplied, it automatically generated a version number
by incrementing the last released minor version.

### Patching a release

To patch a release, push the patch as a commit to the appropriate release
branch.
Then, tag it with a version number:

```shell
git tag <version-number>
git push --tags
```

This version must follow semver: It should increment by one the patch number of
the release branch.

### Preview Versions

Preview versions are [PEP440](https://peps.python.org/pep-0440/)-compliant alpha
versions.
They can be published from `main`.
Their version number is
`<next-release-version>a<number-of-git-commits-since-release>`.
You can install the latest preview version with `pip install --pre dbos`.

### Test Versions

Test versions are built from feature branches.
Their version number is
`<next-release-version>a<number-of-git-commits-since-release>+<git-hash>`.

### Publishing

Run the [`Publish to PyPI`](./.github/workflows/publish.yml) GitHub action on
the target branch.
