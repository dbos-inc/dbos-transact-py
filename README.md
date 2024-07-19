# DBOS Transact Python

This package uses [`pdm`](https://pdm-project.org/en/latest/) for package and virtual environment management.
To install `pdm`, run:

```
curl -sSL https://pdm-project.org/install-pdm.py | python3 -
```

To install dependencies:

```
pdm install
```

To run unit tests:

```
pdm run pytest -s tests
```

To check types:

```
pdm run mypy .
```