from dbos import DBOS


@DBOS.workflow()
def my_function(foo: str) -> str:
    return foo
