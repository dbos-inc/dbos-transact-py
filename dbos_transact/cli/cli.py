import typer

app = typer.Typer()


@app.command()
def hello(name: str) -> None:
    typer.echo(f"Hello {name}")


@app.command()
def goodbye(name: str, formal: bool = False) -> None:
    if formal:
        typer.echo(f"Goodbye, {name}. Have a good day.")
    else:
        typer.echo(f"Bye {name}!")


if __name__ == "__main__":
    app()
