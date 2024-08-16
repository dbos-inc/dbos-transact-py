import os
import re

import typer
from git import Repo

app = typer.Typer()


@app.command()
def make_release(version_number: str) -> None:
    version_pattern = r"^\d+\.\d+\.\d+$"
    if not re.match(version_pattern, version_number):
        raise Exception(f"Invalid version number: {version_number}")

    repo = Repo(os.getcwd())
    if repo.is_dirty():
        raise Exception("Local git repository is not clean")
    if repo.active_branch.name != "main":
        raise Exception("Can only make a release from main")

    create_and_push_release_tag(repo=repo, version_number=version_number)
    create_and_push_release_branch(repo=repo, version_number=version_number)


def create_and_push_release_tag(repo: Repo, version_number: str) -> None:
    release_tag = repo.create_tag(version_number)
    push_info = repo.remote("origin").push(release_tag)
    if push_info[0].flags & push_info[0].ERROR:
        raise Exception(f"Failed to push tags: {push_info[0].summary}")
    print(f"Release tag pushed: {version_number}")


def create_and_push_release_branch(repo: Repo, version_number: str) -> None:
    branch_name = f"release/v{version_number}"
    new_branch = repo.create_head(branch_name, repo.heads["main"])
    new_branch.checkout()
    push_info = repo.remote("origin").push(new_branch)
    if push_info[0].flags & push_info[0].ERROR:
        raise Exception(f"Failed to push branch: {push_info[0].summary}")
    print(f"Release branch pushed: {branch_name}")


if __name__ == "__main__":
    app()
