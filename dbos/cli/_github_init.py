import os
from base64 import b64decode
from typing import List, TypedDict

import requests

DEMO_REPO_API = "https://api.github.com/repos/dbos-inc/dbos-demo-apps"
PY_DEMO_PATH = "python/"
BRANCH = "main"


class GitHubTreeItem(TypedDict):
    path: str
    mode: str
    type: str
    sha: str
    url: str
    size: int


class GitHubTree(TypedDict):
    sha: str
    url: str
    tree: List[GitHubTreeItem]
    truncated: bool


class GitHubItem(TypedDict):
    sha: str
    node_id: str
    url: str
    content: str
    encoding: str
    size: int


def _fetch_github(url: str) -> requests.Response:
    headers = {}
    github_token = os.getenv("GITHUB_TOKEN")
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    response = requests.get(url, headers=headers)

    if not response.ok:
        if response.headers.get("x-ratelimit-remaining") == "0":
            raise Exception(
                "Error fetching from GitHub API: rate limit exceeded.\n"
                "Please wait a few minutes and try again.\n"
                "To increase the limit, you can create a personal access token and set it in the GITHUB_TOKEN environment variable.\n"
                "Details: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api"
            )
        elif response.status_code == 401:
            raise Exception(
                f"Error fetching content from GitHub {url}: {response.status_code} {response.reason}.\n"
                "Please ensure your GITHUB_TOKEN environment variable is set to a valid personal access token."
            )
        raise Exception(
            f"Error fetching content from GitHub {url}: {response.status_code} {response.reason}"
        )

    return response


def _fetch_github_tree(tag: str) -> List[GitHubTreeItem]:
    response = _fetch_github(f"{DEMO_REPO_API}/git/trees/{tag}?recursive=1")
    tree_data: GitHubTree = response.json()
    return tree_data["tree"]


def _fetch_github_item(url: str) -> str:
    response = _fetch_github(url)
    item: GitHubItem = response.json()
    return b64decode(item["content"]).decode("utf-8")


def create_template_from_github(app_name: str, template_name: str) -> None:
    print(
        f"Creating a new application named {app_name} from the template {template_name}"
    )

    tree = _fetch_github_tree(BRANCH)
    template_path = f"{PY_DEMO_PATH}{template_name}/"

    files_to_download = [
        item
        for item in tree
        if item["path"].startswith(template_path) and item["type"] == "blob"
    ]

    # Download every file from the template
    for item in files_to_download:
        raw_content = _fetch_github_item(item["url"])
        file_path = item["path"].replace(template_path, "")
        target_path = os.path.join(".", file_path)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        # Write file with proper permissions
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(raw_content)
        os.chmod(target_path, int(item["mode"], 8))

    print(
        f"Downloaded {len(files_to_download)} files from the template GitHub repository"
    )
