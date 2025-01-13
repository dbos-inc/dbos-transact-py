import json
import os
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Union

import jwt
import requests
import typer
from rich import print

from dbos._error import DBOSInitializationError

from .._logger import dbos_logger
from .authentication import authenticate


# Must be the same as in TS
@dataclass
class DBOSCloudCredentials:
    token: str
    userName: str
    organization: str
    refreshToken: Optional[str] = None


@dataclass
class UserProfile:
    Name: str
    Email: str
    Organization: str
    SubscriptionPlan: str


class AppLanguages(Enum):
    Node = "node"
    Python = "python"


dbos_config_file_path = "dbos-config.yaml"
DBOS_CLOUD_HOST = os.getenv("DBOS_DOMAIN", "cloud.dbos.dev")
dbos_env_path = ".dbos"


def is_token_expired(token: str) -> bool:
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        exp: int = decoded.get("exp")
        if not exp:
            return False
        return time.time() >= exp
    except Exception:
        return True


def credentials_exist() -> bool:
    return os.path.exists(os.path.join(dbos_env_path, "credentials"))


def delete_credentials() -> None:
    credentials_path = os.path.join(dbos_env_path, "credentials")
    if os.path.exists(credentials_path):
        os.unlink(credentials_path)


def write_credentials(credentials: DBOSCloudCredentials) -> None:
    os.makedirs(dbos_env_path, exist_ok=True)
    with open(os.path.join(dbos_env_path, "credentials"), "w", encoding="utf-8") as f:
        json.dump(credentials.__dict__, f)


def check_read_file(path: str, encoding: str = "utf-8") -> Union[str, bytes]:
    # Check if file exists and is a file
    if not os.path.exists(path):
        raise FileNotFoundError(f"File {path} does not exist")
    if not os.path.isfile(path):
        raise IsADirectoryError(f"Path {path} is not a file")

    # Read file content
    with open(path, encoding=encoding) as f:
        return f.read()


@dataclass
class CloudAPIErrorResponse:
    message: str
    status_code: int
    request_id: str
    detailed_error: Optional[str] = None


def is_cloud_api_error_response(obj: Any) -> bool:
    return (
        isinstance(obj, dict)
        and "message" in obj
        and isinstance(obj["message"], str)
        and "statusCode" in obj
        and isinstance(obj["statusCode"], int)
        and "requestID" in obj
        and isinstance(obj["requestID"], str)
    )


def handle_api_errors(label: str, e: requests.exceptions.RequestException) -> None:
    if hasattr(e, "response") and e.response is not None:
        resp = e.response.json()
        if is_cloud_api_error_response(resp):
            message = f"[{resp['requestID']}] {label}: {resp['message']}."
            dbos_logger.error(message)
            raise DBOSInitializationError(message)


def is_valid_username(value: str) -> Union[bool, str]:
    if len(value) < 3 or len(value) > 30:
        return "Username must be 3~30 characters long"
    if not re.match("^[a-z0-9_]+$", value):
        return "Username must contain only lowercase letters, numbers, and underscores."
    return True


def check_user_profile(credentials: DBOSCloudCredentials) -> bool:
    bearer_token = f"Bearer {credentials.token}"
    try:
        response = requests.get(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/user/profile",
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()
        profile = UserProfile(**response.json())
        credentials.userName = profile.Name
        credentials.organization = profile.Organization
        return True
    except requests.exceptions.RequestException as e:
        error_label = "Failed to login"
        if hasattr(e, "response") and e.response is not None:
            resp = e.response.json()
            if is_cloud_api_error_response(resp):
                if "user not found in DBOS Cloud" not in resp["message"]:
                    handle_api_errors(error_label, e)
                    exit(1)
        else:
            dbos_logger.error(f"{error_label}: {str(e)}")
            exit(1)
    return False


def register_user(credentials: DBOSCloudCredentials) -> None:
    print("Please register for DBOS Cloud")

    user_name = None
    while not user_name:
        user_name = typer.prompt("Choose your username")
        validation_result = is_valid_username(user_name)
        if validation_result is not True:
            print(f"[red]Invalid username: {validation_result}[/red]")
            user_name = None
            continue

    bearer_token = f"Bearer {credentials.token}"
    try:
        # Register user
        response = requests.put(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/user",
            json={
                "name": user_name,
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()

        # Get user profile
        response = requests.get(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/user/profile",
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()
        profile = UserProfile(**response.json())
        credentials.userName = profile.Name
        credentials.organization = profile.Organization
        print(f"[green]Successfully registered as {credentials.userName}[/green]")

    except requests.exceptions.RequestException as e:
        error_label = f"Failed to register user {user_name}"
        if hasattr(e, "response") and e.response is not None:
            handle_api_errors(error_label, e)
        else:
            dbos_logger.error(f"{error_label}: {str(e)}")
        exit(1)


def check_credentials() -> DBOSCloudCredentials:
    empty_credentials = DBOSCloudCredentials(token="", userName="", organization="")

    if not credentials_exist():
        return empty_credentials

    try:
        with open(os.path.join(dbos_env_path, "credentials"), "r") as f:
            cred_data = json.load(f)
            credentials = DBOSCloudCredentials(**cred_data)

        # Trim trailing /r /n
        credentials.token = credentials.token.strip()

        if is_token_expired(credentials.token):
            print("Credentials expired. Logging in again...")
            delete_credentials()
            return empty_credentials

        return credentials
    except Exception as e:
        dbos_logger.error(f"Error loading credentials: {str(e)}")
        return empty_credentials


def get_cloud_credentials() -> DBOSCloudCredentials:
    # Check if credentials exist and are not expired
    credentials = check_credentials()

    # Log in the user
    if not credentials.token:
        auth_response = authenticate()
        if auth_response is None:
            dbos_logger.error("Failed to login. Exiting...")
            exit(1)
        credentials.token = auth_response.token
        credentials.refreshToken = auth_response.refresh_token
        write_credentials(credentials)

    # Check if the user exists in DBOS Cloud
    user_exists = check_user_profile(credentials)
    if user_exists:
        write_credentials(credentials)
        print(f"[green]Successfully logged in as {credentials.userName}[/green]")
        return credentials

    # User doesn't exist, register the user in DBOS Cloud
    register_user(credentials)
    write_credentials(credentials)

    return credentials
