import base64
import random
import time
from dataclasses import dataclass
from typing import Any, List, Optional

import requests
from rich import print

from dbos._cloudutils.cloudutils import (
    DBOS_CLOUD_HOST,
    DBOSCloudCredentials,
    handle_api_errors,
    is_cloud_api_error_response,
)
from dbos._error import DBOSInitializationError

from .._logger import dbos_logger


@dataclass
class UserDBCredentials:
    RoleName: str
    Password: str


@dataclass
class UserDBInstance:
    PostgresInstanceName: str = ""
    Status: str = ""
    HostName: str = ""
    Port: int = 0
    DatabaseUsername: str = ""
    IsLinked: bool = False
    SupabaseReference: Optional[str] = None

    def __init__(self, **kwargs: Any) -> None:
        self.PostgresInstanceName = kwargs.get("PostgresInstanceName", "")
        self.Status = kwargs.get("Status", "")
        self.HostName = kwargs.get("HostName", "")
        self.Port = kwargs.get("Port", 0)
        self.DatabaseUsername = kwargs.get("DatabaseUsername", "")
        self.IsLinked = kwargs.get("IsLinked", False)
        self.SupabaseReference = kwargs.get("SupabaseReference", None)


def get_user_db_info(credentials: DBOSCloudCredentials, db_name: str) -> UserDBInstance:
    bearer_token = f"Bearer {credentials.token}"

    try:
        response = requests.get(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/{credentials.organization}/databases/userdb/info/{db_name}",
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()
        data = response.json()
        return UserDBInstance(**data)
    except requests.exceptions.RequestException as e:
        error_label = f"Failed to get status of database {db_name}"
        if hasattr(e, "response") and e.response is not None:
            resp = e.response.json()
            if is_cloud_api_error_response(resp):
                handle_api_errors(error_label, e)
        else:
            dbos_logger.error(f"{error_label}: {str(e)}")
        raise DBOSInitializationError(f"{error_label}: {str(e)}")


def get_user_db_credentials(
    credentials: DBOSCloudCredentials, db_name: str
) -> UserDBCredentials:
    bearer_token = f"Bearer {credentials.token}"
    try:
        response = requests.get(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/{credentials.organization}/databases/userdb/{db_name}/credentials",
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()
        data = response.json()
        return UserDBCredentials(**data)
    except requests.exceptions.RequestException as e:
        error_label = f"Failed to get credentials for database {db_name}"
        if hasattr(e, "response") and e.response is not None:
            resp = e.response.json()
            if is_cloud_api_error_response(resp):
                handle_api_errors(error_label, e)
        else:
            dbos_logger.error(f"{error_label}: {str(e)}")
        raise DBOSInitializationError(f"{error_label}: {str(e)}")


def create_user_role(credentials: DBOSCloudCredentials, db_name: str) -> None:
    bearer_token = f"Bearer {credentials.token}"
    try:
        response = requests.post(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/{credentials.organization}/databases/userdb/{db_name}/createuserdbrole",
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        error_label = f"Failed to create a user role for database {db_name}"
        if hasattr(e, "response") and e.response is not None:
            resp = e.response.json()
            if is_cloud_api_error_response(resp):
                handle_api_errors(error_label, e)
        else:
            dbos_logger.error(f"{error_label}: {str(e)}")
        raise DBOSInitializationError(f"{error_label}: {str(e)}")


def create_user_db(
    credentials: DBOSCloudCredentials,
    db_name: str,
    app_db_username: str,
    app_db_password: str,
) -> int:
    bearer_token = f"Bearer {credentials.token}"

    try:
        response = requests.post(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/{credentials.organization}/databases/userdb",
            json={
                "Name": db_name,
                "AdminName": app_db_username,
                "AdminPassword": app_db_password,
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()

        print(f"Successfully started provisioning {db_name}")

        status = ""
        while status not in ["available", "backing-up"]:
            if status == "":
                time.sleep(5)  # First time sleep 5 sec
            else:
                time.sleep(30)  # Otherwise, sleep 30 sec

            user_db_info = get_user_db_info(credentials, db_name)
            status = user_db_info.Status
            print(
                f"[bold blue]Waiting for cloud database to finish provisioning. Status:[/bold blue] [yellow]{status}[/yellow]"
            )

        print("[green]Database successfully provisioned![/green]")
        return 0

    except requests.exceptions.RequestException as e:
        error_label = f"Failed to create database {db_name}"
        if hasattr(e, "response") and e.response is not None:
            resp = e.response.json()
            if is_cloud_api_error_response(resp):
                handle_api_errors(error_label, e)
        else:
            dbos_logger.error(f"{error_label}: {str(e)}")
        return 1


def choose_database(credentials: DBOSCloudCredentials) -> Optional[UserDBInstance]:
    # List existing database instances
    user_dbs: List[UserDBInstance] = []
    bearer_token = f"Bearer {credentials.token}"

    try:
        response = requests.get(
            f"https://{DBOS_CLOUD_HOST}/v1alpha1/{credentials.organization}/databases",
            headers={
                "Content-Type": "application/json",
                "Authorization": bearer_token,
            },
        )
        response.raise_for_status()
        data = response.json()
        user_dbs = [UserDBInstance(**db) for db in data]

    except requests.exceptions.RequestException as e:
        error_label = "Failed to list databases"
        if hasattr(e, "response") and e.response is not None:
            resp = e.response.json()
            if is_cloud_api_error_response(resp):
                handle_api_errors(error_label, e)
        else:
            dbos_logger.error(f"{error_label}: {str(e)}")
        return None

    if not user_dbs:
        # If not, prompt the user to provision one
        print("Provisioning a cloud Postgres database server")
        user_db_name = f"{credentials.userName}-db-server"

        # Use a default user name and auto generated password
        app_db_username = "dbos_user"
        app_db_password = base64.b64encode(str(random.random()).encode()).decode()
        res = create_user_db(
            credentials, user_db_name, app_db_username, app_db_password
        )
        if res != 0:
            return None
    elif len(user_dbs) > 1:
        # If there is more than one database instance, prompt the user to select one
        choices = [db.PostgresInstanceName for db in user_dbs]
        print("Choose a database instance for this app:")
        for i, entry in enumerate(choices, 1):
            print(f"{i}. {entry}")
        while True:
            try:
                choice = int(input("Enter number: ")) - 1
                if 0 <= choice < len(choices):
                    user_db_name = choices[choice]
                    break
            except ValueError:
                continue
            print("Invalid choice, please try again")
    else:
        # Use the only available database server
        user_db_name = user_dbs[0].PostgresInstanceName
        print(f"[green]Using database instance:[/green] {user_db_name}")

    info = get_user_db_info(credentials, user_db_name)

    if not info.IsLinked:
        create_user_role(credentials, user_db_name)

    return info
