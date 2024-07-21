import os

from dbos_transact import ConfigFile

defaultConfig: ConfigFile = {
    "database": {
        "hostname": "localhost",
        "port": 5432,
        "username": "postgres",
        "password": os.environ["PGPASSWORD"],
        "app_db_name": "dbostestpy",
    }
}
