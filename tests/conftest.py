from dbos_transact import ConfigFile
import os

defaultConfig: ConfigFile = {
    'database': {
        'hostname': "localhost",
        'port': 5432,
        'username': 'postgres',
        'password': os.environ["PGPASSWORD"],
        'app_db_name': 'dbostestpy',
    }
}