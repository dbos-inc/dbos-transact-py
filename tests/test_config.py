from unittest.mock import mock_open

import dbos_transact.dbos_config

def test_config(mocker):
    mock_config = '''
        database:
          hostname: 'some host'
          port: 1234
          username: 'some user'
          password: abc123
          connectionTimeoutMillis: 3000
    '''
    m = mock_open(read_data = mock_config)
    mocker.patch('builtins.open', m)

    configFile = dbos_transact.dbos_config.load_config("test.yaml")
    assert configFile['database']['hostname'] == 'some host'
    assert configFile['database']['port'] == 1234
    assert configFile['database']['username'] == 'some user'
    assert configFile['database']['password'] == 'abc123'
    assert configFile['database']['connectionTimeoutMillis'] == 3000
    
