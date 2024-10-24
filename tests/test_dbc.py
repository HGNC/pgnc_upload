import unittest
from unittest.mock import patch, MagicMock
from sshtunnel import SSHTunnelForwarder
import psycopg2
from pgncdb.dbc import get_tunnel_db_connection

class TestGetTunnelDbConnection(unittest.TestCase):

    @patch('pgncdb.dbc.SSHTunnelForwarder')
    @patch('pgncdb.dbc.psycopg2.connect')
    def test_get_tunnel_db_connection(self, mock_connect, mock_SSHTunnelForwarder):
        # Mock the SSHTunnelForwarder instance
        mock_server = MagicMock(spec=SSHTunnelForwarder)
        mock_server.local_bind_port = 12345
        mock_SSHTunnelForwarder.return_value = mock_server

        # Mock the psycopg2 connection
        mock_connection = MagicMock(spec=psycopg2.extensions.connection)
        mock_connect.return_value = mock_connection

        # Define test parameters
        bastion_host_ip = '192.168.1.1'
        bastion_user = 'test_user'
        bastion_pkey = '/path/to/private/key'
        db_name = 'test_db'
        db_password = 'test_password'
        db_user = 'db_user'
        rds_host = 'rds_host'

        # Call the function
        server, connection = get_tunnel_db_connection(
            bastion_host_ip,
            bastion_user,
            bastion_pkey,
            db_name,
            db_password,
            db_user,
            rds_host
        )

        # Assertions
        mock_SSHTunnelForwarder.assert_called_once_with(
            (bastion_host_ip, 22),
            ssh_username=bastion_user,
            ssh_pkey=bastion_pkey,
            remote_bind_address=(rds_host, 5432)
        )
        mock_server.start.assert_called_once()
        mock_connect.assert_called_once_with(
            host='127.0.0.1',
            port=12345,
            dbname=db_name,
            password=db_password,
            user=db_user
        )
        self.assertEqual(server, mock_server)
        self.assertEqual(connection, mock_connection)

if __name__ == '__main__':
    unittest.main()