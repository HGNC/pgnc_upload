"""
This module provides functionality to establish a database connection through an SSH tunnel.

Functions:
    get_tunnel_db_connection(
        bastion_host_ip: str,
        bastion_user: str,
        bastion_pkey: str,
        db_name: str,
        db_password: str,
        db_user: str,
        rds_host: str
    ) -> TunnelConnection:
        Establishes an SSH tunnel to a bastion host and connects to a PostgreSQL database
        through the tunnel.

Types:
    Connection: Alias for psycopg2.extensions.connection.
    TunnelConnection: Tuple containing an SSHTunnelForwarder instance and a psycopg2
    database connection.
"""
from typing import Tuple
from sshtunnel import SSHTunnelForwarder
import psycopg2


Connection = psycopg2.extensions.connection
TunnelConnection = Tuple[SSHTunnelForwarder, Connection]


def get_tunnel_db_connection(
    bastion_host_ip: str,
    bastion_user: str,
    bastion_pkey: str,
    db_name: str,
    db_password: str,
    db_user: str,
    rds_host: str,
) -> TunnelConnection:
    """
    Establishes an SSH tunnel to a bastion host and connects to an RDS database.
    Args:
        bastion_host_ip (str): The IP address of the bastion host.
        bastion_user (str): The username for SSH connection to the bastion host.
        bastion_pkey (str): The path to the private key file for SSH authentication.
        db_name (str): The name of the database to connect to.
        db_password (str): The password for the database user.
        db_user (str): The username for the database connection.
        rds_host (str): The hostname of the RDS instance.
    Returns:
        TunnelConnection: A tuple containing the SSH tunnel server and the database connection.
    """
    bastion_port = 22
    rds_port = 5432
    server = SSHTunnelForwarder(
        (bastion_host_ip, bastion_port),
        ssh_username=bastion_user,
        ssh_pkey=bastion_pkey,
        remote_bind_address=(rds_host, rds_port)
    )
    server.start()
    connection = psycopg2.connect(
        host='127.0.0.1',
        port=server.local_bind_port,
        dbname=db_name,
        password=db_password,
        user=db_user
    )
    return server, connection
