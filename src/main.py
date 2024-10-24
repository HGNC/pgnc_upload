#!/usr/bin/env python3
"""This script uploads a TSV file to the PGNC database. It parses the input file,
processes the data, and inserts it into the database through an SSH tunnel.

Modules:
    os: Provides a way of using operating system dependent functionality.
    pathlib: Provides classes to handle filesystem paths.
    typing: Provides runtime support for type hints.
    argparse: Provides a command-line argument parsing functionality.
    pandas: Provides data structures and data analysis tools.
    dotenv: Loads environment variables from a .env file.
    psycopg2: Provides a PostgreSQL database adapter for Python.
    pgncdb.dbc: Provides a function to establish a database connection through an SSH tunnel.
    pgnc.inserts: Provides functions to insert gene-related information into the database.

Functions:
    get_args() -> Namespace:
        Parses command line arguments and returns them as a Namespace object.

    get_file_data(file_path: str, cols: Optional[List[str]] = None) -> pd.DataFrame:
        Reads a TSV file and returns its content as a DataFrame.

    process_df(df: pd.DataFrame, cur: cursor) -> None:

    main() -> None:
        Main function to establish a database connection through an SSH tunnel,
        parses the input file, and loads the data into the DB.
"""
import os
from pathlib import Path
from typing import List, Optional
from argparse import ArgumentParser, Namespace
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extensions import cursor
from pgncdb import (
    get_tunnel_db_connection,
    add_gene_location,
    add_gene_locus_type,
    add_gene_name,
    add_gene_symbol
)

load_dotenv(Path(__file__).parent.parent / '.env')
PKEY = str(Path(__file__).parent.parent / os.getenv('PKEY_NAME'))


def get_args() -> Namespace:
    """
    Parse command line arguments.

    Returns:
        Namespace: The parsed arguments.
    """
    parser = ArgumentParser(
        prog='pgnc_upload',
        description='Upload a TSV file to the PGNC database.'
    )
    parser.add_argument(
        '-f', '--file', help='The name of the TSV file to upload.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print verbose output.')
    return parser.parse_args()


def get_file_data(file_path: str, cols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Reads a tab-separated values (TSV) file into a pandas DataFrame.

    Parameters:
        file_path (str): The path to the TSV file.
        cols (Optional[List[str]]): A list of column names to read from the file. If None,
                                    all columns are read.
                                    Allowed columns are:
                                        'PotriID',
                                        'Gene symbol',
                                        'Symbol type',
                                        'Gene name',
                                        'Name type', 
                                        'Location',
                                        'Locus type'.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the TSV file.

    Raises:
        ValueError: If any of the columns specified in `cols` are not in the allowed set of columns.
    """
    if cols is None:
        return pd.read_csv(
            file_path, sep='\t', index_col='PotriID'
        )
    else:
        valid_cols = {
            'Gene symbol', 'Symbol type', 'Gene name', 'Name type', 'Location', 'Locus type'
        }
        if not set(cols).issubset(valid_cols):
            raise ValueError(
                f"Invalid columns found. Allowed columns are: {valid_cols}")
        return pd.read_csv(
            file_path, sep='\t', index_col='PotriID', usecols=cols
        )


def process_df(df: pd.DataFrame, cur: cursor) -> None:
    """
    Processes a DataFrame to extract gene-related information and inserts it into a database.

    Args:
        df (pd.DataFrame): The DataFrame containing gene information with columns 'Location', 
                           'Locus type', 'Gene symbol', 'Symbol type', 'Gene name', and 'Name type'.
        cur (cursor): The database cursor used to execute SQL commands.

    Returns:
        None
    """
    gene_location = []
    gene_locus_type = []
    gene_symbol = []
    gene_name = []
    for index, row in df.iterrows():
        # Get PotriID and get gene ID from the gene table in the DB
        gene_location.append((index, str(row['Location'])))
        gene_locus_type.append((index, row['Locus type']))
        gene_symbol.append((index, row['Gene symbol'], row['Symbol type']))
        gene_name.append((index, row['Gene name'], row['Name type']))
    add_gene_location(cur, gene_location)
    add_gene_locus_type(cur, gene_locus_type)
    add_gene_name(cur, gene_name)
    add_gene_symbol(cur, gene_symbol)


def main():
    """
    Main function to establish a database connection through an SSH tunnel,
    parses the input file and loads the data into the DB.

    Environment Variables:
    - BASTION_IP: IP address of the bastion host.
    - BASTION_USER: Username for the bastion host.
    - PKEY: Private key for SSH authentication.
    - DB_NAME: Name of the database.
    - DB_PASS: Password for the database user.
    - DB_USER: Username for the database.
    - RDS_HOST: Hostname of the RDS instance.
    Args:
    None
    Returns:
    None
    """
    args = get_args()
    df = get_file_data(args.file)
    tunnel, conn = get_tunnel_db_connection(
        bastion_host_ip=os.getenv('BASTION_IP'),
        bastion_user=os.getenv('BASTION_USER'),
        bastion_pkey=PKEY,
        db_name=os.getenv('DB_NAME'),
        db_password=os.getenv('DB_PASS'),
        db_user=os.getenv('DB_USER'),
        rds_host=os.getenv('RDS_HOST')
    )
    cur = conn.cursor()
    process_df(df, cur)
    conn.commit()
    tunnel.stop()


if __name__ == '__main__':
    main()
