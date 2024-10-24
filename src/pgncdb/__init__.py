
"""
This module initializes the pgncdb package and defines the public API.

Exports:
    get_tunnel_db_connection (function): Establishes a database connection through an SSH tunnel.
    add_gene_location (function): Inserts gene location data into the database.
    add_gene_locus_type (function): Inserts gene locus type data into the database.
    add_gene_name (function): Inserts gene name data into the database.
    add_gene_symbol (function): Inserts gene symbol data into the database.

Imports:
    get_tunnel_db_connection (from pgncdb.dbc):
        Function to establish a database connection through an SSH tunnel.
    add_gene_symbol (from pgncdb.inserts):
        Function to insert gene symbol data into the database.
    add_gene_name (from pgncdb.inserts):
        Function to insert gene name data into the database.
    add_gene_location (from pgncdb.inserts):
        Function to insert gene location data into the database.
    add_gene_locus_type (from pgncdb.inserts):
        Function to insert gene locus type data into the database.
"""
__all__ = [
    'get_tunnel_db_connection',
    'add_gene_location',
    'add_gene_locus_type',
    'add_gene_name',
    'add_gene_symbol'
]
from pgncdb.dbc import get_tunnel_db_connection
from pgncdb.inserts import (
    add_gene_symbol,
    add_gene_name,
    add_gene_location,
    add_gene_locus_type
)