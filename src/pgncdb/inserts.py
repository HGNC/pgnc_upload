
"""
This module provides functions to insert gene-related data into a PostgreSQL database.
The functions handle bulk insert operations for various gene associations, including
gene locations, locus types, names, and symbols. Each function ensures data integrity
by checking for existing entries and verifying successful insertions.

Modules:
    - logging: Provides logging for the application.
    - typing: Provides runtime support for type hints.
    - psycopg2: A PostgreSQL database adapter for Python.
    - psycopg2.errors: Provides error codes for PostgreSQL exceptions.
    - psycopg2.extensions: Provides the cursor class for database operations.

Functions:
    - add_gene_location(cur: cursor, data: List[Tuple[str, str]]) -> None:
    - add_gene_locus_type(cur: cursor, data: List[Tuple[str, str]]) -> None:
    - add_gene_name(cur: cursor, data: List[Tuple[str, str, str]]) -> None:
    - add_gene_symbol(cur: cursor, data: List[Tuple[str, str, str]]) -> None:

Notes:
    Each function raises appropriate exceptions for data integrity issues and logs warnings
    for unsuccessful insertions.
"""
from logging import warning
from typing import List, Tuple
import psycopg2
import psycopg2.errors
from psycopg2.extensions import cursor

def add_gene_location(cur: cursor, data: List[Tuple[str, str]]) -> None:
    """
    Inserts gene and location associations into the gene_has_location table.
    
    Args:
        cur (cursor): The database cursor to execute SQL commands.
        data (List[Tuple[str, str]]): A list of tuples where each tuple contains
            a gene PotriID and a location name.
    Raises:
        ValueError: If the combination of PotriID and Location already exists in the
            gene_has_location table.
        RuntimeError: If the number of affected rows does not match the number of rows
            to insert and no missing entries are found.
    Returns:
        None

    Notes:
        - The function uses a bulk insert operation to add multiple gene-location
          associations at once.
        - If any of the provided gene-location pairs already exist in the table,
          a ValueError is raised.
        - After insertion, the function checks if all rows were successfully inserted.
          If not, it logs a warning with the details of the unsuccessful inserts.
    """
    try:
        cur.executemany(
            '''insert into gene_has_location (
                gene_id, location_id, creator_id, editor_id, status
            )
            SELECT
                gene."id",
                "location"."id",
                1,
                1,
                'internal' 
            FROM
                gene,
                "location",
                assembly_has_location 
            WHERE
                gene.potri_id = %s AND
                "location"."name" = %s AND
                assembly_has_location.assembly_id = 1 AND
                "location"."id" = assembly_has_location.location_id;''',
            data
        )
    except psycopg2.errors.lookup('23505') as e:
        raise ValueError(
            '''
            The combination of PotriID and Location already exists in the gene_has_location
            table.
            See the exception above the ValueError for details.
            '''
        ) from e
    # Check for successful insertion
    affected_rows = cur.rowcount
    if affected_rows == len(data):
        return None
    # Check for unsuccessful insertion
    cur.execute('SELECT gene_id, location_id FROM gene_has_location')
    inserted_rows = cur.fetchall()
    inserted_set = set(inserted_rows)
    unsuccessful_inserts = []
    for gene_id, location_name in data:
        cur.execute(
            '''SELECT gene.id, location.id
            FROM gene, location
            WHERE gene.potri_id = %s AND location.name = %s''',
            (gene_id, location_name)
        )
        result = cur.fetchone()
        if result not in inserted_set:
            unsuccessful_inserts.append((gene_id, location_name))

    if unsuccessful_inserts:
        unsuccessful_inserts_str = '\n '.join(
            [f'({gene_id}, {location_name})' for gene_id, location_name in unsuccessful_inserts]
        )
        warning('\n'.join([
            "The following rows were not inserted into the gene_has_location table:",
            unsuccessful_inserts_str
        ]))
        return None
    error_msg = " ".join([
        f"Affected rows ({affected_rows}) do not match the number of rows to insert",
        f"({len(data)}) and yet no missing entries are found."
    ])
    raise RuntimeError(error_msg)

def add_gene_locus_type(cur: cursor, data: List[Tuple[str, str]]) -> None:
    """
    Inserts gene and locus type associations into the gene_has_locus_type table.
    
    Args:
        cur (cursor): The database cursor to execute SQL commands.
        data (List[Tuple[str, str]]): A list of tuples, where each tuple contains
            a gene PotriID and a locus type name.
    Raises:
        ValueError: If the combination of PotriID and Locus Type already exists
            in the gene_has_locus_type table.
        RuntimeError: If the number of affected rows does not match the number of
            rows to insert and no missing fields are found.
    Returns:
        None

    Notes:
        - The function uses a bulk insert operation to add multiple gene-locus type
          associations at once.
        - If any of the provided gene-locus type pairs already exist in the table,
          a ValueError is raised.
        - After insertion, the function checks if all rows were successfully inserted.
          If not, it logs a warning with the details of the unsuccessful inserts.
    """
    try:
        cur.executemany(
            '''insert into gene_has_locus_type (
                gene_id, locus_type_id, creator_id, editor_id, status
            )
            SELECT
                gene."id",
                locus_type."id",
                1,
                1,
                'internal' 
            FROM
                gene,
                locus_type
            WHERE
                gene.potri_id = %s AND
                locus_type."name" = %s;''',
            data
        )
    except psycopg2.errors.lookup('23505') as e:
        raise ValueError(
            '''
            The combination of PotriID and Locus Type already exists in the gene_has_locus_type
            table.
            See the exception above the ValueError for details.
            '''
        ) from e
    # Check for successful insertion
    affected_rows = cur.rowcount
    if affected_rows == len(data):
        return None
    # Check for unsuccessful insertion
    cur.execute('SELECT gene_id, locus_type_id FROM gene_has_locus_type')
    inserted_rows = cur.fetchall()
    inserted_set = set(inserted_rows)
    unsuccessful_inserts = []
    for gene_id, locus_type_name in data:
        cur.execute(
            '''SELECT gene.id, locus_type.id
            FROM gene, locus_type
            WHERE gene.potri_id = %s AND locus_type.name = %s''',
            (gene_id, locus_type_name)
        )
        result = cur.fetchone()
        if result not in inserted_set:
            unsuccessful_inserts.append((gene_id, locus_type_name))

    if unsuccessful_inserts:
        unsuccessful_inserts_str = '\n'.join(
            [f'({gene_id}, {locus_type_name})' for gene_id, locus_type_name in unsuccessful_inserts]
        )
        warning('\n'.join([
            "The following rows were not inserted into the gene_has_locus_type table:",
            unsuccessful_inserts_str
        ]))
        return None
    error_msg = " ".join([
        "Affected rows do not match the number of rows to insert",
        "and yet no missing fields are found."
    ])
    raise RuntimeError(error_msg)

def add_gene_name(cur: cursor, data: List[Tuple[str, str, str]]) -> None:
    """
    Inserts gene names into the database and associates them with genes.
    This function takes a database cursor and a list of tuples containing gene information.
    It inserts the gene names into the `name` table and associates them with genes in the
    `gene_has_name` table. If a name already exists in the `name` table, a ValueError is raised.
    If there is an issue inserting into the `gene_has_name` table, a RuntimeError is raised.
    
    Args:
        cur (cursor): The database cursor used to execute SQL commands.
        data (List[Tuple[str, str, str]]): A list of tuples, each containing:
            - potri_id (str): The gene identifier.
            - name (str): The name to be inserted.
            - name_type (str): The type of the name.
    Raises:
        ValueError: If the name already exists in the `name` table.
        RuntimeError: If there is an issue inserting into the `gene_has_name` table or if the
                      number of affected rows does not match the number of rows to insert.
    Returns:
        None
    """
    cnt = 0
    for potri_id, name, name_type in data:
        try:
            cur.execute('''INSERT INTO name (name) VALUES (%s) RETURNING id''', (name,))
            name_id = cur.fetchone()[0]
        except psycopg2.errors.lookup('23505') as e:
            raise ValueError(
                f'''
                The name "{name}" already exists in the name table.
                See the exception above the ValueError for details.
                '''
            ) from e
        try:
            cur.execute(
                '''INSERT INTO gene_has_name (gene_id, name_id, type, creator_id, editor_id, status)
                SELECT
                    gene.id,
                    %s,
                    %s,
                    1,
                    1,
                    'internal'
                FROM
                    gene
                WHERE
                    gene.potri_id = %s;
                ''',
                (name_id, name_type, potri_id)
            )
            cnt += cur.rowcount
        except psycopg2.Error as e:
            raise RuntimeError(
                '\n'.join([
                    'Problem inserting into gene_has_name with the values:',
                    f'\tName ID: {name_id}',
                    f'\tName type: {name_type}',
                    f'\tPOTRI ID:{potri_id}'
                ])
            ) from e
    if cnt != len(data):
        cur.execute('SELECT gene_id, name_id FROM gene_has_name')
        inserted_rows = cur.fetchall()
        inserted_set = set(inserted_rows)
        unsuccessful_inserts = []
        for potri_id, name, name_type in data:
            cur.execute(
                '''SELECT gene.id, name.id
                FROM gene, name
                WHERE gene.potri_id = %s AND name.name = %s''',
                (potri_id, name)
            )
            result = cur.fetchone()
            if result not in inserted_set:
                unsuccessful_inserts.append((potri_id, name))
        if unsuccessful_inserts:
            unsuccessful_inserts_str = '\n '.join(
                [f'({potri_id}, {name})' for potri_id, name in unsuccessful_inserts]
            )
            warning('\n'.join([
                "The following rows were not inserted into the gene_has_name:",
                unsuccessful_inserts_str
            ]))  
            return None
        error_msg = " ".join([
            "Affected rows do not match the number of rows to insert",
            "and yet no missing fields are found. Poss. type difference?"
        ])
        raise RuntimeError(error_msg)
    return None

def add_gene_symbol(cur: cursor, data: List[Tuple[str, str, str]]) -> None:
    """
    Inserts gene symbols into the database and associates them with genes.
    This function takes a database cursor and a list of tuples containing gene
    information. Each tuple consists of a gene identifier (potri_id), a symbol,
    and a symbol type. The function attempts to insert each symbol into the 
    `symbol` table and then associates the symbol with the corresponding gene 
    in the `gene_has_symbol` table.

    Args:
        cur (cursor): A database cursor object used to execute SQL commands.
        data (List[Tuple[str, str, str]]): A list of tuples, where each tuple 
            contains:
            - potri_id (str): The identifier for the gene.
            - symbol (str): The symbol to be associated with the gene.
            - symbol_type (str): The type of the symbol.
    Raises:
        ValueError: If a symbol already exists in the `symbol` table.
        RuntimeError: If there is an issue inserting into the `gene_has_symbol` 
            table or if the number of affected rows does not match the number 
            of rows to insert.
    Returns:
        None
    """
    cnt = 0
    for potri_id, symbol, symbol_type in data:
        try:
            cur.execute('''INSERT INTO symbol (symbol) VALUES (%s) RETURNING id''', (symbol,))
            symbol_id = cur.fetchone()[0]
        # Below is catching a unique constraint violation error
        except psycopg2.errors.lookup('23505') as e:
            raise ValueError(
                f'''
                The name "{symbol}" already exists in the name table.
                See the exception above the ValueError for details.
                '''
            ) from e
        try:
            cur.execute(
                '''INSERT INTO gene_has_symbol (
                    gene_id, symbol_id, type, creator_id, editor_id, status
                )
                SELECT
                    gene.id,
                    %s,
                    %s,
                    1,
                    1,
                    'internal'
                FROM
                    gene
                WHERE
                    gene.potri_id = %s;
                ''',
                (symbol_id, symbol_type, potri_id)
            )
            cnt += cur.rowcount
        except psycopg2.Error as e:
            raise RuntimeError(
                '\n'.join([
                    'Problem inserting into gene_has_name with the values:',
                    f'\tName ID: {symbol_id}',
                    f'\tName type: {symbol_type}',
                    f'\tPOTRI ID:{potri_id}'
                ])
            ) from e
    if cnt != len(data):
        cur.execute('SELECT gene_id, symbol_id FROM gene_has_symbol')
        inserted_rows = cur.fetchall()
        inserted_set = set(inserted_rows)
        unsuccessful_inserts = []
        for potri_id, symbol, symbol_type in data:
            cur.execute(
                '''SELECT gene.id, symbol.id
                FROM gene, symbol
                WHERE gene.potri_id = %s AND symbol.symbol = %s''',
                (potri_id, symbol)
            )
            result = cur.fetchone()
            if result not in inserted_set:
                unsuccessful_inserts.append((potri_id, symbol))

        if unsuccessful_inserts:
            unsuccessful_inserts_str = '\n '.join(
                [f'({potri_id}, {symbol})' for potri_id, symbol in unsuccessful_inserts]
            )
            warning('\n'.join([
                "The following rows were not inserted into the database:",
                unsuccessful_inserts_str
            ]))
            return None
        error_msg = " ".join([
            "Affected rows do not match the number of rows to insert",
            "and yet no missing fields are found. Poss. type difference?"
        ])
        raise RuntimeError(error_msg)
    return None
