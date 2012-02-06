
"""
*
* Proposed extension, for information_schema, pg_catalog, etc.
*
"""

import sys
import doze.backend.pgsql as pgsql

def current_database(conn):
    """ Get current database. """

    cursor = conn.cursor()
    cursor.execute('SELECT CURRENT_DATABASE()')
    database = str(cursor.fetchone()[0])
    cursor.close()
    
    return database

def databases(conn):
    """ Get list of databases, using conn as the connection. """

    query = """
        SELECT
            d.datname, r.rolname AS owner
        FROM
            pg_catalog.pg_database d,
            pg_catalog.pg_roles r
        WHERE r.oid = d.datdba"""

    cursor = conn.cursor()
    cursor.execute(query)
    
    # Get column map
    columns = [i.name for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows

def tables(conn, schema='public'):
    """
    Get list of tables on current database.
    
    @param      schema - str, list, tuple, or None
    @return     tuple - (schema, table, owner)
    """
    
    where = []
    query = (
        'SELECT schemaname, tablename, tableowner '
        'FROM pg_catalog.pg_tables')
    
    if schema is not None:
        if type(schema) == str:
            # schema is string
            
            if len(schema) > 0:
                query += (' WHERE schemaname = %s')
                where.append(schema)
        elif type(schema) == list or type(schema) == tuple:
            # schema is list or tuple
            
            _in = ','.join(['%s' for i in range(0, len(schema))])
            query += (' WHERE schemaname IN (%s)' % (_in))
            where.extend(schema)
        else:
            raise TypeError(('Parameter "schema" must be either'
                ' str, list, or None'))
    
    cursor = conn.cursor()
    cursor.execute(query, where)
    rows = cursor.fetchall()
    cursor.close()
    
    return rows

def sequences(conn, schema='public'):
    """ Get list of sequences """
    
    escape = []
    query = """
        SELECT
            s.relname AS name,
            ns.nspname AS schema,
            a.rolname AS owner
        FROM
            pg_catalog.pg_class s,
            pg_catalog.pg_namespace ns,
            pg_catalog.pg_roles a
        WHERE
            ns.oid = s.relnamespace
            AND a.oid = s.relowner
            AND s.relkind = 'S'"""
    
    
    if type(schema) == str:
        # Normal case, schema is a string
        query += " AND ns.nspname = %s"
        escape.append(schema)
    elif type(schema) == list or type(schema) == tuple:
        # This function also supports lists/tuples for schema
        _in = ','.join(['%s' for i in range(0, len(schema))])
        query += " AND ns.nspname IN (%s)" % (_in)
        escape.extend(schema)
    
    # Execute query
    cursor = conn.cursor()
    cursor.execute(query, escape)
    
    # Get column map
    columns = [i.name for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows

def views(conn, schema='public'):
    """ Get list of views """
    
    escape = []
    query = """
        SELECT
            s.relname AS name,
            ns.nspname AS schema,
            a.rolname AS owner,
            pg_catalog.pg_get_viewdef(s.oid, true) AS definition
        FROM
                pg_catalog.pg_class s,
                pg_catalog.pg_namespace ns,
                pg_catalog.pg_roles a
        WHERE
                ns.oid = s.relnamespace
                AND a.oid = s.relowner
                AND s.relkind = 'v'"""
        
    if type(schema) == str:
        # Normal case, schema is a string
        query += " AND ns.nspname = %s"
        escape.append(schema)
    elif type(schema) == list or type(schema) == tuple:
        # This function also supports lists/tuples for schema
        _in = ','.join(['%s' for i in range(0, len(schema))])
        query += " AND ns.nspname IN (%s)" % (_in)
        escape.extend(schema)
    
    # Execute query
    cursor = conn.cursor()
    cursor.execute(query, escape)
    
    # Get column map
    columns = [i.name for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows

def columns(conn, table, schema='public'):
    """
    Get column definition for table. Must provide at least a database
    connection and a table. Schema defaults to public.
    
    Query code has been borrowed from PEAR::MDB2, with many modifications.
    
    Returns a list of dictionaries.
    """
    
    # This is a bit of an unholy mess
    query = ("""
        SELECT
        tb.tablename AS table,
        tb.schemaname AS schema,
        a.attname AS name,
        t.typname AS internaltype,
        CASE
            WHEN SUBSTRING(TRIM(t.typname) FROM 1 FOR 3) = 'int' THEN 'integer'
            WHEN SUBSTRING(TRIM(t.typname) FROM 1 FOR 4) = 'float' THEN 'float'
            WHEN t.typname = 'varchar' THEN 'char'
            WHEN t.typname = 'text' THEN 'char'
            WHEN t.typname = 'bytea' THEN 'binary'
            ELSE t.typname
        END AS externaltype,
        CASE a.attlen
            WHEN -1 THEN
                CASE t.typname
                    WHEN 'numeric' THEN (a.atttypmod / 65536)
                    WHEN 'decimal' THEN (a.atttypmod / 65536)
                    WHEN 'money'   THEN (a.atttypmod / 65536)
                    ELSE CASE a.atttypmod
                        WHEN -1 THEN NULL
                        ELSE a.atttypmod - 4
                        END
                    END
                ELSE a.attlen
                END AS length,
                CASE t.typname
                    WHEN 'numeric' THEN (a.atttypmod %% 65536) - 4
                    WHEN 'decimal' THEN (a.atttypmod %% 65536) - 4
                    WHEN 'money'   THEN (a.atttypmod %% 65536) - 4
                    ELSE 0
                END AS scale,
        a.attnotnull AS not_null,
        a.atttypmod AS type_mod,
        a.atthasdef AS has_default,
        (SELECT substring(pg_get_expr(d.adbin, d.adrelid) for 128)
            FROM pg_attrdef d
            WHERE d.adrelid = a.attrelid
                AND d.adnum = a.attnum
                AND a.atthasdef
        ) as default
        FROM pg_catalog.pg_attribute a
            RIGHT JOIN pg_catalog.pg_class c ON (c.oid = a.attrelid)
            RIGHT JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
            LEFT JOIN pg_catalog.pg_tables tb ON (tb.schemaname = %s AND tb.tablename = %s)
        WHERE c.relname = %s
            AND NOT a.attisdropped
            AND a.attnum > 0
        ORDER BY a.attnum;""")
    
    #
    # TODO: Get primary and unique keys
    #
    
    cursor = conn.cursor()
    cursor.execute(query, [schema, table, table])
    
    # Get column map
    columns = [i.name for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows


def indexes(conn, table, schema = 'public'):
    """ Get indexes on table, using connection """
    
    #
    # TODO: Get column order correct
    #
    
    query = """
        SELECT
            ix.indexrelid,
            ix.indrelid,
            ns.nspname AS schema,
            t.relname AS table,
            i.relname AS name,
            ix.indisprimary AS is_primary,
            ix.indisunique AS is_unique,
            a.attname AS column,
            r.rolname AS owner
        FROM
            pg_catalog.pg_class t,
            pg_catalog.pg_class i,
            pg_catalog.pg_index ix,
            pg_catalog.pg_attribute a,
            pg_catalog.pg_namespace ns,
            pg_catalog.pg_roles r
        WHERE
            t.oid = ix.indrelid
            AND r.oid = i.relowner
            AND i.oid = ix.indexrelid
            AND a.attrelid = t.oid
            AND a.attnum = ANY(ix.indkey)
            AND t.relkind = 'r'
            AND ns.oid = t.relnamespace
            AND ns.nspname = %s
            AND t.relname = %s
        ORDER BY t.relname, i.relname"""
    
    cursor = conn.cursor()
    cursor.execute(query, [schema, table])
    
    # Get column map
    columns = [i.name for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows

def schemas(conn):
    """
    Get list of schemas on current connection. Note: if you connected
    as a user who isn't a super user, the owner of the schema may be
    listed incorrectly. I'm not sure if this is a PostgreSQL bug, or
    feature, but I've reproduced the same result on multiple systems.
    """

    query = """ SELECT n.nspname AS name, r.rolname AS owner
                FROM pg_catalog.pg_namespace n, pg_catalog.pg_roles r
                WHERE r.oid = n.nspowner """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    # Get column map
    columns = [i.name for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows
