
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
    columns = [i[0] for i in cursor.description]
    
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
    
    bc = pgsql.BaseClause()
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
    
    seq_map = [
        'last_value',
        'increment_by',
        'max_value',
        'min_value']
    
    seq_info = "SELECT %s FROM %s"
    
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
    columns = [i[0] for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        row = dict(zip(columns, i))
        
        # Fetch sequence details
        path = '.'.join([
            bc.quoteField(row['schema']),
            bc.quoteField(row['name'])])
        
        cursor.execute(seq_info % (','.join(seq_map), path))
        row.update(dict(zip(seq_map, cursor.fetchone())))
        
        rows.append(row)
    
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
    columns = [i[0] for i in cursor.description]
    
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
    columns = [i[0] for i in cursor.description]
    
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
    columns = [i[0] for i in cursor.description]
    
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
    columns = [i[0] for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows

def check_constraints(conn, table, schema='public'):
    """ Get list of Check Constraints on table """
    
    query = """
        SELECT
            n.nspname AS schema,
            t.relname AS table,
            c.conname AS name,
            c.consrc AS definition
            FROM
                pg_catalog.pg_constraint c,
                pg_catalog.pg_class t,
                pg_catalog.pg_namespace n
            WHERE
                t.oid = c.conrelid
                AND t.relnamespace = n.oid
                AND c.contype = 'c'
                AND t.relname = %s
                AND n.nspname = %s"""
    
    cursor = conn.cursor()
    cursor.execute(query, [table, schema])
    
    # Column map
    columns = [i[0] for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    
    return rows

def foreign_key_constraints(conn, table, schema='public'):
    """ Get list of Foreign Key Constraints on table """
    
    cursor2 = conn.cursor()
    colname_query = """
        SELECT
            a.attname AS name
            FROM pg_catalog.pg_attribute a
                RIGHT JOIN pg_catalog.pg_class c ON (c.oid = a.attrelid)
            WHERE c.oid = %s
                AND NOT a.attisdropped
                AND a.attnum > 0
                AND a.attnum = %s"""
    
    # Nested helper function
    def get_columns_by_attrnums(tableoid, attrnums=[]):
        colnames = []
        for i in attrnums:
            cursor.execute(colname_query, [tableoid, i])
            res = cursor.fetchone()
            colnames.append(res[0])
        return colnames
    
    query = """
        SELECT
            n1.nspname AS src_schema,
            n2.nspname AS dst_schema,
            c.conname AS name,
            t1.relname AS src_table,
            t2.relname as dst_table,
            c.condeferrable AS deferrable,
            c.confmatchtype AS match_type,
            c.conkey,
            c.confkey,
            c.conrelid AS src_table_oid,
            c.confrelid AS dst_table_oid,
            c.confupdtype AS on_update_action,
            c.confdeltype AS on_delete_action
            FROM
                pg_catalog.pg_constraint c,
                pg_catalog.pg_class t1,
                pg_catalog.pg_class t2,
                pg_catalog.pg_namespace n1,
                pg_catalog.pg_namespace n2
            WHERE
                t1.oid = c.conrelid
                AND t1.relnamespace = n1.oid
                AND c.confrelid = t2.oid
                AND t2.relnamespace = n2.oid
                AND c.contype = 'f'
                AND n1.nspname = %s
                AND t1.relname = %s"""

    cursor = conn.cursor()
    cursor.execute(query, [schema, table])
    
    # Column map
    columns = [i[0] for i in cursor.description]
    
    # Build result to return
    rows = []
    
    for i in cursor.fetchall():
        row = dict(zip(columns, i))
        src_columns = get_columns_by_attrnums(
            row['src_table_oid'],
            row['conkey'])
        
        dst_columns = get_columns_by_attrnums(
            row['dst_table_oid'],
            row['confkey'])
        
        row['src_columns'] = src_columns
        row['dst_columns'] = dst_columns
        del row['dst_table_oid']
        del row['src_table_oid']
        del row['confkey']
        del row['conkey']
        
        rows.append(row)
    
    cursor.close()
    cursor2.close()
    return rows

def non_referential_triggers(conn, table=None, schema='public'):
    """
    Returns a list of Triggers which are not used for
    referential integrity (non Foreign Keys, etc).
    """

    query = """
        SELECT
            n.nspname AS schema,
            t.relname AS table,
            tg.tgname AS name,
            (tg.tgtype::int)::bit(8)    AS type,
            (tg.tgtype & 1)::bool       AS for_each_row,
            (tg.tgtype & 2)::bool       AS execute_before,
            (tg.tgtype & 4)::bool       AS on_insert,
            (tg.tgtype & 8)::bool       AS on_delete,
            (tg.tgtype & 16)::bool      AS on_update,
            p.proname AS function
            FROM
                pg_catalog.pg_trigger tg,
                pg_catalog.pg_class t,
                pg_catalog.pg_namespace n,
                pg_catalog.pg_proc p
            WHERE
                tg.tgrelid = t.oid
                AND tg.tgfoid = p.oid
                AND t.relnamespace = n.oid
                AND tg.tgisconstraint = 'f'"""
    
    cursor = conn.cursor()
    if table is not None:
        query += "AND n.nspname = %s\n"
        query += "AND t.relname = %s"
        cursor.execute(query, [schema, table])
    else:
        cursor.execute(query)
    
    # Column map
    columns = [i[0] for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows
