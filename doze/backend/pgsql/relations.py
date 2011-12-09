
"""
*
* Proposed extension, for dealing with relations.
*
"""

import doze.backend.pgsql as pgsql

# Relation Types, as per relkind on pg_catalog.pg_class
PG_CLASS_TABLE = 'r'
PG_CLASS_VIEW = 'v'
PG_CLASS_INDEX = 'i'
PG_CLASS_SEQUENCE = 'S'
PG_CLASS_SPECIAL = 's'

def databases(conn = None):
    """ Get list of databases, using conn as the connection. """

    cursor = conn.cursor()
    cursor.execute('SELECT datname FROM pg_catalog.pg_database')
    rows = [str(i[0]) for i in cursor.fetchall()]
    cursor.close()
    
    return rows

def tables(conn = None, schema = 'public'):
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

"""
SELECT
    n.nspname as schema,
    c2.relname as table,
    c.relname as name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'i' THEN 'index'
        WHEN 'S' THEN 'sequence'
        WHEN 's' THEN 'special'
    END as type,
    i.indisunique AS is_unique,
    i.indisprimary AS is_primary,
    u.usename AS owner
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index i ON (i.indexrelid = c.oid)
        JOIN pg_catalog.pg_class c2 ON (i.indrelid = c2.oid)
        LEFT JOIN pg_catalog.pg_user u ON (u.usesysid = c.relowner)
        LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.relkind IN ('i','')
        AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid);

"""


"""

    pg_index
        indexrelid
        indrelid
    pg_attribute
        attrelid
    
    table
        column --\____
        index  --/

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
                    WHEN 'numeric' THEN (a.atttypmod % 65536) - 4
                    WHEN 'decimal' THEN (a.atttypmod % 65536) - 4
                    WHEN 'money'   THEN (a.atttypmod % 65536) - 4
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
            JOIN pg_catalog.pg_tables tb ON (tb.schemaname = 'public' AND tb.tablename = 'gallery')
        WHERE c.relname = 'gallery'
            AND NOT a.attisdropped
            AND a.attnum > 0
        ORDER BY a.attnum;

76992 |      77491


TODO: Autoload columns AND indexes, at the same time,
    when columns are accessed, as they depend on each other.

SELECT
    ix.indexrelid,
    ix.indrelid,
    t.relname AS table,
    i.relname AS index,
    ix.indisprimary AS is_primary,
    ix.indisunique AS is_unique,
    a.attname AS column
    FROM
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a
    WHERE
        t.oid = ix.indrelid
        AND i.oid = ix.indexrelid
        AND a.attrelid = t.oid
        AND a.attnum = ANY(ix.indkey)
        AND t.relkind = 'r'
        AND t.relname = 'clubhouse_members'
    ORDER BY t.relname, i.relname;

"""


def columns(conn, table, schema = 'public'):
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
            JOIN pg_catalog.pg_tables tb ON (tb.schemaname = %s AND tb.tablename = %s)
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
    pass


class ObjectList(object):
    """
    ObjectList class. It provides a friendly interface for accessing
    attributes, so for instance, you can access data as:
    database.tables.my_table.my_column.type
    """

    def __init__(self):
        self.__dict__['objects'] = []
        self.__dict__['keys'] = []
    
    def __getattr__(self, name):
        try:
            idx = self.__dict__['keys'].index(name)
            return self.__dict__['objects'][idx]
        except:
            raise AttributeError('"%s" has no attribute named "%s"'
                % (self.__class__.__name__, name))
    
    def __setattr__(self, name, value):
        if name in self.__dict__['keys']:
            idx = self.__dict__['keys'].index(name)
            self.__dict__['objects'][idx] = value
        else:
            self.__dict__['keys'].append(name)
            self.__dict__['objects'].append(value)
    
    def __iter__(self):
        for i in range(0, len(self.__dict__['keys'])):
            yield (
                self.__dict__['keys'][i],
                self.__dict__['objects'][i])
    
    def setattr(self, name, value):
        self.__setattr__(name, value)

class Relation(object):
    """ Base class for all Relations. """

    def __init__(self):
        # You can define certain instance attributes that will be
        # auto-loaded via the __attributes dictionary. The key is
        # the name of the attribute to auto-load, while the value
        # should be the name of a method to call, with the return
        # value returned to the caller.
        self.__dict__['__attributes'] = {}
    
    def __getattr__(self, name):
        if name in self.__dict__['__attributes']:
            func = getattr(self, self.__dict__['__attributes'][name])
            return func()
    
        raise AttributeError('"%s" has no attribute named "%s"'
            % (self.__class__.__name__, name))
    
    def __unicode__(self):
        # As unicode
        return u'%s' % (self.name)
    
    def __str__(self):
        # Alias for __unicode__
        return self.__unicode__()

class Column(Relation):
    """ Column object """
    
    @staticmethod
    def factory(
            conn,
            table,
            schema,
            name,
            not_null = None,
            has_default = None,
            scale = None,
            default = None,
            externaltype = None,
            internaltype = None,
            type_mod = None,
            length = None):
        
        #
        # TODO: This could be much cleaner, if using **kwargs along
        # with a list of valid parameters
        #
        col = Column()
        col.conn = conn
        col.table = table
        col.schema = schema
        col.name = name
        col.not_null = not_null
        col.has_default = has_default
        col.scale = scale
        col.default = default
        col.type = externaltype
        col.internaltype = internaltype
        col.type_mod = type_mod
        col.length = length
        
        return col

class Table(Relation):
    """ Table object """
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'indexes': 'load_indexes',
            'columns': 'load_columns',
            'constraints': 'load_constraints',
            'triggers': 'load_triggers',
            'rules': 'load_rules'}
    
    def load_columns(self):
        self.columns = ObjectList()
        for i in columns(self.conn, self.name, self.schema):
            col = Column.factory(self.conn, **i)
            self.columns.setattr(i['name'], col)
        return self.columns
    
    @staticmethod
    def get(conn, schema, name, owner):
        tbl = Table()
        tbl.conn = conn
        tbl.schema = schema
        tbl.name = name
        tbl.owner = owner
        return tbl

class Database(Relation):
    """ Database object """
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'tables': 'load_tables'}

    def load_tables(self):
        self.tables = ObjectList()
        tbl_list = tables(self.conn)
        for (schema, name, owner) in tbl_list:
            tbl = Table.get(self.conn, schema, name, owner)
            self.tables.setattr(name, tbl)
        
        return self.tables

    @staticmethod
    def get(conn):
        dbdef = Database()
        dbdef.conn = conn
        
        # Get database
        cursor = conn.cursor()
        cursor.execute('SELECT CURRENT_DATABASE()')
        database = str(cursor.fetchone()[0])
        cursor.close()
        
        dbdef.name = database
        return dbdef

class User(object): pass
class Index(object): pass
class Sequence(object): pass
class UniqueConstraint(object): pass
class CheckConstraint(object): pass
class ForeignKeyConstraint(object): pass
class View(object): pass
