
"""
*
* Proposed extension, for dealing with relations.
*
"""

import sys
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

def sequences(conn, schema='public'):
    """ Get list of sequences """
    
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
            AND s.relkind = 'S'
            AND ns.nspname = %s"""
    
    cursor = conn.cursor()
    cursor.execute(query, [schema])
    
    # Get column map
    columns = [i.name for i in cursor.description]
    
    # Build result to return
    rows = []
    for i in cursor.fetchall():
        rows.append(dict(zip(columns, i)))
    
    cursor.close()
    return rows

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
    
    # Used when a bad argument is passed to factory()
    bad_argument_fmt = "%s() got an unexpected keyword argument '%s'"

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
    
    def __setitem__(self, key, value):
        self.__dict__[key] = value
    
    def __unicode__(self):
        # As unicode
        return u'%s' % (self.name)
    
    def __str__(self):
        # Alias for __unicode__
        return self.__unicode__()

class Index(Relation):
    """ Index object """

    factory_params = [
            'table',
            'schema',
            'name',
            'indexrelid',
            'indrelid',
            'is_primary',
            'is_unique',
            'owner',
            'columns']

    #TypeError: foo() takes no arguments (1 given)

    @staticmethod
    def factory(conn, **kwargs):
        ind = Index()
        ind.conn = conn
        
        for k, v in kwargs.items():
            if k not in Index.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            ind[k] = v
        return ind

class Column(Relation):
    """ Column object """
    
    factory_params = [
            'table',
            'schema',
            'name',
            'not_null',
            'has_default',
            'scale',
            'default',
            'externaltype',
            'internaltype',
            'type_mod',
            'length']
    
    @staticmethod
    def factory(conn, **kwargs):
        col = Column()
        col.conn = conn
        
        for k, v in kwargs.items():
            if k not in Column.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            col[k] = v
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
    
    def load_indexes(self):
        self.indexes = ObjectList()
        
        # Data must be grouped properly
        last = None
        data = []
        for i in indexes(self.conn, self.name, self.schema):
            if last == i['name']:
                data[len(data) - 1]['columns'].append(i['column'])
            else:
                i['columns'] = [i['column']]
                del i['column']
                data.append(i)
            last = i['name']
        
        # Append objects
        for i in data:
            ind = Index.factory(self.conn, **i)
            self.indexes.setattr(i['name'], ind)
        return self.indexes
    
    def load_columns(self):
        self.columns = ObjectList()
        for i in columns(self.conn, self.name, self.schema):
            col = Column.factory(self.conn, **i)
            self.columns.setattr(i['name'], col)
        return self.columns
    
    @staticmethod
    def factory(conn, schema, name, owner):
        tbl = Table()
        tbl.conn = conn
        tbl.schema = schema
        tbl.name = name
        tbl.owner = owner
        return tbl

class Sequence(Relation):
    """ Sequence object """

    factory_params = [
        'name',
        'schema',
        'owner']

    @staticmethod
    def factory(conn, **kwargs):
        seq = Sequence()
        seq.conn = conn
        
        for k, v in kwargs.items():
            if k not in Sequence.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            seq[k] = v
        return seq

class Database(Relation):
    """ Database object """
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'tables': 'load_tables',
            'sequences': 'load_sequences'}

    def load_tables(self):
        self.tables = ObjectList()
        tbl_list = tables(self.conn)
        for (schema, name, owner) in tbl_list:
            tbl = Table.factory(self.conn, schema, name, owner)
            self.tables.setattr(name, tbl)
        
        return self.tables

    def load_sequences(self):
        self.sequences = ObjectList()
        seqs = sequences(self.conn)
        
        for i in seqs:
            s = Sequence.factory(self.conn, **i)
            self.sequences.setattr(i['name'], s)
        return self.sequences

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
class UniqueConstraint(object): pass
class CheckConstraint(object): pass
class ForeignKeyConstraint(object): pass
class View(object): pass
