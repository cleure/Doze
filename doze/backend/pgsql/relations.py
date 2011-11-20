
"""
*
* Proposed extension, for dealing with relations.
*
"""

"""

API Style

import doze.backend.pgsql.relations as relations

database_list = relations.databases(db)
for i in database_list:
    database = relations.Database.get(db, i)

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

class Table(Relation):
    """ Table object """
    
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

class Column(object): pass
class User(object): pass
class Index(object): pass
class Sequence(object): pass
class UniqueConstraint(object): pass
class CheckConstraint(object): pass
class ForeignKeyConstraint(object): pass
class View(object): pass
