
"""
*
* Proposed extension, for dealing with relations.
*
"""

import sys
import doze.backend.pgsql as pgsql
from .base import *
from .information import *

"""

TODO: Cleanup class hierarchy

"""

# Relation Types, as per relkind on pg_catalog.pg_class
PG_CLASS_TABLE = 'r'
PG_CLASS_VIEW = 'v'
PG_CLASS_INDEX = 'i'
PG_CLASS_SEQUENCE = 'S'
PG_CLASS_SPECIAL = 's'

# Lazy loaders
class LazyloadTables(object):
    """ Lazyload Tables Class """
    
    def load_tables(self):
        self.tables = ObjectList()
        tbl_list = tables(self.conn, self.search_path)
        for (schema, name, owner) in tbl_list:
            tbl = Table.factory(self.conn, schema, name, owner)
            self.tables.setattr(name, tbl)
        
        return self.tables

class LazyloadSequences(object):
    """ Lazyload Sequences Class """

    def load_sequences(self):
        self.sequences = ObjectList()
        for i in sequences(self.conn, self.search_path):
            s = Sequence.factory(self.conn, **i)
            self.sequences.setattr(i['name'], s)
        
        return self.sequences

class LazyloadViews(object):
    """ Lazyload Views Class """
    
    def load_views(self):
        """ Auto load views """
    
        self.views = ObjectList()
        for i in views(self.conn, self.search_path):
            v = View.factory(self.conn, **i)
            self.views.setattr(i['name'], v)
        
        return self.views

class LazyloadColumns(object):
    """ Lazyload Views Columns """
    
    def load_columns(self):
        self.columns = ObjectList()
        for i in columns(self.conn, self.name, self.schema):
            col = Column.factory(self.conn, **i)
            self.columns.setattr(i['name'], col)
        return self.columns

# Relations
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

    @staticmethod
    def factory(conn, **kwargs):
        """ Factory method """
    
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
        """ Factory method """
    
        col = Column()
        col.conn = conn
        
        for k, v in kwargs.items():
            if k not in Column.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            col[k] = v
        
        if 'externaltype' in kwargs:
            col['type'] = kwargs['externaltype']
        
        return col

class Table(Relation, LazyloadColumns):
    """ Table object """
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'indexes': 'load_indexes',
            'columns': 'load_columns',
            'constraints': 'load_constraints',
            'triggers': 'load_triggers',
            'rules': 'load_rules'}
    
    def load_indexes(self):
        """ Auto load indexes """
    
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
    
    @staticmethod
    def factory(conn, schema, name, owner):
        """ Factory method """
    
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
        """ Factory method """
    
        seq = Sequence()
        seq.conn = conn
        for k, v in kwargs.items():
            if k not in Sequence.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            seq[k] = v
        
        return seq

class View(Relation, LazyloadColumns):
    """ View object """

    factory_params = [
            'name',
            'schema',
            'owner',
            'definition']
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'columns': 'load_columns'}
    
    @staticmethod
    def factory(conn, **kwargs):
        """ Factory method """
    
        view = View()
        view.conn = conn
        
        for k, v in kwargs.items():
            if k not in View.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            view[k] = v
        return view

class Schema(
        Relation,
        LazyloadTables,
        LazyloadSequences,
        LazyloadViews):
    
    """ Schema object """

    factory_params = [
            'name',
            'owner']
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'tables': 'load_tables',
            'sequences': 'load_sequences',
            'views': 'load_views'}
    
    @staticmethod
    def factory(conn, **kwargs):
        """ Factory method """
    
        schem = Schema()
        schem.conn = conn
        
        for k, v in kwargs.items():
            if k not in Schema.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            schem[k] = v
        
        # Set correct search path
        schem.search_path = kwargs['name']
        return schem

class Database(
        Relation,
        LazyloadTables,
        LazyloadSequences,
        LazyloadViews):
        
    """ Database object """
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'tables': 'load_tables',
            'sequences': 'load_sequences',
            'schemas': 'load_schemas',
            'views': 'load_views'}
        self.search_path = ['public']

    def set_search_path(self, search_path):
        """ Set default search path for session """
    
        if type(search_path) == tuple:
            self.search_path = list(search_path)
        elif type(search_path) == list:
            self.search_path = search_path
        elif type(search_path) == str:
            self.search_path = [search_path]
        else:
            raise TypeError('search_path must be one of: list, tuple, str')
        
        # Sadly, we have to clear out most of the autoloaded data :(
        for k in self.__dict__['__attributes'].keys():
            if k == 'schemas':
                continue
            
            if k in self.__dict__:
                del self.__dict__[k]

    def load_schemas(self):
        """ Auto load schemas """
        
        self.schemas = ObjectList()
        for i in schemas(self.conn):
            s = Schema.factory(self.conn, **i)
            self.schemas.setattr(i['name'], s)
        
        return self.schemas

    @staticmethod
    def get(conn, search_path=['public']):
        dbdef = Database()
        dbdef.search_path = search_path
        dbdef.conn = conn
        dbdef.name = current_database(conn)
        
        return dbdef

class User(object): pass
class UniqueConstraint(object): pass
class CheckConstraint(object): pass
class ForeignKeyConstraint(object): pass
