
"""
*
* Proposed extension, for dealing with relations.
*
"""

import sys
import doze.backend.pgsql as pgsql
from doze.backend.generic.relations.base import *
from information import *

"""

TODO: Cleanup class hierarchy
TODO: Split into multiple files?
TODO: Standardize names of attributes on Relation objects

"""

# Relation Types, as per relkind on pg_catalog.pg_class
PG_CLASS_TABLE = 'r'
PG_CLASS_VIEW = 'v'
PG_CLASS_INDEX = 'i'
PG_CLASS_SEQUENCE = 'S'
PG_CLASS_SPECIAL = 's'

# Trigger Masks, as per the tgtype column on pg_catalog.pg_trigger
PG_TRIGGER_FOR_EACH_MASK    = int('00000001', 2)
PG_TRIGGER_BEFORE_MASK      = int('00000010', 2)
PG_TRIGGER_INS_MASK         = int('00000100', 2)
PG_TRIGGER_DEL_MASK         = int('00001000', 2)
PG_TRIGGER_UPD_MASK         = int('00010000', 2)

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

class LazyLoadConstraints(object):
    """ Lazy Load Constraints """
    
    def load_constraints(self):
        self.constraints = ObjectList()
        
        # Check Constraints
        for i in check_constraints(self.conn, self.name, self.schema):
            obj = CheckConstraint.factory(self.conn, **i)
            self.constraints.setattr(i['name'], obj)
        
        # Foreign Key Constraints
        for i in foreign_key_constraints(self.conn, self.name, self.schema):
            obj = ForeignKeyConstraint.factory(self.conn, **i)
            self.constraints.setattr(i['name'], obj)
            
        return self.constraints

class LazyLoadTriggers(object):
    """ Lazy Load Triggers  """
    
    def load_triggers(self):
        self.triggers = ObjectList()
        
        for i in non_referential_triggers(self.conn, self.name, self.schema):
            obj = Trigger.factory(self.conn, **i)
            self.triggers.setattr(i['name'], obj)
        return self.triggers

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
    
    def __str__(self):
        """ To String """
    
        bc = pgsql.BaseClause()
    
        path = '.'.join([
            bc.quoteField(self.schema),
            bc.quoteField(self.table)])
        
        columns = []
        for i in self.columns:
            columns.append(bc.quoteField(i))
        
        columns = '(' + ', '.join(columns) + ')'
    
        if self.is_primary:
            pre = ['ALTER TABLE', path, 'ADD PRIMARY KEY', columns]
            return ' '.join(pre)
    
        ci = 'CREATE INDEX'
        if self.is_unique:
            ci = 'CREATE UNIQUE INDEX'
        
        pre = [ci, self.name, 'ON', path]
        pre.append(columns)
        
        return ' '.join(pre)


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
    
    def __str__(self):
        """ To String """
    
        bc = pgsql.BaseClause()
        name = bc.quoteField(self.name)
    
        if self.externaltype == 'char':
            pre = [name, '%s(%s)' % (self.internaltype, str(self.length))]
        else:
            pre = [name, self.internaltype]
        if self.has_default:
            pre.append('DEFAULT')
            pre.append(self.default)
        
        if self.not_null:
            pre.append('NOT NULL')
        
        return ' '.join(pre)

class CheckConstraint(Relation):
    """ Check Constraint """

    factory_params = [
            'schema',
            'table',
            'name',
            'definition']

    @staticmethod
    def factory(conn, **kwargs):
        "Factory Method"
        
        constr = CheckConstraint()
        constr.conn = conn
        constr['type'] = 'c'
        
        for k, v in kwargs.items():
            if k not in CheckConstraint.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            constr[k] = v
        
        return constr
    
    def __str__(self):
        """ To String """
    
        bc = pgsql.BaseClause()
    
        path = '.'.join([
            bc.quoteField(self.schema),
            bc.quoteField(self.table)])
        
        pre = [
            'ALTER TABLE', path,
            'ADD CONSTRAINT', self.name,
            'CHECK', self.definition]
        
        return ' '.join(pre)

class ForeignKeyConstraint(Relation):
    factory_params = [
            'src_schema',
            'src_table',
            'dst_schema',
            'dst_table',
            'name',
            'src_columns',
            'dst_columns',
            'match_type',
            'deferrable',
            'on_update_action',
            'on_delete_action']
    
    @staticmethod
    def factory(conn, **kwargs):
        """ Factory Method """
    
        fkey = ForeignKeyConstraint()
        fkey.conn = conn
        
        fkey['type'] = 'f'
        
        for k, v in kwargs.items():
            if k not in ForeignKeyConstraint.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            fkey[k] = v
        return fkey
    
    def __str__(self):
        """ To String """
        
        bc = pgsql.BaseClause()
        src_path = '.'.join([
            bc.quoteField(self.src_schema),
            bc.quoteField(self.src_table)])
            
        dst_path = '.'.join([
            bc.quoteField(self.dst_schema),
            bc.quoteField(self.dst_table)])
        
        src_columns = []
        dst_columns = []
        
        for i in self.src_columns:
            src_columns.append(bc.quoteField(i))
            
        for i in self.dst_columns:
            dst_columns.append(bc.quoteField(i))
        
        src_columns = ', '.join(src_columns)
        dst_columns = ', '.join(dst_columns)
        
        pre = [
            'ALTER TABLE', src_path,
            'ADD CONSTRAINT', self.name,
            'FOREIGN KEY', '(', src_columns, ')',
            'REFERENCES', dst_path, '(', dst_columns, ')']
        
        if self.match_type == 'f':
            pre.append('MATCH FULL')
        elif self.match_type == 'p':
            pre.append('MATCH PARTIAL')
        elif self.match_type == 'u':
            pre.append('MATCH SIMPLE')
        
        if self.on_update_action == 'r':
            pre.append('ON UPDATE RESTRICT')
        elif self.on_update_action == 'c':
            pre.append('ON UPDATE CASCADE')
        elif self.on_update_action == 'a':
            pre.append('ON UPDATE NO ACTION')
        elif self.on_update_action == 'n':
            pre.append('ON UPDATE SET NULL')
        elif self.on_update_action == 'd':
            pre.append('ON UPDATE SET DEFAULT')
            
        if self.on_delete_action == 'r':
            pre.append('ON DELETE RESTRICT')
        elif self.on_delete_action == 'c':
            pre.append('ON DELETE CASCADE')
        elif self.on_delete_action == 'a':
            pre.append('ON DELETE NO ACTION')
        elif self.on_delete_action == 'n':
            pre.append('ON DELETE SET NULL')
        elif self.on_delete_action == 'd':
            pre.append('ON DELETE SET DEFAULT')
        
        return ' '.join(pre)

class Trigger(Relation):
    """ Trigger Class """
    
    factory_params = [
        'schema',
        'table',
        'name',
        'type',
        'for_each_row',
        'execute_before',
        'on_insert',
        'on_delete',
        'on_update',
        'function']

    @staticmethod
    def factory(conn, **kwargs):
        """ Factory Method """
    
        trigger = Trigger()
        trigger.conn = conn
        
        for k, v in kwargs.items():
            if k not in Trigger.factory_params:
                raise TypeError(Relation.bad_argument_fmt
                    % (sys._getframe().f_code.co_name, k))
            trigger[k] = v
        return trigger
    
    def __str__(self):
        """ To String """
        
        bc = pgsql.BaseClause()
        path = '.'.join([
            bc.quoteField(self.schema),
            bc.quoteField(self.table)])
        name = bc.quoteField(self.name)
        function = bc.quoteField(self.function)
        
        pre = ['CREATE TRIGGER', name]
        if (self.execute_before):
            pre.append('AFTER')
        else:
            pre.append('BEFORE')
        
        operations = []
        if self.on_insert:
            operations.append('INSERT')
        
        if self.on_update:
            operations.append('UPDATE')
            
        if self.on_delete:
            operations.append('DELETE')
        
        pre.extend([' OR '.join(operations), 'ON', path, 'EXECUTE PROCEDURE'])
        pre.extend([function, '()'])
        
        return ' '.join(pre)

class Table(
        Relation,
        LazyloadColumns,
        LazyLoadConstraints,
        LazyLoadTriggers):
    
    """ Table object """
    
    def __init__(self):
        self.__dict__['__attributes'] = {
            'indexes': 'load_indexes',
            'columns': 'load_columns',
            'constraints': 'load_constraints',
            'triggers': 'load_triggers',
            'rules': 'load_rules'}
    
    def load_indexes(self):
        """ Lazy load indexes """
    
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
    
    def create_table(self):
        # TODO: Rules
    
        bc = pgsql.BaseClause()
        
        path = '.'.join([
            bc.quoteField(self.schema),
            bc.quoteField(self.name)])
            
        create_table = ['CREATE TABLE', path, '(\n']
        columns = []
        indexes = []
        
        for name, obj in self.columns:
            columns.append(str(obj))
        
        create_table.extend([',\n'.join(columns), '\n)'])
        yield ' '.join(create_table)
        
        for name, obj in self.indexes:
            yield str(obj)
        
        for name, obj in self.constraints:
            yield str(obj)
        
        for name, obj in self.triggers:
            yield str(obj)
    
    def __str__(self):
        lst = list(self.create_table())
        return ';\n'.join(lst) + ';'


class Sequence(Relation):
    """ Sequence object """

    factory_params = [
        'name',
        'schema',
        'owner',
        'last_value',
        'increment_by',
        'max_value',
        'min_value']

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
    
    def __str__(self):
        """ To String """
        
        bc = pgsql.BaseClause()
        
        path = '.'.join([
            bc.quoteField(self.schema),
            bc.quoteField(self.name)])
            
        increment_by = bc.quoteValue(self.increment_by)
        min_value = bc.quoteValue(self.min_value)
        max_value = bc.quoteValue(self.max_value)
        last_value = bc.quoteValue(self.last_value)
        
        pre = [
            'CREATE SEQUENCE', path,
            'INCREMENT BY', increment_by,
            'MINVALUE', min_value,
            'MAXVALUE', max_value,
            'START WITH', last_value]
        
        return ' '.join(pre)

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
    
    def __str__(self):
        """ To String """
        
        bc = pgsql.BaseClause()
        path = '.'.join([
            bc.quoteField(self.schema),
            bc.quoteField(self.name)])
        
        pre = ['CREATE VIEW', path,
               'AS', self.definition]
        
        return ' '.join(pre)

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
    
    def __str__(self):
        bc = pgsql.BaseClause()
        name = bc.quoteField(self.name)
        return ' '.join(['CREATE SCHEMA', name])

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
        """ Lazy load schemas """
        
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

class UniqueConstraint(Relation): pass
class Rule(Relation): pass
class StoredProcedure(Relation): pass
class DataType(Relation): pass
class User(Relation): pass
