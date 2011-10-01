
from doze import *
from doze.backend.generic.base import *
from doze.backend.generic.where import *

class Join(BaseClause):
    """
    **
    * Join clause class. You'll likely want to use this through the Builder class,
    * otherwise you'll have to maintain your own TableContext object.
    *
    * Usage:
    *
    * join = Join(<table>, [kind], [where], [context])
    *   <table> can be a list of [table, alias] or string.
    *   [kind] is the kind of join (JOIN_INNER, JOIN_LEFT, JOIN_RIGHT)
    *   [where] is an optional Where object to use as a qualifier
    *   [context] is an optional reference to a TableContext object
    *
    * Examples:
    *
    * # Using Join directly
    * context = TableContext(['maintable', 'a'])
    * join = Join(['table2', 'b']).where(Where('refid').equals('id', kind=FIELD))
    * join.setTableContext(context)
    * sql, escape = join.sql()
    *
    * # Using Join through Builder
    * builder.select('*').from_(['table1', 'a'])\
    *   .join(Join(['table2', 'b']).where(Where('refid').equals('id', kind=FIELD)))
    *
    * # Specifying a more specific alias context
    * builder.select('*').from(['table1', 'a'])\
    *   .join(Join(['table2', 'b']).where(...))\
    *   .join(Join(['table3', 'c']).where(
    *       Where('refid', alias='b').equals('id', kind=FIELD)))
    **
    """
    def __init__(self, table, kind = JOIN_INNER, where = None, context = None):
        super(Join, self).__init__()
    
        self.where_ = None
        self.tableContext = None
        self.table = self.expandTable(table)
        self.kind = kind
        
        if isinstance(where, Where):
            self.where_ = where
        
        if isinstance(context, TableContext):
            self.setTableContext(context)
    
    def where(self, where):
        self.where_ = where
        return self
    
    def preProcessSql(self):
        """ Method which gets called before sql(), to update tableContext, etc. """
        ctx = self.tableContext
        
        if (type(self.table) == list and len(self.table) > 1
        and not ctx == None and not self.table[0] in ctx):
            ctx[self.table[0]] = self.table[1]
    
    def sql(self):
        """ Build SQL and return string """
        ctx = self.tableContext
        self.where_.setTableContext(ctx)
        
        if self.kind == JOIN_INNER:
            data = ['INNER JOIN']
        elif self.kind == JOIN_LEFT:
            data = ['LEFT JOIN']
        elif self.kind == JOIN_RIGHT:
            data = ['RIGHT JOIN']
        else:
            raise ValueError('Invalid value for kind')

        if not isinstance(self.where_, Where):
            raise TypeError('Where must be a valid Where class object')

        # Handle aliases table vs. non aliased
        if type(self.table) == list and len(self.table) > 1\
        and not ctx == None:
            data.append(' '.join(self.table))
        else:
            if type(self.table) == str:
                if not ctx == None and self.table in ctx:
                    table = ctx[self.table]
                    data.append(self.table + ' '  + table[0])
                else:
                    data.append(self.table)
            else:
                raise ValueError('table must be either a list with the length of 2, or a string')
        
        data.append('ON')
        where, escape = self.where_.sql()
        data.append('(' + where + ')')
        
        return (' '.join(data), escape)
