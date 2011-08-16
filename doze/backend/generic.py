
# -*- coding: utf-8 -*-

from doze import *
import copy

class Where(BaseClause):
    """
    **
    * WHERE clause class.
    *
    * Examples:
    *
    * (where, escape) = Where('foo').equals('bar').sql()
    * (where, escape) = Where('id').gt(100).sql()
    * (where, escape) = Where('id').isIn([1, 2, 3]).and_('type').equals('image').sql()
    *
    * Supported Operators:
    *    equals                  - Equals
    *    notEquals               - Not equals
    *    gt                      - Greater than (>)
    *    gte                     - Greater than or equals (>=)
    *    lt                      - Less than (<)
    *    lte                     - Less than or equals (<=)
    *    isIn                    - Is in [list]
    *    notIn                   - Is not in [list]
    *    isNull                  - Is null
    *    isNotNull               - Is not null
    *    isEmpty                 - Is empty
    *    isNotEmpty              - Is not empty
    *
    * Soon to be supported Operators:
    *    like / iLike                    - LIKE / ILIKE
    *    between(min, max)               - Between Min/Max
    *    operator(operator, field)       - Custom operator
    *    encapsulate                     - Encapsulate expression
    *
    * TODO:
    *   - Add support for sub-queries for isIn and notIn
    *   - Add support for BETWEEN
    *   - Add support for LIKE / ILIKE
    *   - Encapsulate. Ex: WHERE (id = 5 OR parent = 3) AND type = 'type'
    *   - Custom operators. Ex: Where('geom').operator('@@')...
    *
    **
    """
    
    # Conditional operators
    condOperators = [
        'and_',
        'or_']
    
    # Comparison operators
    compOperators = [
        'equals',
        'notEquals',
        'gt',
        'gte',
        'lt',
        'lte',
        'isIn',
        'notIn',
        'isNull',
        'isNotNull',
        'isEmpty',
        'isNotEmpty']
    
    # Simple comparison operators
    simpleCompOperators = {
        'equals': '=',
        'notEquals': '!=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<='}
    
    # Complex comparison operators
    complexCompOperators = [
        'isIn',
        'notIn',
        'isNull',
        'isNotNull',
        'isEmpty',
        'isNotEmpty']

    # Key map
    map = [
        'cond',
        'sourceName',
        'sourceType',
        'sourceAlias',
        'comp',
        'destName',
        'destType',
        'destAlias']

    def __init__(self, field, kind = TYPE_FIELD, alias = None):
        super(Where, self).__init__()
        self.current = [None, field, kind, alias]
        self.where = []
        self.tableContext = None
    
    def __getattr__(self, key):
        def setCurrent(field, kind = TYPE_FIELD, alias = None):
            self.current = [key, field, kind, alias]
            return self
    
        def setWhere(value = None, kind = TYPE_VALUE, alias = None):
            temp = self.current
            temp.extend([key, value, kind, alias])
            self.where.append(temp)
            self.current = None
            return self
    
        if key in self.condOperators:
            return setCurrent
        elif key in self.compOperators:
            return setWhere
        
        raise AttributeError(str(self.__class__) + " instance has no attribute '" + key + "'")
    
    def parseExpression(self, each):
        """ Parse supplied expression """
        data = []
        source = each['sourceName']
        dest = each['destName']
        escape = []
        
        if each['sourceType'] == TYPE_VALUE:
            # Source is a value
            source = '%s'
            escape.append(each['sourceName'])
        else:
            source = self.getAliasedField(each['sourceName'],\
                each['sourceAlias'], 'origin')
                
        if each['destType'] == TYPE_VALUE:
            # Dest is value
            dest = '%s'
            
            if each['destName'] == None:
                # NO-OP
                pass
            elif type(each['destName']) == list:
                escape.extend(each['destName'])
            else:
                escape.append(each['destName'])
        else:
            # Dest is field
            dest = self.getAliasedField(each['destName'],\
                each['destAlias'], 'join')
        
        return (source, dest, escape)

    def buildComparison(self, expression, each):
        """ Build comparison string """
        
        comparison = None
        simple = self.simpleCompOperators
        complex_ = self.complexCompOperators
        
        if each['comp'] in simple:
            comparison = ' '.join([expression[0], simple[each['comp']], expression[1]])
        elif each['comp'] in complex_:
            func = getattr(self, 'build_' + each['comp'])
            comparison = func(expression, each)
        else:
            raise DozeError('Unsupported Operator: ' + each['comp'])
        
        return comparison

    def build_isIn(self, expression, each):
        return expression[0] + ' IN (%s)' % ', '.join(['%s' for i in expression[2]])

    def build_notIn(self, expression, each):
        return expression[0] + ' NOT IN (%s)' % ', '.join(['%s' for i in expression[2]])

    def build_isNull(self, expression, each):
        return expression[0] + ' IS NULL'
    
    def build_isNotNull(self, expression, each):
        return expression[0] + ' IS NOT NULL'
    
    def build_isEmpty(self, expression, each):
        return expression[0] + " = ''"
    
    def build_isNotEmpty(self, expression, each):
        return expression[0] + " != ''"

    def sql(self):
        """ Build SQL and return string """
        
        # Return callers sql() method
        if not self.caller == None:
            return self.caller.sql()
        
        sql = []
        escape = []
        
        for each in self.where:
            # Dictionary-ize
            each = dict(zip(self.map, each))
            
            # Parse expression
            expr = self.parseExpression(each)
            
            # Extend escape list
            escape.extend(expr[2])
            
            # Build actual SQL string
            comp = self.buildComparison(expr, each)
            
            # AND / OR conditionals
            if each['cond'] == 'and_':
                sql.append('AND')
            elif each['cond'] == 'or_':
                sql.append('OR')
            
            sql.append(comp)
        
        return (' '.join(sql), escape)

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
    * join = Join(['table2', 'b']).where(Where('refid').equals('id', kind=TYPE_FIELD))
    * join.setTableContext(context)
    * sql, escape = join.sql()
    *
    * # Using Join through Builder
    * builder.select('*').from_(['table1', 'a'])\
    *   .join(Join(['table2', 'b']).where(Where('refid').equals('id', kind=TYPE_FIELD)))
    *
    * # Specifying a more specific alias context
    * builder.select('*').from(['table1', 'a'])\
    *   .join(Join(['table2', 'b']).where(...))\
    *   .join(Join(['table3', 'c']).where(
    *       Where('refid', alias='b').equals('id', kind=TYPE_FIELD)))
    **
    """
    def __init__(self, table, kind = JOIN_INNER, where = None, context = None):
        super(Join, self).__init__()
    
        self.where_ = None
        self.tableContext = None
        self.table = table
        self.kind = kind
        
        if isinstance(where, Where):
            self.where_ = where
        
        if isinstance(context, TableContext):
            self.setTableContext(context)
    
    def where(self, where):
        self.where_ = where
        return self
    
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

        # Auto add alias to table context
        if type(self.table) == list and len(self.table) > 1\
        and not ctx == None and not self.table[0] in ctx:
            ctx[self.table[0]] = self.table[1]
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

class QueryResult(object):
    """ API Draft """
    def __init__(self, cursor = None, destroy = True):
        self.setCursor(cursor, destroy)
    
    def __len__(self):
        if self.cursor == None:
            return 0
        return self.cursor.rowcount
    
    def __iter__(self):
        res = self.cursor.fetchone()
        if self.cursorDescrInit == False:
            self.initCursorDescr()
    
        while not res == None:
            yield dict(zip(self.labels, res))
            res = self.cursor.fetchone()
        
        if self.destroy == True:
            self.cursor.close()
    
    def next(self):
        res = self.cursor.fetchone()
        if self.cursorDescrInit == False:
            self.initCursorDescr()

        return dict(zip(self.labels, res))
    
    def initCursorDescr(self):
        for i in self.cursor.description:
            self.labels.append(i.name)
        
        self.cursorDescrInit = True
    
    def setCursor(self, cursor = None, destroy = True):
        self.cursor = cursor
        self.destroy = destroy
        self.labels = []
        self.cursorDescrInit = False
    
    def isReady(self):
        return True

class Builder(BaseClause):
    """ API Draft """
    def __init__(self, db = None):
        self.tableContext = TableContext()
        self.db = db
    
    def select(self, columns):
        self.tableContext = TableContext()
        self.kind = 'select'
        self.joins = []
        self.where_ = []
        self.having_ = None
        self.group_ = None
        self.order_ = None
        self.limit_ = None
        self.columns = columns
        
        return self
    
    def from_(self, source):
        self.source = source
        if self.kind == 'select' and type(source) == list:
            self.tableContext[source[0]] = source[1]
        
        return self
    
    def where(self, where):
        self.where_.append(where)
        return self
    
    def join(self, join):
        self.joins.append(join)
        return self
    
    def having(self, having):
        self.having_ = having
        return self
    
    def group(self, group):
        self.group_ = group
        return self
    
    def order(self, order):
        self.order_ = order
        return self
    
    def limit(self, limit):
        self.limit_ = limit
        return self
    
    def normalizeColumns(self, columns):
        if type(columns) == str:
            cols = [i.strip() for i in columns.split(',')]
            columns = cols
        
        if len(self.tableContext) > 0:
            origin = self.tableContext.origin()
            if len(origin) > 1:
                origin = origin[1]
            else:
                origin = origin[0]
            
            cols = []
            for i in columns:
                if self.fieldIsAliased(i):
                    cols.append(i)
                else:
                    cols.append('.'.join([origin, i]))
            
            columns = cols
        return columns
    
    def fromSelect(self):
        query = ['SELECT']
        escape = []
        
        columns = self.normalizeColumns(self.columns)
        query.extend([', '.join(columns), 'FROM'])
        
        if type(self.source) == list:
            query.append(' '.join(self.source))
        else:
            query.append(self.source)
        
        # JOIN
        if len(self.joins) > 0:
            for i in self.joins:
                i.setTableContext(self.tableContext)
                tmpquery, tmpescape = i.sql()
                query.append(tmpquery)
                escape.extend(tmpescape)
        
        # WHERE
        if len(self.where_) > 0:
            query.append('WHERE')
            tmplist = []
            
            for i in self.where_:
                i.setTableContext(self.tableContext)
                tmpquery, tmpescape = i.sql()
                tmplist.append(tmpquery)
                escape.extend(tmpescape)
            
            query.append(' AND '.join(tmplist))
        
        # GROUP BY
        if not self.group_ == None:
            query.append('GROUP BY')
            query.append(', '.join(self.normalizeColumns(self.group_)))
        
        # HAVING
        if not self.having_ == None:
            query.append('HAVING')
            tmpquery, tmpescape = self.having_.sql()
            query.append(tmpquery)
            escape.extend(tmpescape)
        
        # ORDER
        if not self.order_ == None:
            query.append('ORDER BY')
            query.append(', '.join(self.normalizeColumns(self.order_)))
        
        # LIMIT
        # FIXME: Escape LIMIT???
        if not self.limit_ == None:
            query.append('LIMIT')
            query.append(str(self.limit_))
        
        return (' '.join(query), escape)
    
    def cursor(self, server = False):
        """
        **
        * Execute query and return cursor. If server == True, return a
        * server-side cursor.
        *
        * @param    server  bool
        * @return   object
        **
        """
        if self.db == None:
            return None
        
        query, escape = self.sql()
        
        cursor = self.db.cursor()
        cursor.execute(query, escape)
        return cursor
    
    def asObject(self, fetchall = False, server = False, destroy = True):
        """
        **
        * Execute query and return result object. If fetchall == True,
        * cursor.fetchall() will be used instead of wrapping the cursor
        * into an iterable object, and fetching as needed. If server == True,
        * use a server-side cursor and wrap it in an object, so data can be
        * downloaded as needed, instead of all at once.
        *
        * FIXME: Controllable fetch mode (tuple or dictionary)
        *
        * @param    fetchall    bool
        * @param    server      bool
        **
        """
        cursor = self.cursor(server)
        return QueryResult(cursor, destroy)
    
    def insertInto(self, table):
        pass
    
    def update(self, table):
        pass
    
    def deleteFrom(self, table):
        pass
    
    def sql(self):
        if self.kind == 'select':
            return self.fromSelect()
    
    def getConnection(self):
        """ Get database connection, if any """
        return self.db
    
    def setConnection(self, db):
        """ Set database connection """
        self.db = db
    
    def commit(self):
        """ Convenience wrapper for db.commit() """
        if self.db is not None:
            self.db.commit()
    
    def rollback(self):
        """ Convenience wrapper for db.rollback() """
        if self.db is not None:
            self.db.rollback()
    
    def isReady(self):
        return True
