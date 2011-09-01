
# -*- coding: utf-8 -*-

from doze import *

class BaseClause(object):
    """
    This is the Base class, which is extended by various other classes such as
    Join, Where, and Builder. It provides generic utility methods for handling
    SQL functions, and aliased fields.
    """
    
    # Valid boolean types, in lowercase
    booleanTypes = ['true', 'false']
    
    # Field quote
    fieldQuote = '`'
    
    # Field separator
    fieldSeparator = '.'
    
    # Value quote
    valueQuote = '\''
    
    # Characters which must be escaped / quoted to be understood literally by
    # the backend, for use in field, tables, etc.
    escapeLiterals = ' `~!@#$%^&*()-=+[]{}\\|;:\'",.<>/?'

    def setTableContext(self, ctx):
        """ Set the TableContext, for table aliases, etc """
        self.tableContext = ctx
    
    def getAliasedField(self, field, alias, kind = 'origin'):
        """
        Get aliased field. If the field already has a source table specified,
        then that will be used. Otherwise, it will try and guess which table
        each field belongs to, using the first join table. If the field / table
        contains special characters, than it will automatically be escaped and
        quoted.
        
        @param  field   Field.
        @param  alias   Alias
        @param  kind    Kind, either 'origin' or 'join'
        
        TODO: Simply the code below, and make documentation more clear.
        """
        
        ctx = self.tableContext
        aliased = field
        
        # Get alias context
        if alias == None or (type(alias) == list and len(alias) < 1):
            if not ctx == None:
                if kind == 'origin':
                    alias = ctx.origin()
                else:
                    # Get first join
                    # FIXME: TableContext isn't ordered
                    for i in ctx:
                        if i[1][1] == 'join':
                            alias = [i[0], i[1]]
                            break
        
        # Get alias from list
        if type(alias) == list and len(alias) > 0:
            # Get from alias (list)
            if len(alias) > 1:
                if type(alias[1]) == list:
                    alias = alias[1][0]
                else:
                    alias = alias[1]
            else:
                alias = alias[0]
            
            # Quote field?
            if self.fieldNeedsQuoted(alias) or self.fieldNeedsQuoted(field):
                alias = self.quoteField(alias)
                field = self.quoteField(field)
            
            aliased = alias + self.fieldSeparator + field
        elif type(alias) == str:
            # Get from alias (string)
            if not ctx == None:
                # If table name, get alias for table
                if alias in ctx:
                    alias = ctx[alias]
                    alias = alias[0]
            
            # Quote field?
            if self.fieldNeedsQuoted(alias) or self.fieldNeedsQuoted(field):
                alias = self.quoteField(alias)
                field = self.quoteField(field)
            
            aliased = alias + self.fieldSeparator + field
        
        return aliased
    
    def fieldIsAliased(self, field):
        """
        Check if field is aliased.
        
        First, a preliminary check is done to determine if "field" has an "."
        in it. If it does, a further test is done to check that the "." is
        outside of both field quotes and value quotes.
        """
        
        # Preliminary check
        if self.fieldSeparator not in field:
            return False
        
        # More detailed check
        inFieldQuote = False
        inValueQuote = False
        fieldLen = len(field)
        
        for i in range(0, fieldLen):
            if field[i] == self.fieldQuote and not inValueQuote:
                if inFieldQuote:
                    inFieldQuote = False
                else:
                    inFieldQuote = True
                
                # Continue from top
                continue
            
            if field[i] == self.valueQuote and not inFieldQuote:
                if inValueQuote:
                    inValueQuote = False
                else:
                    inValueQuote = True
                
                # Continue from top
                continue
            
            if (not inFieldQuote
            and not inValueQuote
            and field[i] == self.fieldSeparator
            and (i + 1) < fieldLen):
                # Not in any quotes, and character is "."
                # Return True, no further processing is needed
                return True
        
        return False
    
    def isSqlFunction(self, param):
        """
        Check if supplied argument is a SQL function. All database specific
        constants, which wrap to functions, may not be detected by this method.
        Also, PostgreSQL's "SELECT 'NOW()'::timestamp" functionality does not work
        in this implementation.
        
        Currently detected constants are:
            CURRENT_TIME
            CURRENT_DATE
            CURRENT_TIMESTAMP
            LOCALTIMESTAMP
            CURRENT_USER
        """
    
        # Constants, which are usually aliases for functions in most RDBMS
        nonParenthesised = [
            'CURRENT_TIME',
            'CURRENT_DATE',
            'CURRENT_TIMESTAMP',
            'LOCALTIMESTAMP',
            'CURRENT_USER'
        ]
    
        # Check for constants / functions
        if param.strip().upper() in nonParenthesised:
            return True
    
        # Preliminary check for parenthesis
        if '(' not in param or ')' not in param:
            return False
    
        # These will be used to detect the open and close parentheses
        openParen = False
        closeParen = False
    
        # State, for if we're currently in a quoted or double-quoted string
        inFieldQuote = False
        inValueQuote = False
    
        # Fairly basic algorithm. It detects the state of quotes, and uses
        # that information to determine whether or not openParen / closeParen
        # should be changed when parentheses are detected.
        for i in range(0, len(param)):
            if param[i] == self.fieldQuote and not inValueQuote:
                if inFieldQuote:
                    inFieldQuote = False
                else:
                    inFieldQuote = True
                continue
            
            if param[i] == self.valueQuote and not inFieldQuote:
                if inValueQuote:
                    inValueQuote = False
                else:
                    inValueQuote = True
                continue
    
            if param[i] == '(' and not inFieldQuote and not inValueQuote:
                openParen = True
                continue
        
            if param[i] == ')' and not inFieldQuote and not inValueQuote:
                if openParen == True:
                    # Found both parentheses. Can safely break.
                    closeParen = True
                    break
    
        return openParen and closeParen
    
    def isSelectQuery(self, param):
        """
        Determine if param is a SELECT query string, or not. Currently just does
        basic checking. Parenthesised queries will fail. 
        """
    
        param = param.strip().lower()
        
        # Preliminary check
        if param[0:6] == 'select':
            return True
        
        return False
    
    def isSqlExpression(self, param):
        """
        Determine if param is an SQL expression, such as "foo = 1", or
        "CASE WHEN...", etc.
        
        Currently not implemented.
        """
        
        raise NotImplemented('isSqlExpression Not Implemented')
    
    def isQuotedValue(self, param):
        """ Return true if param is a quoted value. """
        param = param.strip()
        return param[0] == self.valueQuote and param[len(param)-1] == self.valueQuote
    
    def isValue(self, param):
        """ Return true if param is a value, quoted or otherwise. """
        
        # Strip space from param
        param = param.strip()
        
        # Check if boolean
        if param.lower() in self.booleanTypes:
            return True
        
        # Check if quoted / string value
        if self.isQuotedValue(param):
            return True
        
        # Check if integer
        if param.isdigit():
            return True
        
        # Check if float
        try:
            float(param)
            return True
        except ValueError:
            pass
        
        return False
    
    def isQuotedField(self, param):
        """ Return true if param is a valid quoted field. """
        param = param.strip()
        return param[0] == self.fieldQuote and param[len(param)-1] == self.fieldQuote
    
    def expandTable(self, table):
        """
        Expand table, such as 'mytable a' to ['mytable', 'a']
        so that they can be used self.tableContext.
        """
        if type(table) == str:
            if (not self.isQuotedValue(table)
            and not self.isQuotedField(table)
            and ' ' in table):
                tmp = table.split(' ')
                if len(tmp) == 2:
                    table = tmp
    
        return table
    
    def fieldNeedsQuoted(self, field):
        """ Check if table or field needs to be quoted. """
        
        if self.isQuotedField(field):
            return False
        
        # Search for literals which need to be quoted
        for literal in self.escapeLiterals:
            if literal in field:
                return True
        
        return False
    
    def quoteField(self, field):
        """ Escape and quote a field or table. """
    
        if self.fieldQuote in field:
            escaped = ''
            for i in range(0, len(field)):
                if field[i] == self.fieldQuote:
                    escaped += self.fieldQuote
                escaped += field[i]
        else:
            escaped = field
        
        return self.fieldQuote + escaped + self.fieldQuote

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

    def __init__(self, field, kind = FIELD, alias = None):
        super(Where, self).__init__()
        self.current = [None, field, kind, alias]
        self.where = []
        self.tableContext = None
    
    def __getattr__(self, key):
        def setCurrent(field, kind = FIELD, alias = None):
            self.current = [key, field, kind, alias]
            return self
    
        def setWhere(value = None, kind = VALUE, alias = None):
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
        
        if each['sourceType'] == VALUE:
            # Source is a value
            source = '%s'
            escape.append(each['sourceName'])
        else:
            source = self.getAliasedField(each['sourceName'],\
                each['sourceAlias'], 'origin')
        
        if each['destType'] == VALUE:
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
        if len(expression[2]) == 1:
            if self.isSelectQuery(expression[2][0]):
                # Is a sub-query
                return expression[0] + ' IN (' + expression[2][0] + ')'
        return expression[0] + ' IN (%s)' % ', '.join(['%s' for i in expression[2]])

    def build_notIn(self, expression, each):
        if len(expression[2]) == 1:
            if self.isSelectQuery(expression[2][0]):
                # Is a sub-query
                return expression[0] + ' NOT IN (' + expression[2][0] + ')'
        return expression[0] + ' NOT IN (%s)' % ', '.join(['%s' for i in expression[2]])

    def build_isNull(self, expression, each):
        return expression[0] + ' IS NULL'
    
    def build_isNotNull(self, expression, each):
        return expression[0] + ' IS NOT NULL'
    
    def build_isEmpty(self, expression, each):
        return expression[0] + ' = ' + self.valueQuote + self.valueQuote
    
    def build_isNotEmpty(self, expression, each):
        return expression[0] + ' != ' + self.valueQuote + self.valueQuote

    def sql(self):
        """ Build SQL and return (query, escape) """
        
        sql = []
        escape = []
        
        for each in self.where:
            # Dictionary-ize
            each = dict(zip(self.map, each))
            
            # Parse expression
            expr = self.parseExpression(each)
            
            # Extend escape list
            if each['comp'] == 'isIn' or each['comp'] == 'isNotIn':
                # Check if isIn/isNotIn has sub-query
                if len(expr[2]) == 1 and self.isSelectQuery(expr[2][0]):
                    # Don't append escape, if sub-query
                    pass
                else:
                    escape.extend(expr[2])
            else:
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
    def __init__(self, cursor = None, destroy = True, fetch = dict):
        self.setCursor(cursor, destroy)
        self.fetch = fetch
    
    def __len__(self):
        if self.cursor == None:
            return 0
        return self.cursor.rowcount
    
    def __iter__(self):
        res = self.cursor.fetchone()
        if self.cursorDescrInit == False:
            self.initCursorDescr()
    
        while not res == None:
            if self.fetch.__name__ == 'dict':
                yield dict(zip(self.labels, res))
            else:
                yield res
            res = self.cursor.fetchone()
        
        if self.destroy == True:
            self.cursor.close()
    
    def next(self):
        res = self.cursor.fetchone()
        if self.cursorDescrInit == False:
            self.initCursorDescr()

        if self.fetch.__name__ == 'dict':
            return dict(zip(self.labels, res))
        return res
    
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
    """ Builder Class. """
    
    def __init__(self, db = None, onError = None):
        self.tableContext = TableContext()
        self.db = db
        self.onError = onError
        self.kind = None
    
    def reset(self):
        """
        Reset variables to undo everything cause by select: insertInto, update,
        deleteFrom, etc.
        
        This is typically called from one of those methods, but may be useful to
        call when recovering from an error.
        """
        
        if self.tableContext is not None:
            del self.tableContext
        
        # Common
        self.tableContext = TableContext()
        self.kind = None
        self.where_ = []
        
        # Select
        self.joins = []
        self.having_ = None
        self.group_ = None
        self.order_ = None
        self.limit_ = None
        self.union_ = []
        self.columns = []
        self.source = None
        
        # With
        self.with__ = []
        self.withQueries = []
        self.withIsRecursive = False
        self.withNotComplete = False
        
        # Insert / Update
        self.values_ = []
        self.destination = None
    
    def select(self, columns):
        if self.kind == 'with' and self.withNotComplete:
            self.columns = columns
            return self
            
        self.reset()
        self.kind = 'select'
        self.columns = columns
        
        return self
    
    def from_(self, source):
        # Expand table
        source = self.expandTable(source)
            
        # Set self.tableContext
        if type(source) == list and len(source) > 1:
            self.tableContext[source[0]] = source[1]
        
        self.source = source
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
    
    def union(self, query, all=False):
        self.union_.append((query, all))
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
                # Append field table when any of the following is true:
                #   - Field is already aliased
                #   - Field is an SQL function
                #   - Field is a quoted value
                #
                # Otherwise, alias it with the origin table and append it.
                if (self.fieldIsAliased(i)
                or self.isSqlFunction(i)
                or self.isValue(i)):
                    cols.append(i)
                else:
                    cols.append(self.fieldSeparator.join([origin, i]))
            
            columns = cols
        return columns
    
    def fromSelect(self):
        query = ['SELECT']
        escape = []
        
        columns = self.normalizeColumns(self.columns)
        query.extend([', '.join(columns)])
        
        # FROM
        if self.source is not None:
            query.append('FROM')
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
        
        # UNION / UNION ALL
        if len(self.union_) > 0:
            for union, all in self.union_:
                if all:
                    query.append('UNION ALL')
                else:
                    query.append('UNION')
                
                if type(union) == str:
                    query.append(union)
                    continue
                
                (tmpquery, tmpescape) = union.sql()
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
    
    def with_(self, name, recursive=False):
        """ Perform WITH / CTE query. """
    
        self.reset()
        self.kind = 'with'
        self.with__.append(name)
        self.withIsRecursive = recursive
        self.withNotComplete = True
        
        return self
    
    def as_(self, query, name = None):
        """ AS, used in WITH / CTE queries """
    
        self.withQueries.append(query)
        if name is not None:
            self.with__.append(name)
        
        return self
    
    def fromWith(self):
        """ Generate (query, escape) from WITH / CTE query. """
    
        escape = []
        query = ['WITH']
        withQuery = []
        
        if self.withIsRecursive:
            query.append('RECURSIVE')
        
        for i in range(0, len(self.with__)):
            if type(self.withQueries[i]) == str:
                tmpquery = self.withQueries[i]
                tmpescape = []
            else:
                (tmpquery, tmpescape) = self.withQueries[i].sql()
            
            escape.extend(tmpescape)
            withQuery.append(' '.join([self.with__[i], 'AS', '(' + tmpquery + ')']))
        
        query.append(', '.join(withQuery))
        (tmpquery, tmpescape) = self.fromSelect()
        query.append(tmpquery)
        escape.extend(tmpescape)
        
        self.withNotComplete = False
        return (' '.join(query), escape)
    
    def insertInto(self, table):
        """ Perform INSERT on table. """
    
        self.reset()
        self.kind = 'insert'
        self.destination = table
        return self
    
    def values(self, values):
        self.values_ = values
        return self
    
    def fromInsert(self):
        """ From INSERT. Returns single SQL statement. """
        
        values = self.values_
        query = ['INSERT INTO', self.destination]
        
        keys = []
        vals = []
        escape = []
        for k, v in values.items():
            keys.append(k)
            vals.append('%s')
            escape.append(v)
        
        query.append('(' + ', '.join(keys) + ')')
        query.append('VALUES')
        query.append('(' + ', '.join(vals) + ')')
        
        return (' '.join(query), escape)
    
    def update(self, table):
        """ Perform UPDATE on table. """

        self.reset()
        self.kind = 'update'
        self.destination = table
        return self
    
    def set(self, values):
        return self.values(values)
    
    def fromUpdate(self):
        """ From UPDATE. Returns (query, escape) """
        
        values = self.values_
        query = ['UPDATE', self.destination, 'SET']
        
        escape = []
        vals = []
        
        for k, v in values.items():
            if type(v) == list and v[1] == FIELD:
                vals.append(k + ' = ' + v[0])
                continue
                
            vals.append(k + ' = %s')
            escape.append(v)
        
        query.append(', '.join(vals))
        
        # WHERE
        if len(self.where_) > 0:
            query.append('WHERE')
            for i in self.where_:
                tmpquery, tmpescape = i.sql()
                query.append(tmpquery)
                escape.extend(tmpescape)
        
        return (' '.join(query), escape)
    
    def deleteFrom(self, table):
        """ Perform DELETE on table. """
    
        self.reset()
        self.kind = 'delete'
        self.destination = table
        return self
    
    def fromDelete(self):
        """ From DELETE. Returns (query, escape). """
        
        query = ['DELETE FROM', self.destination]
        escape = []
        
        # WHERE
        if len(self.where_) > 0:
            query.append('WHERE')
            for i in self.where_:
                tmpquery, tmpescape = i.sql()
                query.append(tmpquery)
                escape.extend(tmpescape)
        
        return (' '.join(query), escape)
    
    @ExceptionWrapper
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
    
    @ExceptionWrapper
    def asObject(self, fetchall = False, server = False, destroy = True, fetch = dict):
        """
        **
        * Execute query and return result object. If fetchall == True,
        * cursor.fetchall() will be used instead of wrapping the cursor
        * into an iterable object, and fetching as needed. If server == True,
        * use a server-side cursor and wrap it in an object, so data can be
        * downloaded as needed, instead of all at once.
        *
        * @param    fetchall    bool
        * @param    server      bool
        * @param    fetch       type, dict returns dictionaries, anything else
        *                       returns tuples
        * @return   QueryResult
        **
        """
        cursor = self.cursor(server)
        return QueryResult(cursor, destroy, fetch)
    
    @ExceptionWrapper
    def execute(self, server = False):
        cursor = self.cursor(server)
        cursor.close()
        return self
    
    @ExceptionWrapper
    def sql(self):
        if self.kind == 'select':
            return self.fromSelect()
        elif self.kind == 'insert':
            return self.fromInsert()
        elif self.kind == 'update':
            return self.fromUpdate()
        elif self.kind == 'delete':
            return self.fromDelete()
        elif self.kind == 'with':
            return self.fromWith()
    
    @ExceptionWrapper
    def commit(self):
        """ Convenience wrapper for db.commit() """
        if self.db is not None:
            self.db.commit()
    
    @ExceptionWrapper
    def rollback(self):
        """ Convenience wrapper for db.rollback() """
        if self.db is not None:
            self.db.rollback()
    
    def getConnection(self):
        """ Get database connection, if any """
        return self.db
    
    def setConnection(self, db):
        """ Set database connection """
        self.db = db
    
    def isReady(self):
        return True
    
    def isConnected(self):
        return True
