
# -*- coding: utf-8 -*-

from doze import *

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
        complex = self.complexCompOperators
        
        if each['comp'] in simple:
            comparison = ' '.join([expression[0], simple[each['comp']], expression[1]])
        elif each['comp'] in complex:
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
    """ Join Class. API Draft Stage """
    def __init__(self, table, kind = JOIN_INNER, filter = None, context = None):
        super(Join, self).__init__()
    
        self.where = None
        self.tableContext = None
        self.table = table
        self.kind = kind
        
        if isinstance(filter, Where):
            self.where = filter
        
        if isinstance(context, TableContext):
            self.setTableContext(context)
    
    def filter(self, where):
        self.where = where
        return self
    
    def sql(self):
        """ Build SQL and return string """
        ctx = self.tableContext
        
        if self.kind == JOIN_INNER:
            data = ['INNER JOIN']
        elif self.kind == JOIN_LEFT:
            data = ['LEFT JOIN']
        elif self.kind == JOIN_RIGHT:
            data = ['RIGHT JOIN']
        else:
            raise ValueError('Invalid value for kind')

        if not isinstance(self.where, Where):
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
        
        data.append('ON')
        
        where, escape = self.where.sql()
        data.append('(' + where + ')')
        
        return (' '.join(data), escape)
        