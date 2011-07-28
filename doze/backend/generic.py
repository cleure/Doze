
# -*- coding: utf-8 -*-

from doze import *

class Where():
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

    def __init__(self, field, type = TYPE_FIELD, alias = None):
        self.current = [None, field, type, alias]
        self.where = []
        self.tableContext = None
    
    def setTableContext(self, ctx):
        """ Set the TableContext, for table aliases, etc """
        self.tableContext = ctx
    
    def __getattr__(self, key):
        def setCurrent(field, type = TYPE_FIELD, alias = None):
            self.current = [key, field, type, alias]
            return self
    
        def setWhere(value = None, type = TYPE_VALUE, alias = None):
            temp = self.current
            temp.extend([key, value, type, alias])
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
        ctx = self.tableContext
        data = []
        source = each['sourceName']
        dest = each['destName']
        escape = []
        
        if each['sourceType'] == TYPE_VALUE:
            # Source is a value
            source = '%s'
            escape.append(each['sourceName'])
        else:
            # Source is a field
            alias = None
            if not each['sourceAlias'] == None:
                # Extract alias from each
                alias = each['sourceAlias']
            elif not ctx == None:
                # Extract alias from tableContext
                alias = ctx.origin()
                
                if not alias == None and len(alias) > 0:
                    if len(alias) > 1:
                        # Table alias
                        alias = alias[1]
                    else:
                        # Table name
                        alias = alias[0]
                
                if not alias == None:
                    source = alias + '.' + source
        
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
            alias = None
            if not each['destAlias'] == None:
                # Extract alias from each
                alias = each['destAlias']
            elif not ctx == None:
                # Extract alias from tableContext
                for alias in ctx.joins():
                    break
                
                if not alias == None and len(alias) > 0:
                    alias = alias[0]
                    if len(alias) > 1:
                        # Table alias
                        alias = alias[1]
                    else:
                        # Table name
                        alias = alias[0]
                
                if not alias == None:
                    dest = alias + '.' + dest
        
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

