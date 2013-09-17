
from doze import *
from doze.backend.generic.base import *

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
    *   equals                  - Equals
    *   notEquals               - Not equals
    *   gt                      - Greater than (>)
    *   gte                     - Greater than or equals (>=)
    *   lt                      - Less than (<)
    *   lte                     - Less than or equals (<=)
    *   isIn                    - Is in [list]
    *   notIn                   - Is not in [list]
    *   isNull                  - Is null
    *   isNotNull               - Is not null
    *   isEmpty                 - Is empty
    *   isNotEmpty              - Is not empty
    *   between (min, max)      - BETWEEN
    *   like                    - LIKE
    *   ilike                   - ILIKE
    *
    * Soon to be supported Operators:
    *    operator(operator, field)       - Custom operator
    *
    * TODO:
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
        'isNotEmpty',
        'between',
        'like',
        'ilike',
        'exists']
    
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
        'isNotEmpty',
        'between',
        'like',
        'ilike',
        'exists']

    # Key map
    map = [
        'cond',             # Conditional AND/OR?
        'sourceName',       # Name/value of source
        'sourceType',       # Source type (doze.FIELD or doze.VALUE)
        'sourceAlias',      # Alias for source table
        'comp',             # Comparison Operator (see self.compOperators)
        'destName',         # Name/value of destination
        'destType',         # Destination type (doze.FIELD or doze.VALUE)
        'destAlias']        # Alias for destination table

    def __init__(self, field = None, kind = FIELD, alias = None):
        """
        Initialize instance variables, etc. This will set self.current, with
        the values that are passed to this method. It also calls the parent
        __init__().
        
        @param  field   - str
            Field name. Can be aliased, however, if it is aliased and
            alias parameter is also set, the field will be treated literally,
            which will likely lead to errors.
            
            NOTE: field can be None, but only for certain types of WHERE's,
            such as EXISTS (sub-query).
        
        @param  kind    - str
            Field/value kind. If its set to doze.FIELD, then the given
            field is treated as a field. If it is set to doze.VALUE, it
            is treated as a value.
        
        @param  alias   - str
            Optional alias for field. In most cases, you probably wont
            need to use this.
        """
    
        super(Where, self).__init__()
        self.current = [None, field, kind, alias]
        self.where = []
        self.tableContext = None
    
    def __getattr__(self, key):
        """
        This method is used for handlind method calls, which don't actually
        exist (such as equals(), isIn(), etc). Handled comparison operators,
        such as "equals", 'notEquals' are in self.compOperators. Handled
        conditionals such as "AND"/"OR" are in self.condOperators.
        
        If an invalid attribute is given, an AttributeError exception will be
        raised.
        
        @param      key     - str, key name
        @return     void
        """
        
        def setCurrent(field, kind = FIELD, alias = None):
            self.current = [key, field, kind, alias]
            return self
    
        def setWhere(value = None, kind = VALUE, alias = None):
            self.current.extend([key, value, kind, alias])
            self.where.append(self.current)
            self.current = None
            return self
    
        if key in self.condOperators:
            return setCurrent
        elif key in self.compOperators:
            return setWhere
        
        raise AttributeError(str(self.__class__)
            + " instance has no attribute '" + key + "'")
    
    def between(self, min, max, kind = VALUE, alias = None):
        """
        Between uses its own method, so you don't have to pass arguments in a
        list, due to limitations in the __getattr__ method.
        
        @param  min
        @param  max
        @kind   doze.FIELD or doze.VALUE
        @alias  alias for field
        @return reference to self
        """
        
        self.current.extend(['between', [min, max], kind, alias])
        self.where.append(self.current)
        self.current = None
        return self
        
    
    def parseExpression(self, each):
        """
        Perform the initial parsing stage, and return tuple.
        
        @param  each    - dict, (see self.map for keys)
        @return tuple of (source_value, destination_value, to_escape)
        """
        data = []
        source = each['sourceName']
        dest = each['destName']
        escape = []
        
        # Source
        if each['sourceType'] == VALUE:
            # Source is a value
            source = self.escapePattern
            escape.append(each['sourceName'])
        else:
            source = self.getAliasedField(each['sourceName'],\
                each['sourceAlias'], 'origin')
        
        # Dest
        if each['destType'] == VALUE:
            # Dest is value
            dest = self.escapePattern
            
            if type(each['destName']) == list:
                escape.extend(each['destName'])
            elif each['destName'] is not None:
                escape.append(each['destName'])
        else:
            # Dest is field
            dest = self.getAliasedField(each['destName'],\
                each['destAlias'], 'join')
        
        return [source, dest, escape]

    def buildComparison(self, expression, each):
        """
        Build comparison string.
        
        @param      expression  - list/tuple
        @param      each        - dict
        @return     str
        """
        
        comparison = None
        simple = self.simpleCompOperators
        complex_ = self.complexCompOperators
        funcName = 'build_' + each['comp']
        
        if each['comp'] in simple:
            if len(expression[2]) == 1 and self.isSelectQuery(expression[2][0]):
                # Sub-query handling
                comparison = ' '.join([
                    expression[0],
                    simple[each['comp']],
                    '(' + expression[2][0] + ')'])
            else:
                # Value handling
                comparison = ' '.join([
                    expression[0],
                    simple[each['comp']],
                    expression[1]])
        elif hasattr(self, funcName):
            func = getattr(self, funcName)
            comparison = func(expression, each)
        else:
            raise DozeError('Unsupported Operator: ' + each['comp'])
        
        return comparison

    def build_isIn(self, expression, each):
        if len(expression[2]) == 1:
            if self.isSelectQuery(expression[2][0]):
                # Is a sub-query
                return expression[0] + ' IN (' + expression[2][0] + ')'
        return expression[0] + ' IN (%s)' % ', '.join([self.escapePattern for i in expression[2]])

    def build_notIn(self, expression, each):
        if len(expression[2]) == 1:
            if self.isSelectQuery(expression[2][0]):
                # Is a sub-query
                return expression[0] + ' NOT IN (' + expression[2][0] + ')'
        return expression[0] + ' NOT IN (%s)' % ', '.join([self.escapePattern for i in expression[2]])

    def build_isNull(self, expression, each):
        return expression[0] + ' IS NULL'
    
    def build_isNotNull(self, expression, each):
        return expression[0] + ' IS NOT NULL'
    
    def build_isEmpty(self, expression, each):
        return expression[0] + ' = ' + self.valueQuote + self.valueQuote
    
    def build_isNotEmpty(self, expression, each):
        return expression[0] + ' != ' + self.valueQuote + self.valueQuote
    
    def build_between(self, expression, each):
        expr = []
        for i in expression[2]:
            if self.isSelectQuery(i):
                expr.append('(' + i + ')')
            else:
                expr.append(self.escapePattern)
        return (expression[0] + ' BETWEEN ' + expr[0] + ' AND ' + expr[1])

    def build_like(self, expression, each):
        exprString = expression[0] + ' LIKE '
        if self.isSelectQuery(expression[2][0]):
            return exprString + '(' + expression[2][0] + ')'
        return exprString + self.escapePattern
        
    def build_ilike(self, expression, each):
        exprString = expression[0] + ' ILIKE '
        if self.isSelectQuery(expression[2][0]):
            return exprString + '(' + expression[2][0] + ')'
        return exprString + self.escapePattern
    
    def build_exists(self, expression, each):
        return 'EXISTS (' + expression[2][0] + ')'

    def sql(self):
        """ Build SQL and return (query, escape) """
        
        sql = []
        escape = []
        
        for each in self.where:
            # Dictionary-ize
            each = dict(zip(self.map, each))
            
            #
            # TODO: Store state of each expression[2] for isSelectQuery(). Since
            # isSelectQuery() is pretty much guaranteed to be called multiple times.
            #
            
            # Parse expression
            expr = self.parseExpression(each)
            
            # Handle parameter(s) being a builder object
            if len(expr[2]) > 0:
                for i in range(0, len(expr[2])):
                    if expr[2][i].__class__.__name__ == 'Builder':
                        tmpquery, tmpescape = expr[2][i].sql()
                        expr[2][i] = tmpquery
                        escape.extend(tmpescape)
                
                    if not self.isSelectQuery(expr[2][i]):
                        escape.append(expr[2][i])
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
