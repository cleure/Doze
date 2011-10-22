
from doze import *

class IterableField():
    """
    Iterable Field. This class is intended to be used for iterating over
    field strings, to match various criteria, in order to simplify logic
    in other places.
    
    Basic Usage:
        it = IterableField('my "string"', check='\'"`')
        for i in it.iterate():
            if i['inside_quote']:
                do_something()
            else:
                do_something_else()
    
    TODO: Implement parenthesis support "()[]{}", etc
    """

    def __init__(self, string, check='\'`'):
        self.string = string
        self.check = check
        self.insideQuote = False
        self.literalQuote = None
    
    def iterate(self):
        """ Iterate over string """
        
        # Helper function, to build the object which will be yielded
        def make_object():
            return {
                'first': first,
                'last': last,
                'index': i,
                'string': self.string[i],
                'inside_quote': self.insideQuote,
                'literal_quote': self.literalQuote}
    
        # Iterate over string
        string_len = len(self.string)
        for i in range(0, string_len):
        
            # First loop?
            first = False
            if i == 0:
                first = True
            
            # Last loop?
            last = False
            if (i + 1) == string_len:
                last = True
        
            # Quotes
            if not self.insideQuote and self.string[i] in self.check:
                self.insideQuote = True
                self.literalQuote = self.string[i]
            elif self.insideQuote and self.string[i] == self.literalQuote:
                yield make_object()
                self.insideQuote = False
                continue
            
            yield make_object()

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
    
    # Search quotes
    searchQuotes = '\'`'
    
    # Characters which must be escaped / quoted to be understood literally by
    # the backend, for use in field, tables, etc.
    escapeLiterals = ' `~!@#$%^&*()-=+[]{}\\|;:\'",.<>/?'
    
    # Assignment / Comparison operators.
    assignments = []

    def __init__(self):
        self.childObjects = []

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
        
        TODO: Simplify the code below, and make documentation more clear.
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
        
        it = IterableField(field, self.searchQuotes)
        for i in it.iterate():
            if (not i['inside_quote']
                and i['string'] == self.fieldSeparator
                and i['last'] == False):
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
        if not type(param) == str:
            return False
    
        param = param.strip().lower()
        
        # Preliminary check
        if param[0:6] == 'select':
            return True
        
        return False
    
    def isSqlExpression(self, param):
        """
        Determine if param is an SQL expression, such as "foo = 1", or
        "CASE WHEN...", etc.
        
        NOTE: The current imlpementation is very primitive, and probably won't catch
        all cases.
        """
        
        basicAssignments = ['=', '<', '>']
        
        if param[0:4].lower() == 'case':
            return True
        
        inFieldQuote = False
        inValueQuote = False
        
        for i in range(0, len(param)):
            if param[i] == self.fieldQuote and not inValueQuote:
                if inFieldQuote:
                    inFieldQuote = False
                else:
                    inFieldQuote = True
            
            if param[i] == self.valueQuote and not inFieldQuote:
                if inValueQuote:
                    inValueQuote = False
                else:
                    inValueQuote = True
        
            if (param[i] in basicAssignments
            and not inValueQuote
            and not inFieldQuote):
                return True
        
        return False
    
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
    
    def splitFields(self, string):
        """
        Split fields from string. This has some pitfalls in MySQL ('"' is ignored),
        however, it's much smarter than 'string'.split(). In the future, Support MySQL
        will be improved.
        """
        
        inFieldQuote = False
        inValueQuote = False
        start = 0
        fields = []
        length = len(string)
    
        for i in range(0, length):
            if (string[i] == self.fieldQuote
            and not inValueQuote):
                if inFieldQuote:
                    inFieldQuote = False
                else:
                    inFieldQuote = True
            
            if (string[i] == self.valueQuote
            and not inFieldQuote):
                if inValueQuote:
                    inValueQuote = False
                else:
                    inValueQuote = True
            
            if (not inFieldQuote and not inValueQuote
            and string[i] == ','):
                fields.append(string[start:i].strip())
                start = (i + 1)
        
        fields.append(string[start:length].strip())
        return fields
    
    def preProcessSql(self):
        """ Method which gets called before sql(), to update tableContext, etc. """
        for i in self.childObjects:
            if i.tableContext is None and self.tableContext is not None:
                i.setTableContext(self.tableContext)
            i.preProcessSql()
