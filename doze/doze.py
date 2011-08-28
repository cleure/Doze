
# -*- coding: utf-8 -*-

VALUE = 1
FIELD = 2

JOIN_INNER = 1
JOIN_LEFT = 2
JOIN_RIGHT = 3

# Default exception for internal errors
class DozeError(Exception): pass

def ExceptionWrapper(func):
    """
    Exception Wrapping Decorator for method calls. If it's applied to a
    method, it will wrap the method in a try/except. On exception, it
    checks for self.onError. When self.onError exists, and is callable,
    it calls self.onError instead of raising an exception, passing the
    exception as a parameter. From there on out, it's up to self.onError
    to decide whether or not to throw an exception.
    
    Also very useful if you want to install cleanup handlers for certain
    types of errors.
    """
    def wrapper(self, *args):
        try:
            return func(self, *args)
        except Exception as ex:
            if hasattr(self, 'onError') and hasattr(self.onError, '__call__'):
                return self.onError(ex)
            else:
                raise ex
    return wrapper

class TableContext(object):
    """
    **
    * This class is a helper for dealing with table contexts. It stores
    * various information about tables, such as which one is the origin
    * (eg, the table SELECT was called on), as well as table aliases.
    *
    * It provides a useful ctx.origin() method, to quickly find out what
    * the origin table is, as well as the generator method ctx.joins(),
    * for quickly iterating over join tables.
    *
    * Aliases can be added by treating ctx as either a class object or
    * a dictionary.
    *
    * Examples:
    *
    *   ctx = TableContext(['table1', 'a', 'origin'], ['table2', 'b', 'join'])
    *   ctx = TableContext()
    *   ctx.table1 = ['a', 'origin']
    *   ctx.table2 = ['b', 'join']
    *
    *   for i in ctx.joins():
    *       print i[0] + ': ' + i[1]
    *
    *   print ctx.origin()
    *
    * When no origin / join data is given, TableContext tries to
    * automatically determine which should be used. For instance, this:
    *   ctx = TableContext(['table1', 'a'], ['table2', 'b'])
    *
    * Is the same as this:
    *   ctx = TableContext(['table1', 'a', 'origin'], ['table2', 'b', 'join'])
    **
    """

    def __init__(self, *args):
        self.__dict__['aliases'] = {}
        
        first = 1
        for arg in args:
            self.setAlias(arg)
            first = 0
    
    def __getattr__(self, name):
        if name not in self.aliases:
            raise AttributeError(str(self.__class__) + " instance has no attribute '" + name + "'")
        
        return self.aliases[name]
    
    def __setattr__(self, name, value):
        tmp = [name]
        tmp.extend(value)
        self.setAlias(tmp)
    
    def __delattr__(self, name):
        if not self.__contains__(name):
            raise KeyError(str(name) + ' not in TableContext')
        del self.aliases[name]
    
    def __getitem__(self, key):
        return self.__getattr__(key)
        
    def __setitem__(self, key, value):
        return self.__setattr__(key, value)
    
    def __delitem__(self, key):
        return self.__delattr__(key)
    
    def __contains__(self, name):
        return name in self.aliases
    
    def __iter__(self):
        for i in self.aliases.items():
            yield i
    
    def __len__(self):
        return len(self.aliases)
    
    def isValidAlias(self, alias):
        """ Checks if user supplied alias is a valid format or not. Returns boolean. """
        
        if type(alias) == list or type(alias) == tuple:
            if not len(alias) >= 2:
                return False
        else:
            return False
        return True
    
    def isValidContext(self, context):
        """ Check to see if supplied context is valid or not """
        if context == 'origin' or context == 'join':
            return True
        return False
    
    def normalize(self, alias):
        """ Normalizes user supplied alias. Transforms dictionary to list. """

        if type(alias[1]) == list:
            tmp = [alias[0]]
            tmp.extend(alias[1])
            alias = tmp
        return alias
    
    def setAlias(self, alias):
        """ Set alias, or raise DozeError on error """
        
        # Automatically append alias (using table name)
        if len(alias) == 1:
            alias.append(alias[0])
        
        # Automatically append origin / join
        if len(alias) == 2:
            origin = self.origin()
            if origin == None:
                alias.append('origin')
            else:
                alias.append('join')
        
        if not self.isValidAlias(alias):
            raise DozeError('Invalid format for alias')
        
        alias = self.normalize(alias)
        if len(alias) >= 3:
            if not self.isValidContext(alias[2]):
                raise DozeError('Invalid context')
            
            if alias[2] == 'origin' and not self.origin() == None:
                raise DozeError('Context "origin" may only be used once')
        
        for k, v in self.aliases.items():
            if alias[1] == v[0]:
                raise DozeError('Duplicate alias for tables: '\
                    + k + ' and ' + alias[0])
        
        self.aliases[alias[0]] = [alias[1], alias[2]]
    
    def setContext(self, name, context):
        """ Set context for table. Context can be either 'origin' or 'join' """
        if not self.isValidContext(context):
            raise DozeError('Invalid context type')
        
        if name not in self.aliases:
            raise DozeError('No such table')
        
        self.aliases[name][1] = context
    
    def aliasFor(self, name):
        """ Get alias for name, of none, return name """
        if name not in self.aliases:
            return name
        return self.aliases[name][0]
    
    def contextFor(self, name):
        """ Get context for name, of none, return none """
        if name not in self.aliases:
            return None
        return self.aliases[name][1]
    
    def origin(self):
        """ Get origin """
        for k, v in self.aliases.items():
            if v[1] == 'origin':
                return [k, v[0]]
        return None
    
    def joins(self):
        """ Generator to get joins """
        for k, v in self.aliases.items():
            if v[1] == 'join':
                yield [k, v[0]]

class BaseClause(object):
    """
    This is the Base class, which is extended by various other classes such as
    Join, Where, and Builder. It provides generic utility methods for handling
    SQL functions, and aliased fields.
    
    In the future, there will need to be some way to override this class, so
    that database specific backends can replace these methods.
    """
    
    fieldQuote = '`'
    valueQuote = '\''
    
    def __init__(self):
        pass

    def setTableContext(self, ctx):
        """ Set the TableContext, for table aliases, etc """
        self.tableContext = ctx
    
    def getAliasedField(self, field, alias, kind = 'origin'):
        """ Get aliased field """
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
            
            aliased = alias + '.' + field
        elif type(alias) == str:
            # Get from alias (string)
            if not ctx == None:
                # If table name, get alias for table
                if alias in ctx:
                    alias = ctx[alias]
                    alias = alias[0]
            aliased = alias + '.' + field
        
        return aliased
    
    def fieldIsAliased(self, field):
        """
        Check if field is aliased.
        
        First, a preliminary check is done to determine if "field" has an "."
        in it. If it does, a further test is done to check that the "." is
        outside of both field quotes and value quotes.
        """
        
        # Preliminary check
        if '.' not in field:
            return False
        
        # More detailed check
        inFieldQuote = False
        inValueQuote = False
        
        for i in range(0, len(field)):
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
            and field[i] == '.'):
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
        inQuote = False
        inDoubleQuote = False
    
        # Fairly basic algorithm. It detects the state of quotes, and uses
        # that information to determine whether or not openParen / closeParen
        # should be changed when parentheses are detected.
        for i in range(0, len(param)):
            if param[i] == '\'':
                if inDoubleQuote == False:
                    if inQuote == True:
                        inQuote = False
                    else:
                        inQuote = True
    
            if param[i] == '"':
                if inQuote == False:
                    if inDoubleQuote == True:
                        inDoubleQuote = False
                    else:
                        inDoubleQuote = True
    
            if param[i] == '(' and not inDoubleQuote and not inQuote:
                openParen = True
        
            if param[i] == ')' and not inDoubleQuote and not inQuote:
                if openParen == True:
                    # Found both parentheses. Can safely break.
                    closeParen = True
                    break
    
        return openParen and closeParen
    
    def isQuotedValue(self, param):
        """ Return true if param is a quoted value. """
        param = param.strip()
        return param[0] == self.valueQuote and param[len(param)-1] == self.valueQuote
    
    def isValue(self, param):
        """ Return true if param is a value, quoted or otherwise. """
        
        if self.isQuotedValue(param):
            return True
        
        param = param.strip()
        if param.lower() in ['true', 'false']:
            return True
    
        return param.isdigit()
    
    def isQuotedField(self, param):
        """ Return true if param is a valid quoted field. """
        param = param.strip()
        return param[0] == self.fieldQuote and param[len(param)-1] == self.fieldQuote
