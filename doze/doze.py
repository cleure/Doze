
# -*- coding: utf-8 -*-

TYPE_VALUE = 1
TYPE_FIELD = 2

JOIN_INNER = 1
JOIN_LEFT = 2
JOIN_RIGHT = 3

class DozeError(Exception):
    def __init__(self, msg):
        self.value = msg
    def __str__(self):
        return repr(self.value)

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
    def __init__(self):
        self.caller = None

    def setTableContext(self, ctx):
        """ Set the TableContext, for table aliases, etc """
        self.tableContext = ctx
    
    def setCaller(self, caller):
        """ Sets a reference the previous caller """
        self.caller = caller
    
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
        """ Check if field is aliased """
        
        for i in range(0, len(field)):
            if field[i] == '.':
                return True
        
        return False
        
    
    def sql(self):
        """ Not Implemented """
        raise DozeError('sql method not implemented')
