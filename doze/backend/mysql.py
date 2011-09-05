
import MySQLdb

from doze import *
import generic as generic

class BaseClause(generic.BaseClause):
    """
    MySQL specific implementation of the BaseClause class. Currently,
    this extends BaseClause, and overrides the class variables for
    booleanTypes, fieldQuote, and valueQuote.
    """
    
    # Valid boolean types, in lowercase
    # See: http://dev.mysql.com/doc/refman/5.0/en/boolean-values.html
    booleanTypes = [
        'true',
        '1',
        'false',
        '0'
    ]
    
    # Field quote
    fieldQuote = '`'
    
    # Value quote
    valueQuote = '\''

class Where(generic.Where, BaseClause): pass
class Join(generic.Join, BaseClause): pass

class QueryResult(generic.QueryResult):
    def initCursorDescr(self):
        """ Init cursor description, for MySQL. """
        for i in self.cursor.description:
            self.labels.append(i[0])
        self.cursorDescrInit = True

class Builder(generic.Builder, BaseClause):
    """
    MySQL specific implementation of the Builder class.
    """

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
        
        if server == True:
            cursor = self.db.cursor(MySQLdb.cursors.SSCursor)
        else:
            cursor = self.db.cursor()
        cursor.execute(query, escape)
        return cursor
