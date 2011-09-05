
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
        
        if server == True:
            cursor = self.db.cursor(MySQLdb.cursors.SSCursor)
        else:
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
