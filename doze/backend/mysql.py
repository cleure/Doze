
try:
    import PyMySQL as MySQLdb
    MYSQL_DRIVER = 'pymysql'
except:
    import MySQLdb
    MYSQL_DRIVER = 'mysqldb'

from doze import *
import generic as generic

def connection_is_open(conn):
    """
    Test if connection is open, and return True if it is.
    
    MySQL specific version, checks conn.open for connection status.
    """
    if conn.open == 1:
        return True
    return False

def connection_is_ready(conn):
    """
    Dummy function that always returns True, as long as the connection
    is still open. As far as I can tell, MySQLdb has no support for
    asynchronous operations.
    """
    return connection_is_open(conn)

def connect(host='',
            user='',
            password='',
            database='',
            port=3306,
            unix_socket=''):
    
    """
    Database connection wrapper, to make connection parameters between
    different database backends consistent.
    
    FIXME: Handle MySQL specific parameters:
        http://mysql-python.sourceforge.net/MySQLdb.html#mysqldb
    
    """
    
    return MySQLdb.connect(
        host=host,
        user=user,
        passwd=password,
        db=database,
        port=port,
        unix_socket=unix_socket)

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
    
    # Search quotes
    searchQuotes = '\'`"'

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
    def with_(self, name, recursive=False):
        """ MySQL doesn't support Common Table Expressions :( """
        raise NotSupported('MySQL doesn\'t support Common Table Expressions :(')

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
