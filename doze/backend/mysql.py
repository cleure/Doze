
import MySQLdb

from doze import DozeError, TableContext
import generic as generic

class Where(generic.Where):
    pass

class Join(generic.Join):
    pass

class QueryResult(generic.QueryResult):
    pass

class Builder(generic.Builder):
    fieldQuote = '`'
    valueQuote = '\''

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
