
from doze import *
import doze.backend.generic as generic

def connection_is_open(conn):
    return True

def connection_is_ready(conn):
    return True

class QueryResultSqlite(generic.QueryResult):
    def __del__(self):
        self.cursor.close()
    
    def __iter__(self):
        res = self.cursor.fetchone()
        if res is None:
            raise StopIteration()
        
        if self.cursorDescrInit == False:
            self.initCursorDescr()
    
        while not res == None:
            if self.fetch.__name__ == 'dict':
                yield dict(zip(self.labels, res))
            else:
                yield res
            res = self.cursor.fetchone()
        
        if self.destroy == True:
            self.cursor.close()

class BaseClause(generic.BaseClause):
    # See: http://www.sqlite.org/datatype3.html
    booleanTypes = ['1', '0']

    # Escape pattern
    escapePattern = '?'

class Where(generic.Where, BaseClause): pass
class Join(generic.Join, BaseClause): pass
class QueryResult(generic.QueryResult): pass

class Builder(generic.Builder, BaseClause):
    @ExceptionWrapper
    def asObject(self, fetchall = False, server = False, destroy = True, fetch = dict):
        cursor = self.cursor(server)
        return QueryResultSqlite(cursor, destroy, fetch)
