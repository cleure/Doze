
import psycopg2
import random

from doze import *
import generic as generic

def connection_is_open(conn):
    """
    Test if connection is open, and return True if it is.
    
    Sometimes, a server process will silently fail, such as when kill -KILL
    is used, or when it's restarted. The problem is, psycopg2 doesn't detect
    that the backend has died, and will happily try to execute a query on
    the connection, raising an exception. This function is meant to eliminate
    that risk.
    """
    try:
        conn.poll()
        return True
    except:
        pass
    return False

def connection_is_ready(conn):
    """
    Returns True when connection is ready for commands.
    """
    try:
        if conn.poll() == psycopg2.extensions.POLL_OK:
            return True
    except:
        pass
    return False

class Where(generic.Where):
    pass

class Join(generic.Join):
    pass

class QueryResult(generic.QueryResult):
    def isReady(self):
        if self.cursor is None:
            return False
        return connection_is_ready(self.cursor.connection)

class Builder(generic.Builder):
    def __init__(self, db = None, onError = None):
        super(Builder, self).__init__(db, onError)

    @ExceptionWrapper
    def cursor(self, server = False):
        """
        **
        * Execute query and return cursor. If server == True, return a
        * server-side cursor.
        *
        * @param    server  bool or str. If server is true, a random cursor
        *                   name is generated. If its a string, the string
        *                   is used as the cursor name.
        * @return   object
        **
        """
        if self.db == None:
            return None
        
        query, escape = self.sql()
        
        if type(server) == str and len(server) > 0:
            # Create a cursor with "server" as name
            cursor = self.db.cursor(server)
        elif server == True:
            # For PostgreSQL, you have to create a named cursor in order for it
            # to be server-side. So this next block attempts to generate a random
            # number between 100 and 1,000,000, to use as a cursor name. It will
            # attempt 20 tries before raising an error.
        
            def getRandNum():
                return random.randint(100, 1000000)
        
            tmpcursor = self.db.cursor()
            rand = getRandNum()
            max = 20
            tries = 1
            
            while True:
                cursorName = 'doze_cursor_' + str(rand)
                tmpcursor.execute('SELECT name FROM pg_catalog.pg_cursors WHERE name = %s',
                    [cursorName])
                
                if tmpcursor.fetchone() == None:
                    break
                elif tries >= max:
                    raise DozeError('Unable to generate unique cursor name.'\
                        + ' Max tries exhausted (' + str(max) + ')')
                
                rand = getRandNum()
                tries += 1
            
            tmpcursor.close()
            cursor = self.db.cursor(cursorName)
        else:
            cursor = self.db.cursor()
        
        cursor.execute(query, escape)
        return cursor
    
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
    
    def isReady(self):
        if self.db is None:
            return False
        return connection_is_ready(self.db)
    
    def isConnected(self):
        if self.db is None:
            return False
        return connection_is_open(self.db)
