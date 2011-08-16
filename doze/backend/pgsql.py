
import psycopg2
import random

from doze import DozeError, TableContext
import generic as generic

class Where(generic.Where):
    pass

class Join(generic.Join):
    pass

class QueryResult(generic.QueryResult):
    pass

class Builder(generic.Builder):
    def __init__(self, db = None):
        super(Builder, self).__init__(db)

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
