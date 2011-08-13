
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
        
        # Server side cursors require a unique name
        # Hack to workaround the inability to create such cursors
        # without a randomly generated name.
        self.ssCursors = []

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
            rand = random.randint(10000, 100000)
            max = 0
            while rand in self.ssCursors:
                if max >= 30:
                    raise DozeError('Unable to generate unique cursor name. Max tries exhausted')
                rand = random.randint(10000, 100000)
                max += 1
            
            self.ssCursors.append(rand)
            cursor = self.db.cursor('doze_cursor_' + str(rand))
        else:
            cursor = self.db.cursor()
        
        cursor.execute(query, escape)
        return cursor
