
import psycopg2

class Builder(generic.Builder):
    def cursor(self, server = False):
        if self.db == None:
            return None
        
        query, escape = self.sql()
        
        if server == True:
            cursor = self.db.cursor('cursor_unique_name')
        else:
            cursor = self.db.cursor()
        
        cursor.execute(query, escape)
        return cursor
