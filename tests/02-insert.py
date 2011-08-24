
import sys
sys.path.append('../doze')

import psycopg2
import doze
import doze.backend.pgsql as pgsql

def connect():
    return psycopg2.connect(
        host='127.0.0.1',
        user='testuser',
        database='testdb',
        port='5432',
        password='8cp1FEqp')

def main():
    db = connect()
    
    builder = pgsql.Builder(db)
    builder.insertInto('languages').values({
        'name': 'German',
        'lang': 'de'
    }).execute()
    
    builder.commit()
    
    """
    
    API Plan: 
        * Ditch Multi-Insert for now
        * Implement batched queries later
        * onError parameter, for external error handling:
        
        def traperror(e):
            pass
        
        builder = pgsql.Builder(db, onError=traperror)
    
    """
    
    """
    # Insert syntax
    builder = pgsql.Builder()
    builder.insertInto('languages').values({
        'name': 'German',
        'lang': 'de'
    })
    
    builder.insertInto('posts').values({
        'title': 'My Title',
        'created': doze.function('NOW()')
    })
    
    API For Insert:
    
    insertInto(table).values(dict or list)
    insertInto(table).values(dict1, dict2, dict3)
    
    Eg:
    
    insertInto('mytable').values(
        {'name': 'Test 1'},
        {'name': 'Test 2'},
        {'name': 'Test 3'}
    )
    
    """
    
    sys.exit(0)

if __name__ == '__main__':
    main()
