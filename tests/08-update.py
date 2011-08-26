
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
    foo = builder.update('languages').set({
        'name': 'Japanese',
        'lang': 'ja'
    }).where(pgsql.Where('id').equals('10')).execute()
    
    builder.commit()
    
    """
    
    There needs to be a way to specify whether a value in a dictionary
    should be treated as a value or a field, as during updates, one might
    want to be able to perform something such as:
    
    UPDATE mytable SET /field1/ = /field2/ WHERE id = 100
    
    Perhaps Something like:
        builder.update('mytable').set({
            'field1': ['field2', doze.FIELD]
        }).where(pgsql.Where('id').equals(100))
    
    It would also be useful if something similar could be done for selects.
    
    """
        
    sys.exit(0)

if __name__ == '__main__':
    main()
