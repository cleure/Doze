
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
    
    # Perform basic update
    builder.update('languages').set({
        'name': 'German',
        'lang': 'de'
    }).where(pgsql.Where('id').equals('10')).execute()
    
    builder.commit()
    
    # Make the parameter a field, instead of a value
    sql = builder.update('languages').set({
        'lang': ['lang', doze.FIELD]
    }).sql()
    
    print sql[0]
    
    sys.exit(0)

if __name__ == '__main__':
    main()
