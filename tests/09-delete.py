
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
    sql = builder.deleteFrom('languages').where(
        pgsql.Where('id').equals(10)).sql()
    
    print sql
    
    builder.isQuoted("'foobar'")
    
    sys.exit(0)

if __name__ == '__main__':
    main()
