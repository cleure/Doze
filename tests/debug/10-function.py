
import sys
sys.path.append('../doze')

import psycopg2
import doze
import doze.backend.pgsql as pgsql
import doze.backend.generic as generic

def connect():
    return psycopg2.connect(
        host='127.0.0.1',
        user='testuser',
        database='testdb',
        port='5432',
        password='8cp1FEqp')

def main():
    builder = pgsql.Builder()

    builder.select('*').from_('mytable')\
        .where(pgsql.Where('id').isIn('SELECT refid FROM othertable'))
    
    print builder.sql()
    
    sys.exit(0)

if __name__ == '__main__':
    main()
