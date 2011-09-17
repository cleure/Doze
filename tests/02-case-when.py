
"""
* 
* Test showing that CASE WHEN breaks in Doze.
* 
"""

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
    builder = pgsql.Builder(db = db)
    
    builder.select('CASE WHEN id < 5 THEN \'lt\' ELSE \'gt\' END AS foo')\
        .from_('languages l').join(
            pgsql.Join('foo f').where(pgsql.Where('refid').equals('id')))
    
    print builder.sql()
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
