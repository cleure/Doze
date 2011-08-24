
"""
* 
* Test demonstrating why functions need to be detected. The following will
* trigger a bug, in which a function is used in a JOIN query, and gets
* aliased, because Doze currently doesn't handle function detection.
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
    builder = pgsql.Builder(db)
    builder.select('a.id, NOW(), a.name, b.created')\
        .from_(['mytable', 'a']).join(pgsql.Join(['table2', 'b']).where(
            pgsql.Where('refid').equals('id')))
    
    # Will output:
    # SELECT a.id, a.NOW(), a.name, b.created...
    #
    # Notice that it says "a.NOW()". That needs to be fixed.
    print builder.sql()[0]
        
    sys.exit(0)

if __name__ == '__main__':
    main()
