
"""
* 
* Test showing basic usage of Doze on PostgreSQL.
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
    builder.select('*').from_('languages')
    
    cursor = builder.cursor()
    row = cursor.fetchone()
    while row is not None:
        print row
        row = cursor.fetchone()
    
    cursor.close()
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
