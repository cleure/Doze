
"""
* 
* Test showing basic usage of Doze on PostgreSQL and MySQL.
* 
"""

import sys
sys.path.append('../doze')

import psycopg2, MySQLdb
import doze
import doze.backend.pgsql as pgsql
import doze.backend.mysql as mysql
import doze.backend.generic as generic

def connect():
    return pgsql.connect(
        host='127.0.0.1',
        user='testuser',
        database='testdb',
        port='5432',
        password='8cp1FEqp')

def my_connect():
    return mysql.connect(
        host='127.0.0.1',
        user='testuser',
        db='cleure',
        passwd='8cp1FEqp',
        port=3306)

def main():
    db = connect()
    builder = pgsql.Builder(db=db)
    res = builder.select('*').from_('subsites').where(
        pgsql.Where('id').isIn([1, 2, 3, 4])).asObject()
    
    while True:
        row = res.next()
        if row is None: break
        print row
    
    sys.exit(0)

if __name__ == '__main__':
    main()
