
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
import time

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
    
    subsites_complete = pgsql.Builder()\
        .select('id, domain').from_('subsites')\
        .union(pgsql.Builder()\
            .select('subsite, domain').from_('subsite_aliases'))
    
    builder.with_('a').as_(subsites_complete)\
        .select('*').from_('a').where(pgsql.Where('id').equals(1))
    
    print builder.sql()
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
