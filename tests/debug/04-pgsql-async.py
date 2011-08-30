
"""
* 
* Test showing usage of async connections with PostgreSQL.
* 
"""

import sys
sys.path.append('../doze')

import psycopg2
import time
import doze
import doze.backend.pgsql as pgsql

def connect():
    return psycopg2.connect(
        host='127.0.0.1',
        user='testuser',
        database='testdb',
        port='5432',
        password='8cp1FEqp',
        async=1)

def main():
    db = connect()
    builder = pgsql.Builder(db)
    builder.select('*').from_('metatags')
    
    # Wait for connection to become established
    while not builder.isReady():
        time.sleep(0.01)
    
    # Get query object, and wait for results to become available
    results = builder.asObject()
    while not results.isReady():
        time.sleep(0.01)
    
    for i in results:
        print i
        break
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
