
"""
* 
* Test showing usage of connection_is_open() and connection_is_ready()
* with PostgreSQL.
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
        password='8cp1FEqp')

def main():
    db = connect()
    pid = db.get_backend_pid()
    print "Run 'sudo kill -KILL " + str(pid) + "' on server to test functions"

    time.sleep(10)
    
    print 'connection_is_ready\t' + str(pgsql.connection_is_ready(db))
    print 'connection_is_open\t' + str(pgsql.connection_is_open(db))
    
    if pgsql.connection_is_open(db):
        db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
