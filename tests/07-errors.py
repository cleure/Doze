
"""
* 
* Test showing usage of user defined exception handling functions.
* 
"""

import sys
sys.path.append('../doze')

import psycopg2
import doze
import doze.backend.pgsql as pgsql

def MyExceptionHandler(ex):
    etype = type(ex)
    if etype == psycopg2.OperationalError:
        print 'Caught Operational Error'
    elif etype == psycopg2.ProgrammingError:
        print 'Caught Programming Error'
    else:
        raise ex

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
    builder = pgsql.Builder(db, onError=MyExceptionHandler)
    cursor = builder.select('*').from_('missing_table').cursor()
    
    sys.exit(0)

if __name__ == '__main__':
    main()
