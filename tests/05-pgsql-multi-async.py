
"""
* 
* Test showing usage of multiple async connections with PostgreSQL.
* 
"""

import sys
sys.path.append('../doze')

import psycopg2
import time, random
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
    # Create 10 database connections
    connections = []
    builders = []
    results = []
    
    for i in range(0, 10):
        connections.append(connect())
    
    # Create queries
    for db in connections:
        builder = pgsql.Builder(db).select('*').from_('metatags')
        builders.append(builder)
    
    connections[3].close()
    
    # Wait for connections and results to become available
    done = False
    while not done:
        # Done when no more operations are left
        if len(builders) == 0 and len(results) == 0:
            done = True
            continue
        
        for builder in builders:
            if builder.isReady():
                results.append(builder.asObject())
                builders.remove(builder)
        
        for result in results:
            if result.isReady():
                print result.next()
                results.remove(result)
        
        time.sleep(0.01)
        
    
    for i in connections:
        i.close()
    
    sys.exit(0)

if __name__ == '__main__':
    main()
