
"""
* 
* Test showing basic usage of Doze on PostgreSQL.
* 
"""

import sys
sys.path.append('../doze')
import doze.backend.pgsql as pgsql
import doze.backend.pgsql.relations as pgsql_relations

def connect():
    return pgsql.connect(
        host='127.0.0.1',
        user='testuser',
        database='testdb',
        port='5432',
        password='8cp1FEqp')

def main():
    db = connect()
    dbdef = pgsql_relations.Database.get(db)
    print dbdef

    for i in dbdef.tables.advertisements.columns:
        print (i[0], i[1].internaltype, i[1].length)
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
