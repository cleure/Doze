
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

    # Databases
    for name in pgsql_relations.databases(db):
        print name
    
    # Tables
    for name, obj in dbdef.tables:
        print (name, str(obj))
    
    # Columns
    for name, obj in dbdef.tables.advertisements.columns:
        print (str(obj), obj.internaltype, obj.length)
    
    for name, obj in dbdef.tables.clubhouse_members.indexes:
        print (str(obj), obj.columns, obj.is_unique, obj.is_primary)
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
