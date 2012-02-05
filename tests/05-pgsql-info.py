
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

    """
    # Databases
    for name in pgsql_relations.databases(db):
        print name
    
    # Tables
    for name, obj in dbdef.tables:
        print (name, str(obj))
    
    # Columns
    for name, obj in dbdef.tables.advertisements.columns:
        print (str(obj), obj.type, obj.internaltype, obj.length)
    
    # Indexes
    for name, obj in dbdef.tables.clubhouse_members.indexes:
        print (str(obj), obj.columns, obj.is_unique, obj.is_primary)
    
    # Sequences
    for i in dbdef.sequences:
        print i
    
    # View definitions
    for name, obj in dbdef.views:
        print obj.definition
    
    # Column lists work on views :)
    for name, obj in dbdef.views.dpages_view.columns:
        print (name, obj.table, obj.internaltype)
    
    """
    
    for i in dbdef.tables:
        print i
    
    dbdef.set_search_path('information_schema')
    for i in dbdef.tables:
        print i
    
    for name, obj in dbdef.schemas.pg_catalog.views:
        print name
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
