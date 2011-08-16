
import os, sys, psycopg2
sys.path.append('../doze')

import doze

def connect():
    return psycopg2.connect(
        host='127.0.0.1',
        user='testuser',
        database='testdb',
        port='5432',
        password='8cp1FEqp')

def main():
    db = connect()
    
    builder = doze.Builder(db = db)
    builder.select('id, site, name, b.title, c.lang')\
        .from_(['givaways', 'a'])\
        .join(doze.Join(['subsites', 'b'],
            where=doze.Where('site').equals('id', kind=doze.FIELD)))\
        .join(doze.Join(['languages', 'c'],
            where=doze.Where('lang_id').equals('id', kind=doze.FIELD)))
    
    for i in doze.QueryResult(builder.cursor()):
        print i['name'] + "\t" + i['lang']
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
