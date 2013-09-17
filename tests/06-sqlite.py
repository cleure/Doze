import os, sys
sys.path.append('../doze')

import doze
import doze.backend.sqlite as dz_sqlite
import sqlite3

DB = None
CREATE_TEST_TABLE = (
    "CREATE TABLE entropy ( "
        "data CHAR(8), "
        "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP "
    ");"
)

def get_entropy(length):
    with open('/dev/urandom', 'r') as fp:
        bytes = fp.read(int(length/2))
        return bytes.encode('hex')
    return None

def get_db():
    global DB
    
    if DB is None:
        DB = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
    return DB

def teardown():
    global DB
    if DB is not None:
        DB.close()

def buildup():
    db = get_db()
    bd = dz_sqlite.Builder(db)
    
    cursor = db.cursor()
    cursor.execute(CREATE_TEST_TABLE)
    cursor.close()

    entropy = []
    for i in range(32):
        ent = get_entropy(8)
        entropy.append(ent)
        bd.insertInto('entropy').values({'data': ent}).execute()

    db.commit()
    return entropy

def main():
    teardown()
    db = get_db()
    bd = dz_sqlite.Builder(db)
    
    rows = buildup()
    
    for i in rows:
        r, = [a for a in bd.select('*').from_('entropy').where(
            dz_sqlite.Where('data').equals(i)).asObject()]

        assert r['data'] == i
        r = bd.deleteFrom('entropy').where(
            dz_sqlite.Where('data').equals(i)).execute()
    
    db.commit()
    sys.exit(0)

if __name__ == '__main__':
    main()
