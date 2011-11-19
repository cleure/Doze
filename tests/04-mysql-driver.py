
"""
* 
* Test showing basic usage of Doze on PostgreSQL.
* 
"""

import sys
sys.path.append('../doze')

import doze
import doze.backend.mysql as mysql

def connect():
    return mysql.connect(
        host='127.0.0.1',
        user='testuser',
        database='cleure',
        password='8cp1FEqp',
        port=3306)

def main():
    db = connect()
    builder = mysql.Builder(db=db)
    builder.select('id, name').from_('timezones')
    res = builder.asObject(server=True)
    print res.cursor
    
    while True:
        row = res.next()
        if row is None:
            break
        print row
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
