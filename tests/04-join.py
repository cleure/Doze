
import sys
sys.path.append('../doze')

import doze

def main():
    ctx = doze.TableContext(['table1', 'a'], ['table2', 'b'])

    join = doze.Join(table='table2', context=ctx)\
        .filter(doze.Where('refid').equals('id', kind=doze.TYPE_FIELD))
    
    join, escape = join.sql()
    
    print join
    print escape
    
    sys.exit(0)

if __name__ == '__main__':
    main()
