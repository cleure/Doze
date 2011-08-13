
import sys
sys.path.append('../doze')

import doze

def main():
    where = doze.Where('id')\
        .isIn([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    clause, escape = where.sql()
    
    print clause
    print escape
    sys.exit(0)

if __name__ == '__main__':
    main()
