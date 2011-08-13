
import sys
sys.path.append('../doze')

import doze

def main():
    where = doze.Where('kind')\
        .equals('image')\
        .and_('category_id')\
        .equals(15)
    
    clause, escape = where.sql()
    
    print clause
    print escape
    sys.exit(0)

if __name__ == '__main__':
    main()
