
import sys
sys.path.append('../doze')

import doze

def main():
    builder = doze.Builder()
    builder.select('a.id, name, something')\
        .from_(['mytable', 'a'])
    
    builder.join(doze.Join(['table2', 'b'])\
        .where(doze.Where('ref_id').equals('id', kind=doze.TYPE_FIELD)))
    
    builder.where(doze.Where('id').gt(25).and_('id').lt(100))
    
    sql = builder.sql()
    print sql
    
    builder.select('COUNT(*), member_id')\
        .from_('posts')\
        .group('member_id')\
        .having(doze.Where('COUNT(*)').gt(10))\
        .order('COUNT(*)')\
        .limit(100)
    
    sql = builder.sql()
    print sql
    
    sys.exit(0)

if __name__ == '__main__':
    main()
