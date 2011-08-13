
import sys
sys.path.append('../doze')

import doze

def main():
    ctx = doze.TableContext(['mainTable', 'a'], ['joinTable', 'b'])
    origin = ctx.origin()
    
    print('Origin: ' + origin[0] + ', ' + origin[1])
    for i in ctx.joins():
        print('Join: ' + i[0] + ', ' + i[1])
    
    sys.exit(0)

if __name__ == '__main__':
    main()
