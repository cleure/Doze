
"""
* 
* Test showing basic usage of Doze on PostgreSQL.
* 
"""

import sys
sys.path.append('../doze')

import doze
import doze.backend.mysql as mysql

def main():
    print mysql.MYSQL_DRIVER
    
    sys.exit(0)

if __name__ == '__main__':
    main()
