
"""
* 
* Test demonstrating why functions need to be detected. The following will
* trigger a bug, in which a function is used in a JOIN query, and gets
* aliased, because Doze currently doesn't handle function detection.
* 
"""

import sys
sys.path.append('../doze')

import psycopg2
import doze
import doze.backend.pgsql as pgsql

def connect():
    return psycopg2.connect(
        host='127.0.0.1',
        user='testuser',
        database='testdb',
        port='5432',
        password='8cp1FEqp')

def isSqlFunction(param):
    """
    Check if supplied argument is a SQL function. Note that this doesn't
    detect all SQL functions. For instance, "SELECT CURRENT_DATE" will fail.
    Further checking is required to filter out special cases such as those.
    """
    
    # Preliminary checks
    if '(' not in param or ')' not in param:
        return False
    
    plen = len(param)
    
    # These will be used to detect the open and close parentheses
    openParan = False
    closeParan = False
    
    # State, for if we're currently in a quoted or double-quoted string
    inQuote = False
    inDoubleQuote = False
    
    # Fairly basic algorithm. It detects the state of quotes, and uses
    # that information to determine whether or not openParan / closeParan
    # should be changed when parentheses are detected.
    for i in range(0, plen):
        if param[i] == '\'':
            if inDoubleQuote == False:
                if inQuote == True:
                    inQuote = False
                else:
                    inQuote = True
    
        if param[i] == '"':
            if inQuote == False:
                if inDoubleQuote == True:
                    inDoubleQuote = False
                else:
                    inDoubleQuote = True
    
        if param[i] == '(' and not inDoubleQuote and not inQuote:
            openParan = True
        
        if param[i] == ')' and not inDoubleQuote and not inQuote:
            if openParan == True:
                # Found both parentheses. Can safely break.
                closeParan = True
                break
    
    return openParan and closeParan

def main():
    db = None
    builder = pgsql.Builder(db)
    builder.select('a.id, NOW(), a.name, b.created')\
        .from_(['mytable', 'a']).join(pgsql.Join(['table2', 'b']).where(
            pgsql.Where('refid').equals('id')))
    
    # Will output:
    # SELECT a.id, a.NOW(), a.name, b.created...
    #
    # Notice that it says "a.NOW()". That needs to be fixed.
    print 'This will be wrong:'
    print builder.sql()[0]
    print ''
    
    # Solution: Implement isSqlFunction()
    functions = [
        'NOW()',
        'NOW ()',
        'NOW( )',
        'CHAR_LENGTH(\'My String\')',
        'name',
        'id',
        'somefield',
        '\'confusing strings with functions()\'',
        '\"try and confuse the \'crap()\' out of you()"'
    ]
    
    print 'Solution: Implement isSqlFunction()'
    for i in functions:
        print i + ': ' + str(isSqlFunction(i))
    
    sys.exit(0)

if __name__ == '__main__':
    main()
