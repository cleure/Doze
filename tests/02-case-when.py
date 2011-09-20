
"""
* 
* Test showing that CASE WHEN breaks in Doze.
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

"""
    *   equals                  - Equals
    *   notEquals               - Not equals
    *   gt                      - Greater than (>)
    *   gte                     - Greater than or equals (>=)
    *   lt                      - Less than (<)
    *   lte                     - Less than or equals (<=)
    *   isIn                    - Is in [list]
    *   notIn                   - Is not in [list]
    *   isNull                  - Is null
    *   isNotNull               - Is not null
    *   isEmpty                 - Is empty
    *   isNotEmpty              - Is not empty
    *   between (min, max)      - BETWEEN
    *   like                    - LIKE
    *   ilike                   - ILIKE
"""

class Assignment:
    defaultExpression = '%key1 %symbol %key2'
    assignements = {
        'equals': '=',
        'notEquals': '!=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<=',
        'isIn': {
            'expression': '%key1 %symbol (%key2)',
            'symbol': 'IN'
        },
        'isNotIn': {
            'expression': '%key1 %symbol (%key2)',
            'symbol': 'NOT IN'
        },
        'isNull': {
            'expression': '%key1 %symbol',
            'symbol': 'IS NULL'
        },
        'isNotIn': {
            'expression': '%key1 %symbol',
            'symbol': 'IS NOT NULL'
        },
        'isEmpty': {
            'expression': '%key1 %symbol %valueQuote%valueQuote',
            'symbol': '='
        },
        'isNotEmpty': {
            'expression': '%key1 %symbol %valueQuote%valueQuote',
            'symbol': '!='
        },
        'between': {
            'expression': '%key1 %symbol %key2 '
        }
    }

def main():
    db = connect()
    builder = pgsql.Builder(db = db)
    
    builder.select('CASE WHEN id < 5 THEN \'lt\' ELSE \'gt\' END AS foo')\
        .from_('languages l').join(
            pgsql.Join('foo f').where(pgsql.Where('refid').equals('id', kind=doze.FIELD)))
    
    print builder.sql()
    print builder.select("'com,ma', 'bar'").from_('mytable a').sql()
    
    print builder.splitFields("'com,ma', 'bar', field, bar  , 2, 4 ,5 ")
    
    db.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
