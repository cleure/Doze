
import sys
sys.path.append('../doze')

import psycopg2
import doze
import doze.backend.pgsql as pgsql
import doze.backend.mysql as mysql
import doze.backend.generic as generic

class DozeTestFramework(object):
    def __init__(self):
        self.backends = [pgsql, mysql, generic]

    def __registerFailure(self, func): pass

    def sqlFunctionMethod(self):
        """ Test for the isSqlFunction method. """
        tests = {
            'function()': True,
            'CURRENT_TIMESTAMP': True,
            'NOW()': True,
            '\'\function()\'': False,
            'field': False,
            '\'value\'': False,
            '`table`.`field()`': False
        }
        
        failMsg = ('Failed: isSqlFunction("%s")\n'
            + '\tOn object: "%s"\n'
            + '\tExpected: "%s"\n'
            + '\tReturned: "%s"')
        
        status = True
        for backend in self.backends:
            builder = backend.Builder()
            
            for test, expected in tests.items():
                result = builder.isSqlFunction(test)
                if result != expected:
                    print failMsg % (test, builder, expected, result)
                    status = False
        
        return status

    def fieldIsAliasedMethod(self):
        """ Test for fieldIsAliased method. """
        
        tests = {
            'table.field': True,
            '"pgsql table"."pgsql field"': True,
            'field': False,
            '\'value\'': False,
            '`mysql table`.`mysql field`': True,
            'field.': False
        }
        
        failMsg = ('Failed fieldIsAliased("%s")\n'
            + '\tOn object: "%s"\n'
            + '\tExpected: "%s"\n'
            + '\tReturned: "%s"')
        
        status = True
        for backend in self.backends:
            builder = backend.Builder()
            for test, expected in tests.items():
                result = builder.fieldIsAliased(test)
                if result != expected:
                    print failMsg % (test, builder, expected, result)
                    status = False
        
        return status

    def isSelectQueryMethod(self):
        """ Test for isSelectQuery method. """
        
        tests = {
            'SELECT 1': True,
            'SELECT * FROM mytable': True,
            '\'value\'': False,
            '"pg table"."pg field"': False,
            '\'SELECT foo\'': False,
            '"SELECT pgsql"': False,
            '`SELECT mysql`': False
        }
        
        failMsg = ('Failed isSelectQuery("%s")\n'
            + '\tOn object: "%s"\n'
            + '\tExpected: "%s"\n'
            + '\tReturned: "%s"')
        
        status = True
        for backend in self.backends:
            builder = backend.Builder()
            for test, expected in tests.items():
                result = builder.isSelectQuery(test)
                if result != expected:
                    print failMsg % (test, builder, expected, result)
                    status = False
        
        return status

    def fieldNeedsQuotedMethod(self):
        """ Test for fieldNeedsQuoted method. """
        
        tests = [
            [pgsql, {
                'field': False,
                'contains spaces': True,
                'contains " quote': True,
                'contains \' quote': True,
                'has_underscore': False,
                '`mysql`': True
            }]
        ]
        
        failMsg = ('Failed isSelectQuery("%s")\n'
            + '\tOn object: "%s"\n'
            + '\tExpected: "%s"\n'
            + '\tReturned: "%s"')
        
        status = True
        
        for i in tests:
            backend = i[0]
            
            builder = backend.Builder()
            for test, expected in i[1].items():
                result = builder.fieldNeedsQuoted(test)
                if result != expected:
                    print failMsg % (test, builder, expected, result)
                    status = False
        
        return status

    def go(self):
        for name, item in self.__class__.__dict__.items():
            # Skip ourself
            if name == 'go': continue
            
            # Skip anything beginning with '_'
            if name[0] == '_': continue
            
            # Skip anything that isn't callable
            if not hasattr(item, '__call__'): continue
            
            # Run test
            try:
                res = item(self)
                if res == False:
                    print '%s(): failed' % (item.__name__)
            except:
                print '%s(): failed with exception' % (item.__name__)
                raise
            

if __name__ == '__main__':
    dtf = DozeTestFramework()
    dtf.go()
    
    builder = pgsql.Builder()
