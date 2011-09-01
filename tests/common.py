
import sys
sys.path.append('../doze')

import doze
import doze.backend.pgsql as pgsql
import doze.backend.mysql as mysql
import doze.backend.generic as generic

class DozeTestFramework(object):
    def __init__(self):
        self.methodTests = []
    
    def __methodTest(self, obj, funcName, tests):
        failMsg = ('Failed: %s()\n'
            + '\tParameters: %s\n'
            + '\tExpected: %s\n'
            + '\tReturned: %s')
        
        func = getattr(obj, funcName)
        status = True
        
        for test in tests:
            result = func(*test[0])
            if result != test[1]:
                print failMsg % (func.__name__, str(test[0]), test[1], result)
                status = False
        
        return status
    
    def registerMethodTest(self, obj, funcName, tests):
        self.methodTests.append([obj, funcName, tests])
    
    def doMethodTests(self):
        for obj, funcName, tests in self.methodTests:
            try:
                res = self.__methodTest(obj, funcName, tests)
                if res == False:
                    print '%s() on %s: Failed' % (funcName, obj.__class__)
                else:
                    print '%s() on %s: Passed' % (funcName, obj.__class__)
            except Exception as e:
                print '%s() on %s: Failed with exception "%s"' % (
                    funcName, obj.__class__, e.__class__)
                
                raise e

if __name__ == '__main__':
    dtf = DozeTestFramework()

    pgsql_Builder = pgsql.Builder()
    mysql_Builder = mysql.Builder()
    generic_Builder = generic.Builder()
    
    # isSqlFunction method tests
    dtf.registerMethodTest(mysql_Builder, 'isSqlFunction', [
        [['CURRENT_TIMESTAMP'], True],
        [['NOW()'], True],
        [['function()'], True],
        [['\'value\''], False],
        [['\'field()\''], False],
        [['`field()`'], False]
    ])
    
    dtf.registerMethodTest(pgsql_Builder, 'isSqlFunction', [
        [['CURRENT_TIMESTAMP'], True],
        [['NOW()'], True],
        [['function()'], True],
        [['\'value\''], False],
        [['\'field()\''], False],
        [['"field()"'], False]
    ])
    
    # fieldIsAliased method tests
    dtf.registerMethodTest(mysql_Builder, 'fieldIsAliased', [
        [['table.field'], True],
        [['field'], False],
        [['\'value\''], False],
        [['`table`.`field`'], True],
        [['table.'], False],
        [['`with` . `spaces`'], True]
    ])
    
    dtf.registerMethodTest(pgsql_Builder, 'fieldIsAliased', [
        [['table.field'], True],
        [['field'], False],
        [['\'value\''], False],
        [['"table"."field"'], True],
        [['table.'], False],
        [['"with" . "spaces"'], True]
    ])
    
    # isSelectQuery method tests
    dtf.registerMethodTest(mysql_Builder, 'isSelectQuery', [
        [['table.field'], False],
        [['SELECT NOW()'], True],
        [['SELECT * FROM table'], True],
        [['\'SELECT NOW()\''], False]
    ])
    
    dtf.registerMethodTest(pgsql_Builder, 'isSelectQuery', [
        [['table.field'], False],
        [['SELECT NOW()'], True],
        [['SELECT * FROM table'], True],
        [['\'SELECT NOW()\''], False]
    ])
    
    # fieldNeedsQuotes method tests
    dtf.registerMethodTest(mysql_Builder, 'fieldNeedsQuoted', [
        [['table'], False],
        [['"table"'], True],
        [['`table`'], False],
        [['table!$'], True]
    ])
    
    dtf.registerMethodTest(pgsql_Builder, 'fieldNeedsQuoted', [
        [['table'], False],
        [['"table"'], False],
        [['`table`'], True],
        [['table!$'], True]
    ])
    
    # quoteField method tests
    dtf.registerMethodTest(mysql_Builder, 'quoteField', [
        [['table'], '`table`'],
        [['`table`'], '```table```']
    ])
    
    dtf.registerMethodTest(pgsql_Builder, 'quoteField', [
        [['table'], '"table"'],
        [['"table"'], '"""table"""']
    ])
    
    # isValue method tests
    dtf.registerMethodTest(mysql_Builder, 'isValue', [
        [['field'], False],
        [['\'value\''], True],
        [['`field`'], False]
    ])
    
    dtf.registerMethodTest(pgsql_Builder, 'isValue', [
        [['field'], False],
        [['\'value\''], True],
        [['"field"'], False]
    ])
    
    dtf.doMethodTests()
