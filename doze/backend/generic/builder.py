
from doze import *
from doze.backend.generic.base import *
from doze.backend.generic.where import *
from doze.backend.generic.join import *

class QueryResult(object):
    """ API Draft """
    def __init__(self, cursor = None, destroy = True, fetch = dict):
        self.setCursor(cursor, destroy)
        self.fetch = fetch
    
    def __len__(self):
        if self.cursor == None:
            return 0
        return self.cursor.rowcount
    
    def __iter__(self):
        res = self.cursor.fetchone()
        if res == None:
            yield None
        
        if self.cursorDescrInit == False:
            self.initCursorDescr()
    
        while not res == None:
            if self.fetch.__name__ == 'dict':
                yield dict(zip(self.labels, res))
            else:
                yield res
            res = self.cursor.fetchone()
        
        if self.destroy == True:
            self.cursor.close()
    
    def next(self):
        res = self.cursor.fetchone()
        if res == None:
            return None
        
        if self.cursorDescrInit == False:
            self.initCursorDescr()

        if self.fetch.__name__ == 'dict':
            return dict(zip(self.labels, res))
        return res
    
    def initCursorDescr(self):
        for i in self.cursor.description:
            self.labels.append(i.name)
        
        self.cursorDescrInit = True
    
    def setCursor(self, cursor = None, destroy = True):
        self.cursor = cursor
        self.destroy = destroy
        self.labels = []
        self.cursorDescrInit = False
    
    def isReady(self):
        return True

class Builder(BaseClause):
    """ Builder Class. """
    
    def __init__(self, db = None, onError = None):
        super(Builder, self).__init__()
        self.tableContext = TableContext()
        self.db = db
        self.onError = onError
        self.kind = None
    
    def reset(self):
        """
        Reset variables to undo everything cause by select: insertInto, update,
        deleteFrom, etc.
        
        This is typically called from one of those methods, but may be useful to
        call when recovering from an error.
        """
        
        if self.tableContext is not None:
            del self.tableContext
        
        # Common
        self.tableContext = TableContext()
        self.kind = None
        self.where_ = []
        
        # Select
        self.joins = []
        self.having_ = None
        self.group_ = None
        self.order_ = None
        self.limit_ = None
        self.union_ = []
        self.columns = []
        self.source = None
        
        # With
        self.with__ = []
        self.withQueries = []
        self.withIsRecursive = False
        self.withNotComplete = False
        
        # General AS
        self.as__ = None
        
        # Insert / Update
        self.values_ = []
        self.destination = None
    
    def select(self, columns):
        if self.kind == 'with' and self.withNotComplete:
            self.columns = columns
            return self
        
        self.reset()
        self.kind = 'select'
        self.columns = columns
        
        return self
    
    def from_(self, source):
        # Expand table
        if source.__class__.__name__ == 'Builder':
            if not source.kind == 'select':
                raise ValueError('Builder passed not initialized for SELECT')
        
        # Expand source from str
        if type(source) == str:
            source = self.expandTable(source)
        
        # Set self.tableContext
        if type(source) == list and len(source) > 1:
            self.tableContext[source[0]] = source[1]
        
        self.source = source
        return self
    
    def where(self, where):
        self.where_.append(where)
        return self
    
    def join(self, join):
        self.joins.append(join)
        self.childObjects.append(join)
        return self
    
    def having(self, having):
        self.having_ = having
        return self
    
    def group(self, group):
        self.group_ = group
        return self
    
    def order(self, order):
        self.order_ = order
        return self
    
    def limit(self, limit):
        self.limit_ = limit
        return self
    
    def union(self, query, all=False):
        self.union_.append((query, all))
        return self
    
    def normalizeColumns(self, columns):
        if type(columns) == str:
            columns = self.splitFields(columns)
        
        if len(self.tableContext) > 0:
            origin = self.tableContext.origin()
            if len(origin) > 1:
                origin = origin[1]
            else:
                origin = origin[0]
            
            cols = []
            for i in columns:
                # Append field table when any of the following is true:
                #   - Field is already aliased
                #   - Field is an SQL function
                #   - Field is a quoted value
                #
                # Otherwise, alias it with the origin table and append it.
                
                if (self.fieldIsAliased(i)
                or self.isSqlFunction(i)
                or self.isValue(i)
                or self.isSqlExpression(i)):
                    cols.append(i)
                else:
                    cols.append(self.fieldSeparator.join([origin, i]))
            
            columns = cols
        return columns
    
    def SqlForSelect(self):
        query = ['SELECT']
        escape = []
        
        self.preProcessSql()
        columns = self.normalizeColumns(self.columns)
        query.extend([', '.join(columns)])
        
        # FROM
        if self.source is not None:
            query.append('FROM')
            
            if self.source.__class__.__name__ == 'Builder':
                # Source table is a sub-query
                if self.source.as__ is not None:
                    as_ = self.source.as__
                else:
                    as_ = 'not_specified'
            
                # Get parent SQL
                (tmpquery, tmpescape) = self.source.sql()
                
                # Append query/escape
                query.append('(' + tmpquery + ') AS ' + as_)
                escape.extend(tmpescape)
            elif type(self.source) == list:
                query.append(' '.join(self.source))
            else:
                query.append(self.source)
        
        # JOIN
        if len(self.joins) > 0:
            for i in self.joins:
                tmpquery, tmpescape = i.sql()
                
                query.append(tmpquery)
                escape.extend(tmpescape)
        
        # WHERE
        if len(self.where_) > 0:
            query.append('WHERE')
            tmplist = []
            
            for i in self.where_:
                if not i.__class__.__name__ == 'Builder':
                    i.setTableContext(self.tableContext)
                
                tmpquery, tmpescape = i.sql()
                tmplist.append(tmpquery)
                escape.extend(tmpescape)
            
            query.append(' AND '.join(tmplist))
        
        # GROUP BY
        if not self.group_ == None:
            query.append('GROUP BY')
            query.append(', '.join(self.normalizeColumns(self.group_)))
        
        # HAVING
        if not self.having_ == None:
            query.append('HAVING')
            tmpquery, tmpescape = self.having_.sql()
            query.append(tmpquery)
            escape.extend(tmpescape)
        
        # UNION / UNION ALL
        if len(self.union_) > 0:
            for union, all in self.union_:
                if all:
                    query.append('UNION ALL')
                else:
                    query.append('UNION')
                
                if type(union) == str:
                    query.append(union)
                    continue
                
                (tmpquery, tmpescape) = union.sql()
                query.append(tmpquery)
                escape.extend(tmpescape)
        
        # ORDER
        if not self.order_ == None:
            query.append('ORDER BY')
            query.append(', '.join(self.normalizeColumns(self.order_)))
        
        # LIMIT
        # FIXME: Escape LIMIT???
        if not self.limit_ == None:
            query.append('LIMIT')
            query.append(str(self.limit_))
        
        return (' '.join(query), escape)
    
    @ExceptionWrapper
    def with_(self, name, recursive=False):
        """ Perform WITH / CTE query. """
    
        self.reset()
        self.kind = 'with'
        self.with__.append(name)
        self.withIsRecursive = recursive
        self.withNotComplete = True
        
        return self
    
    def as_(self, query, name = None):
        """ AS, used in WITH / CTE queries """
        
        if self.kind == 'with':
            self.withQueries.append(query)
            if name is not None:
                self.with__.append(name)
        else:
            self.as__ = query
        
        return self
    
    def SqlForWith(self):
        """ Generate (query, escape) from WITH / CTE query. """
    
        escape = []
        query = ['WITH']
        withQuery = []
        
        if self.withIsRecursive:
            query.append('RECURSIVE')
        
        for i in range(0, len(self.with__)):
            # FIXME: self.quoteField for table names / aliases???
        
            if type(self.withQueries[i]) == str:
                tmpquery = self.withQueries[i]
                tmpescape = []
            else:
                (tmpquery, tmpescape) = self.withQueries[i].sql()
            
            escape.extend(tmpescape)
            withQuery.append(' '.join([self.with__[i], 'AS', '(' + tmpquery + ')']))
        
        query.append(', '.join(withQuery))
        (tmpquery, tmpescape) = self.SqlForSelect()
        query.append(tmpquery)
        escape.extend(tmpescape)
        
        self.withNotComplete = False
        return (' '.join(query), escape)
    
    def insertInto(self, table):
        """ Perform INSERT on table. """
    
        self.reset()
        self.kind = 'insert'
        self.destination = table
        return self
    
    def values(self, values):
        self.values_ = values
        return self
    
    def SqlForInsert(self):
        """ From INSERT. Returns single SQL statement. """
        
        if self.fieldNeedsQuoted(self.destination):
            self.destination = self.quoteField(self.destination)
        
        values = self.values_
        query = ['INSERT INTO', self.destination]
        
        keys = []
        vals = []
        escape = []
        for k, v in values.items():
            keys.append(k)
            vals.append('%s')
            escape.append(v)
        
        query.append('(' + ', '.join(keys) + ')')
        query.append('VALUES')
        query.append('(' + ', '.join(vals) + ')')
        
        return (' '.join(query), escape)
    
    def update(self, table):
        """ Perform UPDATE on table. """

        self.reset()
        self.kind = 'update'
        self.destination = table
        return self
    
    def set(self, values):
        return self.values(values)
    
    def SqlForUpdate(self):
        """ From UPDATE. Returns (query, escape) """
        
        if self.fieldNeedsQuoted(self.destination):
            self.destination = self.quoteField(self.destination)
        
        values = self.values_
        query = ['UPDATE', self.destination, 'SET']
        
        escape = []
        vals = []
        
        for k, v in values.items():
            if type(v) == list and v[1] == FIELD:
                vals.append(k + ' = ' + v[0])
                continue
                
            vals.append(k + ' = %s')
            escape.append(v)
        
        query.append(', '.join(vals))
        
        # WHERE
        if len(self.where_) > 0:
            query.append('WHERE')
            for i in self.where_:
                tmpquery, tmpescape = i.sql()
                query.append(tmpquery)
                escape.extend(tmpescape)
        
        return (' '.join(query), escape)
    
    def deleteFrom(self, table):
        """ Perform DELETE on table. """
    
        self.reset()
        self.kind = 'delete'
        self.destination = table
        return self
    
    def SqlForDelete(self):
        """ From DELETE. Returns (query, escape). """
        
        if self.fieldNeedsQuoted(self.destination):
            self.destination = self.quoteField(self.destination)
        
        query = ['DELETE FROM', self.destination]
        escape = []
        
        # WHERE
        if len(self.where_) > 0:
            query.append('WHERE')
            for i in self.where_:
                tmpquery, tmpescape = i.sql()
                query.append(tmpquery)
                escape.extend(tmpescape)
        
        return (' '.join(query), escape)
    
    @ExceptionWrapper
    def cursor(self, server = False):
        """
        **
        * Execute query and return cursor. If server == True, return a
        * server-side cursor.
        *
        * @param    server  bool
        * @return   object
        **
        """
        if self.db == None:
            return None
        
        query, escape = self.sql()
        
        cursor = self.db.cursor()
        cursor.execute(query, escape)
        return cursor
    
    @ExceptionWrapper
    def asObject(self, fetchall = False, server = False, destroy = True, fetch = dict):
        """
        **
        * Execute query and return result object. If fetchall == True,
        * cursor.fetchall() will be used instead of wrapping the cursor
        * into an iterable object, and fetching as needed. If server == True,
        * use a server-side cursor and wrap it in an object, so data can be
        * downloaded as needed, instead of all at once.
        *
        * @param    fetchall    bool
        * @param    server      bool
        * @param    fetch       type, dict returns dictionaries, anything else
        *                       returns tuples
        * @return   QueryResult
        **
        """
        cursor = self.cursor(server)
        return QueryResult(cursor, destroy, fetch)
    
    @ExceptionWrapper
    def execute(self, server = False):
        cursor = self.cursor(server)
        cursor.close()
        return self
    
    @ExceptionWrapper
    def sql(self):
        if self.kind == 'select':
            return self.SqlForSelect()
        elif self.kind == 'insert':
            return self.SqlForInsert()
        elif self.kind == 'update':
            return self.SqlForUpdate()
        elif self.kind == 'delete':
            return self.SqlForDelete()
        elif self.kind == 'with':
            return self.SqlForWith()
    
    @ExceptionWrapper
    def commit(self):
        """ Convenience wrapper for db.commit() """
        if self.db is not None:
            self.db.commit()
    
    @ExceptionWrapper
    def rollback(self):
        """ Convenience wrapper for db.rollback() """
        if self.db is not None:
            self.db.rollback()
    
    def getConnection(self):
        """ Get database connection, if any """
        return self.db
    
    def setConnection(self, db):
        """ Set database connection """
        self.db = db
    
    def isReady(self):
        return True
    
    def isConnected(self):
        return True
