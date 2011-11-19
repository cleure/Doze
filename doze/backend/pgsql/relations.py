
"""
*
* Proposed extension, for dealing with relations.
*
"""

class Table(object):
    def __init__(self):
        self.name = None
        self.schema = None
        self.owner = None
        self.tablespace = None
        self.indexes = []
        self.triggers = []
        self.rules = []

class User(object):
    def __init__(self):
        self.id = None
        self.name = None
        self.is_superuser = False
        self.can_create_databases = False
        self.can_create_catalogs = False
        self.can_grant = False
        self.expire = None
        self.config = None

class Index(object):
    def __init__(self):
        self.name = None
        self.table = None
        self.kind = None        # BTREE, etc
        self.primary = False
        self.unique = False

class Sequence(object):
    def __init__(self):
        self.name = None
        self.increment = None
        self.min_value = None
        self.max_value = None

class Constraint(object): pass
class CheckConstraint(object): pass
class ForeignKeyConstraint(object): pass
class View(object): pass
