
"""
*
* Base classes
*
"""

class ObjectList(object):
    """
    ObjectList class. It provides a friendly interface for accessing
    attributes, so for instance, you can access data as:
    database.tables.my_table.my_column.type
    """

    def __init__(self):
        self.__dict__['objects'] = []
        self.__dict__['keys'] = []
    
    def __getattr__(self, name):
        try:
            idx = self.__dict__['keys'].index(name)
            return self.__dict__['objects'][idx]
        except:
            raise AttributeError('"%s" has no attribute named "%s"'
                % (self.__class__.__name__, name))
    
    def __setattr__(self, name, value):
        if name in self.__dict__['keys']:
            idx = self.__dict__['keys'].index(name)
            self.__dict__['objects'][idx] = value
        else:
            self.__dict__['keys'].append(name)
            self.__dict__['objects'].append(value)
    
    def __iter__(self):
        for i in range(0, len(self.__dict__['keys'])):
            yield (
                self.__dict__['keys'][i],
                self.__dict__['objects'][i])
    
    def attributes(self):
        """ Returns iterator of accessible attributes """
    
        for i in self.__dict__['keys']:
            if len(i) >= 2 and i[0:2] == '__':
                continue
            yield i
    
    def setattr(self, name, value):
        self.__setattr__(name, value)

class Relation(object):
    """ Base class for all Relations. """
    
    # Used when a bad argument is passed to factory()
    bad_argument_fmt = "%s() got an unexpected keyword argument '%s'"

    def __init__(self):
        # You can define certain instance attributes that will be
        # auto-loaded via the __attributes dictionary. The key is
        # the name of the attribute to auto-load, while the value
        # should be the name of a method to call, with the return
        # value returned to the caller.
        self.__dict__['__attributes'] = {}
    
    def __getattr__(self, name):
        if name in self.__dict__['__attributes']:
            func = getattr(self, self.__dict__['__attributes'][name])
            return func()
    
        raise AttributeError('"%s" has no attribute named "%s"'
            % (self.__class__.__name__, name))

    def __getitem__(self, key, value):
        if key in self.__dict__:
            return self.__dict__[key]
        raise KeyError("'%s'" % (key))
    
    def __setitem__(self, key, value):
        self.__dict__[key] = value
    
    def __iter__(self):
        for k, v in self.__dict__.items():
            if len(k) >= 2 and k[0:2] == '__':
                continue
            yield (k, v)
    
    def attributes(self):
        """ Returns iterator of accessible attributes """
        
        for k in self.__dict__.keys():
            if len(k) >= 2 and k[0:2] == '__':
                continue
            yield k
        
        if '__attributes' in self.__dict__:
            for k in self.__dict__['__attributes'].keys():
                yield k
    
    def __unicode__(self):
        # As unicode
        return u'%s' % (self.name)
    
    def __str__(self):
        # Alias for __unicode__
        return self.__unicode__()

