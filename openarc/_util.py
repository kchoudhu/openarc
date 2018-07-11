#!/usr/bin/env python3

class oagprop(object):
    """Responsible for maitaining _oagcache on decorated properties"""
    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        try:
            return obj.cache.match(self.fget.__name__)
        except:
            subnode = self.fget(obj)
            if subnode is not None:
                from .graph import OAG_RootNode
                if isinstance(subnode, OAG_RootNode):
                    from ._rpc import reqcls
                    reqcls(obj).register(subnode.rpc.router, self.fget.__name__)
                obj.cache.put(self.fget.__name__, subnode)
            return subnode

    def __set__(self, obj, value):
        pass

class staticproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()