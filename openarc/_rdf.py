#!/usr/local/env python3

import collections
import inflection
import weakref

from ._rpc  import reqcls
from ._util import oagprop

from openarc.exception import *

class CacheProxy(object):
    """Responsible for manipulation of relational data frame"""
    def __init__(self, oag):
        self._oag = oag

        # Cache storage object.
        self._oagcache ={}

    def clear(self):
        self._oagcache = {}

    def clone(self, src):
        self._oagcache   = list(src.oagache._oagcache)

    def invalidate(self, invstream):
        # - filter out calcs: they can no longer be trusted as node as been invalidated
        tmpoagcache = {oag:self._oagcache[oag] for oag in self._oagcache if oag in self._oag.streams.keys()}
        # - filter out invalidated downstream node
        tmpoagcache = {oag:tmpoagcache[oag] for oag in tmpoagcache if oag != invstream}

        self._oagcache = tmpoagcache

    def match(self, stream):
        return self._oagcache[stream]

    def put(self, stream, new_value):
        self._oagcache[stream] = new_value

    @property
    def state(self):

        return self._oagcache

class RdfProxy(object):
    """Responsible for manipulation of relational data frame"""
    def __init__(self, oag):
        self._oag = oag

        # All data. Currently can only be set from database.
        self._rdf = None

        # An array of lambdas sequentially used to filter the rdf
        self._rdf_filter_cache = []

        # If this is a slice, it can be used to further filter the rdf
        self._rdf_window_index = 0

        # After
        self._rdf_window = None

    def clone(self, src):
        self._rdf        = list(src.rdf._rdf)
        self._rdf_window = list(src.rdf._rdf_window)
        self._rdf_window_index =\
                                src.rdf._rdf_window_index
        # self._rdf_filter_cache =\
        #                         list(src.rdf._rdf_filter_cache)

    def filter(self, predicate, rerun=False):
        if self._oag.is_unique:
            raise OAError("Cannot filter OAG that is marked unique")

        self._oag.cache.clear()

        self._rdf_window = self._rdf

        if rerun is False:
            self._rdf_filter_cache.append(predicate)

        self._rdf_window = []
        for i, frame in enumerate(self._rdf):
            self._oag.propmgr._cframe = frame
            self._oag.propmgr._set_attrs_from_cframe()
            if predicate(self._oag):
                self._rdf_window.append(self._rdf[i])

        if len(self._rdf_window)>0:
            self._oag.propmgr._cframe = self._rdf_window[0]
        else:
            self._rdf_window = []
            self._oag.propmgr._cframe = {}

        self._oag.propmgr._set_attrs_from_cframe()

        return self._oag

    def reset(self):

        # Clear RDF
        self._rdf = None
        self._rdf_filter_cache = []
        self._rdf_window_index = 0
        self._rdf_window = None

        # Clear RDF cache
        self._oag.cache.clear()

        # Clear cframe
        self._oag.propmgr._cframe = {}

        return self._oag

    def sort(self, key):

        self._rdf.sort(key=lambda x: x[key])
        self._rdf_window = self._rdf
        self._rdf_window_index = None
        self._oag.propmgr._cframe = {}

        return self._oag

class PropProxy(object):
    """Manipulates properties"""
    def __init__(self, oag):

        # OAG whose properties are being managed
        self._oag           = oag

        # Mutuable pointer to a row in OAG's RDF
        self._cframe        = {}

        # Used to store properties for __getattribute__ calls
        self._oagprops      = {}

    def add(self, stream, oagprop, fk=False):
        if fk or self.is_managed_oagprop(stream):
            self._oagprops[stream] = oagprop
        else:
            raise OAGraphIntegrityError("This [%s] is not a managed oagprop" % stream)

    def clear(self):
        for stream in self._oagprops:
            self._oagprops[stream] = None

    def clone(self, src):
        self._cframe = dict(src.propmgr._cframe)

    def get(self, stream):
        try:
            if self.is_managed_oagprop(stream):
                # Set default value on class
                try:
                    attr = object.__getattribute__(self._oag, stream)
                except AttributeError:
                    setattr(self._oag.__class__, stream, None)

                # Return it
                if type(self._oagprops[stream])==oagprop:
                    subnode = self._oagprops[stream].__get__(self._oag)
                    reqcls(self._oag).register(subnode.rpc.router, stream)
                    return subnode
                else:
                    return self._oagprops[stream]
            else:
                raise AttributeError("This attribute is not managed by the propmanager")
        except KeyError as e:
            raise AttributeError("Cannot find attribute [%s] in propmanager" % stream)

    def is_managed_oagprop(self, stream):
        """Takes requested attribute and returns True or false"""
        allowed_streams = list(set(
            [object.__getattribute__(self._oag, 'dbpkname')] +\
            list(object.__getattribute__(self._oag, 'streams').keys()) +\
            list(self._oagprops.keys())
        ))
        return stream in allowed_streams

    def _set_attrs_from_cframe(self):
        from .graph import OAG_RootNode

        # Blank everything if _cframe isn't set
        if len(self._cframe)==0:
            self.clear()
            return

        # Set dbstream attributes
        for stream, streaminfo in self._cframe.items():
            self._set_oagprop(stream, streaminfo)

        # Set forward lookup attributes
        for fk in self._oag.__class__._fkframe:
            classname = "OAG_"+inflection.camelize(fk['table'])
            for cls in OAG_RootNode.__subclasses__():
                if cls.__name__==classname:
                    stream = fk['table']
                    def fget(obj,
                             cls=cls,
                             searchprms=[getattr(self._oag, fk['points_to_id'], None)],
                             searchidx='by_'+{cls.stream_db_mapping[k]:k for k in cls.stream_db_mapping}[fk['id']]):
                        return cls(searchprms, searchidx)
                    fget.__name__ = stream
                    self.add(stream, oagprop(fget), fk=True)

    def _set_attrs_from_cframe_uniq(self):
        if len(self._oag.rdf._rdf_window) > 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")

        if len(self._oag.rdf._rdf_window) == 1:
            self._cframe = self._oag.rdf._rdf_window[0]
        else:
            self._cframe = []

        self._set_attrs_from_cframe()

    def _set_cframe_from_userprms(self, userprms, force_attr_refresh=False, fullhouse=False):

        setattrs = []

        attrinit = len(userprms)>0 or force_attr_refresh

        if attrinit:

            invalid_streams = []
            processed_streams = {}

            if len(userprms)>0:
                invalid_streams = [ s for s in userprms if s not in self._oag.streams.keys() ]
                if len(invalid_streams)>0:
                    raise OAGraphIntegrityError("Invalid update stream(s) detected %s" % invalid_streams)

                processed_streams = { s:userprms[s] for s in userprms if s not in invalid_streams }
                for stream, streaminfo in processed_streams.items():
                    setattr(self._oag, stream, streaminfo)
                    self._set_oagprop(stream, streaminfo, cframe_form=False)
                setattrs = processed_streams.keys()

        self._set_cframe_from_attrs(setattrs, fullhouse=fullhouse)

    def _set_cframe_from_attrs(self, attrs, fullhouse=False):
        cframe_tmp = {}
        raw_missing_streams = []

        all_streams = list(self._oag.streams.keys())
        if len(self._cframe) > 0:
            all_streams.append(self._oag.dbpkname)

        for oagkey in all_streams:

            # Special handling for indices
            if oagkey[0] == '_':
                cframe_tmp[oagkey] = getattr(self._oag, oagkey, None)
                continue

            cfkey = oagkey
            if self._oag.is_oagnode(oagkey):
                cfkey = self._oag.stream_db_mapping[oagkey]
            cfval = getattr(self._oag, oagkey, None)

            # Special handling for nullable items
            if type(self._oag.streams[oagkey][0])!=str\
                and self._oag.streams[oagkey][1] is False:
                cframe_tmp[cfkey] = cfval.id if cfval else None
                continue

            # Is a value missing for this stream?
            if cfval is None:
                raw_missing_streams.append(oagkey)
                continue

            # Ok, actualy set cframe
            if self._oag.is_oagnode(oagkey):
                # this only works if we're in dbpersist mode
                # if there's a key error, we're working in-memory
                try:
                    cfval = cfval.id
                except KeyError:
                    pass
            cframe_tmp[cfkey] = cfval

        if fullhouse:
            missing_streams = []
            for rms in raw_missing_streams:
                if self._oag.is_oagnode(rms):
                    missing_streams.append(rms)
                else:
                    if self._oag.streams[rms][1] is not None:
                        missing_streams.append(rms)
            if len(missing_streams)>0:
                raise OAGraphIntegrityError("Missing streams detected on [%s]: %s" % (self._oag.dbtable, missing_streams))

        self._cframe = cframe_tmp

    def _set_oagprop(self, stream, cfval, searchidx='id', cframe_form=True):

        # primary key: set directly
        if stream[0] == '_':
            setattr(self._oag, stream, self._cframe[stream])
            return

        # Normalize stream name to OAG form
        if cframe_form:
            db_stream_mapping = {self._oag.stream_db_mapping[k]:k for k in self._oag.stream_db_mapping}
            stream = db_stream_mapping[stream]

        if self._oag.is_oagnode(stream):

            # oagprop: update cache if necessary
            try:
                currattr = getattr(self._oag, stream, None)
                if currattr is None:
                    if self._oag.streams[stream][1] is False and cfval:
                        currattr = self._oag.streams[stream][0](cfval, searchidx)
                        if not currattr.is_unique:
                            currattr = currattr[-1]
            except OAGraphRetrieveError:
                currattr = None

            if currattr:
                self._oag.cache.put(stream, currattr)

            # oagprop: actually set it
            def fget(obj,
                     stream=stream,
                     streaminfo=self._oag.streams[stream],
                     searchprms=[cfval],
                     searchidx=searchidx,
                     currattr=currattr):
                # Do not instantiate objects unnecessarily
                if currattr:
                    try:
                        if currattr == searchprms[0]:
                            return currattr
                        if currattr.id == searchprms[0]:
                            return currattr
                    except KeyError:
                        # We're dealing with in-memory OAGs, just return
                        return currattr
                elif streaminfo[1] is False:
                    return currattr
                else:
                    newattr = streaminfo[0](searchprms, searchidx)
                    if not newattr.is_unique:
                        newattr = newattr[-1]
                    return newattr
                newattr = streaminfo[0](searchprms, searchidx)
                return newattr
            fget.__name__ = stream
            self.add(stream, oagprop(fget))
        else:
            setattr(self._oag, stream, cfval)
