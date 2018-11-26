#!/usr/local/env python3

import collections
import copy
import weakref

from ._rpc  import reqcls
from ._util import oagprop

from openarc.env       import gctx
from openarc.exception import *

class CacheProxy(object):
    """Responsible for manipulation of relational data frame"""
    def __init__(self, oag):
        self._oag = oag

        # Cache storage object.
        self._oagcache ={}

    def clear(self):
        for stream, oag in self._oagcache.items():
            if self._oag.is_oagnode(stream):
                gctx().rm_ka_via_rpc(self._oag.rpc.url, oag.rpc.url, stream)
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
        if not new_value:
            return
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

        self._rdf_window = self._rdf

        if rerun is False:
            self._rdf_filter_cache.append(predicate)

        self._rdf_window = []
        for i, frame in enumerate(self._rdf):
            self._oag.props._cframe = frame
            self._oag.cache.clear()
            self._oag.props._set_attrs_from_cframe(fastiter=True, nofk=True)
            if predicate(self._oag):
                self._rdf_window.append(self._rdf[i])

        if len(self._rdf_window)>0:
            self._oag.props._cframe = self._rdf_window[0]
        else:
            self._rdf_window = []
            self._oag.props._cframe = {}

        self._oag.props._set_attrs_from_cframe()

        return self._oag

    def map(self, predicate, rerun=False):
        if self._oag.is_unique:
            raise OAError("Cannot filter OAG that is marked unique")

        ret_list = []

        oagcpy = self._oag.__class__(rpc=False)

        for i, frame in enumerate(self._rdf_window):
            oagcpy.props._cframe = frame
            oagcpy.props._set_attrs_from_cframe(fastiter=True, nofk=True)
            ret_list.append(predicate(oagcpy))

        return ret_list

    def reset(self):

        # Clear RDF
        self._rdf = None
        self._rdf_filter_cache = []
        self._rdf_window_index = 0
        self._rdf_window = None

        # Clear RDF cache
        self._oag.cache.clear()

        # Clear cframe
        self._oag.props._cframe = {}

        return self._oag

    def sort(self, key):

        self._rdf.sort(key=lambda x: x[key])
        self._rdf_window = self._rdf
        self._rdf_window_index = None
        self._oag.props._cframe = {}

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

        # Streams that are managed by this property manager
        self._managed_oagprops = []

    def add(self, stream, cfval, cfcls, searchidx, from_cframe, from_foreign_key, fastiter):
        """Add new cfval to the property management dict. If cfval is an
        oagprop or a non-OAG stream, add it directly. If cfval is a subnode
        wrap it in an oagprop and then add it to the dict."""
        from .graph import OAG_RootNode

        # Cache a few values
        is_oagnode = self._oag.is_oagnode(stream)
        is_managed_oagprop = self.is_managed_oagprop(stream)

        if not (from_foreign_key or is_managed_oagprop):
            raise OAGraphIntegrityError("Stream [%s] is not a managed oagprop" % stream)

        #
        # Register foreign key as managed oagprop
        #
        if from_foreign_key and not is_managed_oagprop:
            self.register_managed_oagprop(stream)

        #
        # Access current value of the stream. Don't bother if we are in fast
        # iteration mode.
        #
        if fastiter:
            currval = None
        else:
            try:
                # If stream is an oagprop, check cache to see whether it is set. If it is, it
                # is safe to getattr the stream. The overarching objective here is to avoid
                # unnecessary oagprop dereferences.
                currval = self._oagprops[stream]
                if type(currval)==oagprop:
                    if self._oag.cache.match(stream):
                        currval = getattr(self._oag, stream, None)
            except KeyError:
                # Either stream was not set on oagprop, or having been set there, it was
                # not subsequently deferenced and cached. Either way, it's time for a
                # fresh start: set currval to None
                currval = None

        #
        # Maintain cache coherence for oagprops
        #
        if from_foreign_key or is_oagnode:
            if from_cframe:
                # if cframe values are the same as current value, refresh
                # cache. This is to handle the case where user sets subnode
                # on an uninitialized OAG, which is subsequently persisted
                # and then refreshed from the underlying datastore.
                try:
                    if currval.id == cfval:
                        self._oag.cache.put(stream, currval)
                except AttributeError:
                    pass
            else:
                # non-cframe values are OAGs, put them in cache immediately
                self._oag.cache.put(stream, cfval)

        #
        # Package cframe cfvals as oagprops
        #
        if from_foreign_key or is_oagnode:

            # Sanity checks
            if from_cframe and cfcls is None:
                raise OAError("Cannot create OAG from cframe without class definition")

            def fget(obj,
                     stream=stream,
                     streaminfo=cfcls,
                     searchprms=[cfval],
                     searchidx=searchidx,
                     searchwin=None,
                     searchoffset=None,
                     searchdesc=False,
                     currval=currval,
                     from_cframe=from_cframe,
                     from_foreign_key=from_foreign_key):

                newval = searchprms[0]

                if from_cframe:
                    # cframe values come in as fk references; they must
                    # be converted to OAGs before they can be returned. If
                    # object conversion cannot happen (i.e. gen_retval is
                    # is False), return currval
                    gen_retval = False

                    if from_foreign_key:
                        gen_retval = True
                    else:
                        if currval:
                            try:
                                # New cframe value is different from current one,
                                # regenerate object
                                if newval != currval.id:
                                    gen_retval = True
                            except KeyError:
                                # ID not found in _cframe, we are dealing with
                                # in-memory OAG. Generate a new ID
                                gen_retval = True
                        else:
                            gen_retval = True

                    if gen_retval:
                        try:
                            attr = streaminfo(searchprms, searchidx, searchwin, searchoffset, searchdesc)
                            retval = attr[-1] if not attr.is_unique else attr
                        except OAGraphRetrieveError:
                            retval  = None
                    else:
                        retval = currval

                    return retval
                else:
                    # raw values come in as OAGs. Return one of
                    # newval or currval depending on whether things
                    # have changed
                    retval = currval
                    if newval != currval:
                        retval = newval

                    return retval

                raise OAError("This should never happen")

            fget.__name__ = stream
            cfval_prop = oagprop(fget)
        else:
            cfval_prop = cfval

        self._oagprops[stream] = cfval_prop

        #
        # Carry out inter-OAG signalling, but only if we are not in fast iteration
        # mode
        #
        if not fastiter and self._oag.rpc.is_enabled and self._oag.rpc.is_init and stream not in self._oag.rpc.stoplist:

            if not from_cframe:

                # Flag driving whether or not to invalidate upstream nodes
                invalidate_upstream = False

                # Handle oagprops
                if isinstance(cfval, OAG_RootNode):
                    # Regenerate connections to surrounding nodes
                    if currval is None:
                        if self._oag.logger.RPC:
                            print("[%s:req] Connecting to subnode [%s], stream [%s] in initmode" % (self._oag.rpc.id, cfval.rpc.id, stream))
                        reqcls(self._oag).register(cfval.rpc.url, stream)
                    else:
                        if currval != cfval:
                            if self._oag.logger.RPC:
                                print("[%s:req] Detected stream change on [%s] from [%s]->[%s]" % (self._oag.rpc.id,
                                                                                                   stream,
                                                                                                   currval.rpc.id,
                                                                                                   cfval.rpc.id))
                            if currval:
                                reqcls(self._oag).deregister(currval.rpc.url, self._oag.rpc.url, stream)
                            reqcls(self._oag).register(cfval.rpc.url, stream)
                            invalidate_upstream = True
                else:
                    if currval and currval != cfval:
                        invalidate_upstream  = True

                if invalidate_upstream:
                    if len(self._oag.rpc.registrations)>0:
                        if self._oag.logger.RPC:
                            print("[%s:req] Informing upstream of [%s] invalidation [%s]->[%s]" % (self._oag.rpc.id, stream, currval, cfval))
                        if self._oag.rpc.transaction.is_active:
                            self._oag.rpc.transaction.notify_upstream = True
                        else:
                            for addr, stream_to_invalidate in self._oag.rpc.registrations.items():
                                reqcls(self._oag).invalidate(addr, stream_to_invalidate)

    def clear(self):
        for stream in self._oagprops:
            self._oagprops[stream] = None

    def clone(self, src):
        self._cframe = dict(src.props._cframe)

    def get(self, stream, searchwin=None, searchoffset=None, searchdesc=False, internal_call=False):
        try:
            if self.is_managed_oagprop(stream):
                # Set default value on class
                try:
                    attr = object.__getattribute__(self._oag, stream)
                except AttributeError:
                    setattr(self._oag.__class__, stream, None)

                # Return it
                if type(self._oagprops[stream])==oagprop:
                    # Ok, a bit of fuckery here: if there is a searchwin/offset defined, we don't want to
                    # poison the original version which should always return the original, non-windowed
                    # dataset. Instead deepcopy the original, and intialize *that*.
                    oag = copy.deepcopy(self._oagprops[stream]) if (searchwin or searchoffset) else self._oagprops[stream]
                    subnode = oag.__get__(self._oag, searchwin=searchwin, searchoffset=searchoffset, searchdesc=searchdesc, cache=internal_call)
                    return subnode
                else:
                    return self._oagprops[stream]
            else:
                raise AttributeError("This attribute is not managed by the propmanager")
        except KeyError as e:
            raise AttributeError("Cannot find attribute [%s] in propmanager" % stream)

    def is_managed_oagprop(self, stream):
        """Takes requested attribute and returns True or false"""
        if len(self._managed_oagprops)==0:
            self._managed_oagprops = list(set(
                [object.__getattribute__(self._oag, 'dbpkname')] +\
                list(object.__getattribute__(self._oag, 'streams').keys())
            ))
        return stream in self._managed_oagprops

    def register_managed_oagprop(self, stream):
        self._managed_oagprops.append(stream)
        list(set(self._managed_oagprops))

    def _set_attrs_from_cframe(self, fastiter=False, nofk=False):
        from .graph import OAG_RootNode

        # Blank everything if _cframe isn't set
        if len(self._cframe)==0:
            self.clear()
            return

        # Set dbstream attributes
        for stream, cfval in self._cframe.items():

            # primary key: set directly
            if stream[0] == '_':
                self._oag.__setattr__(stream, self._cframe[stream], fastiter=fastiter)
                continue

            # Translate stream name and add to prop proxy
            stream = self._oag.db_stream_mapping[stream]
            self.add(stream, cfval, self._oag.streams[stream][0], 'id', True, False, fastiter)

        # Set forward lookup attributes -- but only if nofk flag is set
        if nofk:
            return
        for i, fk in enumerate(self._oag.__class__._fkframe):
            classname = gctx().db_class_mapping(fk['table'])
            for cfcls in OAG_RootNode.__subclasses__():

                if cfcls.__name__==classname:

                    # Generate name of new stream
                    stream = fk['table']
                    if len([f for f in self._oag.__class__._fkframe if f['table']==fk['table']])>1:
                        stream+='_'+cfcls.db_stream_mapping[fk['id']]

                    cfval = getattr(self._oag, fk['points_to_id'], None)
                    searchidx = 'by_'+cfcls.db_stream_mapping[fk['id']]
                    self.add(stream, cfval, cfcls, searchidx, True, True, fastiter)

    def _set_attrs_from_cframe_uniq(self):
        if len(self._oag.rdf._rdf_window) > 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")

        if len(self._oag.rdf._rdf_window) == 1:
            self._cframe = self._oag.rdf._rdf_window[0]
        else:
            self._cframe = []

        self._set_attrs_from_cframe()

    def _set_cframe_from_userprms(self, userprms, force_attr_refresh=False, fullhouse=False):

        # Intialize OAG attributes from userprms if necessary
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

        # Set cframe from attributes
        cframe_tmp = {}
        raw_missing_streams = []

        all_streams = list(self._oag.streams.keys())
        if len(self._cframe) > 0:
            all_streams.append(self._oag.dbpkname)

        for stream in all_streams:

            # Special handling for indices
            if stream[0] == '_':
                cframe_tmp[stream] = getattr(self._oag, stream, None)
                continue

            cfkey = stream
            if self._oag.is_oagnode(stream):
                cfkey = self._oag.stream_db_mapping[stream]
            cfval = getattr(self._oag, stream, None)

            # Special handling for nullable items
            if type(self._oag.streams[stream][0])!=str\
                and self._oag.streams[stream][1] is False:
                cframe_tmp[cfkey] = cfval.id if cfval else None
                continue

            # Is a value missing for this stream?
            if cfval is None:
                raw_missing_streams.append(stream)
                continue

            # Ok, actualy set cframe
            if self._oag.is_oagnode(stream):
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
