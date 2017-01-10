#!/usr/bin/env python2.7

import msgpack
import random
import socket
import zmq.green as zmq

from gevent            import spawn
from textwrap          import dedent as td

from openarc.exception import *
from openarc.dao       import *
from openarc.graph     import *
from openarc.oatime    import *

class OAG_RpcService(OAGraphRootNode):
    # Global interface
    @property
    def is_unique(self): return True
    @property
    def dbcontext(self): return "openarc"

    # Other stuff
    @property
    def SQL(self):
      return {
        "read" : {
          "id" : td("""
            SELECT r.*
              FROM rpc_registry r
             WHERE _rpc_id=%s"""),
          "property" : td("""
            SELECT r.*
              FROM rpc_registry r
             WHERE servicename=%s
                   AND owning_class=%s
                   AND owner_id=%s
                   AND role=%s""")
        }
      }

class OARpcBase(object):
    def __init__(self):
        # centralize all access to common database
        self.dbcontext = "openarc"
        self.is_proxy  = False
        self._proxy    = []
        self._zmqctx   = {r:{} for r in self.roles}
        try:
            self.connects_to
        except:
            self.connects_to = None

    @property
    def status(self):
        return [OAG_RpcService([self.servicename,
                                self._owning_class,
                                self._owner_id,
                                r], "property")
                for r in self.roles]

    @property
    def socket(self):
        return { r:self._zmqctx[r]['socket'] for r in self._roles }

    def __get_connect_target_info(self):
        rpc = OAG_RpcService((self.connects_to.servicename,
                              self.connects_to.owning_class,
                              self.connects_to.owner_id,
                              self.connects_to.role), "property")

        return rpc.connhost, rpc.connport

    def __bind_listener(self, role, zmq_type):
        host = socket.gethostname()
        ctxsoc = self._zmqctx[role]['ctx'].socket(zmq_type)
        ctxsoc.bind("tcp://*:0")
        port = ctxsoc.LAST_ENDPOINT.split(":")[-1]
        return ctxsoc, host, port

    def __proxy_sockets(self, s_from, s_to):
        while True:
            message = s_from.recv_multipart()
            s_to.send_multipart(message)

    def start(self):
        try:
            for role in self.roles:
                # ZMQ initialization
                self._zmqctx[role]['ctx']      = ctx     = zmq.Context()
                self._zmqctx[role]['runhost']  = runhost = socket.gethostname()

                # Role specific ZMQ stuff
                if role == "rep":
                    if self.connects_to is None:
                        ctxsoc, connhost, connport = self.__bind_listener(role, zmq.REP)
                    else:
                        connhost, connport = self.__get_connect_target_info()
                        ctxsoc = ctx.socket(zmq.REP)
                        ctxsoc.connect("tcp://%s:%s" % (connhost, connport))
                elif role == "req":
                    connhost, connport = self.__get_connect_target_info()
                    ctxsoc = ctx.socket(zmq.REQ)
                    ctxsoc.connect("tcp://%s:%s" % (connhost, connport))
                elif role == "dealer":
                    ctxsoc, connhost, connport = self.__bind_listener(role, zmq.DEALER)
                    self.is_proxy = True
                elif role == "router":
                    ctxsoc, connhost, connport = self.__bind_listener(role, zmq.ROUTER)
                    self.is_proxy = True
                elif role == "pub":
                    ctxsoc, connhost, connport = self.__bind_listener(role, zmq.PUB)
                elif role == "sub":
                    connhost, connport = self.__get_connect_target_info()
                    ctxsoc = ctx.socket(zmq.SUB)
                    ctxsoc.connect("tcp://%s:%s" % (connhost, connport))
                    ctxsoc.setsockopt(zmq.SUBSCRIBE, '')
                else:
                    raise OAError("Invalid role specified, should be one of:\n"+
                                  "* req/rep\n"+
                                  "* dealer/router\n"+
                                  "* pub/sub")

                self._zmqctx[role]['socket']   = ctxsoc
                self._zmqctx[role]['connhost'] = connhost
                self._zmqctx[role]['connport'] = connport

                # Database stuff
                with OADao(self.dbcontext) as dao:
                    with dao.cur as cur:
                        try:
                            now  = OATime(cur).now
                            ret  = cur.execute(self.SQL.register_status,
                                              [self.servicename,
                                               self._owning_class,
                                               self._owner_id,
                                               role, runhost, connhost,
                                               connport, now])
                            dao.commit()
                        except Exception as e:
                           raise OAError("Database error:\n%s" % e)
        except OAError as e:
            self.stop()
            # reraise OAError for further processing
            raise OAError("Error registering service\n%s" % str(e))

        self.__start_proxy()

        return self

    def __start_proxy(self):
        if self.is_proxy is True:
            # Proxy requests to replies
            self._proxy.append(spawn(self.__proxy_sockets,
                                     self._zmqctx['router']['socket'],
                                     self._zmqctx['dealer']['socket']))
            # Proxy replies to requests
            self._proxy.append(spawn(self.__proxy_sockets,
                                     self._zmqctx['dealer']['socket'],
                                     self._zmqctx['router']['socket']))

    def _send(self, message, role=None):
        if self.is_proxy is True:
            raise OAError("Send not available for proxy components")
        else:
            socket = self._zmqctx[self.roles[0]]['socket']
            socket.send(msgpack.dumps(message))

    def _recv(self, role=None):
        if self.is_proxy is True:
            raise OAError("Recv not available for proxy components")
        else:
            socket = self._zmqctx[self.roles[0]]['socket']
            return msgpack.loads(socket.recv())

    def stop(self):
        for role in self.roles:
            with OADao(self.dbcontext) as dao:
                with dao.cur as cur:
                    try:
                        ret = cur.execute(self.SQL.delete_registration,
                                         [self.servicename,
                                          self._owning_class,
                                          self._owner_id,
                                          role])
                        dao.commit()
                    except:
                        raise OAError("Error deleting service registration")

    def heartbeat(self):
        raise NotImplementedError("Heartbeat not yet supported")

    class SQL(object):
        register_status = td("""
            INSERT INTO rpc_registry(servicename,
                                     owning_class,
                                     owner_id,
                                     role,
                                     runhost,
                                     connhost,
                                     connport,
                                     heartbeat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")
        delete_registration = td("""
            DELETE FROM rpc_registry
                  WHERE servicename=%s
                        AND owning_class=%s
                        AND owner_id=%s
                        AND role=%s""")
