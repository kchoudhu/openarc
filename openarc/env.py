#!/usr/bin/env python3

import base64
import os
import toml
import traceback

from openarc.exception import *

class OAEnv(object):

    def __init__(self, cfgfile='openarc.toml'):
        self.envid   = base64.b16encode(os.urandom(16)).decode('ascii')

        self.rpctimeout = 5
        cfg_dir = os.environ.get("OPENARC_CFG_DIR") if os.environ.get("OPENARC_CFG_DIR") else str()
        cfg_file_path = "%s/%s" % ( cfg_dir, cfgfile )
        try:
            with open( cfg_file_path ) as f:
                envcfg = toml.loads( f.read() )
                self._envcfg = envcfg

                # The highlights
                self.crypto     = envcfg['crypto']
                self.dbinfo     = envcfg['dbinfo']
                self.extcreds   = envcfg['extcreds']
                self.name       = envcfg['env']
                self.rpctimeout = envcfg['graph']['heartbeat']

        except IOError:
            raise OAError("%s does not exist" % cfg_file_path)

class OALog(object):
    SQL   = False
    Graph = False
    RPC   = False

    def log(loginfo=None, ignore_exceptions=False):
        # Information about run env: hostname, pid, time
        if loginfo:
            print(loginfo)
        # redirect to stdout, other logger as necessary
        logstr = traceback.format_exc()
        if logstr != "None\n" and ignore_exceptions is False:
            print(logstr)

#This is where we hold library state.
#You will get cut if you don't manipulate the p_* variables
#via getenv() and initenv()

p_refcount_env = 0
p_env = None


def initenv():
    """envstr: one of local, dev, qa, prod.
    Does not return OAEnv variable; for that, you
    must call getenv"""
    global p_env
    global p_refcount_env
    if p_refcount_env == 0:
        from gevent import monkey
        p_env = OAEnv()
        p_refcount_env += 1

def getenv():
    """Accessor method for global state"""
    global p_env
    return p_env
