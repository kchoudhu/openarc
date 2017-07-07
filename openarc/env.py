#!/usr/bin/env python2.7

import os
import json
import traceback

class OAEnv(object):
    @property
    def static_http_root(self):
        if self.envcfg['httpinfo']['secure'] is True:
            security = "https://"
        else:
            security = "http://"

        return "%s%s" % ( security, self.envcfg['httpinfo']['httproot'] )

    @property
    def dbinfo(self):
        return self.envcfg['dbinfo']

    @property
    def crypto(self):
        return self.envcfg['crypto']

    @property
    def extcreds(self):
        return self.envcfg['extcreds']

    def __init__(self, requested_env):
        self.envname = requested_env
        cfg_file = "%s/envcfg.json" % ( os.environ.get("OPENARC_CFG_DIR") )
        with open( cfg_file ) as f:
            self.envcfg = json.loads( f.read() )[requested_env]


#This is where we hold library state.
#You will get cut if you don't manipulate the p_* variables
#via getenv() and initenv()

p_refcount_env = 0
p_env = None

def OALog(loginfo=None, ignore_exceptions=False):
    # Information about run env: hostname, pid, time
    if loginfo:
        print loginfo
    # redirect to stdout, other logger as necessary
    logstr = traceback.format_exc()
    if logstr != "None\n" and ignore_exceptions is False:
	   print logstr

def initenv(envstr):
    """envstr: one of local, dev, qa, prod.
    Does not return OAEnv variable; for that, you
    must call getenv"""
    global p_env
    global p_refcount_env
    if p_refcount_env == 0:
        p_env = OAEnv(envstr)
        p_refcount_env += 1

def getenv():
    """Accessor method for global state"""
    global p_env
    return p_env
