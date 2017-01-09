#!/usr/bin/env python2.7

class OAError(Exception):
    """Exception for general purpose openlibarc error"""
    def __init__(self, message):
        super(OAError, self).__init__(message)
        self.message = message

class OAGraphRetrieveError(Exception):
    """Raised when data retrieval fails"""
    def __init__(self, message):
        super(OAGraphRetrieveError, self).__init__(message)
        self.message = message

class OAGraphIntegrityError(Exception):
    """Raised when data integrity issues are detected"""
    def __init__(self, message):
        super(OAGraphIntegrityError, self).__init__(message)
        self.message = message

class OAMarketError(Exception):
    """Exception throw upon incorrect interaction with a market"""
    def __init__(self, message):
        super(OAMarketError, self).__init__(message)
        self.message = message
