#!/usr/bin/env python2.7

class LCError(Exception):
    """Exception for general purpose openlibarc error"""
    def __init__(self, message):
        super(LCError, self).__init__(message)
        self.message = message

class LCGraphRetrieveError(Exception):
    """Raised when data retrieval fails"""
    def __init__(self, message):
        super(LCGraphRetrieveError, self).__init__(message)
        self.message = message

class LCGraphIntegrityError(Exception):
    """Raised when data integrity issues are detected"""
    def __init__(self, message):
        super(LCGraphIntegrityError, self).__init__(message)
        self.message = message

class LCMarketError(Exception):
    """Exception throw upon incorrect interaction with a market"""
    def __init__(self, message):
        super(LCMarketError, self).__init__(message)
        self.message = message
