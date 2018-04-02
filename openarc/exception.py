#!/usr/bin/env python3

class OAError(Exception):
    """Exception for general purpose openarc error"""
    def __init__(self, message):
        super(OAError, self).__init__(message)
        self.message = message

class OAGoddammit(OAError):
    """Subset of OAError, reserved for that class of errors which future
    maintainers will find to be most boneheaded."""
    pass

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
