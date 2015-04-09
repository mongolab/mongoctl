__author__ = 'abdul'

from pymongo.errors import OperationFailure

###############################################################################
# Mongoctl Exception class
###############################################################################
class MongoctlException(Exception):
    def __init__(self, message, cause=None):
        super(MongoctlException, self).__init__(message)
        self._cause = cause


###############################################################################
class FileNotInRepoError(MongoctlException):
    pass


def is_auth_error(e):
    return isinstance(e, OperationFailure) and e.code == 13