__author__ = 'abdul'


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