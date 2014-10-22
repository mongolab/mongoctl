__author__ = 'abdul'

from mongoctl.utils import document_pretty_string
###############################################################################
# Document Wrapper Class
###############################################################################
class DocumentWrapper(object):

    ###########################################################################
    # Constructor
    ###########################################################################

    def __init__(self, document):
        self.__document__ = document

    ###########################################################################
    # Overridden Methods
    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.__document__)

    ###########################################################################
    def get_document(self):
        return self.__document__

    ###########################################################################
    # Properties
    ###########################################################################
    def get_property(self, property_name):
        return self.__document__.get(property_name)

    ###########################################################################
    def set_property(self, name, value):
        self.__document__[name] = value


    ###########################################################################
    def get_ignore_str_property(self, name):
        val = self.get_property(name)
        if val:
            val = val.encode('ascii', 'ignore')
        return val

    ###########################################################################
    @property
    def id(self):
        return self.get_property('_id')

    @id.setter
    def id(self, value):
        self.set_property('_id', value)
