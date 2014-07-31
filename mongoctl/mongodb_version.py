__author__ = 'abdul'

from verlib import NormalizedVersion, suggest_normalized_version
from errors import MongoctlException

# Version support stuff
MIN_SUPPORTED_VERSION = "1.8"


###############################################################################
# MongoDBEdition (enum)
###############################################################################

class MongoDBEdition():
    COMMUNITY = "community"
    COMMUNITY_SSL = "community_ssl"
    ENTERPRISE = "enterprise"

    ALL = [COMMUNITY, COMMUNITY_SSL, ENTERPRISE]

###############################################################################
# MongoDBVersionInfo class
# we had to inherit and override __str__ because the suggest_normalized_version
# method does not maintain the release candidate version properly
###############################################################################
class MongoDBVersionInfo(NormalizedVersion):
    def __init__(self, version_number, edition=None):
        sugg_ver = suggest_normalized_version(version_number)
        super(MongoDBVersionInfo,self).__init__(sugg_ver)
        self.version_number = version_number
        self.edition = edition or MongoDBEdition.COMMUNITY

    ###########################################################################
    def __str__(self):
        return "%s (%s)" % (self.version_number, self.edition)

    ###########################################################################
    def __eq__(self, other):
        return (other is not None and
                super(MongoDBVersionInfo, self).__eq__(other) and
                self.edition == other.edition)

###############################################################################
def is_valid_version_info(version_info):
    return (is_valid_version(version_info.version_number) and
            version_info.edition in MongoDBEdition.ALL)

###############################################################################
def is_valid_version(version_number):
    return suggest_normalized_version(version_number) is not None

###############################################################################
# returns true if version is greater or equal to 1.8
def is_supported_mongo_version(version_number):
    return (make_version_info(version_number)>=
            make_version_info(MIN_SUPPORTED_VERSION))

###############################################################################
def make_version_info(version_number, edition=None):
    if version_number is None:
        return None

    version_number = version_number.strip()
    version_number = version_number.replace("-pre-" , "-pre")
    version_info = MongoDBVersionInfo(version_number, edition=edition)

    # validate version string
    if not is_valid_version_info(version_info):
        raise MongoctlException("Invalid version '%s." % version_info)
    else:
        return version_info

