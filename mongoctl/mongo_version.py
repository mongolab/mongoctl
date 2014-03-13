__author__ = 'abdul'

from verlib import NormalizedVersion, suggest_normalized_version

# Version support stuff
MIN_SUPPORTED_VERSION = "1.8"


###############################################################################
# MongoEdition (enum)
###############################################################################

class MongoEdition():
    COMMUNITY = "community"
    ENTERPRISE = "enterprise"

###############################################################################
# MongoctlNormalizedVersion class
# we had to inherit and override __str__ because the suggest_normalized_version
# method does not maintain the release candidate version properly
###############################################################################
class MongoctlNormalizedVersion(NormalizedVersion):
    def __init__(self, version_str, edition=None):
        sugg_ver = suggest_normalized_version(version_str)
        super(MongoctlNormalizedVersion,self).__init__(sugg_ver)
        self.version_str = version_str
        self.edition = edition or MongoEdition.COMMUNITY

    ###########################################################################
    def __str__(self):
        return self.version_str

    ###########################################################################
    def __eq__(self, other):
        return (other is not None and
                super(MongoctlNormalizedVersion, self).__eq__(other) and
                self.edition == other.edition)

###############################################################################
def is_valid_version(version_str):
    return suggest_normalized_version(version_str) is not None

###############################################################################
# returns true if version is greater or equal to 1.8
def is_supported_mongo_version(version_str):
    return (version_obj(version_str)>=
            version_obj(MIN_SUPPORTED_VERSION))

###############################################################################
def version_obj(version_str, edition=None):
    if version_str is None:
        return None

    #clean version string
    try:
        version_str = version_str.strip()
        version_str = version_str.replace("-pre-" , "-pre")
        return MongoctlNormalizedVersion(version_str, edition=edition)
    except Exception, e:
        return None
