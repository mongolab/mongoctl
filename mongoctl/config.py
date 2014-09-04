__author__ = 'abdul'

import json
import urllib
import mongoctl_globals

from utils import *

from minify_json import minify_json
from errors import MongoctlException

from bson import json_util

###############################################################################
# CONSTS
###############################################################################
MONGOCTL_CONF_FILE_NAME = "mongoctl.config"


###############################################################################
# Config root / files stuff
###############################################################################
__config_root__ = mongoctl_globals.DEFAULT_CONF_ROOT

def _set_config_root(root_path):
    if not is_url(root_path) and not dir_exists(root_path):
        raise MongoctlException("Invalid config-root value: %s does not"
                                " exist or is not a directory" % root_path)
    global __config_root__
    __config_root__ = root_path


###############################################################################
# Configuration Functions
###############################################################################

def get_mongoctl_config_val(key, default=None):
    return get_mongoctl_config().get(key, default)

###############################################################################
def set_mongoctl_config_val(key, value):
    get_mongoctl_config()[key] = value

###############################################################################
def get_generate_key_file_conf(default=None):
    return get_mongoctl_config_val('generateKeyFile', default=default)

###############################################################################
def get_database_repository_conf():
    return get_mongoctl_config_val('databaseRepository')

###############################################################################
def get_file_repository_conf():
    return get_mongoctl_config_val('fileRepository')

###############################################################################
def get_mongodb_installs_dir():
    installs_dir = get_mongoctl_config_val('mongoDBInstallationsDirectory')
    if installs_dir:
        return resolve_path(installs_dir)

###############################################################################
def set_mongodb_installs_dir(installs_dir):
    set_mongoctl_config_val('mongoDBInstallationsDirectory', installs_dir)

###############################################################################

def get_default_users():
    return get_mongoctl_config_val('defaultUsers', {})

###############################################################################
def get_cluster_member_alt_address_mapping():
    return get_mongoctl_config_val('clusterMemberAltAddressesMapping', {})


###############################################################################
def to_full_config_path(path_or_url):
    global __config_root__


    # handle abs paths and abs URLS
    if os.path.isabs(path_or_url):
        return resolve_path(path_or_url)
    elif is_url(path_or_url):
        return path_or_url
    else:
        result =  os.path.join(__config_root__, path_or_url)
        if not is_url(__config_root__):
            result = resolve_path(result)

        return result

###############################################################################
## Global variable CONFIG: a dictionary of configurations read from config file
__mongo_config__ = None

def get_mongoctl_config():

    global __mongo_config__

    if __mongo_config__ is None:
        __mongo_config__ = read_config_json("mongoctl",
                                            MONGOCTL_CONF_FILE_NAME)

    return __mongo_config__


###############################################################################
def read_config_json(name, path_or_url):

    try:
        log_verbose("Reading %s configuration"
                    " from '%s'..." % (name, path_or_url))

        json_str = read_json_string(path_or_url)
        # minify the json/remove comments and sh*t
        json_str = minify_json.json_minify(json_str)
        json_val =json.loads(json_str,
                             object_hook=json_util.object_hook)

        if not json_val and not isinstance(json_val,list): # b/c [] is not True
            raise MongoctlException("Unable to load %s "
                                    "config file: %s" % (name, path_or_url))
        else:
            return json_val
    except MongoctlException,e:
        raise e
    except Exception, e:
        raise MongoctlException("Unable to load %s "
                                "config file: %s: %s" % (name, path_or_url, e))

###############################################################################
def read_json_string(path_or_url, validate_exists=True):
    path_or_url = to_full_config_path(path_or_url)
    # if the path is just filename then append config root

    # check if its a file
    if not is_url(path_or_url):
        if os.path.isfile(path_or_url):
            return open(path_or_url).read()
        elif validate_exists:
            raise MongoctlException("Config file %s does not exist." %
                                    path_or_url)
        else:
            return None

    # Then its url
    response = urllib.urlopen(path_or_url)

    if response.getcode() != 200:
        msg = ("Unable to open url '%s' (response code '%s')."
               % (path_or_url, response.getcode()))

        if validate_exists:
            raise MongoctlException(msg)
        else:
            log_verbose(msg)
            return None
    else:
        return response.read()
