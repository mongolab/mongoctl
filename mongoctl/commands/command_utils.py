__author__ = 'abdul'

import os
import re

import mongoctl.repository as repository
from mongoctl.mongoctl_logging import *
from mongoctl import config
from mongoctl.errors import MongoctlException
from mongoctl.utils import is_exe, which, resolve_path, execute_command
from mongoctl.mongodb_version import make_version_info, MongoDBEdition
from mongoctl.mongo_uri_tools import is_mongo_uri

###############################################################################
# CONSTS
###############################################################################

MONGO_HOME_ENV_VAR = "MONGO_HOME"

MONGO_VERSIONS_ENV_VAR = "MONGO_VERSIONS"

# VERSION CHECK PREFERENCE CONSTS
VERSION_PREF_EXACT = 0
VERSION_PREF_GREATER = 1
VERSION_PREF_MAJOR_GE = 2
VERSION_PREF_LATEST_STABLE = 3
VERSION_PREF_EXACT_OR_MINOR = 4

def extract_mongo_exe_options(parsed_args, supported_options):
    options_extract = {}

    # Iterating over parsed options dict
    # Yeah in a hacky way since there is no clean documented way of doing that
    # See http://bugs.python.org/issue11076 for more details
    # this should be changed when argparse provides a cleaner way

    for (option_name,option_val) in parsed_args.__dict__.items():
        if option_name in supported_options and option_val is not None:
            options_extract[option_name] = option_val

    return options_extract



###############################################################################
def get_mongo_executable(version_info,
                         executable_name,
                         version_check_pref=VERSION_PREF_EXACT):

    mongo_home = os.getenv(MONGO_HOME_ENV_VAR)
    mongo_installs_dir = config.get_mongodb_installs_dir()
    version_number = version_info and version_info.version_number
    mongodb_edition = version_info and (version_info.edition or
                                            MongoDBEdition.COMMUNITY)

    ver_disp = "[Unspecified]" if version_number is None else version_number
    log_verbose("Looking for a compatible %s for mongoVersion=%s." %
                (executable_name, ver_disp))
    exe_version_tuples = find_all_executables(executable_name)

    if len(exe_version_tuples) > 0:
        selected_exe = best_executable_match(executable_name,
                                             exe_version_tuples,
                                             version_info,
                                             version_check_pref=
                                             version_check_pref)
        if selected_exe is not None:
            log_info("Using %s at '%s' version '%s'..." %
                     (executable_name,
                      selected_exe.path,
                      selected_exe.version))
            return selected_exe

    ## ok nothing found at all. wtf case
    msg = ("Unable to find a compatible '%s' executable "
           "for version %s (edition %s). You may need to run 'mongoctl "
           "install-mongodb %s %s' to install it.\n\n"
           "Here is your enviroment:\n\n"
           "$PATH=%s\n\n"
           "$MONGO_HOME=%s\n\n"
           "mongoDBInstallationsDirectory=%s (in mongoctl.config)" %
           (executable_name, ver_disp, mongodb_edition, ver_disp,
            "--edition %s" % mongodb_edition if
            mongodb_edition != MongoDBEdition.COMMUNITY else "",
            os.getenv("PATH"),
            mongo_home,
            mongo_installs_dir))

    raise MongoctlException(msg)

###############################################################################
def find_all_executables(executable_name):
    # create a list of all available executables found and then return the best
    # match if applicable
    executables_found = []

    ####### Look in $PATH
    path_executable = which(executable_name)
    if path_executable is not None:
        add_to_executables_found(executables_found, path_executable)

    #### Look in $MONGO_HOME if set
    mongo_home = os.getenv(MONGO_HOME_ENV_VAR)

    if mongo_home is not None:
        mongo_home = resolve_path(mongo_home)
        mongo_home_exe = get_mongo_home_exe(mongo_home, executable_name)
        add_to_executables_found(executables_found, mongo_home_exe)
        # Look in mongod_installs_dir if set
    mongo_installs_dir = config.get_mongodb_installs_dir()

    if mongo_installs_dir is not None:
        if os.path.exists(mongo_installs_dir):
            for mongo_installation in os.listdir(mongo_installs_dir):
                child_mongo_home = os.path.join(mongo_installs_dir,
                                                mongo_installation)

                child_mongo_exe = get_mongo_home_exe(child_mongo_home,
                                                     executable_name)

                add_to_executables_found(executables_found, child_mongo_exe)

    return get_exe_version_tuples(executables_found)

###############################################################################
def add_to_executables_found(executables_found, executable):
    if is_valid_mongo_exe(executable):
        if executable not in executables_found:
            executables_found.append(executable)
    else:
        log_verbose("Not a valid executable '%s'. Skipping..." % executable)

###############################################################################
def best_executable_match(executable_name,
                          exe_version_tuples,
                          version_object,
                          version_check_pref=VERSION_PREF_EXACT):

    match_func = exact_exe_version_match

    exe_versions_str = exe_version_tuples_to_strs(exe_version_tuples)

    log_verbose("Found the following %s's. Selecting best match "
                "for version %s\n%s" %(executable_name, version_object,
                                       exe_versions_str))

    if version_object is None:
        log_verbose("mongoVersion is null. "
                    "Selecting default %s" % executable_name)
        match_func = default_match
    elif version_check_pref == VERSION_PREF_LATEST_STABLE:
        match_func = latest_stable_exe
    elif version_check_pref == VERSION_PREF_MAJOR_GE:
        match_func = major_ge_exe_version_match
    elif version_check_pref == VERSION_PREF_EXACT_OR_MINOR:
        match_func = exact_or_minor_exe_version_match

    return match_func(executable_name, exe_version_tuples, version_object)

###############################################################################
def default_match(executable_name, exe_version_tuples, version):
    default_exe = latest_stable_exe(executable_name, exe_version_tuples)
    if default_exe is None:
        log_verbose("No stable %s found. Looking for any latest available %s "
                    "..." % (executable_name, executable_name))
        default_exe = latest_exe(executable_name, exe_version_tuples)
    return default_exe

###############################################################################
def exact_exe_version_match(executable_name, exe_version_tuples, version):

    for mongo_exe,exe_version in exe_version_tuples:
        if exe_version == version:
            return mongo_exe_object(mongo_exe, exe_version)

    return None

###############################################################################
def latest_stable_exe(executable_name, exe_version_tuples, version=None):
    log_verbose("Find the latest stable %s" % executable_name)
    # find greatest stable exe
    # hold values in a list of (exe,version) tuples
    stable_exes = []
    for mongo_exe,exe_version in exe_version_tuples:
        # get the release number (e.g. A.B.C, release number is B here)
        release_num = exe_version.parts[0][1]
        # stable releases are the even ones
        if (release_num % 2) == 0:
            stable_exes.append((mongo_exe, exe_version))

    return latest_exe(executable_name, stable_exes)

###############################################################################
def latest_exe(executable_name, exe_version_tuples, version=None):

    # Return nothing if nothing compatible
    if len(exe_version_tuples) == 0:
        return None
        # sort desc by version
    exe_version_tuples.sort(key=lambda t: t[1], reverse=True)

    exe = exe_version_tuples[0]
    return mongo_exe_object(exe[0], exe[1])

###############################################################################
def major_ge_exe_version_match(executable_name, exe_version_tuples, version):
    # find all compatible exes then return closet match (min version)
    # hold values in a list of (exe,version) tuples
    compatible_exes = []
    for mongo_exe,exe_version in exe_version_tuples:
        if (exe_version.edition == version.edition and
            exe_version.parts[0][0] >= version.parts[0][0]):
            compatible_exes.append((mongo_exe, exe_version))

    # Return nothing if nothing compatible
    if len(compatible_exes) == 0:
        return None
        # find the best fit
    compatible_exes.sort(key=lambda t: t[1])
    exe = compatible_exes[-1]
    return mongo_exe_object(exe[0], exe[1])

###############################################################################
def exact_or_minor_exe_version_match(executable_name,
                                     exe_version_tuples,
                                     version):
    """
    IF there is an exact match then use it
     OTHERWISE try to find a minor version match
    """
    exe = exact_exe_version_match(executable_name,
                                  exe_version_tuples,
                                  version)

    if not exe:
        exe = minor_exe_version_match(executable_name,
                                      exe_version_tuples,
                                      version)
    return exe

###############################################################################
def minor_exe_version_match(executable_name,
                            exe_version_tuples,
                            version):

    # hold values in a list of (exe,version) tuples
    compatible_exes = []
    for mongo_exe,exe_version in exe_version_tuples:
        # compatible ==> major + minor equality
        if (exe_version.edition == version.edition and
            exe_version.parts[0][0] == version.parts[0][0] and
            exe_version.parts[0][1] == version.parts[0][1]):
            compatible_exes.append((mongo_exe, exe_version))

    # Return nothing if nothing compatible
    if len(compatible_exes) == 0:
        return None
        # find the best fit
    compatible_exes.sort(key=lambda t: t[1])
    exe = compatible_exes[-1]
    return mongo_exe_object(exe[0], exe[1])

###############################################################################
def get_exe_version_tuples(executables):
    exe_ver_tuples = []
    for mongo_exe in executables:
        try:
            exe_version = mongo_exe_version(mongo_exe)
            exe_ver_tuples.append((mongo_exe, exe_version))
        except Exception, e:
            log_exception(e)
            log_verbose("Skipping executable '%s': %s" % (mongo_exe, e))

    return exe_ver_tuples

###############################################################################
def exe_version_tuples_to_strs(exe_ver_tuples):
    strs = []
    for mongo_exe,exe_version in exe_ver_tuples:
        strs.append("%s = %s" % (mongo_exe, exe_version))
    return "\n".join(strs)

###############################################################################
def is_valid_mongo_exe(path):
    return path is not None and is_exe(path)


###############################################################################
def get_mongo_home_exe(mongo_home, executable_name):
    return os.path.join(mongo_home, 'bin', executable_name)

###############################################################################
def mongo_exe_version(mongo_exe):
    mongod_path = os.path.join(os.path.dirname(mongo_exe), "mongod")

    try:
        re_expr = "v?((([0-9]+)\.([0-9]+)\.([0-9]+))([^, ]*))"
        vers_spew = execute_command([mongod_path, "--version"])
        # only take first line of spew
        vers_spew_line = vers_spew.split('\n')[0]
        vers_grep = re.findall(re_expr, vers_spew_line)
        help_spew = execute_command([mongod_path, "--help"])
        full_version = vers_grep[-1][0]
        if "subscription" in vers_spew or "enterprise" in vers_spew:
            edition = MongoDBEdition.ENTERPRISE
        elif "SSL" in help_spew:
            edition = MongoDBEdition.COMMUNITY_SSL
        else:
            edition = MongoDBEdition.COMMUNITY

        result = make_version_info(full_version, edition=edition)
        if result is not None:
            return result
        else:
            raise MongoctlException("Cannot parse mongo version from the"
                                    " output of '%s --version'" % mongod_path)
    except Exception, e:
        log_exception(e)
        raise MongoctlException("Unable to get mongo version of '%s'."
                                " Cause: %s" % (mongod_path, e))

###############################################################################
class MongoExeObject():
    pass

###############################################################################
def mongo_exe_object(exe_path, exe_version):
    exe_obj = MongoExeObject()
    exe_obj.path =  exe_path
    exe_obj.version =  exe_version

    return exe_obj

###############################################################################
def options_to_command_args(args):

    command_args=[]

    for (arg_name,arg_val) in sorted(args.iteritems()):
    # append the arg name and val as needed
        if not arg_val:
            continue
        elif arg_val is True:
            command_args.append("--%s" % arg_name)
        else:
            command_args.append("--%s" % arg_name)
            command_args.append(str(arg_val))

    return command_args


###############################################################################
def is_server_or_cluster_db_address(value):
    """
    checks if the specified value is in the form of
    [server or cluster id][/database]
    """
    # check if value is an id string
    id_path = value.split("/")
    id = id_path[0]
    return len(id_path) <= 2 and (repository.lookup_server(id) or
                                  repository.lookup_cluster(id))

###############################################################################
def is_db_address(value):
    """
    Checks if the specified value is a valid mongoctl database address
    """
    return value and (is_mongo_uri(value) or
                      is_server_or_cluster_db_address(value))


###############################################################################
def is_dbpath(value):
    """
    Checks if the specified value is a dbpath. dbpath could be an absolute
    file path, relative path or a file uri
    """

    value = resolve_path(value)
    return os.path.exists(value)


###############################################################################
def find__all_mongo_installations():
    all_installs = []
    all_mongod_exes = find_all_executables('mongod')
    for exe_path, exe_version in all_mongod_exes:
        # install dir is exe parent's (bin) parent
        install_dir = os.path.dirname(os.path.dirname(exe_path))
        all_installs.append((install_dir,exe_version))

    return all_installs


###############################################################################
def get_mongo_installation(version_info):
    # get all mongod installation dirs and return the one
    # whose version == specified version. If any...
    for install_dir, install_version in find__all_mongo_installations():
        if install_version == version_info:
            return install_dir

    return None


