__author__ = 'abdul'

import platform
import urllib
import shutil


import mongoctl.config as config
from mongoctl.prompt import prompt_execute_task

from mongoctl.mongo_version import MongoDBEdition

from mongoctl.mongoctl_logging import *

from mongoctl.errors import MongoctlException

from mongoctl.utils import download_url, extract_archive, call_command, which

from mongoctl.mongo_version import make_version_info, is_valid_version_info
from mongoctl.commands.command_utils import (
    find__all_mongo_installations, get_mongo_installation
)

from mongoctl.binary_repo import download_mongodb_binary, get_template_args

###############################################################################
# CONSTS
###############################################################################
LATEST_VERSION_FILE_URL = "https://raw.github.com/mongolab/mongoctl/master/" \
                          "mongo_latest_stable_version.txt"


###############################################################################
# install command
###############################################################################
def install_command(parsed_options):
    install_mongodb(parsed_options.version,
                    mongodb_edition=parsed_options.edition,
                    from_source=parsed_options.fromSource)

###############################################################################
# uninstall command
###############################################################################
def uninstall_command(parsed_options):
    uninstall_mongodb(parsed_options.version, edition=parsed_options.edition)


###############################################################################
# list-versions command
###############################################################################
def list_versions_command(parsed_options):
    mongo_installations = find__all_mongo_installations()

    bar = "-" * 80
    print bar
    formatter = "%-20s %-20s %s"
    print formatter % ("VERSION", "EDITION", "LOCATION")
    print bar

    for install_dir,version in mongo_installations:
        print formatter % (version.version_number,
                           version.edition, install_dir)
    print "\n"


###############################################################################
# install_mongodb
###############################################################################
def install_mongodb(mongob_version, mongodb_edition=None, from_source=False):

    mongodb_edition = mongodb_edition or MongoDBEdition.COMMUNITY
    if mongodb_edition not in MongoDBEdition.ALL:
        raise MongoctlException("Unknown edition '%s'. Please select from %s" %
                                (mongodb_edition, MongoDBEdition.ALL))

    if from_source:
        install_from_source(mongob_version, mongodb_edition)
        return

    bits = platform.architecture()[0].replace("bit", "")
    os_name = platform.system().lower()

    if os_name == 'darwin' and platform.mac_ver():
        os_name = "osx"

    if mongob_version is None:
        version_number = fetch_latest_stable_version()
        log_info("Installing latest stable MongoDB version '%s'..." %
                 version_number)

    version_info = make_version_info(mongob_version, mongodb_edition)

    mongodb_installs_dir = config.get_mongodb_installs_dir()
    if not mongodb_installs_dir:
        raise MongoctlException("No mongoDBInstallationsDirectory configured"
                                " in mongoctl.config")

    platform_spec = get_validate_platform_spec(os_name, bits)

    log_verbose("INSTALL_MONGODB: OS='%s' , BITS='%s' , VERSION='%s', "
                "PLATFORM_SPEC='%s'" % (os_name, bits, version_info,
                                        platform_spec))

    mongo_installation = get_mongo_installation(version_info)

    if mongo_installation is not None: # no-op
        log_info("You already have MongoDB %s installed ('%s'). "
                 "Nothing to do." % (version_info, mongo_installation))
        return mongo_installation



    # XXX LOOK OUT! Two processes installing same version simultaneously => BAD.
    # TODO: mutex to protect the following

    try:
        ## download the url
        archive_path = download_mongodb_binary(mongob_version, mongodb_edition)
        archive_name = os.path.basename(archive_path)
        mongo_dir_name = archive_name.replace(".tgz", "")
        extract_archive(archive_path)

        log_info("Moving extracted folder to %s" % mongodb_installs_dir)
        shutil.move(mongo_dir_name, mongodb_installs_dir)

        os.remove(archive_name)
        log_info("Deleting archive %s" % archive_name)

        log_info("MongoDB %s installed successfully!" % version_info)
        install_dir = os.path.join(mongodb_installs_dir, mongo_dir_name)
        return install_dir
    except Exception, e:
        traceback.print_exc()
        log_exception(e)
        log_error("Failed to install MongoDB '%s'. Cause: %s" %
                  (version_info, e))


###############################################################################
# install from source
###############################################################################
def install_from_source(mongodb_version, mongodb_edition):
    """

    :param version:
    :param ssl:
    :param repo_name: The repo to use to generate archive name
    :return:
    """
    log_info("Installing MongoDB '%s %s' from source" % (mongodb_version,
                                                         mongodb_edition))
    source_archive_name = "r%s.tar.gz" % mongodb_version



    target_dir_name = get_build_target_dir_name(mongodb_version,
                                                mongodb_edition)

    source_url = ("https://github.com/mongodb/mongo/archive/%s" %
                  source_archive_name)

    response = urllib.urlopen(source_url)

    if response.getcode() != 200:
        msg = ("Unable to find a mongodb release for version '%s' in MongoDB"
               " github repo. See https://github.com/mongodb/mongo/releases "
               "for possible releases (response code '%s'). " %
               (mongodb_version, response.getcode()))
        raise MongoctlException(msg)

    log_info("Downloading MongoDB '%s' source from github '%s' ..." %
             (mongodb_version, source_url))

    download_url(source_url)

    log_info("Extract source archive ...")

    source_dir = extract_archive(source_archive_name)

    log_info("Building with scons!")

    scons_exe = which("scons")
    if not scons_exe:
        raise MongoctlException("scons command not found in your path")

    target_path = os.path.join(config.get_mongodb_installs_dir(),
                               target_dir_name)
    scons_cmd = [scons_exe, "core", "tools", "install", "-j", "4",
                 "--prefix=%s" % target_path]

    if mongodb_edition == MongoDBEdition.COMMUNITY_SSL:
        scons_cmd.append("--ssl")

    log_info("Running scons command: %s" % " ".join(scons_cmd))

    call_command(scons_cmd, cwd=source_dir)

    # cleanup
    log_info("Cleanup")
    try:
        os.remove(source_archive_name)
        shutil.rmtree(source_dir)

    except Exception, e:
        log_error(str(e))
        log_exception(e)


###############################################################################
# uninstall_mongodb
###############################################################################
def uninstall_mongodb(version_number, edition=None):

    version_info = make_version_info(version_number, edition=edition)
    # validate version string
    if not is_valid_version_info(version_info):
        raise MongoctlException("Invalid version '%s'. Please provide a"
                                " valid MongoDB version." % version_info)

    mongo_installation = get_mongo_installation(version_info)

    if mongo_installation is None: # no-op
        msg = ("Cannot find a MongoDB installation for version '%s'. Please"
               " use list-versions to see all possible versions " %
               version_info)
        log_info(msg)
        return

    log_info("Found MongoDB '%s' in '%s'" % (version_info, mongo_installation))

    def rm_mongodb():
        log_info("Deleting '%s'" % mongo_installation)
        shutil.rmtree(mongo_installation)
        log_info("MongoDB '%s' Uninstalled successfully!" % version_info)

    prompt_execute_task("Proceed uninstall?" , rm_mongodb)

###############################################################################
def fetch_latest_stable_version():
    response = urllib.urlopen(LATEST_VERSION_FILE_URL)
    if response.getcode() == 200:
        return response.read().strip()
    else:
        raise MongoctlException("Unable to fetch MongoDB latest stable version"
                                " from '%s' (Response code %s)" %
                                (LATEST_VERSION_FILE_URL, response.getcode()))



###############################################################################
def get_build_target_dir_name(mongodb_version, mongodb_edition):
    return "mongodb-%s-%s" % (mongodb_version, mongodb_edition)



###############################################################################
def get_validate_platform_spec(os_name, bits):

    if os_name not in ["linux", "osx", "win32", "sunos5"]:
        raise MongoctlException("Unsupported OS %s" % os_name)

    if bits == "64":
        return "%s-x86_64" % os_name
    else:
        if os_name == "linux":
            return "linux-i686"
        elif os_name in ["osx" , "win32"]:
            return "%s-i386" % os_name
        elif os_name == "sunos5":
            return "i86pc"




