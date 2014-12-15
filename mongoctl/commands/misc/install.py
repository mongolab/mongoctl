__author__ = 'abdul'

import platform
import urllib
import shutil


import mongoctl.config as config
from mongoctl.prompt import prompt_execute_task

from mongoctl.mongodb_version import MongoDBEdition

from mongoctl.mongoctl_logging import *

from mongoctl.errors import MongoctlException

from mongoctl.utils import (
    download_url, extract_archive, call_command, which, ensure_dir,
    validate_openssl, execute_command, list_dir_files, is_exe
)

from mongoctl.mongodb_version import make_version_info, is_valid_version_info
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
                    from_source=parsed_options.fromSource,
                    build_threads=parsed_options.buildThreads,
                    build_tmp_dir=parsed_options.buildTmpDir,
                    include_only=parsed_options.includeOnly)

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
def install_mongodb(mongodb_version, mongodb_edition=None, from_source=False,
                    build_threads=1,
                    build_tmp_dir=None,
                    include_only=None):

    if mongodb_version is None:
        mongodb_version = fetch_latest_stable_version()
        log_info("Installing latest stable MongoDB version '%s'..." %
                 mongodb_version)

    version_info = make_version_info(mongodb_version, mongodb_edition)
    mongo_installation = get_mongo_installation(version_info)
    mongodb_edition = version_info.edition

    if mongo_installation is not None: # no-op
        log_info("You already have MongoDB %s installed ('%s'). "
                 "Nothing to do." % (version_info, mongo_installation))
        return mongo_installation

    target_dir = get_install_target_dir(mongodb_version, mongodb_edition)
    if os.path.exists(target_dir):
        raise MongoctlException("Target directory '%s' already exists" %
                                target_dir)



    if mongodb_edition not in MongoDBEdition.ALL:
        raise MongoctlException("Unknown edition '%s'. Please select from %s" %
                                (mongodb_edition, MongoDBEdition.ALL))

    if from_source:
        install_from_source(mongodb_version, mongodb_edition,
                            build_threads=build_threads,
                            build_tmp_dir=build_tmp_dir)
        return

    bits = platform.architecture()[0].replace("bit", "")
    os_name = platform.system().lower()

    if os_name == 'darwin' and platform.mac_ver():
        os_name = "osx"

    mongodb_installs_dir = config.get_mongodb_installs_dir()
    if not mongodb_installs_dir:
        raise MongoctlException("No mongoDBInstallationsDirectory configured"
                                " in mongoctl.config")

    # ensure the mongo installs dir
    ensure_dir(mongodb_installs_dir)

    platform_spec = get_validate_platform_spec(os_name, bits)

    log_verbose("INSTALL_MONGODB: OS='%s' , BITS='%s' , VERSION='%s', "
                "PLATFORM_SPEC='%s'" % (os_name, bits, version_info,
                                        platform_spec))





    # XXX LOOK OUT! Two processes installing same version simultaneously => BAD.
    # TODO: mutex to protect the following

    try:
        ## download the url
        archive_path = download_mongodb_binary(mongodb_version,
                                               mongodb_edition)
        archive_name = os.path.basename(archive_path)

        mongo_dir_name = extract_archive(archive_name)

        # apply include_only if specified
        if include_only:
            apply_include_only(mongo_dir_name, include_only)

        log_info("Deleting archive %s" % archive_name)
        os.remove(archive_name)
        target_dir_name = os.path.basename(target_dir)
        os.rename(mongo_dir_name, target_dir_name)

        # move target to mongodb install dir (Unless target is already there!
        # i.e current working dir == mongodb_installs_dir
        if os.getcwd() != mongodb_installs_dir:
            log_info("Moving extracted folder to %s" % mongodb_installs_dir)
            shutil.move(target_dir_name, mongodb_installs_dir)

        log_info("MongoDB %s installed successfully!" % version_info)
        install_dir = os.path.join(mongodb_installs_dir, mongo_dir_name)
        return install_dir
    except Exception, e:
        log_exception(e)
        msg = "Failed to install MongoDB '%s'. Cause: %s" % (version_info, e)
        raise MongoctlException(msg)


###############################################################################
# install from source
###############################################################################
def install_from_source(mongodb_version, mongodb_edition, build_threads=None,
                        build_tmp_dir=None):
    """

    :param version:
    :param ssl:
    :param repo_name: The repo to use to generate archive name
    :return:
    """

    if build_tmp_dir:
        ensure_dir(build_tmp_dir)
        os.chdir(build_tmp_dir)

    allowed_build_editions = [MongoDBEdition.COMMUNITY,
                              MongoDBEdition.COMMUNITY_SSL]
    if mongodb_edition not in allowed_build_editions:
        raise MongoctlException("build is only allowed for %s editions" %
                                allowed_build_editions)

    log_info("Installing MongoDB '%s %s' from source" % (mongodb_version,
                                                         mongodb_edition))
    source_archive_name = "r%s.tar.gz" % mongodb_version

    target_dir = get_install_target_dir(mongodb_version, mongodb_edition)

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


    scons_cmd = [scons_exe, "core", "tools", "install"]
    if build_threads:
        scons_cmd.extend(["-j", str(build_threads)])

    scons_cmd.append("--prefix=%s" % target_dir)

    if mongodb_edition == MongoDBEdition.COMMUNITY_SSL:
        validate_openssl()
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
        # make sure that the mongo installation to be removed does not have
        # any running processes
        ensure_mongo_home_not_used(mongo_installation)
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
def get_install_target_dir(mongodb_version, mongodb_edition):
    template = "mongodb-{platform_spec}-{mongodb_edition}-{mongodb_version}"
    args = get_template_args(mongodb_version, mongodb_edition)
    dir_name = template.format(**args)

    return os.path.join(config.get_mongodb_installs_dir(), dir_name)



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

###############################################################################
def ensure_mongo_home_not_used(mongo_installation):
    output = execute_command([
        "ps",
        "-eaf"
    ])

    if mongo_installation in output:
        msg = ("ERROR: Cannot uninstall '%s' because its currently being used. "
               "Please terminate all running processes then try again." %
               mongo_installation)
        raise MongoctlException(msg)

###############################################################################
def apply_include_only(mongo_dir_name, include_only):
    """

    :param mongo_dir_name:
    :param include_only: list of exes names to include only
    :return:
    """
    log_info("Keep include-only files (%s) from new mongo installation..." %
             include_only)
    bin_dir = os.path.join(mongo_dir_name, "bin")
    exes = list_dir_files(bin_dir)
    for exe_name in exes:
        # we always keep mongod because it used to determine mongo
        # installation version
        if exe_name == "mongod":
            continue
        exe_path = os.path.join(bin_dir, exe_name)
        if is_exe(exe_path):
            if exe_name not in include_only:
                log_info("Removing %s" % exe_name)
                os.remove(exe_path)




