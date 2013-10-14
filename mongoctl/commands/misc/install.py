__author__ = 'abdul'

import os
import platform
import urllib
import shutil

from mongoctl.mongo_version import is_valid_version

import mongoctl.config as config
from mongoctl.prompt import prompt_execute_task, is_interactive_mode

from mongoctl.utils import ensure_dir, dir_exists
from mongoctl.mongoctl_logging import log_info, log_error, log_exception

from mongoctl.errors import MongoctlException

from mongoctl.utils import call_command, which

from mongoctl.mongo_version import version_obj
from mongoctl.commands.command_utils import find_all_executables

###############################################################################
# CONSTS
###############################################################################
LATEST_VERSION_FILE_URL = "https://raw.github.com/mongolab/mongoctl/master/" \
                          "mongo_latest_stable_version.txt"


###############################################################################
# install command
###############################################################################
def install_command(parsed_options):
    install_mongodb(version=parsed_options.version)

###############################################################################
# uninstall command
###############################################################################
def uninstall_command(parsed_options):
    uninstall_mongodb(version=parsed_options.version)


###############################################################################
# list-versions command
###############################################################################
def list_versions_command(parsed_options):
    mongo_installations = find__all_mongo_installations()

    bar = "-" * 80
    print bar
    formatter = "%-20s %s"
    print formatter % ("VERSION", "LOCATION")
    print bar

    for install_dir,version in mongo_installations:
        print formatter % (version, install_dir)
    print "\n"


###############################################################################
# install_mongodb
###############################################################################
def install_mongodb(version):

    bits = platform.architecture()[0].replace("bit", "")
    os_name = platform.system().lower()

    if os_name == 'darwin' and platform.mac_ver():
        os_name = "osx"

    return do_install_mongodb(os_name, bits, version)

###############################################################################
def do_install_mongodb(os_name, bits, version):

    if version is None:
        version = fetch_latest_stable_version()
        log_info("Installing latest stable MongoDB version '%s'..." % version)
    # validate version string
    elif not is_valid_version(version):
        raise MongoctlException("Invalid version '%s'. Please provide a"
                                " valid MongoDB version." % version)

    mongodb_installs_dir = config.get_mongodb_installs_dir()
    if not mongodb_installs_dir:
        raise MongoctlException("No mongoDBInstallationsDirectory configured"
                                " in mongoctl.config")

    platform_spec = get_validate_platform_spec(os_name, bits)

    log_info("Running install for %s %sbit to "
             "mongoDBInstallationsDirectory (%s)..." % (os_name, bits,
                                                        mongodb_installs_dir))


    mongo_installation = get_mongo_installation(version)

    if mongo_installation is not None: # no-op
        log_info("You already have MongoDB %s installed ('%s'). "
                 "Nothing to do." % (version, mongo_installation))
        return mongo_installation

    archive_name = "mongodb-%s-%s.tgz" % (platform_spec, version)
    url = "http://fastdl.mongodb.org/%s/%s" % (os_name, archive_name)

    # Validate if the version exists
    response = urllib.urlopen(url)

    if response.getcode() != 200:
        msg = ("Unable to download from url '%s' (response code '%s'). "
               "It could be that version '%s' you specified does not exist."
               " Please double check the version you provide" %
               (url, response.getcode(), version))
        raise MongoctlException(msg)

    mongo_dir_name = "mongodb-%s-%s" % (platform_spec, version)
    install_dir = os.path.join(mongodb_installs_dir, mongo_dir_name)

    ensure_dir(mongodb_installs_dir)

    # XXX LOOK OUT! Two processes installing same version simultaneously => BAD.
    # TODO: mutex to protect the following

    if not dir_exists(install_dir):
        try:
            ## download the url
            download(url)
            extract_archive(archive_name)

            log_info("Moving extracted folder to %s" % mongodb_installs_dir)
            shutil.move(mongo_dir_name, mongodb_installs_dir)

            os.remove(archive_name)
            log_info("Deleting archive %s" % archive_name)

            log_info("MongoDB %s installed successfully!" % version)
            return install_dir
        except Exception, e:
            log_exception(e)
            log_error("Failed to install MongoDB '%s'. Cause: %s" %
                      (version, e))

###############################################################################
# uninstall_mongodb
###############################################################################
def uninstall_mongodb(version):

    # validate version string
    if not is_valid_version(version):
        raise MongoctlException("Invalid version '%s'. Please provide a"
                                " valid MongoDB version." % version)

    mongo_installation = get_mongo_installation(version)

    if mongo_installation is None: # no-op
        msg = ("Cannot find a MongoDB installation for version '%s'. Please"
               " use list-versions to see all possible versions " % version)
        log_info(msg)
        return

    log_info("Found MongoDB '%s' in '%s'" % (version, mongo_installation))

    def rm_mongodb():
        log_info("Deleting '%s'" % mongo_installation)
        shutil.rmtree(mongo_installation)
        log_info("MongoDB '%s' Uninstalled successfully!" % version)

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
def get_mongo_installation(version_str):
    # get all mongod installation dirs and return the one
    # whose version == specified version. If any...
    version = version_obj(version_str)
    for install_dir, install_version in find__all_mongo_installations():
        if install_version == version:
            return install_dir

    return None

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
def download(url):
    log_info("Downloading %s..." % url)

    if which("curl"):
        download_cmd = ['curl', '-O']
        if not is_interactive_mode():
            download_cmd.append('-Ss')
    elif which("wget"):
        download_cmd = ['wget']
    else:
        msg = ("Cannot download file.You need to have 'curl' or 'wget"
               "' command in your path in order to proceed.")
        raise MongoctlException(msg)

    download_cmd.append(url)
    call_command(download_cmd)

###############################################################################
def extract_archive(archive_name):
    log_info("Extracting %s..." % archive_name)
    if not which("tar"):
        msg = ("Cannot extract archive.You need to have 'tar' command in your"
               " path in order to proceed.")
        raise MongoctlException(msg)

    tar_cmd = ['tar', 'xvf', archive_name]
    call_command(tar_cmd)




