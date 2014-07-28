__author__ = 'abdul'

import os
import platform
import urllib
import shutil



from mongoctl.mongoctl_logging import *

from mongoctl.errors import MongoctlException

from mongoctl.utils import download_url, extract_archive, call_command, which


###############################################################################
# build command
###############################################################################
def build_command(parsed_options):
    build_mongodb(parsed_options.version, ssl=parsed_options.ssl)



###############################################################################
# build_mongodb
###############################################################################
def build_mongodb(version, ssl=False):
    archive_name = "r%s.tar.gz" % version
    source_url = ("https://github.com/mongodb/mongo/archive/%s" % archive_name)

    response = urllib.urlopen(source_url)

    if response.getcode() != 200:
        msg = ("Unable to find a mongodb release for version '%s' in MongoDB"
               " github repo. See https://github.com/mongodb/mongo/releases "
               "for possible releases (response code '%s'). " %
               (version, response.getcode()))
        raise MongoctlException(msg)

    log_info("Downloading MongoDB '%s' source from github '%s' ..." %
             (version, source_url))

    download_url(source_url)

    log_info("Extract source archive ...")

    source_dir = extract_archive(archive_name)

    log_info("Building with scons!")

    scons_exe = which("scons")
    if not scons_exe:
        raise MongoctlException("scons command not found in your path")
    call_command([scons_exe, "all"], cwd=source_dir)











