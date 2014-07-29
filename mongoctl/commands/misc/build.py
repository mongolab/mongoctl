__author__ = 'abdul'

import os
import platform
import urllib
import shutil



from mongoctl.mongoctl_logging import *

from mongoctl.errors import MongoctlException

from mongoctl.utils import download_url, extract_archive, call_command, which
from mongoctl.binary_repo import get_binary_repository
from mongoctl.mongo_version import MongoDBEdition
from mongoctl import config

###############################################################################
# build command
###############################################################################
def build_command(parsed_options):
    build_mongodb(parsed_options.version,
                  repo_name=parsed_options.repo,
                  push=parsed_options.push,
                  ssl=parsed_options.ssl)



###############################################################################
# build_mongodb
###############################################################################
def build_mongodb(mongodb_version, repo_name=None, push=False,
                  ssl=False):
    """

    :param version:
    :param ssl:
    :param repo_name: The repo to use to generate archive name
    :return:
    """
    repo_name = repo_name or "default"
    repo = get_binary_repository(repo_name)
    mongodb_edition = (MongoDBEdition.COMMUNITY_SSL if ssl
                       else MongoDBEdition.COMMUNITY)
    source_archive_name = "r%s.tar.gz" % mongodb_version

    target_archive_name = repo.get_archive_name(mongodb_version,
                                                mongodb_edition)

    target_dir_name = target_archive_name.replace(".tgz", "").replace(
        ".tar.gz", "")

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

    if ssl:
        scons_cmd.append("--ssl")

    log_info("Running scons command: %s" % " ".join(scons_cmd))

    call_command(scons_cmd, cwd=source_dir)

    if push:
        tar_exe = which("tar")
        tar_cmd = [tar_exe, "-cvzf", target_archive_name, target_path]
        call_command(tar_cmd)
        repo.upload_file(mongodb_version, mongodb_edition,
                         target_archive_name)

    # cleanup
    log_info("Cleanup")
    try:
        shutil.rmtree(source_archive_name)
        shutil.rmtree(source_dir)
        if push:
            shutil.rmtree(target_archive_name)
    except Exception, e:
        log_error(str(e))
        log_exception(e)













