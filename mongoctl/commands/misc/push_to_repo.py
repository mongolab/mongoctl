__author__ = 'abdul'

import os
import platform
import urllib
import shutil



from mongoctl.mongoctl_logging import *

from mongoctl.errors import MongoctlException

from mongoctl.utils import call_command, which
from mongoctl.binary_repo import get_binary_repository
from mongoctl.mongo_version import make_version_info, MongoDBEdition

from mongoctl.commands.command_utils import get_mongo_installation

###############################################################################
# push to repo command
###############################################################################
def push_to_repo_command(parsed_options):
    push_mongodb(parsed_options.repo,
                 parsed_options.version,
                 mongodb_edition=parsed_options.edition)



###############################################################################
# push_mongodb
###############################################################################
def push_mongodb(repo_name, mongodb_version, mongodb_edition=None):
    """

    :param repo_name:
    :param mongodb_version:
    :param mongodb_edition:
    :return:
    """
    mongodb_edition = mongodb_edition or MongoDBEdition.COMMUNITY
    repo = get_binary_repository(repo_name)
    version_info = make_version_info(mongodb_version, mongodb_edition)
    mongodb_install_dir = get_mongo_installation(version_info)

    if not mongodb_install_dir:
        raise MongoctlException("No mongodb installation found for '%s'" %
                                version_info)

    target_archive_name = repo.get_archive_name(mongodb_version,
                                                mongodb_edition)

    log_info("Taring MongoDB at '%s'" % mongodb_install_dir)

    tar_exe = which("tar")
    tar_cmd = [tar_exe, "-cvzf", target_archive_name, mongodb_install_dir]
    call_command(tar_cmd)

    log_info("Uploading tar to repo")

    repo.upload_file(mongodb_version, mongodb_edition,
                     target_archive_name)

    # cleanup
    log_info("Cleanup")
    try:

        os.remove(target_archive_name)
    except Exception, e:
        log_error(str(e))
        log_exception(e)













