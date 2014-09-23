__author__ = 'abdul'

import os

from mongoctl.mongoctl_logging import log_info, log_error, log_exception

from mongoctl.errors import MongoctlException

from mongoctl.utils import call_command, which
from mongoctl.binary_repo import (
    get_binary_repository, S3MongoDBBinaryRepository
)
from mongoctl.mongodb_version import make_version_info, MongoDBEdition

from mongoctl.commands.command_utils import get_mongo_installation

###############################################################################
# push to repo command
###############################################################################
def publish_mongodb_command(parsed_options):
    push_mongodb(parsed_options.repo,
                 parsed_options.version,
                 mongodb_edition=parsed_options.edition,
                 access_key=parsed_options.accessKey,
                 secret_key=parsed_options.secretKey)

###############################################################################
# push_mongodb
###############################################################################
def push_mongodb(repo_name, mongodb_version, mongodb_edition=None,
                 access_key=None, secret_key=None):
    """

    :param repo_name:
    :param mongodb_version:
    :param mongodb_edition:
    :return:
    """
    mongodb_edition = mongodb_edition or MongoDBEdition.COMMUNITY
    repo = get_binary_repository(repo_name)

    if access_key and isinstance(repo, S3MongoDBBinaryRepository):
        repo.access_key = access_key
        repo.secret_key = secret_key
        repo.validate()

    version_info = make_version_info(mongodb_version, mongodb_edition)
    mongodb_install_dir = get_mongo_installation(version_info)


    if not mongodb_install_dir:
        raise MongoctlException("No mongodb installation found for '%s'" %
                                version_info)

    mongodb_install_home = os.path.dirname(mongodb_install_dir)
    target_archive_name = repo.get_archive_name(mongodb_version,
                                                mongodb_edition)

    target_archive_path = os.path.join(mongodb_install_home,
                                       target_archive_name)

    mongodb_install_dir_name = os.path.basename(mongodb_install_dir)
    log_info("Taring MongoDB at '%s'" % mongodb_install_dir_name)

    tar_exe = which("tar")
    tar_cmd = [tar_exe, "-cvzf", target_archive_name, mongodb_install_dir_name]
    call_command(tar_cmd, cwd=mongodb_install_home)

    log_info("Uploading tar to repo")

    repo.upload_file(mongodb_version, mongodb_edition, target_archive_path)

    # cleanup
    log_info("Cleanup")
    try:
        os.remove(target_archive_path)
    except Exception, e:
        log_error(str(e))
        log_exception(e)













