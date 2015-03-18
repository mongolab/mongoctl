__author__ = 'abdul'

import os
import subprocess
import pwd
import time
import socket

import urlparse
import json

from bson import json_util
from mongoctl_logging import *
from errors import MongoctlException


###############################################################################
def namespace_get_property(namespace, name):
    if hasattr(namespace, name):
        return getattr(namespace,name)

    return None

###############################################################################

def to_string(thing):
    return "" if thing is None else str(thing)

###############################################################################
def document_pretty_string(document):
    return json.dumps(document, indent=4, default=json_util.default)

###############################################################################
def listify(object):
    if isinstance(object, list):
        return object

    return [object]

###############################################################################
def is_url(value):
    scheme = urlparse.urlparse(value).scheme
    return  scheme is not None and scheme != ''


###############################################################################
def wait_for(predicate, timeout=None, sleep_duration=2, grace=True):
    start_time = now()
    must_retry = may_retry = not predicate()

    if must_retry and grace:
        # optimizing for predicates whose first invocations may be slooooooow
        log_verbose("GRACE: First eval finished in %d secs - resetting timer." %
                    (now() - start_time))
        start_time = now()

    while must_retry and may_retry:

        must_retry = not predicate()
        if must_retry:
            net_time = now() - start_time
            if timeout and net_time + sleep_duration > timeout:
                may_retry = False
            else:
                left = "[-%d sec] " % (timeout - net_time) if timeout else ""
                log_info("-- waiting %s--" % left)
                time.sleep(sleep_duration)

    return not must_retry

###############################################################################
def now():
    return time.time()

###############################################################################
# OS Functions
###############################################################################
def which(program):

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

###############################################################################
def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

###############################################################################
def ensure_dir(dir_path):
    """
    If DIR_PATH does not exist, makes it. Failing that, raises Exception.
    Returns True if dir already existed; False if it had to be made.
    """
    exists = dir_exists(dir_path)
    if not exists:
        try:
            os.makedirs(dir_path)
        except(Exception,RuntimeError), e:
            raise Exception("Unable to create directory %s. Cause %s" %
                            (dir_path, e))
    return exists

###############################################################################
def dir_exists(path):
    return os.path.exists(path) and os.path.isdir(path)

###############################################################################
def list_dir_files(path):
    return [name for name in os.listdir(path) if
            os.path.isfile(os.path.join(path, name))]
###############################################################################
def resolve_path(path):
    # handle file uris
    path = path.replace("file://", "")

    # expand vars
    path =  os.path.expandvars(custom_expanduser(path))
    # Turn relative paths to absolute
    try:
        path = os.path.abspath(path)
    except OSError, e:
        # handle the case where cwd does not exist
        if "No such file or directory" in str(e):
            pass
        else:
            raise

    return path

###############################################################################
def custom_expanduser(path):
    if path.startswith("~"):
        login = get_current_login()
        home_dir = os.path.expanduser( "~%s" % login)
        path = path.replace("~", home_dir, 1)

    return path

###############################################################################
def get_current_login():
    try:
        pwuid = pwd.getpwuid(os.geteuid())
        return pwuid.pw_name
    except Exception, e:
        raise Exception("Error while trying to get current os login. %s" % e)

###############################################################################
# sub-processing functions
###############################################################################
def call_command(command, bubble_exit_code=False, **kwargs):
    try:
        return subprocess.check_call(command, **kwargs)
    except subprocess.CalledProcessError, e:
        if bubble_exit_code:
            exit(e.returncode)
        else:
            raise e

###############################################################################
def execute_command(command):

    # Python 2.7+ : Use the new method because i think its better
    if  hasattr(subprocess, 'check_output'):
        return subprocess.check_output(command,stderr=subprocess.STDOUT)
    else: # Python 2.6 compatible, check_output is not available in 2.6
        return subprocess.Popen(command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT).communicate()[0]

###############################################################################
def is_pid_alive(pid):

    try:
        os.kill(pid,0)
        return True
    except OSError:
        return False

###############################################################################
def kill_process(pid, force=False):
    signal = 9 if force else 1
    try:
        os.kill(pid, signal)
        return True
    except OSError:
        return False


###############################################################################
# HELPER functions
###############################################################################
def timedelta_total_seconds(td):
    """
    Equivalent python 2.7+ timedelta.total_seconds()
     This was added for python 2.6 compatibilty
    """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6


###############################################################################
def is_valid_member_address(address):
    if address is None:
        return False
    host_port = address.split(":")

    return (len(host_port) == 2
            and host_port[0]
            and host_port[1]
            and str(host_port[1]).isdigit())


###############################################################################
# Network Utils Functions
###############################################################################

def is_host_local(host):
    if (host == "localhost" or
                host == "127.0.0.1"):
        return True

    return is_same_host(socket.gethostname(), host)

###############################################################################
def is_same_host(host1, host2):

    """
    Returns true if host1 == host2 OR map to the same host (using DNS)
    """

    if host1 == host2:
        return True
    else:
        ips1 = get_host_ips(host1)
        ips2 = get_host_ips(host2)
        return len(set(ips1) & set(ips2)) > 0

###############################################################################
def is_same_address(addr1, addr2):
    """
    Where the two addresses are in the host:port
    Returns true if ports are equals and hosts are the same using is_same_host
    """
    hostport1 = addr1.split(":")
    hostport2 = addr2.split(":")

    return (is_same_host(hostport1[0], hostport2[0]) and
            hostport1[1] == hostport2[1])
###############################################################################
def get_host_ips(host):
    try:

        ips = []
        addr_info = socket.getaddrinfo(host, None)
        for elem in addr_info:
            ip = elem[4]
            if ip not in ips:
                ips.append(ip)

        # TODO remove this temp hack that works around the case where
        # host X has more IPs than X.foo.com.
        if len(host.split(".")) == 3:
            try:
                ips.extend(get_host_ips(host.split(".")[0]))
            except Exception, ex:
                pass

        return ips
    except Exception, e:
        raise MongoctlException("Invalid host '%s'. Cause: %s" % (host, e))

###############################################################################
def resolve_class(kls):
    if kls == "dict":
        return dict

    try:
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m
    except Exception, e:
        raise Exception("Cannot resolve class '%s'. Cause: %s" % (kls, e))


###############################################################################
def download_url(url, destination=None, show_errors=False):
    destination = destination or os.getcwd()

    log_info("Downloading %s..." % url)

    if which("curl"):
        download_cmd = ['curl', '-O', '-L']
        if show_errors:
            download_cmd.append('-Ss')
    elif which("wget"):
        download_cmd = ['wget']
    else:
        msg = ("Cannot download file.You need to have 'curl' or 'wget"
               "' command in your path in order to proceed.")
        raise MongoctlException(msg)

    download_cmd.append(url)
    call_command(download_cmd, cwd=destination)
    file_name = url.split("/")[-1]
    return os.path.join(destination, file_name)

###############################################################################
def extract_archive(archive_name):

    log_info("Extracting %s..." % archive_name)
    if not which("tar"):
        msg = ("Cannot extract archive.You need to have 'tar' command in your"
               " path in order to proceed.")
        raise MongoctlException(msg)

    dir_name = archive_name.replace(".tgz", "").replace(".tar.gz", "")

    ensure_dir(dir_name)
    tar_cmd = ['tar', 'xvf', archive_name, "-C", dir_name,
               "--strip-components", "1"]

    call_command(tar_cmd)

    return dir_name

###############################################################################
def validate_openssl():
    """
        Validates OpenSSL to ensure it has TLS_FALLBACK_SCSV supported
    """
    try:
        open_ssl_exe = which("openssl")
        if not open_ssl_exe:
            raise Exception("No openssl exe found in path")

        try:
            # execute a an invalid command to get output with available options
            # since openssl does not have a --help option unfortunately
            execute_command([open_ssl_exe, "s_client", "invalidDummyCommand"])
        except subprocess.CalledProcessError as e:
            if "fallback_scsv" not in e.output:
                raise Exception("openssl does not support TLS_FALLBACK_SCSV")
    except Exception as e:
        raise MongoctlException("Unsupported OpenSSL. %s" % e)

###############################################################################

def time_string(time_seconds):
    days, remainder = divmod(time_seconds, 3600 * 24)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    result = []
    if days:
        result.append("%d day(s)" % days)
    if days or hours:
        result.append("%d hour(s)" % hours)
    if days or hours or minutes:
        result.append("%d minute(s)" % minutes)

    result.append("%d second(s)" % seconds)

    return " ".join(result)


