from urlparse import urlparse, urlunsplit
import socket
import netifaces
import os
import logging
from subprocess import Popen, PIPE
import threading

import logging
LOG = logging.getLogger(__name__)

def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


def check_uri(uri):
    """Check that the provided *uri* is on the local host and return the
    file path.
    """
    if isinstance(uri, (list, set, tuple)):
        paths = [check_uri(ressource) for ressource in uri]
        return paths
    url = urlparse(uri)
    try:
        if url.hostname:
            url_ip = socket.gethostbyname(url.hostname)

            if url_ip not in get_local_ips():
                try:
                    os.stat(url.path)
                except OSError:
                    raise IOError(
                        "Data file %s unaccessible from this host" % uri)

    except socket.gaierror:
        logging.warning("Couldn't check file location, running anyway")

    return url.path

def logreader(stream, log_func):
    while True:
        s = stream.readline()
        if not s:
            break
        log_func(s.strip())
    stream.close()

def run_command(cmdstr):
    """Run system command"""

    import shlex
    myargs = shlex.split(str(cmdstr))

    LOG.debug("Command: " + str(cmdstr))
    LOG.debug('Command sequence= ' + str(myargs))
    try:
        proc = Popen(myargs, shell=False, stderr=PIPE, stdout=PIPE)
    except NwpPrepareError:
        LOG.exception("Failed when preparing NWP data for PPS...")

    out_reader = threading.Thread(
        target=logreader, args=(proc.stdout, LOG.info))
    err_reader = threading.Thread(
        target=logreader, args=(proc.stderr, LOG.info))
    out_reader.start()
    err_reader.start()
    out_reader.join()
    err_reader.join()

    return proc.returncode

def run_shell_command(command, use_shell=False, use_shlex=True, my_cwd=None, my_env=None, stdout_logfile=None, stderr_logfile=None, stdin=None, my_timeout=24*60*60):
    """Run the given command as a shell and get the return code, stdout and stderr
        Returns True/False and return code.
    """
  
    if use_shlex:
        import shlex
        myargs = shlex.split(str(command))
        LOG.debug('Command sequence= ' + str(myargs))
    else:
        myargs = command
    
    try:
        proc = Popen(myargs,
                     cwd=my_cwd, shell=use_shell, env=my_env,
                     stderr=PIPE, stdout=PIPE, stdin=PIPE, close_fds=True)
        
        LOG.debug("Process pid: {}".format(proc.pid))
    except OSError as e:
        LOG.error("Popen failed for command: {} with {}".format(myargs,e))
        return False
    except ValueError as e:
        LOG.error("Popen called with invalid arguments.")
        return False
    except:
        LOG.error("Popen failed for an unknown reason.")
        return False

    import signal
    
    class Alarm(Exception):
        pass
    
    def alarm_handler(signum, frame):
        raise Alarm
    
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(my_timeout)
    try:
        LOG.debug("Before call to communicate:")
        if stdin == None:
            out, err = proc.communicate()
        else:
            out, err = proc.communicate(input=stdin)

        return_value = proc.returncode
        signal.alarm(0)
    except Alarm:
        LOG.error("Command: {} took to long time(more than {}s) to complete. Terminates the job.".format(command,my_timeout))
        proc.terminate()
        return False
        
    LOG.debug("communicate complete")
    lines = out.splitlines()
    if stdout_logfile == None:
        for line in lines:
            LOG.info(line)
    else:
        try:
            _stdout = open(stdout_logfile, 'w')
            for line in lines:
                _stdout.write(line + "\n")     
            _stdout.close()
        except IOError as e:
            LOG.error("IO operation to file stdout_logfile: {} failed with {}".format(stdout_logfile,e))
            return False
        
    errlines = err.splitlines()
    if (stderr_logfile == None):
        for errline in errlines:
            LOG.info(errline)
    else:
        try:
            _stderr = open(stderr_logfile, 'w')
            for errline in errlines:
                _stderr.write(errline + "\n")     
            _stderr.close()
        except IOError as e:
            LOG.error("IO operation to file stderr_logfile: {} failed with {}".format(stderr_logfile,e))
            return False

    return True, return_value, out, err
