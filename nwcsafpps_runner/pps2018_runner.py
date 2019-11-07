#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2019 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Posttroll runner for PPS v2018
"""

import os
import sys
from glob import glob
from subprocess import Popen, PIPE
import threading
import Queue
from datetime import datetime, timedelta
#
from nwcsafpps_runner.config import get_config
from nwcsafpps_runner.config import MODE

from nwcsafpps_runner.utils import ready2run, publish_pps_files
from nwcsafpps_runner.utils import (get_sceneid, prepare_pps_arguments,
                                    create_pps2018_call_command, get_pps_inputfile,
                                    logreader, terminate_process, get_outputfiles,
                                    message_uid)
from nwcsafpps_runner.utils import PpsRunError
from nwcsafpps_runner.utils import (SENSOR_LIST,
                                    SATELLITE_NAME,
                                    METOP_NAME_LETTER)
from nwcsafpps_runner.publish_and_listen import FileListener, FilePublisher

from nwcsafpps_runner.prepare_nwp import update_nwp

from six.moves.configparser import NoSectionError, NoOptionError

# from ppsRunAll import pps_run_all_serial
# from ppsCmaskProb import pps_cmask_prob

import logging
LOG = logging.getLogger(__name__)

# LVL1_NPP_PATH = os.environ.get('LVL1_NPP_PATH', None)
# LVL1_EOS_PATH = os.environ.get('LVL1_EOS_PATH', None)

NWP_FLENS = [3, 6, 9, 12, 15, 18, 21, 24]


#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

_PPS_LOG_FILE = os.environ.get('PPSRUNNER_LOG_FILE', None)


LOG.debug("PYTHONPATH: " + str(sys.path))
SATNAME = {'Aqua': 'EOS-Aqua'}


class ThreadPool(object):

    def __init__(self, max_nthreads=None):

        self.jobs = set()
        self.sema = threading.Semaphore(max_nthreads)
        self.lock = threading.Lock()

    def new_thread(self, job_id, group=None, target=None, name=None, args=(), kwargs={}):

        def new_target(*args, **kwargs):
            with self.sema:
                result = target(*args, **kwargs)

            self.jobs.remove(job_id)
            return result

        with self.lock:
            if job_id in self.jobs:
                LOG.info("Job with id %s already running!", str(job_id))
                return

            self.jobs.add(job_id)

        thread = threading.Thread(group, new_target, name, args, kwargs)
        thread.start()


def pps_worker(scene, publish_q, input_msg, options):
    """Start PPS on a scene

        scene = {'platform_name': platform_name,
                 'orbit_number': orbit_number,
                 'satday': satday, 'sathour': sathour,
                 'starttime': starttime, 'endtime': endtime}
    """

    try:
        LOG.info("Starting pps runner for scene %s", str(scene))
        job_start_time = datetime.utcnow()

        LOG.debug("Level-1 file: %s", scene['file4pps'])
        LOG.debug("Platform name: %s", scene['platform_name'])
        LOG.debug("Orbit number: %s", str(scene['orbit_number']))

        kwargs = prepare_pps_arguments(scene['platform_name'],
                                       scene['file4pps'],
                                       orbit_number=scene['orbit_number'])
        LOG.debug("pps-arguments: %s", str(kwargs))

        min_thr = options['maximum_pps_processing_time_in_minutes']
        LOG.debug("Maximum allowed  PPS processing time in minutes: %d", min_thr)

        # # Run core PPS PGEs in a serial fashion
        # LOG.info("Run PPS module: pps_run_all_serial")
        # pps_run_all_serial(**kwargs)

        # # Run the PPS CmaskProb (probabilistic Cloudmask):
        # if CMA_PROB:
        #     LOG.info("Run PPS module: pps_cmask_prob")
        #     pps_cmask_prob(**kwargs)
        # else:
        #     LOG.info("Will skip running the PPS module: pps_cmask_prob (probablistic cloud mask)")

        py_exec = options.get('python', '/bin/python')
        pps_script = options.get('run_all_script')
        cmd_str = create_pps2018_call_command(py_exec, pps_script, scene, sequence=False)

        my_env = os.environ.copy()
        for envkey in my_env:
            LOG.debug("ENV: " + str(envkey) + " " + str(my_env[envkey]))

        LOG.debug("PPS_OUTPUT_DIR = " + str(PPS_OUTPUT_DIR))
        LOG.debug("...from config file = " + str(options['pps_outdir']))

        LOG.debug("Run command: " + str(cmd_str))
        try:
            pps_all_proc = Popen(cmd_str, shell=True, stderr=PIPE, stdout=PIPE)
        except PpsRunError:
            LOG.exception("Failed in PPS...")

        t__ = threading.Timer(min_thr * 60.0, terminate_process, args=(pps_all_proc, scene, ))
        t__.start()

        out_reader = threading.Thread(
            target=logreader, args=(pps_all_proc.stdout, LOG.info))
        err_reader = threading.Thread(
            target=logreader, args=(pps_all_proc.stderr, LOG.info))
        out_reader.start()
        err_reader.start()
        out_reader.join()
        err_reader.join()

        LOG.info("Ready with PPS level-2 processing on scene: " + str(scene))

        run_cma_prob = (options.get('run_cmask_prob') == 'yes')
        if run_cma_prob:
            pps_script = options.get('run_cmaprob_script')
            cmdl = create_pps2018_call_command(py_exec, pps_script, scene, sequence=False)

            LOG.debug("Run command: " + str(cmdl))
            try:
                pps_cmaprob_proc = Popen(cmdl, shell=True, stderr=PIPE, stdout=PIPE)
            except PpsRunError:
                LOG.exception("Failed when trying to run the PPS Cma-prob")

            timer_cmaprob = threading.Timer(min_thr * 60.0, terminate_process, args=(pps_cmaprob_proc, scene, ))
            timer_cmaprob.start()

            out_reader2 = threading.Thread(
                target=logreader, args=(pps_cmaprob_proc.stdout, LOG.info))
            err_reader2 = threading.Thread(
                target=logreader, args=(pps_cmaprob_proc.stderr, LOG.info))
            out_reader2.start()
            err_reader2.start()
            out_reader2.join()
            err_reader2.join()

        # Now try perform som time statistics editing with ppsTimeControl.py from
        # pps:
        do_time_control = True
        try:
            from pps_time_control import PPSTimeControl
        except ImportError:
            LOG.warning("Failed to import the PPSTimeControl from pps")
            do_time_control = False

        if STATISTICS_DIR:
            pps_control_path = STATISTICS_DIR
        else:
            pps_control_path = my_env.get('STATISTICS_DIR')

        if do_time_control:
            LOG.info("Read time control ascii file and generate XML")
            platform_id = SATELLITE_NAME.get(
                scene['platform_name'], scene['platform_name'])
            LOG.info("pps platform_id = " + str(platform_id))
            txt_time_file = (os.path.join(pps_control_path, 'S_NWC_timectrl_') +
                             str(METOP_NAME_LETTER.get(platform_id, platform_id)) +
                             '_' + '%.5d' % scene['orbit_number'] + '*.txt')
            LOG.info("glob string = " + str(txt_time_file))
            infiles = glob(txt_time_file)
            LOG.info(
                "Time control ascii file candidates: " + str(infiles))
            if len(infiles) == 1:
                infile = infiles[0]
                LOG.info("Time control ascii file: " + str(infile))
                ppstime_con = PPSTimeControl(infile)
                ppstime_con.sum_up_processing_times()
                ppstime_con.write_xml()

        # The PPS post-hooks takes care of publishing the PPS PGEs
        # For the XML files we keep the publishing from here:
        xml_files = get_outputfiles(pps_control_path,
                                    SATELLITE_NAME[scene['platform_name']],
                                    scene['orbit_number'],
                                    xml_output=True)
        LOG.info("PPS summary statistics files: " + str(xml_files))

        # Now publish:
        publish_pps_files(input_msg, publish_q, scene, xml_files,
                          environment=MODE, servername=options['servername'],
                          station=options['station'])

        dt_ = datetime.utcnow() - job_start_time
        LOG.info("PPS on scene " + str(scene) + " finished. It took: " + str(dt_))

        t__.cancel()
        if run_cma_prob:
            timer_cmaprob.cancel()

    except:
        LOG.exception('Failed in pps_worker...')
        raise


def check_threads(threads):
    """Scan all threads and join those that are finished (dead)"""

    # LOG.debug(str(threading.enumerate()))
    for i, thread in enumerate(threads):
        if thread.is_alive():
            LOG.info("Thread " + str(i) + " alive...")
        else:
            LOG.info(
                "Thread " + str(i) + " is no more alive...")
            thread.join()
            threads.remove(thread)

    return


def run_nwp_and_pps(scene, flens, publish_q, input_msg, nwp_handeling_module):
    """Run first the nwp-preparation and then pps. No parallel running here!"""

    prepare_nwp4pps(flens, nwp_handeling_module)
    pps_worker(scene, publish_q, input_msg, options)

    return


def prepare_nwp4pps(flens, nwp_handeling_module):
    """Prepare NWP data for pps"""

    starttime = datetime.utcnow() - timedelta(days=1)
    if nwp_handeling_module:
        LOG.debug("Use custom nwp_handeling_function provided i config file...")
        LOG.debug("module_name = %s", str(module_name))
        try:
            name = "update_nwp"
            name = name.replace("/", "")
            module = __import__(module_name, globals(), locals(), [name])
            LOG.info("function : {} loaded from module: {}".format([name],module_name))
        except (ImportError, ModuleNotFoundError):
            LOG.exception("Failed to import custom compositer for %s", str(name))
            raise
        try:
            params = {}
            params['starttime'] = starttime
            params['nlengths'] = flens
            params['options'] = OPTIONS
            getattr(module, name)(params)
        except AttributeError:
            LOG.debug("Could not get attribute %s from %s", str(name), str(module))
    else:
        LOG.debug("No custom nwp_handeling_function provided i config file...")
        LOG.debug("Use build in.")
        try:
            update_nwp(starttime, flens)
        except:
            LOG.exception("Something went wrong in update_nwp...")
            raise
        
    LOG.info("Ready with nwp preparation")
    LOG.debug("Leaving prepare_nwp4pps...")


def pps(options):
    """The PPS runner. Triggers processing of PPS main script once AAPP or CSPP
    is ready with a level-1 file"""

    LOG.info("*** Start the PPS level-2 runner:")

    LOG.info("First check if NWP data should be downloaded and prepared")
    nwp_handeling_module=options.get("nwp_handeling_module", None)
    prepare_nwp4pps(NWP_FLENS, nwp_handeling_module)

    listener_q = Queue.Queue()
    publisher_q = Queue.Queue()

    pub_thread = FilePublisher(publisher_q, options['publish_topic'], runner_name='pps2018_runner')
    pub_thread.start()
    listen_thread = FileListener(listener_q, options['subscribe_topics'])
    listen_thread.start()

    files4pps = {}
    LOG.info("Number of threads: %d", options['number_of_threads'])
    thread_pool = ThreadPool(options['number_of_threads'])
    while True:

        try:
            msg = listener_q.get()
        except Queue.Empty:
            continue

        LOG.debug(
            "Number of threads currently alive: " + str(threading.active_count()))

        orbit_number = int(msg.data['orbit_number'])
        platform_name = msg.data['platform_name']
        starttime = msg.data['start_time']
        endtime = msg.data['end_time']

        satday = starttime.strftime('%Y%m%d')
        sathour = starttime.strftime('%H%M')
        sensors = SENSOR_LIST.get(platform_name, None)
        scene = {'platform_name': platform_name,
                 'orbit_number': orbit_number,
                 'satday': satday, 'sathour': sathour,
                 'starttime': starttime, 'endtime': endtime,
                 'sensor': sensors
                 }

        status = ready2run(msg, files4pps,
                           stream_tag_name=options.get('stream_tag_name', 'variant')
                           stream_name=options.get('stream_name', 'EARS')
                           sdr_granule_processing=options.get('sdr_processing') == 'granules')
        if status:
            sceneid = get_sceneid(platform_name, orbit_number, starttime)
            scene['file4pps'] = get_pps_inputfile(platform_name, files4pps[sceneid])

            LOG.info('Start a thread preparing the nwp data and run pps...')
            thread_pool.new_thread(message_uid(msg),
                                   target=run_nwp_and_pps, args=(scene, NWP_FLENS,
                                                                 publisher_q,
                                                                 msg, nwp_handeling_module))

            LOG.debug(
                "Number of threads currently alive: " + str(threading.active_count()))

            # Clean the files4pps dict:
            LOG.debug("files4pps: " + str(files4pps))
            try:
                files4pps.pop(sceneid)
            except KeyError:
                LOG.warning("Failed trying to remove key " + str(sceneid) +
                            " from dictionary files4pps")

            LOG.debug("After cleaning: files4pps = " + str(files4pps))

    # FIXME! Should I clean up the thread_pool (open threads?) here at the end!?

    pub_thread.stop()
    listen_thread.stop()

    return


if __name__ == "__main__":

    from logging import handlers

    OPTIONS = get_config("pps2018_config.ini")
    _PPS_LOG_FILE = OPTIONS.get('pps_log_file', _PPS_LOG_FILE)

    PPS_OUTPUT_DIR = OPTIONS['pps_outdir']
    STATISTICS_DIR = OPTIONS.get('pps_statistics_dir')

    if _PPS_LOG_FILE:
        ndays = int(OPTIONS["log_rotation_days"])
        ncount = int(OPTIONS["log_rotation_backup"])
        handler = handlers.TimedRotatingFileHandler(_PPS_LOG_FILE,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)

        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('pps_runner')

    pps(OPTIONS)
