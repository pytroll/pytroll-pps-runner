#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2021 Adam.Dybbroe

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

"""Posttroll runner for PPS
"""
import os
import sys
from glob import glob
from subprocess import Popen, PIPE
import threading
from six.moves.queue import Queue
from datetime import datetime, timedelta

from nwcsafpps_runner.config import get_config
from nwcsafpps_runner.config import MODE
from nwcsafpps_runner.config import CONFIG_FILE
from nwcsafpps_runner.config import CONFIG_PATH
from nwcsafpps_runner.utils import ready2run, publish_pps_files
from nwcsafpps_runner.utils import (terminate_process,
                                    create_pps_call_command_sequence,
                                    PpsRunError, logreader, get_outputfiles,
                                    message_uid)
from nwcsafpps_runner.utils import (SENSOR_LIST,
                                    SATELLITE_NAME,
                                    METOP_NAME_LETTER)
from nwcsafpps_runner.publish_and_listen import FileListener, FilePublisher

from nwcsafpps_runner.prepare_nwp import update_nwp

import logging
LOG = logging.getLogger(__name__)


PPS_SCRIPT = os.environ['PPS_SCRIPT']
LOG.debug("PPS_SCRIPT = %s", str(PPS_SCRIPT))

NWP_FLENS = [3, 6, 9, 12, 15, 18, 21, 24]


#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

LOG.debug("PYTHONPATH: %s", str(sys.path))


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
        LOG.debug("Starting pps runner for scene %s", str(scene))
        job_start_time = datetime.utcnow()

        pps_call_args = create_pps_call_command_sequence(PPS_SCRIPT, scene, options)
        LOG.info("Command: %s", str(pps_call_args))

        my_env = os.environ.copy()
        # for envkey in my_env:
        # LOG.debug("ENV: " + str(envkey) + " " + str(my_env[envkey]))

        pps_output_dir = my_env.get('SM_PRODUCT_DIR', options.get(['pps_outdir'], './'))
        LOG.debug("PPS_OUTPUT_DIR = %s", str(pps_output_dir))
        LOG.debug("...from config file = %s", str(options['pps_outdir']))
        if not os.path.isfile(PPS_SCRIPT):
            raise IOError("PPS script" + PPS_SCRIPT + " is not there!")
        if not os.access(PPS_SCRIPT, os.X_OK):
            raise IOError(
                "PPS script" + PPS_SCRIPT + " cannot be executed!")

        try:
            pps_proc = Popen(pps_call_args, shell=False, stderr=PIPE, stdout=PIPE)
        except PpsRunError:
            LOG.exception("Failed in PPS...")

        min_thr = options.get('maximum_pps_processing_time_in_minutes', 20)
        t__ = threading.Timer(min_thr * 60.0, terminate_process, args=(pps_proc, scene, ))
        t__.start()

        out_reader = threading.Thread(
            target=logreader, args=(pps_proc.stdout, LOG.info))
        err_reader = threading.Thread(
            target=logreader, args=(pps_proc.stderr, LOG.info))
        out_reader.start()
        err_reader.start()
        out_reader.join()
        err_reader.join()

        LOG.info("Ready with PPS level-2 processing on scene: %s", str(scene))

        # Now try perform som time statistics editing with ppsTimeControl.py from
        # pps:
        do_time_control = True
        try:
            from pps_time_control import PPSTimeControl
        except ImportError:
            LOG.warning("Failed to import the PPSTimeControl from pps")
            do_time_control = False

        pps_control_path = my_env.get('STATISTICS_DIR', options.get('pps_statistics_dir', './'))

        if do_time_control:
            LOG.info("Read time control ascii file and generate XML")
            platform_id = SATELLITE_NAME.get(
                scene['platform_name'], scene['platform_name'])
            LOG.info("pps platform_id = %s", str(platform_id))
            txt_time_file = (os.path.join(pps_control_path, 'S_NWC_timectrl_') +
                             str(METOP_NAME_LETTER.get(platform_id, platform_id)) +
                             '_' + '%.5d' % scene['orbit_number'] + '*.txt')
            LOG.info("glob string = %s", str(txt_time_file))
            infiles = glob(txt_time_file)
            LOG.info("Time control ascii file candidates: %s", str(infiles))
            if len(infiles) == 1:
                infile = infiles[0]
                LOG.info("Time control ascii file: %s", str(infile))
                ppstime_con = PPSTimeControl(infile)
                ppstime_con.sum_up_processing_times()
                ppstime_con.write_xml()

        # Now check what netCDF/hdf5 output was produced and publish
        # them:
        pps_path = pps_output_dir
        result_files = get_outputfiles(pps_path,
                                       SATELLITE_NAME[scene['platform_name']],
                                       scene['orbit_number'],
                                       h5_output=True,
                                       nc_output=True)
        LOG.info("PPS Output files: %s", str(result_files))
        xml_files = get_outputfiles(pps_control_path,
                                    SATELLITE_NAME[scene['platform_name']],
                                    scene['orbit_number'],
                                    xml_output=True)
        LOG.info("PPS summary statistics files: %s", str(xml_files))

        # Now publish:
        publish_pps_files(input_msg, publish_q, scene,
                          result_files + xml_files,
                          environment=MODE,
                          servername=options['servername'],
                          station=options['station'])

        dt_ = datetime.utcnow() - job_start_time
        LOG.info("PPS on scene %s finished. It took: %s", str(scene), str(dt_))

        t__.cancel()

    except Exception:
        LOG.exception('Failed in pps_worker...')
        raise


def run_nwp_and_pps(scene, flens, publish_q, input_msg, options):
    """Run first the nwp-preparation and then pps. No parallel running here!"""

    prepare_nwp4pps(flens)
    pps_worker(scene, publish_q, input_msg, options)


def prepare_nwp4pps(flens):
    """Prepare NWP data for pps"""

    starttime = datetime.utcnow() - timedelta(days=1)
    try:
        update_nwp(starttime, flens)
        LOG.info("Ready with nwp preparation")
        LOG.debug("Leaving prepare_nwp4pps...")
    except Exception:
        LOG.exception("Something went wrong in update_nwp...")
        raise


def pps(options):
    """The PPS runner. Triggers processing of PPS main script once AAPP or CSPP
    is ready with a level-1 file"""

    LOG.info("*** Start the PPS level-2 runner:")

    LOG.info("First check if NWP data should be downloaded and prepared")
    now = datetime.utcnow()
    update_nwp(now - timedelta(days=1), NWP_FLENS)
    LOG.info("Ready with nwp preparation...")

    listener_q = Queue.Queue()
    publisher_q = Queue.Queue()

    pub_thread = FilePublisher(publisher_q, options['publish_topic'], runner_name='pps_runner')
    pub_thread.start()
    listen_thread = FileListener(listener_q, options['subscribe_topics'])
    listen_thread.start()

    files4pps = {}
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
                 'sensor': sensors}

        status = ready2run(msg, files4pps)
        if status:

            LOG.info('Start a thread preparing the nwp data and run pps...')
            thread_pool.new_thread(message_uid(msg),
                                   target=run_nwp_and_pps, args=(scene, NWP_FLENS,
                                                                 publisher_q,
                                                                 msg, options))

            LOG.debug(
                "Number of threads currently alive: " + str(threading.active_count()))

    pub_thread.stop()
    listen_thread.stop()


if __name__ == "__main__":

    from logging import handlers
    LOG.debug("Path to pps_runner config file = " + CONFIG_PATH)
    LOG.debug("Pps_runner config file = " + CONFIG_FILE)
    OPTIONS = get_config(CONFIG_FILE)

    _PPS_LOG_FILE = OPTIONS.get('pps_log_file',
                                os.environ.get('PPSRUNNER_LOG_FILE', False))
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
