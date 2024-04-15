#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2021 Pytroll Developers

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>
#   Erik.Johansson <erik.johansson@smhi.se>

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

"""Posttroll runner for the NWCSAF/PPS version >= v2018.
"""

import argparse
import logging
import os
import sys
import threading
from datetime import datetime, timezone
from queue import Empty, Queue
from subprocess import PIPE, Popen

from nwcsafpps_runner.config import get_config
from nwcsafpps_runner.publish_and_listen import FileListener, FilePublisher
from nwcsafpps_runner.utils import (SENSOR_LIST, PpsRunError,
                                    create_pps_call_command,
                                    create_xml_timestat_from_lvl1c,
                                    find_product_statistics_from_lvl1c,
                                    get_lvl1c_file_from_msg, logreader,
                                    publish_pps_files, ready2run,
                                    terminate_process)

LOG = logging.getLogger(__name__)


#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


LOG.debug("PYTHONPATH: " + str(sys.path))


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
    """Start PPS on a scene.

    scene = {'platform_name': platform_name,
             'orbit_number': orbit_number,
             'satday': satday, 'sathour': sathour,
             'starttime': starttime, 'endtime': endtime}
    """

    try:
        LOG.info("Starting pps runner for scene %s", str(scene))
        job_start_time = datetime.now(tz=timezone.utc)

        LOG.debug("Level-1c file: %s", scene['file4pps'])
        LOG.debug("Platform name: %s", scene['platform_name'])
        LOG.debug("Orbit number: %s", str(scene['orbit_number']))

        min_thr = options['maximum_pps_processing_time_in_minutes']
        LOG.debug("Maximum allowed  PPS processing time in minutes: %d", min_thr)

        py_exec = options.get('python')
        pps_run_all = options.get('run_all_script')
        pps_script = pps_run_all.get('name')
        pps_run_all_flags = pps_run_all.get('flags')
        if not pps_run_all_flags:
            pps_run_all_flags = []

        cmd_str = create_pps_call_command(py_exec, pps_script, scene)
        for flag in pps_run_all_flags:
            cmd_str = cmd_str + ' %s' % flag

        my_env = os.environ.copy()
        for envkey in my_env:
            LOG.debug("ENV: " + str(envkey) + " " + str(my_env[envkey]))

        pps_output_dir = my_env.get('SM_PRODUCT_DIR', options.get('pps_outdir', './'))
        LOG.debug("PPS_OUTPUT_DIR = " + str(pps_output_dir))
        LOG.debug("...from config file = " + str(options['pps_outdir']))

        LOG.debug("Run command: " + str(cmd_str))
        try:
            pps_all_proc = Popen(cmd_str.split(" "), shell=False, stderr=PIPE, stdout=PIPE)
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

        if options['run_cmask_prob']:
            pps_script = options.get('run_cmaprob_script')
            cmdl = create_pps_call_command(py_exec, pps_script, scene)

            LOG.debug("Run command: " + str(cmdl))
            try:
                pps_cmaprob_proc = Popen(cmdl.split(" "), shell=False, stderr=PIPE, stdout=PIPE)
            except PpsRunError:
                LOG.exception("Failed when trying to run the PPS Cma-prob")
            timer_cmaprob = threading.Timer(min_thr * 60.0, terminate_process,
                                            args=(pps_cmaprob_proc, scene, ))
            timer_cmaprob.start()

            out_reader2 = threading.Thread(
                target=logreader, args=(pps_cmaprob_proc.stdout, LOG.info))
            err_reader2 = threading.Thread(
                target=logreader, args=(pps_cmaprob_proc.stderr, LOG.info))
            out_reader2.start()
            err_reader2.start()
            out_reader2.join()
            err_reader2.join()

        pps_control_path = my_env.get('SM_STATISTICS_DIR', options.get('pps_statistics_dir', './'))
        xml_files = create_xml_timestat_from_lvl1c(scene, pps_control_path)
        xml_files += find_product_statistics_from_lvl1c(scene, pps_control_path)
        LOG.info("PPS summary statistics files: %s", str(xml_files))

        # The PPS post-hooks takes care of publishing the PPS cloud products
        # For the XML files we keep the publishing from here:
        publish_pps_files(input_msg, publish_q, scene, xml_files,
                          servername=options['servername'],
                          station=options['station'])

        dt_ = datetime.now(tz=timezone.utc) - job_start_time
        LOG.info("PPS on scene " + str(scene) + " finished. It took: " + str(dt_))

        t__.cancel()
        if options['run_cmask_prob']:
            timer_cmaprob.cancel()

    except Exception:
        LOG.exception('Failed in pps_worker...')
        raise


def check_threads(threads):
    """Scan all threads and join those that are finished (dead)."""

    # LOG.debug(str(threading.enumerate()))
    for i, thread in enumerate(threads):
        if thread.is_alive():
            LOG.info("Thread " + str(i) + " alive...")
        else:
            LOG.info(
                "Thread " + str(i) + " is no more alive...")
            thread.join()
            threads.remove(thread)


def run_pps(scene, publish_q, input_msg, options):
    """Run pps. No parallel running here."""
    pps_worker(scene, publish_q, input_msg, options)


def pps(options):
    """The PPS runner.

    Triggers processing of PPS main script for a level1c files.
    """

    LOG.info("*** Start the PPS level-2 runner:")
    LOG.info("Use level-1c file as input")

    LOG.info("Number of threads: %d", options['number_of_threads'])
    thread_pool = ThreadPool(options['number_of_threads'])

    listener_q = Queue()
    publisher_q = Queue()

    pub_thread = FilePublisher(publisher_q, options['publish_topic'], runner_name='pps_runner',
                               nameservers=options.get('nameservers', None))
    pub_thread.start()
    listen_thread = FileListener(listener_q, options['subscribe_topics'])
    listen_thread.start()

    while True:
        try:
            msg = listener_q.get()
        except Empty:
            continue

        LOG.debug(
            "Number of threads currently alive: " + str(threading.active_count()))
        if 'sensor' in msg.data and isinstance(msg.data['sensor'], list):
            msg.data['sensor'] = msg.data['sensor'][0]
        if 'orbit_number' not in msg.data:
            msg.data.update({'orbit_number': 99999})
        if 'end_time' not in msg.data:
            msg.data.update({'end_time': 99999})

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

        scene['file4pps'] = get_lvl1c_file_from_msg(msg)
        status = ready2run(msg, scene)

        if status:

            LOG.debug("Files for PPS: %s", str(scene['file4pps']))
            LOG.info('Start a thread runing pps...')

            if options['number_of_threads'] == 1:
                run_pps(scene, publisher_q, msg, options)
            else:
                thread_pool.new_thread(scene['file4pps'],
                                       target=run_pps, args=(scene,
                                                             publisher_q,
                                                             msg, options))

            LOG.debug("Number of threads currently alive: %s", str(threading.active_count()))

    # FIXME! Should I clean up the thread_pool (open threads?) here at the end!?

    pub_thread.stop()
    listen_thread.stop()


def get_arguments():
    """Get command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('test_with_l1c_files',
                        metavar='fileN', type=str, nargs='+',
                        default=[],
                        help="To test for l1c file with patched subscriber")
    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='l1c_config.yaml',
                        help="The file containing " +
                        "configuration parameters e.g. product_filter_config.yaml, \n" +
                        "default = ./l1c_config.yaml",
                        required=True)

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    from logging import handlers

    args = get_arguments()
    config_file = args.config_file

    OPTIONS = get_config(config_file, add_defaults=True)

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
    LOG.debug("Path to PPS-runner config file = {:s}".format(args.config_file))

    if args.test_with_l1c_files != []:
        from posttroll.message import Message
        from posttroll.testing import patched_subscriber_recv
        some_files = args.test_with_l1c_files
        messages = [Message("some_topic", "file", data={"uri": f, "orbit_number": 00000, "sensor": "avhrr",
                                                        'platform_name': "EOS-Aqua",
                                                        "start_time": datetime(2024, 4, 9, 8, 3)})
                    for f in some_files]
        subscriber_settings = dict(nameserver=False, addresses=["ipc://bla"])
        with patched_subscriber_recv(messages):
            pps(OPTIONS)
    pps(OPTIONS)
