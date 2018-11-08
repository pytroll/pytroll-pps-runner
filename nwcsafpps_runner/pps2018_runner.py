#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2018 Adam.Dybbroe

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
import ConfigParser
import sys
import socket
from glob import glob
import stat
from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message

import threading
import multiprocessing
import Queue
from datetime import datetime, timedelta
from trollsift.parser import parse
from nwcsafpps_runner.utils import (ready2run, get_sceneid)
from nwcsafpps_runner.utils import (METOP_NUMBER,
                                    METOP_SENSOR,
                                    SENSOR_LIST,
                                    SATELLITE_NAME,
                                    METOP_NAME_INV,
                                    METOP_NAME,
                                    METOP_NAME_LETTER,
                                    NOAA_METOP_PPS_SENSORNAMES,
                                    REQUIRED_MW_SENSORS, PPS_SENSORS,
                                    DATA1KM_PREFIX,
                                    GEOLOC_PREFIX,
                                    SUPPORTED_PPS_SATELLITES,
                                    SUPPORTED_JPSS_SATELLITES,
                                    SUPPORTED_EOS_SATELLITES,
                                    SUPPORTED_METOP_SATELLITES,
                                    SUPPORTED_NOAA_SATELLITES)

from ppsRunAll import pps_run_all_serial
from ppsCmaskProb import pps_cmask_prob

import logging
LOG = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get('PPSRUNNER_CONFIG_DIR', './')

CONF = ConfigParser.ConfigParser()
CONF.read(os.path.join(CONFIG_PATH, "pps2018_config.ini"))

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"


OPTIONS = {}
for option, value in CONF.items(MODE, raw=True):
    OPTIONS[option] = value

PUBLISH_TOPIC = OPTIONS.get('publish_topic')
SUBSCRIBE_TOPICS = OPTIONS.get('subscribe_topics').split(',')
for item in SUBSCRIBE_TOPICS:
    if len(item) == 0:
        SUBSCRIBE_TOPICS.remove(item)

SDR_GRANULE_PROCESSING = (OPTIONS.get('sdr_processing') == 'granules')
CMA_PROB = (OPTIONS.get('run_cmask_prob') == 'yes')

# PPS_OUTPUT_DIR = os.environ.get('SM_PRODUCT_DIR', OPTIONS['pps_outdir'])
PPS_OUTPUT_DIR = OPTIONS['pps_outdir']
STATISTICS_DIR = OPTIONS.get('pps_statistics_dir')

#LVL1_NPP_PATH = os.environ.get('LVL1_NPP_PATH', None)
#LVL1_EOS_PATH = os.environ.get('LVL1_EOS_PATH', None)


servername = None
servername = socket.gethostname()
SERVERNAME = OPTIONS.get('servername', servername)

NWP_FLENS = [3, 6, 9, 12, 15, 18, 21, 24]


PPS_OUT_PATTERN = "S_NWC_{segment}_{orig_platform_name}_{orbit_number:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.{extention}"
PPS_OUT_PATTERN_MULTIPLE = "S_NWC_{segment1}_{segment2}_{orig_platform_name}_{orbit_number:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.{extention}"
PPS_STAT_PATTERN = "S_NWC_{segment}_{orig_platform_name}_{orbit_number:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z_statistics.xml"

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

_PPS_LOG_FILE = os.environ.get('PPSRUNNER_LOG_FILE', None)
_PPS_LOG_FILE = OPTIONS.get('pps_log_file', _PPS_LOG_FILE)


LOG.debug("PYTHONPATH: " + str(sys.path))
from nwcsafpps_runner.prepare_nwp import update_nwp
SATNAME = {'Aqua': 'EOS-Aqua'}


class SceneId(object):

    def __init__(self, platform_name, orbit_number, starttime, threshold=5):
        self.platform_name = platform_name
        self.orbit_number = orbit_number
        self.starttime = starttime
        self.threshold = threshold

    def __str__(self):

        return (str(self.platform_name) + '_' +
                str(self.orbit_number) + '_' +
                str(self.starttime.strftime('%Y%m%d%H%M')))

    def __eq__(self, other):

        return (self.platform_name == other.platform_name and
                self.orbit_number == other.orbit_number and
                abs(self.starttime - other.starttime) < timedelta(minutes=self.threshold))


def message_uid(msg):
    """Create a unique id/key-name for the scene."""

    orbit_number = int(msg.data['orbit_number'])
    platform_name = msg.data['platform_name']
    starttime = msg.data['start_time']

    return SceneId(platform_name, orbit_number, starttime)


def get_outputfiles(path, platform_name, orb):
    """From the directory path and satellite id and orbit number scan the directory
    and find all pps output files matching that scene and return the full
    filenames. Since the orbit number is unstable there might be more than one
    scene with the same orbit number and platform name. In order to avoid
    picking up an older scene we check the file modifcation time, and if the
    file is too old we discard it!

    """

    h5_output = (os.path.join(path, 'S_NWC') + '*' +
                 str(METOP_NAME_LETTER.get(platform_name, platform_name)) +
                 '_' + '%.5d' % int(orb) + '_*.h5')
    LOG.info(
        "Match string to do a file globbing on hdf5 output files: " + str(h5_output))
    nc_output = (os.path.join(path, 'S_NWC') + '*' +
                 str(METOP_NAME_LETTER.get(platform_name, platform_name)) +
                 '_' + '%.5d' % int(orb) + '_*.nc')
    LOG.info(
        "Match string to do a file globbing on netcdf output files: " + str(nc_output))
    xml_output = (os.path.join(path, 'S_NWC') + '*' +
                  str(METOP_NAME_LETTER.get(platform_name, platform_name)) +
                  '_' + '%.5d' % int(orb) + '_*.xml')
    LOG.info(
        "Match string to do a file globbing on xml output files: " + str(xml_output))
    filelist = glob(h5_output) + glob(nc_output) + glob(xml_output)
    now = datetime.utcnow()
    time_threshold = timedelta(minutes=90.)
    filtered_flist = []
    for fname in filelist:
        mtime = datetime.utcfromtimestamp(os.stat(fname)[stat.ST_MTIME])
        if (now - mtime) < time_threshold:
            filtered_flist.append(fname)
        else:
            LOG.info("Found old PPS result: %s", fname)

    return filtered_flist


def pps_worker(scene, publish_q, input_msg):
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

        # Run core PPS PGEs in a serial fashion
        LOG.info("Run PPS module: pps_run_all_serial")
        pps_run_all_serial(**kwargs)

        # Run the PPS CmaskProb (probabilistic Cloudmask):
        if CMA_PROB:
            LOG.info("Run PPS module: pps_cmask_prob")
            pps_cmask_prob(**kwargs)
        else:
            LOG.info("Will skip running the PPS module: pps_cmask_prob (probablistic cloud mask)")

        my_env = os.environ.copy()
        for envkey in my_env:
            LOG.debug("ENV: " + str(envkey) + " " + str(my_env[envkey]))

        LOG.debug("PPS_OUTPUT_DIR = " + str(PPS_OUTPUT_DIR))
        LOG.debug("...from config file = " + str(OPTIONS['pps_outdir']))

        LOG.info("Ready with PPS level-2 processing on scene: " + str(scene))

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
                             '_' + str(scene['orbit_number']) + '*.txt')
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

        # Now check what netCDF/hdf5 output was produced and publish
        # them:
        pps_path = my_env.get('SM_PRODUCT_DIR', PPS_OUTPUT_DIR)
        result_files = get_outputfiles(
            pps_path, SATELLITE_NAME[scene['platform_name']], scene['orbit_number'])
        LOG.info("PPS Output files: " + str(result_files))
        xml_files = get_outputfiles(
            pps_control_path, SATELLITE_NAME[scene['platform_name']], scene['orbit_number'])
        LOG.info("PPS summary statistics files: " + str(xml_files))

        # Now publish:
        for result_file in result_files + xml_files:
            # Get true start and end time from filenames and adjust the end time in
            # the publish message:
            filename = os.path.basename(result_file)
            LOG.info("file to publish = " + str(filename))
            try:
                try:
                    metadata = parse(PPS_OUT_PATTERN, filename)
                except ValueError:
                    metadata = parse(PPS_OUT_PATTERN_MULTIPLE, filename)
                    metadata['segment'] = '_'.join([metadata['segment1'],
                                                    metadata['segment2']])
                    del metadata['segment1'], metadata['segment2']
            except ValueError:
                metadata = parse(PPS_STAT_PATTERN, filename)

            endtime = metadata['end_time']
            starttime = metadata['start_time']

            to_send = input_msg.data.copy()
            to_send.pop('dataset', None)
            to_send.pop('collection', None)
            to_send['uri'] = (
                'ssh://%s/%s' % (SERVERNAME, result_file))
            to_send['uid'] = filename
            to_send['sensor'] = scene.get('instrument', None)
            if not to_send['sensor']:
                to_send['sensor'] = scene.get('sensor', None)

            to_send['platform_name'] = scene['platform_name']
            to_send['orbit_number'] = scene['orbit_number']
            if result_file.endswith("xml"):
                to_send['format'] = 'PPS-XML'
                to_send['type'] = 'XML'
            if result_file.endswith("nc"):
                to_send['format'] = 'CF'
                to_send['type'] = 'netCDF4'
            if result_file.endswith("h5"):
                to_send['format'] = 'PPS'
                to_send['type'] = 'HDF5'
            to_send['data_processing_level'] = '2'

            environment = MODE
            to_send['start_time'], to_send['end_time'] = starttime, endtime
            pubmsg = Message('/' + to_send['format'] + '/' +
                             to_send['data_processing_level'] +
                             '/norrköping/' + environment +
                             '/polar/direct_readout/',
                             "file", to_send).encode()
            LOG.debug("sending: " + str(pubmsg))
            LOG.info("Sending: " + str(pubmsg))
            publish_q.put(pubmsg)

            dt_ = datetime.utcnow() - job_start_time
            LOG.info("PPS on scene " + str(scene) +
                     " finished. It took: " + str(dt_))

    except:
        LOG.exception('Failed in pps_worker...')
        raise


class FilePublisher(threading.Thread):

    """A publisher for the PPS result files. Picks up the return value from the
    pps_worker when ready, and publishes the files via posttroll"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish('pps_runner', 0, PUBLISH_TOPIC) as publisher:

            while self.loop:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the files...")
                    publisher.send(retv)


class FileListener(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        LOG.debug("Subscribe topics = %s", str(SUBSCRIBE_TOPICS))
        with posttroll.subscriber.Subscribe("", SUBSCRIBE_TOPICS, True) as subscr:

            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                # Check if it is a relevant message:
                if self.check_message(msg):
                    LOG.info("Put the message on the queue...")
                    LOG.debug("Message = " + str(msg))
                    self.queue.put(msg)

    def check_message(self, msg):

        if not msg:
            return False

        if ('platform_name' not in msg.data or
                'orbit_number' not in msg.data or
                'start_time' not in msg.data):
            LOG.warning("Message is lacking crucial fields...")
            return False

        if (msg.data['platform_name'] not in SUPPORTED_PPS_SATELLITES):
            LOG.info(str(msg.data['platform_name']) + ": " +
                     "Not a NOAA/Metop/S-NPP/Terra/Aqua scene. Continue...")
            return False

        return True


def run_nwp_and_pps(scene, flens, publish_q, input_msg):
    """Run first the nwp-preparation and then pps. No parallel running here!"""

    prepare_nwp4pps(flens)
    pps_worker(scene, publish_q, input_msg)

    return


def prepare_nwp4pps(flens):
    """Prepare NWP data for pps"""

    starttime = datetime.utcnow() - timedelta(days=1)
    try:
        update_nwp(starttime, flens)
        LOG.info("Ready with nwp preparation")
        LOG.debug("Leaving prepare_nwp4pps...")
    except:
        LOG.exception("Something went wrong in update_nwp...")
        raise


def get_pps_inputfile(platform_name, ppsfiles):

    if platform_name in SUPPORTED_EOS_SATELLITES:
        for ppsfile in ppsfiles:
            if os.path.basename(ppsfile).find('021km') > 0:
                return ppsfile
    elif platform_name in SUPPORTED_NOAA_SATELLITES:
        for ppsfile in ppsfiles:
            if os.path.basename(ppsfile).find('hrpt_') >= 0:
                return ppsfile
    elif platform_name in SUPPORTED_METOP_SATELLITES:
        for ppsfile in ppsfiles:
            if os.path.basename(ppsfile).find('hrpt_') >= 0:
                return ppsfile
    elif platform_name in SUPPORTED_JPSS_SATELLITES:
        for ppsfile in ppsfiles:
            if os.path.basename(ppsfile).find('SVM01') >= 0:
                return ppsfile

    return None


def pps():
    """The PPS runner. Triggers processing of PPS main script once AAPP or CSPP
    is ready with a level-1 file"""

    LOG.info("*** Start the PPS level-2 runner:")

    LOG.info("First check if NWP data should be downloaded and prepared")
    now = datetime.utcnow()
    update_nwp(now - timedelta(days=1), NWP_FLENS)
    LOG.info("Ready with nwp preparation...")

    pps_manager = multiprocessing.Manager()

    listener_q = pps_manager.Queue()
    publisher_q = pps_manager.Queue()

    pub_thread = FilePublisher(publisher_q)
    pub_thread.start()
    listen_thread = FileListener(listener_q)
    listen_thread.start()

    files4pps = {}
    mpool = multiprocessing.Pool(processes=5)
    while True:

        try:
            msg = listener_q.get()
        except Queue.Empty:
            continue

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

        status = ready2run(msg, files4pps, sdr_granule_processing=SDR_GRANULE_PROCESSING)
        if status:
            sceneid = get_sceneid(platform_name, orbit_number, starttime)
            scene['file4pps'] = get_pps_inputfile(platform_name, files4pps[sceneid])

            LOG.info('Start a multiprocessing thread preparing the nwp data and run pps...')
            mpool.apply_async(run_nwp_and_pps, args=(scene, NWP_FLENS,
                                                     publisher_q,
                                                     msg))

            # Clean the files4pps dict:
            LOG.debug("files4pps: " + str(files4pps))
            try:
                files4pps.pop(sceneid)
            except KeyError:
                LOG.warning("Failed trying to remove key " + str(sceneid) +
                            " from dictionary files4pps")

            LOG.debug("After cleaning: files4pps = " + str(files4pps))

    mpool.close()
    mpool.join()

    pub_thread.stop()
    listen_thread.stop()

    return


def prepare_pps_arguments(platform_name, level1_filepath, **kwargs):

    orbit_number = kwargs.get('orbit_number')
    pps_args = {}

    if platform_name in SUPPORTED_EOS_SATELLITES:
        pps_args['modisorbit'] = orbit_number
        pps_args['modisfile'] = level1_filepath

    elif platform_name in SUPPORTED_JPSS_SATELLITES:
        pps_args['csppfile'] = level1_filepath

    elif platform_name in SUPPORTED_METOP_SATELLITES:
        pps_args['hrptfile'] = level1_filepath

    elif platform_name in SUPPORTED_NOAA_SATELLITES:
        pps_args['hrptfile'] = level1_filepath

    return pps_args


if __name__ == "__main__":

    from logging import handlers

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

    pps()
