#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2020 PyTroll

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

"""Utility functions for NWCSAF/pps runner(s)
"""

import os
import stat
import netifaces
from glob import glob
from subprocess import Popen, PIPE
from posttroll.message import Message
from trollsift.parser import parse
import socket
from six.moves.urllib.parse import urlparse
from datetime import datetime, timedelta
from nwcsafpps_runner.config import (LVL1_NPP_PATH, LVL1_EOS_PATH)
import shlex
import threading
import logging

LOG = logging.getLogger(__name__)


PPS_OUT_PATTERN = "S_NWC_{segment}_{orig_platform_name}_{orbit_number:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.{extention}"
PPS_OUT_PATTERN_MULTIPLE = "S_NWC_{segment1}_{segment2}_{orig_platform_name}_{orbit_number:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.{extention}"
PPS_STAT_PATTERN = "S_NWC_{segment}_{orig_platform_name}_{orbit_number:05d}_{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z_statistics.xml"

SUPPORTED_NOAA_SATELLITES = ['NOAA-15', 'NOAA-18', 'NOAA-19']
SUPPORTED_METOP_SATELLITES = ['Metop-B', 'Metop-A', 'Metop-C']
SUPPORTED_EOS_SATELLITES = ['EOS-Terra', 'EOS-Aqua']
SUPPORTED_JPSS_SATELLITES = ['Suomi-NPP', 'NOAA-20', 'NOAA-21']
SUPPORTED_METEOSAT_SATELLITES = ['Meteosat-09', 'Meteosat-10', 'Meteosat-11']

SUPPORTED_PPS_SATELLITES = (SUPPORTED_NOAA_SATELLITES +
                            SUPPORTED_METOP_SATELLITES +
                            SUPPORTED_EOS_SATELLITES +
                            SUPPORTED_METEOSAT_SATELLITES +
                            SUPPORTED_JPSS_SATELLITES)

GEOLOC_PREFIX = {'EOS-Aqua': 'MYD03', 'EOS-Terra': 'MOD03'}
DATA1KM_PREFIX = {'EOS-Aqua': 'MYD021km', 'EOS-Terra': 'MOD021km'}

PPS_SENSORS = ['amsu-a', 'amsu-b', 'mhs', 'avhrr/3', 'viirs', 'modis', 'seviri']
REQUIRED_MW_SENSORS = {}
REQUIRED_MW_SENSORS['NOAA-15'] = ['amsu-a', 'amsu-b']
# REQUIRED_MW_SENSORS['NOAA-18'] = ['amsu-a', 'mhs']
REQUIRED_MW_SENSORS['NOAA-18'] = []
REQUIRED_MW_SENSORS['NOAA-19'] = ['amsu-a', 'mhs']
REQUIRED_MW_SENSORS['Metop-A'] = ['amsu-a', 'mhs']
REQUIRED_MW_SENSORS['Metop-B'] = ['amsu-a', 'mhs']
REQUIRED_MW_SENSORS['Metop-C'] = ['amsu-a', 'mhs']
NOAA_METOP_PPS_SENSORNAMES = ['avhrr/3', 'amsu-a', 'amsu-b', 'mhs']

METOP_NAME_LETTER = {'metop01': 'metopb', 'metop02': 'metopa', 'metop03': 'metopc'}
METOP_NAME = {'metop01': 'Metop-B', 'metop02': 'Metop-A', 'metop03': 'Metop-C'}
METOP_NAME_INV = {'metopb': 'metop01', 'metopa': 'metop02', 'metopc': 'metop03'}

SATELLITE_NAME = {'NOAA-19': 'noaa19', 'NOAA-18': 'noaa18',
                  'NOAA-15': 'noaa15',
                  'Metop-A': 'metop02', 'Metop-B': 'metop01',
                  'Metop-C': 'metop03',
                  'Suomi-NPP': 'npp',
                  'NOAA-20': 'noaa20', 'NOAA-21': 'noaa21',
                  'EOS-Aqua': 'eos2', 'EOS-Terra': 'eos1',
                  'Meteosat-09': 'meteosat09', 'Meteosat-10': 'meteosat10',
                  'Meteosat-11': 'meteosat11'}
SENSOR_LIST = {}
for sat in SATELLITE_NAME:
    if sat in ['NOAA-15']:
        SENSOR_LIST[sat] = ['avhrr/3', 'amsu-b', 'amsu-a']
    elif sat in ['EOS-Aqua', 'EOS-Terra']:
        SENSOR_LIST[sat] = 'modis'
    elif sat in ['Suomi-NPP', 'NOAA-20', 'NOAA-21']:
        SENSOR_LIST[sat] = 'viirs'
    elif 'Meteosat' in sat:
        SENSOR_LIST[sat] = 'seviri'
    else:
        SENSOR_LIST[sat] = ['avhrr/3', 'mhs', 'amsu-a']


METOP_SENSOR = {'amsu-a': 'amsua', 'avhrr/3': 'avhrr',
                'amsu-b': 'amsub', 'hirs/4': 'hirs'}
# METOP_NUMBER = {'b': '01', 'a': '02'}


def run_command(cmdstr):
    """Run system command"""
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

    return proc.wait()


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
        LOG.warning("Couldn't check file location, running anyway")

    return url.path


class PpsRunError(Exception):
    pass


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

    def __hash__(self):
        return hash(str(self.platform_name) + '_' +
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


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


def get_sceneid(platform_name, orbit_number, starttime):

    if starttime:
        sceneid = (str(platform_name) + '_' +
                   str(orbit_number) + '_' +
                   str(starttime.strftime('%Y%m%d%H%M%S')))
    else:
        sceneid = (str(platform_name) + '_' +
                   str(orbit_number))

    LOG.debug("Scene identifier = " + str(sceneid))
    return sceneid


def ready2run(msg, files4pps, **kwargs):
    """Check whether pps is ready to run or not"""
    # """Start the PPS processing on a NOAA/Metop/S-NPP/EOS scene"""
    # LOG.debug("Received message: " + str(msg))

    LOG.debug("Ready to run...")
    LOG.info("Got message: " + str(msg))

    sdr_granule_processing = kwargs.get('sdr_granule_processing')
    stream_tag_name = kwargs.get('stream_tag_name', 'variant')
    stream_name = kwargs.get('stream_name', 'EARS')
    destination = msg.data.get('destination')

    uris = []
    if (msg.type == 'dataset' and
            msg.data['platform_name'] in SUPPORTED_EOS_SATELLITES):
        LOG.info('Dataset: ' + str(msg.data['dataset']))
        LOG.info('Got a dataset on an EOS satellite')
        LOG.info('\t ...thus we can assume we have everything we need for PPS')
        for obj in msg.data['dataset']:
            uris.append(obj['uri'])

    elif (sdr_granule_processing and msg.type == 'dataset' and
            msg.data['platform_name'] in SUPPORTED_JPSS_SATELLITES):
        LOG.info('Dataset: ' + str(msg.data['dataset']))
        LOG.info('Got a dataset on a JPSS/SNPP satellite')
        if destination == None:
            for obj in msg.data['dataset']:
                uris.append(obj['uri'])
        else:
            for obj in msg.data['dataset']:
                uris.append(os.path.join(destination, obj['uid']))

    elif msg.type == 'collection' and not sdr_granule_processing:
        if 'dataset' in msg.data['collection'][0]:
            for dataset in msg.data['collection']:
                uris.extend([mda['uri'] for mda in dataset['dataset']])

    elif msg.type == 'file':
        if destination == None:
            uris = [(msg.data['uri'])]
        else:
            uris = [os.path.join(destination, msg.data['uid'])]
    else:
        LOG.debug(
            "Ignoring this type of message data: type = " + str(msg.type))
        return False

    try:
        level1_files = check_uri(uris)
    except IOError:
        LOG.info('One or more files not present on this host!')
        return False

    try:
        url_ip = socket.gethostbyname(msg.host)
        if url_ip not in get_local_ips():
            LOG.warning("Server %s not the current one: %s", str(url_ip), socket.gethostname())
            return False
    except (AttributeError, socket.gaierror) as err:
        LOG.error("Failed checking host! Hostname = %s", socket.gethostname())
        LOG.exception(err)

    LOG.info("Sat and Sensor: " + str(msg.data['platform_name'])
             + " " + str(msg.data['sensor']))
    if msg.data['sensor'] not in PPS_SENSORS:
        LOG.info("Data from sensor " + str(msg.data['sensor']) +
                 " not needed by PPS " +
                 "Continue...")
        return False

    if msg.data['platform_name'] in SUPPORTED_METEOSAT_SATELLITES:
        if msg.data['sensor'] not in ['seviri', ]:
            LOG.info(
                'Sensor ' + str(msg.data['sensor']) +
                ' not required for MODIS PPS processing...')
            return False
    elif msg.data['platform_name'] in SUPPORTED_EOS_SATELLITES:
        if msg.data['sensor'] not in ['modis', ]:
            LOG.info(
                'Sensor ' + str(msg.data['sensor']) +
                ' not required for MODIS PPS processing...')
            return False
    elif msg.data['platform_name'] in SUPPORTED_JPSS_SATELLITES:
        if msg.data['sensor'] not in ['viirs', ]:
            LOG.info(
                'Sensor ' + str(msg.data['sensor']) +
                ' not required for S-NPP/VIIRS PPS processing...')
            return False
    else:
        if msg.data['sensor'] not in NOAA_METOP_PPS_SENSORNAMES:
            LOG.info(
                'Sensor ' + str(msg.data['sensor']) + ' not required...')
            return False
        required_mw_sensors = REQUIRED_MW_SENSORS.get(
            msg.data['platform_name'])
        if (msg.data['sensor'] in required_mw_sensors and
                msg.data['data_processing_level'] != '1C'):
            if msg.data['data_processing_level'] == '1c':
                LOG.warning("Level should be in upper case!")
            else:
                LOG.info('Level not the required type for PPS for this sensor: ' +
                         str(msg.data['sensor']) + ' ' +
                         str(msg.data['data_processing_level']))
                return False

    # The orbit number is mandatory!
    orbit_number = int(msg.data['orbit_number'])
    LOG.debug("Orbit number: " + str(orbit_number))

    # sensor = (msg.data['sensor'])
    platform_name = msg.data['platform_name']

    if platform_name not in SATELLITE_NAME:
        LOG.warning("Satellite not supported: " + str(platform_name))
        return False

    starttime = msg.data.get('start_time')
    sceneid = get_sceneid(platform_name, orbit_number, starttime)

    if sceneid not in files4pps:
        files4pps[sceneid] = []

    LOG.debug("level1_files = %s", level1_files)
    if platform_name in SUPPORTED_EOS_SATELLITES:
        for item in level1_files:
            fname = os.path.basename(item)
            LOG.debug("EOS level-1 file: %s", item)
            if (fname.startswith(GEOLOC_PREFIX[platform_name]) or
                    fname.startswith(DATA1KM_PREFIX[platform_name])):
                files4pps[sceneid].append(item)
    else:
        for item in level1_files:
            # fname = os.path.basename(item)
            files4pps[sceneid].append(item)

    LOG.debug("files4pps: %s", str(files4pps[sceneid]))
    if (stream_tag_name in msg.data and msg.data[stream_tag_name] in [stream_name, ] and
            platform_name in SUPPORTED_METOP_SATELLITES):
        LOG.info("EARS Metop data. Only require the HRPT/AVHRR level-1b file to be ready!")
    elif (platform_name in SUPPORTED_METOP_SATELLITES or
          platform_name in SUPPORTED_NOAA_SATELLITES):
        if len(files4pps[sceneid]) < len(REQUIRED_MW_SENSORS[platform_name]) + 1:
            LOG.info("Not enough NOAA/Metop sensor data available yet...")
            return False
    elif platform_name in SUPPORTED_EOS_SATELLITES:
        if len(files4pps[sceneid]) < 2:
            LOG.info("Not enough MODIS level 1 files available yet...")
            return False

    if len(files4pps[sceneid]) > 10:
        LOG.info(
            "Number of level 1 files ready = " + str(len(files4pps[sceneid])))
        LOG.info("Scene = " + str(sceneid))
    else:
        LOG.info("Level 1 files ready: " + str(files4pps[sceneid]))

    if msg.data['platform_name'] in SUPPORTED_PPS_SATELLITES:
        LOG.info(
            "This is a PPS supported scene. Start the PPS lvl2 processing!")
        LOG.info("Process the scene (sat, orbit) = " +
                 str(platform_name) + ' ' + str(orbit_number))

        return True


def terminate_process(popen_obj, scene):
    """Terminate a Popen process"""
    if popen_obj.returncode == None:
        popen_obj.kill()
        LOG.info(
            "Process timed out and pre-maturely terminated. Scene: " + str(scene))
    else:
        LOG.info(
            "Process finished before time out - workerScene: " + str(scene))
    return


def prepare_pps_arguments(platform_name, level1_filepath, **kwargs):
    """Prepare the platform specific arguments to be passed to the PPS scripts/modules"""

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


def create_pps_call_command_sequence(pps_script_name, scene, options):
    LVL1_NPP_PATH = os.environ.get('LVL1_NPP_PATH',
                                   options.get('LVL1_NPP_PATH', None))
    LVL1_EOS_PATH = os.environ.get('LVL1_EOS_PATH',
                                   options.get('LVL1_EOS_PATH', None))

    if scene['platform_name'] in SUPPORTED_EOS_SATELLITES:
        cmdstr = "%s %s %s %s %s" % (pps_script_name,
                                     SATELLITE_NAME[
                                         scene['platform_name']],
                                     scene['orbit_number'], scene[
                                         'satday'],
                                     scene['sathour'])
    else:
        cmdstr = "%s %s %s 0 0" % (pps_script_name,
                                   SATELLITE_NAME[
                                       scene['platform_name']],
                                   scene['orbit_number'])

    cmdstr = cmdstr + ' ' + str(options['aapp_level1files_max_minutes_old'])

    if scene['platform_name'] in SUPPORTED_JPSS_SATELLITES and LVL1_NPP_PATH:
        cmdstr = cmdstr + ' ' + str(LVL1_NPP_PATH)
    elif scene['platform_name'] in SUPPORTED_EOS_SATELLITES and LVL1_EOS_PATH:
        cmdstr = cmdstr + ' ' + str(LVL1_EOS_PATH)

    return shlex.split(str(cmdstr))


def create_pps2018_call_command(python_exec, pps_script_name, scene, sequence=True):

    if scene['platform_name'] in SUPPORTED_EOS_SATELLITES:
        cmdstr = ("%s " % python_exec + " %s " % pps_script_name +
                  " --modisfile %s" % scene['file4pps'])
    elif scene['platform_name'] in SUPPORTED_JPSS_SATELLITES:
        cmdstr = ("%s " % python_exec + " %s " % pps_script_name +
                  " --csppfile %s" % scene['file4pps'])
    elif scene['platform_name'] in SUPPORTED_METEOSAT_SATELLITES:
        cmdstr = ("%s" % python_exec + " %s " % pps_script_name +
                  "-af %s" % scene['file4pps'])
    else:
        cmdstr = ("%s " % python_exec + " %s " % pps_script_name +
                  " --hrptfile %s" % scene['file4pps'])
    if sequence:
        return shlex.split(str(cmdstr))
    else:
        return cmdstr


def get_pps_inputfile(platform_name, ppsfiles):
    """From the set of files picked up in the PostTroll messages decide the input
       file used in the PPS call
    """

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
    elif platform_name in SUPPORTED_METEOSAT_SATELLITES:
        for ppsfile in ppsfiles:
            if os.path.basename(ppsfile).find('NWC') >= 0:
                return ppsfile

    return None


def get_outputfiles(path, platform_name, orb, **kwargs):
    """From the directory path and satellite id and orbit number scan the directory
    and find all pps output files matching that scene and return the full
    filenames. Since the orbit number is unstable there might be more than one
    scene with the same orbit number and platform name. In order to avoid
    picking up an older scene we check the file modifcation time, and if the
    file is too old we discard it!

    """

    filelist = []
    h5_output = kwargs.get('h5_output')
    if h5_output:
        h5_output = (os.path.join(path, 'S_NWC') + '*' +
                     str(METOP_NAME_LETTER.get(platform_name, platform_name)) +
                     '_' + '%.5d' % int(orb) + '_*.h5')
        LOG.info(
            "Match string to do a file globbing on hdf5 output files: " + str(h5_output))
        filelist = filelist + glob(h5_output)

    nc_output = kwargs.get('nc_output')
    if nc_output:
        nc_output = (os.path.join(path, 'S_NWC') + '*' +
                     str(METOP_NAME_LETTER.get(platform_name, platform_name)) +
                     '_' + '%.5d' % int(orb) + '_*.nc')
        LOG.info(
            "Match string to do a file globbing on netcdf output files: " + str(nc_output))
        filelist = filelist + glob(nc_output)

    xml_output = kwargs.get('xml_output')
    if xml_output:
        xml_output = (os.path.join(path, 'S_NWC') + '*' +
                      str(METOP_NAME_LETTER.get(platform_name, platform_name)) +
                      '_' + '%.5d' % int(orb) + '_*.xml')
        LOG.info(
            "Match string to do a file globbing on xml output files: " + str(xml_output))
        filelist = filelist + glob(xml_output)

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


def publish_pps_files(input_msg, publish_q, scene, result_files, **kwargs):
    """
    Publish messages for the files provided
    """

    environment = kwargs.get('environment')
    servername = kwargs.get('servername')
    station = kwargs.get('station', 'unknown')

    for result_file in result_files:
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
            'ssh://%s/%s' % (servername, result_file))
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

        to_send['start_time'], to_send['end_time'] = starttime, endtime
        pubmsg = Message('/' + to_send['format'] + '/' +
                         to_send['data_processing_level'] +
                         '/' + station + '/' + environment +
                         '/polar/direct_readout/',
                         "file", to_send).encode()
        LOG.debug("sending: " + str(pubmsg))
        LOG.info("Sending: " + str(pubmsg))
        try:
            publish_q.put(pubmsg)
        except:
            LOG.warning("Failed putting message on the queue, will send it now...")
            publish_q.send(pubmsg)

    return


def logreader(stream, log_func):
    while True:
        s = stream.readline()
        if not s:
            break
        log_func(s.strip())
    stream.close()
