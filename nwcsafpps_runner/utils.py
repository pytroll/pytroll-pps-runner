#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2022 Pytroll Developers

# Author(s):

#   Adam.Dybbroe <Firstname.Lastname at smhi.se>

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

"""Utility functions for NWCSAF/pps runner(s).
"""
import threading
from trollsift.parser import parse  # @UnresolvedImport
# from trollsift import Parser
from posttroll.message import Message  # @UnresolvedImport
from subprocess import Popen, PIPE
import os
import shlex
from glob import glob
import socket

#: Python 2/3 differences
from six.moves.urllib.parse import urlparse

from posttroll.address_receiver import get_local_ips

import logging
LOG = logging.getLogger(__name__)


class NwpPrepareError(Exception):
    pass


class FindTimeControlFileError(Exception):
    pass


PPS_OUT_PATTERN = ("S_NWC_{segment}_{orig_platform_name}_{orbit_number:05d}_" +
                   "{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.{extention}")
PPS_OUT_PATTERN_MULTIPLE = ("S_NWC_{segment1}_{segment2}_{orig_platform_name}_{orbit_number:05d}_" +
                            "{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z.{extention}")
PPS_STAT_PATTERN = ("S_NWC_{segment}_{orig_platform_name}_{orbit_number:05d}_" +
                    "{start_time:%Y%m%dT%H%M%S%f}Z_{end_time:%Y%m%dT%H%M%S%f}Z_statistics.xml")

SUPPORTED_AVHRR_SATELLITES = ['NOAA-15', 'NOAA-18', 'NOAA-19',
                              'Metop-B', 'Metop-A', 'Metop-C']
SUPPORTED_EARS_AVHRR_SATELLITES = ['Metop-B', 'Metop-C']
SUPPORTED_MODIS_SATELLITES = ['EOS-Terra', 'EOS-Aqua']
SUPPORTED_VIIRS_SATELLITES = ['Suomi-NPP', 'NOAA-20', 'NOAA-21', 'NOAA-22', 'NOAA-23']
SUPPORTED_SEVIRI_SATELLITES = ['Meteosat-09', 'Meteosat-10', 'Meteosat-11']
SUPPORTED_METIMAGE_SATELLITES = ['Metop-SG-A1', 'Metop-SG-A2', 'Metop-SG-A3']

SUPPORTED_PPS_SATELLITES = (SUPPORTED_AVHRR_SATELLITES +
                            SUPPORTED_MODIS_SATELLITES +
                            SUPPORTED_SEVIRI_SATELLITES +
                            SUPPORTED_METIMAGE_SATELLITES +
                            SUPPORTED_VIIRS_SATELLITES)

GEOLOC_PREFIX = {'EOS-Aqua': 'MYD03', 'EOS-Terra': 'MOD03'}
DATA1KM_PREFIX = {'EOS-Aqua': 'MYD021km', 'EOS-Terra': 'MOD021km'}

PPS_SENSORS = ['amsu-a', 'amsu-b', 'mhs', 'avhrr/3', 'viirs', 'modis', 'seviri', 'metimage']
NOAA_METOP_PPS_SENSORNAMES = ['avhrr/3', 'amsu-a', 'amsu-b', 'mhs']

METOP_NAME_LETTER = {'metop01': 'metopb', 'metop02': 'metopa', 'metop03': 'metopc'}
METOP_NAME = {'metop01': 'Metop-B', 'metop02': 'Metop-A', 'metop03': 'Metop-C'}
METOP_NAME_INV = {'metopb': 'metop01', 'metopa': 'metop02', 'metopc': 'metop03'}

# SATELLITE_NAME = {}
# for sat in SUPPORTED_PPS_SATELLITES:
#     SATELLITE_NAME[sat] = sat.lower().replace('-', '')
# historic exceptions
# SATELLITE_NAME['Suomi-NPP'] = 'npp'
# SATELLITE_NAME['EOS-Aqua'] = 'eos2'
# SATELLITE_NAME['EOS-Terra'] = 'eos1'
# SATELLITE_NAME['Metop-A']= 'metop02'
# SATELLITE_NAME['Metop-B']= 'metop01'
# SATELLITE_NAME['Metop-C']= 'metop03'

SENSOR_LIST = {}
for sat in SUPPORTED_PPS_SATELLITES:
    if sat in SUPPORTED_AVHRR_SATELLITES:
        SENSOR_LIST[sat] = ['avhrr/3']
    elif sat in SUPPORTED_MODIS_SATELLITES:
        SENSOR_LIST[sat] = ['modis']
    elif sat in SUPPORTED_VIIRS_SATELLITES:
        SENSOR_LIST[sat] = ['viirs']
    elif sat in SUPPORTED_SEVIRI_SATELLITES:
        SENSOR_LIST[sat] = ['seviri']
    elif sat in SUPPORTED_METIMAGE_SATELLITES:
        SENSOR_LIST[sat] = ['metimage']

METOP_SENSOR = {'amsu-a': 'amsua', 'avhrr/3': 'avhrr',
                'amsu-b': 'amsub', 'hirs/4': 'hirs'}


def run_command(cmdstr):
    """Run system command."""
    myargs = shlex.split(str(cmdstr))

    LOG.debug("Command: " + str(cmdstr))
    LOG.debug('Command sequence= ' + str(myargs))
    #: TODO: What is this
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


def get_lvl1c_file_from_msg(msg):
    """Get level1c file from msg."""
    destination = msg.data.get('destination')

    uris = []

    if msg.type == 'file':
        if destination is None:
            uris = [(msg.data['uri'])]
        else:
            uris = [os.path.join(destination, msg.data['uid'])]
    else:
        LOG.debug(
            "Ignoring this type of message data: type = " + str(msg.type))
        return None

    try:
        level1c_files = check_uri(uris)
    except IOError:
        LOG.info('One or more files not present on this host!')
        return None

    LOG.debug("files4pps: %s", str(level1c_files))
    return level1c_files[0]


def check_host_ok(msg):
    """Check that host is ok."""
    try:
        url_ip = socket.gethostbyname(msg.host)
        if url_ip not in get_local_ips():
            LOG.warning("Server %s not the current one: %s", str(url_ip), socket.gethostname())
            return False
    except (AttributeError, socket.gaierror) as err:
        LOG.error("Failed checking host! Hostname = %s", socket.gethostname())
        LOG.exception(err)
    return True


def ready2run(msg, scene, **kwargs):
    """Check whether pps is ready to run or not."""

    LOG.info("Got message: " + str(msg))
    if not check_host_ok(msg):
        return False

    if scene['file4pps'] is None:
        return False

    if msg.data['platform_name'] in SUPPORTED_PPS_SATELLITES:
        LOG.info(
            "This is a PPS supported scene. Start the PPS lvl2 processing!")
        LOG.info("Process the file = %s" +
                 os.path.basename(scene['file4pps']))

        LOG.debug("Ready to run...")
        return True


def terminate_process(popen_obj, scene):
    """Terminate a Popen process."""
    if popen_obj.returncode is None:
        popen_obj.kill()
        LOG.info(
            "Process timed out and pre-maturely terminated. Scene: " + str(scene))
    else:
        LOG.info(
            "Process finished before time out - workerScene: " + str(scene))


def create_pps_call_command(python_exec, pps_script_name, scene):
    """Create the pps call command.

    Supports PPSv2021.
    """
    cmdstr = ("%s" % python_exec + " %s " % pps_script_name +
              "-af %s" % scene['file4pps'])
    LOG.debug("PPS call command: %s", str(cmdstr))
    return cmdstr


def create_xml_timestat_from_lvl1c(scene, pps_control_path):
    """From lvl1c file create XML file and return a file list."""
    try:
        txt_time_control = create_pps_file_from_lvl1c(scene['file4pps'], pps_control_path, "timectrl", ".txt")
    except KeyError:
        return []
    if os.path.exists(txt_time_control):
        return create_xml_timestat_from_ascii(txt_time_control, pps_control_path)
    else:
        LOG.warning('No XML Time statistics file created!')
        return []


def find_product_statistics_from_lvl1c(scene, pps_control_path):
    """From lvl1c file find product XML files and return a file list."""
    try:
        glob_pattern = create_pps_file_from_lvl1c(scene['file4pps'], pps_control_path, "*", "_statistics.xml")
        return glob(glob_pattern)
    except KeyError:
        return []


def create_pps_file_from_lvl1c(l1c_file_name, pps_control_path, name_tag, file_type):
    """From lvl1c file create name_tag-file of type file_type."""
    from trollsift import parse, compose
    f_pattern = 'S_NWC_{name_tag}_{platform_id}_{orbit_number}_{start_time}Z_{end_time}Z{file_type}'
    l1c_path, l1c_file = os.path.split(l1c_file_name)
    data = parse(f_pattern, l1c_file)
    data["name_tag"] = name_tag
    data["file_type"] = file_type
    return os.path.join(pps_control_path, compose(f_pattern, data))


def create_xml_timestat_from_ascii(infile, pps_control_path):
    """From ascii file(s) with PPS time statistics create XML file(s) and return a file list."""
    try:
        from pps_time_control import PPSTimeControl
    except ImportError:
        LOG.warning("Failed to import the PPSTimeControl from pps")
        return []
    LOG.info("Time control ascii file: " + str(infile))
    LOG.info("Read time control ascii file and generate XML")
    ppstime_con = PPSTimeControl(infile)
    ppstime_con.sum_up_processing_times()
    try:
        ppstime_con.write_xml()
    except Exception as e:  # TypeError as e:
        LOG.warning('Not able to write time control xml file')
        LOG.warning(e)

    # There should always be only one xml file for each ascii file found above!
    return [infile.replace('.txt', '.xml')]


def publish_pps_files(input_msg, publish_q, scene, result_files, **kwargs):
    """
    Publish messages for the files provided.
    """

    servername = kwargs.get('servername')
    station = kwargs.get('station', 'unknown')

    for result_file in result_files:
        # Get true start and end time from filenames and adjust the end time in
        # the publish message:
        filename = os.path.basename(result_file)
        LOG.info("file to publish = %s", str(filename))
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
        to_send['uri'] = result_file
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
        to_send['data_processing_level'] = '2'

        to_send['start_time'], to_send['end_time'] = starttime, endtime
        pubmsg = Message('/' + to_send['format'] + '/' +
                         to_send['data_processing_level'] +
                         '/' + station +
                         '/polar/direct_readout/',
                         "file", to_send).encode()
        LOG.info("Sending: %s", str(pubmsg))
        try:
            publish_q.put(pubmsg)
        except Exception:
            LOG.warning("Failed putting message on the queue, will send it now...")
            publish_q.send(pubmsg)


def logreader(stream, log_func):
    while True:
        mystring = stream.readline()
        if not mystring:
            break
        log_func(mystring.strip())
    stream.close()
