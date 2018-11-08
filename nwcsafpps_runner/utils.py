#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

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
import socket
import netifaces
from trollduction.producer import check_uri
#from socket import gethostbyaddr, gaierror
from socket import gaierror
import logging
LOG = logging.getLogger(__name__)


SUPPORTED_NOAA_SATELLITES = ['NOAA-15', 'NOAA-18', 'NOAA-19']
SUPPORTED_METOP_SATELLITES = ['Metop-B', 'Metop-A']
SUPPORTED_EOS_SATELLITES = ['EOS-Terra', 'EOS-Aqua']
SUPPORTED_JPSS_SATELLITES = ['Suomi-NPP', 'NOAA-20', 'NOAA-21']

SUPPORTED_PPS_SATELLITES = (SUPPORTED_NOAA_SATELLITES +
                            SUPPORTED_METOP_SATELLITES +
                            SUPPORTED_EOS_SATELLITES +
                            SUPPORTED_JPSS_SATELLITES)

GEOLOC_PREFIX = {'EOS-Aqua': 'MYD03', 'EOS-Terra': 'MOD03'}
DATA1KM_PREFIX = {'EOS-Aqua': 'MYD021km', 'EOS-Terra': 'MOD021km'}

PPS_SENSORS = ['amsu-a', 'amsu-b', 'mhs', 'avhrr/3', 'viirs', 'modis']
REQUIRED_MW_SENSORS = {}
REQUIRED_MW_SENSORS['NOAA-15'] = ['amsu-a', 'amsu-b']
#REQUIRED_MW_SENSORS['NOAA-18'] = ['amsu-a', 'mhs']
REQUIRED_MW_SENSORS['NOAA-18'] = []
REQUIRED_MW_SENSORS['NOAA-19'] = ['amsu-a', 'mhs']
REQUIRED_MW_SENSORS['Metop-A'] = ['amsu-a', 'mhs']
REQUIRED_MW_SENSORS['Metop-B'] = ['amsu-a', 'mhs']
NOAA_METOP_PPS_SENSORNAMES = ['avhrr/3', 'amsu-a', 'amsu-b', 'mhs']

METOP_NAME_LETTER = {'metop01': 'metopb', 'metop02': 'metopa'}
METOP_NAME = {'metop01': 'Metop-B', 'metop02': 'Metop-A'}
METOP_NAME_INV = {'metopb': 'metop01', 'metopa': 'metop02'}

SATELLITE_NAME = {'NOAA-19': 'noaa19', 'NOAA-18': 'noaa18',
                  'NOAA-15': 'noaa15',
                  'Metop-A': 'metop02', 'Metop-B': 'metop01',
                  'Metop-C': 'metop03',
                  'Suomi-NPP': 'npp',
                  'NOAA-20': 'noaa20', 'NOAA-21': 'noaa21',
                  'EOS-Aqua': 'eos2', 'EOS-Terra': 'eos1'}
SENSOR_LIST = {}
for sat in SATELLITE_NAME:
    if sat in ['NOAA-15']:
        SENSOR_LIST[sat] = ['avhrr/3', 'amsu-b', 'amsu-a']
    elif sat in ['EOS-Aqua', 'EOS-Terra']:
        SENSOR_LIST[sat] = 'modis'
    elif sat in ['Suomi-NPP', 'NOAA-20', 'NOAA-21']:
        SENSOR_LIST[sat] = 'viirs'
    else:
        SENSOR_LIST[sat] = ['avhrr/3', 'mhs', 'amsu-a']


METOP_SENSOR = {'amsu-a': 'amsua', 'avhrr/3': 'avhrr',
                'amsu-b': 'amsub', 'hirs/4': 'hirs'}
METOP_NUMBER = {'b': '01', 'a': '02'}


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
                   str(starttime.strftime('%Y%m%d%H%M')))
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
    except (AttributeError, gaierror) as err:
        LOG.error("Failed checking host! Hostname = %s", socket.gethostname())
        LOG.exception(err)

    LOG.info("Sat and Sensor: " + str(msg.data['platform_name'])
             + " " + str(msg.data['sensor']))
    if msg.data['sensor'] not in PPS_SENSORS:
        LOG.info("Data from sensor " + str(msg.data['sensor']) +
                 " not needed by PPS " +
                 "Continue...")
        return False

    if msg.data['platform_name'] in SUPPORTED_EOS_SATELLITES:
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
            #fname = os.path.basename(item)
            files4pps[sceneid].append(item)

    LOG.debug("files4pps: %s", str(files4pps[sceneid]))
    if (msg.data['variant'] in ['EARS', ] and platform_name in SUPPORTED_METOP_SATELLITES):
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
