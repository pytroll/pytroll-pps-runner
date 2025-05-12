#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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

"""Message utilities."""

import logging
import os

from posttroll.message import Message
from nwcsafpps_runner.utils import create_pps_file_from_lvl1c


LOG = logging.getLogger(__name__)


def remove_non_pps_products(msg_data):
    """ Remove non-PPS files from datasetlis.t"""
    msg_data["dataset"] = [item for item in msg_data["dataset"] if "S_NWC" in item["uid"]]


def get_pps_sensor_from_msg(sensor_msg):
    """ Get pps sensor from msg sensor."""
    sensor = None
    if type(sensor_msg) is list and len(sensor_msg) == 1:
        sensor = sensor_msg[0]
    if sensor is None:
        for pps_sensor in ['viirs', 'avhrr', 'modis', 'mersi2', 'metimage', 'slstr']:
            if pps_sensor in sensor_msg:
                sensor = pps_sensor
    if "avhrr/3" in sensor_msg:
        sensor = "avhrr"
    return sensor


def add_lvl1c_to_msg(msg_data, options):
    """Add PPS lvl1c file to a collection of PPS products."""
    level1c_path = os.environ.get('SM_IMAGER_DIR', options.get('pps_lvl1c_dir', './'))
    sensor = options.get('sensor', get_pps_sensor_from_msg(msg_data["sensor"]))
    num_files = len(msg_data['dataset'])
    to_add = {}
    for item in msg_data['dataset']:
        lvl1c_file = create_pps_file_from_lvl1c(item["uri"], level1c_path,
                                                name_tag=sensor, file_type='.nc')
        to_add[lvl1c_file] = {
            "uri": lvl1c_file,
            "uid": os.path.basename(lvl1c_file)}
    msg_data['dataset'].extend(to_add.values())


def flatten_collection(msg_data):
    """Flatten collection msg to dataset msg."""
    if "collection" in msg_data:
        collection = msg_data.pop("collection")
        msg_data["dataset"] = []
        for ind in range(0, len(collection)):
            for item in collection[ind]["dataset"]:
                if type(item) == dict:
                    msg_data["dataset"].append(item)


def prepare_pps_collector_message(msg, options):
    to_send = msg.data.copy()
    flatten_collection(to_send)
    remove_non_pps_products(to_send)
    add_lvl1c_to_msg(to_send, options)
    return to_send


def prepare_nwp_message(result_file, publish_topic):
    """Prepare message for NWP files."""
    to_send = {}
    to_send["uri"] = result_file
    filename = os.path.basename(result_file)
    to_send["uid"] = filename
    to_send['format'] = 'NWP grib'
    to_send['type'] = 'grib'
    return to_send


def prepare_l1c_message(result_file, mda, **kwargs):
    """Prepare the output message for the level-1c file creation."""
    if not result_file:
        return

    # Now publish:
    to_send = mda.copy()
    # Delete the input level-1 dataset from the message:
    try:
        del to_send['dataset']
    except KeyError:
        LOG.warning("Couldn't remove dataset from message")
    try:
        del to_send['filename']
    except KeyError:
        LOG.warning("Couldn't remove filename from message")

    if ('orbit' in kwargs) and ('orbit_number' in to_send.keys()):
        to_send["orig_orbit_number"] = to_send["orbit_number"]
        to_send["orbit_number"] = kwargs['orbit']

    to_send["uri"] = result_file
    filename = os.path.basename(result_file)
    to_send["uid"] = filename

    to_send['format'] = 'PPS-L1C'
    to_send['type'] = 'NETCDF'
    to_send['data_processing_level'] = '1c'

    return to_send


def publish_l1c(publisher, publish_msg, publish_topic, msg_type="file"):
    """Publish the messages that l1c files are ready."""
    LOG.debug('Publish topic = %s', publish_topic)
    for topic in publish_topic:
        msg = Message(topic, msg_type, publish_msg).encode()
        LOG.debug("sending: %s", str(msg))
        publisher.send(msg)
