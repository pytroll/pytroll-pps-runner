#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2021 Adam.Dybbroe

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

"""A PPS processing post hook to be run once PPS is ready with a PGE. Using
Posttroll and sends messages notifying the completion of a PGE

The metadata passed to a hook is::

    metadata = {'module': Name of the PPS-script calling this hook.
                'pps_version': PPS version (string)
                'platform_name': Name of the satellite
                'orbit': Orbit number (Can be 00000 or 99999 if unknown eg. for MODIS or GAC)
                'sensor': Name of the satellite sensor
                'start_time': Start time of the satellite scene. (string)
                'end_time': End time of the satellite scene. (string)
                'filename': The name of the file that is supposed to no be produced by this module. It can also
                            be a list of filenames.
                'file_was_already_processed': A Boolean value. If True, the file(s) to be produced by the script already
                                             exist; thus the script did basically nothing, but ended with status
                                             SUCCEED. If False, the output file did not exist in beforehand. But this
                                             parameter does not tell if the script failed or succeeded. (Use ‘status’
                                             for that information.)
               }

The hook is initialized when the yaml config file is read, so it needs a `__setstate__` method.

"""

import os
import socket
import logging
from posttroll.publisher import Publish
from posttroll.message import Message
from multiprocessing import Manager
import threading
from datetime import timedelta
import time

LOG = logging.getLogger(__name__)

VIIRS_TIME_THR1 = timedelta(seconds=81)
VIIRS_TIME_THR2 = timedelta(seconds=87)
WAIT_SECONDS_TO_ALLOW_PUBLISHER_TO_BE_REGISTERED = 2.2

PPS_PRODUCT_FILE_ID = {'ppsMakeAvhrr': 'RAD_SUN',
                       'ppsMakeViirs': 'RAD_SUN',
                       'ppsMakePhysiography': 'PHY',
                       'ppsMakeNwp': 'NWP',
                       'ppsCmaskPrepare': 'CMA-PRE',
                       'ppsCmask': 'CMA',
                       'ppsCmaskProb': 'CMAProb',
                       'ppsCtth': 'CTTH',
                       'ppsCtype': 'CT',
                       'ppsCpp': 'CPP',
                       'ppsCmic': 'CMIC',
                       'ppsPrecip': 'PC',
                       'ppsPrecipPrepare': 'PC-PRE'}

PLATFORM_CONVERSION_PPS2OSCAR = {'noaa20': 'NOAA-20',
                                 'noaa19': 'NOAA-19',
                                 'noaa18': 'NOAA-18',
                                 'noaa15': 'NOAA-15',
                                 'metop02': 'Metop-A',
                                 'metop01': 'Metop-B',
                                 'metop03': 'Metop-C',
                                 'npp': 'Suomi-NPP',
                                 'eos1': 'EOS-Terra',
                                 'eos2': 'EOS-Aqua',
                                 }

MANDATORY_FIELDS_FROM_YAML = {'level': 'data_processing_level',
                              'output_format': 'format',
                              'variant': 'variant',
                              'geo_or_polar': 'geo_or_polar',
                              'software': 'software'}

VARIANT_TRANSLATE = {'DR': 'direct_readout'}

SEC_DURATION_ONE_GRANULE = 1.779
MIN_VIIRS_GRANULE_LENGTH_SECONDS = timedelta(seconds=60)
MAX_VIIRS_GRANULE_LENGTH_SECONDS = timedelta(seconds=88)
# One nominal VIIRS granule is 48 scans. The duration of one scan is 1.779 seconds.
# Thus one granule is 1.779*48 = 85.4 seconds long.
# Sometimes an SDR granule may be shorter if one or more scans are missing.
# https://ncc.nesdis.noaa.gov/documents/documentation/viirs-users-guide-tech-report-142a-v1.3.pdf
# Check page 37!
#


class PPSPublisher(threading.Thread):

    """A publisher for the PPS modules.

    It publish a message via posttroll when a PPS module has finished.

    """

    def __init__(self, queue, nameservers=None):
        threading.Thread.__init__(self)
        self.queue = queue
        self.nameservers = nameservers

    def stop(self):
        """Stops the file publisher"""
        self.queue.put(None)

    def run(self):

        with Publish('PPS', 0, nameservers=self.nameservers) as publisher:
            time.sleep(WAIT_SECONDS_TO_ALLOW_PUBLISHER_TO_BE_REGISTERED)

            while True:
                retv = self.queue.get()

                if retv is not None:
                    LOG.info("Publish the message...")
                    publisher.send(retv)
                    LOG.info("Message published!")
                else:
                    time.sleep(1.0)
                    break


class PPSMessage(object):

    """A Posttroll message class to trigger the sending of a notifcation that a PPS PGE is ready

    """

    def __init__(self, description, metadata):

        # __init__ is not run when created from yaml
        # See http://pyyaml.org/ticket/48
        pass

    def __getstate__(self):
        """Example - metadata:
        posttroll_topic: "/PPSv2018"
        station: "norrkoping"
        output_format: "CF"
        level: "2"
        variant: DR
        geo_or_polar: "polar"
        software: "NWCSAF-PPSv2018"
        """
        d__ = {'metadata': self.metadata}
        return d__

    def __setstate__(self, mydict):
        self.metadata = mydict['metadata']

    def __call__(self, status, mda):
        """Send the message based on the metadata and the fields picked up from the yaml config."""

        self._collect_all_metadata(mda)
        message = PostTrollMessage(status, self.metadata)
        message.send()

    def _collect_all_metadata(self, mda):
        """Collect the static (from yaml config) and dynamic metadata into one dict."""
        self.metadata.update(mda)


class PostTrollMessage(object):
    """Create a Posttroll message from metadata."""

    def __init__(self, status, metadata):
        """Initialize the object."""
        self.metadata = metadata
        self.status = status
        self._to_send = {}
        self.viirs_granule_time_bounds = (MIN_VIIRS_GRANULE_LENGTH_SECONDS,
                                          MAX_VIIRS_GRANULE_LENGTH_SECONDS)
        # Check that the metadata has what is required:
        self.check_metadata_contains_mandatory_parameters()
        self.check_metadata_contains_filename()
        self.nameservers = self.get_nameservers()

        for key in self.metadata:
            LOG.debug("%s = %s", str(key), str(self.metadata[key]))

    def get_nameservers(self):
        """Get nameserver from metadata. Defaults to None"""
        nameservers = self.metadata.get('nameservers', None)
        if nameservers and not isinstance(nameservers, list):
            LOG.warning("Nameserver metadata must be a list. Setting to None.")
            nameservers = None
        return nameservers

    def check_metadata_contains_mandatory_parameters(self):
        """Check that all necessary metadata attributes are available."""

        attributes = ['start_time', 'end_time']
        for attr in attributes:
            if attr not in self.metadata:
                raise AttributeError("%s is a required attribute but is missing in metadata!" % attr)

    def check_metadata_contains_filename(self):
        """Check that the input metadata structure contains filename."""

        if 'filename' not in self.metadata:
            raise KeyError('filename')

    def check_mandatory_fields(self):
        """Check that mandatory fields are available in the metadata dict.

        level, output_format and station are all required fields,
        unless the posttroll_topic is specified.
        """
        if 'posttroll_topic' in self.metadata:
            return

        for attr in MANDATORY_FIELDS_FROM_YAML:
            if attr not in self.metadata:
                raise AttributeError("pps_hook must contain metadata attribute %s" % attr)

    def send(self):
        """Create and publish (send) the message."""

        if self.status != 0:
            # Error
            # pubmsg = self.create_message("FAILED", self.metadata)
            LOG.warning("Module %s failed, so no message sent", self.metadata.get('module', 'unknown'))
        else:
            # Ok
            pubmsg = self.create_message("OK")
            self.publish_message(pubmsg)

    def publish_message(self, mymessage):
        """Publish the message."""
        posttroll_msg = Message(mymessage['header'], mymessage['type'], mymessage['content'])
        msg_to_publish = posttroll_msg.encode()

        manager = Manager()
        publisher_q = manager.Queue()

        pub_thread = PPSPublisher(publisher_q, self.nameservers)
        pub_thread.start()
        LOG.info("Sending: " + str(msg_to_publish))
        publisher_q.put(msg_to_publish)
        pub_thread.stop()

    def create_message(self, status):
        """Create the posttroll message from the PPS metadata.

        The metadata provided by pps has the following keys: module, pps_version, platform_name, orbit, sensor,
        start_time, end_time, filename, file_was_already_processed.
        This class adds also the following metadata keys: pps_product
        Also the extra metadata provided in the configuration yaml file is available.
        That way, the publish_topic can be a pattern with metadata keys in it, eg::

          '/my/pps/publish/topic/{pps_product}/{sensor}/'

        """
        self._to_send = self.create_message_content_from_metadata()
        self._to_send.update({'status': status})
        # Add uri/uids to message content
        self._to_send.update(self.get_message_with_uri_and_uid())

        self.fix_mandatory_fields_in_message()
        self.clean_unused_keys_in_message()

        publish_topic = self._create_message_topic()

        return {'header': publish_topic, 'type': 'file', 'content': self._to_send}

    def _create_message_topic(self):
        """Create the publish topic from yaml file items and PPS metadata."""
        to_send = self._to_send.copy()
        to_send["pps_product"] = PPS_PRODUCT_FILE_ID.get(self.metadata.get('module', 'unknown'), 'UNKNOWN')
        to_send["variant"] = VARIANT_TRANSLATE.get(self._to_send['variant'], self._to_send['variant'])

        topic_pattern = to_send.get('publish_topic', self._create_default_topic())

        topic_str = topic_pattern.format(**to_send)
        return topic_str

    def _create_default_topic(self):
        topic = '/segment' if self.is_segment() else ""
        topic_pattern = "/".join((topic,
                                  "{geo_or_polar}",
                                  "{variant}",
                                  "{format}",
                                  "{data_processing_level}",
                                  "{pps_product}",
                                  "{software}",
                                  ""))
        return topic_pattern

    def create_message_content_from_metadata(self):
        """Create message content from the metadata."""
        msg = {}
        for key in self.metadata:
            # Disregard the PPS keyword "filename". We will use URI/UID instead - see below:
            if key not in msg and key != 'filename':
                msg[key] = self.metadata[key]

            if key == 'platform_name':
                msg[key] = PLATFORM_CONVERSION_PPS2OSCAR.get(self.metadata[key], self.metadata[key])

        return msg

    def fix_mandatory_fields_in_message(self):
        """Fix the message keywords from the mandatory fields."""
        self.check_mandatory_fields()

        # Initialize:
        for attr in MANDATORY_FIELDS_FROM_YAML:
            self._to_send[MANDATORY_FIELDS_FROM_YAML.get(attr)] = self.metadata[attr]

    def clean_unused_keys_in_message(self):
        """Clean away the unused keyword names from message."""
        for attr in MANDATORY_FIELDS_FROM_YAML:
            if attr not in MANDATORY_FIELDS_FROM_YAML.values():
                del self._to_send[attr]

    def get_message_with_uri_and_uid(self):
        """Generate a dict with the uri and uid's and return it."""
        if 'filename' not in self.metadata:
            return {}

        servername = socket.gethostname()
        LOG.debug("Servername = %s", str(servername))

        msg = {}
        if isinstance(self.metadata['filename'], list):
            dataset = []
            for filename in self.metadata['filename']:
                uri = 'ssh://{server}{path}'.format(server=servername, path=os.path.abspath(filename))
                uid = os.path.basename(filename)
                dataset.append({'uri': uri, 'uid': uid})
            msg['dataset'] = dataset
        else:
            filename = self.metadata['filename']
            uri = 'ssh://{server}{path}'.format(server=servername, path=os.path.abspath(filename))
            msg['uri'] = uri
            if 'uid' not in self.metadata:
                LOG.debug("Add uid as it was not included in the metadata from PPS")
                LOG.debug("Filename = %s", filename)
                msg['uid'] = os.path.basename(filename)

        return msg

    def is_segment(self):
        """Determine if the scene is a 'segment'.

        That means a sensor data granule, e.g. 85 seconds of VIIRS.
        """
        if not self.sensor_is_viirs():
            LOG.debug("Scene is not a VIIRS scene - and we assume then not a segment of a larger scene")
            return False

        delta_t = self.get_granule_duration()
        LOG.debug("Scene length: %s", str(delta_t.total_seconds()))
        if self.viirs_granule_time_bounds[0] < delta_t < self.viirs_granule_time_bounds[1]:
            LOG.info("VIIRS scene is a segment. Scene length = %s", str(delta_t))
            return True

        LOG.debug("VIIRS scene is not a segment")
        return False

    def sensor_is_viirs(self):
        """Check if the sensor is equal to VIIRS."""
        sensor = self.metadata.get('sensor')
        LOG.debug("Sensor = %s", str(sensor))
        return sensor == 'viirs'

    def get_granule_duration(self):
        """Derive the scene/granule duration as a timedelta object."""
        starttime = self.metadata['start_time']
        endtime = self.metadata['end_time']
        return (endtime - starttime + timedelta(seconds=SEC_DURATION_ONE_GRANULE))
