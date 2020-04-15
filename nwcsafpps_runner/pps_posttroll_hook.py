#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2019 Adam.Dybbroe

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

"""

import os
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
WAIT_NSECS_PPS_PUBLISH = 1

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"

PPS_PRODUCT_FILE_ID = {'ppsMakeAvhrr': 'RAD_SUN',
                       'ppsMakeViirs': 'RAD_SUN',
                       'ppsMakePhysiography': 'PHY',
                       'ppsMakeNwp': 'NWP',
                       'ppsCmaskPrepare': 'CMA-PRE',
                       'ppsCmask': 'CMA',
                       'ppsCtth': 'CTTH',
                       'ppsCtype': 'CT',
                       'ppsCpp': 'CPP',
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


class PPSPublisher(threading.Thread):

    """A publisher for the PPS modules"""

    """
    It publish a message via posttroll when a PPS module has finished

    """

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def stop(self):
        """Stops the file publisher"""
        self.queue.put(None)

    def run(self):

        with Publish('PPS', 0, ) as publisher:
            time.sleep(WAIT_NSECS_PPS_PUBLISH)

            while True:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the message...")
                    publisher.send(retv)
                    LOG.info("Message published!")
                else:
                    break


class PPSMessage(object):

    """A Posttroll message class to trigger the sending of a notifcation that a PPS PGE os ready

    """

    def __init__(self, station, posttroll_topic, output_format, level):

        # __init__ is not run when created from yaml
        # See http://pyyaml.org/ticket/48
        pass

    def __getstate__(self):
        d__ = {'station': self.station,
               'posttroll_topic': self.posttroll_topic,
               'output_format': self.output_format,
               'level': self.level}
        return d__

    def __setstate__(self, mydict):
        self.station = mydict['station']
        self.posttroll_topic = mydict['posttroll_topic']
        self.output_format = mydict['output_format']
        self.level = mydict['level']

    def __call__(self, status, mda):

        LOG.debug("Station = %s", str(self.station))
        LOG.debug("PostTroll topic = %s", str(self.posttroll_topic))
        LOG.debug("Output Format = %s", str(self.output_format))
        LOG.debug("Level = %s", str(self.level))

        if status != 0:
            # Error
            # pubmsg = self.create_message("FAILED", mda)
            LOG.warning("Module %s failed, so no message sent", mda.get('module', 'unknown'))
        else:
            # Ok
            pubmsg = self.create_message("OK", mda)

            manager = Manager()
            publisher_q = manager.Queue()

            pub_thread = PPSPublisher(publisher_q)
            pub_thread.start()
            LOG.info("Sending: " + str(pubmsg))
            publisher_q.put(pubmsg)
            pub_thread.stop()

    def create_message(self, status, mda):
        """Create the posttroll message from the PPS metadata"""

        import socket

        servername = socket.gethostname()
        LOG.debug("Servername = %s", str(servername))

        to_send = {}
        for key in mda:
            # Disregard the PPS keyword "filename". We will use URI/UID instead - see below:
            if key not in to_send and key != 'filename':
                to_send[key] = mda[key]

            if key == 'platform_name':
                to_send[key] = PLATFORM_CONVERSION_PPS2OSCAR.get(mda[key], mda[key])

        if isinstance(mda['filename'], list):
            dataset = []
            for filename in mda['filename']:
                uri = 'ssh://{server}{path}'.format(server=servername, path=os.path.abspath(filename))
                uid = os.path.basename(filename)
                dataset.append({'uri': uri, 'uid': uid})
            to_send['dataset'] = dataset
        else:
            filename = mda['filename']

            uri = 'ssh://{server}{path}'.format(server=servername, path=os.path.abspath(filename))
            to_send['uri'] = uri
            if 'uid' not in to_send:
                LOG.debug("Add uid as it was not included in the metadata from PPS")
                LOG.debug("Filename = %s", filename)
                to_send['uid'] = os.path.basename(filename)

        to_send['data_processing_level'] = self.level
        to_send['format'] = self.output_format
        to_send['status'] = status
        station = self.station

        pps_product = PPS_PRODUCT_FILE_ID.get(mda.get('module', 'unknown'), 'UNKNOWN')
        environment = MODE

        if is_segment(mda):
            topic = '/segment/'
        else:
            topic = '/'

        pub_message = Message(topic + to_send['format'] + '/' +
                              to_send['data_processing_level'] + '/' +
                              pps_product + '/' +
                              station + '/' + environment +
                              '/polar/direct_readout/',
                              "file", to_send).encode()
        return pub_message


def is_segment(pps_info):
    """Determine if the scene is a 'segment' (that is a sensor data granule,
       e.g. 85 seconds of VIIRS)
    """

    starttime = pps_info['start_time']
    endtime = pps_info['end_time']
    sensor = pps_info.get('sensor')
    LOG.debug("Sensor = %s", str(sensor))
    if sensor and sensor == 'viirs':
        delta_t = (endtime - starttime)
        LOG.debug("Scene length: %s", str(delta_t))
        if delta_t < VIIRS_TIME_THR2 and delta_t > VIIRS_TIME_THR1:
            LOG.info("VIIRS scene is a segment. Scene length = %s", str(delta_t))
            return True

    LOG.debug("Scene is not a segment")
    return False
