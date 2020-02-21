#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Pytroll
#
# Author(s):
#
#   Erik Johansson <Firstname.Lastname@smhi.se>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

#: General import
import sys
import pdb
import numpy as np
import logging
import yaml
import datetime
from multiprocessing import cpu_count
import threading
# import posttroll.subscriber  # @UnresolvedImport
from posttroll.subscriber import Subscribe  # @UnresolvedImport
from posttroll.publisher import Publish  # @UnresolvedImport
#: Module import
#from publish_and_listen import FileListener, FilePublisher  # @UnresolvedImport

#: import differences between python 2 and 3
import six
import os
import socket
import netifaces
if six.PY2:
    import Queue
    from urlparse import urlparse
#     from urlparse import urlunsplit
elif six.PY3:
    import queue as Queue  # @UnresolvedImport
    from urllib.parse import urlparse  # @UnresolvedImport
#     from urllib.parse import urlunsplit  # @UnresolvedImport

#: yaml importer
try:
    from yaml import UnsafeLoader
except ImportError:
    from yaml import Loader as UnsafeLoader



SUPPORTED_METEOSAT_SATELLITES = ['meteosat-09', 'meteosat-10', 'meteosat-11']
PPS_SENSORS = ['amsu-a', 'amsu-b', 'mhs', 'avhrr/3', 'viirs', 'modis'] + ['seviri']
SENSOR_LIST = {}
for sat in SUPPORTED_METEOSAT_SATELLITES:
    SENSOR_LIST[sat] = ['seviri']
SUPPORTED_EOS_SATELLITES = SUPPORTED_JPSS_SATELLITES = NOAA_METOP_PPS_SENSORNAMES = REQUIRED_MW_SENSORS = SUPPORTED_NOAA_SATELLITES = SUPPORTED_METOP_SATELLITES = []
# : Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# : Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'





class FileListener(threading.Thread):

    def __init__(self, queue, subscribe_topics):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.subscribe_topics = subscribe_topics

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        LOG.debug("Subscribe topics = %s", str(self.subscribe_topics))
        with Subscribe("", self.subscribe_topics, True) as subscr:

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
                'start_time' not in msg.data):# or 
#                 'orbit_number' not in msg.data or
            LOG.warning("Message is lacking crucial fields...")
            return False

        if (msg.data['platform_name'].lower() not in SUPPORTED_METEOSAT_SATELLITES):
            LOG.info(str(msg.data['platform_name']) + ": " +
                     "Not a NOAA/Metop/S-NPP/Terra/Aqua scene. Continue...")
            return False

        return True


class FilePublisher(threading.Thread):

    """A publisher for the PPS result files. Picks up the return value from the
    pps_worker when ready, and publishes the files via posttroll"""

    def __init__(self, queue, publish_topic, **kwargs):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}
        self.publish_topic = publish_topic
        self.runner_name = kwargs.get('runner_name', 'pps_runner')

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish(self.runner_name, 0, self.publish_topic) as publisher:

            while self.loop:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the files...")
                    publisher.send(retv)


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

def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips

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
        if destination is None:
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
            #: TODO: Re instate this statement. Just remove during devolpment on bi
#             return False
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

    if msg.data['platform_name'].lower() in SUPPORTED_METEOSAT_SATELLITES:
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

#     if platform_name not in SATELLITE_NAME:
#         LOG.warning("Satellite not supported: " + str(platform_name))
#         return False
    
    if platform_name.lower() not in SUPPORTED_METEOSAT_SATELLITES:
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
    
    #: TODO: Remove
    return True
    pdb.set_trace()
    
    
    
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
    elif platform_name.lower() in SUPPORTED_METEOSAT_SATELLITES:
        for ppsfile in ppsfiles:
            if os.path.basename(ppsfile).find('NWC') >= 0: #: TODO: What i this?
                return ppsfile
            
    return None

        

def pps_runner(options):
#     LOG.debug("Listens for messages of type: %s", str(options['message_types']))
# 
#     ncpus_available = cpu_count()
#     LOG.info("Number of CPUs available = " + str(ncpus_available))
#     ncpus = int(options.get('num_of_cpus', 1))
#     LOG.info("Will use %d CPUs when running the PPS SEVIRI instances", ncpus)
# 
#     af_proc = ActiveL1cProcessor(ncpus)
    
    """PPS SEVIRI runner. Triggers processing of PPS main script once 
    seviri_l1c_runner is ready with a level-1 file for SEVIRI"""
    
    LOG.info("*** Start the PPS level-2 runner for SEVIRI:")

#     LOG.info("First check if NWP data should be downloaded and prepared")
#     now = datetime.utcnow()  # @UndefinedVariable
#     update_nwp(now - datetime.timedelta(days=1), NWP_FLENS)
#     LOG.info("Ready with nwp preparation...")
    
    
    ncpus_available = cpu_count()
    LOG.info("Number of CPUs available = " + str(ncpus_available))
    ncpus_wanted = int(options.get('num_of_cpus', 1))
    ncpus = int(np.min([ncpus_available, ncpus_wanted]))
    LOG.info("Will use %d CPUs when running the PPS SEVIRI instances", ncpus)
#     listener_q = Queue.Queue()
#     listen_thread = FileListener(listener_q, options['subscribe_topic'])
#     listen_thread.start()

#     publisher_q = Queue.Queue()
#     pub_thread = FilePublisher(publisher_q, options['publish_topic'], runner_name='pps2018_runner')
#     pub_thread.start()

    files4pps = {}
    thread_pool = ThreadPool(ncpus)
    with Subscribe('', options['message_types'], True) as sub:
        with Publish('seviri_l1c_runner', 0) as publisher:
            while True:
                for msg in sub.recv():
                    
#         try:
#             msg = listener_q.get()
#         except Queue.Empty:
#             continue
                    LOG.debug(
                        "Number of threads currently alive: " + str(threading.active_count()))
                    if isinstance(msg.data['sensor'], list):
                        msg.data['sensor'] = msg.data['sensor'][0]
                    if 'orbit_number' not in msg.data.keys():
                        msg.data.update({'orbit_number': 99999})
                    if 'end_time' not in msg.data.keys():
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
                    status = ready2run(msg, files4pps,
                                       sdr_granule_processing=options.get('sdr_processing') == 'granules')
                    if status:
                        sceneid = get_sceneid(platform_name, orbit_number, starttime)
                        scene['file4pps'] = get_pps_inputfile(platform_name, files4pps[sceneid])
                        pdb.set_trace()
            
                        LOG.info('Start a thread preparing the nwp data and run pps...')
                        thread_pool.new_thread(message_uid(msg),
                                               target=run_nwp_and_pps, args=(scene, NWP_FLENS,
                                                                             publisher_q,
                                                                             msg, options))
            
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


def get_config(configfile, service, procenv):
    """Get the configuration from file"""

    with open(configfile, 'r') as fp_:
        config = yaml.load(fp_, Loader=UnsafeLoader)

    options = {}
    for item in config:
        if not isinstance(config[item], dict):
            options[item] = config[item]
        elif item in [service]:
            for key in config[service]:
                if not isinstance(config[service][key], dict):
                    options[key] = config[service][key]
                elif key in [procenv]:
                    for memb in config[service][key]:
                        options[memb] = config[service][key][memb]

    return options


if __name__ == '__main__':
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)
    
    config_filename = "test_config.yaml.template"
    service_name = "test"
    environ = "utv"
    
    OPTIONS = get_config(config_filename, service_name, environ)
    
    OPTIONS['environment'] = environ
    OPTIONS['nagios_config_file'] = None

    LOG = logging.getLogger('test-runner')
    
    pps_runner(OPTIONS)

