#
# Copyright 2020 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os

import structlog
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from etcd3.watch import WatchResponse
from etcd3.events import PutEvent, DeleteEvent

from .common.kvstore.twisted_etcd_store import TwistedEtcdStore         # pylint: disable=relative-beyond-top-level
from ..common.structlog_setup import update_logging, string_to_int      # pylint: disable=relative-beyond-top-level

COMPONENT_NAME = os.environ.get("COMPONENT_NAME")
GLOBAL_CONFIG_ROOT_NODE = "global"
DEFAULT_KV_STORE_CONFIG_PATH = "config"
KV_STORE_DATA_PATH_PREFIX = "service/voltha"
KV_STORE_PATH_SEPARATOR = "/"
CONFIG_TYPE = "loglevel"
DEFAULT_PACKAGE_NAME = "default"
GLOBAL_DEFAULT_LOGLEVEL = b"WARN"


class LogController:
    instance_id = None
    active_log_level = None

    def __init__(self, etcd_host, etcd_port):
        self.log = structlog.get_logger()
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.etcd_client = TwistedEtcdStore(self.etcd_host, self.etcd_port, KV_STORE_DATA_PATH_PREFIX)
        self.global_config_path = self.make_config_path(GLOBAL_CONFIG_ROOT_NODE)
        self.component_config_path = self.make_config_path(COMPONENT_NAME)
        self.global_log_level = GLOBAL_DEFAULT_LOGLEVEL

    @staticmethod
    def make_config_path(key):
        return DEFAULT_KV_STORE_CONFIG_PATH + KV_STORE_PATH_SEPARATOR + key + \
               KV_STORE_PATH_SEPARATOR + CONFIG_TYPE + KV_STORE_PATH_SEPARATOR + DEFAULT_PACKAGE_NAME

    def is_global_config_path(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf-8')

        return key is not None and \
               key[:len(KV_STORE_DATA_PATH_PREFIX)] == KV_STORE_DATA_PATH_PREFIX and \
               key[len(KV_STORE_DATA_PATH_PREFIX)+1:] == self.global_config_path

    def is_component_default_path(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf-8')

        default_path = self.make_config_path(DEFAULT_PACKAGE_NAME)
        return key is not None and \
               key[:len(KV_STORE_DATA_PATH_PREFIX)] == KV_STORE_DATA_PATH_PREFIX and \
               key[len(KV_STORE_DATA_PATH_PREFIX)+1:] == default_path

    @inlineCallbacks
    def get_global_loglevel(self):
        loglevel = self.global_log_level
        try:
            level = yield self.etcd_client.get(self.global_config_path)
            if level is not None:
                level_int = string_to_int(str(level, 'utf-8'))

                if level_int == 0:
                    self.log.warn("unsupported-log-level", requested_level=level)
                else:
                    loglevel = level

        except KeyError:
            self.log.warn("failed-to-retrieve-log-level", path=self.global_config_path)

        returnValue(loglevel)

    @inlineCallbacks
    def get_component_loglevel(self, global_default_loglevel):
        self.log.info("entry", path=self.component_config_path)

        component_default_loglevel = global_default_loglevel

        try:
            level = yield self.etcd_client.get(self.component_config_path)

            if level is not None:
                level_int = string_to_int(str(level, 'utf-8'))

                if level_int == 0:
                    self.log.warn("unsupported-log-level", unsupported_level=level)

                else:
                    component_default_loglevel = level
                    self.log.info("default-loglevel", new_level=level)

        except KeyError:
            self.log.warn("failed-to-retrieve-log-level", path=self.component_config_path)

        if component_default_loglevel == "":
            component_default_loglevel = GLOBAL_DEFAULT_LOGLEVEL.encode('utf-8')

        returnValue(component_default_loglevel)

    @inlineCallbacks
    def start_watch_log_config_change(self, instance_id, initial_default_loglevel):
        self.log.debug("entry")
        LogController.instance_id = instance_id

        if COMPONENT_NAME is None:
            raise Exception("Unable to retrieve pod component name from runtime env")

        self.global_config_path = self.make_config_path(GLOBAL_CONFIG_ROOT_NODE)
        self.component_config_path = self.make_config_path(COMPONENT_NAME)

        self.set_default_loglevel(self.global_config_path,
                                  self.component_config_path,
                                  initial_default_loglevel.upper())
        # Initial seed
        self.process_log_config_change('startup')

        yield self.etcd_client.watch(self.global_config_path, self.watch_callback)
        yield self.etcd_client.watch(self.component_config_path, self.watch_callback)

    def watch_callback(self, event):
        reactor.callFromThread(self.process_log_config_change, event)

    @inlineCallbacks
    def process_log_config_change(self, watch_event=None):
        if isinstance(watch_event, WatchResponse):
            self.log.info("log-change-occurred", events=watch_event.events)
            event_list = list()
            for evt in watch_event.events:
                if isinstance(evt, PutEvent):
                    info = {'type': 'put', 'key': evt.key.decode('utf-8'), 'value': evt.value.decode('utf-8')}
                elif isinstance(evt, DeleteEvent):
                    info = {'type': 'delete', 'key': evt.key.decode('utf-8'), 'value': None}
                else:
                    info = {'type': 'unsupported', 'key': str(evt), 'value': None}

                event_list.append(info)
        else:
            self.log.info("log-change-occurred", event_info=watch_event)
            event_list = [{'type': 'startup', 'key': None, 'value': None}]

        self.log.info('log-change-list', event_list=event_list)
        for event in event_list:
            self.log.info('event-info', event_type=event.get('type'),
                          key=event.get('key'), value=event.get('value'),
                          is_global=self.is_global_config_path(event.get('key')),
                          is_default=self.is_component_default_path(event.get('key')))

        global_default_level = yield self.get_global_loglevel()
        if self.global_log_level != global_default_level:
            self.log.info('global-loglevel-changed', previous_level=self.global_log_level,
                          new_level=global_default_level)
            self.global_log_level = global_default_level

        # Get this component's default level
        level = yield self.get_component_loglevel(global_default_level)
        level_int = string_to_int(str(level, 'utf-8'))

        current_log_level = level_int

        if LogController.active_log_level != current_log_level:
            self.log.info("applying-updated-loglevel",
                          previous_level=LogController.active_log_level,
                          new_level=current_log_level)
            LogController.active_log_level = current_log_level
            update_logging(LogController.instance_id, verbosity_adjust=level_int)

        else:
            self.log.info("Loglevel not updated", current_level=LogController.active_log_level)

    @inlineCallbacks
    def set_default_loglevel(self, global_config_path, component_config_path, initial_default_loglevel):
        self.log.debug('set-default-level', global_path=global_config_path,
                       component_path=component_config_path,
                       def_level=initial_default_loglevel)

        if (yield self.etcd_client.get(global_config_path)) is None:
            yield self.etcd_client.set(global_config_path, GLOBAL_DEFAULT_LOGLEVEL)

        if (yield self.etcd_client.get(component_config_path)) is None:
            yield self.etcd_client.set(component_config_path, initial_default_loglevel)
