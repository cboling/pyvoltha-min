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

from .common.kvstore.twisted_etcd_store import TwistedEtcdStore         # pylint: disable=relative-beyond-top-level
from ..common.structlog_setup import update_logging, string_to_int      # pylint: disable=relative-beyond-top-level

COMPONENT_NAME = os.environ.get("COMPONENT_NAME")
GLOBAL_CONFIG_ROOT_NODE = "global"
DEFAULT_KV_STORE_CONFIG_PATH = "config"
KV_STORE_DATA_PATH_PREFIX = "service/voltha"
KV_STORE_PATH_SEPARATOR = "/"
CONFIG_TYPE = "loglevel"
DEFAULT_PACKAGE_NAME = "default"
GLOBAL_DEFAULT_LOGLEVEL = "WARN"


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

    @staticmethod
    def make_config_path(key):
        return DEFAULT_KV_STORE_CONFIG_PATH + KV_STORE_PATH_SEPARATOR + key + \
               KV_STORE_PATH_SEPARATOR + CONFIG_TYPE + KV_STORE_PATH_SEPARATOR + DEFAULT_PACKAGE_NAME

    @inlineCallbacks
    def get_global_loglevel(self):
        global_default_loglevel = ""

        try:
            level = yield self.etcd_client.get(self.global_config_path)
            if level is not None:
                level_int = string_to_int(str(level, 'utf-8'))

                if level_int == 0:
                    self.log.warn("unsupported-log-level", requested_level=level)
                else:
                    global_default_loglevel = level
                    self.log.debug("default-loglevel", new_level=level)

        except KeyError:
            self.log.warn("failed-to-retrieve-log-level", path=self.global_config_path)

        returnValue(global_default_loglevel)

    @inlineCallbacks
    def get_component_loglevel(self, global_default_loglevel):

        component_default_loglevel = global_default_loglevel

        try:
            level = yield self.etcd_client.get(self.component_config_path)

            if level is not None:
                level_int = string_to_int(str(level, 'utf-8'))

                if level_int == 0:
                    self.log.warn("unsupported-log-level", unsupported_level=level)

                else:
                    component_default_loglevel = level
                    self.log.debug("default-loglevel", new_level=level)

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

        self.process_log_config_change('startup')

        yield self.etcd_client.watch(self.global_config_path, self.watch_callback)
        yield self.etcd_client.watch(self.component_config_path, self.watch_callback)

    def watch_callback(self, event):
        reactor.callFromThread(self.process_log_config_change, event)

    @inlineCallbacks
    def process_log_config_change(self, event=None):
        self.log.info("log-change-occurred", event_info=event)

        global_default_level = yield self.get_global_loglevel()
        level = yield self.get_component_loglevel(global_default_level)

        level_int = string_to_int(str(level, 'utf-8'))

        current_log_level = level_int
        if LogController.active_log_level != current_log_level:
            LogController.active_log_level = current_log_level
            self.log.debug("applying-updated-loglevel",
                           previous_level=LogController.active_log_level,
                           new_level=current_log_level)
            update_logging(LogController.instance_id, verbosity_adjust=level_int)

        else:
            self.log.debug("Loglevel not updated")

    @inlineCallbacks
    def set_default_loglevel(self, global_config_path, component_config_path, initial_default_loglevel):
        self.log.debug('set-default-level', global_path=global_config_path,
                       component_path=component_config_path,
                       def_level=initial_default_loglevel)

        if (yield self.etcd_client.get(global_config_path)) is None:
            yield self.etcd_client.set(global_config_path, GLOBAL_DEFAULT_LOGLEVEL)

        if (yield self.etcd_client.get(component_config_path)) is None:
            yield self.etcd_client.set(component_config_path, initial_default_loglevel)
