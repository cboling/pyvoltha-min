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
import structlog
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from .common.kvstore.twisted_etcd_store import TwistedEtcdStore         # pylint: disable=relative-beyond-top-level
from ..common.structlog_setup import string_to_int                      # pylint: disable=relative-beyond-top-level

GLOBAL_CONFIG_ROOT_NODE = "global"
DEFAULT_KV_STORE_CONFIG_PATH = "config"
KV_STORE_DATA_PATH_PREFIX = "service/voltha"
KV_STORE_PATH_SEPARATOR = "/"
CONFIG_TYPE = "loglevel"
DEFAULT_PACKAGE_NAME = "default"
DEFAULT_SUFFIX = KV_STORE_PATH_SEPARATOR + DEFAULT_PACKAGE_NAME
GLOBAL_DEFAULT_LOGLEVEL = b"WARN"
LOG_LEVEL_INT_NOT_FOUND = 0


class LogController:
    instance_id = None
    active_log_level = None

    def __init__(self, etcd_host, etcd_port, component_name, watch_callback=None):
        self.log = structlog.get_logger()
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.etcd_client = TwistedEtcdStore(self.etcd_host, self.etcd_port, KV_STORE_DATA_PATH_PREFIX)
        self.global_config_path = self.make_component_path(GLOBAL_CONFIG_ROOT_NODE) + DEFAULT_SUFFIX
        self.component_config_path = self.make_component_path(component_name)
        self.global_log_level = GLOBAL_DEFAULT_LOGLEVEL
        self._watch_callback = watch_callback
        self._component_name = component_name

    @staticmethod
    def make_component_path(component):
        return DEFAULT_KV_STORE_CONFIG_PATH + KV_STORE_PATH_SEPARATOR + component + \
               KV_STORE_PATH_SEPARATOR + CONFIG_TYPE

    def make_config_path(self, key):
        return self.component_config_path + KV_STORE_PATH_SEPARATOR + key

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
    def get_component_default_loglevel(self, global_default_loglevel):
        self.log.info("entry", path=self.component_config_path)
        component_default_loglevel = global_default_loglevel

        try:
            path = self.make_config_path(DEFAULT_PACKAGE_NAME)
            level = yield self.etcd_client.get(path)

            if level is not None:
                level_int = string_to_int(str(level, 'utf-8'))

                if level_int == 0:
                    self.log.warn("unsupported-log-level", unsupported_level=level)

                else:
                    component_default_loglevel = level
                    self.log.info("default-loglevel", new_level=level)

        except KeyError:
            self.log.warn("failed-to-retrieve-log-level",
                          path=self.component_config_path + DEFAULT_SUFFIX)

        if component_default_loglevel == "":
            component_default_loglevel = GLOBAL_DEFAULT_LOGLEVEL.encode('utf-8')

        returnValue(component_default_loglevel)

    @inlineCallbacks
    def get_package_loglevel_int(self, package):
        self.log.debug("entry", path=self.component_config_path)
        level_int = LOG_LEVEL_INT_NOT_FOUND
        path = self.make_config_path(package)
        try:
            level = yield self.etcd_client.get(path)
            if level is not None:
                level_int = string_to_int(str(level, 'utf-8'))
                if level_int == 0:
                    self.log.warn("unsupported-log-level", unsupported_level=level)
                    level_int = LOG_LEVEL_INT_NOT_FOUND

        except KeyError:
            self.log.warn("failed-to-retrieve-log-level", path=path)

        returnValue(level_int)

    @inlineCallbacks
    def start_watch_log_config_change(self, instance_id, initial_default_loglevel):
        self.log.debug("entry")
        LogController.instance_id = instance_id

        if self._component_name is None:
            raise Exception("Invalid pod component name")

        _results = yield self.etcd_client.watch(self.global_config_path, self.watch_callback)
        _results = yield self.etcd_client.watch(self.component_config_path, self.watch_callback,
                                                watch_prefix=True)

        _results = yield self.set_default_loglevel(self.global_config_path,
                                                   self.component_config_path,
                                                   initial_default_loglevel.upper())

    def watch_callback(self, event):
        reactor.callFromThread(self.process_log_config_change, event)

    @inlineCallbacks
    def process_log_config_change(self, watch_event=None):
        # Forward to user callback
        if self._watch_callback is not None:
            results = yield self._watch_callback(watch_event)
            returnValue(results)

        returnValue(False)

    @inlineCallbacks
    def set_default_loglevel(self, global_config_path, component_config_path, initial_default_loglevel):
        self.log.debug('set-default-level', global_path=global_config_path,
                       component_path=component_config_path,
                       def_level=initial_default_loglevel)

        if (yield self.etcd_client.get(global_config_path)) is None:
            _results = yield self.etcd_client.set(global_config_path, GLOBAL_DEFAULT_LOGLEVEL)

        default_path = self.make_config_path(DEFAULT_PACKAGE_NAME)
        if (yield self.etcd_client.get(default_path)) is None:
            _results = yield self.etcd_client.set(default_path, initial_default_loglevel)
