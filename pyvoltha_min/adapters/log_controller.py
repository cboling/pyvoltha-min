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

from jaeger_client import Config
from jaeger_client.constants import SAMPLER_TYPE_CONST

from pyvoltha_min.common.config.twisted_etcd_store import TwistedEtcdStore
from pyvoltha_min.common.structlog_setup import string_to_int
from pyvoltha_min.common.config.kvstore_prefix import KvStore
from pyvoltha_min.adapters.log_features import GlobalTracingSupport

GLOBAL_CONFIG_ROOT_NODE = "global"
DEFAULT_KV_STORE_CONFIG_PATH = "config"
KV_STORE_PATH_SEPARATOR = "/"
CONFIG_TYPE = "loglevel"
FEATURE_TYPE = "logfeatures"
TRACING_TYPE = "trace_publish"
CORRELATION_TYPE = "log_correlation"
FEATURE_ENABLED = b'ENABLED'
FEATURE_DISABLED = b'DISABLED'
DEFAULT_PACKAGE_NAME = "default"
DEFAULT_SUFFIX = KV_STORE_PATH_SEPARATOR + DEFAULT_PACKAGE_NAME
GLOBAL_DEFAULT_LOGLEVEL = b'WARN'
LOG_LEVEL_INT_NOT_FOUND = 0


class LogController:
    instance_id = None
    active_log_level = None

    def __init__(self, etcd_host, etcd_port, scope_manager, config, watch_callback=None):
        self.log = structlog.get_logger()
        self._initialized = False
        self._etcd_host = etcd_host
        self._etcd_port = etcd_port
        self._etcd_client = TwistedEtcdStore(self._etcd_host, self._etcd_port, KvStore.prefix)
        self.global_config_path = self.make_component_path(GLOBAL_CONFIG_ROOT_NODE) + DEFAULT_SUFFIX

        self._component_name = config.get('component_name')
        self._component_config_path = self.make_component_path(self._component_name)
        self._feature_root = self.make_root_path(self._component_name)
        self._global_log_level = GLOBAL_DEFAULT_LOGLEVEL
        self._watch_callback = watch_callback

        # Reference
        GlobalTracingSupport.set_log_controller(self, scope_manager, config)

    @staticmethod
    def make_component_path(component):
        return DEFAULT_KV_STORE_CONFIG_PATH + KV_STORE_PATH_SEPARATOR + component + \
               KV_STORE_PATH_SEPARATOR + CONFIG_TYPE

    @staticmethod
    def make_root_path(component):
        return DEFAULT_KV_STORE_CONFIG_PATH + KV_STORE_PATH_SEPARATOR + component + \
               KV_STORE_PATH_SEPARATOR + FEATURE_TYPE

    def make_config_path(self, key):
        return self._component_config_path + KV_STORE_PATH_SEPARATOR + key

    def make_feature_path(self, feature):
        return self._feature_root + KV_STORE_PATH_SEPARATOR + feature

    def is_global_config_path(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf-8')

        return key is not None and \
               key[:len(KvStore.prefix)] == KvStore.prefix and \
               key[len(KvStore.prefix)+1:] == self.global_config_path

    def is_component_default_path(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf-8')

        default_path = self.make_config_path(DEFAULT_PACKAGE_NAME)
        return key is not None and \
               key[:len(KvStore.prefix)] == KvStore.prefix and \
               key[len(KvStore.prefix)+1:] == default_path

    def is_feature_path(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf-8')

        if self.make_feature_path(TRACING_TYPE) in key:
            return True, TRACING_TYPE

        if self.make_feature_path(CORRELATION_TYPE) in key:
            return True, CORRELATION_TYPE

        return False, None

    @inlineCallbacks
    def get_global_loglevel(self):
        loglevel = self._global_log_level
        try:
            level = yield self._etcd_client.get(self.global_config_path)
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
    def get_log_features(self):
        features = dict()
        try:
            features_info = yield self._etcd_client.get(self.feature_path)
            if features_info is not None:
                # level_int = string_to_int(str(level, 'utf-8'))
                #
                # if level_int == 0:
                #     self.log.warn("unsupported-log-level", requested_level=level)
                # else:
                #     loglevel = level
                pass
            # TODO: Convert to dictionary
        except KeyError:
            self.log.warn("failed-to-retrieve-log-level", path=self.global_config_path)

        returnValue(features)

    @inlineCallbacks
    def get_component_default_loglevel(self, global_default_loglevel):
        self.log.info("entry", path=self._component_config_path)
        component_default_loglevel = global_default_loglevel

        try:
            path = self.make_config_path(DEFAULT_PACKAGE_NAME)
            level = yield self._etcd_client.get(path)

            if level is not None:
                level_int = string_to_int(str(level, 'utf-8'))

                if level_int == 0:
                    self.log.warn("unsupported-log-level", unsupported_level=level)

                else:
                    component_default_loglevel = level
                    self.log.info("default-loglevel", new_level=level)

        except KeyError:
            self.log.warn("failed-to-retrieve-log-level",
                          path=self._component_config_path + DEFAULT_SUFFIX)

        if component_default_loglevel == "":
            component_default_loglevel = GLOBAL_DEFAULT_LOGLEVEL.encode('utf-8')

        returnValue(component_default_loglevel)

    @inlineCallbacks
    def get_package_loglevel_int(self, package):
        self.log.debug("entry", path=self._component_config_path)
        level_int = LOG_LEVEL_INT_NOT_FOUND
        path = self.make_config_path(package)
        try:
            level = yield self._etcd_client.get(path)
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

        _results = yield self._etcd_client.watch(self.global_config_path, self.watch_callback)
        _results = yield self._etcd_client.watch(self._component_config_path, self.watch_callback,
                                                 watch_prefix=True)
        _results = yield self._etcd_client.watch(self._feature_root, self.watch_callback,
                                                 watch_prefix=True)
        _results = yield self.set_default_loglevel(self.global_config_path,
                                                   self._component_config_path,
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

        if (yield self._etcd_client.get(global_config_path)) is None:
            _results = yield self._etcd_client.set(global_config_path, GLOBAL_DEFAULT_LOGLEVEL)

        default_path = self.make_config_path(DEFAULT_PACKAGE_NAME)
        if (yield self._etcd_client.get(default_path)) is None:
            _results = yield self._etcd_client.set(default_path, initial_default_loglevel)

    @inlineCallbacks
    def persist_initial_log_features(self):
        log_correlation = FEATURE_ENABLED if GlobalTracingSupport.log_correlation_status else FEATURE_DISABLED
        trace_enabled = FEATURE_ENABLED if GlobalTracingSupport.trace_publishing_status else FEATURE_DISABLED

        self.log.debug('presist-log-features', feature_root=self._feature_root,
                       correlation=log_correlation, tracing=trace_enabled)
        try:
            path = self.make_feature_path(CORRELATION_TYPE)
            status = yield self._etcd_client.get(path)
            if status != log_correlation:
                _results = yield self._etcd_client.set(path, log_correlation)

            path = self.make_feature_path(TRACING_TYPE)
            status = yield self._etcd_client.get(path)
            if status != trace_enabled:
                _results = yield self._etcd_client.set(path, trace_enabled)

        except Exception as e:
            self.log.exception('initial-persist-failed', e=e)
            # Return none always

    def enable_jaeger_client(self):
        log_correlation_enabled = GlobalTracingSupport.log_correlation_status
        tracing_enabled = GlobalTracingSupport.trace_publishing_status

        if tracing_enabled or log_correlation_enabled:
            # The following are available 'config' dictionary options
            #
            # logging
            #   - If true, the jaeger tracing structured logger (jaeger_tracing component) is also provided
            #     to the ErrorReporter for any RemoteThrottler and a CompositeReporter is provided to the
            #     Tracer
            #   - Default: False
            #
            # local_agent (dict)
            #   - Default:
            #      - 'enabled' (bool), Default: LOCAL_AGENT_DEFAULT_ENABLED = True
            #      - 'sampling_port' , Default: DEFAULT_SAMPLING_PORT = 5778
            #      - 'reporting_port', Default: DEFAULT_REPORTING_PORT = 6831
            #      - 'reporting_host', Default: DEFAULT_REPORTING_HOST = 'localhost' if environment variable
            #                                   'JAEGER_AGENT_HOST' is not defined
            #
            # sampler
            #   - Sampler config (dict) A sampler is responsible for deciding if a particular
            #                           span should be "sampled", i.e. recorded in permanent storage. Besides
            #                           the ones that can be defined via config, Jaeger also has several
            #                           additional Samplers available including AdaptiveSampler,
            #                           RemoteControlledSampler, GuaranteedThroughputProbabilisticSampler
            #     - 'type': Sampler type (str) See below
            #        - default: None
            #             - SAMPLER_TYPE_CONST = "const" A sampler always returns the same decision.
            #                  - param -> decision (bool)
            #                             default: False
            #
            #             - SAMPLER_TYPE_PROBABILISTIC = "PROBABILISTIC". A sampler that randomly samples
            #                             a certain percentage of traces specified by the samplingRate, in
            #                             the range between 0.0 and 1.0.
            #                              It relies on the fact that new trace IDs are 64bit random numbers
            #                              themselves, thus making the sampling decision without generating
            #                              a new random number, but simply calculating if
            #                              traceID < (samplingRate * 2^64). Note that we actually
            #                              (zero out) the most significant bit.
            #                  - param -> rate (float) Sampling rate must be between 0.0 and 1.0
            #                             default: False
            #
            #             - SAMPLER_TYPE_RATE_LIMITING = "RATE_LIMITING" or 'rate_limiting' Samples at most
            #                              max_traces_per_second. The distribution of sampled traces follows
            #                              burstiness of the service, i.e. a service with uniformly distributed
            #                              requests will have those requests sampled uniformly as well, but if
            #                              requests are bursty, especially sub-second, then a number of
            #                              sequential requests can be sampled each second.
            #                  - param -> max_traces_per_second (float)
            #                             default: 10
            #
            #   - 'param': Parameter to provide to sampler type. See defined sampler types above
            #              for more information.
            #              - default: None
            # tags
            #   - Process-wide tracer tags
            #   - Default: None
            #
            # enabled
            #   - (bool) Defined, but I do not see it in use any where in Jaeger-cient
            #   - Default: True
            #
            # reporter_batch_size
            #   - How many spans a remote reporter can submit at once to Collector
            #   - Default: 10
            #
            # reporter_queue_size
            #   - Remote reporter queue size.  How many spans a remote reporter can hold
            #     in memory before starting to drop spans
            #   - Default: 100
            #
            # propagation
            #   - (str) if equal to 'b3' replace the codec with a B3 enabled instance
            #   - Default: None
            #
            # max_tag_value_length
            #   - Max allowed tag value length. Longer values will be truncated.
            #   - Default: MAX_TAG_VALUE_LENGTH = 1024
            #
            # max_traceback_length
            #   -  Max length for traceback data. Longer values will be truncated.
            #   - Default: MAX_TRACEBACK_LENGTH = 4096
            #
            # reporter_flush_interval
            #   - How often remote reporter does a preemptive flush of its buffers, A remote reporter
            #     receives completed spans from Tracer and submits them out of process
            #   - Default: DEFAULT_FLUSH_INTERVAL = 1
            #
            # sampling_refresh_interval
            #   - interval in seconds for polling for strategy
            #   - Default: DEFAULT_SAMPLING_INTERVAL = 5
            #
            # trace_id_header
            #   - The name of the HTTP header used to encode trace ID
            #   - Default: TRACE_ID_HEADER -> 'uber-trace-id' if six.PY3 else b'uber-trace-id'
            #
            # generate_128bit_trace_id
            #   - Generate 128 bit trace id, otherwise 16
            #   - Default: False
            #   - Override: True if env variable 'JAEGER_TRACEID_128BIT' == 'true'
            #
            # baggage_header_prefix
            #   - The prefix for HTTP headers used to record baggage
            #   - Default: 'uberctx-' if six.PY3 else b'uberctx-'
            #
            # service_name
            #   - Service Name. Used to override service name defined in 'service_name' parameter to
            #                   the Config() initializer
            #   - Default: None
            #
            # throttler
            #   - Throttler Group (dict). If not specified, a default of 'None' is used which specifies
            #                             that a remote throttler will be created.  If created a Remote
            #                             Throttler controls the flow of spans emitted from client to
            #                             prevent flooding. RemoteThrottler requests credits from the
            #                             throttling service periodically. These credits determine the
            #                             amount of debug spans a client may emit for a particular
            #                             operation without receiving more credits.
            #      - 'port':             (int) DEFAULT_THROTTLER_PORT = DEFAULT_SAMPLING_PORT = 5778
            #      - 'refresh_interval': (int) DEFAULT_THROTTLER_INTERVAL = 5
            #   - Default: None
            #
            address = GlobalTracingSupport.active_trace_agent_address.split(':')

            config = Config(
                config={  # usually read from some yaml config
                    'sampler': {
                        'type': SAMPLER_TYPE_CONST,
                        'param': tracing_enabled,
                    },
                    'local_agent': {
                        'reporting_host': address[0],
                        'reporting_port': int(address[1]),
                    },
                    'logging': True,
                },
                service_name=GlobalTracingSupport.component_name,
                scope_manager=GlobalTracingSupport.scope_manager,
                validate=True,
            )
            # this call also sets opentracing.tracer
            if self._initialized:
                config.new_tracer()
            else:
                self._initialized = True
                config.initialize_tracer()
