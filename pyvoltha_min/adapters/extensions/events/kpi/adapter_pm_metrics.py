# Copyright 2017-present Open Networking Foundation
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

import time
import random
import structlog
from twisted.internet.task import LoopingCall
from voltha_protos.device_pb2 import PmConfig
from voltha_protos.events_pb2 import EventType, EventCategory, EventSubCategory
from voltha_protos.events_pb2 import KpiEvent2, KpiEventType, MetricInformation, MetricMetaData


class AdapterPmMetrics:
    """
    Base class for Device Adapter PM Metrics Manager

    Device specific (OLT, ONU, OpenOMCI, ...) will derive groups of PM information
    and this base class is primarily used to provide a consistent interface to configure,
    start, and stop statistics collection.
    """
    DEFAULT_FREQUENCY_KEY = 'default-collection-frequency'
    DEFAULT_COLLECTION_FREQUENCY = 15      # seconds

    # If the collection object has a property of the following name, it will be used
    # to retrieve the UTC Collection Timestamp (UTC seconds since epoch). If the collection
    # object does not support this attribute, the current time will be used. If the attribute
    # is supported, but returns None, this signals that no metrics are currently available
    # for collection.
    TIMESTAMP_ATTRIBUTE = 'timestamp'

    # pylint: disable=too-many-arguments
    def __init__(self, event_mgr, core_proxy, device_id, logical_device_id, serial_number,
                 grouped=False, freq_override=False, max_skew=5, **kwargs):
        """
        Initializer for shared Device Adapter PM metrics manager

        :param core_proxy: (CoreProxy) Gateway between CORE and an adapter
        :param device_id: (str) Device ID
        :param logical_device_id: (str) VOLTHA Logical Device ID
        :param grouped: (bool) Flag indicating if statistics are managed as a group
        :param freq_override: (bool) Flag indicating if frequency collection can be specified
                                     on a per group basis
        :param kwargs: (dict) Device Adapter specific values
        """
        self.log = structlog.get_logger(device_id=device_id)
        self.event_mgr = event_mgr
        self.device_id = device_id
        self.core_proxy = core_proxy
        self.name = core_proxy.listening_topic
        self.logical_device_id = logical_device_id
        self.serial_number = serial_number
        self.default_freq = kwargs.get(AdapterPmMetrics.DEFAULT_FREQUENCY_KEY,
                                       AdapterPmMetrics.DEFAULT_COLLECTION_FREQUENCY)
        self._event = "KPI_EVENT"
        self._category = EventCategory.EQUIPMENT
        self._sub_category = kwargs.get('subcategory', EventSubCategory.OLT)
        self.grouped = grouped
        self.freq_override = grouped and freq_override
        self.pm_group_metrics = {}      # name -> PmGroupConfig
        self.lp_callbacks = {}          # name -> LoopingCallback
        self.max_skew = max_skew

    def update(self, pm_config):
        # TODO: Move any common steps into base class
        raise NotImplementedError('Your derived class should override this method')

    def make_proto(self, pm_config=None):
        raise NotImplementedError('Your derived class should override this method')

    def start_collector(self, callback=None):
        """
        Start the collection loop for an adapter if the frequency > 0

        :param callback: (callable) Function to call to collect PM data
        """
        self.log.info("starting-pm-collection", device_name=self.name, default_freq=self.default_freq)
        if callback is None:
            callback = self.collect_and_publish_metrics

        for name, group_config in self.pm_group_metrics.items():
            if name not in self.lp_callbacks:
                self.lp_callbacks[name] = LoopingCall(callback, group_name=name)

            if self.default_freq > 0 and group_config.group_freq > 0 and \
                not self.lp_callbacks[name].running:
                # Adjust next time if there is a skew
                interval = group_config.group_freq
                if self.max_skew != 0:
                    skew = random.uniform(-interval * (self.max_skew / 100), interval * (self.max_skew / 100))  # nosec
                    interval += skew

                self.lp_callbacks[name].start(interval=interval)

    def stop_collector(self):
        """ Stop the collection loop"""
        for name, _ in self.pm_group_metrics.items():
            if name in self.lp_callbacks and self.lp_callbacks[name].running:
                self.lp_callbacks[name].stop()

    def collect_group_metrics(self, group_name, group, names, config):
        """
        Collect the metrics for a specific PM group.

        This common collection method expects that the 'group object' provide as the second
        parameter supports an attribute or property with the name of the value to
        retrieve.

        :param group_name: (str) The unique collection name. The name should not contain spaces.
        :param group: (object) The object to query for the value of various attributes (PM names)
        :param names: (set) A collection of PM names that, if implemented as a property in the object,
                            will return a value to store in the returned PM dictionary
        :param config: (PMConfig) PM Configuration settings. The enabled flag is examined to determine
                                  if the data associated with a PM Name will be collected.

        :return: (MetricInformation) collected metrics
        """
        if ' ' in group_name:
            raise ValueError('Spaces are not allowed in metric titles, use an underscore')

        if group is None:
            return None

        metrics = {}
        context = {}

        now = getattr(group, AdapterPmMetrics.TIMESTAMP_ATTRIBUTE) \
            if hasattr(group, AdapterPmMetrics.TIMESTAMP_ATTRIBUTE) \
            else time.time()

        if now is None:
            return None     # No metrics available at this time for collection

        for (metric, _t) in names:
            if config[metric].type == PmConfig.CONTEXT and hasattr(group, metric):
                context[metric] = str(getattr(group, metric))

            elif config[metric].type in (PmConfig.COUNTER, PmConfig.GAUGE, PmConfig.STATE) and config[metric].enabled:
                value = getattr(group, metric, None)
                # Do not report KPI metric if not supported by adapter (return of None)
                if value is not None:
                    metrics[metric] = value

        # Check length of metric data. Will be zero if if/when individual group
        # metrics can be disabled and all are (or or not supported by the
        # underlying adapter)
        if len(metrics) == 0:
            return None

        return MetricInformation(metadata=MetricMetaData(title=group_name,
                                                         ts=now,
                                                         logical_device_id=self.logical_device_id,
                                                         serial_no=self.serial_number,
                                                         device_id=self.device_id,
                                                         context=context),
                                 metrics=metrics)

    def collect_metrics(self, group_name=None, data=None):
        """
        Collect metrics for this adapter.

        The adapter type (OLT, ONU, ..) should provide a derived class where this
        method iterates through all metrics and collects them up in a dictionary with
        the group/metric name as the key, and the metric values as the contents.

        The data collected (or passed in) is a list of pairs/tuples.  Each
        pair is composed of a MetricMetaData metadata-portion and list of MetricValuePairs
        that contains a single individual metric or list of metrics if this is a
        group metric.

        This method is called for each adapter at a fixed frequency.
        TODO: Currently all group metrics are collected on a single timer tick.
              This needs to be fixed as independent group or instance collection is
              desirable.

        :param group_name: (str) Individual group name or None for all groups
        :param data: (list) Existing list of collected metrics (MetricInformation).
                            This is provided to allow derived classes to call into
                            further encapsulated classes.

        :return: (list) metadata and metrics pairs - see description above
        """
        raise NotImplementedError('Your derived class should override this method')

    def collect_and_publish_metrics(self, group_name=None):
        """ Request collection of all enabled metrics and publish them """
        try:
            self.log.debug('entry')

            # Collect the data
            data = self.collect_metrics(group_name=group_name)
            raised_ts = time.time_ns()
            self.publish_metrics(data, raised_ts)

        except Exception as e:
            self.log.exception('failed-to-collect-kpis', e=e)

    def publish_metrics(self, data, raised_ts):
        """
        Publish the metrics during a collection.

        The data collected (or passed in) is a list of dictionary pairs/tuple.  Each
        pair is composed of a metadata-portion and a metrics-portion that contains
        information for a specific instance of an individual metric or metric group.

        :param data: (list) Existing list of collected metrics (MetricInformation)
                            to convert to a KPIEvent and publish
        """
        self.log.debug('publish-metrics', data=data)
        event_header = self.event_mgr.get_event_header(EventType.KPI_EVENT2,
                                                       self._category,
                                                       self._sub_category,
                                                       self._event,
                                                       raised_ts)
        if len(data) > 0:
            try:
                event_body = KpiEvent2(type=KpiEventType.slice,
                                       ts=time.time(),
                                       slice_data=data)
                self.event_mgr.send_event(event_header, event_body)

            except Exception as e:
                self.log.exception('failed-to-submit-kpis', e=e)
