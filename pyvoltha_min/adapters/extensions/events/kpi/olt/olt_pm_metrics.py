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

import random

from voltha_protos.device_pb2 import PmConfig, PmConfigs, PmGroupConfig

from pyvoltha_min.adapters.extensions.events.kpi.adapter_pm_metrics import AdapterPmMetrics

MIN_PM_FREQUENCY = 50       # Five Seconds
MIN_PM_SKEW = 0             # 0%
MAX_PM_SKEW = 50            # 50%


class OltPmMetrics(AdapterPmMetrics):
    """
    Shared OL Device Adapter PM Metrics Manager

    This class specifically addresses ONU general PM (health, ...) area
    specific PM (OMCI, PON, UNI) is supported in encapsulated classes accessible
    from this object
    """
    # pylint: disable=too-many-arguments
    def __init__(self, event_mgr, core_proxy, device_id, logical_device_id, serial_number,
                 grouped=False, freq_override=False, support_onu_statistics=False,
                 support_gem_port_statistic=False, **kwargs):
        """
        Initializer for shared ONU Device Adapter PM metrics

        :param core_proxy: (CoreProxy) Gateway between CORE and an adapter
        :param device_id: (str) Device ID
        :param logical_device_id: (str) VOLTHA Logical Device ID
        :param grouped: (bool) Flag indicating if statistics are managed as a group
        :param freq_override: (bool) Flag indicating if frequency collection can be specified
                                     on a per group basis
        :param kwargs: (dict) Device Adapter specific values. For an ONU Device adapter, the
                              expected key-value pairs are listed below. If not provided, the
                              associated PM statistics are not gathered:

                              'nni-ports': List of objects that provide NNI (northbound) port statistics
                              'pon-ports': List of objects that provide PON port statistics
        """
        super().__init__(event_mgr, core_proxy, device_id, logical_device_id, serial_number,
                         grouped=grouped, freq_override=freq_override,
                         **kwargs)

        self.support_onu_stats = support_onu_statistics
        self.support_gem_stats = support_onu_statistics and support_gem_port_statistic

        # PM Config Types are COUNTER, GAUGE, and STATE
        self.nni_pm_names = {
            ('oltid', PmConfig.CONTEXT),       # Physical device interface ID/Port number
            ('devicetype', PmConfig.CONTEXT),
            ('portlabel', PmConfig.CONTEXT),
            ('portno', PmConfig.CONTEXT),
            # ('admin_state', PmConfig.STATE),
            # ('oper_status', PmConfig.STATE),

            ('rx_bytes', PmConfig.COUNTER),
            ('rx_packets', PmConfig.COUNTER),
            ('rx_ucast_packets', PmConfig.COUNTER),
            ('rx_mcast_packets', PmConfig.COUNTER),
            ('rx_bcast_packets', PmConfig.COUNTER),
            ('rx_error_packets', PmConfig.COUNTER),

            ('tx_bytes', PmConfig.COUNTER),
            ('tx_packets', PmConfig.COUNTER),
            ('tx_ucast_packets', PmConfig.COUNTER),
            ('tx_mcast_packets', PmConfig.COUNTER),
            ('tx_bcast_packets', PmConfig.COUNTER),
            ('rx_runt_packets', PmConfig.COUNTER),
            ('rx_huge_packets', PmConfig.COUNTER),
            ('rx_crc_errors', PmConfig.COUNTER),
            ('rx_overflow_errors', PmConfig.COUNTER),
            ('rx_linecode_errors', PmConfig.COUNTER),
            ('tx_error_packets', PmConfig.COUNTER),
            ('bip_errors', PmConfig.COUNTER),

            # Frame buckets
            ('rx_packets_64', PmConfig.COUNTER),
            ('rx_packets_65_127', PmConfig.COUNTER),
            ('rx_packets_128_255', PmConfig.COUNTER),
            ('rx_packets_256_511', PmConfig.COUNTER),
            ('rx_packets_512_1023', PmConfig.COUNTER),
            ('rx_packets_1024_1518', PmConfig.COUNTER),
            ('rx_packets_1519_plus', PmConfig.COUNTER),

            ('tx_packets_64', PmConfig.COUNTER),
            ('tx_packets_65_127', PmConfig.COUNTER),
            ('tx_packets_128_255', PmConfig.COUNTER),
            ('tx_packets_256_511', PmConfig.COUNTER),
            ('tx_packets_512_1023', PmConfig.COUNTER),
            ('tx_packets_1024_1518', PmConfig.COUNTER),
            ('tx_packets_1519_plus', PmConfig.COUNTER),
        }
        self.pon_pm_names = {
            ('oltid', PmConfig.CONTEXT),  # Physical device interface ID/Port number
            ('devicetype', PmConfig.CONTEXT),
            ('portlabel', PmConfig.CONTEXT),
            ('portno', PmConfig.CONTEXT),

            # ('admin_state', PmConfig.STATE),
            # ('oper_status', PmConfig.STATE),
            ('rx_packets', PmConfig.COUNTER),
            ('rx_bytes', PmConfig.COUNTER),
            ('rx_ucast_bytes', PmConfig.COUNTER),
            ('rx_bcast_mcast_bytes', PmConfig.COUNTER),
            ('rx_runt_packets', PmConfig.COUNTER),
            ('rx_huge_packets', PmConfig.COUNTER),
            ('rx_crc_errors', PmConfig.COUNTER),
            ('rx_overflow_errors', PmConfig.COUNTER),

            ('tx_packets', PmConfig.COUNTER),
            ('tx_bytes', PmConfig.COUNTER),
            ('tx_ucast_bytes', PmConfig.COUNTER),
            ('tx_bcast_mcast_bytes', PmConfig.COUNTER),
            ('tx_bip_errors', PmConfig.COUNTER),
            ('in_service_onus', PmConfig.GAUGE),
            ('closest_onu_distance', PmConfig.GAUGE),

            # Frame buckets
            ('rx_packets_64', PmConfig.COUNTER),
            ('rx_packets_65_127', PmConfig.COUNTER),
            ('rx_packets_128_255', PmConfig.COUNTER),
            ('rx_packets_256_511', PmConfig.COUNTER),
            ('rx_packets_512_1023', PmConfig.COUNTER),
            ('rx_packets_1024_1518', PmConfig.COUNTER),
            ('rx_packets_1519_plus', PmConfig.COUNTER),

            ('tx_packets_64', PmConfig.COUNTER),
            ('tx_packets_65_127', PmConfig.COUNTER),
            ('tx_packets_128_255', PmConfig.COUNTER),
            ('tx_packets_256_511', PmConfig.COUNTER),
            ('tx_packets_512_1023', PmConfig.COUNTER),
            ('tx_packets_1024_1518', PmConfig.COUNTER),
            ('tx_packets_1519_plus', PmConfig.COUNTER),
        }
        self.onu_pm_names = {
            # ('intf_id', PmConfig.CONTEXT),        # Physical device port number (PON)
            # ('pon_id', PmConfig.CONTEXT),
            # ('onu_id', PmConfig.CONTEXT),

            ('fiber_length', PmConfig.GAUGE),
            ('equalization_delay', PmConfig.GAUGE),
            ('rssi', PmConfig.GAUGE),
        }
        self.gem_pm_names = {
            # ('intf_id', PmConfig.CONTEXT),        # Physical device port number (PON)
            # ('pon_id', PmConfig.CONTEXT),
            # ('onu_id', PmConfig.CONTEXT),
            # ('gem_id', PmConfig.CONTEXT),

            ('alloc_id', PmConfig.GAUGE),
            ('rx_packets', PmConfig.COUNTER),
            ('rx_bytes', PmConfig.COUNTER),
            ('tx_packets', PmConfig.COUNTER),
            ('tx_bytes', PmConfig.COUNTER),
        }
        self.nni_metrics_config = {m: PmConfig(name=m, type=t, enabled=True)
                                   for (m, t) in self.nni_pm_names}
        self.pon_metrics_config = {m: PmConfig(name=m, type=t, enabled=True)
                                   for (m, t) in self.pon_pm_names}
        if self.support_onu_stats:
            self.onu_metrics_config = {m: PmConfig(name=m, type=t, enabled=True)
                                       for (m, t) in self.onu_pm_names}
        else:
            self.onu_metrics_config = dict()

        if self.support_gem_stats:
            self.gem_metrics_config = {m: PmConfig(name=m, type=t, enabled=True)
                                       for (m, t) in self.gem_pm_names}
        else:
            self.gem_metrics_config = dict()

        self._nni_ports = kwargs.pop('nni-ports', None)
        self._pon_ports = kwargs.pop('pon-ports', None)

    def update(self, pm_config):    # pylint: disable=too-many-branches
        self.log.debug('update-pm-config', pm_config=pm_config)
        try:
            restart = False

            if self.default_freq != pm_config.default_freq:
                if pm_config.default_freq < MIN_PM_FREQUENCY and pm_config.default_freq != 0:
                    self.log.warn('invalid-pm-frequency', value=pm_config.default_freq,
                                  minimum=MIN_PM_FREQUENCY)

                else:
                    # Update the callback to the new frequency.
                    self.default_freq = pm_config.default_freq
                    restart = True

            if self.max_skew != pm_config.max_skew:
                if MIN_PM_SKEW <= pm_config.max_skew <= MAX_PM_SKEW:
                    self.max_skew = pm_config.max_skew
                    restart = True

                else:
                    self.log.warn('invalid-pm-skew', value=pm_config.max_skew,
                                  minimum=MIN_PM_SKEW, maximum=MAX_PM_SKEW)

            if restart and self.lp_callback is not None:
                if self.lp_callback.running:
                    self.lp_callback.stop()

                if self.default_freq > 0:
                    # Adjust next time if there is a skew
                    interval = self.default_freq / 10
                    if self.max_skew != 0:
                        skew = random.uniform(-interval * (self.max_skew / 100),
                                              interval * (self.max_skew / 100))  # nosec
                        interval += skew
                    self.lp_callback.start(interval=interval)

            if pm_config.grouped:
                for group in pm_config.groups:
                    group_config = self.pm_group_metrics.get(group.group_name)
                    if group_config is not None:
                        group_config.enabled = group.enabled
            else:
                msg = 'There are no independent OLT metrics, only group metrics at this time'
                raise NotImplementedError(msg)

        except Exception as e:
            self.log.exception('update-failure', e=e)
            raise

    def make_proto(self, pm_config=None):   # pylint: disable=too-many-branches
        if pm_config is None:
            pm_config = PmConfigs(id=self.device_id, default_freq=self.default_freq,
                                  grouped=self.grouped,
                                  freq_override=self.freq_override,
                                  max_skew=self.max_skew)
        metrics = set()
        have_nni = self._nni_ports is not None and len(self._nni_ports) > 0
        have_pon = self._pon_ports is not None and len(self._pon_ports) > 0

        if self.grouped:
            if have_nni:
                pm_ether_stats = PmGroupConfig(group_name='ETHERNET_NNI',
                                               group_freq=self.default_freq,
                                               enabled=True)
                self.pm_group_metrics[pm_ether_stats.group_name] = pm_ether_stats

            else:
                pm_ether_stats = None

            if have_pon:
                pm_pon_stats = PmGroupConfig(group_name='PON_OLT',
                                             group_freq=self.default_freq,
                                             enabled=True)

                self.pm_group_metrics[pm_pon_stats.group_name] = pm_pon_stats

                if self.support_onu_stats:
                    pm_onu_stats = PmGroupConfig(group_name='ONU',
                                                 group_freq=self.default_freq,
                                                 enabled=True)
                    self.pm_group_metrics[pm_onu_stats.group_name] = pm_onu_stats
                else:
                    pm_onu_stats = None

                if self.support_gem_stats:
                    # pm_gem_stats = PmGroupConfig(group_name='GEM',
                    #                              group_freq=self.default_freq,
                    #                              enabled=True)
                    pm_gem_stats = PmGroupConfig(group_name='GEM',
                                                 group_freq=self.default_freq,
                                                 enabled=False)

                    self.pm_group_metrics[pm_gem_stats.group_name] = pm_gem_stats
                else:
                    pm_gem_stats = None
            else:
                pm_pon_stats = None
                pm_onu_stats = None
                pm_gem_stats = None

        else:
            pm_ether_stats = pm_config if have_nni else None
            pm_pon_stats = pm_config if have_pon else None
            pm_onu_stats = pm_config if have_pon and self.support_onu_stats else None
            pm_gem_stats = pm_config if have_pon and self.support_gem_stats else None

        if have_nni:
            for metric in sorted(self.nni_metrics_config):
                pmetric = self.nni_metrics_config[metric]
                if not self.grouped:
                    if pmetric.name in metrics:
                        continue
                    metrics.add(pmetric.name)
                pm_ether_stats.metrics.extend([PmConfig(name=pmetric.name,
                                                        type=pmetric.type,
                                                        enabled=pmetric.enabled)])
        if have_pon:
            for metric in sorted(self.pon_metrics_config):
                pmetric = self.pon_metrics_config[metric]
                if not self.grouped:
                    if pmetric.name in metrics:
                        continue
                    metrics.add(pmetric.name)
                pm_pon_stats.metrics.extend([PmConfig(name=pmetric.name,
                                                      type=pmetric.type,
                                                      enabled=pmetric.enabled)])

            if self.support_onu_stats:
                for metric in sorted(self.onu_metrics_config):
                    pmetric = self.onu_metrics_config[metric]
                    if not self.grouped:
                        if pmetric.name in metrics:
                            continue
                        metrics.add(pmetric.name)
                    pm_onu_stats.metrics.extend([PmConfig(name=pmetric.name,
                                                          type=pmetric.type,
                                                          enabled=pmetric.enabled)])

            if self.support_gem_stats:
                for metric in sorted(self.gem_metrics_config):
                    pmetric = self.gem_metrics_config[metric]
                    if not self.grouped:
                        if pmetric.name in metrics:
                            continue
                        metrics.add(pmetric.name)
                    pm_gem_stats.metrics.extend([PmConfig(name=pmetric.name,
                                                          type=pmetric.type,
                                                          enabled=pmetric.enabled)])
        if self.grouped:
            pm_config.groups.extend(self.pm_group_metrics.values())

        return pm_config

    def collect_metrics(self, data=None):   # pylint: disable=too-many-branches
        """
        Collect metrics for this adapter.

        The data collected (or passed in) is a list of pairs/tuples.  Each
        pair is composed of a MetricMetaData metadata-portion and list of MetricValuePairs
        that contains a single individual metric or list of metrics if this is a
        group metric.

        This method is called for each adapter at a fixed frequency.
        TODO: Currently all group metrics are collected on a single timer tick.
              This needs to be fixed as independent group or instance collection is
              desirable.

        :param data: (list) Existing list of collected metrics (MetricInformation).
                            This is provided to allow derived classes to call into
                            further encapsulated classes.

        :return: (list) metadata and metrics pairs - see description above
        """
        self.log.debug('entry')
        if data is None:
            data = list()

        group_name = 'ETHERNET_NNI'
        if self.pm_group_metrics[group_name].enabled:
            for port in self._nni_ports:
                group_data = self.collect_group_metrics(group_name,
                                                        port,
                                                        self.nni_pm_names,
                                                        self.nni_metrics_config)
                if group_data is not None:
                    data.append(group_data)

        for port in self._pon_ports:      # pylint: disable=too-many-nested-blocks
            group_name = 'PON_OLT'
            if self.pm_group_metrics[group_name].enabled:
                group_data = self.collect_group_metrics(group_name,
                                                        port,
                                                        self.pon_pm_names,
                                                        self.pon_metrics_config)
                if group_data is not None:
                    data.append(group_data)

            if self.support_onu_stats:
                for onu_id in port.onu_ids:
                    onu = port.onu(onu_id)
                    if onu is not None:
                        group_name = 'ONU'
                        if self.pm_group_metrics[group_name].enabled:
                            group_data = self.collect_group_metrics(group_name,
                                                                    onu,
                                                                    self.onu_pm_names,
                                                                    self.onu_metrics_config)
                            if group_data is not None:
                                data.append(group_data)

                    if self.support_gem_stats:
                        group_name = 'GEM'
                        if self.pm_group_metrics[group_name].enabled:
                            for gem in onu.gem_ports:
                                if not gem.multicast:
                                    group_data = self.collect_group_metrics(group_name,
                                                                            onu,
                                                                            self.gem_pm_names,
                                                                            self.gem_metrics_config)
                                    if group_data is not None:
                                        data.append(group_data)

                            # TODO: Do any multicast GEM PORT metrics here...
        return data
