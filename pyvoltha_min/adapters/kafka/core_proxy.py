#
# Copyright 2018 the original author or authors.
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
"""
Agent to play gateway between CORE and an adapter.
"""
import structlog
from google.protobuf.message import Message
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.defer import TimeoutError as TwistedTimeoutErrorDefer
from twisted.internet.error import TimeoutError as TwistedTimeoutError
from voltha_protos.common_pb2 import ID, ConnectStatus, OperStatus
from voltha_protos.device_pb2 import Device, Ports, Devices
from voltha_protos.inter_container_pb2 import StrType, BoolType, IntType, Packet
from voltha_protos.voltha_pb2 import CoreInstance

from .container_proxy import ContainerProxy

log = structlog.get_logger()
DEFAULT_CORE_CONTAINER_TIMEOUT = 60


class CoreProxy(ContainerProxy):  # pylint: disable=too-many-public-methods

    def __init__(self, kafka_proxy, default_core_topic, default_event_topic, my_listening_topic,
                 default_timeout=DEFAULT_CORE_CONTAINER_TIMEOUT):
        super().__init__(kafka_proxy, default_core_topic, my_listening_topic, default_timeout=default_timeout)
        self.core_default_topic = default_core_topic
        self.event_default_topic = default_event_topic
        self.device_id_to_core_map = {}

    def update_device_core_reference(self, device_id, core_topic):
        log.debug("update_device_core_reference")
        self.device_id_to_core_map[device_id] = core_topic

    def delete_device_core_reference(self, device_id, _core_topic):
        log.debug("delete_device_core_reference")
        del self.device_id_to_core_map[device_id]

    def get_adapter_topic(self, **_kwargs):
        return self.listening_topic

    def get_core_topic(self, device_id):
        if device_id in self.device_id_to_core_map:
            return self.device_id_to_core_map[device_id]
        return self.core_default_topic

    @ContainerProxy.wrap_request(CoreInstance)
    @inlineCallbacks
    def register(self, adapter, device_types):
        log.debug("register")

        if adapter.totalReplicas == 0 and adapter.currentReplica != 0:
            raise Exception("totalReplicas can't be 0, since you're here you have at least one")

        if adapter.currentReplica == 0 and adapter.totalReplicas != 0:
            raise Exception("currentReplica can't be 0, it has to start from 1")

        if adapter.currentReplica == 0 and adapter.totalReplicas == 0:
            # if the adapter is not setting these fields they default to 0,
            # in that case it means the adapter is not ready to be scaled
            # and thus it defaults to a single instance
            adapter.currentReplica = 1
            adapter.totalReplicas = 1

        if adapter.currentReplica > adapter.totalReplicas:
            raise Exception("currentReplica (%d) can't be greater than totalReplicas (%d)"
                            % (adapter.currentReplica, adapter.totalReplicas))

        try:
            res = yield self.invoke(rpc="Register",
                                    adapter=adapter,
                                    deviceTypes=device_types,
                                    timeout=5, retries=2)
            log.info("registration-returned", res=res)
            returnValue(res)

        except (TwistedTimeoutError, TwistedTimeoutErrorDefer):
            log.info('register-timeout', adapter=adapter, deviceTypes=device_types)
            raise   # Let caller get timeout so it can decide its own retransmission pattern

        except Exception as e:
            log.exception("registration-exception", e=e)
            raise

    @ContainerProxy.wrap_request(Device)
    @inlineCallbacks
    def get_device(self, device_id):
        log.debug("get-device")
        dev_id = ID()
        dev_id.id = device_id
        # Once we have a device being managed, all communications between the
        # the adapter and the core occurs over a topic associated with that
        # device
        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="GetDevice",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id)
        returnValue(res)

    @ContainerProxy.wrap_request(Device)
    @inlineCallbacks
    def get_child_device(self, parent_device_id, **kwargs):
        log.debug("get-child-device")
        dev_id = ID()
        dev_id.id = parent_device_id
        to_topic = self.get_core_topic(parent_device_id)
        reply_topic = self.get_adapter_topic()
        args = self._to_proto(**kwargs)
        res = yield self.invoke(rpc="GetChildDevice",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id,
                                **args)
        returnValue(res)

    @ContainerProxy.wrap_request(Ports)
    @inlineCallbacks
    def get_ports(self, device_id, port_type):
        dev_id = ID()
        dev_id.id = device_id
        p_type = IntType()
        p_type.val = port_type
        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="GetPorts",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id,
                                port_type=p_type)
        returnValue(res)

    @ContainerProxy.wrap_request(Devices)
    @inlineCallbacks
    def get_child_devices(self, parent_device_id):
        log.debug("get-child-devices")
        dev_id = ID()
        dev_id.id = parent_device_id
        to_topic = self.get_core_topic(parent_device_id)
        reply_topic = self.get_adapter_topic()
        res = yield self.invoke(rpc="GetChildDevices",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id)
        returnValue(res)

    @ContainerProxy.wrap_request(Device)
    @inlineCallbacks
    def get_child_device_with_proxy_address(self, proxy_address):
        log.debug("get-child-device-with-proxy-address")
        dev_id = ID()
        dev_id.id = proxy_address.device_id
        to_topic = self.get_core_topic(proxy_address.device_id)
        reply_topic = self.get_adapter_topic()
        res = yield self.invoke(rpc="GetChildDeviceWithProxyAddress",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                proxy_address=proxy_address)
        returnValue(res)

    @staticmethod
    def _to_proto(**kwargs):
        encoded = {}
        for k, val in kwargs.items():
            if isinstance(val, Message):
                encoded[k] = val
            elif isinstance(val, int):
                i_proto = IntType()
                i_proto.val = val
                encoded[k] = i_proto
            elif isinstance(val, str):
                s_proto = StrType()
                s_proto.val = val
                encoded[k] = s_proto
            elif isinstance(val, bool):
                b_proto = BoolType()
                b_proto.val = val
                encoded[k] = b_proto
            else:
                raise TypeError(f'Unsupported type: {type(val)} for key {k}')
        return encoded

    @ContainerProxy.wrap_request(Device)
    @inlineCallbacks
    def child_device_detected(self,
                              parent_device_id,
                              parent_port_no,
                              child_device_type,
                              channel_id,
                              **kw):
        dev_id = ID()
        dev_id.id = parent_device_id
        ppn = IntType()
        ppn.val = parent_port_no
        cdt = StrType()
        cdt.val = child_device_type
        channel = IntType()
        channel.val = channel_id
        to_topic = self.get_core_topic(parent_device_id)
        reply_topic = self.get_adapter_topic()

        args = self._to_proto(**kw)
        res = yield self.invoke(rpc="ChildDeviceDetected",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                parent_device_id=dev_id,
                                parent_port_no=ppn,
                                child_device_type=cdt,
                                channel_id=channel,
                                **args)
        returnValue(res)

    # @ContainerProxy.wrap_request(Devices)
    # @inlineCallbacks
    # def child_device_lost(self, parent_device_id):
    #     log.debug("child-device-lost")
    #     id = ID()
    #     id.id = parent_device_id
    #     to_topic = self.get_core_topic(parent_device_id)
    #     reply_topic = self.get_adapter_topic()
    #     res = yield self.invoke(rpc="ChildDevicesLost",
    #                             to_topic=to_topic,
    #                             reply_topic=reply_topic,
    #                             device_id=id)
    #     returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def device_update(self, device):
        log.debug("device_update")
        to_topic = self.get_core_topic(device.id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="DeviceUpdate",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device=device)
        returnValue(res)

    def child_device_removed(self, parent_device_id, child_device_id):
        raise NotImplementedError()

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def device_state_update(self, device_id,
                            oper_status=None,
                            connect_status=None):
        dev_id = ID()
        dev_id.id = device_id
        o_status = IntType()
        if oper_status or oper_status == OperStatus.UNKNOWN:
            o_status.val = oper_status
        else:
            o_status.val = -1
        c_status = IntType()
        if connect_status or connect_status == ConnectStatus.UNKNOWN:
            c_status.val = connect_status
        else:
            c_status.val = -1

        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="DeviceStateUpdate",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id,
                                oper_status=o_status,
                                connect_status=c_status)
        returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def children_state_update(self, device_id,
                              oper_status=None,
                              connect_status=None):
        dev_id = ID()
        dev_id.id = device_id
        o_status = IntType()
        if oper_status or oper_status == OperStatus.UNKNOWN:
            o_status.val = oper_status
        else:
            o_status.val = -1
        c_status = IntType()
        if connect_status or connect_status == ConnectStatus.UNKNOWN:
            c_status.val = connect_status
        else:
            c_status.val = -1

        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="ChildrenStateUpdate",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id,
                                oper_status=o_status,
                                connect_status=c_status)
        returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def reconcile_child_devices(self, parent_device_id):

        log.warn("todo", reason='This has not been debugged yet')
        dev_id = ID()
        dev_id.id = parent_device_id

        to_topic = self.get_core_topic(parent_device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="ReconcileChildDevices",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id)
        returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def port_state_update(self,
                          device_id,
                          port_type,
                          port_no,
                          oper_status):
        dev_id = ID()
        dev_id.id = device_id
        ptype = IntType()
        ptype.val = port_type
        pnumber = IntType()
        pnumber.val = port_no
        o_status = IntType()
        o_status.val = oper_status

        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="PortStateUpdate",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id,
                                port_type=ptype,
                                port_no=pnumber,
                                oper_status=o_status)
        returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def child_devices_state_update(self, parent_device_id,
                                   oper_status=None,
                                   connect_status=None):

        dev_id = ID()
        dev_id.id = parent_device_id
        o_status = IntType()
        if oper_status or oper_status == OperStatus.UNKNOWN:
            o_status.val = oper_status
        else:
            o_status.val = -1
        c_status = IntType()
        if connect_status or connect_status == ConnectStatus.UNKNOWN:
            c_status.val = connect_status
        else:
            c_status.val = -1

        to_topic = self.get_core_topic(parent_device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="child_devices_state_update",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                parent_device_id=dev_id,
                                oper_status=o_status,
                                connect_status=c_status)
        returnValue(res)

    def child_devices_removed(self, parent_device_id):
        raise NotImplementedError()

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def device_pm_config_update(self, device_pm_config, init=False):   # TODO: Core does not have the init parameter
        log.debug("device_pm_config_update")
        is_init = BoolType()
        is_init.val = init
        to_topic = self.get_core_topic(device_pm_config.id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="DevicePMConfigUpdate",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_pm_config=device_pm_config,
                                init=is_init)
        returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def port_created(self, device_id, port):
        log.debug("port_created")
        proto_id = ID()
        proto_id.id = device_id
        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="PortCreated",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=proto_id,
                                port=port)
        returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def ports_state_update(self,                # TODO: Golang rw-core hdoes not have filter
                           device_id,
                           port_type_filter,
                           oper_status):
        log.debug("ports_state_update", device_id=device_id, oper_status=oper_status)
        dev_id = ID()
        dev_id.id = device_id
        t_filter = IntType()
        t_filter.val = port_type_filter
        o_status = IntType()
        o_status.val = oper_status

        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="PortsStateUpdate",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id,
                                port_type_filter=t_filter,
                                oper_status=o_status)
        log.debug("ports_state_update_response", device_id=device_id, port_type_filter=port_type_filter, oper_status=oper_status, response=res)
        returnValue(res)

    def port_removed(self, device_id, port):
        raise NotImplementedError()

    def ports_enabled(self, device_id):
        raise NotImplementedError()

    def ports_disabled(self, device_id):
        raise NotImplementedError()

    def ports_oper_status_update(self, device_id, oper_status):
        raise NotImplementedError()

    def image_download_update(self, img_dnld):
        raise NotImplementedError()

    def image_download_deleted(self, img_dnld):
        raise NotImplementedError()

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def send_packet_in(self, device_id, port, packet, response_required=True):
        log.debug("send_packet_in", device_id=device_id)
        proto_id = ID()
        proto_id.id = device_id
        port_num = IntType()
        port_num.val = port
        pac = Packet()
        pac.payload = packet
        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()
        res = yield self.invoke(rpc="PacketIn",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=proto_id,
                                port=port_num,
                                packet=pac,
                                response_required=response_required)
        returnValue(res)

    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def device_reason_update(self, device_id, reason):
        dev_id = ID()
        dev_id.id = device_id
        rsn = StrType()
        rsn.val = reason
        to_topic = self.get_core_topic(device_id)
        reply_topic = self.get_adapter_topic()

        res = yield self.invoke(rpc="DeviceReasonUpdate",
                                to_topic=to_topic,
                                reply_topic=reply_topic,
                                device_id=dev_id,
                                device_reason=rsn)
        returnValue(res)

    # ~~~~~~~~~~~~~~~~~~~ Handle event submissions ~~~~~~~~~~~~~~~~~~~~~

    def filter_alarm(self, _device_id, _alarm_event):   # pylint: disable=no-self-use
        '''
        TODO
        alarm filtering functionality is not implemented
        in Voltha 1.x
        '''
        log.warn('filter_alarm is not implemented')
        #return
        #alarm_filters = self.root_proxy.get('/alarm_filters')

        # rule_values = {
        #     'id': alarm_event.id,
        #     'type': AlarmEventType.AlarmEventType.Name(alarm_event.type),
        #     'category': AlarmEventCategory.AlarmEventCategory.Name(
        #         alarm_event.category),
        #     'severity': AlarmEventSeverity.AlarmEventSeverity.Name(
        #         alarm_event.severity),
        #     'resource_id': alarm_event.resource_id,
        #     'device_id': device_id
        # }
        #
        # for alarm_filter in alarm_filters:
        #     if alarm_filter.rules:
        #         exclude = True
        #         for rule in alarm_filter.rules:
        #             log.debug("compare-alarm-event",
        #                       key=EventFilterRuleKey.EventFilterRuleKey.Name(
        #                           rule.key),
        #                       actual=rule_values[
        #                           EventFilterRuleKey.EventFilterRuleKey.Name(
        #                               rule.key)].lower(),
        #                       expected=rule.value.lower())
        #             exclude = exclude and \
        #                       (rule_values[
        #                           EventFilterRuleKey.EventFilterRuleKey.Name(
        #                               rule.key)].lower() == rule.value.lower())
        #             if not exclude:
        #                 break
        #
        #         if exclude:
        #             log.info("filtered-alarm-event", alarm=alarm_event)
        #             return True
        #
        # return False

    @inlineCallbacks
    def submit_event(self, event_msg):
        try:
            res = yield self.kafka_proxy.send_kafka_message(self.event_default_topic, event_msg, None)
            returnValue(res)

        except Exception as e:
            log.exception('failed-event-submission', type=type(event_msg), e=e)
