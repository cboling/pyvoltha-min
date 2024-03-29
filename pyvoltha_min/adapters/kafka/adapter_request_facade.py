#
# Copyright 2017 the original author or authors.
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
This facade handles kafka-formatted messages from the Core, extracts the kafka
formatting and forwards the request to the concrete handler.
"""
import structlog
from twisted.internet.defer import TimeoutError as TwistedTimeoutError
from twisted.internet.defer import inlineCallbacks, returnValue
from voltha_protos.device_pb2 import Device, Port, ImageDownload, SimulateAlarmRequest, PmConfigs
from voltha_protos.extensions_pb2 import SingleGetValueRequest, SingleGetValueResponse, GetValueResponse
from voltha_protos.inter_container_pb2 import IntType, InterAdapterMessage, StrType, Error, ErrorCode
from voltha_protos.openflow_13_pb2 import FlowChanges, FlowGroups, Flows, \
    FlowGroupChanges, ofp_packet_out
from voltha_protos.voltha_pb2 import OmciTestRequest, FlowMetadata, ValueSpecifier
from zope.interface import implementer

from .kafka_inter_container_library import get_messaging_proxy, ARG_FROM_TOPIC
from ..interface import IAdapterInterface

log = structlog.get_logger()


class MacAddressError(BaseException):
    def __init__(self, error):
        super().__init__()
        self.error = error


class IDError(BaseException):
    def __init__(self, error):
        super().__init__()
        self.error = error


@implementer(IAdapterInterface)
class AdapterRequestFacade:             # pylint: disable=too-many-public-methods
    """
    Gate-keeper between CORE and device adapters.

    On one side it interacts with Core's internal model and update/dispatch
    mechanisms.

    On the other side, it interacts with the adapters standard interface as
    defined in
    """

    def __init__(self, adapter, core_proxy):
        self.adapter = adapter
        self.core_proxy = core_proxy

    @inlineCallbacks
    def start(self):
        log.debug('starting', adapter=str(self.adapter))

    @inlineCallbacks
    def stop(self):
        log.debug('stopping', adapter=str(self.adapter))

    def start_omci_test(self, device, omcitestrequest, **_kwargs):
        if not device:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        if not omcitestrequest:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="omcitestrequest-invalid")

        d = Device()
        device.Unpack(d)
        omci_test = OmciTestRequest()
        omcitestrequest.Unpack(omci_test)
        result = self.adapter.start_omci_test(d, omci_test.uuid)
        return True, result

    def adopt_device(self, device, **kwargs):
        d = Device()
        if device:
            device.Unpack(d)

            # Update the core reference for that device as it will be used
            # by the adapter to send async messages to the Core.
            if ARG_FROM_TOPIC in kwargs:
                str_arg = StrType()
                kwargs[ARG_FROM_TOPIC].Unpack(str_arg)
                # Update the core reference for that device
                self.core_proxy.update_device_core_reference(d.id, str_arg.val)

            # # Start the creation of a device specific topic to handle all
            # # subsequent requests from the Core. This adapter instance will
            # # handle all requests for that device.
            # reactor.callLater(0, self.createKafkaDeviceTopic, d.id)

            result = self.adapter.adopt_device(d)
            # return True, self.adapter.adopt_device(d)

            return True, result

        return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                            reason="device-invalid")

    def get_ofp_device_info(self, device, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
            return True, self.adapter.get_ofp_device_info(d)

        return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                            reason="device-invalid")

    def reconcile_device(self, device, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
            return True, self.adapter.reconcile_device(d)

        return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                            reason="device-invalid")

    def abandon_device(self, device, **_kwargs):
        return self.adapter.abandon_device(device)

    def disable_device(self, device, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
            return True, self.adapter.disable_device(d)

        return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                            reason="device-invalid")

    def reenable_device(self, device, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
            return True, self.adapter.reenable_device(d)

        return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                            reason="device-invalid")

    def reboot_device(self, device, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
            return True, self.adapter.reboot_device(d)

        return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                            reason="device-invalid")

    def update_pm_config(self, device, pm_configs, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        pm_config = PmConfigs()
        if pm_configs:
            pm_configs.Unpack(pm_config)

        return True, self.adapter.update_pm_config(d, pm_config)

    def download_image(self, device, request, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        img = ImageDownload()
        if request:
            request.Unpack(img)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="port-no-invalid")

        return True, self.adapter.download_image(device, request)

    def get_image_download_status(self, device, request, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        img = ImageDownload()
        if request:
            request.Unpack(img)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="port-no-invalid")

        return True, self.adapter.get_image_download_status(device, request)

    def cancel_image_download(self, device, request, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        img = ImageDownload()
        if request:
            request.Unpack(img)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="port-no-invalid")

        return True, self.adapter.cancel_image_download(device, request)

    def activate_image_update(self, device, request, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        img = ImageDownload()
        if request:
            request.Unpack(img)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="port-no-invalid")

        return True, self.adapter.activate_image_update(device, request)

    def revert_image_update(self, device, request, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        img = ImageDownload()
        if request:
            request.Unpack(img)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="port-no-invalid")

        return True, self.adapter.revert_image_update(device, request)

    def enable_port(self, deviceId, port, **_kwargs):       # pylint: disable=invalid-name
        if not deviceId:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="device-id")
        if not port:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="port-invalid")
        str_arg = StrType()
        deviceId.Unpack(str_arg)
        port_arg = Port()
        port.Unpack(port_arg)

        return True, self.adapter.enable_port(str_arg.val, port_arg.port_no)

    def disable_port(self, deviceId, port, **_kwargs):       # pylint: disable=invalid-name
        if not deviceId:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="device-id")
        if not port:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="port-invalid")
        str_arg = StrType()
        deviceId.Unpack(str_arg)
        port_arg = Port()
        port.Unpack(port_arg)

        return True, self.adapter.disable_port(str_arg.val, port_arg.port_no)

    def child_device_lost(self, pDeviceId, pPortNo, onuID, **_kwargs):       # pylint: disable=invalid-name
        if not pDeviceId:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="device-id")
        if not pPortNo:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="p-port-no")
        if not onuID:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="onu-id")
        str_arg = StrType()
        pDeviceId.Unpack(str_arg)

        port_no = IntType()
        pPortNo.Unpack(port_no)

        oid = IntType()
        onuID.Unpack(oid)

        return True, self.adapter.child_device_lost(str_arg.val, port_no.val, oid.val)

    def self_test(self, device, **_kwargs):
        return self.adapter.self_test_device(device)

    def delete_device(self, device, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
            result = self.adapter.delete_device(d)
            # return (True, self.adapter.delete_device(d))

            # Before we return, delete the device specific topic as we will no
            # longer receive requests from the Core for that device
            kafka_proxy = get_messaging_proxy()
            device_topic = kafka_proxy.get_default_topic() + "/" + d.id
            kafka_proxy.unsubscribe(topic=device_topic)

            return True, result

        return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                            reason="device-invalid")

    def get_device_details(self, device, **_kwargs):
        return self.adapter.get_device_details(device)

    def update_flows_bulk(self, device, flows, groups, **_kwargs):
        d = Device()
        if device:
            device.Unpack(d)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        flow_info = Flows()
        if flows:
            flows.Unpack(flow_info)

        group_info = FlowGroups()
        if groups:
            groups.Unpack(group_info)

        return True, self.adapter.update_flows_bulk(d, flow_info, group_info)

    def update_flows_incrementally(self, device, flow_changes, group_changes, flow_metadata, **_kwargs):
        dev = Device()
        if device:
            device.Unpack(dev)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        flows = FlowChanges()
        if flow_changes:
            flow_changes.Unpack(flows)

        groups = FlowGroupChanges()
        if group_changes:
            group_changes.Unpack(groups)

        metadata = FlowMetadata()
        if flow_metadata:
            flow_metadata.Unpack(metadata)

        return True, self.adapter.update_flows_incrementally(dev, flows, groups, metadata)

    def suppress_event(self, event_filter, **_kwargs):
        return self.adapter.suppress_event(event_filter)

    def unsuppress_event(self, event_filter, **_kwargs):
        return self.adapter.unsuppress_event(event_filter)

    def process_inter_adapter_message(self, msg, **kwargs):
        ia_msg = InterAdapterMessage()
        if msg:
            msg.Unpack(ia_msg)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="msg-invalid")
        from_topic = StrType()
        topic = kwargs.get('fromTopic')
        if topic:
            topic.Unpack(from_topic)

        log.debug('rx-message', message=ia_msg, from_topic=from_topic)
        return True, self.adapter.process_inter_adapter_message(ia_msg, from_topic=from_topic.val)

    def process_tech_profile_instance_request(self, msg, **kwargs):
        ia_msg = InterAdapterMessage()
        if msg:
            msg.Unpack(ia_msg)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="msg-invalid")
        from_topic = StrType()
        topic = kwargs.get('fromTopic')
        if topic:
            topic.Unpack(from_topic)

        log.debug('rx-tp-message', message=ia_msg, from_topic=from_topic)
        return True, self.adapter.process_tech_profile_instance_request(ia_msg, from_topic=from_topic.val)

    def receive_packet_out(self, deviceId, outPort, packet, **_kwargs):       # pylint: disable=invalid-name
        try:
            d_id = StrType()
            if deviceId:
                deviceId.Unpack(d_id)
            else:
                return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                    reason="device-id-invalid")
            port_number = IntType()
            if outPort:
                outPort.Unpack(port_number)
            else:
                return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                    reason="outport-invalid")

            pkt = ofp_packet_out()
            if packet:
                packet.Unpack(pkt)
            else:
                return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                    reason="packet-invalid")

            return True, self.adapter.receive_packet_out(d_id.val, port_number.val, pkt)

        except Exception as e:
            log.exception("error-processing-receive_packet_out", e=e)
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason=f"exception-during-processing: {e}")

    def simulate_alarm(self, device, request, **_kwargs):
        dev = Device()
        if device:
            device.Unpack(dev)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        req = SimulateAlarmRequest()
        if request:
            request.Unpack(req)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="simulate-alarm-request-invalid")

        return True, self.adapter.simulate_alarm(dev, req)

    def get_ext_value(self, deviceId, device, value_flag, **_kwargs):       # pylint: disable=invalid-name
        if deviceId:
            str_arg = StrType()
            deviceId.Unpack(str_arg)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETER,
                                reason="device-id")
        if device:
            dev = Device()
            device.Unpack(dev)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="device-invalid")
        if value_flag:
            value = ValueSpecifier()
            value_flag.Unpack(value)
        else:
            return False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                reason="value-flag-invalid")

        return True, self.adapter.get_ext_value(str_arg.val, dev, value)

    @inlineCallbacks
    def single_get_value_request(self, request, **_kwargs):
        req = SingleGetValueRequest()
        if request:
            request.Unpack(req)
            try:
                result = self.adapter.single_get_value_request(req)
                response = yield result

            except NotImplementedError:
                resp = GetValueResponse(status=GetValueResponse.Status.ERROR,
                                        errReason=GetValueResponse.ErrorReason.UNSUPPORTED)
                response = (True, SingleGetValueResponse(response=resp))

            except TwistedTimeoutError:
                resp = GetValueResponse(status=GetValueResponse.Status.ERROR,
                                        errReason=GetValueResponse.ErrorReason.TIMEOUT)
                response = (True, SingleGetValueResponse(response=resp))

            except Exception as e:
                log.exception('failed', e=e, request=request)
                resp = GetValueResponse(status=GetValueResponse.Status.ERROR,
                                        errReason=GetValueResponse.ErrorReason.REASON_UNDEFINED)
                response = (True, SingleGetValueResponse(response=resp))
        else:
            response = (False, Error(code=ErrorCode.INVALID_PARAMETERS,
                                     reason="request-invalid"))
        returnValue(response)
