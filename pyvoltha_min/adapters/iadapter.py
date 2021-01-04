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
Adapter abstract base class
"""

import structlog
from twisted.internet import reactor, task
from twisted.internet.defer import succeed
from voltha_protos.adapter_pb2 import Adapter, AdapterConfig
from voltha_protos.common_pb2 import AdminState
from voltha_protos.device_pb2 import DeviceType, DeviceTypes
from voltha_protos.health_pb2 import HealthStatus
from zope.interface import implementer

from .interface import IAdapterInterface

log = structlog.get_logger()


# pylint: disable=too-many-arguments, too-many-public-methods, abstract-method

@implementer(IAdapterInterface)
class IAdapter:
    def __init__(self,
                 core_proxy,
                 adapter_proxy,
                 config,
                 device_handler_class,
                 name,
                 vendor,
                 version,
                 device_type, vendor_id,
                 accepts_bulk_flow_update=True,
                 accepts_add_remove_flow_updates=False,
                 endpoint=None,
                 current_replica=1,
                 total_replicas=1,
                 adapter_type=None):
        log.debug(
            'Initializing adapter: {} {} {}'.format(vendor, name, version))
        self.core_proxy = core_proxy
        self.adapter_proxy = adapter_proxy
        self.config = config
        self.name = name
        self.supported_device_types = [
            DeviceType(
                id=device_type,
                vendor_id=vendor_id,
                adapter=name,
                accepts_bulk_flow_update=accepts_bulk_flow_update,
                accepts_add_remove_flow_updates=accepts_add_remove_flow_updates
            )
        ]
        self.descriptor = Adapter(
            id=self.name,
            vendor=vendor,
            version=version,
            config=AdapterConfig(),
            currentReplica=current_replica,
            totalReplicas=total_replicas,
            endpoint=endpoint,
            type=adapter_type or name
        )
        self.devices_handlers = dict()  # device_id -> Olt/OnuHandler()
        self.device_handler_class = device_handler_class

    def start(self):
        log.info('Starting adapter: {}'.format(self.name))

    def stop(self):
        log.info('Stopping adapter: {}'.format(self.name))

    def adapter_descriptor(self):
        return self.descriptor

    def device_types(self):
        return DeviceTypes(items=self.supported_device_types)

    def health(self):       # pylint: disable=no-self-use
        # return HealthStatus(state=HealthStatus.HealthState.HEALTHY)
        return HealthStatus(state=HealthStatus.HEALTHY)

    def change_master_state(self, master):
        raise NotImplementedError()

    def get_ofp_device_info(self, device):
        log.debug('get_ofp_device_info_start', device_id=device.id)
        ofp_device_info = self.devices_handlers[device.id].get_ofp_device_info(
            device)
        log.debug('get_ofp_device_info_ends', device_id=device.id)
        return ofp_device_info

    def adopt_device(self, device):
        log.debug('adopt_device', device_id=device.id)
        self.devices_handlers[device.id] = self.device_handler_class(self,
                                                                     device.id)
        reactor.callLater(0, self.devices_handlers[device.id].activate, device)
        log.debug('adopt_device_done', device_id=device.id)
        return device

    def reconcile_device(self, device):
        raise NotImplementedError()

    def abandon_device(self, device):
        raise NotImplementedError()

    def disable_device(self, device):
        log.info('disable-device', device_id=device.id)
        reactor.callLater(0, self.devices_handlers[device.id].disable)
        log.debug('disable-device-done', device_id=device.id)
        return device

    def reenable_device(self, device):
        log.info('reenable-device', device_id=device.id)
        reactor.callLater(0, self.devices_handlers[device.id].reenable)
        log.info('reenable-device-done', device_id=device.id)
        return device

    def reboot_device(self, device):
        log.info('reboot-device', device_id=device.id)
        reactor.callLater(0, self.devices_handlers[device.id].reboot)
        log.info('reboot-device-done', device_id=device.id)
        return device

    def download_image(self, device, request):
        raise NotImplementedError()

    def get_image_download_status(self, device, request):
        raise NotImplementedError()

    def cancel_image_download(self, device, request):
        raise NotImplementedError()

    def activate_image_update(self, device, request):
        raise NotImplementedError()

    def revert_image_update(self, device, request):
        raise NotImplementedError()

    def enable_port(self, device_id, port_no):
        raise NotImplementedError()

    def disable_port(self, device_id, port_no):
        raise NotImplementedError()

    def child_device_lost(self, device_id, parent_port_no, onu_id):
        raise NotImplementedError()

    def self_test_device(self, device):
        log.info('self-test', device_id=device.id)
        result = reactor.callLater(0, self.devices_handlers[
            device.id].self_test_device)
        log.info('self-test-done', device_id=device.id)
        return result

    def delete_device(self, device):
        log.info('delete-device', device_id=device.id)
        reactor.callLater(0, self.devices_handlers[device.id].delete)
        log.info('delete-device-done', device_id=device.id)
        return device

    def get_device_details(self, device):
        raise NotImplementedError()

    def update_flows_bulk(self, device, flows, groups):
        log.debug('bulk-flow-update', device_id=device.id,
                 flows=flows, groups=groups)
        reactor.callLater(0, self.devices_handlers[device.id].update_flow_table,
                          flows.items)
        return device

    def update_flows_incrementally(self, device, flow_changes, group_changes, flow_metadata):
        log.debug('incremental-flow-update', device_id=device.id,
                  flows=flow_changes, groups=group_changes, metadata=flow_metadata)

        handler = self.devices_handlers[device.id]
        # Remove flows
        if len(flow_changes.to_remove.items) != 0:
            reactor.callLater(0, handler.remove_from_flow_table,
                              flow_changes.to_remove.items)

        # Add flows
        if len(flow_changes.to_add.items) != 0:
            reactor.callLater(0, handler.add_to_flow_table,
                              flow_changes.to_add.items, flow_metadata)
        return device

    def get_ext_value(self, device_id, device, value_flag):
        log.debug("adapter-get-ext-value", device=device, device_id=device_id)

        if device_id not in self.devices_handlers:
            return succeed(None)

        handler = self.devices_handlers[device.id]

        def success(results):
            log.info('get-ext-value-success', results=results)
            return results

        def failure(reason):
            log.warn('get-ext-value-failure', results=reason)
            return reason

        d = task.deferLater(reactor, 0, handler.get_ext_value(device, value_flag))
        return d.addCallbacks(success, failure)

    def update_pm_config(self, device, pm_config):
        log.debug("adapter-update-pm-config", device=device, pm_config=pm_config)
        handler = self.devices_handlers[device.id]
        if handler:
            reactor.callLater(0, handler.update_pm_config, device, pm_config)

    def process_inter_adapter_message(self, msg, from_topic=None):
        raise NotImplementedError()

    def receive_packet_out(self, device_id, egress_port_no, msg):
        raise NotImplementedError()

    def suppress_event(self, filter):
        raise NotImplementedError()

    def unsuppress_event(self, filter):
        raise NotImplementedError()

    def single_get_value_request(self, request):
        raise NotImplementedError()

    def single_set_value_request(self, request):
        raise NotImplementedError()

    def _get_handler(self, device):
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                return handler
            return None
        return None


class OltAdapter(IAdapter):
    """
    OLT Adapter base class
    """
    def __init__(self,
                 core_proxy,
                 adapter_proxy,
                 config,
                 device_handler_class,
                 name,
                 vendor,
                 version,
                 device_type,
                 accepts_bulk_flow_update=True,
                 accepts_add_remove_flow_updates=False,
                 endpoint=None,
                 current_replica=1,
                 total_replicas=1,
                 adapter_type=None):

        super().__init__(core_proxy=core_proxy,
                         adapter_proxy=adapter_proxy,
                         config=config,
                         device_handler_class=device_handler_class,
                         name=name,
                         vendor=vendor,
                         version=version,
                         device_type=device_type,
                         vendor_id=None,
                         accepts_bulk_flow_update=accepts_bulk_flow_update,
                         accepts_add_remove_flow_updates=accepts_add_remove_flow_updates,
                         endpoint=endpoint,
                         current_replica=current_replica,
                         total_replicas=total_replicas,
                         adapter_type=adapter_type)
        self.logical_device_id_to_root_device_id = dict()

    def reconcile_device(self, device):
        try:
            self.devices_handlers[device.id] = self.device_handler_class(self,
                                                                         device.id)
            # Work only required for devices that are in ENABLED state
            if device.admin_state == AdminState.ENABLED:
                reactor.callLater(0,
                                  self.devices_handlers[device.id].reconcile,
                                  device)
            else:
                # Invoke the children reconciliation which would setup the
                # basic children data structures
                self.core_proxy.reconcile_child_devices(device.id)
            return device
        except Exception as e:
            log.exception('Exception', e=e)

    def send_proxied_message(self, proxy_address, msg):
        log.debug('send-proxied-message', proxy_address=proxy_address, msg=msg)
        handler = self.devices_handlers[proxy_address.device_id]
        handler.send_proxied_message(proxy_address, msg)

    def process_inter_adapter_message(self, msg, from_topic=None):
        log.debug('process-inter-adapter-message', msg=msg, from_topic=from_topic)
        # Unpack the header to know which device needs to handle this message
        handler = None
        if msg.header.proxy_device_id:
            # typical request
            handler = self.devices_handlers[msg.header.proxy_device_id]
        elif msg.header.to_device_id and \
                msg.header.to_device_id in self.devices_handlers:
            # typical response
            handler = self.devices_handlers[msg.header.to_device_id]
        if handler:
            reactor.callLater(0, handler.process_inter_adapter_message, msg, from_topic=from_topic)

    def receive_packet_out(self, device_id, egress_port_no, msg):
        try:
            log.debug('receive_packet_out', device_id=device_id,
                      egress_port=egress_port_no, msg=msg)
            handler = self.devices_handlers[device_id]
            if handler:
                reactor.callLater(0, handler.packet_out, egress_port_no, msg.data)
        except Exception as e:
            log.exception('packet-out-failure', e=e)
