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
import copy
import structlog

from twisted.internet.defer import inlineCallbacks, returnValue

from voltha_protos.adapter_pb2 import Adapter
from voltha_protos.device_pb2 import DeviceType, Device

DEFAULT_PARTITION_COUNT = 1117
DEFAULT_REPLICATION_FACTOR = 117
DEFAULT_LOAD = 1.1

log = structlog.get_logger()


class EndpointManager:
    def __init__(self, kv_backend,
                 partition_count=DEFAULT_PARTITION_COUNT,
                 replication_factor=DEFAULT_REPLICATION_FACTOR,
                 load=DEFAULT_LOAD):
        self._partition_count = partition_count        # int
        self._replication_factor = replication_factor  # int
        self._load = load                              # float64
        self._backend = kv_backend                     # *db.Backend
        self._services = dict()                        # map[string]*service
        # Device type below is Type as reported by 'voltctl adapter list'
        self._device_type_service_map = dict()         # Device Type (str) -> map[string]string

    @inlineCallbacks
    def get_endpoint(self, device_id, service_type):
        """
        GetEndpoint is called to get the endpoint to communicate with for a specific
        device and service type.  For now this will return the topic name
        :param device_id:  (str)    Device ID
        :param service_type: (str)  Service Type
        :return: (str) Endpoint
        """
        log.debug('entry', device_id=device_id, service=service_type)

        member = yield self._get_owner(device_id, service_type)
        if member is None:
            log.info('owner-not-found', device_id=device_id, service=service_type)
            returnValue('')

        if member.endpoint is None or len(member.endpoint) == 0:
            log.info('endpoint-is-not-set', device_id=device_id, service=service_type)
            returnValue('')

        returnValue(member.endpoint)

    @inlineCallbacks
    def get_all_service_endpoints(self, service_type):
        """ Get a dict() of replica IDs -> endpoints """
        log.debug('entry', service=service_type)

        service, _device_type = yield self._get_service_and_device_type(service_type)
        if service is None:
            log.info('service-not-found', service=service_type)
            returnValue(None)

        returnValue(service.endpoints.copy())

    @inlineCallbacks
    def is_device_owned_by_service(self, device_id, service_type, replica_number):
        log.debug('entry', device_id=device_id, service=service_type, replica_number=replica_number)

        member = yield self._get_owner(device_id, service_type)
        if member is None:
            log.info('owner-not-found', device_id=device_id, service=service_type)
            returnValue(False)

        returnValue(member.replica == replica_number)

    @inlineCallbacks
    def get_replica_assignment(self, device_id, service_type):
        log.debug('entry', device_id=device_id, service=service_type)

        member = yield self._get_owner(device_id, service_type)
        if member is None:
            log.info('owner-not-found', device_id=device_id, service=service_type)
            returnValue(0)

        returnValue(member.replica)

    @inlineCallbacks
    def _get_owner(self, device_id, service_type):
        log.debug('entry', device_id=device_id, service=service_type)

        service, device_type = yield self._get_service_and_device_type(service_type)
        if service is None:
            log.info('service-not-found', device_id=device_id, service=service_type)
            returnValue(None)

        key = self._make_key(device_id, device_type, service_type)
        returnValue(service.get_owner(key))

    @staticmethod
    def _make_key(device_id, device_type, service_type):
        return '{}_{}_{}'.format(service_type, device_type, device_id).encode('utf-8')

    @inlineCallbacks
    def _get_service_and_device_type(self, service_type):
        log.debug('entry', service=service_type)
        service = self._services.get(service_type)

        if service is None or service.total_replicas != service.number_of_owners:
            loaded = yield self._load_services()
            if not loaded:
                returnValue((None, ''))

            # Try again
            service = self._services.get(service_type)
            if service is None or service.total_replicas != service.number_of_owners:
                returnValue((None, ''))

        for device_type, srv_type in self._device_type_service_map.items():
            if srv_type == service_type:
                returnValue((service, device_type))

        returnValue((None, ''))

    @inlineCallbacks
    def get_device_types(self):
        device_types = dict()
        try:
            blobs = yield self._backend.list('device_types')
            if blobs is None:
                log.error('adapters-not-found')
                returnValue(device_types)

        except Exception as e:
            log.exception('device-lookup-failed', e=e)
            returnValue(device_types)

        for blob, kv_metadata in blobs:
            key = kv_metadata.key.decode('utf-8')
            device_type = DeviceType()
            device_type.ParseFromString(blob)
            device_types[key] = device_type

        returnValue(device_types)

    @inlineCallbacks
    def get_adapters(self):
        adapters = dict()
        try:
            blobs = yield self._backend.list('adapters')
            if blobs is None:
                log.error('adapters-not-found')
                returnValue(adapters)

        except Exception as e:
            log.exception('adapter-lookup-failed', e=e)
            returnValue(adapters)

        # Data is marshalled as proto bytes in the data store
        for blob, kv_metadata in blobs:
            key = kv_metadata.key.decode('utf-8')
            adapter = Adapter()
            adapter.ParseFromString(blob)
            adapters[key] = adapter

        returnValue(adapters)

    @inlineCallbacks
    def get_devices(self):
        devices = dict()
        try:
            blobs = yield self._backend.list('devices')
            if blobs is None:
                log.error('devices-not-found')
                returnValue(devices)

        except Exception as e:
            log.exception('devices-lookup-failed', e=e)
            returnValue(devices)

        # Data is marshalled as proto bytes in the data store
        for blob, kv_metadata in blobs:
            key = kv_metadata.key.decode('utf-8')
            device = Device()
            device.ParseFromString(blob)
            devices[key] = device

        returnValue(devices)

    @inlineCallbacks
    def _load_services(self):
        log.debug('entry')

        if self._backend is None:
            log.error('backend-not-set')
            returnValue(False)

        self._services = dict()
        self._device_type_service_map = dict()

        # Load the adapters
        try:
            blobs = yield self._backend.list('adapters')
            if blobs is None:
                log.error('adapters-not-found')
                returnValue(False)

        except Exception as e:
            log.exception('adapter-lookup-failed', e=e)
            returnValue(False)

        # Data is marshalled as proto bytes in the data store
        for blob, kv_metadata in blobs:
            log.debug('key', key=kv_metadata.key.decode('utf-8'))
            adapter = Adapter()
            adapter.ParseFromString(blob)

            # A valid adapter should have the vendorID set
            if adapter.vendor is not None and adapter.vendor != '':
                if adapter.type not in self._services:
                    self._services[adapter.type] = Service(adapter.type,
                                                           adapter.totalReplicas)

                service = self._services[adapter.type]

                if service.total_replicas < adapter.totalReplicas:
                    service.total_replicas = adapter.totalReplicas

                current_replica = adapter.currentReplica
                endpoint = adapter.endpoint

                service.endpoints[current_replica] = endpoint

                key = self._make_key(current_replica, adapter.type, adapter.type)
                service.add_owner(key, Member(adapter.id, adapter.type, adapter.vendor,
                                              endpoint, adapter.version, adapter.currentReplica))

        # Load the device types
        try:
            blobs = yield self._backend.list('device_types')
            if blobs is None:
                log.error('adapters-not-found')
                returnValue(False)

        except Exception as e:
            log.exception('device-lookup-failed', e=e)
            returnValue(False)

        for blob, kv_metadata in blobs:
            log.debug('key', key=kv_metadata.key.decode('utf-8'))
            device_type = DeviceType()
            device_type.ParseFromString(blob)

            if device_type.id not in self._device_type_service_map:
                self._device_type_service_map[device_type.id] = device_type.adapter
        returnValue(True)


class Member:
    """
    Member is an element in the VOLTHA Adapters list
    """
    def __init__(self, ident, srv_type, vendor, endpoint, version, replica):
        """
        Create a new instance in the member list
        :param ident:    (str) Device ID
        :param srv_type: (str) Adapter name
        :param vendor:   (str) Vendor name
        :param endpoint: (str) Listening topic endpoint (Adapter Type + Current Replica)
        :param version:  (str) Build version
        :param replica:  (int) Current Replica number (1..max)
        """
        self._id = ident
        self._service_type = srv_type
        self._vendor = vendor
        self._endpoint = endpoint or ''
        self._version = version
        self._replica = replica
        log.info('member-init', ident=ident, service=srv_type, endpoint=endpoint)

    def __str__(self):
        return self._endpoint

    @property
    def ident(self):
        return self._id

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def replica(self):
        return self._replica

    @property
    def service_type(self):
        return self._service_type


class Service:
    def __init__(self, ident, total_replicas):
        """
        Define a service
        :param ident: (str) ID
        :param total_replicas: (int) Number of replicas
        """
        self._id = ident
        self._total_replicas = total_replicas
        self._endpoints = dict()   # Replica ID (int) -> Endpoint (str)
        self._member = dict()      # Key -> Member

    def get_owner(self, key):
        return self._member.get(key)

    def add_owner(self, key, member):
        self._member[key] = member

    @property
    def endpoints(self):
        return self._endpoints

    @property
    def owner(self):
        return self._member

    @property
    def total_replicas(self):
        return self._total_replicas

    @total_replicas.setter
    def total_replicas(self, value):
        if value < self._total_replicas:
            raise ValueError('Invalid Total Replicas: {}, must be > {}'.format(value, self._total_replicas))
        self._total_replicas = value

    @property
    def number_of_owners(self):
        return len(self._member)
