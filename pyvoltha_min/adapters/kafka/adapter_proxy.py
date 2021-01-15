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
Agent to play gateway between adapters.
"""

import codecs
from uuid import uuid4

import six
import structlog
from twisted.internet.defer import inlineCallbacks, returnValue
from voltha_protos.inter_container_pb2 import InterAdapterHeader, InterAdapterMessage

from pyvoltha_min.adapters.common.kvstore.twisted_etcd_store import TwistedEtcdStore
from pyvoltha_min.common.config.kvstore_prefix import KvStore
from .container_proxy import ContainerProxy
from .endpoint_manager import EndpointManager

log = structlog.get_logger()


class AdapterProxy(ContainerProxy):     # pylint: disable=too-few-public-methods
    def __init__(self, kafka_proxy, adapter_topic, my_listening_topic, kv_store_address):
        super().__init__(kafka_proxy, adapter_topic, my_listening_topic)
        # KV store's IP Address and PORT
        host, port = kv_store_address.split(':', 1)
        etcd = TwistedEtcdStore(host, port, KvStore.prefix)
        self._endpoint_manager = EndpointManager(etcd)

    @staticmethod
    def _to_string(unicode_str):
        if unicode_str is not None:
            if isinstance(unicode_str, six.string_types):
                return unicode_str
            return codecs.encode(unicode_str, 'ascii')
        return None

    # pylint: disable=too-many-arguments
    @ContainerProxy.wrap_request(None)
    @inlineCallbacks
    def send_inter_adapter_message(self, msg, msg_type, from_adapter, to_adapter, endpoint,
                                   to_device_id=None, proxy_device_id=None,
                                   message_id=None):
        """
        Sends a message directly to an adapter. This is typically used to send
        proxied messages from one adapter to another.  An initial ACK response
        is sent back to the invoking adapter.  If there is subsequent response
        to be sent back (async) then the adapter receiving this request will
        use this same API to send back the async response.
        :param msg : GRPC message to send
        :param msg_type : InterAdapterMessageType of the message to send
        :param from_adapter: Name of the adapter making the request.
        :param to_adapter: Name of the remote adapter.
        :param endpoint: Topic Endpoint. If not provided, it will be looked up
        :param to_device_id: The ID of the device for to the message is
        intended. if it's None then the message is not intended to a specific
        device.  Its interpretation is adapter specific.
        :param proxy_device_id: The ID of the device which will proxy that
        message. If it's None then there is no specific device to proxy the
        message.  Its interpretation is adapter specific.
        :param message_id: A unique number for this transaction that the
        adapter may use to correlate a request and an async response.
        """
        try:
            # Set to_adapter
            if to_adapter is None:
                to_adapter = self.remote_topic

            # HACK: If endpoint not provided assume a single replica instance
            if endpoint is None or len(endpoint) == 0:
                endpoint = to_adapter + '_1'

            # Build the inter adapter message
            hdr = InterAdapterHeader()
            hdr.type = msg_type
            hdr.from_topic = self._to_string(from_adapter)
            hdr.to_topic = self._to_string(endpoint)
            hdr.to_device_id = self._to_string(to_device_id)
            hdr.proxy_device_id = self._to_string(proxy_device_id)
            hdr.id = self._to_string(message_id) if message_id else uuid4().hex

            hdr.timestamp.GetCurrentTime()

            ia_msg = InterAdapterMessage()
            ia_msg.header.CopyFrom(hdr)
            ia_msg.body.Pack(msg)

            log.debug("sending-inter-adapter-message", type=ia_msg.header.type,
                      from_topic=ia_msg.header.from_topic, to_topic=ia_msg.header.to_topic,
                      to_device_id=ia_msg.header.to_device_id)

            res = yield self.invoke(rpc="process_inter_adapter_message",
                                    to_topic=ia_msg.header.to_topic,
                                    msg=ia_msg)
            returnValue(res)

        except Exception as e:
            log.warn("error-sending-request", e=e)

    @property
    def endpoint_manager(self):
        return self._endpoint_manager
