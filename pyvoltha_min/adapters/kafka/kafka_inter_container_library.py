#!/usr/bin/env python

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

# pylint: disable=import-error

import codecs
from uuid import uuid4
import json

import six
import structlog
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred, \
    DeferredQueue, succeed

from zope.interface import implementer

from opentracing import global_tracer, Format
from voltha_protos.inter_container_pb2 import MessageType, Argument, \
    InterContainerRequestBody, InterContainerMessage, InterContainerResponseBody, StrType, \
    InterAdapterMessage
from voltha_protos.inter_container_pb2 import _INTERADAPTERMESSAGETYPE_TYPES as IA_MSG_ENUM

from pyvoltha_min.common.utils import asleep
from pyvoltha_min.common.utils.tracing import create_async_span, create_child_span
from pyvoltha_min.common.utils.registry import IComponent
from .kafka_proxy import KafkaProxy, get_kafka_proxy


log = structlog.get_logger()

KAFKA_OFFSET_LATEST = 'latest'
KAFKA_OFFSET_EARLIEST = 'earliest'
ARG_FROM_TOPIC = 'fromTopic'
SPAN_ARG = 'span'
MESSAGE_KEY = 'msg'
PROCESS_IA_MSG_RPC = 'process_inter_adapter_message'
ROOT_SPAN_NAME_KEY = 'op-name'

# TODO: Look at rw-core transaction ID implications to this library


class KafkaMessagingError(Exception):
    def __init__(self, error):
        super().__init__()
        self.error = error


@implementer(IComponent)
class IKafkaMessagingProxy:
    _kafka_messaging_instance = None

    def __init__(self,
                 kafka_host_port,
                 default_topic,
                 group_id_prefix,
                 target_cls):
        """
        Initialize the kafka proxy.  This is a singleton (may change to
        non-singleton if performance is better)
        :param kafka_host_port: Kafka host and port
        :param default_topic: Default topic to subscribe to
        :param target_cls: target class - method of that class is invoked
        when a message is received on the default_topic
        """
        # return an exception if the object already exist
        if IKafkaMessagingProxy._kafka_messaging_instance:
            raise Exception('Singleton-exist')

        log.debug("Initializing-KafkaProxy")
        self.kafka_host_port = kafka_host_port
        self.default_topic = default_topic
        self.default_group_id = "_".join((group_id_prefix, default_topic))
        self.target_cls = target_cls
        self.topic_target_cls_map = {}
        self.topic_callback_map = {}
        self.subscribers = {}
        self.kafka_proxy = None
        self.transaction_id_deferred_map = {}
        self.received_msg_queue = DeferredQueue()
        self.stopped = False

        self.init_time = 0
        self.init_received_time = 0

        self.init_resp_time = 0
        self.init_received_resp_time = 0

        self.num_messages = 0
        self.total_time = 0
        self.num_responses = 0
        self.total_time_responses = 0
        log.debug("KafkaProxy-initialized")

    def start(self):
        try:
            log.debug("KafkaProxy-starting")

            # Get the kafka proxy instance.  If it does not exist then
            # create it
            self.kafka_proxy = get_kafka_proxy()
            if self.kafka_proxy is None:
                KafkaProxy(kafka_endpoint=self.kafka_host_port).start()
                self.kafka_proxy = get_kafka_proxy()

            # Subscribe the default topic and target_cls
            self.topic_target_cls_map[self.default_topic] = self.target_cls

            # Start the queue to handle incoming messages
            reactor.callLater(0, self._received_message_processing_loop)

            # Subscribe using the default topic and default group id.  Whenever
            # a message is received on that topic then teh target_cls will be
            # invoked.
            reactor.callLater(0, self.subscribe,
                              topic=self.default_topic,
                              target_cls=self.target_cls,
                              group_id=self.default_group_id)

            # Setup the singleton instance
            IKafkaMessagingProxy._kafka_messaging_instance = self
            log.debug("KafkaProxy-started")
        except Exception as e:
            log.exception("Failed-to-start-proxy", e=e)

    def stop(self):
        """
        Invoked to stop the kafka proxy
        :return: None on success, Exception on failure
        """
        log.debug("Stopping-messaging-proxy ...")
        d = succeed(None)
        if self.kafka_proxy is not None:
            try:
                # Stop the kafka proxy.  This will stop all the consumers
                # and producers
                self.stopped = True
                proxy, self.kafka_proxy = self.kafka_proxy, None
                queue, self.received_msg_queue = self.received_msg_queue, None

                if queue is not None:
                    try:
                        queue.put("bye-bye")
                    except Exception:            # nosec
                        pass

                if proxy is not None:
                    d = proxy.stop()

                log.debug("Messaging-proxy-stopped.")

            except Exception as e:
                log.exception("Exception-when-stopping-messaging-proxy:", e=e)
                # TODO: Only success for now

        return d

    def get_target_cls(self):
        return self.target_cls

    def get_default_topic(self):
        return self.default_topic

    @inlineCallbacks
    def _subscribe_group_consumer(self, group_id, topic, offset, callback=None,
                                  target_cls=None):
        try:
            log.debug("subscribing-to-topic-start", topic=topic)
            yield self.kafka_proxy.subscribe(topic,
                                             self._enqueue_received_group_message,
                                             group_id, offset)

            if target_cls is not None and callback is None:
                # Scenario #1
                if topic not in self.topic_target_cls_map:
                    self.topic_target_cls_map[topic] = target_cls

            elif target_cls is None and callback is not None:
                # Scenario #2
                log.debug("custom-callback", topic=topic,
                          callback_map=self.topic_callback_map)
                if topic not in self.topic_callback_map:
                    self.topic_callback_map[topic] = [callback]
                else:
                    self.topic_callback_map[topic].extend([callback])
            else:
                log.warn("invalid-parameters")

            returnValue(True)

        except Exception as e:
            log.exception("Exception-during-subscription", e=e)
            returnValue(False)

    @inlineCallbacks
    def subscribe(self, topic, callback=None, target_cls=None,
                  max_retry=3, group_id=None, offset=KAFKA_OFFSET_LATEST):
        """
        Scenario 1:  invoked to subscribe to a specific topic with a
        target_cls to invoke when a message is received on that topic.  This
        handles the case of request/response where this library performs the
        heavy lifting. In this case the m_callback must to be None

        Scenario 2:  invoked to subscribe to a specific topic with a
        specific callback to invoke when a message is received on that topic.
        This handles the case where the caller wants to process the message
        received itself. In this case the target_cls must to be None

        :param topic: topic to subscribe to
        :param callback: Callback to invoke when a message is received on
        the topic. Either one of callback or target_cls needs can be none
        :param target_cls:  Target class to use when a message is
        received on the topic. There can only be 1 target_cls per topic.
        Either one of callback or target_cls needs can be none
        :param max_retry:  the number of retries before reporting failure
        to subscribe.  This caters for scenario where the kafka topic is not
        ready.
        :param group_id:  The ID of the group the consumer is subscribing to
        :param offset: The topic offset on the kafka bus from where message consumption will start
        :return: True on success, False on failure
        """
        retry_backoff = [0.05, 0.1, 0.2, 0.5, 1, 2, 5]

        def _backoff(msg, retries):
            log.debug('backing-off', retries=retries)
            wait_time = retry_backoff[min(retries,
                                          len(retry_backoff) - 1)]
            log.info(msg, retry_in=wait_time)
            return asleep.asleep(wait_time)

        log.debug("subscribing", topic=topic, group_id=group_id,
                  callback=callback, target=target_cls)

        retry = 0
        subscribed = False
        if group_id is None:
            group_id = self.default_group_id

        while not subscribed:
            subscribed = yield self._subscribe_group_consumer(group_id, topic,
                                                              callback=callback,
                                                              target_cls=target_cls,
                                                              offset=offset)
            if subscribed:
                returnValue(True)
            elif retry > max_retry:
                returnValue(False)
            else:
                _backoff("subscription-not-complete", retry)
                retry += 1

    def unsubscribe(self, topic, callback=None, target_cls=None):
        """
        Invoked when unsubscribing to a topic
        :param topic: topic to unsubscribe from
        :param callback:  the callback used when subscribing to the topic, if any
        :param target_cls: the targert class used when subscribing to the topic, if any
        :return: None on success or Exception on failure
        """
        log.debug("Unsubscribing-to-topic", topic=topic)

        try:
            if self.kafka_proxy is not None:
                self.kafka_proxy.unsubscribe(topic,
                                             self._enqueue_received_group_message)
            # Both are None on device delete...
            # if callback is None and target_cls is None:
            #     log.error("both-call-and-target-cls-cannot-be-none",
            #               topic=topic)
            #     raise KafkaMessagingError(
            #         error="both-call-and-target-cls-cannot-be-none")

            if target_cls is not None and topic in self.topic_target_cls_map:
                self.topic_target_cls_map.pop(topic)

            if callback is not None and topic in self.topic_callback_map:
                index = 0
                for topic_cb in self.topic_callback_map[topic]:
                    if topic_cb == callback:
                        break
                    index += 1
                if index < len(self.topic_callback_map[topic]):
                    self.topic_callback_map[topic].pop(index)

                if len(self.topic_callback_map[topic]) == 0:
                    del self.topic_callback_map[topic]

        except Exception as e:
            log.exception("Exception-when-unsubscribing-to-topic", topic=topic, e=e)

    @inlineCallbacks
    def _enqueue_received_group_message(self, msg):
        """
        Internal method to continuously queue all received messaged
        irrespective of topic
        :param msg: Received message
        :return: None on success, Exception on failure
        """
        if self.received_msg_queue is not None:
            try:
                log.debug("received-msg", message=msg)
                yield self.received_msg_queue.put(msg)

            except Exception as e:
                log.exception("Failed-enqueueing-received-message", e=e)

    @inlineCallbacks
    def _received_message_processing_loop(self):    # pylint: disable=no-self-use
        """
        Internal method to continuously process all received messages one
        at a time
        :return: None on success, Exception on failure
        """
        log.debug("entry")
        while not self.stopped and self.received_msg_queue is not None:
            try:
                message = yield self.received_msg_queue.get()
                if not self.stopped:
                    reactor.callLater(0, self._handle_message, message)

            except Exception as e:
                log.exception("Failed-dequeueing-received-message", e=e)
        log.debug("exiting")

    @staticmethod
    def _to_string(unicode_str):
        if unicode_str is not None:
            if isinstance(unicode_str, six.string_types):
                return unicode_str

            return codecs.encode(unicode_str, 'ascii')
        return None

    @staticmethod
    def _format_request(rpc, to_topic, reply_topic, **kwargs):
        """
        Format a request to send over kafka
        :param rpc: Requested remote API
        :param to_topic: Topic to send the request
        :param reply_topic: Topic to receive the resulting response, if any
        :param kwargs: Dictionary of key-value pairs to pass as arguments to
        the remote rpc API.
        :return: A InterContainerMessage message type on success or None on
        failure
        """
        try:
            transaction_id = uuid4().hex
            request = InterContainerMessage()
            request_body = InterContainerRequestBody()
            request.header.id = transaction_id
            request.header.type = MessageType.Value("REQUEST")
            request.header.from_topic = reply_topic
            request.header.to_topic = to_topic

            if reply_topic:
                request_body.reply_to_topic = reply_topic
                request_body.response_required = True
                response_required = True
            else:
                response_required = False

            request.header.timestamp.GetCurrentTime()
            request_body.rpc = rpc
            for key, value in kwargs.items():
                arg = Argument()
                arg.key = key
                try:
                    arg.value.Pack(value)
                    request_body.args.extend([arg])

                except Exception as e:
                    log.exception("Failed-parsing-value", e=e, key=key)

            request.body.Pack(request_body)
            return request, transaction_id, response_required

        except Exception as e:
            log.exception("formatting-request-failed",
                          rpc=rpc,
                          to_topic=to_topic,
                          reply_topic=reply_topic,
                          args=kwargs)
            return None, None, None

    @staticmethod
    def _format_response(msg_header, msg_body, status):
        """
        Format a response
        :param msg_header: The header portion of a received request
        :param msg_body: The response body
        :param status: True is this represents a successful response
        :return: a InterContainerMessage message type
        """
        try:
            response = InterContainerMessage()
            response_body = InterContainerResponseBody()
            response.header.id = msg_header.id
            response.header.timestamp.GetCurrentTime()
            response.header.type = MessageType.Value("RESPONSE")
            response.header.from_topic = msg_header.to_topic
            response.header.to_topic = msg_header.from_topic
            if isinstance(msg_body, Deferred):
                msg_body = msg_body.result
            if msg_body is not None:
                response_body.result.Pack(msg_body)
            response_body.success = status
            response.body.Pack(response_body)
            return response

        except Exception as e:
            log.exception("formatting-response-failed", header=msg_header,
                          body=msg_body, status=status, e=e)
            return None

    @staticmethod
    def _parse_response(msg):
        try:
            message = InterContainerMessage()
            message.ParseFromString(msg)
            resp = InterContainerResponseBody()
            if message.body.Is(InterContainerResponseBody.DESCRIPTOR):
                message.body.Unpack(resp)
            else:
                log.debug("unsupported-msg", msg_type=type(message.body))
                return None
            log.debug("parsed-response", type=message.header.type, from_topic=message.header.from_topic,
                      to_topic=message.header.to_topic, transaction_id=message.header.id)
            return resp

        except Exception as e:
            log.exception("parsing-response-failed", message=msg, e=e)
            return None

    @inlineCallbacks
    def _handle_message(self, msg):     # pylint: disable=too-many-locals, too-many-branches
        """
        Default internal method invoked for every batch of messages received
        from Kafka.
        """
        if self.stopped:
            returnValue(None)

        def _augment_args_with_from_topic(args, from_topic):
            arg = Argument(key=ARG_FROM_TOPIC)
            arg.value.Pack(StrType(val=from_topic))
            args.extend([arg])
            return args

        def _to_dict(args):
            """
            Convert a repeatable Argument type into a python dictionary
            :param args: Repeatable core_adapter.Argument type
            :return: a python dictionary
            """
            return {arg.key: arg.value for arg in args} if args is not None else None

        try:
            val = msg.value()
            log.debug("rx-msg", message=msg)

            # Go over customized callbacks first
            m_topic = msg.topic()

            if m_topic in self.topic_callback_map:
                for topic_cb in self.topic_callback_map[m_topic]:
                    yield topic_cb(val)

            # Check whether we need to process request/response scenario
            if m_topic not in self.topic_target_cls_map:
                returnValue(None)

            # Process request/response scenario
            message = InterContainerMessage()
            message.ParseFromString(val)

            if message.header.type == MessageType.Value("REQUEST"):
                # Get the target class for that specific topic
                targetted_topic = self._to_string(message.header.to_topic)
                msg_body = InterContainerRequestBody()

                if message.body.Is(InterContainerRequestBody.DESCRIPTOR):
                    message.body.Unpack(msg_body)
                else:
                    log.debug("unsupported-msg", msg_type=type(message.body))
                    returnValue(None)

                # Extract opentrace span from the message
                with self.enrich_context_with_span(msg_body.rpc, msg_body.args) as scope:
                    log.debug('rx-span')
                    span = scope.span if scope is not None else None

                    if targetted_topic in self.topic_target_cls_map:

                        # TODO: If transaction ID in v2.6 required, support here
                        # // let the callee unpack the arguments as its the only one that knows the real proto type
                        # // Augment the requestBody with the message Id as it will be used in scenarios where cores
                        # // are set in pairs and competing
                        # requestBody.Args = kp.addTransactionId(msg.Header.Id, requestBody.Args)

                        # Augment the request arguments with the from_topic
                        augmented_args = _augment_args_with_from_topic(msg_body.args,
                                                                      msg_body.reply_to_topic)
                        try:
                            if augmented_args:
                                log.debug("message-body-args-present", rpc=msg_body.rpc,
                                          response_required=msg_body.response_required,
                                          reply_to_topic=msg_body.reply_to_topic)
                                (status, res) = yield getattr(
                                    self.topic_target_cls_map[targetted_topic],
                                    self._to_string(msg_body.rpc))(**_to_dict(augmented_args))
                            else:
                                log.debug("message-body-args-absent", rpc=msg_body.rpc,
                                          response_required=msg_body.response_required,
                                          reply_to_topic=msg_body.reply_to_topic,)
                                (status, res) = yield getattr(
                                    self.topic_target_cls_map[targetted_topic],
                                    self._to_string(msg_body.rpc))()

                            if msg_body.response_required:
                                response = self._format_response(
                                    msg_header=message.header,
                                    msg_body=res,
                                    status=status,
                                )
                                if response is not None:
                                    res_topic = self._to_string(response.header.to_topic)
                                    res_span, span = span, None
                                    self.send_kafka_message(res_topic, response, res_span)

                                log.debug("Response-sent", to_topic=res_topic)

                        except Exception as _e:
                            # TODO: set error in span
                            log.exception('request-failure', e=_e)

                        finally:
                            if span is not None:
                                span.finish()

            elif message.header.type == MessageType.Value("RESPONSE"):
                trns_id = self._to_string(message.header.id)
                log.debug('received-response', transaction_id=trns_id)
                if trns_id in self.transaction_id_deferred_map:
                    resp = self._parse_response(val)
                    self.transaction_id_deferred_map[trns_id].callback(resp)
            else:
                log.error("INVALID-TRANSACTION-TYPE")

        except Exception as e:
            log.exception("Failed-to-process-message", message=msg, e=e)

    @inlineCallbacks
    def send_kafka_message(self, topic, msg, span):
        if self.kafka_proxy is not None:
            log.debug('sending', topic=topic, message=msg)
            try:
                yield self.kafka_proxy.send_message(topic, msg.SerializeToString(), span=span)

            except Exception as e:
                log.info("failed-sending-message", message=msg, e=e)

        elif span is not None:
            span.error('kafka-proxy not available')
            span.finish()

    @inlineCallbacks
    def send_request(self, rpc, to_topic, reply_topic=None,   # pylint: disable=too-many-branches
                     callback=None, **kwargs):
        """
        Invoked to send a message to a remote container and receive a response if required

        :param rpc:         The remote API to invoke
        :param to_topic:    Send the message to this kafka topic
        :param reply_topic: If not None then a response is expected on this
                            topic.  If set to None then no response is required.
        :param callback:    Callback to invoke when a response is received.
        :param kwargs:      Key-value pairs representing arguments to pass to the
                            rpc remote API.

        :return: Either no response is required, or a response is returned
                 via the callback or the response is a tuple of (status, return_cls)
        """
        if self.stopped:
            returnValue(None)

        span = None
        try:
            # Embed opentrace span
            is_async = not reply_topic
            span_arg, span = self.embed_span_as_arg(rpc, is_async, kwargs.get(MESSAGE_KEY))

            if span_arg is not None:
                kwargs[SPAN_ARG] = span_arg

            # Ensure all strings are not unicode encoded
            rpc = self._to_string(rpc)
            to_topic = self._to_string(to_topic)
            reply_topic = self._to_string(reply_topic)

            request, transaction_id, response_required = self._format_request(rpc=rpc,
                                                                              to_topic=to_topic,
                                                                              reply_topic=reply_topic,
                                                                              **kwargs)
            if request is None:
                if span is not None:
                    span.error('failed to format request')
                returnValue(None)

            # Add the transaction to the transaction map before sending the
            # request.  This will guarantee the eventual response will be
            # processed.
            wait_for_result = None

            if response_required:
                wait_for_result = Deferred()
                self.transaction_id_deferred_map[self._to_string(request.header.id)] = wait_for_result

            log.debug("message-send", transaction_id=transaction_id, to_topic=to_topic,
                      from_topic=reply_topic, rpc=rpc)

            if is_async:
                async_span, span = span, None
            else:
                async_span = None

            yield self.send_kafka_message(to_topic, request, async_span)

            log.debug("message-sent", transaction_id=transaction_id, to_topic=to_topic,
                      from_topic=reply_topic, rpc=rpc)

            if response_required:
                res = yield wait_for_result

                # Remove the transaction from the transaction map
                del self.transaction_id_deferred_map[transaction_id]

                if res is not None:
                    if res.success:
                        log.debug("send-message-response", transaction_id=transaction_id, rpc=rpc)
                        if callback:
                            callback((res.success, res.result))
                        else:
                            returnValue((res.success, res.result))
                    else:
                        # this is the case where the core API returns a grpc code.NotFound.  Return or callback
                        # so the caller can act appropriately (i.e add whatever was not found)
                        log.info("send-message-response-error-result", transaction_id=transaction_id,
                                 rpc=rpc, kafka_request=request, kafka_result=res)
                        if callback:
                            callback((res.success, None))
                        else:
                            returnValue((res.success, None))
                else:
                    raise KafkaMessagingError("failed-response-for-request:{}".format(request))

        except Exception as e:
            log.exception("Exception-sending-request", e=e)
            raise KafkaMessagingError(e) from e

        finally:
            if span is not None:
                span.finish()

    @staticmethod
    def embed_span_as_arg(rpc, is_async, msg=None):
        """
        Method to extract Open-tracing Span from Context and serialize it for transport over Kafka embedded
        as a additional argument.

        Additional argument is injected using key as "span" and value as Span marshalled into a byte slice

        The span name is automatically constructed using the RPC name with following convention
        (<rpc-name> represents name of invoked method):

          - RPC invoked in Sync manner (WaitForResponse=true) : kafka-rpc-<rpc-name>
          - RPC invoked in Async manner (WaitForResponse=false) : kafka-async-rpc-<rpc-name>
          - Inter Adapter RPC invoked in Sync manner (WaitForResponse=true) : kafka-inter-adapter-rpc-<rpc-name>
          - Inter Adapter RPC invoked in Async manner (WaitForResponse=false) : kafka-inter-adapter-async-rpc-<rpc-name>
        """
        tracer = global_tracer()
        if tracer is None:
            return None, None

        try:
            span_name = 'kafka-'

            # In case of inter adapter message, use Msg Type for constructing RPC name
            if rpc == PROCESS_IA_MSG_RPC:
                span_name += 'inter-adapter-'
                msg_type = msg.header.type
                try:
                    rpc = IA_MSG_ENUM.values_by_number[msg_type].name

                except Exception as _e:
                    rpc = 'msg-type-{}'.format(msg_type)

            span_name += 'async-rpc-' if is_async else 'rpc-'
            span_name += rpc

            if is_async:
                span_to_inject = create_async_span(span_name)
            else:
                span_to_inject = create_child_span(span_name)

            if span_to_inject is None:
                return None, None

            span_to_inject.set_baggage_item('rpc-span-name', span_name)

            text_map = dict()
            tracer.inject(span_to_inject, Format.TEXT_MAP, text_map)
            text_map_json = json.dumps(text_map, indent=None, separators=(',', ':'))
            span_arg = StrType(val=text_map_json)

            return span_arg, span_to_inject

        except Exception as _e:
            return None, None

    @staticmethod
    def enrich_context_with_span(rpc_name, args):
        """
        Method to extract the Span embedded in Kafka RPC request on the receiver side.

        If span is found embedded in the KV args (with key as "span"), it is de-serialized and injected
        into the Context to be carried forward by the RPC request processor thread.  If no span is found
        embedded, even then a span is created with name as "kafka-rpc-<rpc-name>" to enrich the Context
        for RPC calls coming from components currently not sending the span (e.g. openonu adapter)
        """
        tracer = global_tracer()

        try:
            for arg in args:
                if arg.key == SPAN_ARG and arg.value is not None:
                    text_map_string = StrType()
                    arg.value.Unpack(text_map_string)

                    span_dict = json.loads(text_map_string.val)
                    span_ctx = tracer.extract(Format.TEXT_MAP, span_dict)

                    rx_rpc_name = span_ctx.baggage.get('rpc-span-name')

                    return tracer.start_active_span(rx_rpc_name, child_of=span_ctx,
                                                    finish_on_close=False)
        except Exception as e:
            log.info('exception-during-context-decode', err=str(e))

        # If here, no active span found in request, start a new span instead
        span_name = 'kafka-'

        if rpc_name == PROCESS_IA_MSG_RPC:
            for arg in args:
                if arg.key == MESSAGE_KEY:
                    ia_msg = InterAdapterMessage()
                    arg.value.Unpack(ia_msg)
                    span_name += 'inter-adapter-'
                    msg_type = ia_msg.header.type
                    try:
                        rpc_name = IA_MSG_ENUM.values_by_number[msg_type].name

                    except Exception as _e:
                        rpc_name = 'msg-type-{}'.format(msg_type)
                    break

        span_name += 'rpc-' + rpc_name

        return tracer.start_active_span(span_name, ignore_active_span=True,
                                        finish_on_close=False)


# Common method to get the singleton instance of the kafka proxy class
def get_messaging_proxy():
    # pylint: disable=protected-access
    return IKafkaMessagingProxy._kafka_messaging_instance
