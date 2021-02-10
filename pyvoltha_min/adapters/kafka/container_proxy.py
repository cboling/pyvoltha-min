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
The superclass for all kafka proxy subclasses.
"""
import time
import structlog
from twisted.internet.defer import inlineCallbacks, returnValue, failure
from twisted.internet.defer import TimeoutError as TwistedTimeoutErrorDefer
from twisted.internet.error import TimeoutError as TwistedTimeoutError
from zope.interface import implementer

from pyvoltha_min.common.utils.deferred_utils import DeferredWithTimeout
from pyvoltha_min.common.utils.registry import IComponent

log = structlog.get_logger()


class KafkaMessagingError(BaseException):
    def __init__(self, error):
        super().__init__()
        self.error = error


@implementer(IComponent)
class ContainerProxy:

    def __init__(self, kafka_proxy, remote_topic, my_listening_topic):
        self.kafka_proxy = kafka_proxy
        self.listening_topic = my_listening_topic
        self.remote_topic = remote_topic
        self.default_timeout = 60    # VOL-2163 changed this from 10s to 60s for the
                                     # voltha-lib-go
    def start(self):
        log.info('started')
        return self

    @staticmethod
    def stop():
        log.info('stopped')

    @classmethod
    def wrap_request(cls, return_cls):
        def real_wrapper(func):
            @inlineCallbacks
            def wrapper(*args, **kw):
                try:
                    results = yield func(*args, **kw)

                    if not isinstance(results, tuple):
                        returnValue(results)

                    success = results[0]
                    value = results[1]

                    if success:
                        log.debug("successful-response", func=func)
                        if return_cls is not None:
                            rclass = return_cls()
                            if value is not None:
                                value.Unpack(rclass)
                            returnValue(rclass)
                        else:
                            log.debug("successful-response-none", func=func)
                            returnValue(None)
                    else:
                        log.info("unsuccessful-request", func=func, args=args, kw=kw, value=value)
                        returnValue(value)

                except (TwistedTimeoutError, TwistedTimeoutErrorDefer):
                    log.warn("request-timeout", func=func)
                    raise

                except Exception as e:
                    log.exception("request-wrapper-exception", func=func, e=e)
                    raise

            return wrapper

        return real_wrapper

    @inlineCallbacks
    def invoke(self, rpc, to_topic=None, reply_topic=None, response_required=True, timeout=None, retries=0, **kwargs):
        @inlineCallbacks
        def _send_request(rpc_to_call, m_callback, to_container, reply, response, **kw):
            try:
                log.debug("sending-request", rpc=rpc, to_topic=to_container, reply_topic=reply)

                if to_container is None:
                    to_container = self.remote_topic

                if reply is None:
                    reply = self.listening_topic

                result = yield self.kafka_proxy.send_request(rpc=rpc_to_call,
                                                             to_topic=to_container,
                                                             reply_topic=reply,
                                                             callback=None,
                                                             response_required=response,
                                                             **kw)
                if not m_callback.called:
                    m_callback.callback(result)
                else:
                    log.debug('timeout-already-occurred', rpc=rpc)

            except Exception as _e:
                log.exception("failure-sending-request", rpc=rpc, kw=kw)
                if not m_callback.called:
                    m_callback.errback(failure.Failure())

        # We are going to resend the request on the to_topic if there is a
        # timeout error. This time the timeout will be longer.  If the second
        # request times out then we will send the request to the default
        # core_topic.
        attempts = retries + 1
        timeouts = [timeout or self.default_timeout] * attempts
        retry = 0

        for response_timeout in timeouts:
            d = DeferredWithTimeout(timeout=response_timeout)

            send_time_1 = time.monotonic()
            _send_request(rpc, d, to_topic, reply_topic, response_required, **kwargs)
            send_time_2 = time.monotonic()
            try:
                res = yield d
                returnValue(res)

            except (TwistedTimeoutError, TwistedTimeoutErrorDefer) as e:
                now = time.monotonic()
                if retry >= attempts - 1:
                    log.warn('invoke-timeout', e=e, retry=retry, attempts=attempts,
                             delta_1=now - send_time_1, delta_2=now - send_time_2)
                    raise e

                log.info('invoke-timeout', e=e, retry=retry, attempts=attempts,
                         delta_1=now - send_time_1, delta_2=now - send_time_2)
                retry += 1

            except Exception as e:
                log.exception('send-request-failed', e=e)
                raise
