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

import structlog
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.defer import TimeoutError as TwistedTimeoutErrorDefer
from twisted.internet.error import TimeoutError as TwistedTimeoutError
from twisted.python import failure
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
        self.default_timeout = 6

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
                    if isinstance(results, tuple):
                        success = results[0]
                        d = results[1]
                    else:
                        success = False
                        d = None

                    if success:
                        log.debug("successful-response", func=func)
                        if return_cls is not None:
                            rclass = return_cls()
                            if d is not None:
                                d.Unpack(rclass)
                            returnValue(rclass)
                        else:
                            log.debug("successful-response-none", func=func)
                            returnValue(None)
                    else:
                        log.info("unsuccessful-request", func=func, args=args, kw=kw)
                        returnValue(d)

                except Exception as e:
                    log.exception("request-wrapper-exception", func=func, e=e)
                    raise

            return wrapper

        return real_wrapper

    @inlineCallbacks
    def invoke(self, rpc, to_topic=None, reply_topic=None, **kwargs):
        @inlineCallbacks
        def _send_request(rpc, m_callback, to_topic, reply_topic, **kwargs):
            try:
                log.debug("sending-request",
                          rpc=rpc,
                          to_topic=to_topic,
                          reply_topic=reply_topic)
                if to_topic is None:
                    to_topic = self.remote_topic
                if reply_topic is None:
                    reply_topic = self.listening_topic
                result = yield self.kafka_proxy.send_request(rpc=rpc,
                                                             to_topic=to_topic,
                                                             reply_topic=reply_topic,
                                                             callback=None,
                                                             **kwargs)
                if not m_callback.called:
                    m_callback.callback(result)
                else:
                    log.debug('timeout-already-occurred', rpc=rpc)

            except Exception as _e:
                log.exception("failure-sending-request", rpc=rpc, kw=kwargs)
                if not m_callback.called:
                    m_callback.errback(failure.Failure())

        # We are going to resend the request on the to_topic if there is a
        # timeout error. This time the timeout will be longer.  If the second
        # request times out then we will send the request to the default
        # core_topic.
        timeouts = [self.default_timeout,
                    self.default_timeout*2,
                    self.default_timeout]
        retry = 0
        max_retry = 2
        for timeout in timeouts:
            d = DeferredWithTimeout(timeout=timeout)
            _send_request(rpc, d, to_topic, reply_topic, **kwargs)
            try:
                res = yield d
                returnValue(res)

            except (TwistedTimeoutError, TwistedTimeoutErrorDefer) as e:
                if retry == max_retry:
                    log.warn('invoke-timeout', e=e, retry=retry, max_retry=max_retry)
                    raise e

                log.info('invoke-timeout', e=e, retry=retry, max_retry=max_retry)
                retry += 1
                if retry == max_retry:
                    to_topic = self.remote_topic

            except Exception as e:
                log.exception('send-request-failed', e=e)
                raise
