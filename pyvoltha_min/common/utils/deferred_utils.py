# Copyright 2017-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.internet.error import AlreadyCalled, AlreadyCancelled
from twisted.internet.defer import TimeoutError as TwistedTimeoutErrorDefer


class DeferredWithTimeout(Deferred):
    """
    Deferred with a timeout. If neither the callback nor the errback method
    is not called within the given time, the deferred's errback will be called
    with a Twisted TimeOutError() exception.

    All other uses are the same as of Deferred().
    """
    def __init__(self, timeout=1.0):
        Deferred.__init__(self)
        self._timeout = timeout
        self._timer = reactor.callLater(timeout, self.timed_out)

    def timed_out(self):
        self.errback(TwistedTimeoutErrorDefer('timed out after {} seconds'.
                                              format(self._timeout)))

    def callback(self, result):
        self._cancel_timer()
        return Deferred.callback(self, result)

    def errback(self, fail=None):
        self._cancel_timer()
        return Deferred.errback(self, fail)

    def cancel(self):
        self._cancel_timer()
        return Deferred.cancel(self)

    def _cancel_timer(self):
        timer, self._timer = self._timer, None
        try:
            if timer is not None and not timer.called and not timer.cancelled:
                timer.cancel()
                return

        except (AlreadyCalled, AlreadyCancelled):
            pass
