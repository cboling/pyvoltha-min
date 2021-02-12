# Copyright 2020 the original author or authors.
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
import structlog
import etcd3
import etcd3.utils as utils

from twisted.internet import threads
from twisted.internet.defer import CancelledError, inlineCallbacks, returnValue
from twisted.python.failure import Failure

DEFAULT_KVSTORE_RETRIES = 0
DEFAULT_KVSTORE_TIMEOUT = 5


class TwistedEtcdStore:
    def __init__(self, host, port, path_prefix, timeout=DEFAULT_KVSTORE_TIMEOUT,
                 default_retries=DEFAULT_KVSTORE_RETRIES):
        self._etcd = etcd3.client(host=host, port=port, timeout=timeout)
        self._host = host
        self._port = port
        self._path_prefix = path_prefix
        self._default_retries = default_retries
        self._timeout = timeout

        self.log = structlog.get_logger(client='{}:{}'.format(self._host, self._port))
        # TODO: Before deprecating the blocking 'EtcdStore' class, add support
        #       for retries. Specifically look for a failed connection where a
        #       a new client needs to be added.

    def __str__(self):
        return 'etcd-{}:{}/{}, Timeout: {}'.format(self._host, self._port,
                                                   self._path_prefix, self._timeout)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @inlineCallbacks
    def __getitem__(self, key):
        (value, _meta) = yield self.get(self.make_path(key))
        if value is not None:
            returnValue(value)

        raise KeyError(key)

    @inlineCallbacks
    def __contains__(self, key):
        (value, _meta) = yield self.get(key)
        returnValue(value is not None)

    @inlineCallbacks
    def __setitem__(self, key, value):
        _results = yield self.set(key, value)

    @inlineCallbacks
    def __delitem__(self, key):
        _results = yield self.delete(key)

    def close(self):
        client, self._etcd = self._etcd, None
        if client is not None:
            threads.deferToThread(client.close)

    def make_path(self, key):
        if key is None:
            return self._path_prefix
        return '{}/{}'.format(self._path_prefix, key)

    def _failure(self, reason, key, operation, retries):
        """ Common error handler """
        if isinstance(reason, Failure) and issubclass(type(reason.value), CancelledError):
            self.log.debug('{}-cancelled'.format(operation))
        else:
            self.log.info('{}-failure'.format(operation), error=reason, key=key)

            if retries:
                # TODO: Need to retry and set up *args, **kwargs appropriately
                #       for the command.  Watch specifically for client connection
                #       failures and test best way to handle this.   See also if
                #       timeouts are supported...  The older EtcdStore() that blocked
                #       had a small pause on the retry mechanism.
                retries -= 1
                self.log.debug('retries-not-yet-supported', remaining=retries)
                # TODO: what is best way to handle callback/errback chain and can it even be
                #       done?  May just need to watch for connection failures and determine
                #       the best way to recover.

        return reason

    def get(self, key, retries=None):
        def success(results):
            (value, _meta) = results
            return value

        deferred = threads.deferToThread(self._etcd.get, self.make_path(key))
        deferred.addCallbacks(success, self._failure,
                              errbackArgs=[key, 'get', retries or self._default_retries])
        return deferred

    def set(self, key, value, retries=None):

        def success(results):
            if results:
                return results
            return False

        deferred = threads.deferToThread(self._etcd.put, self.make_path(key), value)
        deferred.addCallbacks(success, self._failure,
                              errbackArgs=[key, 'set', retries or self._default_retries])
        return deferred

    def list(self, key, keys_only=False, retries=None):

        def success(results):
            if results:
                return results
            return False

        deferred = threads.deferToThread(self._etcd.get_prefix, self.make_path(key),
                                         keys_only=keys_only)
        deferred.addCallbacks(success, self._failure,
                              errbackArgs=[key, 'list', retries or self._default_retries])
        return deferred

    def watch(self, key, callback, watch_prefix=False, retries=None):

        def success(results):
            return results

        path = self.make_path(key)
        if watch_prefix:
            kwargs = dict(range_end=utils.increment_last_byte(utils.to_bytes(path)))
            deferred = threads.deferToThread(self._etcd.add_watch_callback, path, callback, **kwargs)
        else:
            deferred = threads.deferToThread(self._etcd.add_watch_callback, path, callback)

        deferred.addCallbacks(success, self._failure,
                              errbackArgs=[key, 'watch', retries or self._default_retries])
        return deferred

    def delete(self, key, retries=None):
        self.log.debug('entry', key=key)

        if key is None or len(key) == 0:
            raise ValueError("key-not-provided: '{}'".format(key))

        def success(results, k):
            self.log.debug('delete-success', results=results, key=k)
            if results:
                return results
            return False

        deferred = threads.deferToThread(self._etcd.delete, self.make_path(key))
        deferred.addCallbacks(success, self._failure, callbackArgs=[key],
                              errbackArgs=[key, 'delete', retries or self._default_retries])
        return deferred

    def delete_prefix(self, prefix, retries=None):
        self.log.debug('entry', prefix=prefix)

        def success(results, k):
            self.log.debug('delete-prefix-success', results=results, prefix=k)
            if results:
                return results
            return False

        if prefix is not None and len(prefix) > 0:
            prefix = self.make_path(prefix)
        else:
            # Deleting a range of keys with a prefix
            prefix = self._path_prefix
            while prefix[-1:] == '/':
                prefix = prefix[:-1]

        deferred = threads.deferToThread(self._etcd.delete_prefix, prefix)
        deferred.addCallbacks(success, self._failure, callbackArgs=[prefix],
                              errbackArgs=[prefix, 'delete-prefix', retries or self._default_retries])
        return deferred
