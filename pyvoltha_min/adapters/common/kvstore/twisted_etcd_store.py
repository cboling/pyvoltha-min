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
from twisted.internet.defer import CancelledError
from twisted.python.failure import Failure

log = structlog.get_logger()


class TwistedEtcdStore:

    def __init__(self, host, port, path_prefix, timeout=None):
        self._etcd = etcd3.client(host=host, port=port, timeout=timeout)
        self.host = host
        self.port = port
        self._path_prefix = path_prefix

    def close(self):
        client, self._etcd = self._etcd, None
        if client is not None:
            threads.deferToThread(client.close)

    def make_path(self, key):
        if key is None:
            return self._path_prefix
        return '{}/{}'.format(self._path_prefix, key)

    def get(self, key):

        def success(results):
            (value, _meta) = results
            return value

        def failure(reason):
            if isinstance(reason, Failure) and issubclass(type(reason.value), CancelledError):
                log.debug('get-cancelled')
            else:
                log.info('get-failure', error=reason, key=key)
            return reason

        deferred = threads.deferToThread(self._etcd.get, self.make_path(key))
        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def set(self, key, value):

        def success(results):
            if results:
                return results
            return False

        def failure(reason):
            if isinstance(reason, Failure) and issubclass(type(reason.value), CancelledError):
                log.debug('set-cancelled')
            else:
                log.info('set-failure', error=reason, key=key, value=value)
            return reason

        deferred = threads.deferToThread(self._etcd.put, self.make_path(key), value)
        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def list(self, key, keys_only=False):

        def success(results):
            if results:
                return results
            return False

        def failure(reason):
            if isinstance(reason, Failure) and issubclass(type(reason.value), CancelledError):
                log.debug('list-cancelled')
            else:
                log.info('list-failure', error=reason, key=key)
            return reason

        deferred = threads.deferToThread(self._etcd.get_prefix, self.make_path(key), keys_only=keys_only)
        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def watch(self, key, callback, watch_prefix=False):

        def success(results):
            return results

        def failure(reason):
            if isinstance(reason, Failure) and issubclass(type(reason.value), CancelledError):
                log.debug('watch-cancelled')
            else:
                log.info('watch-failure', error=reason, key=key)
            return reason

        path = self.make_path(key)
        if watch_prefix:
            kwargs = dict(range_end=utils.increment_last_byte(utils.to_bytes(path)))
            deferred = threads.deferToThread(self._etcd.add_watch_callback, path, callback, **kwargs)
        else:
            deferred = threads.deferToThread(self._etcd.add_watch_callback, path, callback)

        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def delete(self, key):
        log.debug('entry', key=key)

        if key is None or len(key) == 0:
            raise ValueError("key-not-provided: '{}'".format(key))

        def success(results, k):
            log.debug('delete-success', results=results, key=k)
            if results:
                return results
            return False

        def failure(reason):
            if isinstance(reason, Failure) and issubclass(type(reason.value), CancelledError):
                log.debug('delete-cancelled')
            else:
                log.info('delete-failure', error=reason, key=key)
            return reason

        deferred = threads.deferToThread(self._etcd.delete, self.make_path(key))
        deferred.addCallback(success, key)
        deferred.addErrback(failure)
        return deferred

    def delete_prefix(self, prefix):
        log.debug('entry', prefix=prefix)

        def success(results, k):
            log.debug('delete-prefix-success', results=results, prefix=k)
            if results:
                return results
            return False

        def failure(reason):
            if isinstance(reason, Failure) and issubclass(type(reason.value), CancelledError):
                log.debug('delete-prefix-cancelled')
            else:
                log.info('delete-prefix-failure', error=reason, prefix=prefix)
            return reason

        if prefix is not None and len(prefix) > 0:
            prefix = self.make_path(prefix)
        else:
            # Deleting a range of keys with a prefix
            prefix = self._path_prefix
            while prefix[-1:] == '/':
                prefix = prefix[:-1]

        deferred = threads.deferToThread(self._etcd.delete_prefix, prefix)
        deferred.addCallback(success, prefix)
        deferred.addErrback(failure)
        return deferred
