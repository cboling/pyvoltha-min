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
from twisted.internet import threads

log = structlog.get_logger()


class TwistedEtcdStore:

    def __init__(self, host, port, path_prefix):
        self._etcd = etcd3.client(host=host, port=port)
        self.host = host
        self.port = port
        self._path_prefix = path_prefix

    def make_path(self, key):
        return '{}/{}'.format(self._path_prefix, key)

    def get(self, key):

        def success(results):
            (value, _meta) = results
            return value

        def failure(exception):
            raise exception

        deferred = threads.deferToThread(self._etcd.get, self.make_path(key))
        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def set(self, key, value):

        def success(results):
            if results:
                return results
            return False

        def failure(exception):
            raise exception

        deferred = threads.deferToThread(self._etcd.put, self.make_path(key), value)
        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def list(self, key,  keys_only=False):

        def success(results):
            if results:
                return results
            return False

        def failure(exception):
            raise exception

        deferred = threads.deferToThread(self._etcd.get_prefix, self.make_path(key), keys_only=keys_only)
        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def watch(self, key, callback):

        def success(results):
            return results

        def failure(exception):
            raise exception

        deferred = threads.deferToThread(self._etcd.add_watch_callback, self.make_path(key), callback)
        deferred.addCallback(success)
        deferred.addErrback(failure)
        return deferred

    def delete(self, key):
        log.info('entry', key=key)

        def success(results, k):
            log.info('delete-success', results=results, key=k)
            if results:
                return results
            return False

        def failure(exception):
            raise exception

        deferred = threads.deferToThread(self._etcd.delete, self.make_path(key))
        deferred.addCallback(success, key)
        deferred.addErrback(failure)
        return deferred

    def delete_prefix(self, prefix):
        log.info('entry', prefix=prefix)

        def success(results, k):
            log.info('delete-prefix-success', results=results, prefix=k)
            if results:
                return results
            return False

        def failure(exception):
            raise exception

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
