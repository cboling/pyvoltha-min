# Copyright 2017 the original author or authors.
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
import codecs

import etcd3
import time
import structlog
from twisted.internet.defer import inlineCallbacks

from pyvoltha_min.common.utils.asleep import asleep
from pyvoltha_min.common.utils.tracing import traced_function


class EtcdStore:
    """ Config kv store for etcd with a cache for quicker subsequent reads

        TODO: This will block the reactor. Should either change
        whole call stack to yield or put the put/delete transactions into a
        queue to write later with twisted. Will need a transaction
        log to ensure we don't lose anything.
        Making the whole callstack yield is troublesome because other tasks can
        come in on the side and start modifying things which could be bad.
    """

    RETRY_BACKOFF = [0.05, 0.1, 0.2, 0.5, 1, 2, 5]

    def __init__(self, host, port, path_prefix):

        self.log = structlog.get_logger()
        self._etcd = etcd3.client(host=host, port=port)
        self.host = host
        self.port = port
        self._path_prefix = path_prefix
        self.retries = 0

    def make_path(self, key):
        return '{}/{}'.format(self._path_prefix, key)

    def __getitem__(self, key):
        (value, _meta) = self._kv_get(self.make_path(key))
        if value is not None:
            return value
        raise KeyError(key)

    def __contains__(self, key):
        (value, _meta) = self._kv_get(self.make_path(key))
        if value is not None:
            return True
        return False

    def __setitem__(self, key, value):
        try:
            self._kv_put(self.make_path(key), value)
        except Exception as e:
            self.log.exception('cannot-set-item', e=e)

    def __delitem__(self, key):
        self._kv_delete(self.make_path(key))

    def list(self, key):
        return self._kv_list(self.make_path(key))

    @inlineCallbacks
    def _backoff(self, msg):
        wait_time = self.RETRY_BACKOFF[min(self.retries,
                                           len(self.RETRY_BACKOFF) - 1)]
        self.retries += 1
        self.log.error(msg, retry_in=wait_time)
        yield asleep(wait_time)

    def _redo_etcd_connection(self):
        self._etcd = etcd3.client(host=self.host, port=self.port)

    def _clear_backoff(self):
        if self.retries:
            self.log.info('reconnected-to-etcd', after_retries=self.retries)
            self.retries = 0

    def _get_etcd(self):
        return self._etcd

    # Proxy methods for etcd with retry support
    @traced_function(name='etd-blocking-get')
    def _kv_get(self, *args, **kw):
        return self._retry('GET', *args, **kw)

    @traced_function(name='etd-blocking-put')
    def _kv_put(self, *args, **kw):
        return self._retry('PUT', *args, **kw)

    @traced_function(name='etd-blocking-delete')
    def _kv_delete(self, *args, **kw):
        return self._retry('DELETE', *args, **kw)

    @traced_function(name='etd-blocking-list')
    def _kv_list(self, *args, **kw):
        return self._retry('LIST', *args, **kw)

    def _retry(self, operation, *args, **kw):
        # etcd data sometimes contains non-utf8 sequences, replace
        # self.log.debug('backend-op',
        #                operation=operation,
        #                args=[codecs.encode(x, 'utf8', 'replace') for x in args],
        #                kw=kw)
        retries = 0
        start = time.monotonic()
        while 1:
            try:
                etcd = self._get_etcd()
                self.log.debug('etcd', etcd=etcd, operation=operation,
                               args=[codecs.encode(x, 'utf8', 'replace') for x in args])
                if operation == 'GET':
                    (value, meta) = etcd.get(*args, **kw)
                    result = (value, meta)
                elif operation == 'PUT':
                    result = etcd.put(*args, **kw)
                elif operation == 'DELETE':
                    result = etcd.delete(*args, **kw)
                elif operation == 'LIST':
                    result = etcd.get_prefix(*args, *kw)
                else:
                    # Default case - consider operation as a function call
                    result = operation(*args, **kw)
                self._clear_backoff()
                break
            except Exception as e:
                self.log.exception(e)
                self._backoff('unknown-error-with-etcd')
            self._redo_etcd_connection()
            retries += 1

        now = time.monotonic()
        self.log.info(operation, delta=now-start, retries=retries)
        return result
