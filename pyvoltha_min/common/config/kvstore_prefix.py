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
import os

KV_STORE_PREFIX_DEFAULT = 'service/voltha'


class KvStore:      # pylint: disable=too-few-public-methods
    """
    prefix for voltha-stack kv-store.  If you need to allow other alternative
    ways to modify it, do so as early as possible in the main startup
    """
    prefix = None


if KvStore.prefix is None:
    KvStore.prefix = os.getenv('KV_STORE_DATAPATH_PREFIX', KV_STORE_PREFIX_DEFAULT)
