# Copyright 2017-present Adtran, Inc.
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
from voltha_protos.events_pb2 import EventCategory, EventSubCategory

from ...adapter_events import DeviceEventBase


class OnuDyingGaspEvent(DeviceEventBase):
    def __init__(self, event_mgr, onu_id, intf_id, raised_ts):
        super(OnuDyingGaspEvent, self).__init__(event_mgr, raised_ts, object_type='ONU Dying Gasp',
                                                event='ONU_DYING_GASP',
                                                category=EventCategory.COMMUNICATION,
                                                sub_category=EventSubCategory.PON)
        self._onu_id = onu_id
        self._intf_id = intf_id

    def get_context_data(self):
        return {'onu-id': str(self._onu_id),
                'intf-id': str(self._intf_id)}
