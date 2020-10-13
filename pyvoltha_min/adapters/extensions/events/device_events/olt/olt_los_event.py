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

from pyvoltha_min.adapters.extensions.events.adapter_events import DeviceEventBase


class OltLosEvent(DeviceEventBase):
    event_name = 'OLT_LOSS_OF_SIGNAL'

    def __init__(self, event_mgr, intf_id, raised_ts):
        super().__init__(event_mgr, raised_ts, object_type='OLT LOS',
                         event=self.event_name,
                         category=EventCategory.COMMUNICATION,
                         sub_category=EventSubCategory.OLT)
        # Added port type to indicate if alarm was on NNI or PON
        self._intf_id = intf_id

    def get_context_data(self):
        return {'intf-id:': str(self._intf_id)}

    def to_dict(self):
        """ Serialize to a dictionary """
        data = super().to_dict()
        data['intf_id'] = self._intf_id
        return data

    @staticmethod
    def from_dict(data, event_mgr):
        return OltLosEvent(event_mgr, data['intf_id'], data['raised_ts'])
