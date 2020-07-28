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

"""
    message OnuStartupFailureIndication {
    fixed32 intf_id = 1;
    fixed32 onu_id = 2;
    string status = 3;
}

"""

class OnuStartupEvent(DeviceEventBase):

    def __init__(self, event_mgr, onu_id, intf_id, serial_number, raised_ts):
        super(OnuStartupEvent, self).__init__(event_mgr, raised_ts, object_type='onu STARTUP FAIL',
                                              event='ONU_STARTUP_FAIL',
                                              category=EventCategory.COMMUNICATION,
                                              sub_category=EventSubCategory.ONU)

        self._onu_id = onu_id
        self._intf_id = intf_id
        self._serial_number = serial_number

    def get_context_data(self):
        return {'onu-id': self._onu_id,
                'onu-intf-id': self._intf_id,
                'onu-serial-number': self._serial_number}
