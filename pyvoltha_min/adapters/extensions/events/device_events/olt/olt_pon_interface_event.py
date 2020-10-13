# --------------------------------------------------------------------------#
# Copyright (C) 2015 - Present by Tibit Communications, Inc.                #
# All rights reserved.                                                      #
#                                                                           #
#    _______ ____  _ ______                                                 #
#   /_  __(_) __ )(_)_  __/                                                 #
#    / / / / __  / / / /                                                    #
#   / / / / /_/ / / / /                                                     #
#  /_/ /_/_____/_/ /_/                                                      #
#                                                                           #
# --------------------------------------------------------------------------#

from voltha_protos.events_pb2 import EventCategory, EventSubCategory
from voltha_protos.common_pb2 import OperStatus

from pyvoltha_min.adapters.extensions.events.adapter_events import DeviceEventBase


class OltPonInterfaceDownEvent(DeviceEventBase):
    event_name = 'OLT_PON_INTERFACE_DOWN'

    def __init__(self, event_mgr, intf_id, oper_status, raised_ts):
        super().__init__(event_mgr, raised_ts, object_type='OLT Pon Interface Down',
                         event=self.event_name,
                         category=EventCategory.COMMUNICATION,
                         sub_category=EventSubCategory.OLT)
        # Added port type to indicate if alarm was on NNI or PON
        self._intf_id = str(intf_id)
        self._oper_status = 'up' if oper_status == OperStatus.ACTIVE else 'down'

    def get_context_data(self):
        """ Strangly enough, the PON ID is not encoded into this message in the OpenOLT """
        return {'intf-id:': str(self._intf_id),
                'oper-state': self._oper_status}

    def to_dict(self):
        """ Serialize to a dictionary """
        data = super().to_dict()
        data['int_id'] = self._intf_id
        data['oper_status'] = self._oper_status
        return data

    @staticmethod
    def from_dict(data, event_mgr):
        return OltPonInterfaceDownEvent(event_mgr, data['int_id'],
                                        data['oper_status'], data['raised_ts'])
