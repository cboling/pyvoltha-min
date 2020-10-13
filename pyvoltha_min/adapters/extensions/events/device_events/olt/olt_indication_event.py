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


class OltIndicationEvent(DeviceEventBase):
    event_name = 'OLT_DOWN_INDICATION'

    def __init__(self, event_mgr, oper_status, raised_ts):
        super().__init__(event_mgr, raised_ts, object_type='OLT Down Indication',
                         event=self.event_name,
                         category=EventCategory.COMMUNICATION,
                         sub_category=EventSubCategory.OLT)

        self._oper_status = 'up' if oper_status == OperStatus.ACTIVE else 'down'

    def get_context_data(self):
        return {'oper-state': self._oper_status}

    def to_dict(self):
        """ Serialize to a dictionary """
        data = super().to_dict()
        data['oper_status'] = self._oper_status
        return data

    @staticmethod
    def from_dict(data, event_mgr):
        return OltIndicationEvent(event_mgr, data['oper_status'], data['raised_ts'])
