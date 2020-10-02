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

from pyvoltha_min.adapters.extensions.events.adapter_events import DeviceEventBase


class OltIndicationEvent(DeviceEventBase):
    def __init__(self, event_mgr, oper_status, raised_ts):
        super().__init__(event_mgr, raised_ts, object_type='olt',
                         event='OLT_DOWN_INDICATION',
                         category=EventCategory.COMMUNICATION,
                         sub_category=EventSubCategory.OLT)
        self._oper_status = oper_status

    def get_context_data(self):
        return {'oper-state': self._oper_status}
