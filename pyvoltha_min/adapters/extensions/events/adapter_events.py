#
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

from enum import IntEnum
import structlog
from twisted.internet.defer import inlineCallbacks
from voltha_protos.events_pb2 import Event, EventType, EventCategory, EventSubCategory, DeviceEvent, EventHeader

log = structlog.get_logger()

# TODO: In the device adapter, the following events are still TBD
#       (Taken from openolt_events)
# onu_alarm_ind
# onu_startup_failure_indication
# onu_signal_degrade_indication
# onu_drift_of_window_ind
# onu_loss_omci_ind
# onu_signals_fail_ind
# onu_tiwi_ind
# onu_activation_fail_ind
# onu_processing_error_ind


class AdapterEventStatus(IntEnum):
    CLEAR_EVENT = 0             # Alarm clearing
    RAISE_EVENT = 1             # Alarm going active
    EVENT = 2                   # Notification/on-shot event


class AdapterEvents:
    """
    Class for managing Events within a given Device Handler instance
    """
    def __init__(self, core_proxy, device_id, logical_device_id, serial_number):
        """
        Adapter event manager initializer

        :param core_proxy: (CoreProxy) Core proxy reference
        :param device_id: (str) Device handler's unique device id
        :param logical_device_id: (str) Logical Device that the device is a member of
        :param serial_number: (str) Serial number of the device(OLT) that created this instance
        """
        self.type_version = "0.1"
        self.device_id = device_id
        self.core_proxy = core_proxy
        self.serial_number = serial_number
        self.logical_device_id = logical_device_id
        self.adapter_name = core_proxy.listening_topic
        self.log = structlog.get_logger(device_id=device_id)

    def format_id(self, event):
        """
        Format the Unique Event ID for this event.  This is provided in the events
        'id' field

        :param event: (str) The name of the event such as 'Discover' or 'LOS'

        :return: (str) Event ID
        """
        return 'voltha.{}.{}.{}'.format(self.adapter_name,
                                        self.device_id, event)

    def get_event_header(self, _type, category, sub_category, event, raised_ts):
        """
        :return: (dict) Event header
        """
        hdr = EventHeader(id=self.format_id(event),
                          category=category,
                          sub_category=sub_category,
                          type=_type,
                          type_version=self.type_version)
        hdr.raised_ts.FromSeconds(raised_ts)
        hdr.reported_ts.GetCurrentTime()
        return hdr

    @inlineCallbacks
    def send_event(self, event_header, event_body):
        """
        Send the event to the event bus

        :param event_header: (dict) Event specific context data
        :param event_body: (dict) Common Event information dictionary
        """
        event = None
        try:
            self.log.debug('send_event')

            if event_header.type == EventType.DEVICE_EVENT:
                event = Event(header=event_header, device_event=event_body)
            elif event_header.type == EventType.KPI_EVENT:
                event = Event(header=event_header, kpi_event=event_body)
            elif event_header.type == EventType.KPI_EVENT2:
                event = Event(header=event_header, kpi_event2=event_body)
            elif event_header.type == EventType.CONFIG_EVENT:
                event = Event(header=event_header, config_event=event_body)

            if event is not None:
                yield self.core_proxy.submit_event(event)

        except Exception as e:
            self.log.exception('failed-to-send-event', e=e)
            raise
        log.debug('event-sent-to-kafka', event_type=event_header.type)

    @staticmethod
    def event_from_dict(event, event_mgr):
        """ Deserialize from a dictionary """
        # pylint: disable=import-outside-toplevel
        # NOTE: Autoclear events are not serialized
        from .device_events.olt.olt_indication_event import OltIndicationEvent
        from .device_events.olt.olt_los_event import OltLosEvent
        from .device_events.olt.olt_pon_interface_event import OltPonInterfaceDownEvent

        decoder = {
            OltIndicationEvent.event_name: OltIndicationEvent.from_dict,
            OltLosEvent.event_name: OltLosEvent.from_dict,
            OltPonInterfaceDownEvent.event_name: OltPonInterfaceDownEvent.from_dict,
        }.get(event['object_type'], lambda _: None)

        return decoder(event, event_mgr)


class DeviceEventBase:
    """Base class for device events"""
    event_name = 'base_class'

    # pylint: disable=too-many-arguments
    def __init__(self, event_mgr, raised_ts, object_type,
                 event, resource_id=None,
                 category=EventCategory.EQUIPMENT,
                 sub_category=EventSubCategory.PON):
        """
        Initializer for the Event base class

        :param event_mgr: (AdapterEvents) Reference to the device handler's Adapter
                                          Event manager
        :param object_type: (str) Type of device generating the event such as 'olt' or 'onu'
        :param event: (str) A textual name for the event such as 'HeartBeat' or 'Discovery'
        :param event_category: (EventCategory) Refers to functional category of
                                                    the event
        :param event_category: (EventSubCategory) Refers to functional sub category of
                                                    the event
        :param resource_id: (str) Identifier of the originating resource of the event
        """
        self.event_mgr = event_mgr
        self._object_type = object_type
        self._event = event
        self._category = category
        self._sub_category = sub_category
        self._type = EventType.DEVICE_EVENT
        self._resource_id = resource_id
        self.raised_ts = raised_ts

    @staticmethod
    def _format_description(object_type, device_event, status):
        """
        Format the textual description field of this event

        :param object_type: (str) Better textual description of the event type
        :param device_event: (str) The name of the event such as 'Discover' or 'LOS'
        :param status: (AdapterEventStatus) Status/type of event

        :return: (str) Event description
        """
        state = {
            AdapterEventStatus.CLEAR_EVENT: 'Cleared',
            AdapterEventStatus.RAISE_EVENT: 'Raised',
            AdapterEventStatus.EVENT: 'Event'
        }.get(status)

        if state is None:
            raise TypeError('status should be an AdapterEventStatus enumeration:{}'.format(status))

        return '{} Event - {} - {}'.format(object_type.upper(), device_event.upper(), state)

    def _get_device_event_data(self, status):
        """
        Get the event specific data and format it into a dictionary.  When the event
        is being sent to the event bus, this dictionary provides a majority of the
        fields for the events.

        :param status: (AdapterEventStatus) Type/state of event
        :return: (dict) Event data
        """
        context_data = self.get_context_data()

        current_context = {}
        if isinstance(context_data, dict):
            for key, value in context_data.items():
                current_context[key] = str(value)
        # Always insert serial number of the OLT, ONU serial number comes in the context
        current_context["serial-number"] = self.event_mgr.serial_number

        return DeviceEvent(resource_id=self.event_mgr.device_id,
                           device_event_name="{}_{}".format(self._event, status.name),
                           description=self._format_description(self._object_type, self._event, status),
                           context=current_context)

    def get_context_data(self):  # pylint: disable=no-self-use
        """
        Get event specific context data. If an event has specific data to specify, it is
        included in the context field in the published event

        :return: (dict) Dictionary (str -> str) with event specific context data
        """
        return {}   # NOTE: You should override this if needed

    def send(self, status):
        """
        Called to send a device event to the event bus
        """
        event_header = self.event_mgr.get_event_header(EventType.DEVICE_EVENT, self._category,
                                                       self._sub_category, self._event, self.raised_ts)
        device_event_data = self._get_device_event_data(status)
        self.event_mgr.send_event(event_header, device_event_data)

    def to_dict(self):
        """ Serialize to a dictionary """
        return {
            'event': self._event,
            'raised_ts': self.raised_ts,
        }
