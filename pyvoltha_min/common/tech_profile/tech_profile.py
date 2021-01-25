#
# Copyright 2018 the original author or authors.
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
#
# pylint: disable=too-few-public-methods, too-many-arguments, import-error

import re
import json
from collections import namedtuple
from enum import Enum

import structlog
from voltha_protos import openolt_pb2

from pyvoltha_min.common.config.config_backend import EtcdStore
from pyvoltha_min.common.utils.registry import registry
from pyvoltha_min.common.config.kvstore_prefix import KvStore

# logger
log = structlog.get_logger()

DEFAULT_TECH_PROFILE_TABLE_ID = 64

XGS_PON = 'XGS-PON'
GPON = 'GPON'
EPON = 'EPON'

# Enums used while creating TechProfileInstance
Direction = Enum('Direction', ['UPSTREAM', 'DOWNSTREAM', 'BIDIRECTIONAL'],
                 start=0)

SchedulingPolicy = Enum('SchedulingPolicy',
                        ['WRR', 'StrictPriority', 'Hybrid'], start=0)

AdditionalBW = Enum('AdditionalBW', ['None', 'NA', 'BestEffort', 'Auto'],
                    start=0)

DiscardPolicy = Enum('DiscardPolicy',
                     ['TailDrop', 'WTailDrop', 'Red', 'WRed'], start=0)

InferredAdditionBWIndication = Enum('InferredAdditionBWIndication',
                                    ['None', 'NoneAssured', 'BestEffort'],
                                    start=0)


class InstanceControl:
    # Default value constants
    ONU_DEFAULT_INSTANCE = 'multi-instance'
    UNI_DEFAULT_INSTANCE = 'single-instance'
    DEFAULT_NUM_GEM_PORTS = 1
    DEFAULT_GEM_PAYLOAD_SIZE = 'auto'

    def __init__(self, onu=ONU_DEFAULT_INSTANCE,
                 uni=UNI_DEFAULT_INSTANCE,
                 num_gem_ports=DEFAULT_NUM_GEM_PORTS,
                 max_gem_payload_size=DEFAULT_GEM_PAYLOAD_SIZE):
        self.onu = onu
        self.uni = uni
        self._num_gem_ports = num_gem_ports
        self.max_gem_payload_size = max_gem_payload_size


class Scheduler:
    # Default value constants
    DEFAULT_ADDITIONAL_BW = 'BestEffort'
    DEFAULT_PRIORITY = 0
    DEFAULT_WEIGHT = 0
    DEFAULT_Q_SCHED_POLICY = 'Hybrid'

    def __init__(self, direction, additional_bw=DEFAULT_ADDITIONAL_BW,
                 priority=DEFAULT_PRIORITY,
                 weight=DEFAULT_WEIGHT,
                 q_sched_policy=DEFAULT_Q_SCHED_POLICY):
        self.direction = direction
        self.additional_bw = additional_bw
        self.priority = priority
        self.weight = weight
        self.q_sched_policy = q_sched_policy


class GemPortAttribute:
    # Default value constants
    DEFAULT_AES_ENCRYPTION = 'True'
    DEFAULT_PRIORITY_Q = 0
    DEFAULT_WEIGHT = 0
    DEFAULT_MAX_Q_SIZE = 'auto'
    DEFAULT_DISCARD_POLICY = DiscardPolicy.TailDrop.name
    DEFAULT_SCHEDULING_POLICY = SchedulingPolicy.WRR.name
    DEFAULT_DYNAMIC_ACL = '224.0.0.0-239.255.255.255'
    DEFAULT_STATIC_ACL = DEFAULT_DYNAMIC_ACL
    DEFAULT_MULTICAST_GEM_ID = 4096

    def __init__(self, pbit_map, discard_config,
                 aes_encryption=DEFAULT_AES_ENCRYPTION,
                 scheduling_policy=DEFAULT_SCHEDULING_POLICY,
                 priority_q=DEFAULT_PRIORITY_Q,
                 weight=DEFAULT_WEIGHT,
                 max_q_size=DEFAULT_MAX_Q_SIZE,
                 discard_policy=DiscardPolicy.TailDrop.name,
                 is_multicast=False,
                 dynamic_access_control_list=DEFAULT_DYNAMIC_ACL,
                 static_access_control_list=DEFAULT_STATIC_ACL,
                 multicast_gem_id=DEFAULT_MULTICAST_GEM_ID):
        self.max_q_size = max_q_size
        self.pbit_map = pbit_map
        self.aes_encryption = aes_encryption
        self.scheduling_policy = scheduling_policy
        self.priority_q = priority_q
        self.weight = weight
        self.discard_policy = discard_policy
        self.discard_config = discard_config
        self.is_multicast = is_multicast
        self.dynamic_access_control_list = dynamic_access_control_list
        self.static_access_control_list = static_access_control_list
        self.multicast_gem_id = multicast_gem_id


class DiscardConfig:
    # Default value constants
    DEFAULT_MIN_THRESHOLD = 0
    DEFAULT_MAX_THRESHOLD = 0
    DEFAULT_MAX_PROBABILITY = 0

    def __init__(self, min_threshold=DEFAULT_MIN_THRESHOLD,
                 max_threshold=DEFAULT_MAX_THRESHOLD,
                 max_probability=DEFAULT_MAX_PROBABILITY):
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.max_probability = max_probability


# TODO: Defaults (constants) needed for EPON here
class QThresholds:      # for EPON
    def __init__(self, q_threshold1=5500,
                 q_threshold2=0,
                 q_threshold3=0,
                 q_threshold4=0,
                 q_threshold5=0,
                 q_threshold6=0,
                 q_threshold7=0):
        self.q_threshold1 = q_threshold1
        self.q_threshold2 = q_threshold2
        self.q_threshold3 = q_threshold3
        self.q_threshold4 = q_threshold4
        self.q_threshold5 = q_threshold5
        self.q_threshold6 = q_threshold6
        self.q_threshold7 = q_threshold7


# TODO: Defaults needed for EPON here
class UpstreamQueueAttribute:      # for EPON
    def __init__(self, max_q_size,
                 pbit_map,
                 aes_encryption,
                 traffic_type="BE",
                 unsolicited_grant_size=0,
                 nominal_interval=0,
                 tolerated_poll_jitter=0,
                 request_transmission_policy=0,
                 num_q_sets=2,
                 q_thresholds=None,
                 scheduling_policy=None,    # TODO: Not yet defined
                 priority_q=None,           # TODO: Not yet defined
                 weight=None,               # TODO: Not yet defined
                 discard_policy=None,       # TODO: Not yet defined
                 discard_config=None):      # TODO: Not yet defined
        self.max_q_size = max_q_size
        self.pbit_map = pbit_map
        self.aes_encryption = aes_encryption
        self.traffic_type = traffic_type
        self.unsolicited_grant_size = unsolicited_grant_size
        self.nominal_interval = nominal_interval
        self.tolerated_poll_jitter = tolerated_poll_jitter
        self.request_transmission_policy = request_transmission_policy
        self.num_q_sets = num_q_sets
        self.q_thresholds = q_thresholds or QThresholds()
        self.scheduling_policy = scheduling_policy
        self.priority_q = priority_q
        self.weight = weight
        self.discard_policy = discard_policy
        self.discard_config = discard_config


# TODO: Defaults needed for EPON here
class DownstreamQueueAttribute:      # for EPON
    def __init__(self, max_q_size,      # TODO: Not yet defined
                 pbit_map,              # TODO: Not yet defined
                 aes_encryption,        # TODO: Not yet defined
                 scheduling_policy,     # TODO: Not yet defined
                 priority_q,            # TODO: Not yet defined
                 weight,                # TODO: Not yet defined
                 discard_policy,        # TODO: Not yet defined
                 discard_config):       # TODO: Not yet defined
        self.max_q_size = max_q_size
        self.pbit_map = pbit_map
        self.aes_encryption = aes_encryption
        self.scheduling_policy = scheduling_policy
        self.priority_q = priority_q
        self.weight = weight
        self.discard_policy = discard_policy
        self.discard_config = discard_config


class TechProfile:
    # Constants used in default tech profile
    DEFAULT_TECH_PROFILE_NAME = 'Default_1tcont_1gem_Profile'
    DEFAULT_VERSION = 1.0
    DEFAULT_GEMPORTS_COUNT = 1
    pbits = ['0b11111111']

    # Tech profile path prefix in kv store
    kv_store_prefix = KvStore.prefix
    KV_STORE_TECH_PROFILE_PATH_PREFIX = '%s/technology_profiles' % kv_store_prefix

    # Tech profile path in kv store
    TECH_PROFILE_PATH = '{}/{}'  # <technology>/<table_id>

    # Tech profile instance path in kv store
    # Format: <technology>/<table_id>/<uni_port_name>
    TECH_PROFILE_INSTANCE_PATH = '{}/{}/{}'

    # Tech-Profile JSON String Keys
    NAME = 'name'
    PROFILE_TYPE = 'profile_type'
    VERSION = 'version'
    NUM_GEM_PORTS = 'num_gem_ports'
    INSTANCE_CONTROL = 'instance_control'
    US_SCHEDULER = 'us_scheduler'
    DS_SCHEDULER = 'ds_scheduler'
    UPSTREAM_GEM_PORT_ATTRIBUTE_LIST = 'upstream_gem_port_attribute_list'
    DOWNSTREAM_GEM_PORT_ATTRIBUTE_LIST = 'downstream_gem_port_attribute_list'
    ONU = 'onu'
    UNI = 'uni'
    MAX_GEM_PAYLOAD_SIZE = 'max_gem_payload_size'
    DIRECTION = 'direction'
    ADDITIONAL_BW = 'additional_bw'
    PRIORITY = 'priority'
    Q_SCHED_POLICY = 'q_sched_policy'
    WEIGHT = 'weight'
    PBIT_MAP = 'pbit_map'
    DISCARD_CONFIG = 'discard_config'
    MAX_THRESHOLD = 'max_threshold'
    MIN_THRESHOLD = 'min_threshold'
    MAX_PROBABILITY = 'max_probability'
    DISCARD_POLICY = 'discard_policy'
    PRIORITY_Q = 'priority_q'
    SCHEDULING_POLICY = 'scheduling_policy'
    MAX_Q_SIZE = 'max_q_size'
    AES_ENCRYPTION = 'aes_encryption'

    def __init__(self, resource_mgr):
        self.args = registry('main').get_args()
        self.resource_mgr = resource_mgr

        # KV store's IP Address and PORT
        host, port = self.args.kv_store_address.split(':', 1)
        self._kv_store = EtcdStore(
            host, port, TechProfile.
            KV_STORE_TECH_PROFILE_PATH_PREFIX)

    @property
    def kv_store(self):
        return self._kv_store

    class DefaultTechProfile:
        def __init__(self, name, **kwargs):
            self.name = name
            self.profile_type = kwargs[TechProfile.PROFILE_TYPE]
            self.version = kwargs[TechProfile.VERSION]
            self.num_gem_ports = kwargs[TechProfile.NUM_GEM_PORTS]
            self.instance_control = kwargs[TechProfile.INSTANCE_CONTROL]
            self.us_scheduler = kwargs[TechProfile.US_SCHEDULER]
            self.ds_scheduler = kwargs[TechProfile.DS_SCHEDULER]
            self.upstream_gem_port_attribute_list = kwargs[
                TechProfile.UPSTREAM_GEM_PORT_ATTRIBUTE_LIST]
            self.downstream_gem_port_attribute_list = kwargs[
                TechProfile.DOWNSTREAM_GEM_PORT_ATTRIBUTE_LIST]

        def to_json(self):
            return json.dumps(self, default=lambda o: o.__dict__,
                              indent=2)

    def get_tp_path(self, table_id, uni_port_name):
        path = TechProfile.TECH_PROFILE_INSTANCE_PATH.format(
            self.resource_mgr.technology, table_id, uni_port_name)
        log.debug("constructed-tp-path", table_id=table_id, technology=self.resource_mgr.technology,
                  uni_port_name=uni_port_name, path=path)
        return path

    @staticmethod
    def _tp_load_hook(d):
        t_f = ('true', 'false')
        results = namedtuple('tech_profile_instance',
                             list(d.keys()))(*list(val if not isinstance(val, str) or val.lower() not in t_f
                                                   else val.lower() == 'true' for val in d.values()))
        return results

    def load_tech_profile_instance(self, tp_id, uni_port_name, _intf_id):
        """ Load TechProfileInstance from kv-store """
        # path to fetch tech profile instance json from kv store
        path = TechProfile.TECH_PROFILE_INSTANCE_PATH.format(self.resource_mgr.technology,
                                                             tp_id, uni_port_name)
        try:
            tp_json = self._kv_store[path]
            log.debug("Tech-profile-instance-present-in-kvstore", path=path,
                      tech_profile_instance=tp_json)

            # Parse JSON into an object with attributes corresponding to dict keys
            # and convert strings with 'True' or 'False' to booleans
            tp_instance = json.loads(tp_json, object_hook=self._tp_load_hook)

            tech_profile_instance = TechProfileInstance.restore(tp_instance, uni_port_name)
            return tech_profile_instance

        except KeyError:
            return None

        except Exception as e:
            log.exception('tech-profile-load-error', e=e)
            return None

    def delete_tech_profile_instance(self, tp_path):
        try:
            del self._kv_store[tp_path]
            log.debug("Delete-tech-profile-instance-success", path=tp_path)
            return True

        except Exception as e:
            log.debug("Delete-tech-profile-instance-failed", path=tp_path,
                      exception=e)
            return False

    def load_tech_profile(self, table_id):
        """
        Get tech profile from kv store.  The returned value is a simple
        dictionary.  It can be used as a parameter to the _create_from_dict()
        static method to be converted back into an object.

        :param table_id: reference to get tech profile
        :return: (dict) tech profile if present in kv store else None
        """
        # get tech profile from kv store
        path = TechProfile.TECH_PROFILE_PATH.format(self.resource_mgr.technology,
                                                    table_id)
        try:
            tech_profile = self._kv_store[path]
            if tech_profile != '':
                log.debug("get-tp-success", tech_profile=tech_profile)
                return json.loads(tech_profile)

        except KeyError as e:
            log.debug('tp-not-found', path=path)

        except Exception as e:
            log.exception('get-tp-failed', exception=e, path=path)

        return None

    def _default_tech_profile(self):
        """
        A default tech profile is used when a TechProfileInstance needs to be created but
        the technology profile ID was not found in the kv-store.  It provides a very minimal
        single-tcont/gem-port service
        """
        upstream_gem_port_attribute_list = list()
        downstream_gem_port_attribute_list = list()
        for pbit in TechProfile.pbits:
            upstream_gem_port_attribute_list.append(
                GemPortAttribute(pbit_map=pbit,
                                 discard_config=DiscardConfig()))
            downstream_gem_port_attribute_list.append(
                GemPortAttribute(pbit_map=pbit,
                                 discard_config=DiscardConfig()))

        return TechProfile.DefaultTechProfile(
            TechProfile.DEFAULT_TECH_PROFILE_NAME,
            profile_type=self.resource_mgr.technology,
            version=TechProfile.DEFAULT_VERSION,
            num_gem_ports=TechProfile.DEFAULT_GEMPORTS_COUNT,
            instance_control=InstanceControl(),
            us_scheduler=Scheduler(direction=Direction.UPSTREAM.name),
            ds_scheduler=Scheduler(direction=Direction.DOWNSTREAM.name),
            upstream_gem_port_attribute_list=upstream_gem_port_attribute_list,
            downstream_gem_port_attribute_list=downstream_gem_port_attribute_list)

    @staticmethod
    def create(tech_profile):
        """
        Create TechProfile() from dictionary object

        This is often use to unmarshal saved JSON from the kv-store for a tech profile.
        """
        if not isinstance(tech_profile, dict):
            raise TypeError('A dictionary was expected, received a {}'.format(type(tech_profile)))

        # Tech profile fetched from kv store
        instance_control = tech_profile[TechProfile.INSTANCE_CONTROL]
        instance_control = InstanceControl(
            onu=instance_control[TechProfile.ONU],
            uni=instance_control[TechProfile.UNI],
            max_gem_payload_size=instance_control[
                TechProfile.MAX_GEM_PAYLOAD_SIZE])

        us_scheduler = tech_profile[TechProfile.US_SCHEDULER]
        us_scheduler = Scheduler(direction=us_scheduler[TechProfile.DIRECTION],
                                 additional_bw=us_scheduler[
                                     TechProfile.ADDITIONAL_BW],
                                 priority=us_scheduler[TechProfile.PRIORITY],
                                 weight=us_scheduler[TechProfile.WEIGHT],
                                 q_sched_policy=us_scheduler[
                                     TechProfile.Q_SCHED_POLICY])
        ds_scheduler = tech_profile[TechProfile.DS_SCHEDULER]
        ds_scheduler = Scheduler(direction=ds_scheduler[TechProfile.DIRECTION],
                                 additional_bw=ds_scheduler[
                                     TechProfile.ADDITIONAL_BW],
                                 priority=ds_scheduler[TechProfile.PRIORITY],
                                 weight=ds_scheduler[TechProfile.WEIGHT],
                                 q_sched_policy=ds_scheduler[
                                     TechProfile.Q_SCHED_POLICY])

        upstream_gem_port_attribute_list = list()
        downstream_gem_port_attribute_list = list()
        us_gemport_attr_list = tech_profile[TechProfile.UPSTREAM_GEM_PORT_ATTRIBUTE_LIST]

        for attr in us_gemport_attr_list:
            upstream_gem_port_attribute_list.append(
                GemPortAttribute(pbit_map=attr[TechProfile.PBIT_MAP],
                                 discard_config=DiscardConfig(
                                     max_threshold=attr[TechProfile.DISCARD_CONFIG][TechProfile.MAX_THRESHOLD],
                                     min_threshold=attr[TechProfile.DISCARD_CONFIG][TechProfile.MIN_THRESHOLD],
                                     max_probability=attr[TechProfile.DISCARD_CONFIG][TechProfile.MAX_PROBABILITY]),
                                 discard_policy=attr[TechProfile.DISCARD_POLICY],
                                 priority_q=attr[TechProfile.PRIORITY_Q],
                                 weight=attr[TechProfile.WEIGHT],
                                 scheduling_policy=attr[TechProfile.SCHEDULING_POLICY],
                                 max_q_size=attr[TechProfile.MAX_Q_SIZE],
                                 aes_encryption=attr[TechProfile.AES_ENCRYPTION]))

        ds_gemport_attr_list = tech_profile[TechProfile.DOWNSTREAM_GEM_PORT_ATTRIBUTE_LIST]

        for attr in ds_gemport_attr_list:
            downstream_gem_port_attribute_list.append(
                GemPortAttribute(pbit_map=attr[TechProfile.PBIT_MAP],
                                 discard_config=DiscardConfig(
                                     max_threshold=attr[TechProfile.DISCARD_CONFIG][TechProfile.MAX_THRESHOLD],
                                     min_threshold=attr[TechProfile.DISCARD_CONFIG][TechProfile.MIN_THRESHOLD],
                                     max_probability=attr[TechProfile.DISCARD_CONFIG][TechProfile.MAX_PROBABILITY]),
                                 discard_policy=attr[TechProfile.DISCARD_POLICY],
                                 priority_q=attr[TechProfile.PRIORITY_Q],
                                 weight=attr[TechProfile.WEIGHT],
                                 scheduling_policy=attr[TechProfile.SCHEDULING_POLICY],
                                 max_q_size=attr[TechProfile.MAX_Q_SIZE],
                                 aes_encryption=attr[TechProfile.AES_ENCRYPTION]))

        return TechProfile.DefaultTechProfile(
            tech_profile[TechProfile.NAME],
            profile_type=tech_profile[TechProfile.PROFILE_TYPE],
            version=tech_profile[TechProfile.VERSION],
            num_gem_ports=tech_profile[TechProfile.NUM_GEM_PORTS],
            instance_control=instance_control,
            us_scheduler=us_scheduler,
            ds_scheduler=ds_scheduler,
            upstream_gem_port_attribute_list=upstream_gem_port_attribute_list,
            downstream_gem_port_attribute_list=
            downstream_gem_port_attribute_list)

    def _add_tech_profile_instance(self, path, tech_profile_instance):
        """
        Add tech profile to kv store.

        :param path: path to add tech profile
        :param tech_profile_instance: (str) tech profile instance JSON need to be added
        """
        try:
            self._kv_store[path] = tech_profile_instance
            log.debug("success", path=path,
                      tech_profile_instance=tech_profile_instance)
            return True
        except BaseException as e:
            log.exception("failed", path=path,
                          tech_profile_instance=tech_profile_instance,
                          exception=e)
        return False

    @staticmethod
    def get_us_scheduler(tech_profile_instance):
        # upstream scheduler
        us_scheduler = openolt_pb2.Scheduler(
            direction=TechProfile.get_parameter(
                'direction', tech_profile_instance.us_scheduler.direction),
            additional_bw=TechProfile.get_parameter(
                'additional_bw', tech_profile_instance.us_scheduler.additional_bw),
            priority=tech_profile_instance.us_scheduler.priority,
            weight=tech_profile_instance.us_scheduler.weight,
            sched_policy=TechProfile.get_parameter(
                'sched_policy', tech_profile_instance.us_scheduler.q_sched_policy))

        return us_scheduler

    @staticmethod
    def get_ds_scheduler(tech_profile_instance):
        ds_scheduler = openolt_pb2.Scheduler(
            direction=TechProfile.get_parameter(
                'direction', tech_profile_instance.ds_scheduler.direction),
            additional_bw=TechProfile.get_parameter(
                'additional_bw', tech_profile_instance.ds_scheduler.additional_bw),
            priority=tech_profile_instance.ds_scheduler.priority,
            weight=tech_profile_instance.ds_scheduler.weight,
            sched_policy=TechProfile.get_parameter(
                'sched_policy', tech_profile_instance.ds_scheduler.q_sched_policy))

        return ds_scheduler

    @staticmethod
    def get_tconts(tech_profile_instance, us_scheduler=None, ds_scheduler=None):
        if us_scheduler is None:
            us_scheduler = TechProfile.get_us_scheduler(tech_profile_instance)
        if ds_scheduler is None:
            ds_scheduler = TechProfile.get_ds_scheduler(tech_profile_instance)

        raise Exception('TODO: Should not have downstream TCONTs')
        # TODO: Deprecate this method if never used.  Note downstream should not have TCONTs associated with it
        # tconts = [openolt_pb2.Tcont(direction=TechProfile.get_parameter(
        #     'direction',
        #     tech_profile_instance.us_scheduler.direction),
        #     alloc_id=tech_profile_instance.us_scheduler.alloc_id,
        #     scheduler=us_scheduler),
        #     openolt_pb2.Tcont(direction=TechProfile.get_parameter(
        #         'direction',
        #         tech_profile_instance.ds_scheduler.direction),
        #         alloc_id=tech_profile_instance.ds_scheduler.alloc_id,
        #         scheduler=ds_scheduler)]
        # return tconts

    @staticmethod
    def get_parameter(param_type, param_value):     # pylint: disable=too-many-branches
        parameter = None
        try:
            if param_type == 'direction':
                for direction in openolt_pb2.Direction.keys():
                    if param_value == direction:
                        parameter = direction
            elif param_type == 'discard_policy':
                for discard_policy in openolt_pb2.DiscardPolicy.keys():
                    if param_value == discard_policy:
                        parameter = discard_policy
            elif param_type == 'sched_policy':
                for sched_policy in openolt_pb2.SchedulingPolicy.keys():
                    if param_value == sched_policy:
                        parameter = sched_policy
            elif param_type == 'additional_bw':
                for bw_component in openolt_pb2.AdditionalBW.keys():
                    if param_value == bw_component:
                        parameter = bw_component
        except BaseException as e:
            log.exception(exception=e)
        return parameter


class EponProfile(TechProfile):
    def __init__(self, resource_mgr):  # pylint: disable=super-init-not-called
        super().__init__(resource_mgr)
        raise NotImplementedError('TODO: Not yet implemented')

    class DefaultTechProfile:
        def __init__(self, _name, **_kwargs):
            raise NotImplementedError('TODO: Not yet implemented')

        def to_json(self):
            return json.dumps(self, default=lambda o: o.__dict__,
                              indent=2)


class CustomEncoder(json.JSONEncoder):
    """
    OpenONU Go adapter JSON unmarshalling is strict and does not accept additional
    fields.  Those have been moved to attributes starting with _ and this encoder is
    used to insure that they are not serialized into the KV-Store.
    """
    def default(self, o):
        return {k: v if not isinstance(v, bool) else str(v) for k, v in vars(o).items()
                if k[0] != '_'}


class TechProfileInstance:
    """
    A TechProfileInstance is an instance of a particular Technology Profile on
    a specific interface.  The main difference is that it will have resources (AllocIDs,
    GEM Port IDs, ...) associated with it.
    """
    def __init__(self, subscriber_identifier, tech_profile):
        if tech_profile is None:
            raise ValueError('Technology Profile not provided')

        self.subscriber_identifier = subscriber_identifier
        self.name = tech_profile.name
        self.profile_type = tech_profile.profile_type
        self.version = tech_profile.version
        self.num_gem_ports = tech_profile.num_gem_ports
        self.instance_control = tech_profile.instance_control
        self._num_of_tconts = 1       # This may change in future

        self.us_scheduler = None
        self.ds_scheduler = None

        self.upstream_gem_port_attribute_list = list()
        self.downstream_gem_port_attribute_list = list()

    @staticmethod
    def create(subscriber_identifier, tech_profile, resource_mgr, intf_id, tp_path, kv_store):
        """
        Realize a new instance of a Technology profile on the given interface

        The 'tech_profile' passed to this method is a TechnologyProfile object
        """
        # pylint: disable=import-outside-toplevel
        from pyvoltha_min.adapters.common.pon_resource_manager.resource_manager import PONResourceManager

        tp_instance = TechProfileInstance(subscriber_identifier, tech_profile)

        # Get TCONT alloc id(s) using resource manager
        if tech_profile.instance_control.onu == 'multi-instance':
            # Each UNI port gets its own TCONT
            alloc_id = resource_mgr.get_resource_id(intf_id, PONResourceManager.ALLOC_ID,
                                                    tp_instance._num_of_tconts)
        else:       # 'single-instance'
            # All service flows referencing this TP instance will share a single TCONT. Search
            # any existing UNI ports of this ONU and see if they reference this TP-ID and have
            # already allocated a TCONT
            existing = TechProfileInstance.get_single_instance_tp(tp_path, kv_store)

            if existing is None:
                # No, we are the first UNI on this ONU with this TPID. Get the TCONT now
                alloc_id = resource_mgr.get_resource_id(intf_id, PONResourceManager.ALLOC_ID,
                                                        tp_instance._num_of_tconts)
            else:
                # Use alloc ID from existing UNI instance
                alloc_id = existing.us_scheduler.alloc_id

        # Get GEM Port id(s) using resource manager
        gem_ports = resource_mgr.get_resource_id(intf_id, PONResourceManager.GEMPORT_ID,
                                                 tp_instance.num_gem_ports)
        if isinstance(gem_ports, int):
            gemport_list = [gem_ports]

        elif isinstance(gem_ports, (list, set, tuple)):
            gemport_list = list(gem_ports)

        else:
            raise ValueError("invalid GEM Port type: '{}'".format(type(gem_ports)))

        # Assign the allocated resources
        return tp_instance.assign_resources(tech_profile, alloc_id, gemport_list)

    @staticmethod
    def restore(subscriber_identifier, tech_profile):
        """
        Restore (or make a copy) of an existing Technology Profile instance for the given interface

        The 'tech_profile' passed to this method needs to have attributes equivalent to the TechProfile
        class as well as access to Alloc and GEM Port IDs currently allocated to the instance.
        """
        tp_instance = TechProfileInstance(subscriber_identifier, tech_profile)

        alloc_id = tech_profile.us_scheduler.alloc_id
        gemport_ids = {gem_port.gemport_id for gem_port in tech_profile.upstream_gem_port_attribute_list} | \
            {gem_port.gemport_id for gem_port in tech_profile.downstream_gem_port_attribute_list}

        # Assign the allocated resources
        return tp_instance.assign_resources(tech_profile, alloc_id, list(gemport_ids))

    def assign_resources(self, tech_profile, alloc_id, gemport_list):
        """ Create / restore resource assignment step """
        self.us_scheduler = TechProfileInstance.IScheduler(alloc_id, tech_profile.us_scheduler)
        self.ds_scheduler = TechProfileInstance.IScheduler(None, tech_profile.ds_scheduler)

        mcast_gem_port_attribute_list = list()

        for idx in range(self.num_gem_ports):
            # Add upstream GEM Ports
            self.upstream_gem_port_attribute_list.append(
                TechProfileInstance.IGemPortAttribute(gemport_list[idx],
                    tech_profile.upstream_gem_port_attribute_list[idx]))

            # Add unicast downstream GEM Ports (save of mcast downstream for last)
            if tech_profile.downstream_gem_port_attribute_list[0].is_multicast:
                mcast_gem_port_attribute_list.append(
                    TechProfileInstance.IGemPortAttribute(
                        gemport_list[idx],
                        tech_profile.downstream_gem_port_attribute_list[idx]))
            else:
                self.downstream_gem_port_attribute_list.append(
                    TechProfileInstance.IGemPortAttribute(
                        gemport_list[idx],
                        tech_profile.downstream_gem_port_attribute_list[idx]))

        # Now fold in any multicast downstream GEM ports at end of DS list
        self.downstream_gem_port_attribute_list.extend(mcast_gem_port_attribute_list)
        return self

    class IScheduler(Scheduler):
        def __init__(self, alloc_id, scheduler):
            super().__init__(
                scheduler.direction, scheduler.additional_bw,
                scheduler.priority,
                scheduler.weight, scheduler.q_sched_policy)
            self.alloc_id = alloc_id

    class IGemPortAttribute(GemPortAttribute):
        def __init__(self, gemport_id, gem_port_attribute):
            super().__init__(
                gem_port_attribute.pbit_map, gem_port_attribute.discard_config,
                aes_encryption=gem_port_attribute.aes_encryption,
                scheduling_policy=gem_port_attribute.scheduling_policy,
                priority_q=gem_port_attribute.priority_q,
                weight=gem_port_attribute.weight,
                max_q_size=gem_port_attribute.max_q_size,
                discard_policy=gem_port_attribute.discard_policy,
                is_multicast=gem_port_attribute.is_multicast,
                dynamic_access_control_list=gem_port_attribute.dynamic_access_control_list,
                static_access_control_list=gem_port_attribute.static_access_control_list,
                multicast_gem_id=gem_port_attribute.multicast_gem_id)
            self.gemport_id = gemport_id

    def to_json(self):
        return json.dumps(self, indent=2, cls=CustomEncoder)

    @property
    def alloc_id(self):
        return self.us_scheduler.alloc_id

    @property
    def gem_port_ids(self):
        return {gem_port.gemport_id for gem_port in self.upstream_gem_port_attribute_list} | \
            {gem_port.gemport_id for gem_port in self.downstream_gem_port_attribute_list}

    @property
    def mcast_gem_port_ids(self):
        return {gem_port.gemport_id for gem_port in self.downstream_gem_port_attribute_list
                if gem_port.is_multicast}

    @staticmethod
    def get_single_instance_tp(tp_path, kv_store):
        """ Gets another TP Instance for an ONU on a different UNI port for the same TP ID"""
        # For example:
        # tpPath like "service/voltha/technology_profiles/XGS-PON/64/pon-{0}/onu-{1}/uni-{1}"
        # is broken into ["service/voltha/technology_profiles/XGS-PON/64/pon-{0}/onu-{1}", ""]
        try:
            uni_path_slice = re.split('/uni-[0-9]+$', tp_path, maxsplit=2)
            if uni_path_slice is None:
                return None

            kv_list = kv_store.list(uni_path_slice[0])
            for data, _metadata in kv_list:
                try:
                    tp_dict = json.loads(data)
                    return TechProfile.create(tp_dict)

                except Exception as e:
                    log.warn('single-instance-json-failure', e=e, tech_profile=data, path=tp_path)

            return None

        except Exception as e:
            log.exception('single-instance-failure', e=e)
            return None
