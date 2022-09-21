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
#
from hashlib import md5

import structlog
from voltha_protos import openflow_13_pb2 as voltha__protos_dot_openflow__13__pb2

log = structlog.get_logger()

# aliases
ofb_field = voltha__protos_dot_openflow__13__pb2.ofp_oxm_ofb_field
action = voltha__protos_dot_openflow__13__pb2.ofp_action

# OFPAT_* shortcuts
OUTPUT = voltha__protos_dot_openflow__13__pb2.OFPAT_OUTPUT
COPY_TTL_OUT = voltha__protos_dot_openflow__13__pb2.OFPAT_COPY_TTL_OUT
COPY_TTL_IN = voltha__protos_dot_openflow__13__pb2.OFPAT_COPY_TTL_IN
SET_MPLS_TTL = voltha__protos_dot_openflow__13__pb2.OFPAT_SET_MPLS_TTL
DEC_MPLS_TTL = voltha__protos_dot_openflow__13__pb2.OFPAT_DEC_MPLS_TTL
PUSH_VLAN = voltha__protos_dot_openflow__13__pb2.OFPAT_PUSH_VLAN
POP_VLAN = voltha__protos_dot_openflow__13__pb2.OFPAT_POP_VLAN
PUSH_MPLS = voltha__protos_dot_openflow__13__pb2.OFPAT_PUSH_MPLS
POP_MPLS = voltha__protos_dot_openflow__13__pb2.OFPAT_POP_MPLS
SET_QUEUE = voltha__protos_dot_openflow__13__pb2.OFPAT_SET_QUEUE
GROUP = voltha__protos_dot_openflow__13__pb2.OFPAT_GROUP
SET_NW_TTL = voltha__protos_dot_openflow__13__pb2.OFPAT_SET_NW_TTL
NW_TTL = voltha__protos_dot_openflow__13__pb2.OFPAT_DEC_NW_TTL
SET_FIELD = voltha__protos_dot_openflow__13__pb2.OFPAT_SET_FIELD
PUSH_PBB = voltha__protos_dot_openflow__13__pb2.OFPAT_PUSH_PBB
POP_PBB = voltha__protos_dot_openflow__13__pb2.OFPAT_POP_PBB
EXPERIMENTER = voltha__protos_dot_openflow__13__pb2.OFPAT_EXPERIMENTER

# OFPXMT_OFB_* shortcuts (incomplete)
IN_PORT = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IN_PORT
IN_PHY_PORT = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IN_PHY_PORT
METADATA = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_METADATA
ETH_DST = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ETH_DST
ETH_SRC = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ETH_SRC
ETH_TYPE = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ETH_TYPE
VLAN_VID = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_VLAN_VID
VLAN_PCP = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_VLAN_PCP
IP_DSCP = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IP_DSCP
IP_ECN = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IP_ECN
IP_PROTO = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IP_PROTO
IPV4_SRC = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV4_SRC
IPV4_DST = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV4_DST
TCP_SRC = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_TCP_SRC
TCP_DST = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_TCP_DST
UDP_SRC = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_UDP_SRC
UDP_DST = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_UDP_DST
SCTP_SRC = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_SCTP_SRC
SCTP_DST = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_SCTP_DST
ICMPV4_TYPE = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ICMPV4_TYPE
ICMPV4_CODE = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ICMPV4_CODE
ARP_OP = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ARP_OP
ARP_SPA = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ARP_SPA
ARP_TPA = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ARP_TPA
ARP_SHA = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ARP_SHA
ARP_THA = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ARP_THA
IPV6_SRC = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV6_SRC
IPV6_DST = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV6_DST
IPV6_FLABEL = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV6_FLABEL
ICMPV6_TYPE = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ICMPV6_TYPE
ICMPV6_CODE = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_ICMPV6_CODE
IPV6_ND_TARGET = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV6_ND_TARGET
OFB_IPV6_ND_SLL = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV6_ND_SLL
IPV6_ND_TLL = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV6_ND_TLL
MPLS_LABEL = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_MPLS_LABEL
MPLS_TC = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_MPLS_TC
MPLS_BOS = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_MPLS_BOS
PBB_ISID = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_PBB_ISID
TUNNEL_ID = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_TUNNEL_ID
IPV6_EXTHDR = voltha__protos_dot_openflow__13__pb2.OFPXMT_OFB_IPV6_EXTHDR
IPV4_DHCP_SRC_PORT = 68
IPV6_DHCP_SRC_PORT = 546

# ofp_action_* shortcuts

def output(port, max_len=voltha__protos_dot_openflow__13__pb2.OFPCML_MAX):
    return action(
        type=OUTPUT,
        output=voltha__protos_dot_openflow__13__pb2.ofp_action_output(port=port, max_len=max_len)
    )


def mpls_ttl(ttl):
    return action(
        type=SET_MPLS_TTL,
        mpls_ttl=voltha__protos_dot_openflow__13__pb2.ofp_action_mpls_ttl(mpls_ttl=ttl)
    )


def push_vlan(e_type):
    return action(
        type=PUSH_VLAN,
        push=voltha__protos_dot_openflow__13__pb2.ofp_action_push(ethertype=e_type)
    )


def pop_vlan():
    return action(
        type=POP_VLAN
    )


def pop_mpls(e_type):
    return action(
        type=POP_MPLS,
        pop_mpls=voltha__protos_dot_openflow__13__pb2.ofp_action_pop_mpls(ethertype=e_type)
    )


def group(group_id):
    return action(
        type=GROUP,
        group=voltha__protos_dot_openflow__13__pb2.ofp_action_group(group_id=group_id)
    )


def nw_ttl(ttl):
    return action(
        type=NW_TTL,
        nw_ttl=voltha__protos_dot_openflow__13__pb2.ofp_action_nw_ttl(nw_ttl=ttl)
    )


def set_field(field):
    return action(
        type=SET_FIELD,
        set_field=voltha__protos_dot_openflow__13__pb2.ofp_action_set_field(
            field=voltha__protos_dot_openflow__13__pb2.ofp_oxm_field(
                oxm_class=voltha__protos_dot_openflow__13__pb2.OFPXMC_OPENFLOW_BASIC,
                ofb_field=field))
    )


def experimenter(experimenter_val, data):
    return action(
        type=EXPERIMENTER,
        experimenter=voltha__protos_dot_openflow__13__pb2.ofp_action_experimenter(
            experimenter=experimenter_val, data=data)
    )


# ofb_field generators (incomplete set)

def in_port(_in_port):
    return ofb_field(type=IN_PORT, port=_in_port)


def in_phy_port(_in_phy_port):
    return ofb_field(type=IN_PHY_PORT, port=_in_phy_port)


def metadata(_table_metadata):
    return ofb_field(type=METADATA, table_metadata=_table_metadata)


def eth_dst(_eth_dst):
    return ofb_field(type=ETH_DST, table_metadata=_eth_dst)


def eth_src(_eth_src):
    return ofb_field(type=ETH_SRC, table_metadata=_eth_src)


def eth_type(_eth_type):
    return ofb_field(type=ETH_TYPE, eth_type=_eth_type)


def vlan_vid(_vlan_vid):
    return ofb_field(type=VLAN_VID, vlan_vid=_vlan_vid)


def vlan_pcp(_vlan_pcp):
    return ofb_field(type=VLAN_PCP, vlan_pcp=_vlan_pcp)


def ip_dscp(_ip_dscp):
    return ofb_field(type=IP_DSCP, ip_dscp=_ip_dscp)


def ip_ecn(_ip_ecn):
    return ofb_field(type=IP_ECN, ip_ecn=_ip_ecn)


def ip_proto(_ip_proto):
    return ofb_field(type=IP_PROTO, ip_proto=_ip_proto)


def ipv4_src(_ipv4_src):
    return ofb_field(type=IPV4_SRC, ipv4_src=_ipv4_src)


def ipv4_dst(_ipv4_dst):
    return ofb_field(type=IPV4_DST, ipv4_dst=_ipv4_dst)


def tcp_src(_tcp_src):
    return ofb_field(type=TCP_SRC, tcp_src=_tcp_src)


def tcp_dst(_tcp_dst):
    return ofb_field(type=TCP_DST, tcp_dst=_tcp_dst)


def udp_src(_udp_src):
    return ofb_field(type=UDP_SRC, udp_src=_udp_src)


def udp_dst(_udp_dst):
    return ofb_field(type=UDP_DST, udp_dst=_udp_dst)


def sctp_src(_sctp_src):
    return ofb_field(type=SCTP_SRC, sctp_src=_sctp_src)


def sctp_dst(_sctp_dst):
    return ofb_field(type=SCTP_DST, sctp_dst=_sctp_dst)


def icmpv4_type(_icmpv4_type):
    return ofb_field(type=ICMPV4_TYPE, icmpv4_type=_icmpv4_type)


def icmpv4_code(_icmpv4_code):
    return ofb_field(type=ICMPV4_CODE, icmpv4_code=_icmpv4_code)


def arp_op(_arp_op):
    return ofb_field(type=ARP_OP, arp_op=_arp_op)


def arp_spa(_arp_spa):
    return ofb_field(type=ARP_SPA, arp_spa=_arp_spa)


def arp_tpa(_arp_tpa):
    return ofb_field(type=ARP_TPA, arp_tpa=_arp_tpa)


def arp_sha(_arp_sha):
    return ofb_field(type=ARP_SHA, arp_sha=_arp_sha)


def arp_tha(_arp_tha):
    return ofb_field(type=ARP_THA, arp_tha=_arp_tha)


def ipv6_src(_ipv6_src):
    return ofb_field(type=IPV6_SRC, arp_tha=_ipv6_src)


def ipv6_dst(_ipv6_dst):
    return ofb_field(type=IPV6_DST, arp_tha=_ipv6_dst)


def ipv6_flabel(_ipv6_flabel):
    return ofb_field(type=IPV6_FLABEL, arp_tha=_ipv6_flabel)


def ipmpv6_type(_icmpv6_type):
    return ofb_field(type=ICMPV6_TYPE, arp_tha=_icmpv6_type)


def icmpv6_code(_icmpv6_code):
    return ofb_field(type=ICMPV6_CODE, arp_tha=_icmpv6_code)


def ipv6_nd_target(_ipv6_nd_target):
    return ofb_field(type=IPV6_ND_TARGET, arp_tha=_ipv6_nd_target)


def ofb_ipv6_nd_sll(_ofb_ipv6_nd_sll):
    return ofb_field(type=OFB_IPV6_ND_SLL, arp_tha=_ofb_ipv6_nd_sll)


def ipv6_nd_tll(_ipv6_nd_tll):
    return ofb_field(type=IPV6_ND_TLL, arp_tha=_ipv6_nd_tll)


def mpls_label(_mpls_label):
    return ofb_field(type=MPLS_LABEL, arp_tha=_mpls_label)


def mpls_tc(_mpls_tc):
    return ofb_field(type=MPLS_TC, arp_tha=_mpls_tc)


def mpls_bos(_mpls_bos):
    return ofb_field(type=MPLS_BOS, arp_tha=_mpls_bos)


def pbb_isid(_pbb_isid):
    return ofb_field(type=PBB_ISID, arp_tha=_pbb_isid)


def tunnel_id(_tunnel_id):
    return ofb_field(type=TUNNEL_ID, tunnel_id=_tunnel_id)


def ipv6_exthdr(_ipv6_exthdr):
    return ofb_field(type=IPV6_EXTHDR, arp_tha=_ipv6_exthdr)


# frequently used extractors:

def get_metadata_from_write_metadata(flow):
    for instruction in flow.instructions:
        if instruction.type == voltha__protos_dot_openflow__13__pb2.OFPIT_WRITE_METADATA:
            return instruction.write_metadata.metadata
    return None


def get_tp_id_from_metadata(write_metadata_value):
    return (write_metadata_value >> 32) & 0xffff


def get_egress_port_number_from_metadata(flow):
    write_metadata_value = get_write_metadata(flow)
    if write_metadata_value is None:
        return None

    # Lower 32-bits
    return write_metadata_value & 0xffffffff


def get_inner_tag_from_write_metadata(flow):
    write_metadata_value = get_write_metadata(flow)
    if write_metadata_value is None:
        return None

    return (write_metadata_value >> 48) & 0xffff


def get_actions(flow):
    """Extract list of ofp_action objects from flow spec object"""
    # assert isinstance(flow, voltha__protos_dot_openflow__13__pb2.ofp_flow_stats)
    # we have the following hard assumptions for now
    for instruction in flow.instructions:
        if instruction.type == voltha__protos_dot_openflow__13__pb2.OFPIT_APPLY_ACTIONS:
            return instruction.actions.actions
    return None


def get_default_vlan(flow):
    for field in get_ofb_fields(flow):
        if field.type == VLAN_VID:
            return field.vlan_vid & 0xfff
    return 0


def get_ofb_fields(flow):
    # assert isinstance(flow, voltha__protos_dot_openflow__13__pb2.ofp_flow_stats)
    # assert flow.match.type == voltha__protos_dot_openflow__13__pb2.OFPMT_OXM
    ofb_fields = []
    for field in flow.match.oxm_fields:
        # assert field.oxm_class == voltha__protos_dot_openflow__13__pb2.OFPXMC_OPENFLOW_BASIC
        ofb_fields.append(field.ofb_field)
    return ofb_fields


def get_meter_id_from_flow(flow):
    for instruction in flow.instructions:
        if instruction.type == voltha__protos_dot_openflow__13__pb2.OFPIT_METER:
            return instruction.meter.meter_id
    return None


def get_out_port(flow):
    for flow_action in get_actions(flow):
        if flow_action.type == OUTPUT:
            return flow_action.output.port
    return None


def get_in_port(flow):
    for field in get_ofb_fields(flow):
        if field.type == IN_PORT:
            return field.port
    return None


def get_goto_table_id(flow):
    for instruction in flow.instructions:
        if instruction.type == voltha__protos_dot_openflow__13__pb2.OFPIT_GOTO_TABLE:
            return instruction.goto_table.table_id
    return None

def get_write_metadata(flow):
    for instruction in flow.instructions:
        if instruction.type == voltha__protos_dot_openflow__13__pb2.OFPIT_WRITE_METADATA:
            return instruction.write_metadata.metadata
    return None

def get_tunnelid(flow):
    for field in get_ofb_fields(flow):
        if field.type == TUNNEL_ID:
            return field.tunnel_id
    return None


def is_dhcp_flow(flow):
    for field in get_ofb_fields(flow):
        if field.type == UDP_SRC and field.udp_src in (IPV4_DHCP_SRC_PORT, IPV6_DHCP_SRC_PORT):
            return True
    return False


def get_metadata(flow):
    ''' legacy get method (only want lower 32 bits '''
    for field in get_ofb_fields(flow):
        if field.type == METADATA:
            return field.table_metadata & 0xffffffff
    return None


def get_metadata_64_bit(flow):
    for field in get_ofb_fields(flow):
        if field.type == METADATA:
            return field.table_metadata
    return None


def get_port_number_from_metadata(flow):
    """
    The port number (UNI on ONU) is in the lower 32-bits of metadata and
    the inner_tag is in the upper 32-bits

    This is set in the ONOS OltPipeline as a metadata field
    """
    mdata = get_metadata_64_bit(flow)

    if mdata is None:
        return None

    if mdata <= 0xffffffff:
        log.warn('onos-upgrade-suggested',
                 netadata=mdata,
                 message='Legacy MetaData detected form OltPipeline')
        return mdata

    return mdata & 0xffffffff


def get_inner_tag_from_metadata(flow):
    """
    The port number (UNI on ONU) is in the lower 32-bits of metadata and
    the inner_tag is in the upper 32-bits

    This is set in the ONOS OltPipeline as a metadata field
    """
    mdata = get_metadata_64_bit(flow)

    if mdata is None:
        return None

    if mdata <= 0xffffffff:
        log.warn('onos-upgrade-suggested',
                 netadata=mdata,
                 message='Legacy MetaData detected form OltPipeline')
        return mdata

    return (mdata >> 32) & 0xffffffff


def get_child_port_from_tunnelid(flow):
    """
    Extract the child device port from a flow that contains the parent device peer port.  Typically the UNI port of an
    ONU child device.  Per TST agreement this will be the lower 32 bits of tunnel id reserving upper 32 bits for later
    use
    """
    tid = get_tunnelid(flow)

    if tid is None:
        return None

    return int(tid & 0xffffffff)


# test and extract next table and group information
def has_next_table(flow):
    return get_goto_table_id(flow) is not None


def get_group(flow):
    for flow_action in get_actions(flow):
        if flow_action.type == GROUP:
            return flow_action.group.group_id
    return None


def has_group(flow):
    return get_group(flow) is not None


def mk_oxm_fields(match_fields):
    oxm_fields = [
        voltha__protos_dot_openflow__13__pb2.ofp_oxm_field(
            oxm_class=voltha__protos_dot_openflow__13__pb2.OFPXMC_OPENFLOW_BASIC,
            ofb_field=field
        ) for field in match_fields
    ]

    return oxm_fields


def mk_instructions_from_actions(actions):
    instructions_action = voltha__protos_dot_openflow__13__pb2.ofp_instruction_actions()
    instructions_action.actions.extend(actions)
    instruction = voltha__protos_dot_openflow__13__pb2.ofp_instruction(type=voltha__protos_dot_openflow__13__pb2.OFPIT_APPLY_ACTIONS,
                                      actions=instructions_action)
    return [instruction]


def mk_simple_flow_mod(match_fields, actions, command=voltha__protos_dot_openflow__13__pb2.OFPFC_ADD,
                       next_table_id=None, **kw):
    """
    Convenience function to generare ofp_flow_mod message with OXM BASIC match
    composed from the match_fields, and single APPLY_ACTIONS instruction with
    a list if ofp_action objects.
    :param match_fields: list(ofp_oxm_ofb_field)
    :param actions: list(ofp_action)
    :param command: one of OFPFC_*
    :param kw: additional keyword-based params to ofp_flow_mod
    :return: initialized ofp_flow_mod object
    """
    instructions = [
        voltha__protos_dot_openflow__13__pb2.ofp_instruction(
            type=voltha__protos_dot_openflow__13__pb2.OFPIT_APPLY_ACTIONS,
            actions=voltha__protos_dot_openflow__13__pb2.ofp_instruction_actions(actions=actions)
        )
    ]
    if next_table_id is not None:
        instructions.append(voltha__protos_dot_openflow__13__pb2.ofp_instruction(
            type=voltha__protos_dot_openflow__13__pb2.OFPIT_GOTO_TABLE,
            goto_table=voltha__protos_dot_openflow__13__pb2.ofp_instruction_goto_table(table_id=next_table_id)
        ))

    return voltha__protos_dot_openflow__13__pb2.ofp_flow_mod(
        command=command,
        match=voltha__protos_dot_openflow__13__pb2.ofp_match(
            type=voltha__protos_dot_openflow__13__pb2.OFPMT_OXM,
            oxm_fields=[
                voltha__protos_dot_openflow__13__pb2.ofp_oxm_field(
                    oxm_class=voltha__protos_dot_openflow__13__pb2.OFPXMC_OPENFLOW_BASIC,
                    ofb_field=field
                ) for field in match_fields
            ]
        ),
        instructions=instructions,
        **kw
    )


def mk_multicast_group_mod(group_id, buckets, command=voltha__protos_dot_openflow__13__pb2.OFPGC_ADD):
    mc_group = voltha__protos_dot_openflow__13__pb2.ofp_group_mod(
        command=command,
        type=voltha__protos_dot_openflow__13__pb2.OFPGT_ALL,
        group_id=group_id,
        buckets=buckets
    )
    return mc_group


def hash_flow_stats(flow):
    """
    Return unique 64-bit integer hash for flow covering the following
    attributes: 'table_id', 'priority', 'flags', 'cookie', 'match', '_instruction_string'
    """
    _instruction_string = ""
    for _instruction in flow.instructions:
        _instruction_string += _instruction.SerializeToString()

    fmt = f'{flow.table_id},{flow.priority},{flow.flags},{flow.cookie},{flow.match.SerializeToString()},{_instruction_string}'
    hex_val = md5(fmt).hexdigest()

    return int(hex_val[:16], 16)


def flow_stats_entry_from_flow_mod_message(mod):
    flow = voltha__protos_dot_openflow__13__pb2.ofp_flow_stats(
        table_id=mod.table_id,
        priority=mod.priority,
        idle_timeout=mod.idle_timeout,
        hard_timeout=mod.hard_timeout,
        flags=mod.flags,
        cookie=mod.cookie,
        match=mod.match,
        instructions=mod.instructions
    )
    flow.id = hash_flow_stats(flow)
    return flow


def group_entry_from_group_mod(mod):
    entry = voltha__protos_dot_openflow__13__pb2.ofp_group_entry(
        desc=voltha__protos_dot_openflow__13__pb2.ofp_group_desc(
            type=mod.type,
            group_id=mod.group_id,
            buckets=mod.buckets
        ),
        stats=voltha__protos_dot_openflow__13__pb2.ofp_group_stats(
            group_id=mod.group_id
            # TODO do we need to instantiate bucket bins?
        )
    )
    return entry


def mk_flow_stat(**kw):
    return flow_stats_entry_from_flow_mod_message(mk_simple_flow_mod(**kw))


def mk_group_stat(**kw):
    return group_entry_from_group_mod(mk_multicast_group_mod(**kw))
