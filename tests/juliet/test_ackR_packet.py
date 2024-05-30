import pytest
import io
import struct

import numpy as np
import pandas as pd
import bip
from bip.plugins.juliet.ackR_packet import _ExtensionCommandPacket as ackRp
from bip.plugins.juliet.ackR_packet import AckR_Packet as ap


@pytest.fixture
def simple_ackR_packet():
    header = 0x7CE1000F #header word
    packet_data = [
        header,
        0xB1DED1ED, # 1 stream id
        0x00000001, # 2 class id 0
        0xF0000000, # 3 class id 1
        0x0000FFFF, # 4 tsi
        0x00000000, # 5 tsf0
        0x10000000, # 6 tsf1
        0x01008010, # 7 cam
        0x0000000F, # 8 messageId
        0x00000014, # 9 cif0
        0x40000000, #10 cif2
        0x50000000, #11 cif4
        0x00000000, #12 citedSID
        0x00000000, #13 rejectReason
        0x00000000  #14 dataAddrIndex
        ]

    with io.BytesIO() as f:
        for d in packet_data:
            # "<I" is '<' big endian 'I' is unsigned integer
            f.write(struct.pack("<I", d))

        yield header, bytearray(f.getvalue())


def test_ackR_packet_create(simple_ackR_packet):
    header, payload = simple_ackR_packet
    packet = ackRp(payload)
    assert packet is not None


def test_data_context_packet_header(simple_ackR_packet):
    header, payload = simple_ackR_packet

    packet = ackRp(payload)
    assert packet.packet_header.value == header
    assert packet.packet_header.packet_size ==  len(payload) // 4
    assert packet.packet_header.tsf == 0x2
    assert packet.packet_header.tsi == 0x3
    assert packet.packet_header.packet_type == 0b0111
    assert packet.packet_header.indicators == 0b0100
    assert packet.packet_header.class_id == True


def test_dcp_packet(simple_ackR_packet):
    packet = ackRp(simple_ackR_packet[1])

    assert packet.stream_id == 0xB1DED1ED

    assert packet.integer_timestamp == 0xFFFF
    assert packet.fractional_timestamp[0]== 0
    assert packet.fractional_timestamp[1] == 0x10000000


    


