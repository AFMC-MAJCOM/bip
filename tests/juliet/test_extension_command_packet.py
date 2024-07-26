import pytest
import io
import struct

import numpy as np

import bip
from bip.plugins.juliet.extension_command_packet import _ExtensionCommandPacket as Ecp

@pytest.fixture
def simple_ecp_packet():
    header = 0x7CE1001F #header word
    packet_data = [
        header,
        0xDEADBEEF, # 1 stream id
        0x00000001, # 2 class id 0
        0xF0000000, # 3 class id 1
        0x0000FFFF, # 4 tsi
        0x00000000, # 5 tsf0
        0x10000000, # 6 tsf1
        0x01004000, # 7 cam
        0x00000001, # 8 message id
        0x8C20801E, # 9 cif0
        0x20000000, #10 cif1
        0x00000000, #11 cif2
        0x00200000, #12 cif3
        0x01000000, #13 cif4
        0x00000000, #14 bandwidth0
        0x00000000, #15 bandwidth1
        0x00430E23, #16 rf_ref_freq0
        0x40000000, #17 rf_ref_freq1
        0x00009896, #18 rf_ref_freq_offset0
        0x80000000, #19 rf_ref_freq_offset 1
        0x00000000, #20 gain
        0x00009896, #21 sample_rate0
        0x80000000, #22 sample_rate1
        0x200003CF, #23 data_format0
        0x00000000, #24 data_format1
        0x010A020B, #26 pointing3d
        0xFEEDBEEF, #27 cited_sid
        0x00000000, #28 polarization
        0x00001234, #29 dwell0
        0xFEDC0000, #30 dwell1
        0x00000000 #31 mod_type
        ]

    with io.BytesIO() as f:
        for d in packet_data:
            f.write(struct.pack("<I", d))

        yield header, bytearray(f.getvalue())


def test_ecp_packet_create(simple_ecp_packet):
    header, payload = simple_ecp_packet
    packet = Ecp(payload)
    assert packet is not None


def test_ecp_packet_header(simple_ecp_packet):
    header, payload = simple_ecp_packet

    packet = Ecp(payload)
    assert packet.packet_header.value == header
    assert packet.packet_header.packet_size ==  len(payload) // 4
    assert packet.packet_header.tsf == 0x2
    assert packet.packet_header.tsi == 0x3
    assert packet.packet_header.packet_type == 0x7
    assert packet.packet_header.class_id == True


def test_ecp_packet(simple_ecp_packet):
    packet = Ecp(simple_ecp_packet[1])

    assert packet.stream_id == 0xDEADBEEF

    assert packet.integer_timestamp == 0xFFFF
    assert packet.fractional_timestamp[0]== 0
    assert packet.fractional_timestamp[1] == 0x10000000

    assert packet.freq == 18 #GHz
    assert packet.offset ==  160
    assert packet.sampling_rate == 160

    assert packet.az == 4.0859375
    assert packet.el == 2.078125

    assert packet.cited_sid == 0xFEEDBEEF

    assert packet.dwell == 20018823430144 * 10**-9


@pytest.fixture
def simple_ecp_packet_II():
    header = 0x71F2007C #header word
    packet_data = [
        header,
        0xBEADE007, # 1 stream id
        0x00000000, # 2 class id 0
        0x00000000, # 3 class id 1
        0x0000FFFF, # 4 tsi
        0x00000001, # 5 tsf0
        0x10000000, # 6 tsf1
        0xA51F1000, # 7 cam
        0x00000001, # 8 message id
        0x8C20801E, # 9 cif0
        0x20000000, #10 cif1
        0x00000000, #11 cif2
        0x00200000, #12 cif3
        0x01000000, #13 cif4
        0x00000000, #14 bandwidth0
        0x00000000, #15 bandwidth1
        0x00000000, #16 rf_ref_freq0
        0x11111111, #17 rf_ref_freq1
        0x00009896, #18 rf_ref_freq_offset0
        0x80000000, #19 rf_ref_freq_offset 1
        0x00000000, #20 gain
        0x00009896, #21 sample_rate0
        0x80000000, #22 sample_rate1
        0x200003CF, #23 data_format0
        0x00000000, #24 data_format1
        0x020D090F, #26 pointing3d
        0xBEEFFEED, #27 cited_sid
        0x00000000, #28 polarization
        0x00011112, #29 dwell0
        0xFFFC0000, #30 dwell1
        0x00000000 #31 mod_type

        ]

    with io.BytesIO() as f:
        for d in packet_data:
            f.write(struct.pack("<I", d))

        yield header, bytearray(f.getvalue())

def test_ecp_packet_create_II(simple_ecp_packet_II):
    header, payload = simple_ecp_packet_II
    packet = Ecp(payload)
    assert packet is not None


def test_ecp_packet_header_II(simple_ecp_packet_II):
    header, payload = simple_ecp_packet_II

    packet = Ecp(payload)
    assert packet.packet_header.value == header
    assert packet.packet_header.packet_size ==  len(payload) 
    assert packet.packet_header.tsf == 0x3
    assert packet.packet_header.tsi == 0x3
    assert packet.packet_header.packet_type == 0b0111
    assert packet.packet_header.indicators == 0b0001
    assert packet.packet_header.class_id == False


def test_ecp_packet_II(simple_ecp_packet_II):
    packet = Ecp(simple_ecp_packet_II[1])
    print(packet.offset)
    assert packet.stream_id == 0xBEADE007

    assert packet.integer_timestamp == 0xFFFF
    assert packet.fractional_timestamp[0]== 1
    assert packet.fractional_timestamp[1] == 0x10000000

    assert packet.freq == 2.730666666030884 * (10**-7) #Hertz
    assert packet.offset == 160
    assert packet.sampling_rate == 160

    assert packet.az == 18.1171875
    assert packet.el == 4.1015625

    assert packet.cited_sid == 0xBEEFFEED

    assert packet.dwell == 300248278499328 * (10**-9)