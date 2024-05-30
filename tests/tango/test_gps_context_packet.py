import pytest
import io
import struct

import numpy as np


import bip
from bip.plugins.tango.gps_context_packet import _GPSExtensionContextPacket as GPSExtensionContextPacket


@pytest.fixture
def simple_gps_packet():
    header = 0x5CA10275 #header word
    empty_nav_struct = [
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
        0x00000000,
    ]
    packet_data = [
        header,
        
        0x576EA41D, # 1 stream id

        0x00000001, # 2 class id 0
        0x00030003, # 3 class id 1

        #payload
        0x123B0000, #4 system_status & filter status
        0x650C6993, #5 unix time seconds
        0x0009DC14, #6 microseconds
        0x00000000, #7 latitude
        0x00000000, #8
        0x00000000, #9 longitude
        0x00000000, #10
        0x00000000, #11 altitude
        0x00000000, #12
        0x43551BEF, #13 velocity_0
        0xBF343967, #14 velocity_1
        0x3F24EB95, #15 velocity_2
        0xBF271FBB, #16 acceleration_0
        0x3DC83DDC, #17 acceleration_1
        0xBEB6F4BE, #18 acceleration_2
        0x3F6FA802, #19 gforce
        0x3EBD01E8, #20 attitude_0
        0x3FC0EDD8, #21 attitude_1
        0x40737CAC, #22 attitude_2
        0x3B9ACFB3, #23 attitude_rate_0
        0xBC28BD44, #24 attitude_rate_1
        0x3C1CA84F, #25 attitude_rate_2
        0x00000000, #26 latitude_std_dev
        0x00000000, #27 longitude_std_dev
        0x00000000, #28 altitude_std_dev
        ]
    
    for i in range(24):
        packet_data += empty_nav_struct

    with io.BytesIO() as f:
        for d in packet_data:
            f.write(struct.pack("<I", d))

        yield header, bytearray(f.getvalue())


def test_gps_packet_create(simple_gps_packet):
    header, payload = simple_gps_packet
    packet = GPSExtensionContextPacket(payload)
    assert packet is not None


def test_gps_packet_header(simple_gps_packet):
    header, payload = simple_gps_packet

    packet = GPSExtensionContextPacket(payload)
    assert packet.packet_header.value == header
    assert packet.packet_header.packet_size ==  len(payload) // 4
    assert packet.packet_header.packet_type == 0x5
    assert packet.packet_header.class_id == True


def test_gps_packet(simple_gps_packet):
    packet = GPSExtensionContextPacket(simple_gps_packet[1])

    assert packet.stream_id == 0x576EA41D

    assert packet.system_status[0] == 0
    assert packet.filter_status[0] == 4667
    assert packet.unix_time_seconds[0] == 1695312275
    assert packet.microseconds[0] == 646164
    assert packet.latitude[0] == 0
    assert packet.longitude[0] == 0
    assert packet.altitude[0] == 0
    assert packet.velocity_0[0] == pytest.approx(213.10912)
    assert packet.velocity_1[0] == pytest.approx(-0.7040009)
    assert packet.velocity_2[0] == pytest.approx(0.6442197)
    assert packet.acceleration_0[0] == pytest.approx(-0.6528279)
    assert packet.acceleration_1[0] == pytest.approx(0.09777424)
    assert packet.acceleration_2[0] == pytest.approx(-0.35733598)
    assert packet.gforce[0] == pytest.approx(0.93615735)
    assert packet.attitude_0[0] == pytest.approx(0.36915517)
    assert packet.attitude_1[0] == pytest.approx(1.5072584)
    assert packet.attitude_2[0] == pytest.approx(3.8044844)
    assert packet.attitude_rate_0[0] == pytest.approx(0.0047244667)
    assert packet.attitude_rate_1[0] == pytest.approx(-0.010299031)
    assert packet.attitude_rate_2[0] == pytest.approx(0.009561612)
    assert packet.latitude_std_dev[0] == 0
    assert packet.longitude_std_dev[0] == 0
    assert packet.altitude_std_dev[0] == 0
