import pytest
from io import BytesIO

from bip.vita import vrt_header
from bip.vita.vrt_packet import VRTPacketHeader, VRTPacket

def test_vita_header():
    header = vrt_header(0x1F5CDEAD.to_bytes(4, byteorder='little'))
    assert isinstance(header, VRTPacketHeader)

    assert header.packet_type == 1
    assert header.class_id == 1
    assert header.indicators == 0b111
    assert header.tsi == 0b01
    assert header.tsf == 0b01
    assert header.packet_count == 12
    assert header.packet_size == 0xDEAD


def test_vita_header_alt():
    header = vrt_header(0xF0AAFFFF.to_bytes(4, byteorder='little'))
    assert isinstance(header, VRTPacketHeader)

    assert header.packet_type == 15
    assert header.class_id == 0
    assert header.indicators == 0
    assert header.tsi == 0b10
    assert header.tsf == 0b10
    assert header.packet_count == 10
    assert header.packet_size == 65535


def test_vrt_packet_makes_header(fake_packet):
    val = 0x1F5C0001
    packet = VRTPacket(val.to_bytes(4, byteorder='little'))
    assert packet.packet_header.value == val



@pytest.fixture
def fake_packet():
    content = [
            0x1F5C0008, #header
            0x55555555, #stream id
            0xEFFEEFFE, #class id 0
            0xFEEFFEEF, #class id 1
            0x11111111, #integer time
            0x22222222, #fractional time 0
            0x33333333, #fractional time 1
            0xDEADBEEF  #packet data
            ]

    f = BytesIO()
    for c in content:
        f.write(c.to_bytes(4, byteorder='little'))
    return content, f.getvalue()

def test_vrt_packet_payload(fake_packet):
    _, payload = fake_packet
    packet = VRTPacket(payload)
    assert packet.payload == payload

def test_vrt_packet_words(fake_packet):
    content, payload = fake_packet
    packet = VRTPacket(payload)
    assert packet.packet_header.value == content[0]
    assert len(content) == len(packet.words)
    for c, w in zip(content, packet.words):
        assert c == w

def test_vrt_packet_accessors(fake_packet):
    content, payload = fake_packet
    packet = VRTPacket(payload)
    assert packet.packet_header.value == content[0]
    assert packet.stream_identifier == content[1]
    assert packet.class_identifier[0] == content[2]
    assert packet.class_identifier[1] == content[3]
    assert packet.integer_timestamp == content[4]
    assert packet.fractional_timestamp[0] == content[5]
    assert packet.fractional_timestamp[1] == content[6]

