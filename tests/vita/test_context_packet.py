import pytest
from io import BytesIO

from bip.vita.context_packet import ContextPacket, ContextIndicators


@pytest.fixture
def _context_packet():
    return [
            0x1F5CDEAD, #header
            0x0BAD0BAD, #streamid
            0x10012345, #classid 0
            0x00010001, #classid 2
            0x11111111, #payload
            ]

@pytest.fixture
def context_packet(_context_packet):
    with BytesIO() as f:
        for n in _context_packet:
            f.write(n.to_bytes(4, byteorder='little'))
        return (_context_packet, f.getvalue())

def test_context_indicators(context_packet):
    raw, bytes_ = context_packet

    packet = ContextPacket(bytes_)
    ci = packet.context_indicators

    assert ci.timestamp_mode == True
    assert ci.not_vita_49_0 == True

def test_context_indicators_tsm_off():
    ci = ContextIndicators(0x1F5CDEAD & ~(1<<24))

    assert ci.timestamp_mode == False
    assert ci.not_vita_49_0 == True

def test_context_indicators_not_vita_off():
    ci = ContextIndicators(0x1F5CDEAD & ~(1<<25))

    assert ci.timestamp_mode == True
    assert ci.not_vita_49_0 == False


def test_stream_id(context_packet):
    raw, bytes_ = context_packet
    packet = ContextPacket(bytes_)
    assert packet.stream_id == int(raw[1])


def test_class_id(context_packet):
    raw, bytes_ = context_packet

    packet = ContextPacket(bytes_)
    assert packet.class_id._data[0] == raw[2]
    assert packet.class_id._data[1] == raw[3]


