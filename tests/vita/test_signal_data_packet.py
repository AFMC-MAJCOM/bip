import pytest
from io import BytesIO

import numpy as np

from bip.vita.signal_data_packet import SignalDataIndicators, SignalDataPacket


@pytest.fixture
def _signal_data_packet():
    return [
            0x1F5C0009, #header
            0x0BAD0BAD, #streamid
            0x0,  #classid 0
            0x0,  #class id 1
            0x0,  #tsi
            0x0,  #tsf0
            0x0,  #tsf1
            0x11112222, #payload
            0xCCCCCCCC, #trailer
            ]


@pytest.fixture
def signal_data_packet(_signal_data_packet):
    with BytesIO() as f:
        for n in _signal_data_packet:
            f.write(n.to_bytes(4, byteorder='little'))
        return (_signal_data_packet, f.getvalue())



def test_signal_data_indicators(signal_data_packet):
    raw, bytes_ = signal_data_packet

    packet = SignalDataPacket(bytes_)
    si = packet.signal_data_indicators

    assert si.signal_spectrum == True
    assert si.not_vita_49_0 == True
    assert si.trailer == True

def test_signal_data_indicators_spectrume_off():
    si = SignalDataIndicators(0x1F5CDEAD & ~(1<<24))

    assert si.signal_spectrum == False
    assert si.not_vita_49_0 == True
    assert si.trailer == True

def test_signal_data_indicators_not_vita_off():
    si = SignalDataIndicators(0x1F5CDEAD & ~(1<<25))

    assert si.signal_spectrum == True
    assert si.not_vita_49_0 == False
    assert si.trailer == True

def test_signal_data_indicators_trailer_off():
    si = SignalDataIndicators(0x1F5CDEAD & ~(1<<26))

    assert si.signal_spectrum == True
    assert si.not_vita_49_0 == True
    assert si.trailer == False



def test_stream_id(signal_data_packet):
    raw, bytes_ = signal_data_packet
    packet = SignalDataPacket(bytes_)
    assert packet.stream_id == int(raw[1])

def test_trailer(signal_data_packet):
    raw, bytes_ = signal_data_packet
    packet = SignalDataPacket(bytes_)
    assert packet.trailer == int(raw[-1])

def test_trailer_not_present(signal_data_packet):
    raw, _ = signal_data_packet
    raw[0] = 0x1F5C0001 & ~(1<<26)
    with BytesIO() as f:
        for n in raw:
            f.write(n.to_bytes(4, byteorder='little'))
        bytes_ = f.getvalue()

    packet = SignalDataPacket(bytes_)
    assert packet.trailer == int(0)


def test_data(signal_data_packet):
    raw, bytes_ = signal_data_packet
    packet = SignalDataPacket(bytes_)

    assert packet.data.shape[0] == 1
    assert packet.data[0][0] == 0x2222
    assert packet.data[0][1] == 0x1111


def test_data_no_trailer(signal_data_packet):
    raw, _ = signal_data_packet
    raw[0] = 0x1F5C0009 & ~(1<<26)
    with BytesIO() as f:
        for n in raw:
            f.write(n.to_bytes(4, byteorder='little'))
        bytes_ = f.getvalue()

    packet = SignalDataPacket(bytes_)

    assert packet.data.shape[0] == 2
    assert packet.data[0][0] == 0x2222
    assert packet.data[0][1] == 0x1111
    assert packet.data[1][0] == np.uint16(0xCCCC).astype(np.int16)
    assert packet.data[1][1] == np.uint16(0xCCCC).astype(np.int16)

