import pytest

from bip.vita import vrt_header, VRTPacketHeader

def test_vita_header():
    header = vrt_header(0x1F5CDEAD.to_bytes(4, byteorder='little'))
    assert isinstance(header, VRTPacketHeader)

