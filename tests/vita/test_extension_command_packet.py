import pytest
from io import BytesIO

from bip.vita.extension_command_packet import ExtensionCommandPacket


@pytest.fixture
def _extension_command_packet():
    return [
            0x1F5CDEAD, #header
            0x0BAD0BAD, #streamid
            0x11111111, #payload
            ]

@pytest.fixture
def extension_command_packet(_extension_command_packet):
    with BytesIO() as f:
        for n in _extension_command_packet:
            f.write(n.to_bytes(4, byteorder='little'))
        return (_extension_command_packet, f.getvalue())


def test_stream_id(extension_command_packet):
    raw, bytes_ = extension_command_packet
    packet = ExtensionCommandPacket(bytes_)
    assert packet.stream_id == int(raw[1])

