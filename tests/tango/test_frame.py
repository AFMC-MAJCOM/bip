import pytest
from io import BytesIO

from bip.plugins.tango.frame import unpack_header, read_header, first_header

def test_unpack_header():
    header = int((0x1<<20) | 108).to_bytes(4, byteorder='little')
    frame_cnt, frame_size = unpack_header(header)
    assert frame_cnt == 1
    assert frame_size == 108


def test_read_header():
    with BytesIO() as f:
        f.write(bytes("PLRV", encoding="ascii"))
        for i in [1, 2, 3]:
            f.write(i.to_bytes(4, byteorder='little'))

        f.seek(0)
        br, h = read_header(f)
        assert br == 8


def test_first_header():
    with BytesIO() as f:
        for i in range(8):
            f.write(int(7).to_bytes(4, byteorder='little'))
        f.write(bytes("PLRV", encoding="ascii"))
        for i in [1, 2, 3]:
            f.write(i.to_bytes(4, byteorder='little'))

        f.seek(0)
        n = first_header(f)

        assert n == 4*8


def test_bad_header():
    with BytesIO() as f:
        f.write(bytes("XYZW", encoding="ascii"))
        for i in [1, 2, 3]:
            f.write(i.to_bytes(4, byteorder='little'))

        f.seek(0)
        with pytest.raises(RuntimeError):
            br, h = read_header(f)


def test_short_header():
    with BytesIO() as f:
        f.write(bytes("PLRV", encoding="ascii"))
        f.seek(0)

        with pytest.raises(RuntimeError):
            br, h = read_header(f)


def test_artifacts_before_header():
    with BytesIO() as f:
        f.write(bytes("GARBAGEX", encoding="ascii"))
        f.write(bytes("PLRV", encoding="ascii"))
        for i in [1, 2, 3]:
            f.write(i.to_bytes(4, byteorder='little'))

        f.seek(0)
        br, h = read_header(f)
        assert br == 16