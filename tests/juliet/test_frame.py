import pytest
from io import BytesIO

from bip.plugins.juliet.frame import unpack_header, read_header

def test_unpack_header():
    header = [ 0x11111111, 0x22222222, 0x33333333 ]
    bytes_ = bytearray()
    for w in header:
        for b in w.to_bytes(4, byteorder='little'):
            bytes_.append(b)

    t, wc = unpack_header(bytes_)

    assert t == 1229782938533634594 # (0x11111111 << 32) + 0x22222222
    assert wc == 858993459 # 0x33333333

def test_read_header():
    with BytesIO() as f:
        for i in [1, 2, 3]:
            f.write(i.to_bytes(4, byteorder='little'))

        f.seek(0)
        br, h = read_header(f)

        assert br == 12


