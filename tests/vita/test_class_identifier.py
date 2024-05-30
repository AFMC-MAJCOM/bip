import pytest

import numpy as np
from bip.vita.class_identifier import ClassIdentifier

@pytest.fixture
def _class_id():
    return {
            "pad_bit_count": 17,
            "oui": 0xFFFFFF,
            "information_code": 15,
            "packet_code": 13
    }

@pytest.fixture
def class_id(_class_id):
    val = [ ((_class_id["pad_bit_count"] & 0x1F) << 27) \
                | (_class_id["oui"] & 0xFFFFFF),
            ((_class_id["information_code"] & 0xFFFF) << 16) \
                | (_class_id["packet_code"] & 0xFFFF) ]
    return (_class_id, val)


def test_class_identifier(class_id):
    expected, bytes_ = class_id
    cid = ClassIdentifier(bytes_)
    assert cid._data == bytes_


def test_class_identifier_values(class_id):
    expected, bytes_ = class_id
    cid = ClassIdentifier(bytes_)
    assert cid.pad_bit_count == expected["pad_bit_count"]
    assert cid.oui == expected["oui"]
    assert cid.information_class_code == expected["information_code"]
    assert cid.packet_class_code == expected["packet_code"]


