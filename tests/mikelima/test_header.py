import pytest
import io
import struct

from bip.plugins.mikelima.header import read_message_header, unpack_som, read_first_header

@pytest.fixture
def fake_header():
    h = io.BytesIO()
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Nonsense
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Nonsense
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Nonsense
    h.write(bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #SOM Markers
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    h.write(bytes())
    h.seek(0)
    yield h


def test_unpack_som():
    '''
    '''
    header_bytes = bytes.fromhex('F07FFF7FFF7FFF7F')
    h = io.BytesIO()
    h.write(bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #SOM Markers
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    h.write(bytes())
    h.seek(0)

    bytes_out, number_of_bytes_read = unpack_som(header_bytes, h)

    a = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOM Header
    b = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefined Words
    c = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefinedWords
    expected_payload = a + b + c

    assert bytes_out == expected_payload
    assert number_of_bytes_read == 36*8


def test_read_message_header(fake_header):
    h = io.BytesIO()
    h.write(bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #SOM Markers
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    h.write(bytes())
    h.seek(0)

    bytes_out, number_of_bytes_read = read_message_header(fake_header)

    a = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOM Header
    b = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefined Words
    c = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefinedWords
    expected_payload = a + b + c

    assert bytes_out == expected_payload
    assert number_of_bytes_read == 36*8

def test_unpack_som_bad_header():
    header_bytes = bytes.fromhex('F07FFF7FFF7FFF7F')
    h = io.BytesIO()
    h.write(bytes.fromhex('F17FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #Wrong SOM Markers
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    h.write(bytes())
    h.seek(0)

    with pytest.raises(RuntimeError):
        unpack_som(header_bytes, h)


def test_unpack_som_end_of_file():
    header_bytes = bytes.fromhex('F07FFF7FFF7FFF7F')
    h = io.BytesIO()
    h.write(bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #Wrong SOM Markers
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    h.write(bytes())
    h.seek(0)

    with pytest.raises(RuntimeError):
        unpack_som(header_bytes, h)


def test_read_message_header_bad_header():
    h = io.BytesIO()
    h.write(bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7F')) #Wrong SOM Markers
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    h.write(bytes())
    h.seek(0)

    bytes_out, number_of_bytes_read = read_message_header(h)

    assert bytes_out == 0
    assert number_of_bytes_read == 0

def test_read_first_header():
    h = io.BytesIO()
    h.write(bytes.fromhex('80040000446174613A202052617720436F6E74696E756F7573204F7574707574'))
    h.write(bytes.fromhex('20202020200032303233203330332032303A31383A34352E3435343637370030'))
    h.write(bytes.fromhex('303030203030302030303A30302E303030303030000000002000000000000000'))
    h.write(bytes.fromhex('880000002C000000B400000026000000DA00000008000000E2000000B3010000'))
    h.write(bytes.fromhex('950200000000000032303233313033303230313834355F4D454C4C4F50484F4E'))
    h.write(bytes.fromhex('455F425F56315F4951305F303031392E62696E002F736973646174612F726169'))
    h.write(bytes.fromhex('64302F32303233313033302F536573736E3030312F4951302F00736973415F65'))
    h.write(bytes.fromhex('F07FFF7FFF7FFF7F'))
    h.seek(0)

    length, orphan_data, timestamp, iq_type, session_id, _, _= read_first_header(h)
    assert length == 32*7
    assert timestamp == 1698697125454677
    assert iq_type == 0
    assert session_id == 1
    assert len(orphan_data) == 0


def test_read_first_header_orphan_data():
    h = io.BytesIO()
    h.write(bytes.fromhex('80040000446174613A202052617720436F6E74696E756F7573204F7574707574'))
    h.write(bytes.fromhex('20202020200032303233203330332032303A31383A34352E3435343637370030'))
    h.write(bytes.fromhex('303030203030302030303A30302E303030303030000000002000000000000000'))
    h.write(bytes.fromhex('880000002C000000B400000026000000DA00000008000000E2000000B3010000'))
    h.write(bytes.fromhex('950200000000000032303233313033303230313834355F4D454C4C4F50484F4E'))
    h.write(bytes.fromhex('455F425F56315F4951305F303031392E62696E002F736973646174612F726169'))
    h.write(bytes.fromhex('64302F32303233313033302F536573736E3030312F4951302F00736973415F65'))
    h.write(struct.pack("<QQQQQQQQQQQQ", 8, 8, 8, 8, 8, 8, 8, 8 , 8, 8, 8, 8)) #Nonsense
    h.write(bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F')) #SOP Markers
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 0, 0, 0x0000006000000000, 0, 0, 7, 0 , 0, 10, 0, 0)) #SOP Header
    h.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Data
    h.write(struct.pack("<QQQQQQQQQQQQ", 8, 8, 8, 8, 8, 8, 8, 8 , 8, 8, 8, 8)) #Nonsense
    h.write(struct.pack("<QQQQQQQQQQQQ", 8, 8, 8, 8, 8, 8, 8, 8 , 8, 8, 8, 8)) #Nonsense
    h.write(bytes.fromhex('F07FFF7FFF7FFF7F'))
    h.seek(0)

    length, orphan_data, timestamp, _, _, _, _ = read_first_header(h)
    assert len(orphan_data) == 1
    assert length == ((32*7) + (36*8)) # 7 lines of 32 bytes at the top, 36 Q values of nonsense
    assert timestamp == 1698697125454677

def test_read_first_bad_header():
    h = io.BytesIO()
    h.write(bytes.fromhex('80040000446174613A202052617720436F6E74696E756F7573204F7574707574'))
    h.write(bytes.fromhex('20202020200032303233203330332032303A31383A34352E3435343637370030'))
    h.write(bytes.fromhex('303030203030302030303A30302E303030303030000000002000000000000000'))
    h.write(bytes.fromhex('880000002C000000B400000026000000DA00000008000000E2000000B3010000'))
    h.write(bytes.fromhex('950200000000000032303233313033303230313834355F4D454C4C4F50484F4E'))
    h.write(bytes.fromhex('455F425F56315F4951305F303031392E62696E002F736973646174612F726169'))
    h.seek(0)
    with pytest.raises(RuntimeError):
        read_first_header(h)

