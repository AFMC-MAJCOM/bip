import pytest
import io
import struct

from pathlib import Path

from bip.non_vita import mblb as mb
from bip.plugins.mikelima.parser import Parser
from bip.recorder.dummy.dummywriter import DummyWriter
from bip.recorder.parquet.pqwriter import PQWriter



@pytest.fixture
def fake_file():
    f = io.BytesIO()
    f.write(bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #SOM Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    f.write(bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F')) #SOP Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOP Header
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Data
    f.write(bytes.fromhex('F27FFF7FFF7FFF7FF27FFF7FFF7FFF7FF27FFF7FFF7FFF7F')) #EOM Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #EOM information
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #EOM information
    f.write(bytes())
    f.seek(0)
    yield f


@pytest.fixture
def fake_file_no_eom():
    f = io.BytesIO()
    f.write(bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #SOM Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    f.write(bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F')) #SOP Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 0x0000006000000000, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOP Header
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Data
    f.write(bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F')) #SOP Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 0x000000F000000000, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOP Header
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Data
    f.write(bytes())
    f.seek(0)
    yield f


@pytest.fixture
def fake_file_unhandled_markers():
    f = io.BytesIO()
    f.write(bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7FF07FFF7FFF7FFF7F')) #SOM Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOM Header
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefined Words
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SWDefinedWords
    f.write(bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F')) #SOP Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #SOP Header
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #Data
    f.write(bytes.fromhex('F37FFF7FFF7FFF7FF37FFF7FFF7FFF7FF37FFF7FFF7FFF7F')) #Change of Mode Markers
    f.write(bytes.fromhex('F27FFF7FFF7FFF7FF27FFF7FFF7FFF7FF27FFF7FFF7FFF7F')) #EOM Markers
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #EOM information
    f.write(struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12)) #EOM information
    f.write(bytes())
    f.seek(0)
    yield f


def test_read_message(fake_file):
    parser =  Parser(Path(), Path(), DummyWriter)
    iq_type = 2
    session_id = 15
    increment = 4
    timestamp_from_filename = 19411207120000
    parser.initialize_message_processor(iq_type)
    payload, som_obj = parser.read_message(fake_file)

    a = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    b = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOP Header
    c = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #Data
    d = bytes.fromhex('F27FFF7FFF7FFF7FF27FFF7FFF7FFF7FF27FFF7FFF7FFF7F') #EOM Markers
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #EOM Trailer
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #EOM Trailer
    expected_payload = a+b+c+d+e+f
    expected_payload = bytearray(expected_payload)

    assert payload == expected_payload

    d = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOM Header
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefined Words
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefinedWords
    fake_som_obj = d+e+f

    timestamp = 0
    expected_som_obj = mb.MblbSOM(fake_som_obj, timestamp, iq_type, session_id, increment, timestamp_from_filename)

    assert type(som_obj) == type(expected_som_obj)
    assert som_obj.freq_ghz == expected_som_obj.freq_ghz



def test_broken_message(fake_file_no_eom):
    parser =  Parser(Path(), Path(), DummyWriter)
    iq_type = 2
    session_id = 15
    increment = 4
    timestamp_from_filename = 19411207120000

    parser.initialize_message_processor(iq_type)
    payload, som_obj = parser.read_message(fake_file_no_eom)

    a = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    b = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 0x0000006000000000, 5, 6, 7, 8 , 9, 10, 11, 12) #SOP Header
    c = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #Data
    d = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 0x000000F000000000, 5, 6, 7, 8 , 9, 10, 11, 12) #SOP Header
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #Data
    expected_payload = a+b+c+d+e+f
    expected_payload = bytearray(expected_payload)

    assert payload == expected_payload

    d = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOM Header
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefined Words
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefinedWords
    fake_som_obj = d+e+f

    timestamp = 0
    expected_som_obj = mb.MblbSOM(fake_som_obj, timestamp, iq_type, session_id, increment, timestamp_from_filename)

    assert type(som_obj) == type(expected_som_obj)
    assert som_obj.freq_ghz == expected_som_obj.freq_ghz


def test_bad_message_unhandled_markers(fake_file_unhandled_markers):
    parser =  Parser(Path(), Path(), DummyWriter)

    with pytest.raises(Exception):
        parser.read_message(fake_file_unhandled_markers)

