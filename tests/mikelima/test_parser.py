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
def fake_file_no_EOM(): 
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
    IQ_type = 2
    session_id = 15
    increment = 4
    timestamp_from_filename = 19411207120000
    parser.initialize_message_processor(IQ_type)
    payload, SOM_obj = parser.read_message(fake_file)
    
    expected_payload = bytearray(54*8)
    a = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    b = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOP Header
    c = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #Data
    d = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOP Header
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #Data
    expected_payload = a+b+c+d+e+f
    
    assert payload == expected_payload

    fake_SOM_obj = bytearray(36*8)
    d = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOM Header
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefined Words
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefinedWords
    fake_SOM_obj = d+e+f
    
    timestamp = 0
    expected_SOM_obj = mb.mblb_SOM(fake_SOM_obj, timestamp, IQ_type, session_id, increment, timestamp_from_filename)
    
    assert type(SOM_obj) == type(expected_SOM_obj)
    assert SOM_obj.freq_GHz == expected_SOM_obj.freq_GHz
    


def test_broken_message(fake_file_no_EOM):
    parser =  Parser(Path(), Path(), DummyWriter)
    IQ_type = 2
    session_id = 15
    increment = 4
    timestamp_from_filename = 19411207120000
    
    parser.initialize_message_processor(IQ_type)
    payload, SOM_obj = parser.read_message(fake_file_no_EOM)
    
    expected_payload = bytearray(54*8)
    a = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    b = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOP Header
    c = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #Data
    d = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOP Header
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #Data
    expected_payload = a+b+c+d+e+f
    
    assert payload == expected_payload

    fake_SOM_obj = bytearray(36*8)
    d = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SOM Header
    e = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefined Words
    f = struct.pack("<QQQQQQQQQQQQ", 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12) #SWDefinedWords
    fake_SOM_obj = d+e+f
    
    timestamp = 0
    expected_SOM_obj = mb.mblb_SOM(fake_SOM_obj, timestamp, IQ_type, session_id, increment, timestamp_from_filename)
    
    assert type(SOM_obj) == type(expected_SOM_obj)
    assert SOM_obj.freq_GHz == expected_SOM_obj.freq_GHz


def test_bad_message_unhandled_markers(fake_file_unhandled_markers):
    parser =  Parser(Path(), Path(), DummyWriter)
    
    with pytest.raises(Exception):
        parser.read_message(fake_file_unhandled_markers)

