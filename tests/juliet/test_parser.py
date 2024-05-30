import pytest
import io
import struct
import unittest #

import numpy as np
import pyarrow as pa #

from pathlib import Path

from bip.plugins.juliet.frame import unpack_header
from bip.plugins.juliet.frame import read_header #
from bip import vita

from bip.plugins.juliet.parser import Parser
from bip.recorder.dummy.dummywriter import DummyWriter

from bip.recorder.parquet.pqwriter import PQWriter
from unittest.mock import MagicMock #


def test_custom_header():
    with io.BytesIO() as f:
        f.write(int(0).to_bytes(4, byteorder='little'))
        f.write(int(123456789).to_bytes(4, byteorder='little'))
        f.write(int(5000).to_bytes(4, byteorder='little'))
        bytes_ = f.getvalue()

    time_ns, word_cnt = unpack_header(bytes_)
    assert time_ns == 123456789
    assert word_cnt == 5000


@pytest.fixture
def fake_file():
    #Packet_Type - Extension Context Packet - 0b0101
    #Indicators - 0b0000
    #Payload Length - 28
    f = io.BytesIO()
    f.write(struct.pack("<III", 0x0000002, 0x00000002, 7))
    f.write(struct.pack(">I", 0x501A0018)) 
    f.write(struct.pack(">I", 0x00000001)) 
    f.write(struct.pack(">I", 0xF0000000)) 
    f.write(struct.pack(">I", 0x0000FFFF)) 
    f.write(struct.pack(">I", 0x10000000)) 
    f.write(struct.pack(">I", 0x1234ABCD))
    f.write(struct.pack(">I", 0xCCCCCCCC))
    f.seek(0)
    yield f   
    
@pytest.fixture
def fake_fileII():
    #Packet_Type - Extension Command Packet - 0b0111
    #Indicators - 0b0100
    #Payload Length - 60
    f = io.BytesIO()
    f.write(struct.pack("<III", 0x0000003, 0x00000003, 15))
    f.write(struct.pack(">I", 0x74110020)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0x01000000)) 
    f.write(struct.pack(">I", 0xAAAAAAAA))
    f.seek(0)
    yield f

@pytest.fixture
def simple_bad_packet(): 
    f = io.BytesIO()
    f.write(struct.pack("<IIII", 1, 2, 100, 0))
    f.write(struct.pack("<II", 1, 2))
    f.write(struct.pack("<IIII", 1, 2, 100, 0))
    f.seek(0)
    yield f



@pytest.fixture
def simple_strange_packet(): 
    f = io.BytesIO()
    f.write(struct.pack("<III", 0x0000002, 0x00000002, 8))
    f.write(struct.pack("<I", 0x88883333)) 
    f.write(struct.pack("<I", 0x00000001)) 
    f.write(struct.pack("<I", 0xF0000000)) 
    f.write(struct.pack("<I", 0x0000FFFF)) 
    f.write(struct.pack("<I", 0x00000000)) 
    f.write(struct.pack("<I", 0x10000000)) 
    f.write(struct.pack("<I", 0x1234ABCD)) 
    f.write(struct.pack("<I", 0xCCCCCCCC)) 
    f.seek(0)
    yield f

def test_unknown_packet_write(simple_strange_packet):
#Tests that the correct packet data is passed into the add_record method, used by the PQWriter
    bytes_read, header = read_header(simple_strange_packet)
    assert header[1] > 0

    expected_size = 4 * header[1]
    payload = bytearray(expected_size)
    
    a = np.uint64(bytes_read) #start_bytes
    b = payload 

    assert a == 12
    assert b == bytearray(32)
    

def test_unknown_packet_count(simple_strange_packet):
    parser = Parser(Path(), Path(), DummyWriter)
    parser.read_packet(simple_strange_packet)
    assert parser._unknown_packets > 0
    assert parser._bad_packets == 0

def test_bad_packet_write(simple_bad_packet):
#Tests that the correct packet data is passed into the add_record method, used by the PQWriter    
    bytes_read, header = read_header(simple_bad_packet)
    assert header[1] > 0

    expected_size = 4 * header[1]
    payload = bytearray(expected_size)
    
    a = np.uint64(bytes_read) #start_bytes
    b = payload

    assert a == 12
    assert b == bytearray(400)
 

def test_bad_packet_count(simple_bad_packet):
    parser = Parser(Path(), Path(), DummyWriter)
    with pytest.raises(RuntimeError):
        parser.read_packet(simple_bad_packet)
    assert parser._bad_packets > 0

def test_proper_byteswap(fake_file):
    #Verifies that the byteswapping in read_packet() provides the expected packet_type and payload endinaness
    parser = Parser(Path(), Path(), DummyWriter)
    payload = parser.read_packet(fake_file)

    pkt_header = vita.vrt_header(payload)
    expected_payload = struct.pack(">IIIIIII", 1343881240, 1, 4026531840, 65535, 268435456, 305441741, 3435973836) 
    #(0x501A0018, 0x00000001, 0xF0000000, 0x0000FFFF, 0x10000000, 0x1234ABCD, 0xCCCCCCCC)

    assert pkt_header.packet_type == 5

    assert payload != expected_payload


def test_read_packet(fake_file):

    def add_record(rec: dict):
        assert rec["time_ns"] == 8589934594 
        assert rec["word_count"] == 7
    
    parser =  Parser(Path(), Path(), DummyWriter,
                     recorder_opts={ 'add_record_callback': add_record }
            )
    payload = parser.read_packet(fake_file)
    assert parser._unknown_packets == 0 and parser._bad_packets == 0
    assert parser._packets_read > 0

    expected_payload = struct.pack(">IIIIIII", 402659920, 16777216, 240, 4294901760, 16, 3450549266, 3435973836) 
    #(0x501A0018, 0x00000001, 0xF0000000, 0x0000FFFF, 0x10000000, 0x1234ABCD, 0xCCCCCCCC)
    assert payload == expected_payload
 
def test_read_packetII(fake_fileII):

    def add_record(rec: dict):
        assert rec["time_ns"] == 12884901891 
        assert rec["word_count"] == 15
    
    parser =  Parser(Path(), Path(), DummyWriter,
                     recorder_opts={ 'add_record_callback': add_record }
            )
    payload = parser.read_packet(fake_fileII)

    assert parser._unknown_packets == 0 and parser._bad_packets == 0
    assert parser._packets_read > 0

    expected_payload = struct.pack(">IIIIIIIIIIIIIII", 536875380, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2863311530) 
    #(0x74110020, 0x01000000, 0x01000000, 0x01000000, 0x01000000, 0x01000000, 0x01000000, 0x01000000, 0x01000000, 
    # 0x01000000, 0x01000000, 0x01000000, 0x01000000, 0x01000000, 0xAAAAAAAA)
    assert payload == expected_payload    

@pytest.fixture
def bad_header():
    f = io.BytesIO()
    f.write(struct.pack("<II", 1, 2))
    f.seek(0)
    yield f

def test_custom_packet_read_bad_header(bad_header):
    parser = Parser(Path(), Path(), DummyWriter)

    with pytest.raises(RuntimeError):
        payload = parser.read_packet(bad_header)


@pytest.fixture
def bad_payload():
    f = io.BytesIO()
    f.write(struct.pack("<IIII", 1, 2, 100, 0))
    f.seek(0)
    yield f


def test_custom_packet_read_bad_payload(bad_payload):
    parser = Parser(Path(), Path(), DummyWriter)
    with pytest.raises(RuntimeError):
        parser.read_packet(bad_payload)
    assert parser._bad_packets > 0 


def test_read_custom_packet_empty():
    parser = Parser(Path(), Path(), PQWriter)
    payload = parser.read_packet(io.BytesIO())
    assert payload is None