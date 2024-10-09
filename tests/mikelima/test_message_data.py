import pytest
from io import BytesIO
import struct
import uuid

from pathlib import Path

from bip.recorder.dummy.dummywriter import DummyWriter
from bip.plugins.mikelima.message_data import Process_Message
from bip.non_vita import mblb as mb

@pytest.fixture
def fake_SOM():
    content = [
            0x060300000000000A, # word0-Lane1_word1-CI_Number, Lane1_ID
            0x060300000000000B, # word1-Lane2_word1-CI_Number, Lane2_ID
            0x060300000000000C, # word2-Lane3_word1-CI_Number, Lane3_ID
            0x221100000000000A, # word3-Lane1_word2-msg_num, SI_num
            0x000000000000000B, # word4-Lane2_word2
            0x000000000000000C, # word5-Lane3_word2
            0x000000000000000A, # word6-Lane1_word3
            0x000000000000000B, # word7-Lane2_word3
            0x000000000000000C, # word8-Lane3_word3
            0x000000EA5644330A, # word9-Lane1_word4-
                                #   path/subpath ID and width
                                #   BE, beam_select, AFS_mode
            0x000000000000000B, # word10-Lane2_word4
            0x000000000000000C, # word11-Lane3_word4
            0x16001500EFBEEDFE, # word12-SDW1-
                                #   SchedNum and SI_in_SchedNum
                                #   high_gain
            0x00000000A0000000, # word13-SDW2-Event_StartTime
                                #   watch the flipping and endianness on this one
            0xA0000000A0000000, # word14-SDW3-BTI_length and Dwell
            0x7200000080000000, # word15-SDW4-frequency
            0x0000000000000000, # word16-SDW5
            0x0000000000000000, # word17-SDW6
            0x0000000000000000, # word18-SDW7
            0x0000000000000000, # word19-SDW8
            0x0000000000000000, # word20-SDW9
            0x0000000000000000, # word21-SDW10
            0x0000000000000000, # word22-SDW11
            0x0000000000000000, # word23-SDW12
            0x0000000000000000, # word24-SDW13
            0x0000000000000000, # word25-SDW14
            0x0000000000000000, # word26-SDW15
            0x0000000000000000, # word27-SDW16
            0x0000000000000000, # word28-SDW17
            0x0000000000000000, # word29-SDW18
            0x0000000000000000, # word30-SDW29
            0x0000000000000000, # word31-SDW20
            0x0000000000000000, # word32-SDW21
            0x0000000000000000, # word33-SDW22
            0x0000000000000000, # word34-SDW23
            0x0000000000000000, # word35-SDW24
            ]
    f = BytesIO()
    for c in content:
        f.write(c.to_bytes(8, byteorder='big'))
    return content, f.getvalue()
    

@pytest.fixture
def fake_packet():
    a = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    b = struct.pack("<QQQ", 0xEFBEEDFEADDEBAAB, 0x0000000000000000, 0x0000000000000000) #SOP Header word 1
    c = struct.pack("<QQQ", 0x00000060889ABEAC, 0x0000000000000000, 0x0000000000000000) #SOP Header word 2
    d = struct.pack("<QQQ", 0xAAF3065410513412, 0x0000000000000000, 0x0000000000000000) #SOP Header word 3
    e = struct.pack("<QQQ", 0xECBA00E059EABAAF, 0x0000000000000000, 0x0000000000000000) #SOP Header word 4
    f = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    g = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    h = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    packet = bytearray(a+b+c+d+e+f+g+h)
    yield packet

@pytest.fixture
def fake_message(fake_packet):
    fake_message = fake_packet + fake_packet + fake_packet
    yield fake_message

def test_process_message(fake_message, fake_SOM):
    _ , payload = fake_SOM
    timestamp = 123456789
    IQ_type = 5
    session_id = 15
    increment = 4
    timestamp_from_filename = 19411207120000
    fake_SOM_obj = mb.mblb_SOM(payload, timestamp, IQ_type, session_id, increment, timestamp_from_filename)
    
    message_processor =  Process_Message(Path(), DummyWriter, IQ_type = IQ_type)
    number_of_packets = message_processor.process_msg(fake_message, fake_SOM_obj)
    
    assert number_of_packets == 3


def test_read_packets(fake_message, fake_packet):
    message_processor =  Process_Message(Path(), DummyWriter)
    
    packet_list = message_processor.read_packets(fake_message)

    assert len(packet_list) == 3
    assert list(packet_list[0]) == list(fake_packet)
    
def test_read_orphan_packets(fake_packet):
    IQ_type = 5
    session_id = 15
    increment = 4
    timestamp_from_filename = 19411207120000
    
    fake_packet_list = [fake_packet, fake_packet]
    message_processor =  Process_Message(Path(), DummyWriter)
    
    orphan_packet_number = message_processor.process_orphan_packets(fake_packet_list, IQ_type, session_id, increment, timestamp_from_filename)
    
    assert orphan_packet_number == 2