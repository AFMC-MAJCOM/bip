import pytest
from io import BytesIO
import struct
import numpy as np
import uuid
from pathlib import Path

from bip.non_vita.mblb import mblb_Packet
from bip.non_vita import mblb as mb
from bip.plugins.mikelima.IQ0_packet_data import Process_IQ0_Packet
from bip.plugins.mikelima.IQ5_packet_data import Process_IQ5_Packet
from bip.recorder.dummy.dummywriter import DummyWriter


@pytest.fixture
def fake_packet():
    a = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    b = struct.pack("<QQQ", 0xEFBEEDFEADDEBAAB, 0x0000000000000000, 0x0000000000000000) #SOP Header word 1
    c = struct.pack("<QQQ", 0x000000608805BEAC, 0x0000000000000000, 0x0000000000000000) #SOP Header word 2
    d = struct.pack("<QQQ", 0xAAF3065410013412, 0x0000000000000000, 0x0000000000000000) #SOP Header word 3
    e = struct.pack("<QQQ", 0xECBA00E059EABAAF, 0x0000000000000000, 0x0000000000000000) #SOP Header word 4
    f = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    g = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    h = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    packet = bytearray(a+b+c+d+e+f+g+h)
    yield packet

@pytest.fixture
def fake_packet_bad_event_id():
    a = bytes.fromhex('F17FFF7FFF7FFF7FF17FFF7FFF7FFF7FF17FFF7FFF7FFF7F') #SOP Markers
    b = struct.pack("<QQQ", 0xEFBEEDFEADDEBAAB, 0x0000000000000000, 0x0000000000000000) #SOP Header word 1
    c = struct.pack("<QQQ", 0x0000006088FFBEAC, 0x0000000000000000, 0x0000000000000000) #SOP Header word 2
    d = struct.pack("<QQQ", 0xAAF3065410013412, 0x0000000000000000, 0x0000000000000000) #SOP Header word 3
    e = struct.pack("<QQQ", 0xECBA00E059EABAAF, 0x0000000000000000, 0x0000000000000000) #SOP Header word 4
    f = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    g = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    h = struct.pack("<QQQQ", 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D, 0x0A0B0C0D0A0B0C0D) #Data
    packet = bytearray(a+b+c+d+e+f+g+h)
    yield packet
    

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


def test_process_IQ0_packet(fake_packet, fake_SOM):
    SOP_obj = mblb_Packet(fake_packet[24:120])
    packet_processor =  Process_IQ0_Packet(Path(), DummyWriter)
    _ , payload = fake_SOM
    timestamp = 123456789
    IQ_type = 2
    packet_list_index = 4
    session_id = 15
    SOM_obj = mb.mblb_SOM(payload, timestamp, IQ_type, session_id)
    
    packet_processor.process_packet(fake_packet, SOP_obj, SOM_obj, packet_list_index)
    
    assert packet_processor.time == 123456794
    
    assert np.array_equal(
            packet_processor.left_data[0],
            np.array([0x0c0d, 0x0a0b], dtype = np.int16))
    assert len(packet_processor.left_data) == 12
    
    assert np.array_equal(
            packet_processor.right_data[0],
            np.array([0x0c0d, 0x0a0b], dtype = np.int16))
    assert len(packet_processor.right_data) == 12


def test_process_IQ5_packet(fake_packet, fake_SOM):
    SOP_obj = mblb_Packet(fake_packet[24:120])
    packet_processor =  Process_IQ5_Packet(Path(), DummyWriter)
    _ , payload = fake_SOM
    timestamp = 123456789
    IQ_type = 5
    session_id = 15
    SOM_obj = mb.mblb_SOM(payload, timestamp, IQ_type, session_id)

    packet_processor.process_packet(fake_packet, SOP_obj, SOM_obj, 4)
    
    assert packet_processor.time == 123456794
    
    assert np.array_equal(
            packet_processor.left_data[0],
            np.array([0x0c0d, 0x0a0b], dtype = np.int16))
    assert len(packet_processor.left_data) == 8
    
    assert np.array_equal(
            packet_processor.right_data[0],
            np.array([0x0c0d, 0x0a0b], dtype = np.int16))
    assert len(packet_processor.right_data) == 8
    
    assert np.array_equal(
            packet_processor.center_data[0],
            np.array([0x0c0d, 0x0a0b], dtype = np.int16))
    assert len(packet_processor.center_data) == 8
