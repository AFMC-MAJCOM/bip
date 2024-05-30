import pytest
import numpy as np
from io import BytesIO
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
            0xEFBEADDEEFBEEDFE, # word12-SDW1-
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
def fake_SOP():
    content = [
            0xEFBEEDFEADDEBAAB, # word0-Lane1_word1-
                                #   packet_number, mode_tag, CI_number
            0x0000000000000000, # word1-Lane2_word1
            0x0000000000000000, # word2-Lane3_word1
            0x889ABEACBAABADDE, # word3-Lane1_word2
                                #   packet_size, data_fmt
                                #   event_id, message_number, subCCI_number
            0x0000000000000000, # word4-Lane2_word2
            0x0000000000000000, # word5-Lane3_word2
            0xAAF3065410013412, # word6-Lane1_word3-
                                #   BTI_number, RF, CAGC, 
                                #   Rx_beam_id, Rx_config, 
                                #   channelizer_chan, DBF,routing_index
            0x0000000000000000, # word7-Lane2_word3
            0x0000000000000000, # word8-Lane3_word3
            0xECBA00E059EABAAD, # word9-Lane1_word4-
                                #   lane1_ID, path_id, path_width,
                                #   subpath_id, subpath_width, DV, RS, 
                                #   valid_channels_beams,
                                #   channels_beams_per_subpath
            0x00000000000000AE, # word10-Lane2_word4
            0x00000000000000AF, # word11-Lane3_word4
            ]
    f = BytesIO()
    for c in content:
        f.write(c.to_bytes(8, byteorder='big'))
    return content, f.getvalue()
    
@pytest.fixture
def fake_EOM():
    content = [
            0xEFBEEDFE0000ADDE, # word0-Lane1_word1-
                                #   packet_count, mode_tag, CI_number
            0xAEBCEFBEADDEEDFE, # word1-Lane2_word1- 
                                #   error status, message_number, subCCI_number
            0xBABAEFBEADDEEDFE, # word2-Lane3_word1- CRC
            0x0000000000000000, # word3-Lane1_word2
            0x0000000000000000, # word4-Lane2_word2
            0x0000000000000000, # word5-Lane3_word2
            0x0000000000000000, # word6-Lane1_word3
            0x0000000000000000, # word7-Lane2_word3
            0x0000000000000000, # word8-Lane3_word3
            0x00000000EFAECB04, # word9-Lane1_word4-
                                #   lane1_ID, path_id, path_width
                                #   subpath_id, subpath_width
            0x0000000000000005, # word10-Lane2_word4-lane2_ID
            0x0000000000000006, # word11-Lane3_word4-lane3_ID
            ]
    f = BytesIO()
    for c in content:
        f.write(c.to_bytes(8, byteorder='big'))
    return content, f.getvalue()
    
    
def test_SOM(fake_SOM):
    _ , payload = fake_SOM
    timestamp = 123456789
    IQ_type = 5
    session_id = 15
    mblb_SOM_obj = mb.mblb_SOM(payload, timestamp, IQ_type, session_id)
    assert mblb_SOM_obj.IQ_type == 5
    assert mblb_SOM_obj.session_id == 15
    assert mblb_SOM_obj.lane1_ID == 0x0A
    assert mblb_SOM_obj.lane2_ID == 0x0B
    assert mblb_SOM_obj.lane3_ID == 0x0C
    assert mblb_SOM_obj.message_number == 0x11
    assert mblb_SOM_obj.CI_number == 0x0306
    assert mblb_SOM_obj.SI_number == 0x22
    assert mblb_SOM_obj.path_id == 0x33
    assert mblb_SOM_obj.path_width == 0x44
    assert mblb_SOM_obj.subpath_id == 0x5
    assert mblb_SOM_obj.subpath_width == 0x6
    assert mblb_SOM_obj.BE == 1
    assert mblb_SOM_obj.beam_select == 0b11
    assert mblb_SOM_obj.AFS_mode == 0b0101
    assert mblb_SOM_obj.SchedNum == 0xDEAD
    assert mblb_SOM_obj.SIinSchedNum == 0xBEEF
    assert mblb_SOM_obj.high_gain == 0xFEEDBEEF
    assert mblb_SOM_obj.EventStartTime_us == 1
    assert mblb_SOM_obj.BTI_length == 1
    assert mblb_SOM_obj.Dwell == 1
    assert mblb_SOM_obj.freq_GHz == 4.560
    assert isinstance(mblb_SOM_obj.message_key, str)
    assert mblb_SOM_obj._timestamp == 123456789
    
def test_SOP(fake_SOP):
    _, payload = fake_SOP
    mblb_SOP_obj = mb.mblb_Packet(payload)
    assert mblb_SOP_obj.packet_number == 0xABBA
    assert mblb_SOP_obj.mode_tag == 0xDEAD
    assert mblb_SOP_obj.CI_number == 0xFEEDBEEF
    assert mblb_SOP_obj.packet_size == 0xDEADABBA
    assert mblb_SOP_obj.data_fmt == 0xAC
    assert mblb_SOP_obj.event_id == 0xBE    
    assert mblb_SOP_obj.message_number == 0x9A
    assert mblb_SOP_obj.subCCI_number == 0x88
    assert mblb_SOP_obj.BTI_number == 0x1234
    assert mblb_SOP_obj.RF == 4
    assert mblb_SOP_obj.CAGC == 256
    assert mblb_SOP_obj.Rx_beam_id == 0x54
    assert mblb_SOP_obj.Rx_config == 1
    assert mblb_SOP_obj.channelizer_chan == 47
    assert mblb_SOP_obj.DBF == 3
    assert mblb_SOP_obj.routing_index == 0xAA
    assert mblb_SOP_obj.lane1_ID == 0xAD
    assert mblb_SOP_obj.lane2_ID == 0xAE
    assert mblb_SOP_obj.lane3_ID == 0xAF
    assert mblb_SOP_obj.path_id == 0xBA
    assert mblb_SOP_obj.path_width == 0xEA
    assert mblb_SOP_obj.subpath_id == 0x5
    assert mblb_SOP_obj.subpath_width == 0x9
    assert mblb_SOP_obj.DV == 1
    assert mblb_SOP_obj.RS == 1
    assert mblb_SOP_obj.valid_channels_beams == 0xBA
    assert mblb_SOP_obj.channels_beams_per_subpath == 0xEC

def test_EOM(fake_EOM):
    _ , payload = fake_EOM
    mblb_EOM_obj = mb.mblb_EOM(payload)
    assert mblb_EOM_obj.packet_count == 0xDEAD
    assert mblb_EOM_obj.CI_number == 0xFEEDBEEF
    assert mblb_EOM_obj.error_status == 0xFEEDDEADBEEF
    assert mblb_EOM_obj.message_number == 0xBC
    assert mblb_EOM_obj.subCCI_number == 0xAE
    assert mblb_EOM_obj.CRC == 0xFEEDDEADBEEFBABA
    assert mblb_EOM_obj.lane1_ID == 0x4
    assert mblb_EOM_obj.lane2_ID == 0x5
    assert mblb_EOM_obj.lane3_ID == 0x6
    assert mblb_EOM_obj.path_id == 0xCB
    assert mblb_EOM_obj.path_width == 0xAE
    assert mblb_EOM_obj.subpath_id == 0xE
    assert mblb_EOM_obj.subpath_width == 0xF
