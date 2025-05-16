import pytest
import numpy as np
from io import BytesIO
from bip.non_vita import mblb as mb

@pytest.fixture
def fake_som():
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
def fake_sop():
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
def fake_eom():
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


def test_som(fake_som):
    _ , payload = fake_som
    timestamp = 123456789
    iq_type = 5
    session_id = 15
    increment = 4
    timestamp_from_filename = 19411207120000
    mblb_som_obj = mb.MblbSOM(payload, timestamp, iq_type, session_id, increment, timestamp_from_filename)
    assert mblb_som_obj.iq_type == 5
    assert mblb_som_obj.session_id == 15
    assert mblb_som_obj.lane1_id == 0x0A
    assert mblb_som_obj.lane2_id == 0x0B
    assert mblb_som_obj.lane3_id == 0x0C
    assert mblb_som_obj.message_number == 0x11
    assert mblb_som_obj.ci_number == 0x0306
    assert mblb_som_obj.si_number == 0x22
    assert mblb_som_obj.path_id == 0x33
    assert mblb_som_obj.path_width == 0x44
    assert mblb_som_obj.subpath_id == 0x5
    assert mblb_som_obj.subpath_width == 0x6
    assert mblb_som_obj.be == 1
    assert mblb_som_obj.beam_select == 0b11
    assert mblb_som_obj.afs_mode == 0b0101
    assert mblb_som_obj.sched_num == 0xDEAD
    assert mblb_som_obj.si_in_sched_num == 0xBEEF
    assert mblb_som_obj.high_gain == 0xFEEDBEEF
    assert mblb_som_obj.event_start_time_us == 1
    assert mblb_som_obj.bti_length == 1
    assert mblb_som_obj.dwell == 1
    assert abs(mblb_som_obj.freq_ghz - 4.560) < 1e-6
    assert isinstance(mblb_som_obj.message_key, str)
    assert mblb_som_obj._timestamp == 123456789

def test_sop(fake_sop):
    _, payload = fake_sop
    mblb_sop_obj = mb.MblbPacket(payload)
    assert mblb_sop_obj.packet_number == 0xABBA
    assert mblb_sop_obj.mode_tag == 0xDEAD
    assert mblb_sop_obj.ci_number == 0xFEEDBEEF
    assert mblb_sop_obj.packet_size == 0xDEADABBA
    assert mblb_sop_obj.data_fmt == 0xAC
    assert mblb_sop_obj.event_id == 0xBE
    assert mblb_sop_obj.message_number == 0x9A
    assert mblb_sop_obj.sub_cci_number == 0x88
    assert mblb_sop_obj.bti_number == 0x1234
    assert mblb_sop_obj.rf == 4
    assert mblb_sop_obj.cagc == 256
    assert mblb_sop_obj.rx_beam_id == 0x54
    assert mblb_sop_obj.rx_config == 1
    assert mblb_sop_obj.channelizer_chan == 47
    assert mblb_sop_obj.dbf == 3
    assert mblb_sop_obj.routing_index == 0xAA
    assert mblb_sop_obj.lane1_id == 0xAD
    assert mblb_sop_obj.lane2_id == 0xAE
    assert mblb_sop_obj.lane3_id == 0xAF
    assert mblb_sop_obj.path_id == 0xBA
    assert mblb_sop_obj.path_width == 0xEA
    assert mblb_sop_obj.subpath_id == 0x5
    assert mblb_sop_obj.subpath_width == 0x9
    assert mblb_sop_obj.dv == 1
    assert mblb_sop_obj.rs == 1
    assert mblb_sop_obj.valid_channels_beams == 0xBA
    assert mblb_sop_obj.channels_beams_per_subpath == 0xEC

def test_eom(fake_eom):
    _ , payload = fake_eom
    mblb_eom_obj = mb.MblbEOM(payload)
    assert mblb_eom_obj.packet_count == 0xDEAD
    assert mblb_eom_obj.ci_number == 0xFEEDBEEF
    assert mblb_eom_obj.error_status == 0xFEEDDEADBEEF
    assert mblb_eom_obj.message_number == 0xBC
    assert mblb_eom_obj.sub_cci_number == 0xAE
    assert mblb_eom_obj.crc == 0xFEEDDEADBEEFBABA
    assert mblb_eom_obj.lane1_id == 0x4
    assert mblb_eom_obj.lane2_id == 0x5
    assert mblb_eom_obj.lane3_id == 0x6
    assert mblb_eom_obj.path_id == 0xCB
    assert mblb_eom_obj.path_width == 0xAE
    assert mblb_eom_obj.subpath_id == 0xE
    assert mblb_eom_obj.subpath_width == 0xF
