from pathlib import Path
from bip.non_vita import mblb

import pyarrow as pa
import numpy as np
import os

LANES = 3
MARKER_BYTES = 8
PACKET_HEADER_BYTES = 32

_IQ5_packet_schema = [
    ("IQ_type", pa.float32()),
    ("packet_number", pa.uint16()),
    ("mode_tag", pa.uint16()),
    ("CI_number", pa.uint32()),
    ("packet_size", pa.uint32()),
    ("data_fmt", pa.uint8()),
    ("event_id", pa.uint8()),
    ("message_number", pa.uint8()),
    ("subCCI_number", pa.uint8()),
    ("BTI_number", pa.uint16()),
    ("RF", pa.uint8()),
    ("CAGC", pa.uint8()),
    ("Rx_beam_id", pa.uint8()),
    ("Rx_config", pa.uint8()),
    ("sample_rate", pa.uint32(), "MSps"),
    ("channelizer_chan", pa.uint16()),
    ("DBF", pa.uint8()),
    ("routing_index", pa.uint8()),
    ("packet_lane1_id", pa.uint8()),
    ("packet_lane2_id", pa.uint8()),
    ("packet_lane3_id", pa.uint8()),
    ("path_id", pa.uint8()),
    ("path_width", pa.uint8()),
    ("subpath_id", pa.uint8()),
    ("subpath_width", pa.uint8()),
    ("DV", pa.uint8()),
    ("RS", pa.uint8()),
    ("valid_channels_beams", pa.uint8()),
    ("channels_beams_per_subpath", pa.uint8()),
    ("AFS_mode", pa.float32()),
    ("SchedNum", pa.float32()),
    ("SIinSchedNum", pa.float32()),
    ("time", pa.float64()),
    ("samples_i_left", pa.list_(pa.int16(), -1)),
    ("samples_q_left", pa.list_(pa.int16(), -1)),
    ("samples_i_right", pa.list_(pa.int16(), -1)),
    ("samples_q_right", pa.list_(pa.int16(), -1)),
    ("samples_i_center", pa.list_(pa.int16(), -1)),
    ("samples_q_center", pa.list_(pa.int16(), -1))
]

def _schema_elt(e: tuple) -> dict:
   if len(e) > 2  and e[2] is not None:
       unit =  { "unit": str(e[2])  }
   else:
       unit = {}

   return {
           "name": e[0],
           "type": str(e[1]),
   } | unit

IQ5_schema = [ _schema_elt(e) for e in _IQ5_packet_schema ]

class Process_IQ5_Packet():
    def __init__(self,
            output_path: Path,
            Recorder: type,
            recorder_opts: dict = {},
            batch_size: int = 10,
            **kwargs):
        self.options = kwargs
        
        self.packet_recorder = Recorder(
                output_path,
                schema=pa.schema([(e[0], e[1]) for e in _IQ5_packet_schema]),
                options=recorder_opts,
                batch_size=batch_size)

        self.packet_id = 0
        self.data = 0
        self.message_key = ''
        self.time = 0


    def __add_record(self,
            packet: mblb.mblb_Packet,
            left_data,
            right_data,
            center_data
            ):
        '''
        Add a row to the packet_content parquet
        '''
        assert isinstance(packet, mblb.mblb_Packet)
    
        self.packet_recorder.add_record({
            "IQ_type": np.float32(self.IQ_type),
            "packet_number": np.uint16(packet.packet_number),
            "mode_tag": np.uint16(packet.mode_tag),
            "CI_number": np.uint32(packet.CI_number),
            "packet_size": np.uint32(packet.packet_size),
            "data_fmt": np.uint8(packet.data_fmt),
            "event_id": np.uint8(packet.event_id),
            "message_number": np.uint8(packet.message_number),
            "subCCI_number": np.uint8(packet.subCCI_number),
            "BTI_number": np.uint16(packet.BTI_number),
            "RF": np.uint8(packet.RF),
            "CAGC": np.uint8(packet.CAGC),
            "Rx_beam_id": np.uint8(packet.Rx_beam_id),
            "Rx_config": np.uint8(packet.Rx_config),
            "sample_rate": np.uint32(1280/(2**packet.Rx_config)),
            "channelizer_chan": np.uint16(packet.channelizer_chan),
            "DBF": np.uint8(packet.DBF),
            "routing_index": np.uint8(packet.routing_index),
            "packet_lane1_id": np.uint8(packet.lane1_ID),
            "packet_lane2_id": np.uint8(packet.lane2_ID),
            "packet_lane3_id": np.uint8(packet.lane3_ID),
            "path_id": np.uint8(packet.path_id),
            "path_width": np.uint8(packet.path_width),
            "subpath_id": np.uint8(packet.subpath_id),
            "subpath_width": np.uint8(packet.subpath_width),
            "DV": np.uint8(packet.DV),
            "RS": np.uint8(packet.RS),
            "valid_channels_beams": np.uint8(packet.valid_channels_beams),
            "channels_beams_per_subpath": 
                np.uint8(packet.channels_beams_per_subpath),
            "AFS_mode": np.float32(self.AFS_mode),
            "SchedNum": np.float32(self.SchedNum),
            "SIinSchedNum": np.float32(self.SIinSchedNum),
            "time": np.float64(self.time / 1000000), #us to s
            "samples_i_left": left_data[:,0],
            "samples_q_left": left_data[:,1],
            "samples_i_right": right_data[:,0],
            "samples_q_right": right_data[:,1],
            "samples_i_center": center_data[:,0],
            "samples_q_center": center_data[:,1]
        })

    @property
    def metadata(self) -> dict :
        return self.packet_recorder.metadata | {"schema": IQ5_schema}
        
    def process_orphan_packet(self, packet: bytearray, SOP_obj):
        data = np.frombuffer(packet, 
                            offset=np.uint64(LANES*(MARKER_BYTES+PACKET_HEADER_BYTES)),
                            count=np.uint64((SOP_obj.packet_size)/2), 
                            dtype = np.int16).reshape((-1, 2))
        self.time = np.nan
        self.left_data = data[::3]
        self.right_data = data[1::3]
        self.center_data = data[2::3]
        self.IQ_type = np.nan
        self.AFS_mode = np.nan
        self.SchedNum = np.nan
        self.SIinSchedNum = np.nan
        self.__add_record(SOP_obj, self.left_data, self.right_data, self.center_data)

    def process_packet(self, stream: bytearray, packet, SOM_obj, packet_list_index):
        '''
        the packet is passed in, now we stream the data (I/Q) into a 
        1 dimensional array of 2 values per element (I/Q)
        The format in the word is IB QB IA QA, laid out across 64 bits with
        B the later sample and A being the earlier sample.  
        
        we know that bytearray starts at SOP and ends 
        right before the next SOP or EOM
        '''

        data = np.frombuffer(stream, 
                            offset=np.uint64(LANES*(MARKER_BYTES+PACKET_HEADER_BYTES)),
                            count=np.uint64((packet.packet_size)/2), 
                            dtype = np.int16).reshape((-1, 2))
        self.left_data = data[::3]
        self.right_data = data[1::3]
        self.center_data = data[2::3]
        self.time = SOM_obj.Time_since_epoch_us + (packet_list_index * SOM_obj.Dwell)
        self.IQ_type = SOM_obj.IQ_type
        self.AFS_mode = SOM_obj.AFS_mode
        self.SchedNum = SOM_obj.SchedNum
        self.SIinSchedNum = SOM_obj.SIinSchedNum
            
        self.__add_record(packet, self.left_data, self.right_data, self.center_data)