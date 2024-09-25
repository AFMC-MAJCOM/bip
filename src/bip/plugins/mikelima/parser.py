from io import RawIOBase
from pathlib import Path
import traceback

import numpy as np
import pyarrow as pa 

from bip import non_vita

from . __version__ import __version__ as version
from . import header
from . message_data import Process_Message
from . import message_data
from bip.non_vita import mblb

MESSAGE_FILENAME="message_content"
PACKET_FILENAME="packet_content"
UNHANDLED_MARKERS = [bytes.fromhex('F37FFF7FFF7FFF7F'), 
                    bytes.fromhex('F77FFF7FFF7FFF7F'), 
                    bytes.fromhex('F87FFF7FFF7FFF7F'),
                    bytes.fromhex('F97FFF7FFF7FFF7F'),
                    bytes.fromhex('FA7FFF7FFF7FFF7F')]
LANES = 3
MARKER_BYTES = 8
HEADER_BYTES = 32
EOM_BYTES = 24

class Parser:
    options: dict
    _bytes_read: int
    _packets_read: int
    _messages_read: int

    def __init__(self,
            input_path: Path,
            output_path: Path,
            Recorder: type,  #class instance to be instantiated
            log_level = None,
            data_recorder: type=None,
            context_key_function=None,
            recorder_opts: dict = {},
            **kwargs
            ):
        self._output_path = output_path
        self._recorder = Recorder
        self._recorder_options = recorder_opts
        

        self.options = kwargs
        self._bytes_read = 0
        self._packets_read = 0
        self._messages_read = 0
        self._message_index = 0
        self._message_key = ''
        self._timestamp = 0
        self._IQ_type = 0
        self._session_id = 0

        self._closed = False  
        
        if not data_recorder:
            data_recorder = Recorder

    @property
    def metadata(self) -> dict:
        return {
                'name': 'MikeLima parser',
                'version': version,
                'options': self.options,
                'messages_read': self._messages_read,
        } 

    @property
    def bytes_read(self) -> int:
        return self._bytes_read

    @property
    def messages_read(self) -> int:
        return self._messages_read

    @property
    def packets_read(self) -> int:
        return self._packets_read
        
    def initialize_message_processor(self, IQ_type: int):
        message_data_filename = f"{MESSAGE_FILENAME}.{self._recorder.extension()}"
        
        self.message_processor = Process_Message(
                self._output_path,
                self._recorder,
                options = self._recorder_options,
                batch_size = 10,
                IQ_type = IQ_type)
        self.options["message_data"] = {
                "filename": message_data_filename
        } | self.message_processor.metadata

        self.message_recorder = self._recorder(
            self._output_path / message_data_filename,
            schema= pa.schema([(e[0], e[1]) for e in message_data._message_schema]),
            options=self._recorder_options,
            batch_size=10)
    
    def read_message(self, buf: RawIOBase):
        '''
        Take in 1 64-bit word at a time, looking for EOM marker
        return the bytearray of the entire message.
        Will raise exception if we encounter any of the unhandled markers
        '''
        header_data, bytes_read = header.read_message_header(buf) # Bytearray and integer
        if bytes_read == 0: 
            if self.message_recorder.writer != None: 
                self.message_recorder.close()
            return None

        SOM_obj = mblb.mblb_SOM(header_data, self._timestamp, self._IQ_type, self._session_id)
        self._message_key = SOM_obj.message_key

        message = bytearray(8)
        message_size = buf.readinto(message)
        
        # Look for SOP not EOM
        # read packet size of packet
        while message[-8:] != bytes.fromhex('F27FFF7FFF7FFF7F'):
            if message[-8:] == bytes.fromhex('F17FFF7FFF7FFF7F'):
                #16 bytes for the packet markers for the other 2 lanes
                #plus 3 32-byte headers
                packet_header = bytearray(16 + (LANES*HEADER_BYTES))

                header_length = buf.readinto(packet_header)
                if header_length != (16+(LANES*HEADER_BYTES)):
                    print('Message is broken in the packet header')
                    blank_end_of_message = bytearray(24*8)
                    blank_EOM_obj = mblb.mblb_EOM(blank_end_of_message)
                    self.__add_record(SOM_obj, blank_EOM_obj)
        
                    self._bytes_read += bytes_read + message_size
                    return message[:-8], SOM_obj
                SOP_obj = mblb.mblb_Packet(packet_header[16:(16+(LANES*HEADER_BYTES))])
                packet_data = bytearray(SOP_obj.packet_size)
                data_length = buf.readinto(packet_data)
                if data_length != SOP_obj.packet_size:
                    print('Message is broken in the packet data')
                    blank_end_of_message = bytearray(EOM_BYTES*8)
                    blank_EOM_obj = mblb.mblb_EOM(blank_end_of_message)
                    self.__add_record(SOM_obj, blank_EOM_obj)
        
                    self._bytes_read += bytes_read + message_size
                    return message[:-8], SOM_obj
                
                message += packet_header + packet_data
                message_size += header_length + data_length

            additional_message = bytearray(8)
            additional_message_length = buf.readinto(additional_message)
           
            if additional_message in UNHANDLED_MARKERS:
                raise Exception('This Bin file contains unhandled markers')
            if additional_message_length != 8:
                print('Message is broken no EOM found')
                blank_end_of_message = bytearray(EOM_BYTES*8)
                blank_EOM_obj = mblb.mblb_EOM(blank_end_of_message)
                self.__add_record(SOM_obj, blank_EOM_obj)
        
                self._bytes_read += bytes_read + message_size
                return message, SOM_obj

            message += additional_message
            message_size += additional_message_length
            
        #Read the EOM markers for the other 2 lanes
        EOM_marker = bytearray(16)
        buf.readinto(EOM_marker)
        assert EOM_marker == bytes.fromhex('F27FFF7FFF7FFF7FF27FFF7FFF7FFF7F')
        #EOM is 24 8-byte WORDS
        end_of_message = bytearray(EOM_BYTES*8)
        buf.readinto(end_of_message)
        EOM_obj = mblb.mblb_EOM(end_of_message)

        self.__add_record(SOM_obj, EOM_obj)
        
        self._bytes_read += bytes_read + message_size + 16 + (EOM_BYTES*8)
        return message[:-8], SOM_obj
    
    def __add_record(self,
            message: mblb.mblb_SOM,
            end: mblb.mblb_EOM
            ):
        '''
        Add a row to the message_content parquet
        '''
        assert isinstance(message, mblb.mblb_SOM)
        assert isinstance(end, mblb.mblb_EOM)
    
        self.message_recorder.add_record({
            "IQ_type": np.uint8(message.IQ_type),
            "session_id": np.uint8(message.session_id),
            "SOM_lane1_id": np.uint8(message.lane1_ID),
            "SOM_lane2_id": np.uint8(message.lane2_ID),
            "SOM_lane3_id": np.uint8(message.lane3_ID),
            "CI_number": np.uint32(message.CI_number),
            "message_key": str(message.message_key),
            "SOM_message_number": np.uint8(message.message_number),
            "SOM_SI_number": np.uint8(message.SI_number),
            "SOM_path_id": np.uint8(message.path_id),
            "SOM_path_width": np.uint8(message.path_width),
            "SOM_subpath_id": np.uint8(message.subpath_id),
            "SOM_subpath_width": np.uint8(message.subpath_width),
            "BE": np.uint8(message.BE),
            "Beam_select": np.uint8(message.beam_select),
            "AFS_mode": np.uint8(message.AFS_mode),
            "High_gain": np.uint32(message.high_gain),
            "Schedule_number": np.uint16(message.SchedNum),
            "SI_in_schedule_number": np.uint16(message.SIinSchedNum),
            "Event_start_time": np.uint64(message.EventStartTime_us),
            "message_time": np.uint64(message.Time_since_epoch_us),
            "BTI_length": np.uint32(message.BTI_length),
            "Dwell": np.uint32(message.Dwell),
            "Frequency_Message": np.float32(message.freq_GHz),
            "Packet_Count": np.uint16(end.packet_count),
            "EOM_CI_number": np.uint32(end.CI_number),
            "Error_status": np.uint64(end.error_status),
            "EOM_message_number": np.uint16(end.message_number),
            "SubCCI_number": np.uint16(end.subCCI_number),
            "CRC": np.uint64(end.CRC),
            "EOM_lane1_id": np.uint16(end.lane1_ID),
            "EOM_lane2_id": np.uint16(end.lane2_ID),
            "EOM_lane3_id": np.uint16(end.lane3_ID),
            "EOM_path_id": np.uint16(end.path_id),
            "EOM_path_width": np.uint16(end.path_width),
            "EOM_subpath_id": np.uint8(end.subpath_id),
            "EOM_subpath_width": np.uint8(end.subpath_width)
        })

    def parse_stream(self, stream: RawIOBase, progress_bar=None):
        '''
        Process the stream for the messages
        '''
        if progress_bar is not None:
            last_read = 0
        
        num_bytes_read, orphan_packet_list, self._timestamp, self._IQ_type, self._session_id = header.read_first_header(stream)
        
        self.initialize_message_processor(self._IQ_type)
        
        self._bytes_read += num_bytes_read
        
        orphan_packet_number = self.message_processor.process_orphan_packets(orphan_packet_list)
        
        msg_words, SOM_obj = self.read_message(stream) #bytearray of the whole message
        while msg_words:
            
            num_packets = self.message_processor.process_msg(msg_words, SOM_obj) # break into packets
            self._messages_read += 1
            self._packets_read += num_packets
            if progress_bar is not None:
                progress_bar.update(self._bytes_read - last_read)
                last_read = self._bytes_read

            try:
                msg_words, SOM_obj = self.read_message(stream)
            except:
                msg_words = None
      
