from io import RawIOBase
import datetime
from bip.non_vita import mblb

def _SOM_search(b: bytes) -> int:
    '''
    Method to confirm presence of the header marker
    exceptions raised are passed in read_SOM
    '''
    if len(b) == 0:
        return None
    elif len(b) != 8:
        raise RuntimeError(f"incomplete read of SOM header")
    if b == bytes.fromhex('F07FFF7FFF7FFF7F'):
        return True
    else:
        raise RuntimeError("start not found")


def Determine_timestamp(header: bytearray):
    '''
    This function is based on the mikelima bins that we have seen. If future 
    bins have a slightly different header formet then this will all break.
	'''
    year = str(header[38:42].decode("utf-8"))
    julian_day = str(header[43:46].decode("utf-8"))
    h = int(header[47:49].decode("utf-8"))
    m = int(header[50:52].decode("utf-8"))
    s = int(header[53:55].decode("utf-8"))
    us = int(header[56:62].decode("utf-8"))
    fmt = '%Y%j'
    
    date_std = datetime.datetime.strptime(f"{year}{julian_day}", fmt).date()
    year = int(date_std.year)
    month = int(date_std.month)
    day = int(date_std.day)
    
    epoch_string = f"{year}-{month}-{day}_{h}:{m}:{s}.{us}"
    Time_Format = '%Y-%m-%d_%H:%M:%S.%f'
    
    epoch = datetime.datetime.strptime(epoch_string,Time_Format).timestamp()

    return int(epoch*(10**6))
    
def Determine_IQ_and_Session(header:bytearray):
    '''
    This function asuumes that the full file path of the bin file will appear
    in ascii at the start of the bin file.
    If the bin stops recording this or if the file path format changes then this
    will break
    '''
    header_string = header.decode("ascii", 'ignore')

    header_string_parts = header_string.split('/')
    
    IQ = header_string_parts[5]
    Session = header_string_parts[4]
    
    assert IQ[0:2] == "IQ"
    assert Session[0:5] == "Sessn"
    
    return int(IQ[2]), int(Session[5:8])

def unpack_SOM(header_bytes: bytes, stream: RawIOBase):
    '''
    This method unpacks the SOM header
    '''
    assert len(header_bytes) == 8
    buffer = stream.read(16)
    if buffer != bytes.fromhex('F07FFF7FFF7FFF7FF07FFF7FFF7FFF7F'):
        raise RuntimeError("unexpected SOM format")
        
    # 3 lanes of 4 words plus 24 SDW is 36 8-byte words
    bytes_out = bytearray(36*8)
    
    number_of_bytes_read= stream.readinto(bytes_out)
    if number_of_bytes_read != (36*8):
        raise RuntimeError('Stop, unexpected number_of_bytes_read in unpack_SOM')

    return bytes_out, number_of_bytes_read

def read_first_header(stream: RawIOBase):
    orphan_packet_list = []
    header = bytearray()
    #read first 8 byte word
    buffer = stream.read(8)
    while True:
        try:
            _SOM_search(buffer)
            break
        except Exception as e:
            if buffer == bytes.fromhex('F17FFF7FFF7FFF7F'):
                orphan_packet = bytearray()
                #16 bytes are the headers for the other 2 lanes
                #Packet headers consist of 4 8-byte words
                #Each of the 3 lanes have their own header
                orphan_packet_header = stream.read((16 + 3*32))
                SOP_obj = mblb.mblb_Packet(orphan_packet_header[16:112])
                orphan_packet_data = stream.read(SOP_obj.packet_size)
                orphan_packet += buffer + orphan_packet_header + orphan_packet_data
                orphan_packet_list.append(orphan_packet)
            else:
                header += buffer
            buffer = stream.read(8)
            pass
    if buffer == bytes.fromhex('F07FFF7FFF7FFF7F'):
        stream.seek(-8,1)
        timestamp = Determine_timestamp(header)
        IQ_type, Session_id = Determine_IQ_and_Session(header)
        return len(header), orphan_packet_list, timestamp, IQ_type, Session_id
    else:
        raise RuntimeError("cannot find first message")

def read_message_header(stream: RawIOBase):
    ''' 
    This method will read 8 bytes at a time, looking for the SOM
    marker then unpack the SOM header, right now this will
    remove the pseudo preamble at the start of the bin file, we may want
    to use that later if it becomes meaningful.
    '''
    buffer = stream.read(8)
    while True:
        try:
            _SOM_search(buffer)
            break
        except Exception as e:
            buffer = stream.read(8)
            pass
    if buffer == bytes.fromhex('F07FFF7FFF7FFF7F'):
        return unpack_SOM(buffer, stream)
    else:
        return 0,0
