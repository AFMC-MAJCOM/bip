import numpy as np
import uuid

class mblb_SOM:

    clocksPerUs = 160 #hardcoded, source unknown

    def __init__(self, payload: bytes, timestamp: int, IQ_type: int, session_id: int):
        self._payload = payload
        self._timestamp = timestamp
        self._IQ_type = IQ_type
        self._session_id = session_id
        self._words = np.frombuffer(self._payload, dtype=np.uint64)
        self._uuid = uuid.uuid4().hex
        
    @property
    def IQ_type(self) -> int:
        return self._IQ_type
    
    @property
    def session_id(self) -> int:
        return self._session_id
        
    @property
    def lane1_ID(self) -> int:
        return (self._words[0] & 0xFF00000000000000) >> np.uint64(56)
    
    @property
    def lane2_ID(self) -> int:
        return (self._words[1] & 0xFF00000000000000) >> np.uint64(56)
    
    @property
    def lane3_ID(self) -> int:
        return (self._words[2] & 0xFF00000000000000) >> np.uint64(56)

    @property
    def CI_number(self) -> int:
        return self._words[0] & np.uint64(0x00000000FFFFFFFF)
        
    @property
    def message_key(self) -> str:
        return self._uuid
    
    @property
    def message_number(self) -> int:
        return (self._words[3] & np.uint64(0x000000000000FF00)) >> np.uint64(8)
    
    @property
    def SI_number(self) -> int:
        return self._words[3] & np.uint64(0x00000000000000FF)
        
    @property
    def path_id(self) -> int:
        return (self._words[9] & np.uint64(0x00FF000000000000)) >> np.uint64(48)
    @property
    def path_width(self) -> int:
        return (self._words[9] & np.uint64(0x0000FF0000000000)) >> np.uint64(40)
    
    @property
    def subpath_id(self) -> int:
        return (self._words[9] & np.uint64(0x000000F000000000)) >> np.uint64(36)
    
    @property
    def subpath_width(self) -> int:
        return (self._words[9] & np.uint64(0x0000000F00000000)) >> np.uint64(32)
 
    @property
    def BE(self) -> int:
        return (self._words[9] & np.uint64(0x0000000080000000)) >> np.uint64(31)
    
    @property
    def beam_select(self) -> int:
        return (self._words[9] & np.uint64(0x0000000060000000)) >> np.uint64(29)
    
    @property
    def AFS_mode(self) -> int:
        return (self._words[9] & np.uint64(0x000000001E000000)) >> np.uint64(25)
    
    @property
    def SchedNum(self) -> int:
        return (self._words[12] & np.uint64(0x00000000FFFF0000)) >> np.uint64(16)
    
    @property
    def SIinSchedNum(self) -> int:
        return self._words[12] & np.uint64(0x000000000000FFFF)
    
    @property
    def high_gain(self) -> int:
        return (self._words[12] & np.uint64(0xFFFFFFFF00000000)) >> np.uint64(32)
    
    @property
    def EventStartTime_us(self) -> int:
        back_half = (self._words[13] & np.uint64(0x00000000FFFFFFFF)) << np.uint64(32)
        front_half = (self._words[13] & np.uint64(0xFFFFFFFF00000000)) >> np.uint64(32)
        return (back_half + front_half) / mblb_SOM.clocksPerUs 
        
    @property
    def Time_since_epoch_us(self) -> int:
        return self.EventStartTime_us + self._timestamp
        
    @property
    def BTI_length(self) -> int:
        return ((self._words[14] & np.uint64(0xFFFFFFFF00000000)) >> np.uint64(32)) / mblb_SOM.clocksPerUs
        
    @property
    def Dwell(self) -> int:
        return (self._words[14] & np.uint64(0x00000000FFFFFFFF)) / mblb_SOM.clocksPerUs
        
    @property
    def freq_GHz(self) -> float:
        Fs              = 2560*16 #MHz
        fineTuneLSB_MHz = 0.625   #MHz
        ctStep          = Fs/128  #320 MHz
        
        ct = int(self._words[15] & np.uint64(0x00000000FFFFFFFF))
        ft = int((self._words[15] & np.uint64(0xFFFFFFFF00000000)) >> np.uint64(32))
        
        fineTune_MHz = ft * fineTuneLSB_MHz

        ctf            = (2**7 - ct)
        cal_ct         = (ctf + 1)/3
        coarseTune_MHz = cal_ct*ctStep*3 - ctStep
        
        return (coarseTune_MHz + fineTune_MHz) / 1000
        
        
class mblb_Packet:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._words = np.frombuffer(self._payload, dtype=np.uint64)


    @property
    def packet_number(self):
        return (self._words[0] & np.uint64(0xFFFF000000000000)) >> np.uint64(48)

    @property
    def mode_tag(self):
        return (self._words[0] & np.uint64(0x0000FFFF00000000)) >> np.uint64(32)

    @property
    def CI_number(self):
        return self._words[0] & np.uint64(0x00000000FFFFFFFF)

    @property
    def packet_size(self):
        return (self._words[3] & np.uint64(0xFFFFFFFF00000000)) >> np.uint64(32)
    
    @property
    def data_fmt(self):
        return (self._words[3] & np.uint64(0x00000000FF000000)) >> np.uint64(24)
    
    @property
    def event_id(self):
        return (self._words[3] & np.uint64(0x0000000000FF0000)) >> np.uint64(16)

    @property
    def message_number(self):
        return (self._words[3] & np.uint64(0x000000000000FF00)) >> np.uint64(8)
        
    @property
    def subCCI_number(self):
        return self._words[3] & np.uint64(0x00000000000000FF)
    
    @property
    def BTI_number(self):
        return (self._words[6] & np.uint64(0xFFFF000000000000)) >> np.uint64(48)
    
    @property
    def RF(self):
        return (self._words[6] & np.uint64(0x0000FFC000000000)) >> np.uint64(38)
    
    @property
    def CAGC(self):
        return (self._words[6] & np.uint64(0x0000003F00000000)) >> np.uint64(28)
    
    @property
    def Rx_beam_id(self):
        return (self._words[6] & np.uint64(0x00000000FF000000)) >> np.uint64(24)

    @property
    def Rx_config(self):
        return (self._words[6] & np.uint64(0x0000000000FC0000)) >> np.uint64(18) 
    
    @property
    def channelizer_chan(self):
        return (self._words[6] & np.uint64(0x000000000003F000)) >> np.uint64(12) 
    
    @property
    def DBF(self):
        return (self._words[6] & np.uint64(0x0000000000000F00)) >> np.uint64(8) 
    
    @property
    def routing_index(self):
        return self._words[6] & np.uint64(0x00000000000000FF)
    
    @property
    def lane1_ID(self):
        return (self._words[9] & np.uint64(0xFF00000000000000)) >> np.uint64(56)
    
    @property
    def lane2_ID(self):
        return (self._words[10] & np.uint64(0xFF00000000000000)) >> np.uint64(56)
    
    @property
    def lane3_ID(self):
        return (self._words[11] & np.uint64(0xFF00000000000000)) >> np.uint64(56)
    
    @property
    def path_id(self) -> int:
        return (self._words[9] & np.uint64(0x00FF000000000000)) >> np.uint64(48)
    
    @property
    def path_width(self) -> int:
        return (self._words[9] & np.uint64(0x0000FF0000000000)) >> np.uint64(40)
    
    @property
    def subpath_id(self) -> int:
        return (self._words[9] & np.uint64(0x000000F000000000)) >> np.uint64(36)
    
    @property
    def subpath_width(self) -> int:
        return (self._words[9] & np.uint64(0x0000000F00000000)) >> np.uint64(32)
    
    @property
    def DV(self) -> int:
        return (self._words[9] & np.uint64(0x0000000080000000)) >> np.uint64(31)
    
    @property
    def RS(self) -> int:
        return (self._words[9] & np.uint64(0x0000000060000000)) >> np.uint64(30)
    
    @property
    def valid_channels_beams(self) -> int:
        return (self._words[9] & np.uint64(0x000000000000FF00)) >> np.uint64(8)
    
    @property
    def channels_beams_per_subpath(self) -> int:
        return self._words[9] & np.uint64(0x00000000000000FF)

        
        
class mblb_EOM:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._words = np.frombuffer(self._payload, dtype=np.uint64)
    
    @property
    def packet_count(self) -> int:
        return (self._words[0] & np.uint64(0xFFFF000000000000)) >> np.uint64(48)

    @property
    def CI_number(self) -> int:
        return self._words[0] & np.uint64(0x00000000FFFFFFFF)

    @property
    def error_status(self) -> int:
        return (self._words[1] & np.uint64(0xFFFFFFFFFFFF0000)) >> np.uint64(16)

    @property
    def message_number(self):
        return (self._words[1] & np.uint64(0x000000000000FF00)) >> np.uint64(8)
    
    @property
    def subCCI_number(self):
        return self._words[1] & np.uint64(0x00000000000000FF)

    @property
    def CRC(self) -> int:
        return int(self._words[2]) 
    
    @property
    def lane1_ID(self) -> int:
        return (self._words[9] & np.uint64(0xFF00000000000000)) >> np.uint64(56)
    
    @property
    def lane2_ID(self) -> int:
        return (self._words[10] & np.uint64(0xFF00000000000000)) >> np.uint64(56)
    
    @property
    def lane3_ID(self) -> int:
        return (self._words[11] & np.uint64(0xFF00000000000000)) >> np.uint64(56)

    @property
    def path_id(self) -> int:
        return (self._words[9] & np.uint64(0x00FF000000000000)) >> np.uint64(48)
    @property
    def path_width(self) -> int:
        return (self._words[9] & np.uint64(0x0000FF0000000000)) >> np.uint64(40) 
     
    @property
    def subpath_id(self) -> int:
        return (self._words[9] & np.uint64(0x000000F000000000)) >> np.uint64(36)  
    
    @property
    def subpath_width(self) -> int:
        return (self._words[9] & np.uint64(0x0000000F00000000)) >> np.uint64(32)
    

