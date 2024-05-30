import numpy as np
from . vrt_packet import VRTPacket
from . class_identifier import ClassIdentifier


# right now this has a lot of assumptions about what the data look like
# assumes stream id, class id, are in place, assumes the data is time series
# not frequency

class SignalDataIndicators:
    #signal data packets define the indicators as:
    # trailer included
    # not vita 49.0 packet indicator
    # signal spectrum / signal time data packet

    def __init__(self, header):
        self.value = header

    @property
    def trailer(self) -> bool:
        return bool(self.value & (1 << 26))

    @property
    def not_vita_49_0(self) -> bool:
        return bool(self.value & (1 << 25))

    @property
    def signal_spectrum(self) -> bool:
        return bool(self.value & (1 << 24))





class SignalDataPacket(VRTPacket):
    def __init__(self, payload: bytes, **kwargs):
        super().__init__(payload)
        
        T = self.signal_data_indicators.trailer
        self.trailer = self.words[-1] if T else 0
        if 'payload_size' in kwargs:
            self.sample_count = (kwargs.get('payload_size')) - 7 - int(T)
        else:
            self.sample_count = self.packet_header.packet_size - 7 - int(T)

        self.data = np.frombuffer(
                self.payload,
                offset = 7 *4,
                count = 2*self.sample_count,
                dtype = np.int16).reshape((-1, 2))


    @property
    def signal_data_indicators(self) -> SignalDataIndicators:
        return SignalDataIndicators(self.words[0])


    # signal data packets vita 49.2 properties

    @property
    def stream_id(self) -> int:
        return self.words[1]


    @property
    def class_id_codes(self) -> ClassIdentifier:
        return ClassIdentifier(self.words[2:4])