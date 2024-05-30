import ctypes

""" common value extractions that multiple parsers can implement.
    do consider the payload endianness, and if/when it may be 
    converted when using these, little endian is currently assumed """

#Current Juliet Implementations
def bandwidth(word1, word2):
    raw_data = ctypes.c_uint64((word1 << 32) | word2)
    return ctypes.c_int64(raw_data.value).value * (10**-6) * (2**-20) 

def dwell(word1, word2):
    raw_data = ctypes.c_uint64((word1 << 32) | word2)
    # data comes in femtoseconds -9 converts to microseconds
    return ctypes.c_uint64(raw_data.value).value * (10**-9)  

def frequency(word1, word2):
    raw_data = ctypes.c_uint64((word1 << 32) | word2)
    return ctypes.c_int64(raw_data.value).value * (10**-9) * (2**-20)

def gain(word1): # needs work, but not implemented in juliet yet
    return word1

def offset(word1, word2):
    raw_data = ctypes.c_uint64((word1 << 32) | word2)
    return ctypes.c_int64(raw_data.value).value * (10**-6) * (2**-20) 

def pointing_vector(word):
    el = (word >> 16)
    if el >= 0x8000: #sign bit present, take 2's complement
        msb = el & 0x8000
        el_out = (0-msb) + (el-msb)
        el_out = el_out * (2**-7)
    else:
        el_out = el * (2**-7)
    
    az = (word & 0xFFFF) * (2**-7)
    if az >= 280:
        az = az - 360
        
    return az, el_out

def sample_rate(word1, word2):
    raw_data = ctypes.c_uint64((word1 << 32) | word2)
    return ctypes.c_int64(raw_data.value).value * (10**-6) * (2**-20) 

def time(tsi, tsf0, tsf1):
    return tsi + ((tsf0 << 32) + tsf1) * 1e-12


