import struct
import random
import uuid

def add_packet_header(bin_file):
    bin_file.write('P'.encode('utf-8'))
    bin_file.write('R'.encode('utf-8'))
    bin_file.write('L'.encode('utf-8'))
    bin_file.write('V'.encode('utf-8'))

    frame_header = random.randint(20,40) << 20
    bin_file.write(struct.pack('<I',frame_header))

def add_packet_trailer(bin_file):
    bin_file.write('D'.encode('utf-8'))
    bin_file.write('N'.encode('utf-8'))
    bin_file.write('E'.encode('utf-8'))
    bin_file.write('V'.encode('utf-8'))

def add_random_data_packet(bin_file):
    add_packet_header(bin_file)
    packet_size = 64767
    packet_id = random.randint(7000, 8000)
    bin_file.write(struct.pack('H',packet_size))
    bin_file.write(struct.pack('H',packet_id))
    stream_id = random.randint(11,21)
    bin_file.write(struct.pack('<I',stream_id))
    class_id_0 = 16777215
    bin_file.write(struct.pack('<I',class_id_0))
    class_id_1 = 65537
    bin_file.write(struct.pack('<I',class_id_1))
    tsi = random.randint(1690000000, 1700000000)
    bin_file.write(struct.pack('<I',tsi))
    tsf = random.randint(0, 18000000000000000000)
    bin_file.write(struct.pack('<Q',tsf))

    for k in range(65534):
        I = random.randint(-40, 40)
        Q = random.randint(-40,40)
        bin_file.write(struct.pack('<h', I))
        bin_file.write(struct.pack('<h', Q))
    
    add_packet_trailer(bin_file)

def add_random_context_packet(bin_file):
    add_packet_header(bin_file)
    
    packet_size = 64767
    packet_id = random.randint(7000, 8000)
    bin_file.write(struct.pack('<H',packet_size))
    bin_file.write(struct.pack('<H',packet_id))
    stream_id = random.randint(11,21)
    bin_file.write(struct.pack('<I',stream_id))
    class_id_0 = 16777215
    bin_file.write(struct.pack('<I',class_id_0))
    class_id_1 = 65538
    bin_file.write(struct.pack('<I',class_id_1))
    tsi = random.randint(1690000000, 1700000000)
    bin_file.write(struct.pack('<I',tsi))
    tsf = random.randint(0, 18000000000000000000)
    bin_file.write(struct.pack('<Q',tsf))
    cif0 = 0b00111000101001000000000000001110
    bin_file.write(struct.pack('<I',cif0))
    cif1 = 0b11010011000000000000000000010000
    bin_file.write(struct.pack('<I',cif1))
    cif2 = 0b00000000000000000000000110000000
    bin_file.write(struct.pack('<I',cif2))
    cif3 = 0b00000001110000000000000000000000
    bin_file.write(struct.pack('<I',cif3))
    bandwidth = random.uniform(0.00, 8.79)
    bin_file.write(struct.pack('<d',bandwidth))
    IF_freq = random.uniform(-8.79, 8.79)
    bin_file.write(struct.pack('<d',IF_freq))
    RF_freq = random.uniform(0,55)
    bin_file.write(struct.pack('<d',RF_freq))
    gain1 = random.randint(-256,256)
    bin_file.write(struct.pack('<h',gain1))
    gain2 = random.randint(-256,256)
    bin_file.write(struct.pack('<h',gain2))
    sample_rate = 80
    bin_file.write(struct.pack('<Q',sample_rate))
    temperature = random.uniform(-273.15, 511.984375)
    bin_file.write(struct.pack('<f',temperature))
    phase_offset = random.uniform(-10,10)
    bin_file.write(struct.pack('<f',phase_offset))
    elliticity = 0
    bin_file.write(struct.pack('<h',elliticity))
    tilt = random.choice([0, 12868, 6434])
    bin_file.write(struct.pack('<h',tilt))
    pointing_header = 16
    bin_file.write(struct.pack('<I',pointing_header))
    pointing_size = 2
    bin_file.write(struct.pack('<I',pointing_size))
    pointing_ecef_0 = 0
    bin_file.write(struct.pack('<Q',pointing_ecef_0))
    pointing_ecef_1 = 0
    bin_file.write(struct.pack('<Q',pointing_ecef_1))
    pointing_ecef_2 = 0
    bin_file.write(struct.pack('<Q',pointing_ecef_2))
    pointing_azimuth = random.randint(0, 511)
    bin_file.write(struct.pack('<H',pointing_azimuth))
    pointing_elevation = random.randint(-90,90)
    bin_file.write(struct.pack('<h',pointing_elevation))
    pointing_steering_mode = random.randint(0,10)
    bin_file.write(struct.pack('<I',pointing_steering_mode))
    pointing_reserved = 4294967295
    bin_file.write(struct.pack('<I',pointing_reserved))
    bin_file.write(struct.pack('<I',pointing_reserved))
    beam_width_vert = random.randint(0,360)
    bin_file.write(struct.pack('<H',beam_width_vert))
    beam_width_horiz = random.randint(0,360)
    bin_file.write(struct.pack('<H',beam_width_horiz))
    beam_range = random.randint(0,67108864)
    bin_file.write(struct.pack('<I',beam_range))
    health = random.randint(0,10)
    bin_file.write(struct.pack('<I',health))
    mode_id = random.randint(0,10)
    bin_file.write(struct.pack('<I',mode_id))
    event_id = random.randint(0,10)
    bin_file.write(struct.pack('<I',event_id))
    pulse_width = 0.099
    bin_file.write(struct.pack('<d',pulse_width))
    PRI = 0.1
    bin_file.write(struct.pack('<d',PRI))
    duration = 0.1
    bin_file.write(struct.pack('<d',duration))
    VITA_filler = 4294967295
    bin_file.write(struct.pack('<I',VITA_filler))
    bin_file.write(struct.pack('<I',VITA_filler))
    bin_file.write(struct.pack('<I',VITA_filler))
   
    add_packet_trailer(bin_file)

def main():
    file_uuid = uuid.uuid4()
    file_name = f"MOCK_TANGO_DATA_{file_uuid}.bin"
    
    with open(file_name, 'wb') as bin_file:
        for i in range(10):
            add_random_context_packet(bin_file)

            for j in range(400):
                add_random_data_packet(bin_file)

if __name__ == "__main__":
    main()