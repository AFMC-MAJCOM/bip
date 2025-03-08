import struct
import random
import uuid

def add_random_data_packet(bin_file, payload_size, trailer_size):

    bin_file.write(struct.pack('I',payload_size))

    for k in range(payload_size/4):
        I = random.randint(-40, 40)
        Q = random.randint(-40,40)
        bin_file.write(struct.pack('<h', I))
        bin_file.write(struct.pack('<h', Q))

    for i in range(trailer_size):
        trailer_data = 0xAAAAAAAA
        bin_file.write(struct.pack('I',trailer_data))

def add_random_job_interval_header(bin_file):
    A  = 0XFFFFFFFF
    bin_file.write(struct.pack('I',A))

    B = 1
    bin_file.write(struct.pack('I',B))

    C = 4
    bin_file.write(struct.pack('I',C))

    D = 128
    bin_file.write(struct.pack('I',D))

    E = 1024
    bin_file.write(struct.pack('I',E))

    F = random.randint(0, 2147483648)
    bin_file.write(struct.pack('I',F))

    G = random.randint(0, 2147483648)
    bin_file.write(struct.pack('I',G))

    H = random.randint(0, 2147483648)
    bin_file.write(struct.pack('I',H))

    I = random.randint(0, 2147483648)
    bin_file.write(struct.pack('I',I))

    J = random.randint(0, 2147483648)
    bin_file.write(struct.pack('I',J))

    K = random.randint(0, 10)
    bin_file.write(struct.pack('I',K))

    L = random.randint(0, 10)
    bin_file.write(struct.pack('I',L))

    M = random.randint(0, 4294967296)
    bin_file.write(struct.pack('Q',M))

    N = 0x01001101000000000000000000000000
    bin_file.write(struct.pack('I',N))


def add_random_job_event_header(bin_file, payload_size, trailer_size):
    A = random.randint(0, 2147483648)
    bin_file.write(struct.pack('I',A))

    B = random.randint(0, 100)
    bin_file.write(struct.pack('I',B))

    C = random.randint(0, 1024)
    bin_file.write(struct.pack('I',C))

    D = random.randint(0, 1024)
    bin_file.write(struct.pack('I',D))

    E = random.choice([payload_size/4, payload_size/40, payload_size/400, payload_size/4000]) 
    bin_file.write(struct.pack('Q',E))

    F = random.gauss(1704067200000, 100000000000)
    bin_file.write(struct.pack('Q',F))

    G = random.randint(0,5)
    bin_file.write(struct.pack('I',G))

    H = random.randint(0,10)
    bin_file.write(struct.pack('I',H))

    I = random.uniform(0,55)
    J = random.gauss(I,0.01)
    bin_file.write(struct.pack('Q',J))

    bin_file.write(struct.pack('Q',I))

    K = random.gauss(J, 0.1)
    bin_file.write(struct.pack('Q',K))

    L = random.randint(0, 2147483648)
    bin_file.write(struct.pack('Q',L))

    pointing_elevation = random.randint(-90,90)
    bin_file.write(struct.pack('<h',pointing_elevation))
    pointing_azimuth = random.randint(0, 511)
    bin_file.write(struct.pack('<h',pointing_azimuth))

    polarization_tilt_angle = random.choice([0, 12868])
    bin_file.write(struct.pack('<h',polarization_tilt_angle))
    polarization_ellpticity = random.choice([0, 12868, 6434])
    bin_file.write(struct.pack('<h',polarization_ellpticity))

    num_samples = payload_size/4
    bin_file.write(struct.pack('I',num_samples))

    bin_file.write(struct.pack('I',payload_size))

    bin_file.write(struct.pack('I',trailer_size))

    user_defined_data_size = 3
    bin_file.write(struct.pack('I',user_defined_data_size))

    for i in range(user_defined_data_size):
        user_defined_data = random.randint(0,2048)
        bin_file.write(struct.pack('I',user_defined_data))

def main():
    file_uuid = uuid.uuid4()
    file_name = f"MOCK_JOB_FORMAT_DATA_{file_uuid}.bin"
    job_events = 10
    
    with open(file_name, 'wb') as bin_file:
        add_random_job_interval_header(bin_file)

        trailer_size = 0
        payload_size = 262140
        for i in range(job_events):
            add_random_job_event_header(bin_file, payload_size, trailer_size)

        for j in range(job_events):
            add_random_data_packet(bin_file, payload_size, trailer_size)

if __name__ == "__main__":
    main()