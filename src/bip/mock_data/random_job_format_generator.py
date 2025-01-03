import struct
import random
import uuid

def add_random_data_packet(bin_file):
    
    packet_size = 
    protocol_metadata_version = 
    bin_file.write(struct.pack('I',packet_size))
    bin_file.write(struct.pack('I',protocol_metadata_version))


    for k in range(packet_size):
        I = random.randint(-40, 40)
        Q = random.randint(-40,40)
        bin_file.write(struct.pack('<h', I))
        bin_file.write(struct.pack('<h', Q))
    
    trailer = 
    bin_file.write(struct.pack('I',trailer))

def add_random_job_interval_header(bin_file):
    job_details_id = 
    bin_file.write(struct.pack('I',job_details_id))
    va_definition_id = 
    bin_file.write(struct.pack('I',va_definition_id))
    va_instance_id = 
    bin_file.write(struct.pack('I',va_instance_id))
    job_interval_id = 
    bin_file.write(struct.pack('I',job_interval_id))
    sequence_repeater_count = 
    bin_file.write(struct.pack('H',sequence_repeater_count))
    iterations_per_second = 
    bin_file.write(struct.pack('H',iterations_per_second))
    iterations_start_count = 
    bin_file.write(struct.pack('H',iterations_start_count))
    capability_id = 
    bin_file.write(struct.pack('I',capability_id))
    activity_id = 
    bin_file.write(struct.pack('I',activity_id))
    signal_bit_depth = 
    bin_file.write(struct.pack('I',signal_bit_depth))
    if_type_id = 
    bin_file.write(struct.pack('I',if_type_id))
    if_instance_id = 
    bin_file.write(struct.pack('H',if_instance_id))
    num_rx_events = 
    bin_file.write(struct.pack('H',num_rx_events))
    num_tx_events = 
    bin_file.write(struct.pack('H',num_tx_events))
    cal_duration = 
    bin_file.write(struct.pack('Q',cal_duration))

def add_random_job_event_header(bin_file):
    event_id = 
    bin_file.write(struct.pack('I',event_id))
    payload_size = 
    bin_file.write(struct.pack('I',payload_size))
    timestamp_of_first_sample = 
    bin_file.write(struct.pack('Q',timestamp_of_first_sample))
    duration = 
    bin_file.write(struct.pack('Q',duration))
    requested_frequency = 
    bin_file.write(struct.pack('Q',requested_frequency))
    true_center_frequency = 
    bin_file.write(struct.pack('Q',true_center_frequency))
    polarization = 
    bin_file.write(struct.pack('Q',polarization))
    sample_frequency_hz = 
    bin_file.write(struct.pack('Q',sample_frequency_hz))
    num_samples = 
    bin_file.write(struct.pack('I',num_samples))
    pointing_vector_index = 
    bin_file.write(struct.pack('I',pointing_vector_index))
    phase_offset = 
    bin_file.write(struct.pack('Q',phase_offset))
    termination_count = 
    bin_file.write(struct.pack('I',termination_count))
    aqc_pulses = 
    bin_file.write(struct.pack('I',aqc_pulses))
    post_aqc_pulses = 
    bin_file.write(struct.pack('I',post_aqc_pulses))
    weight_id = 
    bin_file.write(struct.pack('I',weight_id))
    datapipe_id = 
    bin_file.write(struct.pack('I',datapipe_id))
    rx_element_group_id = 
    bin_file.write(struct.pack('I',rx_element_group_id))
    user_defined_data_size = 
    bin_file.write(struct.pack('I',user_defined_data_size))

def main():
    file_uuid = uuid.uuid4()
    file_name = f"MOCK_JOB_FORMAT_DATA_{file_uuid}.bin"
    job_events = 10
    
    with open(file_name, 'wb') as bin_file:
        add_random_job_interval_header(bin_file)
        for i in range(job_events):
            add_random_job_event_header(bin_file)

        for j in range(job_events):
            add_random_data_packet(bin_file)

if __name__ == "__main__":
    main()