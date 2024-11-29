from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import numpy as np

from dataclasses import dataclass

class DwellFragment:
    start_time : int
    end_time : int
    n_samples : int
    data_key : int

    samples_i : list[np.int16]
    samples_q : list[np.int16]


samples_schema = pa.schema([
    ("samples_i", pa.int16()),
    ("samples_q", pa.int16())
])

class DwellPQWriter:
    @staticmethod
    def extension() -> str:
        return "parquet"
    
    def __init__(self,
        filename:Path,
        schema: pa.schema,
        options: dict = {},
        batch_size: int = 1000,
    ):

        self._filename = filename
        self._schema = schema
        self._options = options.copy()
        self._closed = False

        self.batch_size = batch_size
        self.schema = schema

        self.packet_data = []
        self.sample_data_i = np.zeros(0, dtype=np.int16)
        self.sample_data_q = np.zeros(0, dtype=np.int16)
        self.dwell_metadata = []
        self.written_keys = {}
        self.current_index = 0
        self.packet_writer = None
        self.sample_writer = None
        self.dwell_metadata_writer = None
        self.last_packet_time = 0

        self._current_local_key = None
        self._current_sample_file = None

        self._dirname = filename.with_suffix("")
        self._dirname.mkdir()
        self._data_packet_filename = self._dirname / "packet_metadata.parquet"
        self._dwell_metadata_filename = self._dirname / "samples_metadata.parquet"

    def _record(self, end_of_dwell:bool):
        packet_df = pd.DataFrame(self.packet_data)
        packet_table = pa.Table.from_pandas(packet_df)

        dwell_df = pd.DataFrame(self.dwell_metadata)
        dwell_table = pa.Table.from_pandas(dwell_df)

        samples_df = pd.DataFrame(
            {
                "samples_i": self.sample_data_i,
                "samples_q": self.sample_data_q
            }
        )
        samples_table = pa.Table.from_pandas(samples_df)
        try:
            if self.packet_writer == None and self.packet_data:
                self.packet_writer = pq.ParquetWriter(
                    self._data_packet_filename, 
                    pa.Schema.from_pandas(packet_df)
                )

            if self.dwell_metadata_writer == None and self.dwell_metadata:
                self.dwell_metadata_writer = pq.ParquetWriter(
                    self._dwell_metadata_filename,
                    pa.Schema.from_pandas(dwell_df)
                )

            if self.sample_writer == None:
                self.sample_writer = pq.ParquetWriter(
                    self._current_sample_file,
                    samples_schema
                )

            if self.packet_data:
                self.packet_writer.write_table(packet_table)
            if self.dwell_metadata:
                self.dwell_metadata_writer.write_table(dwell_table)
            
            self.sample_writer.write_table(samples_table)

            if end_of_dwell:
                self.sample_writer.close()
                self.sample_writer = None

        except Exception as e:
            print(f"Error writing to file: {e}, {self._dirname}")

        # clear out written data
        self.packet_data = []
        self.dwell_metadata = []
        self.sample_data_i = np.zeros(0, dtype=np.int16)
        self.sample_data_q = np.zeros(0, dtype=np.int16)

    def add_record(self, record: dict):
        # temporary for now, this will only work for Juliet.
        local_key = record["stream_id"]
        
        if local_key != self._current_local_key:
            # This is sample data for a new dwell, so write out all the sample
            # data for the current dwell before starting on this one. 
            self._record(True)

            # If we've written data for this local key before, then we'll have to 
            # write the rest of the data to a new file. 
            if not local_key in self.written_keys:
                self.written_keys[local_key] = 0
                suffix = "0"
            else:
                times_key_written = self.written_keys[local_key]
                times_key_written += 1
                suffix = str(times_key_written)
                self.written_keys[local_key] = times_key_written

            self._current_sample_file = self._dirname / f"{str(local_key)}-{suffix}.parquet"
            self.dwell_metadata.append(
                {
                    "local_key":local_key,
                    "filename":str(self._current_sample_file),
                    "first_packet_index": record["packet_id"]
                }
            )

            self._current_local_key = local_key

        self.sample_data_i = np.concat([self.sample_data_i, record.pop("samples_i")])
        self.sample_data_q = np.concat([self.sample_data_q, record.pop("samples_q")])
        self.packet_data.append(record)

        self.current_index += 1
        if self.current_index == self.batch_size:
            self._record(False)

    def close(self):
        if self._closed:
            return
        
        if self.current_index != 0:
            self._record()

        self.packet_writer.close()
        self.dwell_metadata_writer.close()

        self._closed = True

    @property
    def metadata(self) -> dict:
        return  {
            "output": str(self._filename),
            "options": self._options,
            "batch_size": self.batch_size
        }
    
    def __del__(self):
        try:
            if not self._closed:
                self.close()
        except:
            pass
