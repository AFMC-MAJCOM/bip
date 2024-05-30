### python parser for .BIN files ###

Parser for custom .BIN files.  .BIN files are a sequence of
one or more vita 49 packets wrapped in a custom binary 
header.


## installation instructions ##

prerequisite python packages:
 - pyarrow
 - numpy

for testing:
 - pytest


```
python3 -m pip install .
```

## Usage ##

```
bip -o output_directory path_to_bin_file.BIN
```

### Compression ###

Compression algorithms supported by each recorder type:
- pyorc: none, zlib, lz4, zstd
- avro: null, deflate
- fastavro: null, deflate
- parquet: none, gzip, lz4, brotli, zstd

Compression levels supported by each algorithm:
- lz4: 0-16
- zlib: 0-9
- zstd: 1-22
- deflate: 1-9
- gzip: 0-9
- brotli: 1-11

pyorc only supports compression levels 0 or 1 regardless of the algorithm chosen.
avro does not support setting the compression level at all.

## Output Details ##

### Tango ###

When using the partitioned output, the `local_context_key` in the context
packets match exactly with the `data_key` in the partitioned data packet 
outputs if those data packets are associated with the context packet.

