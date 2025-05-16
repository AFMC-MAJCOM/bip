from io import RawIOBase
from pathlib import Path
from typing import Protocol, runtime_checkable
import abc

import tqdm

from bip.recorder.recorder_protocol import Recorder

@runtime_checkable
@abc.ABC
class Parser(Protocol):
    def __init__(self,
            input_path: Path,
            output_path: Path,
            recorder: Recorder,
            recorder_opts: dict = None
            ):
        """
        Defines the protocol for the protocol constructor.
        """
        pass

    @abc.abstractmethod
    def parse_stream(self, stream: RawIOBase, progress_bar: tqdm.tqdm) -> None:
        """
        Parses a stream of binary data for a specific format.
        """
        pass

    @property
    @abc.abstractmethod
    def metadata(self) -> dict:
        """
        Information about this parser.
        """
        pass

    @property
    @abc.abstractmethod
    def bytes_read(self) -> int:
        """
        How many bytes have been read by this parser from the stream.
        """
        pass

    @property
    @abc.abstractmethod
    def packets_read(self) -> int:
        """
        How many packets have been read by this parser from the stream.
        """
        pass

