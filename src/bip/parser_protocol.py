from io import RawIOBase
from pathlib import Path
from typing import Protocol, runtime_checkable

import tqdm

from bip.recorder.recorder_protocol import Recorder

@runtime_checkable
class Parser(Protocol):
    def __init__(self,
            input_path: Path,
            output_path: Path,
            recorder: Recorder,
            recorder_opts: dict = {}
            ):
        pass


    def parse_stream(self, stream: RawIOBase, progress_bar: tqdm.tqdm) -> None:
        pass

    @property
    def metadata(self) -> dict:
        pass

    @property
    def bytes_read(self) -> int:
        pass

    @property
    def packets_read(self) -> int:
        pass

