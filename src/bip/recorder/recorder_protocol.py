from pathlib import Path
from typing import Protocol, runtime_checkable

import pyarrow as pa

@runtime_checkable
class Recorder(Protocol):
    def __init__(self,
            filename: Path,
            schema: pa.Schema=None,
            options: dict = {}
            ):
        pass

    @staticmethod
    def extension() -> str:
        pass

    @property
    def metadata(self) -> dict:
        pass

    def add_record(self, record: dict, dwell_key: int = None):
        pass

    def close(self):
        pass
