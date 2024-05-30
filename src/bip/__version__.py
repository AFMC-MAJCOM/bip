import importlib.metadata
__version__ = importlib.metadata.version("bip")


def metadata():
    return dict(importlib.metadata.metadata("bip"))
