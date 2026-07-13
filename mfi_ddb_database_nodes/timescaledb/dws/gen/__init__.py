"""Local import shim for the TimescaleDB DWS generated stubs.

The checked-in gencode imports sibling modules as top-level names. Register
those aliases here so the package works without editing the generated files.
"""

import sys

from . import models_pb2 as _models_pb2

sys.modules.setdefault("models_pb2", _models_pb2)

from . import service_pb2 as _service_pb2

sys.modules.setdefault("service_pb2", _service_pb2)

__all__ = ["models_pb2", "service_pb2", "service_pb2_grpc", "models_pb2_grpc"]
