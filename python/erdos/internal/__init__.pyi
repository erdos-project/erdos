from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type

from erdos import OperatorConfig
from erdos.operator import BaseOperator

class PyTimestamp:
    def __init__(
        self, coordinates: Optional[List[int]], is_top: bool, is_bottom: bool
    ) -> None: ...
    def is_top(self) -> bool: ...
    def is_bottom(self) -> bool: ...
    def coordinates(self) -> Optional[List[int]]: ...
    def __lt__(self, other: Any) -> bool: ...
    def __eq__(self, other: Any) -> bool: ...
    def __le__(self, other: Any) -> bool: ...

class PyMessage:
    timestamp: Optional[PyTimestamp] = ...
    data: Optional[bytes] = ...
    def __init__(self, timestamp: PyTimestamp, data: Optional[bytes]) -> None: ...
    def is_timestamped_data(self) -> bool: ...
    def is_watermark(self) -> bool: ...
    def is_top_watermark(self) -> bool: ...

class PyStream:
    def name(self) -> str: ...
    def set_name(self, name: str) -> None: ...
    def id(self) -> str: ...
    def _map(self, function: Callable[[bytes], bytes]) -> PyOperatorStream: ...
    def _flat_map(
        self, function: Callable[[bytes], Iterable[bytes]]
    ) -> PyOperatorStream: ...
    def _filter(self, function: Callable[[bytes], bool]) -> PyOperatorStream: ...
    def _split(
        self, function: Callable[[bytes], bool]
    ) -> Tuple[PyOperatorStream, PyOperatorStream]: ...
    def _timestamp_join(
        self, other: PyStream, function: Callable[[bytes, bytes], bytes]
    ) -> PyOperatorStream: ...
    def _concat(self, other: PyStream) -> PyOperatorStream: ...

class PyOperatorStream(PyStream): ...

class PyLoopStream(PyStream):
    def __init__(self) -> None: ...
    def connect_loop(self, stream: PyOperatorStream) -> None: ...

class PyReadStream:
    def is_closed(self) -> bool: ...
    def name(self) -> str: ...
    def id(self) -> str: ...
    def read(self) -> PyMessage: ...
    def try_read(self) -> Optional[PyMessage]: ...

class PyWriteStream:
    def is_closed(self) -> bool: ...
    def name(self) -> str: ...
    def id(self) -> str: ...
    def send(self, message: PyMessage) -> None: ...

class PyIngestStream(PyStream):
    def __init__(self, name: Optional[str]) -> None: ...
    def is_closed(self) -> bool: ...
    def send(self, message: PyMessage) -> None: ...

class PyExtractStream:
    def __init__(self, py_stream: PyOperatorStream) -> None: ...
    def is_closed(self) -> bool: ...
    def read(self) -> PyMessage: ...
    def try_read(self) -> Optional[PyMessage]: ...
    def name(self) -> str: ...
    def id(self) -> str: ...

def connect_source(
    operator: Type[BaseOperator],
    config: OperatorConfig,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    node_id: int,
) -> PyOperatorStream: ...
def connect_sink(
    operator: Type[BaseOperator],
    config: OperatorConfig,
    read_stream: PyStream,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    node_id: int,
) -> None: ...
def connect_one_in_one_out(
    operator: Type[BaseOperator],
    config: OperatorConfig,
    read_stream: PyStream,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    node_id: int,
) -> PyOperatorStream: ...
def connect_one_in_two_out(
    operator: Type[BaseOperator],
    config: OperatorConfig,
    read_stream: PyStream,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    node_id: int,
) -> Tuple[PyOperatorStream, PyOperatorStream]: ...
def connect_two_in_one_out(
    operator: Type[BaseOperator],
    config: OperatorConfig,
    left_read_stream: PyStream,
    right_read_stream: PyStream,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    node_id: int,
) -> PyOperatorStream: ...
def run(
    node_id: int,
    data_addresses: List[str],
    control_addresses: List[str],
    graph_filename: Optional[str],
) -> None: ...
def run_async(
    node_id: int,
    data_addresses: List[str],
    control_addresses: List[str],
    graph_filename: Optional[str],
) -> PyNodeHandle: ...
def reset() -> None: ...

class PyNodeHandle:
    def shutdown_node(self) -> None: ...
