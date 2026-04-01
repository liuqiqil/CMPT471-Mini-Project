import struct
from enum import Enum
from utils.globals import Resource

# Used only by the proxy to communicate the status of the request back to the client
# SERVER_BUSY and SERVER_UNREACHABLE are only returned by non-ping requests!
class CPPStatus(Enum):
    SUCCESS = 0
    SERVER_BUSY = 1
    SERVER_UNREACHABLE = 2
    PROXY_BUSY = 3
    INVALID_REQUEST = 4

# We use an unassigned protocol number for the ID. Source: https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
PROTOCOL_ID = 148

# Protocol header format: [1 byte protocol ID][1 byte resource][1 byte source ID][1 byte status code]
CPP_HEADER_FORMAT = "!BBBB"

def encode_cpp(source_id: int, resource: Resource, payload: str = "", status: CPPStatus = CPPStatus.SUCCESS) -> bytes:
    if not (0 <= source_id <= 255):
        raise ValueError("Source ID must be between 0 and 255")
    
    header = struct.pack(CPP_HEADER_FORMAT, PROTOCOL_ID, resource.value, source_id, status.value)
    return header + payload.encode()
    
def decode_cpp(data: bytes) -> dict:
    if len(data) < struct.calcsize(CPP_HEADER_FORMAT):
        raise ValueError("Data is too short to contain a valid CPP header")
    
    header = data[:struct.calcsize(CPP_HEADER_FORMAT)]
    payload = data[struct.calcsize(CPP_HEADER_FORMAT):]
    
    protocol_id, resource_value, source_id, status_code = struct.unpack(CPP_HEADER_FORMAT, header)
    
    if protocol_id != PROTOCOL_ID:
        raise ValueError("Invalid protocol ID")
    
    try:
        resource = Resource(resource_value)
    except ValueError:
        raise ValueError("Invalid resource value")
    
    return {
        "source_id": source_id,
        "resource": resource,
        "payload": payload.decode(),
        "status": CPPStatus(status_code)
    }