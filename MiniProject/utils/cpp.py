import struct
from utils.globals import Resource

# We use an unassigned protocol number for the ID. Source: https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
PROTOCOL_ID = 148

# Protocol header format: [1 byte protocol ID][1 byte resource][2 bytes source ID]
CPP_HEADER_FORMAT = "!BBH"

def encode_cpp(source_id: int, resource: Resource, payload: str = "") -> bytes:
    if not (0 <= source_id <= 65535):
        raise ValueError("Source ID must be between 0 and 65535")
    
    header = struct.pack(CPP_HEADER_FORMAT, PROTOCOL_ID, resource.value, source_id)
    return header + payload.encode()
    
def decode_cpp(data: bytes) -> dict:
    if len(data) < struct.calcsize(CPP_HEADER_FORMAT):
        raise ValueError("Data is too short to contain a valid CPP header")
    
    header = data[:struct.calcsize(CPP_HEADER_FORMAT)]
    payload = data[struct.calcsize(CPP_HEADER_FORMAT):]
    
    protocol_id, resource_value, source_id = struct.unpack(CPP_HEADER_FORMAT, header)
    
    if protocol_id != PROTOCOL_ID:
        raise ValueError("Invalid protocol ID")
    
    try:
        resource = Resource(resource_value)
    except ValueError:
        raise ValueError("Invalid resource value")
    
    return {
        "source_id": source_id,
        "resource": resource,
        "payload": payload.decode()
    }