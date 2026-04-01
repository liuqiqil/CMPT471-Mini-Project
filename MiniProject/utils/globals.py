from enum import Enum
import base64

HOST = "127.0.0.1"
STREAM_RESPONSE_COUNT = 10
STREAM_RESPONSE_INTERVAL = 1  # seconds

class Resource(Enum):
    PING = 0
    PAGE = 1
    STREAM = 2
    
def base64_encode(data: str) -> str:
    return base64.b64encode(data.encode()).decode()
    