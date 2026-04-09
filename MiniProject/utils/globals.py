from enum import Enum
import base64
import os

HOST = "127.0.0.1"
BUFFER_SIZE = 4096
STREAM_RESPONSE_COUNT = int(os.getenv("TEST_STREAM_COUNT", 10))
TIMEOUT = int(os.getenv("TEST_TIMEOUT", 5))  # seconds
STREAM_RESPONSE_INTERVAL = float(os.getenv("TEST_STREAM_INTERVAL", 1.0))  # seconds
LOGGING_DIR = "logs"
CLIENT_CONFIG_PATH = str(os.getenv("CLIENT_CONFIG_PATH", "../configs/client_network_config.json")) # Relative to utils/ directory!
SERVER_CONFIG_PATH = str(os.getenv("SERVER_CONFIG_PATH", "../configs/server_network_config.json"))

class Resource(Enum):
    PING = 0
    PAGE = 1
    STREAM = 2
    
def base64_encode(data: str) -> str:
    return base64.b64encode(data.encode()).decode()
    