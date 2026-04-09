import socket
from utils.cpp import get_cpp_sender_id, CPPDecodeError
from utils.network_config import ClientNetworkConfig
from utils.globals import HOST, BUFFER_SIZE, TIMEOUT

class MessageTimeoutError(Exception):
    def __init__(self, message="Timed out: No response received within timeout period of {} seconds.".format(TIMEOUT)):
        self.message = message
        super().__init__(self.message)

clientNetwork = ClientNetworkConfig()
client_port_dict = {client_id: None for client_id in clientNetwork.client_ids}
client_port_dict[clientNetwork.proxy_id] = clientNetwork.proxy_port

underlay_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def setup_underlay(client_id: int):
    if client_id == clientNetwork.proxy_id:
        underlay_socket.bind((HOST, clientNetwork.proxy_port))
        underlay_socket.settimeout(None)
    else:
        underlay_socket.bind((HOST, 0))
        underlay_socket.settimeout(TIMEOUT)

def send_cpp_packet(dest_id: int, message: bytes) -> None:
    dest_port = client_port_dict.get(dest_id)
    if dest_port is None:
        print(f"Unauthorized destination ID: {dest_id}")
        return
    underlay_socket.sendto(message, (HOST, dest_port))

def proxy_receive_cpp_packet() -> tuple[bytes, int]:
    while True:
        try:
            data, addr = underlay_socket.recvfrom(BUFFER_SIZE)
            client_id = get_cpp_sender_id(data)
            
            if client_id in client_port_dict:
                client_port_dict[client_id] = addr[1]
                
                return data, client_id
            else:
                print(f"Unauthorized client ID: {client_id}")
        except CPPDecodeError as e:
            print(f"Decoding error: {e}")

def client_receive_cpp_packet() -> bytes:
    while True:
        try:
            data, addr = underlay_socket.recvfrom(BUFFER_SIZE)
            client_id = get_cpp_sender_id(data)
            
            if client_id == clientNetwork.proxy_id:
                client_port_dict[client_id] = addr[1]
                return data
            else:
                print(f"Ignoring message from unauthorized ID: {client_id}")
        except socket.timeout:
            raise MessageTimeoutError()
        except CPPDecodeError as e:
            print(f"Decoding error: {e}")
    
def cleanup_underlay():
    underlay_socket.close()