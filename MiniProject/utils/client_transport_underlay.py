import socket
from utils.network_config import ClientNetworkConfig

HOST = "127.0.0.1"

clientNetwork = ClientNetworkConfig()
client_port_dict = {client_id: None for client_id in clientNetwork.client_ids}
client_port_dict[clientNetwork.proxy_id] = clientNetwork.proxy_port

def send_cpp_packet(dest_id: int, message: bytes, socket: socket.socket) -> None:
    dest_port = client_port_dict.get(dest_id)
    if dest_id not in client_port_dict:
        print(f"Unknown destination ID: {dest_id}")
        return
    if client_port_dict[dest_id] is None:
        print(f"Destination port for ID {dest_id} is not initialized")
        return
    socket.sendto(message, (HOST, dest_port))
    
def update_client_port(client_id: int, port: int) -> None:
    if client_id in client_port_dict:
        client_port_dict[client_id] = port
    else:
        print(f"Unauthorized client ID: {client_id}")