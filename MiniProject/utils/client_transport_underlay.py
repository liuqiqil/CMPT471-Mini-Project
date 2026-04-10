import socket
from utils.cpp import get_cpp_sender_id, CPPDecodeError
from utils.network_config import ClientNetworkConfig
from utils.globals import HOST, BUFFER_SIZE, TIMEOUT


class MessageTimeoutError(Exception):
    def __init__(self, message=None):
        if message is None:
            message = (
                "Timed out: current proxy unavailable or no response received "
                f"within timeout period of {TIMEOUT} seconds."
            )
        self.message = message
        super().__init__(self.message)


clientNetwork = ClientNetworkConfig()

client_port_dict = {client_id: None for client_id in clientNetwork.client_ids}
for proxy_id in clientNetwork.proxy_ids:
    client_port_dict[proxy_id] = clientNetwork.get_proxy_port(proxy_id)

underlay_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def setup_underlay(client_id: int):
    if client_id in clientNetwork.proxy_ids:
        underlay_socket.bind((HOST, clientNetwork.get_proxy_port(client_id)))
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
                # Learn/update client ephemeral port dynamically.
                # For proxies this will just refresh the configured port.
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
            sender_id = get_cpp_sender_id(data)

            if sender_id in clientNetwork.proxy_ids:
                client_port_dict[sender_id] = addr[1]
                return data
            else:
                print(f"Ignoring message from unauthorized ID: {sender_id}")
        except socket.timeout:
            raise MessageTimeoutError()
        except CPPDecodeError as e:
            print(f"Decoding error: {e}")


def cleanup_underlay():
    underlay_socket.close()