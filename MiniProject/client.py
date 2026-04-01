import socket
import sys
from utils.network_config import ClientNetworkConfig
from utils.cpp import decode_cpp, encode_cpp
from utils.client_transport_underlay import send_cpp_packet
from utils.globals import Resource, HOST, STREAM_RESPONSE_COUNT

BUFFER_SIZE = 4096
TIMEOUT = 5

def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python client.py <client_id> <resource> <port>")
        print("Example: python client.py 1 page 8080")
        return

    client_id = int(sys.argv[1])
    resource = Resource[sys.argv[2].upper()]
    port = int(sys.argv[3])

    config = ClientNetworkConfig()
    if not config.is_authorized(client_id):
        print("Unauthorized client. Use another client ID.")
        return
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.bind((HOST, port))
    cpp_message = encode_cpp(source_id=client_id, resource=resource)
    send_cpp_packet(dest_id=config.proxy_id, message=cpp_message, socket=client_socket)
    
    # For STREAM resource, we expect 10 (defined in globals.py) responses spaced 1s apart. Only 1 response for the rest.
    response_count = STREAM_RESPONSE_COUNT if resource == Resource.STREAM else 1

    for _ in range(response_count):
        client_socket.settimeout(TIMEOUT)
        
        try:
            response = decode_cpp(client_socket.recvfrom(BUFFER_SIZE)[0])
            if (response['source_id'] != config.proxy_id):
                print("Received response not from proxy (id = {})! Sender ID:".format(config.proxy_id), response['source_id'])
                return
            print(response)
        except socket.timeout:
            print("Timed out: No response received within timeout period of {} seconds.".format(TIMEOUT))
            return

    client_socket.close()


if __name__ == "__main__":
    main()