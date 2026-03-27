import socket
import json
import sys

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 8080
BUFFER_SIZE = 4096


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python client.py <client_id> <resource>")
        print("Example: python client.py client1 /test")
        return

    client_id = sys.argv[1]
    resource = sys.argv[2]

    request_obj = {
        "action": "fetch",
        "resource": resource,
        "client_id": client_id,
    }

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((PROXY_HOST, PROXY_PORT))
    client_socket.sendall(json.dumps(request_obj).encode())

    response = client_socket.recv(BUFFER_SIZE).decode()
    print("Response from proxy:")
    print(response)

    client_socket.close()


if __name__ == "__main__":
    main()