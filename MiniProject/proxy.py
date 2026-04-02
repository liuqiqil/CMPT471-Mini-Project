import socket
import threading
import time
from typing import Dict, Tuple, Optional

from utils.cpp import decode_cpp, encode_cpp
from utils.globals import Resource, HOST
from utils.network_config import ClientNetworkConfig, ServerNetworkConfig
from utils.client_transport_underlay import send_cpp_packet, update_client_port

BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 5

client_config = ClientNetworkConfig()
server_config = ServerNetworkConfig()

# client_id -> (ip, port)
CLIENT_ADDRESS_MAP: Dict[int, Tuple[str, int]] = {}

STATS = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
}

LOCK = threading.Lock()

RESOURCE_TO_PATH = {
    Resource.PING: "/ping",
    Resource.PAGE: "/page",
    Resource.STREAM: "/stream",
}


def log_message(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def build_http_request(resource: Resource) -> str:
    path = RESOURCE_TO_PATH[resource]
    auth_value = f"Basic {__import__('utils.globals').globals.base64_encode(server_config.proxy_auth_token)}"
    return (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {HOST}\r\n"
        f"Authorization: {auth_value}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )


def choose_backend_port(resource: Resource) -> Optional[int]:
    ports = server_config.get_server_ports(resource.name)
    if not ports:
        return None
    return ports[0]


def parse_standard_http_response(http_response: str) -> Tuple[int, str]:
    status_code = 500
    body = ""

    parts = http_response.split("\r\n\r\n", 1)
    header_text = parts[0]
    body = parts[1] if len(parts) > 1 else ""

    status_line = header_text.split("\r\n")[0]
    status_parts = status_line.split()
    if len(status_parts) >= 2 and status_parts[1].isdigit():
        status_code = int(status_parts[1])

    return status_code, body


def parse_chunked_http_response(http_response: str) -> Tuple[int, list[str]]:
    status_code = 500
    chunks = []

    parts = http_response.split("\r\n\r\n", 1)
    header_text = parts[0]
    body = parts[1] if len(parts) > 1 else ""

    status_line = header_text.split("\r\n")[0]
    status_parts = status_line.split()
    if len(status_parts) >= 2 and status_parts[1].isdigit():
        status_code = int(status_parts[1])

    lines = body.split("\r\n")
    i = 0
    while i < len(lines):
        size_line = lines[i].strip()
        if not size_line:
            i += 1
            continue
        try:
            chunk_size = int(size_line, 16)
        except ValueError:
            break
        if chunk_size == 0:
            break
        i += 1
        if i < len(lines):
            chunks.append(lines[i])
        i += 1

    return status_code, chunks


def forward_to_backend(resource: Resource) -> Tuple[int, list[str]]:
    backend_port = choose_backend_port(resource)
    if backend_port is None:
        raise RuntimeError(f"No backend configured for resource {resource.name}")

    http_request = build_http_request(resource)

    backend_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backend_socket.settimeout(SOCKET_TIMEOUT)

    try:
        backend_socket.connect((HOST, backend_port))
        backend_socket.sendall(http_request.encode())

        response_data = b""
        while True:
            chunk = backend_socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            response_data += chunk

        decoded = response_data.decode(errors="replace")

        if "Transfer-Encoding: chunked" in decoded:
            status_code, chunks = parse_chunked_http_response(decoded)
            return status_code, chunks

        status_code, body = parse_standard_http_response(decoded)
        return status_code, [body]

    finally:
        backend_socket.close()


def send_error_response(proxy_socket: socket.socket, client_id: int, error_msg: str) -> None:
    message = encode_cpp(
        source_id=client_config.proxy_id,
        resource=Resource.PAGE,
        payload=f"ERROR: {error_msg}"
    )
    send_cpp_packet(dest_id=client_id, message=message, socket=proxy_socket)


def handle_cpp_request(data: bytes, addr: Tuple[str, int], proxy_socket: socket.socket) -> None:
    try:
        packet = decode_cpp(data)
    except Exception as exc:
        log_message(f"Dropped malformed CPP packet from {addr}: {exc}")
        return

    client_id = packet["source_id"]
    resource = packet["resource"]

    if not client_config.is_authorized(client_id):
        log_message(f"Dropped packet from unauthorized client ID {client_id}")
        return

    with LOCK:
        STATS["total_requests"] += 1
        CLIENT_ADDRESS_MAP[client_id] = addr

    update_client_port(client_id, addr[1])
    log_message(f"Received {resource.name} request from client {client_id} at {addr}")

    try:
        status_code, payload_parts = forward_to_backend(resource)

        if status_code != 200:
            with LOCK:
                STATS["failed_requests"] += 1
            send_error_response(proxy_socket, client_id, f"Backend returned HTTP {status_code}")
            return

        for part in payload_parts:
            response_packet = encode_cpp(
                source_id=client_config.proxy_id,
                resource=resource,
                payload=part
            )
            send_cpp_packet(dest_id=client_id, message=response_packet, socket=proxy_socket)

        with LOCK:
            STATS["successful_requests"] += 1

        log_message(f"Completed request for client {client_id} ({resource.name})")

    except Exception as exc:
        with LOCK:
            STATS["failed_requests"] += 1
        log_message(f"Error handling client {client_id}: {exc}")
        send_error_response(proxy_socket, client_id, str(exc))


def main() -> None:
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    proxy_socket.bind((HOST, client_config.proxy_port))

    log_message(f"CPP proxy listening on UDP {HOST}:{client_config.proxy_port}")

    try:
        while True:
            data, addr = proxy_socket.recvfrom(BUFFER_SIZE)
            thread = threading.Thread(
                target=handle_cpp_request,
                args=(data, addr, proxy_socket),
                daemon=True
            )
            thread.start()

    except KeyboardInterrupt:
        log_message("Proxy shutting down.")
        log_message(f"Final stats: {STATS}")
    finally:
        proxy_socket.close()


if __name__ == "__main__":
    main()