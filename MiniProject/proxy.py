import socket
import threading
import time
from typing import Dict, Tuple, Optional

from utils.cpp import decode_cpp, encode_cpp
from utils.globals import Resource, HOST, base64_encode
from utils.network_config import ClientNetworkConfig, ServerNetworkConfig
from utils.client_transport_underlay import send_cpp_packet, update_client_port

BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 5

client_config = ClientNetworkConfig()
server_config = ServerNetworkConfig()

# track the most recent UDP address for each authorized client
CLIENT_ADDRESS_MAP: Dict[int, Tuple[str, int]] = {}

ROUND_ROBIN_INDEX: Dict[Resource, int] = {
    Resource.PING: 0,
    Resource.PAGE: 0,
    Resource.STREAM: 0,
}

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


def choose_backend_port(resource: Resource) -> Optional[int]:
    ports = server_config.get_server_ports(resource.name)
    if not ports:
        return None

    with LOCK:
        index = ROUND_ROBIN_INDEX[resource]
        chosen = ports[index % len(ports)]
        ROUND_ROBIN_INDEX[resource] = (index + 1) % len(ports)

    return chosen


def build_http_request(resource: Resource) -> str:
    path = RESOURCE_TO_PATH[resource]
    auth_header = f"Basic {base64_encode(server_config.proxy_auth_token)}"

    return (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {HOST}\r\n"
        f"Authorization: {auth_header}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )


def parse_http_status_and_headers(header_bytes: bytes) -> Tuple[int, Dict[str, str]]:
    header_text = header_bytes.decode(errors="replace")
    lines = header_text.split("\r\n")

    status_code = 500
    headers: Dict[str, str] = {}

    if lines:
        parts = lines[0].split()
        if len(parts) >= 2 and parts[1].isdigit():
            status_code = int(parts[1])

    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()

    return status_code, headers


def send_error_response(
    proxy_socket: socket.socket,
    client_id: int,
    resource: Resource,
    error_msg: str
) -> None:
    response_packet = encode_cpp(
        source_id=client_config.proxy_id,
        resource=resource,
        payload=f"ERROR: {error_msg}"
    )
    send_cpp_packet(dest_id=client_id, message=response_packet, socket=proxy_socket)


def forward_standard_response(
    backend_socket: socket.socket,
    initial_body: bytes,
    client_id: int,
    resource: Resource,
    proxy_socket: socket.socket
) -> None:
    body = initial_body

    while True:
        chunk = backend_socket.recv(BUFFER_SIZE)
        if not chunk:
            break
        body += chunk

    response_packet = encode_cpp(
        source_id=client_config.proxy_id,
        resource=resource,
        payload=body.decode(errors="replace")
    )
    send_cpp_packet(dest_id=client_id, message=response_packet, socket=proxy_socket)


def forward_chunked_response(
    backend_socket: socket.socket,
    initial_body: bytes,
    client_id: int,
    resource: Resource,
    proxy_socket: socket.socket
) -> None:
    buffer = initial_body

    while True:
        # at least a chunk-size line
        if b"\r\n" not in buffer:
            chunk = backend_socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            buffer += chunk
            continue

        size_line, rest = buffer.split(b"\r\n", 1)

        try:
            chunk_size = int(size_line.decode().strip(), 16)
        except ValueError:
            raise ValueError("Malformed chunked response from backend")

        # end of chunked stream
        if chunk_size == 0:
            return

        # full chunk data + trailing CRLF
        required = chunk_size + 2
        while len(rest) < required:
            chunk = backend_socket.recv(BUFFER_SIZE)
            if not chunk:
                raise ValueError("Incomplete chunked response from backend")
            rest += chunk

        chunk_data = rest[:chunk_size]
        trailer = rest[chunk_size:chunk_size + 2]

        if trailer != b"\r\n":
            raise ValueError("Invalid chunk terminator from backend")

        buffer = rest[chunk_size + 2:]

        response_packet = encode_cpp(
            source_id=client_config.proxy_id,
            resource=resource,
            payload=chunk_data.decode(errors="replace")
        )
        send_cpp_packet(dest_id=client_id, message=response_packet, socket=proxy_socket)


def forward_to_backend(
    resource: Resource,
    client_id: int,
    proxy_socket: socket.socket
) -> int:
    backend_port = choose_backend_port(resource)
    if backend_port is None:
        raise RuntimeError(f"No backend configured for resource {resource.name}")

    http_request = build_http_request(resource)

    backend_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backend_socket.settimeout(SOCKET_TIMEOUT)

    try:
        backend_socket.connect((HOST, backend_port))
        backend_socket.sendall(http_request.encode())

        response_buffer = b""
        while b"\r\n\r\n" not in response_buffer:
            chunk = backend_socket.recv(BUFFER_SIZE)
            if not chunk:
                raise RuntimeError("Backend closed connection before sending headers")
            response_buffer += chunk

        header_bytes, initial_body = response_buffer.split(b"\r\n\r\n", 1)
        status_code, headers = parse_http_status_and_headers(header_bytes)

        if status_code != 200:
            error_body = initial_body
            while True:
                chunk = backend_socket.recv(BUFFER_SIZE)
                if not chunk:
                    break
                error_body += chunk
            raise RuntimeError(
                f"Backend returned HTTP {status_code}: {error_body.decode(errors='replace')}"
            )

        transfer_encoding = headers.get("transfer-encoding", "").lower()

        if transfer_encoding == "chunked":
            forward_chunked_response(
                backend_socket=backend_socket,
                initial_body=initial_body,
                client_id=client_id,
                resource=resource,
                proxy_socket=proxy_socket,
            )
        else:
            forward_standard_response(
                backend_socket=backend_socket,
                initial_body=initial_body,
                client_id=client_id,
                resource=resource,
                proxy_socket=proxy_socket,
            )

        return backend_port

    finally:
        backend_socket.close()


def handle_cpp_request(
    data: bytes,
    addr: Tuple[str, int],
    proxy_socket: socket.socket
) -> None:
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
        backend_port = forward_to_backend(resource, client_id, proxy_socket)

        with LOCK:
            STATS["successful_requests"] += 1

        log_message(
            f"Completed request for client {client_id} ({resource.name}) via backend port {backend_port}"
        )

    except Exception as exc:
        with LOCK:
            STATS["failed_requests"] += 1

        log_message(f"Error handling client {client_id} ({resource.name}): {exc}")
        send_error_response(proxy_socket, client_id, resource, str(exc))


def main() -> None:
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    proxy_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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