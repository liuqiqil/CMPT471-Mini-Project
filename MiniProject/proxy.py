from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import socket
import threading
from typing import Dict, List, Tuple, Optional
from utils.network_config import ClientNetworkConfig, ServerNetworkConfig
from utils.globals import HOST, BUFFER_SIZE, TIMEOUT, Resource, base64_encode
from backend_server import PATH_DB
from utils.cpp import CPPStatus, encode_cpp, decode_cpp, CPPDecodeError
from utils.client_transport_underlay import cleanup_underlay, proxy_receive_cpp_packet, send_cpp_packet, setup_underlay

# Simple session map
SESSION_MAP: Dict[int, Dict[Resource, int]] = {}

# Round-robin pointer for backend selection
NEXT_INDEX_MAP: Dict[Resource, int] = {res: 0 for res in Resource}

# Basic stats
STATS = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "failover_count": 0,
}

LOCK = threading.Lock()

server_config = ServerNetworkConfig()
client_config = ClientNetworkConfig()


def log_message(message: str) -> None:
    timestamp = datetime.now().strftime('%a %b %d %H:%M:%S %Y')
    thread_id = threading.get_ident()
    print(f"[{timestamp}] [Thread:{thread_id}] {message}")

def choose_backends_for_client(client_id: int, resource: Resource) -> List[int]:
    global SESSION_MAP, NEXT_INDEX_MAP

    available_servers = []
    if resource != Resource.PING:
        available_servers = server_config.get_server_ports(resource)
    else:
        # For PING, return all servers
        return server_config.server_ports()
    
    if not available_servers:
        return []

    with LOCK:
        if client_id in SESSION_MAP and resource in SESSION_MAP[client_id]:
            preferred_port = SESSION_MAP[client_id][resource]
            # For existing sessions, use the same backend and add other servers as backup
            if preferred_port in available_servers:
                others = [p for p in available_servers if p != preferred_port]
                return [preferred_port] + others

        # Choose backend using round-robin for new sessions otherwise
        current_idx = NEXT_INDEX_MAP.get(resource, 0)
        chosen_port = available_servers[current_idx % len(available_servers)]
        NEXT_INDEX_MAP[resource] = (current_idx + 1) % len(available_servers)

        # Save Client to Session Map
        if client_id not in SESSION_MAP:
            SESSION_MAP[client_id] = {}
        SESSION_MAP[client_id][resource] = chosen_port
        others = [p for p in available_servers if p != chosen_port]
        return [chosen_port] + others


def write_http_request(resource: Resource) -> Optional[bytes]:
    resource_path = PATH_DB.get(resource)
    if resource_path is None:
        raise ValueError("Unsupported resource type")

    http_request = (
        f"GET {resource_path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1\r\n"
        f"Authorization: Basic {base64_encode(server_config.proxy_auth_token)}\r\n"
        f"\r\n"
    )
    return http_request.encode()

def parse_http_response(http_response: bytes) -> Tuple[int, bytes, bool]:
    status_code = 500
    body = b""
    is_chunked = False

    separator = b"\r\n\r\n"
    if separator in http_response:
        header_section, body = http_response.split(separator, 1)
    else:
        header_section = http_response

    lines = header_section.split(b"\r\n")
    if lines:
        parts = lines[0].split()
        if len(parts) >= 2 and parts[1].isdigit():
            status_code = int(parts[1])

        for line in lines[1:]:
            if b":" in line:
                key, value = line.split(b":", 1)
                if key.strip().lower() == b"transfer-encoding" and b"chunked" in value.lower():
                    is_chunked = True
                    break

    return status_code, body, is_chunked

def read_http_chunk(sock: socket.socket) -> Tuple[bytes, bool]:
    def read_until(delimiter: bytes) -> bytes:
        data = b""
        while not data.endswith(delimiter):
            char = sock.recv(1)
            if not char: break
            data += char
        return data
    size_line = read_until(b"\r\n").strip()
    if not size_line:
        return b"", True
    try:
        chunk_size = int(size_line, 16)
    except ValueError:
        return b"", True

    if chunk_size == 0:
        read_until(b"\r\n")
        return b"", True
    chunk_data = b""
    while len(chunk_data) < chunk_size:
        to_read = chunk_size - len(chunk_data)
        chunk_data += sock.recv(to_read)
    read_until(b"\r\n")

    return chunk_data, False

def forward_to_backend(client_id: int, resource: Resource):
    global SESSION_MAP

    payload = write_http_request(resource)

    if resource == Resource.PING:

        total_available = 0
        total_busy = 0
        total_unreachable = 0
        total_servers = 0

        lines = []

        def ping_server(port):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(TIMEOUT)
                    sock.connect((HOST, port))
                    sock.sendall(payload)
                    initial_data = sock.recv(BUFFER_SIZE)
                    status_code, _, _ = parse_http_response(initial_data)
                    log_message(f"Pinged server on port {port} with resource {resource.name}: Status code {status_code}")
                    return port, status_code
            except (ConnectionRefusedError, socket.timeout, Exception):
                log_message(f"Server {port} unreachable during PING.")
                return port, "unreachable"

        resource_ports = {}
        all_ports = []

        for res in Resource:
            ports = server_config.get_server_ports(res)
            if not ports:
                continue
            resource_ports[res] = ports
            all_ports.extend((res, port) for port in ports)

        with ThreadPoolExecutor(max_workers=len(all_ports)) as executor:
            results = list(executor.map(lambda rp: (rp[0], *ping_server(rp[1])), all_ports))

        # Group results by resource
        results_by_resource = {res: [] for res in resource_ports}

        for res, port, status in results:
            results_by_resource[res].append((port, status))

        # Process each resource
        for res, ports in resource_ports.items():
            available, busy, unreachable = [], [], []

            for port, status in results_by_resource[res]:
                if status == 200:
                    available.append(port)
                elif status == 503:
                    busy.append(port)
                else:
                    unreachable.append(port)

            total = len(ports)
            a, b, u = len(available), len(busy), len(unreachable)

            total_available += a
            total_busy += b
            total_unreachable += u
            total_servers += total

            def pct(x, t):
                return int(x / t * 100) if t > 0 else 0

            line = (
                f"{res}: {pct(a, total)}% ({a}/{total}) available, "
                f"{pct(b, total)}% ({b}/{total}) busy, "
                f"{pct(u, total)}% ({u}/{total}) unreachable. "
                f"Total online: {pct(a, total)}% ({a}/{total})"
            )
            lines.append(line)

        # Total line
        def pct(x, t):
            return int(x / t * 100) if t > 0 else 0

        total_line = (
            f"Total: "
            f"{pct(total_available, total_servers)}% ({total_available}/{total_servers}) available, "
            f"{pct(total_busy, total_servers)}% ({total_busy}/{total_servers}) busy, "
            f"{pct(total_unreachable, total_servers)}% ({total_unreachable}/{total_servers}) unreachable. "
            f"Total online: {pct(total_available, total_servers)}% ({total_available}/{total_servers})"
        )

        final_body = "\n".join(lines + [total_line])
        return_to_client(CPPStatus.SUCCESS_DONE.value, final_body.encode(), client_id)
        with LOCK:
            STATS["successful_requests"] += 1
        return

    # Non-PING resources
    backend_ports = choose_backends_for_client(client_id, resource)

    if not backend_ports:
        with LOCK:
            STATS["failed_requests"] += 1
        raise RuntimeError(f"No backend servers available for resource: {resource}")

    any_busy = False
    any_unreachable = False

    for index, port in enumerate(backend_ports):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, port))
            sock.settimeout(TIMEOUT)
            sock.sendall(payload)

            log_message(f"Sent request for resource {resource.name} from client {client_id} to backend server on port {port}")
            initial_data = sock.recv(BUFFER_SIZE)
            status_code, body, is_chunked = parse_http_response(initial_data)
            log_message(f"Received response with status code {status_code} from backend server on port {port} for client {client_id}")

            if status_code == 503:
                any_busy = True
                log_message(f"Server {port} is busy. Trying next...")
                sock.close()
                continue

            elif status_code != 200:
                sock.close()
                with LOCK:
                    STATS["failed_requests"] += 1
                return_to_client(CPPStatus.INTERNAL_ERROR.value, body, client_id)
                return

            with LOCK:
                STATS["successful_requests"] += 1
                if index > 0:
                    STATS["failover_count"] += 1

            if port != backend_ports[0]:
                with LOCK:
                    SESSION_MAP[client_id][resource] = port

            if not is_chunked:
                return_to_client(CPPStatus.SUCCESS_DONE.value, body, client_id)
                sock.close()
            else:
                is_done = False
                while not is_done:
                    chunk_data, is_done = read_http_chunk(sock)
                    if chunk_data:
                        return_to_client(CPPStatus.SUCCESS_PARTIAL.value, chunk_data, client_id)
                    else:
                        return_to_client(CPPStatus.SUCCESS_DONE.value, b"", client_id)
                sock.close()

            return

        except (ConnectionRefusedError, socket.timeout):
            any_unreachable = True
            log_message(f"Server {port} unreachable. Trying next...")
            continue

    with LOCK:
        STATS["failed_requests"] += 1

    if any_busy and any_unreachable:
        return_to_client(CPPStatus.SERVER_BUSY.value, b"", client_id)
    elif any_busy:
        return_to_client(CPPStatus.SERVER_BUSY.value, b"", client_id)
    else:
        return_to_client(CPPStatus.SERVER_UNREACHABLE.value, b"", client_id)

def return_to_client(status_code: int, body: bytes, client_id: int) -> None:
    response = encode_cpp(source_id=client_config.proxy_id, resource=Resource.PAGE, payload=body.decode(), status=CPPStatus(status_code))
    send_cpp_packet(client_id, response)

def handle_client(data: bytes, client_id: int) -> None:
    try:
        if not client_config.is_authorized(client_id):
            log_message(f"Unauthorized client {client_id} tried to send data; dropping packet.")
            with LOCK:
                STATS["unauthorized_requests"] += 1
            return

        raw_data = data
        if not raw_data:
            return

        with LOCK:
            STATS["total_requests"] += 1

        try:
            request_obj = decode_cpp(data)
            log_message(f"Received request from client {client_id} for resource {Resource(request_obj['resource']).name}")
        except CPPDecodeError as e:
            with LOCK:
                STATS["failed_requests"] += 1
            target_client_id = getattr(e, "source_id", client_id)
            if target_client_id is not None:
                return_to_client(
                    CPPStatus.INVALID_REQUEST.value,
                    str(e).encode(),
                    target_client_id
                )
            return

        forward_to_backend(request_obj['source_id'], request_obj['resource'])

    except Exception as exc:
        log_message(f"Proxy error handling client {client_id}: {exc}")
        with LOCK:
            STATS["failed_requests"] += 1


def main() -> None:
    log_message(f"Proxy listening on {HOST}:{client_config.proxy_port}")
    setup_underlay(client_config.proxy_id)

    try:
        while True:
            try:
                data, client_id = proxy_receive_cpp_packet()
            except CPPDecodeError as e:
                log_message(f"Failed to extract client_id: {e}")
                with LOCK:
                    STATS["failed_requests"] += 1
                continue
            
            if data is None or client_id is None:
                continue

            thread = threading.Thread(
                target=handle_client,
                args=(data, client_id),
                daemon=True
            )
            thread.start()

    except KeyboardInterrupt:
        log_message("Proxy shutting down.")
        log_message(f"Final stats: {STATS}")
    finally:
        cleanup_underlay()


if __name__ == "__main__":
    main()