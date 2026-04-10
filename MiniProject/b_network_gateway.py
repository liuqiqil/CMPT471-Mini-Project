from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import socket
import threading
from typing import Dict

from utils.globals import BUFFER_SIZE, HOST, Resource, TIMEOUT, base64_encode
from utils.network_config import ServerNetworkConfig

MAX_GATEWAY_CONNECTIONS = 20

PATH_DB = {
    Resource.PING: "/ping",
    Resource.PAGE: "/page",
    Resource.STREAM: "/stream",
}

LOCK = threading.Lock()

# B-side sticky sessions
B_SESSION_MAP: Dict[str, Dict[str, str | int]] = {}
NEXT_INDEX_MAP: Dict[str, int] = {}

STATS = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "failover_count": 0,
}

server_config = ServerNetworkConfig()


def log_message(message: str) -> None:
    timestamp = datetime.now().strftime("%a %b %d %H:%M:%S %Y")
    thread_id = threading.get_ident()
    print(f"[{timestamp}] [Thread:{thread_id}] {message}")


def build_standard_http_response(
    status_line: str,
    body: str,
    extra_headers: Dict[str, str] | None = None,
) -> bytes:
    header_lines = [
        status_line,
        f"Content-Length: {len(body.encode())}",
        "Content-Type: text/plain",
    ]

    if extra_headers:
        for key, value in extra_headers.items():
            header_lines.append(f"{key}: {value}")

    response = "\r\n".join(header_lines) + "\r\n\r\n" + body
    return response.encode()


def build_chunked_http_header(
    status_line: str,
    extra_headers: Dict[str, str] | None = None,
) -> bytes:
    header_lines = [
        status_line,
        "Content-Type: text/plain",
        "Transfer-Encoding: chunked",
    ]

    if extra_headers:
        for key, value in extra_headers.items():
            header_lines.append(f"{key}: {value}")

    response = "\r\n".join(header_lines) + "\r\n\r\n"
    return response.encode()


def build_chunked_body(content: str) -> bytes:
    chunk = content.encode()
    chunk_size = f"{len(chunk):X}\r\n".encode()
    return chunk_size + chunk + b"\r\n"


def parse_http_response(http_response: bytes) -> tuple[int, bytes, bool, Dict[str, str]]:
    status_code = 500
    body = b""
    is_chunked = False
    headers = {}

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
                decoded_key = key.strip().decode().lower()
                decoded_value = value.strip().decode()
                headers[decoded_key] = decoded_value
                if decoded_key == "transfer-encoding" and "chunked" in decoded_value.lower():
                    is_chunked = True

    return status_code, body, is_chunked, headers


def read_http_chunk(sock: socket.socket) -> tuple[bytes, bool]:
    def read_until(delimiter: bytes) -> bytes:
        data = b""
        while not data.endswith(delimiter):
            char = sock.recv(1)
            if not char:
                break
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

def try_parse_one_chunk_from_buffer(buffer: bytes) -> tuple[bytes | None, bytes, bool]:
    """
    Try to parse one chunk from an in-memory buffer.

    Returns:
        (chunk_data_or_None, remaining_buffer, done)

    - chunk_data_or_None is None if buffer does not yet contain a full chunk frame
    - done=True means terminal 0-size chunk was parsed
    """
    sep = b"\r\n"
    idx = buffer.find(sep)
    if idx == -1:
        return None, buffer, False

    size_line = buffer[:idx]
    try:
        chunk_size = int(size_line.decode(), 16)
    except ValueError:
        return None, buffer, False

    needed = idx + 2 + chunk_size + 2
    if len(buffer) < needed:
        return None, buffer, False

    chunk_start = idx + 2
    chunk_end = chunk_start + chunk_size
    chunk_data = buffer[chunk_start:chunk_end]
    trailer = buffer[chunk_end:chunk_end + 2]
    if trailer != b"\r\n":
        return None, buffer, False

    remaining = buffer[needed:]

    if chunk_size == 0:
        return b"", remaining, True

    return chunk_data, remaining, False


def forward_chunked_response(
    source_sock: socket.socket,
    dest_conn: socket.socket,
    initial_body: bytes,
) -> None:
    """
    Forward a chunked HTTP response body where part of the chunk stream may already
    be present in initial_body.
    """
    buffer = initial_body
    done = False

    while not done:
        chunk_data, buffer, done = try_parse_one_chunk_from_buffer(buffer)

        if chunk_data is None:
            more = source_sock.recv(BUFFER_SIZE)
            if not more:
                break
            buffer += more
            continue

        if done:
            dest_conn.sendall(b"0\r\n\r\n")
            break

        if chunk_data:
            dest_conn.sendall(build_chunked_body(chunk_data.decode(errors="replace")))

def choose_backends_for_b_session(b_session_id: str, service_id: str):
    backends = server_config.get_service_backends(service_id)
    if not backends:
        return []

    with LOCK:
        if b_session_id in B_SESSION_MAP:
            chosen = B_SESSION_MAP[b_session_id]
            for record in backends:
                if record["host"] == chosen["host"] and record["port"] == chosen["port"]:
                    others = [
                        rec for rec in backends
                        if not (rec["host"] == chosen["host"] and rec["port"] == chosen["port"])
                    ]
                    return [record] + others

        idx = NEXT_INDEX_MAP.get(service_id, 0)
        chosen = backends[idx % len(backends)]
        NEXT_INDEX_MAP[service_id] = (idx + 1) % len(backends)
        B_SESSION_MAP[b_session_id] = {"host": chosen["host"], "port": chosen["port"]}
        others = [
            rec for rec in backends
            if not (rec["host"] == chosen["host"] and rec["port"] == chosen["port"])
        ]
        return [chosen] + others


def write_backend_request(
    resource: Resource,
    service_id: str,
    b_session_id: str,
    interop_session_id: str,
) -> bytes:
    path = PATH_DB[resource]
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {HOST}\r\n"
        f"Authorization: Basic {base64_encode(server_config.interop_auth_token)}\r\n"
        f"X-Origin-Network: B_GATEWAY\r\n"
        f"X-B-Service-ID: {service_id}\r\n"
        f"X-B-Session-ID: {b_session_id}\r\n"
        f"X-Interop-Session-ID: {interop_session_id}\r\n"
        f"\r\n"
    )
    return request.encode()


def build_status_body() -> str:
    lines = []
    records = server_config.server_records

    grouped = {}
    for record in records:
        grouped.setdefault(record["service_id"], []).append(record)

    for service_id, service_records in grouped.items():
        available = 0
        unreachable = 0

        for record in service_records:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(TIMEOUT)
                    sock.connect((record["host"], record["port"]))
                    # Lightweight reachability probe only.
                    # Do NOT issue real business requests like /stream here,
                    # otherwise stream backends may remain occupied and skew tests.
                    available += 1
            except Exception:
                unreachable += 1

        total = len(service_records)

        def pct(x, t):
            return int(x / t * 100) if t > 0 else 0

        lines.append(
            f"{service_id}: "
<<<<<<< HEAD
            f"{pct(available, total)}% ({available}/{total}) reachable, "
            # f"{pct(busy, total)}% ({busy}/{total}) busy, "
=======
            f"{pct(available, total)}% ({available}/{total}) available, "
>>>>>>> da80ea0114bfe73288066e86206e4f4c2abdc98c
            f"{pct(unreachable, total)}% ({unreachable}/{total}) unreachable"
        )

    return "\n".join(lines) if lines else "No B-network services configured."


def forward_to_b_endpoint(
    interop_conn: socket.socket,
    resource: Resource,
    service_id: str,
    b_session_id: str,
    interop_session_id: str,
) -> None:
    backends = choose_backends_for_b_session(b_session_id, service_id)

    if not backends:
        interop_conn.sendall(
            build_standard_http_response(
                "HTTP/1.1 404 Not Found",
                "Unknown B-network service.",
            )
        )
        return

    any_busy = False

    for index, backend in enumerate(backends):
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(TIMEOUT)
            sock.connect((backend["host"], backend["port"]))
            sock.sendall(
                write_backend_request(
                    resource,
                    service_id,
                    b_session_id,
                    interop_session_id,
                )
            )

            initial_data = sock.recv(BUFFER_SIZE)
            status_code, body, is_chunked, _ = parse_http_response(initial_data)

            if status_code == 503:
                any_busy = True
                if sock is not None:
                    sock.close()
                    sock = None
                continue

            if status_code != 200:
                interop_conn.sendall(
                    build_standard_http_response(
                        "HTTP/1.1 502 Bad Gateway",
                        body.decode(errors="replace") if body else "B-endpoint error",
                        {
                            "X-B-Service-ID": service_id,
                            "X-B-Session-ID": b_session_id,
                            "X-Interop-Session-ID": interop_session_id,
                        },
                    )
                )
                return

            with LOCK:
                STATS["successful_requests"] += 1
                if index > 0:
                    STATS["failover_count"] += 1

            response_headers = {
                "X-B-Service-ID": service_id,
                "X-B-Session-ID": b_session_id,
                "X-Interop-Session-ID": interop_session_id,
            }

            if not is_chunked:
                interop_conn.sendall(
                    build_standard_http_response(
                        "HTTP/1.1 200 OK",
                        body.decode(errors="replace"),
                        response_headers,
                    )
                )
            else:
                interop_conn.sendall(
                    build_chunked_http_header("HTTP/1.1 200 OK", response_headers)
                )
                forward_chunked_response(sock, interop_conn, body)
            return

        except (ConnectionRefusedError, socket.timeout, OSError):
            continue
        except Exception as exc:
            interop_conn.sendall(
                build_standard_http_response(
                    "HTTP/1.1 500 Internal Server Error",
                    f"B-network gateway forwarding error: {exc}",
                )
            )
            return
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    with LOCK:
        STATS["failed_requests"] += 1

    if any_busy:
        interop_conn.sendall(
            build_standard_http_response(
                "HTTP/1.1 503 Service Unavailable",
                "All B-network endpoints are busy.",
            )
        )
    else:
        interop_conn.sendall(
            build_standard_http_response(
                "HTTP/1.1 504 Gateway Timeout",
                "All B-network endpoints are unreachable.",
            )
        )


def handle_interop_connection(client_connection: socket.socket) -> None:
    try:
        request = client_connection.recv(BUFFER_SIZE).decode()
        if not request:
            return

        with LOCK:
            STATS["total_requests"] += 1

        request_parts = request.split("\r\n")
        request_line = request_parts[0]
        parts = request_line.split()
        if len(parts) < 2:
            client_connection.sendall(
                build_standard_http_response("HTTP/1.1 400 Bad Request", "Bad Request")
            )
            return

        method = parts[0]
        path = parts[1]
        version = parts[2] if len(parts) > 2 else "HTTP/1.0"

        if version != "HTTP/1.1":
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 505 HTTP Version Not Supported",
                    "Use HTTP/1.1",
                )
            )
            return

        if method != "GET":
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 405 Method Not Allowed",
                    "Method Not Allowed",
                )
            )
            return

        headers = {}
        for line in request_parts[1:]:
            if line == "" or line == "\r":
                break
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.strip().lower()] = value.strip()

        auth_header = headers.get("authorization")
        expected_token = "Basic " + base64_encode(server_config.interop_auth_token)
        if auth_header != expected_token:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 403 Forbidden",
                    "Invalid Token",
                )
            )
            return

        if path == "/status":
            body = build_status_body()
            client_connection.sendall(
                build_standard_http_response("HTTP/1.1 200 OK", body)
            )
            return

        if not path.startswith("/service/"):
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 404 Not Found",
                    "Unknown B-network route",
                )
            )
            return

        service_id = path[len("/service/"):]
        b_session_id = headers.get("x-b-session-id")
        interop_session_id = headers.get("x-interop-session-id")
        a_request_id = headers.get("x-a-request-id")

        if not b_session_id or not interop_session_id or not a_request_id:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 400 Bad Request",
                    "Missing A/B interoperation session headers.",
                )
            )
            return

        backends = server_config.get_service_backends(service_id)
        if not backends:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 404 Not Found",
                    "Unknown service_id.",
                )
            )
            return

        resource = backends[0]["resource"]

        forward_to_b_endpoint(
            client_connection,
            resource,
            service_id,
            b_session_id,
            interop_session_id,
        )

    except Exception as exc:
        client_connection.sendall(
            build_standard_http_response(
                "HTTP/1.1 500 Internal Server Error",
                f"B-network gateway error: {exc}",
            )
        )
    finally:
        client_connection.close()


def start_gateway() -> None:
    gateway_host, gateway_port = server_config.b_gateway_endpoint

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((gateway_host, gateway_port))
    server_socket.listen(MAX_GATEWAY_CONNECTIONS)

    log_message(f"B-network gateway started on {gateway_host}:{gateway_port}")

    with ThreadPoolExecutor(max_workers=MAX_GATEWAY_CONNECTIONS) as executor:
        while True:
            client_conn, _ = server_socket.accept()
            executor.submit(handle_interop_connection, client_conn)


def main() -> None:
    try:
        start_gateway()
    except KeyboardInterrupt:
        log_message("B-network gateway shutting down.")
        log_message(f"Final stats: {STATS}")


if __name__ == "__main__":
    main()