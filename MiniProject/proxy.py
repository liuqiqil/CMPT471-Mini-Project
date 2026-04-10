from datetime import datetime
import socket
import sys
import threading
from typing import Dict, Optional

from utils.client_transport_underlay import (
    cleanup_underlay,
    proxy_receive_cpp_packet,
    send_cpp_packet,
    setup_underlay,
)
from utils.cpp import CPPDecodeError, CPPStatus, decode_cpp, encode_cpp
from utils.globals import BUFFER_SIZE, Resource, TIMEOUT, base64_encode
from utils.network_config import ClientNetworkConfig, ServerNetworkConfig

MAX_PROXY_WORKERS = 20
WORKER_SEMAPHORE = threading.BoundedSemaphore(MAX_PROXY_WORKERS)

LOCK = threading.Lock()

STATS = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "proxy_busy": 0,
    "internal_errors": 0,
}

# Compound session table
COMPOUND_SESSION_MAP: Dict[tuple[int, int], dict] = {}
B_SESSION_BY_CLIENT_RESOURCE: Dict[tuple[int, Resource], str] = {}

server_config = ServerNetworkConfig()
client_config = ClientNetworkConfig()
CURRENT_PROXY_ID: Optional[int] = None


def log_message(message: str) -> None:
    timestamp = datetime.now().strftime("%a %b %d %H:%M:%S %Y")
    thread_id = threading.get_ident()
    print(f"[{timestamp}] [Thread:{thread_id}] {message}")


def parse_http_response(http_response: bytes):
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


def read_http_chunk(sock: socket.socket):
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
        chunk_data += sock.recv(chunk_size - len(chunk_data))

    read_until(b"\r\n")
    return chunk_data, False


def try_parse_one_chunk_from_buffer(buffer: bytes) -> tuple[bytes | None, bytes, bool]:
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


def return_to_client(
    status_code: int,
    body: bytes,
    client_id: int,
    resource: Resource,
    request_id: int,
) -> None:
    if CURRENT_PROXY_ID is None:
        raise RuntimeError("Current proxy ID is not initialized")

    response = encode_cpp(
        source_id=CURRENT_PROXY_ID,
        resource=resource,
        payload=body.decode(errors="replace"),
        status=CPPStatus(status_code),
        request_id=request_id,
    )
    send_cpp_packet(client_id, response)


def stream_chunks_to_client(
    source_sock: socket.socket,
    client_id: int,
    resource: Resource,
    request_id: int,
    initial_body: bytes,
) -> None:
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
            return_to_client(
                CPPStatus.SUCCESS_DONE.value,
                b"",
                client_id,
                resource,
                request_id,
            )
            break

        if chunk_data:
            return_to_client(
                CPPStatus.SUCCESS_PARTIAL.value,
                chunk_data,
                client_id,
                resource,
                request_id,
            )


def map_resource_to_service(resource: Resource) -> str:
    if resource == Resource.PAGE:
        return server_config.get_service_id(Resource.PAGE)
    if resource == Resource.STREAM:
        return server_config.get_service_id(Resource.STREAM)
    if resource == Resource.PING:
        return "b.network.status"
    raise ValueError(f"Unsupported resource: {resource}")


def build_gateway_request(
    path: str,
    headers: Dict[str, str] | None = None,
) -> bytes:
    gateway_host, gateway_port = server_config.b_gateway_endpoint

    header_lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {gateway_host}:{gateway_port}",
        f"Authorization: Basic {base64_encode(server_config.interop_auth_token)}",
    ]

    if headers:
        for key, value in headers.items():
            header_lines.append(f"{key}: {value}")

    request = "\r\n".join(header_lines) + "\r\n\r\n"
    return request.encode()


def forward_to_b_network(client_id: int, resource: Resource, request_id: int) -> None:
    gateway_host, gateway_port = server_config.b_gateway_endpoint

    if resource == Resource.PING:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(TIMEOUT)
            sock.connect((gateway_host, gateway_port))
            sock.sendall(build_gateway_request("/status"))
            initial_data = sock.recv(BUFFER_SIZE)
            status_code, body, _, _ = parse_http_response(initial_data)

            if status_code != 200:
                return_to_client(
                    CPPStatus.INTERNAL_ERROR.value,
                    body if body else b"B-network status error.",
                    client_id,
                    resource,
                    request_id,
                )
                return

            with LOCK:
                STATS["successful_requests"] += 1

            return_to_client(
                CPPStatus.SUCCESS_DONE.value,
                body,
                client_id,
                resource,
                request_id,
            )
            return

        except (ConnectionRefusedError, socket.timeout, OSError):
            with LOCK:
                STATS["failed_requests"] += 1
            return_to_client(
                CPPStatus.SERVER_UNREACHABLE.value,
                b"B-network gateway unreachable.",
                client_id,
                resource,
                request_id,
            )
            return

        except Exception as exc:
            with LOCK:
                STATS["failed_requests"] += 1
                STATS["internal_errors"] += 1
            return_to_client(
                CPPStatus.INTERNAL_ERROR.value,
                f"Interop proxy error: {exc}".encode(),
                client_id,
                resource,
                request_id,
            )
            return

        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    service_id = map_resource_to_service(resource)
    interop_session_id = f"interop-{client_id}-{request_id}"

    session_key = (client_id, resource)
    with LOCK:
        b_session_id = B_SESSION_BY_CLIENT_RESOURCE.get(session_key)
        if b_session_id is None:
            b_session_id = f"b-session-{client_id}-{resource.value}"
            B_SESSION_BY_CLIENT_RESOURCE[session_key] = b_session_id

        COMPOUND_SESSION_MAP[(client_id, request_id)] = {
            "a_client_id": client_id,
            "a_request_id": request_id,
            "a_resource": resource.name,
            "b_service_id": service_id,
            "b_session_id": b_session_id,
            "interop_session_id": interop_session_id,
        }

    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TIMEOUT)
        sock.connect((gateway_host, gateway_port))
        sock.sendall(
            build_gateway_request(
                f"/service/{service_id}",
                {
                    "X-A-Request-ID": str(request_id),
                    "X-Interop-Session-ID": interop_session_id,
                    "X-B-Session-ID": b_session_id,
                },
            )
        )

        initial_data = sock.recv(BUFFER_SIZE)
        status_code, body, is_chunked, _ = parse_http_response(initial_data)

        if status_code == 503:
            with LOCK:
                STATS["failed_requests"] += 1
            return_to_client(
                CPPStatus.SERVER_BUSY.value,
                body if body else b"B-network busy.",
                client_id,
                resource,
                request_id,
            )
            return

        if status_code != 200:
            with LOCK:
                STATS["failed_requests"] += 1
            return_to_client(
                CPPStatus.INTERNAL_ERROR.value,
                body if body else b"Interop proxy received B-network error.",
                client_id,
                resource,
                request_id,
            )
            return

        with LOCK:
            STATS["successful_requests"] += 1

        if not is_chunked:
            return_to_client(
                CPPStatus.SUCCESS_DONE.value,
                body,
                client_id,
                resource,
                request_id,
            )
        else:
            stream_chunks_to_client(
                sock,
                client_id,
                resource,
                request_id,
                body,
            )

    except (ConnectionRefusedError, socket.timeout, OSError):
        with LOCK:
            STATS["failed_requests"] += 1
        return_to_client(
            CPPStatus.SERVER_UNREACHABLE.value,
            b"B-network gateway unreachable.",
            client_id,
            resource,
            request_id,
        )

    except Exception as exc:
        with LOCK:
            STATS["failed_requests"] += 1
            STATS["internal_errors"] += 1
        return_to_client(
            CPPStatus.INTERNAL_ERROR.value,
            f"Interop proxy forwarding error: {exc}".encode(),
            client_id,
            resource,
            request_id,
        )

    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def handle_client(data: bytes, client_id: int) -> None:
    acquired = WORKER_SEMAPHORE.acquire(blocking=False)
    if not acquired:
        with LOCK:
            STATS["proxy_busy"] += 1

        try:
            request_obj = decode_cpp(data)
            return_to_client(
                CPPStatus.PROXY_BUSY.value,
                b"",
                client_id,
                request_obj["resource"],
                request_obj["request_id"],
            )
        except Exception:
            pass
        return

    request_obj = None

    try:
        if not client_config.is_authorized(client_id):
            with LOCK:
                STATS["failed_requests"] += 1
            return

        if not data:
            return

        with LOCK:
            STATS["total_requests"] += 1

        try:
            request_obj = decode_cpp(data)
            log_message(
                f"Received A-network request from client {client_id} "
                f"for resource {request_obj['resource'].name}, "
                f"request_id={request_obj['request_id']}"
            )
        except CPPDecodeError as e:
            with LOCK:
                STATS["failed_requests"] += 1
            target_client_id = getattr(e, "source_id", client_id)
            if target_client_id is not None:
                return_to_client(
                    CPPStatus.INVALID_REQUEST.value,
                    str(e).encode(),
                    target_client_id,
                    Resource.PAGE,
                    0,
                )
            return

        forward_to_b_network(
            request_obj["source_id"],
            request_obj["resource"],
            request_obj["request_id"],
        )

    except Exception as exc:
        with LOCK:
            STATS["failed_requests"] += 1
            STATS["internal_errors"] += 1

        try:
            resource = request_obj["resource"] if request_obj else Resource.PAGE
            request_id = request_obj["request_id"] if request_obj else 0
            return_to_client(
                CPPStatus.INTERNAL_ERROR.value,
                f"Interop proxy internal error: {exc}".encode(),
                client_id,
                resource,
                request_id,
            )
        except Exception:
            pass

    finally:
        WORKER_SEMAPHORE.release()


def resolve_proxy_id() -> int:
    if len(sys.argv) == 1:
        return client_config.proxy_id

    if len(sys.argv) == 2:
        try:
            return int(sys.argv[1])
        except ValueError:
            sys.exit("Proxy ID must be an integer.")

    sys.exit("Usage: python proxy.py <proxy_id>")


def main() -> None:
    global CURRENT_PROXY_ID

    CURRENT_PROXY_ID = resolve_proxy_id()

    if CURRENT_PROXY_ID not in client_config.proxy_ids:
        sys.exit(f"Invalid proxy ID: {CURRENT_PROXY_ID}")

    proxy_port = client_config.get_proxy_port(CURRENT_PROXY_ID)
    log_message(
        f"Interoperation proxy {CURRENT_PROXY_ID} listening on UDP underlay port {proxy_port}"
    )
    setup_underlay(CURRENT_PROXY_ID)

    try:
        while True:
            try:
                data, client_id = proxy_receive_cpp_packet()
            except CPPDecodeError as e:
                log_message(f"Failed to extract client_id: {e}")
                continue

            if data is None or client_id is None:
                continue

            thread = threading.Thread(
                target=handle_client,
                args=(data, client_id),
                daemon=True,
            )
            thread.start()

    except KeyboardInterrupt:
        log_message(f"Interoperation proxy {CURRENT_PROXY_ID} shutting down.")
        log_message(f"Final stats: {STATS}")
    finally:
        cleanup_underlay()


if __name__ == "__main__":
    main()