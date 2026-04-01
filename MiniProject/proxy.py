import socket
import threading
import json
import time
from typing import Dict, List, Tuple, Optional

BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 5

# Simple session map
SESSION_MAP: Dict[str, Tuple[str, int, str]] = {}

# Round-robin pointer for new clients
NEXT_BACKEND_INDEX = 0

# Basic stats
STATS = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "failover_count": 0,
}

LOCK = threading.Lock()


def log_message(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open("proxy_log.txt", "a", encoding="utf-8") as log_file:
        log_file.write(line + "\n")


def make_json_response(
    status: int,
    body: str = "",
    error: str = "",
    server: str = "",
    latency_ms: Optional[float] = None
) -> bytes:
    response = {
        "status": status,
        "server": server,
        "body": body,
    }
    if error:
        response["error"] = error
    if latency_ms is not None:
        response["latency_ms"] = round(latency_ms, 2)
    return json.dumps(response).encode()


def choose_backends_for_client(client_id: str) -> List[Tuple[str, int, str]]:

    # For repeated requests from the same client, use the same backend.
    # If it fails, try the others.

    global NEXT_BACKEND_INDEX

    with LOCK:
        if client_id in SESSION_MAP:
            preferred = SESSION_MAP[client_id]
            others = [srv for srv in BACKEND_SERVERS if srv != preferred]
            return [preferred] + others

        chosen = BACKEND_SERVERS[NEXT_BACKEND_INDEX]
        NEXT_BACKEND_INDEX = (NEXT_BACKEND_INDEX + 1) % len(BACKEND_SERVERS)
        others = [srv for srv in BACKEND_SERVERS if srv != chosen]
        return [chosen] + others


def translate_json_to_http(request_obj: dict) -> Optional[str]:

    # Translate a simple JSON request into HTTP.
    # Expected input: { "action": "fetch", "resource": "/test", "client_id": "client1"}

    action = request_obj.get("action")
    resource = request_obj.get("resource")
    client_id = request_obj.get("client_id", "unknown")

    if action != "fetch" or not resource:
        return None

    http_request = (
        f"GET {resource} HTTP/1.0\r\n"
        f"Host: 127.0.0.1\r\n"
        f"X-Client-ID: {client_id}\r\n"
        f"X-Proxy-Bridge: basic-interoperation\r\n"
        f"\r\n"
    )
    return http_request


def parse_http_response(http_response: str) -> Tuple[int, str]:
    status_code = 500
    body = ""

    lines = http_response.split("\r\n")
    if lines:
        parts = lines[0].split()
        if len(parts) >= 2 and parts[1].isdigit():
            status_code = int(parts[1])

    separator = "\r\n\r\n"
    if separator in http_response:
        body = http_response.split(separator, 1)[1]

    return status_code, body


def forward_to_backend(http_request: str, backend: Tuple[str, int, str]) -> Tuple[int, str]:
    host, port, _ = backend
    backend_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backend_socket.settimeout(SOCKET_TIMEOUT)

    try:
        backend_socket.connect((host, port))
        backend_socket.sendall(http_request.encode())

        response_data = ""
        while True:
            chunk = backend_socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            response_data += chunk.decode()

        return parse_http_response(response_data)
    finally:
        backend_socket.close()


def handle_client(client_connection: socket.socket, client_address: Tuple[str, int]) -> None:
    start_time = time.time()

    try:
        raw_data = client_connection.recv(BUFFER_SIZE).decode()
        if not raw_data:
            return

        log_message(f"Received from {client_address}: {raw_data}")

        with LOCK:
            STATS["total_requests"] += 1

        try:
            request_obj = json.loads(raw_data)
        except json.JSONDecodeError:
            with LOCK:
                STATS["failed_requests"] += 1
            client_connection.sendall(
                make_json_response(400, error="Invalid JSON request")
            )
            return

        client_id = request_obj.get("client_id", "unknown")
        http_request = translate_json_to_http(request_obj)

        if http_request is None:
            with LOCK:
                STATS["failed_requests"] += 1
            client_connection.sendall(
                make_json_response(400, error="Unsupported request format")
            )
            return

        candidate_backends = choose_backends_for_client(client_id)
        last_error = None

        for index, backend in enumerate(candidate_backends):
            host, port, server_name = backend
            try:
                status_code, body = forward_to_backend(http_request, backend)

                with LOCK:
                    SESSION_MAP[client_id] = backend
                    STATS["successful_requests"] += 1
                    if index > 0:
                        STATS["failover_count"] += 1

                latency_ms = (time.time() - start_time) * 1000
                log_message(
                    f"Client {client_id} -> {server_name} ({host}:{port}), "
                    f"status={status_code}, latency={latency_ms:.2f} ms"
                )

                client_connection.sendall(
                    make_json_response(
                        status=status_code,
                        body=body,
                        server=server_name,
                        latency_ms=latency_ms,
                    )
                )
                return

            except Exception as exc:
                last_error = str(exc)
                log_message(f"Backend {server_name} failed: {exc}")

        with LOCK:
            STATS["failed_requests"] += 1

        client_connection.sendall(
            make_json_response(503, error=f"No backend available: {last_error}")
        )

    except Exception as exc:
        log_message(f"Proxy error: {exc}")
        with LOCK:
            STATS["failed_requests"] += 1
        try:
            client_connection.sendall(
                make_json_response(500, error="Internal proxy error")
            )
        except Exception:
            pass
    finally:
        client_connection.close()


def main() -> None:
    open("proxy_log.txt", "w", encoding="utf-8").close()

    proxy_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    proxy_server.bind((PROXY_HOST, PROXY_PORT))
    proxy_server.listen(5)

    log_message(f"Proxy listening on {PROXY_HOST}:{PROXY_PORT}")

    try:
        while True:
            client_connection, client_address = proxy_server.accept()
            thread = threading.Thread(
                target=handle_client,
                args=(client_connection, client_address),
                daemon=True
            )
            thread.start()
    except KeyboardInterrupt:
        log_message("Proxy shutting down.")
        log_message(f"Final stats: {STATS}")
    finally:
        proxy_server.close()


if __name__ == "__main__":
    main()