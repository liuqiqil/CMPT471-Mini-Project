from concurrent.futures import ThreadPoolExecutor
import logging
import os
import socket
import sys
import time
from typing import Dict

from utils.globals import (
    BUFFER_SIZE,
    HOST,
    LOGGING_DIR,
    Resource,
    STREAM_RESPONSE_COUNT,
    STREAM_RESPONSE_INTERVAL,
    base64_encode,
)
from utils.network_config import ServerNetworkConfig

MAX_CONNECTIONS = 5

PATH_DB = {
    Resource.PING: "/ping",
    Resource.PAGE: "/page",
    Resource.STREAM: "/stream",
}

RESPONSE_DB: Dict[Resource, str] = {
    Resource.PING: "Pong from B-endpoint!",
    Resource.PAGE: "Welcome to the B-network content endpoint.",
    Resource.STREAM: "B-network streaming content...",
}

config = ServerNetworkConfig()
loggers = {}


def log_message(server_port: int, message: str) -> None:
    if server_port not in loggers:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, LOGGING_DIR)
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"server_{server_port}.log")

        logger = logging.getLogger(str(server_port))
        logger.setLevel(logging.INFO)

        handler = logging.FileHandler(log_file_path)
        formatter = logging.Formatter(
            "[%(asctime)s] [Thread:%(thread)d] %(message)s",
            datefmt="%a %b %d %H:%M:%S %Y",
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        loggers[server_port] = logger

    loggers[server_port].info(message)


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


def handle_client_connection(
    client_connection: socket.socket,
    server_port: int,
    content_type: Resource,
) -> None:
    try:
        request = client_connection.recv(BUFFER_SIZE).decode()
        if not request:
            return

        request_parts = request.split("\r\n")
        request_line = request_parts[0]
        log_message(server_port, f"Received: {request_line}")

        parts = request_line.split()
        if len(parts) < 2:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 400 Bad Request",
                    "Bad Request",
                )
            )
            log_message(server_port, "400 Bad Request: Malformed request line")
            return

        method = parts[0]
        path = parts[1]
        version = parts[2] if len(parts) > 2 else "HTTP/1.0"

        if version != "HTTP/1.1":
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 505 HTTP Version Not Supported",
                    "HTTP 1.0 Not Supported. Use HTTP 1.1",
                )
            )
            log_message(server_port, f"505 HTTP Version Not Supported: {version}")
            return

        if method != "GET":
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 405 Method Not Allowed",
                    "Method Not Allowed",
                )
            )
            log_message(server_port, f"405 Method Not Allowed: {method}")
            return

        headers = {}
        for line in request_parts[1:]:
            if line == "" or line == "\r":
                break
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.strip().lower()] = value.strip()

        auth_header = headers.get("authorization")
        expected_token = "Basic " + base64_encode(config.proxy_auth_token)

        if not auth_header:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 401 Unauthorized",
                    "Authorization Required",
                )
            )
            log_message(server_port, "401 Unauthorized: Missing Authorization Header")
            return

        if auth_header != expected_token:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 403 Forbidden",
                    "Invalid Token",
                )
            )
            log_message(server_port, "403 Forbidden: Invalid Token")
            return

        origin_network = headers.get("x-origin-network")
        b_service_id = headers.get("x-b-service-id")
        b_session_id = headers.get("x-b-session-id")

        if origin_network != "B_GATEWAY":
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 400 Bad Request",
                    "Missing or invalid X-Origin-Network header",
                )
            )
            log_message(server_port, "400 Bad Request: Invalid origin network")
            return

        if not b_service_id or not b_session_id:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 400 Bad Request",
                    "Missing B-network session headers",
                )
            )
            log_message(server_port, "400 Bad Request: Missing B-network headers")
            return

        response_headers = {
            "X-B-Service-ID": b_service_id,
            "X-B-Session-ID": b_session_id,
        }

        if path == PATH_DB[content_type]:
            body = RESPONSE_DB[content_type]

            if content_type == Resource.STREAM:
                client_connection.sendall(
                    build_chunked_http_header("HTTP/1.1 200 OK", response_headers)
                )
                for i in range(STREAM_RESPONSE_COUNT):
                    chunk_content = f"{body} - Part {i + 1}"
                    client_connection.sendall(build_chunked_body(chunk_content))
                    if i == STREAM_RESPONSE_COUNT - 1:
                        client_connection.sendall(b"0\r\n\r\n")
                    time.sleep(STREAM_RESPONSE_INTERVAL)
            else:
                client_connection.sendall(
                    build_standard_http_response(
                        "HTTP/1.1 200 OK",
                        body,
                        response_headers,
                    )
                )
        else:
            client_connection.sendall(
                build_standard_http_response(
                    "HTTP/1.1 404 Not Found",
                    "Resource Not Found",
                    response_headers,
                )
            )

    except Exception as exc:
        log_message(server_port, f"Error: {exc}")
    finally:
        client_connection.close()


def start_server(port: int, content_type: Resource) -> None:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, port))
    server_socket.listen(MAX_CONNECTIONS)
    log_message(port, f"B-endpoint for {content_type.name} started on port {port}")

    with ThreadPoolExecutor(max_workers=MAX_CONNECTIONS) as executor:
        while True:
            try:
                client_conn, client_addr = server_socket.accept()
                log_message(port, f"Accepted connection from port {client_addr[1]}")

                if len(executor._threads) >= MAX_CONNECTIONS:
                    log_message(port, "Endpoint busy. Sending 503.")
                    busy_resp = build_standard_http_response(
                        "HTTP/1.1 503 Service Unavailable",
                        "B-endpoint Busy",
                    )
                    client_conn.sendall(busy_resp)
                    client_conn.close()
                    continue

                executor.submit(
                    handle_client_connection,
                    client_conn,
                    port,
                    content_type,
                )
            except Exception as e:
                log_message(port, f"Error accepting connection: {e}")


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("Usage: python backend_server.py <port> <resource_name>")

    port = int(sys.argv[1])
    resource_name = sys.argv[2].upper()

    try:
        content_type = Resource[resource_name]
    except KeyError:
        sys.exit(f"Invalid resource: {resource_name}")

    try:
        start_server(port, content_type)
    except KeyboardInterrupt:
        print(f"\nB-endpoint on port {port} shutting down.")


if __name__ == "__main__":
    main()