from concurrent.futures import ThreadPoolExecutor
import logging
import os
import socket
import threading
import sys
import time
from utils.network_config import ServerNetworkConfig
from utils.globals import Resource, HOST, STREAM_RESPONSE_COUNT, STREAM_RESPONSE_INTERVAL, base64_encode
from typing import Dict

BUFFER_SIZE = 4096
MAX_CONNECTIONS = 5
LOGGING_PATH = "logs"

PATH_DB: Dict[str, Resource] = {
    "/ping": Resource.PING,
    "/page": Resource.PAGE,
    "/stream": Resource.STREAM
}

RESPONSE_DB: Dict[Resource, str] = {
    Resource.PING: "Pong!",
    Resource.PAGE: "Welcome to the backend service.",
    Resource.STREAM: "Streaming content..."
}

config = ServerNetworkConfig()

loggers = {}

def log_message(server_port: int, message: str) -> None:
    if server_port not in loggers:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, LOGGING_PATH)
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"server_{server_port}.log")

        logger = logging.getLogger(str(server_port))
        logger.setLevel(logging.INFO)
        
        handler = logging.FileHandler(log_file_path)
        formatter = logging.Formatter('[%(asctime)s] [Thread:%(thread)d] %(message)s', datefmt='%a %b %d %H:%M:%S %Y')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        loggers[server_port] = logger

    loggers[server_port].info(message)

def build_standard_http_response(status_line: str, body: str) -> bytes:
    response = (
        f"{status_line}\r\n"
        f"Content-Length: {len(body.encode())}\r\n"
        f"Content-Type: text/plain\r\n"
        f"\r\n"
        f"{body}"
    )
    return response.encode()

def build_chunked_http_header(status_line: str) -> bytes:
    response = (
        f"{status_line}\r\n"
        f"Content-Type: text/plain\r\n"
        f"Transfer-Encoding: chunked\r\n"
        f"\r\n"
    )
    return response.encode()

def build_chunked_body(content: str) -> bytes:
    chunk = content.encode()
    chunk_size = f"{len(chunk):X}\r\n".encode()
    return chunk_size + chunk + b"\r\n"

def handle_client_connection(client_connection: socket.socket, server_port: int, content_type: Resource) -> None:
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
                build_standard_http_response("HTTP/1.1 400 Bad Request", "Bad Request")
            )
            log_message(server_port, "400 Bad Request: Malformed request line")
            return

        method, path, version = parts[0], parts[1], parts[2] if len(parts) > 2 else "HTTP/1.0"
        
        if version != "HTTP/1.1":
            client_connection.sendall(
                build_standard_http_response("HTTP/1.1 505 HTTP Version Not Supported", "HTTP 1.0 Not Supported. Use HTTP 1.1")
            )
            log_message(server_port, f"505 HTTP Version Not Supported: {version}")
            return

        if method != "GET":
            client_connection.sendall(
                build_standard_http_response("HTTP/1.1 405 Method Not Allowed", "Method Not Allowed")
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
        log_message(server_port, f"Authorization Header: {auth_header}")

        if not auth_header:
            client_connection.sendall(
                build_standard_http_response("HTTP/1.1 401 Unauthorized", "Authorization Required")
            )
            log_message(server_port, "401 Unauthorized: Missing Authorization Header")
            return

        EXPECTED_TOKEN = "Basic " + base64_encode(config.proxy_auth_token)
        if auth_header != EXPECTED_TOKEN:
            client_connection.sendall(
                build_standard_http_response("HTTP/1.1 403 Forbidden", "Invalid Token")
            )
            log_message(server_port, f"403 Forbidden: Invalid Token - Received: {auth_header}, Expected: {EXPECTED_TOKEN}")
            return

        if path in PATH_DB and PATH_DB[path] == content_type:
            body = RESPONSE_DB[content_type]
            if content_type == Resource.STREAM:
                for i in range(STREAM_RESPONSE_COUNT):
                    if i == 0:
                        client_connection.sendall(build_chunked_http_header("HTTP/1.1 200 OK"))
                    chunk_content = f"{body} - Part {i+1}"
                    client_connection.sendall(build_chunked_body(chunk_content))
                    if i == STREAM_RESPONSE_COUNT - 1:
                        client_connection.sendall(b"0\r\n\r\n")  # End of chunks
                    time.sleep(STREAM_RESPONSE_INTERVAL)
            else:
                client_connection.sendall(
                    build_standard_http_response("HTTP/1.1 200 OK", body)
                )
        else:
            client_connection.sendall(
                build_standard_http_response("HTTP/1.1 404 Not Found", "Resource Not Found")
            )

    except Exception as exc:
        log_message(server_port, f"Error: {exc}")
    finally:
        client_connection.close()
        
def start_server(port: int, content_type: Resource) -> None:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, port))
    server_socket.listen(MAX_CONNECTIONS)
    log_message(port, f"Server for {content_type.name} started on port {port}")

    with ThreadPoolExecutor(max_workers=MAX_CONNECTIONS) as executor:
            while True:
                try:
                    client_conn, client_addr = server_socket.accept()
                    log_message(port, f"Accepted connection from port {client_addr[1]}")
                    
                    # Send simple server full response if max connections are reached
                    if len(executor._threads) >= MAX_CONNECTIONS:
                        log_message(port, "Server Full. Sending 503.")
                        busy_resp = build_standard_http_response("HTTP/1.1 503 Service Unavailable", "Server Busy")
                        client_conn.sendall(busy_resp)
                        client_conn.close()
                        continue
                    
                    executor.submit(
                        handle_client_connection, 
                        client_conn, 
                        port, 
                        content_type
                    )
                except Exception as e:
                    log_message(port, f"Error accepting connection: {e}")


def main() -> None:
    if len(sys.argv) != 1:
        sys.exit("Usage: python backend_server.py")
    
    threads = []
    for content_type, port in config.server_ports:
        t =threading.Thread(
            target=start_server,
            args=(port, Resource[content_type]),
            daemon=True
        )
        t.start()
        threads.append(t)
        print(f"Started server for {content_type} on port {port}")
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1) 
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()