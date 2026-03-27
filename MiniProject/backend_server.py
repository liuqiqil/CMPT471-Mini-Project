import socket
import threading
import sys
from typing import Dict

HOST = "127.0.0.1"
BUFFER_SIZE = 4096

CONTENT_DB: Dict[str, str] = {
    "/": "Welcome to the backend service.",
    "/test": "This is the test resource.",
    "/hello": "Hello from the backend server.",
    "/health": "OK"
}


def log_message(server_name: str, message: str) -> None:
    print(f"[{server_name}] {message}")


def build_response(status_line: str, body: str) -> bytes:
    response = (
        f"{status_line}\r\n"
        f"Content-Length: {len(body.encode())}\r\n"
        f"Content-Type: text/plain\r\n"
        f"\r\n"
        f"{body}"
    )
    return response.encode()


def handle_client_connection(client_connection: socket.socket, server_name: str) -> None:
    try:
        request = client_connection.recv(BUFFER_SIZE).decode()
        if not request:
            return

        request_line = request.split("\r\n")[0]
        log_message(server_name, f"Received: {request_line}")

        parts = request_line.split()
        if len(parts) < 2:
            client_connection.sendall(
                build_response("HTTP/1.0 400 Bad Request", "Bad Request")
            )
            return

        method, path = parts[0], parts[1]

        if method != "GET":
            client_connection.sendall(
                build_response("HTTP/1.0 405 Method Not Allowed", "Method Not Allowed")
            )
            return

        if path in CONTENT_DB:
            body = f"{server_name} served: {CONTENT_DB[path]}"
            client_connection.sendall(
                build_response("HTTP/1.0 200 OK", body)
            )
        else:
            client_connection.sendall(
                build_response("HTTP/1.0 404 Not Found", "Resource Not Found")
            )

    except Exception as exc:
        log_message(server_name, f"Error: {exc}")
    finally:
        client_connection.close()


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("Usage: python backend_server.py <port> <server_name>")

    port = int(sys.argv[1])
    server_name = sys.argv[2]

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, port))
    server_socket.listen(5)

    log_message(server_name, f"Listening on {HOST}:{port}")

    try:
        while True:
            client_connection, _ = server_socket.accept()
            thread = threading.Thread(
                target=handle_client_connection,
                args=(client_connection, server_name),
                daemon=True
            )
            thread.start()
    except KeyboardInterrupt:
        log_message(server_name, "Shutting down.")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()