import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


HOST = "127.0.0.1"
GATEWAY_PORT = 8500

BACKEND_ENDPOINTS = [
    (9001, "PAGE"),
    (9002, "PAGE"),
    (9003, "STREAM"),
    (9004, "STREAM"),
]


class FixedResponseHTTPServer:

    # A tiny TCP server used by tests to simulate backend behavior.
    # delay before responding (for PROXY_BUSY tests)
    # return a fixed HTTP response (for INTERNAL_ERROR tests)


    def __init__(self, port: int, response_bytes: bytes, delay_before_response: float = 0.0):
        self.port = port
        self.response_bytes = response_bytes
        self.delay_before_response = delay_before_response
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._server_socket = None
        self._client_threads = []

    def start(self):
        self._thread.start()
        time.sleep(0.1)

    def stop(self):
        self._stop_event.set()

        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except Exception:
                pass

        for t in self._client_threads:
            t.join(timeout=1)

        self._thread.join(timeout=1)

    def _handle_client(self, conn: socket.socket):
        try:
            conn.recv(4096)
            if self.delay_before_response > 0:
                time.sleep(self.delay_before_response)
            conn.sendall(self.response_bytes)
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _serve(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket = server_socket
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, self.port))
        server_socket.listen(20)
        server_socket.settimeout(0.2)

        try:
            while not self._stop_event.is_set():
                try:
                    conn, _ = server_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                t = threading.Thread(
                    target=self._handle_client,
                    args=(conn,),
                    daemon=True,
                )
                self._client_threads.append(t)
                t.start()
        finally:
            try:
                server_socket.close()
            except Exception:
                pass


def build_http_response(status_line: str, body: str) -> bytes:
    response = (
        f"{status_line}\r\n"
        f"Content-Length: {len(body.encode())}\r\n"
        f"Content-Type: text/plain\r\n"
        f"\r\n"
        f"{body}"
    )
    return response.encode()


def write_test_configs(
    tmp_path: Path,
    client_config: dict,
    server_config: dict,
) -> tuple[Path, Path]:
    client_config_path = tmp_path / "client_network_config.json"
    server_config_path = tmp_path / "server_network_config.json"

    client_config_path.write_text(
        json.dumps(client_config),
        encoding="utf-8",
    )
    server_config_path.write_text(
        json.dumps(server_config),
        encoding="utf-8",
    )

    return client_config_path, server_config_path


def build_env(
    client_config_path: Path,
    server_config_path: Path,
    *,
    timeout: int = 2,
    stream_interval: float = 0.1,
    stream_count: int = 3,
) -> dict:
    env = os.environ.copy()
    env["CLIENT_CONFIG_PATH"] = str(client_config_path)
    env["SERVER_CONFIG_PATH"] = str(server_config_path)
    env["TEST_TIMEOUT"] = str(timeout)
    env["TEST_STREAM_INTERVAL"] = str(stream_interval)
    env["TEST_STREAM_COUNT"] = str(stream_count)
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    return env


def start_process(cmd: list[str], env: dict) -> subprocess.Popen:
    proc = subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.3)

    if proc.poll() is not None:
        stdout, stderr = proc.communicate()
        raise RuntimeError(
            f"Process failed to start: {' '.join(cmd)}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    return proc


def stop_process(proc: subprocess.Popen | None):
    if proc is None:
        return

    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)

    try:
        proc.communicate(timeout=0.2)
    except Exception:
        pass


def wait_for_tcp_port(host: str, port: int, timeout: float = 3.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def run_client(client_id: int, resource: str, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "client.py", str(client_id), resource],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


def create_patched_proxy_script(tmp_path: Path, max_proxy_workers: int) -> Path:
    # Create a temporary copy of proxy.py with MAX_PROXY_WORKERS replaced.
    # This makes PROXY_BUSY deterministic in tests.

    proxy_source = (PROJECT_ROOT / "proxy.py").read_text(encoding="utf-8")

    patched_source, count = re.subn(
        r"MAX_PROXY_WORKERS\s*=\s*\d+",
        f"MAX_PROXY_WORKERS = {max_proxy_workers}",
        proxy_source,
        count=1,
    )

    if count != 1:
        raise RuntimeError("Failed to patch MAX_PROXY_WORKERS in proxy.py")

    patched_proxy_path = tmp_path / "proxy_patched.py"
    patched_proxy_path.write_text(patched_source, encoding="utf-8")
    return patched_proxy_path


@pytest.fixture
def interop_base_system(tmp_path: Path):
    client_config = {
        "proxies": [
            {"id": 0, "port": 8000},
            {"id": 4, "port": 8001},
        ],
        "client_ids": [1, 2, 3],
    }

    server_config = {
        "proxy": {
            "port": 8000,
            "auth_token": "proxy:IamProxy",
        },
        "b_gateway": {
            "host": "127.0.0.1",
            "port": GATEWAY_PORT,
        },
        "servers": [
            {
                "content_type": "PAGE",
                "service_id": "content.page",
                "host": "127.0.0.1",
                "port": 9001,
            },
            {
                "content_type": "PAGE",
                "service_id": "content.page",
                "host": "127.0.0.1",
                "port": 9002,
            },
            {
                "content_type": "STREAM",
                "service_id": "content.stream",
                "host": "127.0.0.1",
                "port": 9003,
            },
            {
                "content_type": "STREAM",
                "service_id": "content.stream",
                "host": "127.0.0.1",
                "port": 9004,
            },
        ],
    }

    client_config_path, server_config_path = write_test_configs(
        tmp_path,
        client_config,
        server_config,
    )
    env = build_env(
        client_config_path,
        server_config_path,
        timeout=2,
        stream_interval=0.1,
        stream_count=3,
    )

    backend_procs = []
    gateway_proc = None

    try:
        for port, resource_name in BACKEND_ENDPOINTS:
            proc = start_process(
                [sys.executable, "backend_server.py", str(port), resource_name],
                env,
            )
            backend_procs.append(proc)

        for port, _ in BACKEND_ENDPOINTS:
            assert wait_for_tcp_port(HOST, port, timeout=3.0), f"backend {port} not ready"

        gateway_proc = start_process([sys.executable, "b_network_gateway.py"], env)
        assert wait_for_tcp_port(HOST, GATEWAY_PORT, timeout=3.0), "gateway not ready"

        yield {
            "env": env,
            "tmp_path": tmp_path,
            "backend_procs": backend_procs,
            "gateway_proc": gateway_proc,
        }

    finally:
        stop_process(gateway_proc)
        for proc in reversed(backend_procs):
            stop_process(proc)


def test_proxy_busy_returns_status(tmp_path: Path):

    # Force proxy capacity to 1 worker.
    # The first request occupies the only worker while backend is sleeping.
    # The second request should get PROXY_BUSY immediately.

    client_config = {
        "proxies": [
            {"id": 0, "port": 8000},
        ],
        "client_ids": [1, 2, 3],
    }

    server_config = {
        "proxy": {
            "port": 8000,
            "auth_token": "proxy:IamProxy",
        },
        "b_gateway": {
            "host": "127.0.0.1",
            "port": GATEWAY_PORT,
        },
        "servers": [
            {
                "content_type": "PAGE",
                "service_id": "content.page",
                "host": "127.0.0.1",
                "port": 9101,
            }
        ],
    }

    client_config_path, server_config_path = write_test_configs(
        tmp_path,
        client_config,
        server_config,
    )
    env = build_env(
        client_config_path,
        server_config_path,
        timeout=2,
        stream_interval=0.1,
        stream_count=3,
    )

    slow_backend = FixedResponseHTTPServer(
        port=9101,
        response_bytes=build_http_response(
            "HTTP/1.1 200 OK",
            "Welcome to the B-network content endpoint.",
        ),
        delay_before_response=1.0,
    )
    slow_backend.start()

    gateway_proc = None
    proxy_proc = None
    background_client = None

    try:
        gateway_proc = start_process([sys.executable, "b_network_gateway.py"], env)
        assert wait_for_tcp_port(HOST, GATEWAY_PORT, timeout=3.0), "gateway not ready"

        patched_proxy = create_patched_proxy_script(tmp_path, max_proxy_workers=1)
        proxy_proc = start_process([sys.executable, str(patched_proxy), "0"], env)

        # First request occupies the only proxy worker
        background_client = subprocess.Popen(
            [sys.executable, "client.py", "1", "page"],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )

        time.sleep(0.2)

        # Second request should now hit PROXY_BUSY
        result = run_client(2, "page", env)

        assert result.returncode == 0
        assert "Proxy 0 is currently busy. Status code 4." in result.stdout

    finally:
        stop_process(background_client)
        stop_process(proxy_proc)
        stop_process(gateway_proc)
        slow_backend.stop()


def test_internal_error_returns_status(tmp_path: Path):
    # Replace B-network endpoints with one fake PAGE backend that returns HTTP 500.
    # Proxy should translate that into INTERNAL_ERROR for the client.

    client_config = {
        "proxies": [
            {"id": 0, "port": 8000},
        ],
        "client_ids": [1, 2, 3],
    }

    server_config = {
        "proxy": {
            "port": 8000,
            "auth_token": "proxy:IamProxy",
        },
        "b_gateway": {
            "host": "127.0.0.1",
            "port": GATEWAY_PORT,
        },
        "servers": [
            {
                "content_type": "PAGE",
                "service_id": "content.page",
                "host": "127.0.0.1",
                "port": 9102,
            }
        ],
    }

    client_config_path, server_config_path = write_test_configs(
        tmp_path,
        client_config,
        server_config,
    )
    env = build_env(
        client_config_path,
        server_config_path,
        timeout=2,
        stream_interval=0.1,
        stream_count=3,
    )

    error_backend = FixedResponseHTTPServer(
        port=9102,
        response_bytes=build_http_response(
            "HTTP/1.1 500 Internal Server Error",
            "Backend exploded.",
        ),
        delay_before_response=0.0,
    )
    error_backend.start()

    gateway_proc = None
    proxy_proc = None

    try:
        gateway_proc = start_process([sys.executable, "b_network_gateway.py"], env)
        assert wait_for_tcp_port(HOST, GATEWAY_PORT, timeout=3.0), "gateway not ready"

        proxy_proc = start_process([sys.executable, "proxy.py", "0"], env)

        time.sleep(0.5)

        result = run_client(1, "page", env)

        assert result.returncode == 0
        assert "Proxy 0 internal error. Status code 6." in result.stdout
        assert "Backend exploded." in result.stdout
    finally:
        stop_process(proxy_proc)
        stop_process(gateway_proc)
        error_backend.stop()