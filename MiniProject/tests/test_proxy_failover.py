import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


TEST_TIMEOUT = 1
TEST_STREAM_RESPONSE_INTERVAL = 0.1
TEST_RESPONSE_COUNT = 3

BACKEND_ENDPOINTS = [
    (9001, "PAGE"),
    (9002, "PAGE"),
    (9003, "STREAM"),
    (9004, "STREAM"),
]

GATEWAY_PORT = 8500


def write_test_configs(tmp_path: Path):
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

    client_config_path = tmp_path / "client_network_config.json"
    server_config_path = tmp_path / "server_network_config.json"

    client_config_path.write_text(json.dumps(client_config), encoding="utf-8")
    server_config_path.write_text(json.dumps(server_config), encoding="utf-8")

    return client_config_path, server_config_path


def build_env(client_config_path: Path, server_config_path: Path) -> dict:
    env = os.environ.copy()
    env["CLIENT_CONFIG_PATH"] = str(client_config_path)
    env["SERVER_CONFIG_PATH"] = str(server_config_path)
    env["TEST_TIMEOUT"] = str(TEST_TIMEOUT)
    env["TEST_STREAM_INTERVAL"] = str(TEST_STREAM_RESPONSE_INTERVAL)
    env["TEST_STREAM_COUNT"] = str(TEST_RESPONSE_COUNT)
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


def collect_process_logs(name: str, proc: subprocess.Popen | None) -> str:
    if proc is None:
        return f"[{name}] process is None\n"

    try:
        stdout, stderr = proc.communicate(timeout=0.2)
    except Exception:
        stdout, stderr = "", ""

    return (
        f"[{name}] returncode={proc.poll()}\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}\n"
    )


def run_client(client_id: int, resource: str, env: dict):
    return subprocess.run(
        [sys.executable, "client.py", str(client_id), resource],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def interop_system(tmp_path: Path):
    client_config_path, server_config_path = write_test_configs(tmp_path)
    env = build_env(client_config_path, server_config_path)

    backend_procs: list[subprocess.Popen] = []
    gateway_proc = None
    proxy0_proc = None
    proxy4_proc = None

    try:
        # Start B-endpoints
        for port, resource_name in BACKEND_ENDPOINTS:
            proc = start_process(
                [sys.executable, "backend_server.py", str(port), resource_name],
                env,
            )
            backend_procs.append(proc)

        # Wait for backend ports
        for port, _ in BACKEND_ENDPOINTS:
            assert wait_for_tcp_port("127.0.0.1", port, timeout=3.0), (
                f"Backend endpoint on port {port} did not become ready.\n"
                + "\n".join(
                    collect_process_logs(f"backend:{p}", proc)
                    for p, proc in zip([x[0] for x in BACKEND_ENDPOINTS], backend_procs)
                )
            )

        # Start B-network forwarding member
        gateway_proc = start_process([sys.executable, "b_network_gateway.py"], env)
        assert wait_for_tcp_port("127.0.0.1", GATEWAY_PORT, timeout=3.0), (
            "B-network gateway did not become ready.\n"
            + collect_process_logs("gateway", gateway_proc)
        )

        # Start proxies
        proxy0_proc = start_process([sys.executable, "proxy.py", "0"], env)
        proxy4_proc = start_process([sys.executable, "proxy.py", "4"], env)

        # Let UDP proxies stabilize
        time.sleep(0.8)

        # Smoke test
        smoke_result = run_client(1, "page", env)
        if (
            smoke_result.returncode != 0
            or "Request successful (Status code 0)" not in smoke_result.stdout
        ):
            debug_msg = [
                "Smoke test failed before proxy failover testing.",
                f"client stdout:\n{smoke_result.stdout}",
                f"client stderr:\n{smoke_result.stderr}",
                collect_process_logs("gateway", gateway_proc),
                collect_process_logs("proxy0", proxy0_proc),
                collect_process_logs("proxy4", proxy4_proc),
            ]
            for port, proc in zip([x[0] for x in BACKEND_ENDPOINTS], backend_procs):
                debug_msg.append(collect_process_logs(f"backend:{port}", proc))
            raise RuntimeError("\n".join(debug_msg))

        yield {
            "env": env,
            "backend_procs": backend_procs,
            "gateway_proc": gateway_proc,
            "proxy0_proc": proxy0_proc,
            "proxy4_proc": proxy4_proc,
        }

    finally:
        stop_process(proxy4_proc)
        stop_process(proxy0_proc)
        stop_process(gateway_proc)

        for proc in reversed(backend_procs):
            stop_process(proc)


def test_page_fails_over_when_primary_proxy_down(interop_system):
    # If the primary proxy goes down, the client should automatically switch to the backup proxy
    # and the PAGE request should still succeed.
    env = interop_system["env"]

    stop_process(interop_system["proxy0_proc"])
    time.sleep(0.1)

    result = run_client(1, "page", env)

    assert result.returncode == 0
    assert "Proxy 0 timed out." in result.stdout
    assert "Failing over to next proxy: 4" in result.stdout
    assert "Request successful (Status code 0)" in result.stdout


def test_stream_fails_over_when_primary_proxy_down(interop_system):
    # If the primary proxy goes down before the request begins, the client should switch to the backup proxy
    # and the STREAM request should be returned in full.
    env = interop_system["env"]

    stop_process(interop_system["proxy0_proc"])
    time.sleep(0.1)

    result = run_client(1, "stream", env)

    assert result.returncode == 0
    assert "Proxy 0 timed out." in result.stdout
    assert "Failing over to next proxy: 4" in result.stdout
    assert result.stdout.count("Request partially successful (Status code 1)") == TEST_RESPONSE_COUNT
    assert result.stdout.count("B-network streaming content...") == TEST_RESPONSE_COUNT
    assert "Request successful (Status code 0)" in result.stdout


def test_all_proxies_down_reports_failure(interop_system):
    # If all proxies go down, the client should report that all proxies have failed.
    env = interop_system["env"]

    stop_process(interop_system["proxy0_proc"])
    stop_process(interop_system["proxy4_proc"])
    time.sleep(0.1)

    result = run_client(1, "page", env)

    assert result.returncode == 0
    assert "Proxy 0 timed out." in result.stdout
    assert "Failing over to next proxy: 4" in result.stdout
    assert "Proxy 4 timed out." in result.stdout
    assert "All proxies failed." in result.stdout