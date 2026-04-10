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
            
def test_ping_normal(interop_system):
    """
    Test normal PING request flow across the full interop stack.
    All B-endpoints are up, gateway is up, both proxies are up.
    """
    env = interop_system["env"]
    result = run_client(1, "ping", env)
    assert result.returncode == 0
    assert "Request successful (Status code 0)" in result.stdout
    assert "content.page" in result.stdout
    assert "content.stream" in result.stdout


def test_page_normal(interop_system):
    """
    Test normal PAGE request flow across the full interop stack.
    """
    env = interop_system["env"]
    result = run_client(1, "page", env)
    assert result.returncode == 0
    assert "Request successful (Status code 0)" in result.stdout
    assert "Welcome to the B-network content endpoint." in result.stdout


def test_stream_normal(interop_system):
    """
    Test normal STREAM request flow across the full interop stack.
    """
    env = interop_system["env"]
    result = run_client(1, "stream", env)
    assert result.returncode == 0
    assert result.stdout.count("Request partially successful (Status code 1)") == TEST_RESPONSE_COUNT
    assert result.stdout.count("B-network streaming content...") == TEST_RESPONSE_COUNT
    assert "Request successful (Status code 0)" in result.stdout


def test_gateway_failover_on_backend_down(interop_system):
    """
    Test that the gateway fails over to the second backend when the first is killed.
    The PAGE request should still succeed even with one backend down.
    """
    env = interop_system["env"]

    # Kill first PAGE backend
    stop_process(interop_system["backend_procs"][0])
    time.sleep(0.1)

    result = run_client(1, "page", env)
    assert result.returncode == 0
    assert "Request successful (Status code 0)" in result.stdout
    assert "Welcome to the B-network content endpoint." in result.stdout


def test_server_unreachable_when_all_backends_down(interop_system):
    """
    Test that when all backends for a resource are down, the gateway returns 504
    which the proxy translates to INTERNAL_ERROR with an unreachable message.
    """
    env = interop_system["env"]

    stop_process(interop_system["backend_procs"][0])
    stop_process(interop_system["backend_procs"][1])
    time.sleep(0.1)

    result = run_client(1, "page", env)
    assert result.returncode == 0
    assert "Proxy 0 internal error. Status code 6." in result.stdout
    assert "All B-network endpoints are unreachable." in result.stdout



def test_session_stickiness_same_backend(interop_system):
    """
    Test that repeated requests from the same client are routed to the same backend.
    Kill the first backend after establishing a session, verify the gateway
    remaps the session to the surviving backend rather than failing entirely.
    """
    env = interop_system["env"]

    result1 = run_client(1, "page", env)
    assert result1.returncode == 0
    assert "Request successful (Status code 0)" in result1.stdout

    stop_process(interop_system["backend_procs"][0])
    time.sleep(0.1)

    result2 = run_client(1, "page", env)
    assert result2.returncode == 0
    assert "Request successful (Status code 0)" in result2.stdout


def test_compound_session_isolation_between_clients(interop_system):
    """
    Test that two different clients maintain independent compound sessions.
    Both should succeed concurrently without interfering with each other.
    """
    import threading

    env = interop_system["env"]
    results = {}

    def run(client_id):
        results[client_id] = run_client(client_id, "page", env)

    threads = [threading.Thread(target=run, args=(cid,)) for cid in [1, 2, 3]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for client_id, result in results.items():
        assert result.returncode == 0, f"Client {client_id} failed"
        assert "Request successful (Status code 0)" in result.stdout, f"Client {client_id} got wrong response"


def test_stream_interrupted_mid_stream(interop_system):
    """
    Test that the client handles a stream backend dying mid-stream gracefully,
    reporting partial success without hanging or crashing.
    """
    import threading

    env = interop_system["env"]
    result_holder = {}

    def run_stream():
        result_holder["result"] = run_client(1, "stream", env)

    t = threading.Thread(target=run_stream)
    t.start()

    time.sleep(0.3)
    stop_process(interop_system["backend_procs"][2])
    stop_process(interop_system["backend_procs"][3])

    t.join(timeout=10)

    result = result_holder.get("result")
    assert result is not None
    assert result.returncode == 0
    assert "Request partially successful (Status code 1)" in result.stdout


def test_round_robin_distributes_across_backends(interop_system):
    """
    Test that the gateway distributes PAGE requests across both backends
    rather than always hitting the same one. Kill one backend and verify
    requests still succeed, then kill the other and verify failure,
    confirming both were in rotation.
    """
    env = interop_system["env"]

    stop_process(interop_system["backend_procs"][0])
    time.sleep(0.1)

    result = run_client(1, "page", env)
    assert result.returncode == 0
    assert "Request successful (Status code 0)" in result.stdout

    stop_process(interop_system["backend_procs"][1])
    time.sleep(0.1)

    result2 = run_client(2, "page", env)
    assert result2.returncode == 0
    assert "Proxy 0 internal error. Status code 6." in result2.stdout
    assert "All B-network endpoints are unreachable." in result2.stdout


def test_ping_fails_over_when_primary_proxy_down(interop_system):
    """
    Test that PING also fails over to the backup proxy when the primary is down.
    """
    env = interop_system["env"]

    stop_process(interop_system["proxy0_proc"])
    time.sleep(0.1)

    result = run_client(1, "ping", env)
    assert result.returncode == 0
    assert "Proxy 0 timed out." in result.stdout
    assert "Failing over to next proxy: 4" in result.stdout
    assert "Request successful (Status code 0)" in result.stdout

def test_ping_with_one_backend_down(interop_system):
    """
    Test PING response when one PAGE backend is down.
    The ping should still succeed and report partial availability for content.page.
    """
    env = interop_system["env"]

    stop_process(interop_system["backend_procs"][0])
    time.sleep(0.1)

    result = run_client(1, "ping", env)
    assert result.returncode == 0
    assert "Request successful (Status code 0)" in result.stdout
    # One of two PAGE backends is down — should show partial availability
    assert "content.page" in result.stdout
    assert "50%" in result.stdout


def test_ping_with_all_backends_of_one_type_down(interop_system):
    """
    Test PING response when all PAGE backends are down but STREAM backends are up.
    The ping should still succeed and report 0% for content.page, 100% for content.stream.
    """
    env = interop_system["env"]

    stop_process(interop_system["backend_procs"][0])
    stop_process(interop_system["backend_procs"][1])
    time.sleep(0.1)

    result = run_client(1, "ping", env)
    assert result.returncode == 0
    assert "Request successful (Status code 0)" in result.stdout
    assert "content.page" in result.stdout
    assert "content.stream" in result.stdout
    # PAGE should show 0% available, STREAM should show 100%
    assert "content.page: 0%" in result.stdout
    assert "content.stream: 100%" in result.stdout


def test_ping_with_all_backends_down(interop_system):
    """
    Test PING response when every backend is down.
    Ping itself should still succeed (gateway is up) but report 0% availability.
    """
    env = interop_system["env"]

    for proc in interop_system["backend_procs"]:
        stop_process(proc)
    time.sleep(0.1)

    result = run_client(1, "ping", env)
    assert result.returncode == 0
    assert "Request successful (Status code 0)" in result.stdout
    assert "0%" in result.stdout

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