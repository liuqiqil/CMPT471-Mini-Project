import pytest
import subprocess
import sys
import time
from server_manager import ServerManager

TEST_TIMEOUT = 1  # seconds
TEST_STREAM_RESPONSE_INTERVAL = 0.1
TEST_RESPONSE_COUNT = 3

@pytest.fixture(autouse=True)
def setup_test_configs(monkeypatch):
    monkeypatch.setenv("CLIENT_CONFIG_PATH", "../tests/configs/client_network_config.json")
    monkeypatch.setenv("SERVER_CONFIG_PATH", "../tests/configs/server_network_config.json")
    monkeypatch.setenv("TEST_TIMEOUT", str(TEST_TIMEOUT))
    monkeypatch.setenv("TEST_STREAM_INTERVAL", str(TEST_STREAM_RESPONSE_INTERVAL))
    monkeypatch.setenv("TEST_STREAM_COUNT", str(TEST_RESPONSE_COUNT))

@pytest.fixture
def system_env():
    manager = ServerManager()
    manager.start_all()
    
    proxy_proc = subprocess.Popen([sys.executable, "proxy.py"])
    
    time.sleep(0.2)
    
    yield manager
    
    manager.stop_all()
    proxy_proc.terminate()
    proxy_proc.wait()

def run_client(client_id, resource):
    return subprocess.run(
        [sys.executable, "client.py", str(client_id), resource],
        capture_output=True,
        text=True
    )

def test_ping_normal(system_env):
    """
    Test normal PING request flow.
    Servers are all up and not busy, so we should get a successful response.
    """
    result = run_client(1, "ping")
    assert "Request successful (Status code 0)" in result.stdout
    assert "Total online: 100% (2/2)" in result.stdout
    assert "Total: 100% (4/4) available" in result.stdout
    
def test_page_normal(system_env):
    """
    Test normal PAGE request flow.
    Servers are all up and not busy, so we should get a successful response.
    """
    result = run_client(1, "page")
    assert "Request successful (Status code 0)" in result.stdout
    assert "Welcome to the backend service." in result.stdout
    
def test_stream_normal(system_env):
    """
    Test normal STREAM request flow.
    Servers are all up and not busy, so we should get successful responses for all stream chunks.
    """
    result = run_client(1, "stream")
    assert result.returncode == 0
    assert result.stdout.count("Request partially successful (Status code 1)") == TEST_RESPONSE_COUNT
    assert result.stdout.count("Streaming content...") == TEST_RESPONSE_COUNT
    assert "Request successful (Status code 0)" in result.stdout