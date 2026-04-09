import subprocess
import sys
import time

from utils.network_config import ClientNetworkConfig


class ProxyManager:
    def __init__(self):
        self.config = ClientNetworkConfig()
        self.processes = {}

    def start_all(self):
        for proxy_id in self.config.proxy_ids:
            self.start_one(proxy_id)

    def start_one(self, proxy_id: int):
        if proxy_id in self.processes:
            print(f"Proxy {proxy_id} is already running.")
            return

        cmd = [sys.executable, "proxy.py", str(proxy_id)]
        proc = subprocess.Popen(cmd)
        self.processes[proxy_id] = proc
        print(f"Started proxy {proxy_id} (PID: {proc.pid})")

    def kill_one(self, proxy_id: int):
        proc = self.processes.get(proxy_id)
        if proc is None:
            return

        proc.terminate()
        proc.wait()
        del self.processes[proxy_id]
        print(f"Killed proxy {proxy_id}")

    def stop_all(self):
        for proxy_id in list(self.processes.keys()):
            self.kill_one(proxy_id)


if __name__ == "__main__":
    manager = ProxyManager()
    try:
        manager.start_all()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop_all()