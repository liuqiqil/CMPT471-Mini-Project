import subprocess
import sys
import time
from utils.network_config import ServerNetworkConfig

class ServerManager:
    def __init__(self):
        self.config = ServerNetworkConfig()
        self.processes = {}

    def start_all(self):
        for content_type, port in self.config.server_ports:
            self.start_one(port, content_type.name)

    def start_one(self, port, resource_name):
        if port in self.processes:
            print(f"Server on port {port} is already running.")
            return

        cmd = [sys.executable, "backend_server.py", str(port), resource_name]
        proc = subprocess.Popen(cmd)
        self.processes[port] = proc
        print(f"Started {resource_name} server on port {port} (PID: {proc.pid})")

    def kill_one(self, port):
        proc = self.processes.get(port)
        if proc:
            proc.terminate()
            proc.wait()
            del self.processes[port]
            print(f"Killed server on port {port}")

    def stop_all(self):
        for port in list(self.processes.keys()):
            self.kill_one(port)

if __name__ == "__main__":
    manager = ServerManager()
    try:
        manager.start_all()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop_all()