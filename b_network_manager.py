import subprocess
import sys
import time


class BNetworkManager:
    def __init__(self):
        self.process = None

    def start(self):
        if self.process is not None:
            print("B-network gateway is already running.")
            return

        cmd = [sys.executable, "b_network_gateway.py"]
        self.process = subprocess.Popen(cmd)
        print(f"Started B-network gateway (PID: {self.process.pid})")

    def stop(self):
        if self.process is None:
            return

        self.process.terminate()
        self.process.wait()
        print("Stopped B-network gateway.")
        self.process = None


if __name__ == "__main__":
    manager = BNetworkManager()
    try:
        manager.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()