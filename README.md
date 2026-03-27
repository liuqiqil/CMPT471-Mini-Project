# CMPT471-Mini-Project

# Interoperation Bridge Using a Python Proxy

## Components
- client.py
- proxy.py
- backend_server.py

## How to Run
1. Start backend servers
2. Start proxy
3. Run client

## Example
Terminal 1:
Terminal 1 (Backend Server 1):
python backend_server.py 8001 Server1
Terminal 2 (Backend Server 2):
python backend_server.py 8002 Server2
Terminal 3 (proxy):
python proxy.py
Terminal 4 (client):
python client.py client1 /test
You can run additional clients (e.g., client2) to observe session behaviour and load distribution.
