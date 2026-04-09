# CMPT471-Mini-Project

# Interoperation Bridge Using a Python Proxy

## Components
- client.py
- proxy.py
- backend_server.py
- server_manager.py
- utils
- tests

## How to Run
1. Change network configs if you like
2. Start server_manager
3. Start proxy
4. Run client

## Example
Terminal 1:
Terminal 1 (Server Manager):
python server_manager.py
Terminal 2 (proxy):
python proxy.py
Terminal 3 (client):
python client.py 1 stream
You can run additional clients (e.g., client2) to observe session behaviour and load distribution.
