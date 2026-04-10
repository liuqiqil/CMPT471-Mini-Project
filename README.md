# CMPT471-Mini-Project

# Interoperation Bridge Using a Python Proxy

## Components
- client.py
- proxy.py
- b_network_gateway.py
- backend_server.py
- utils
- tests

## How to Run
1. Configure configs if you'd like
2. Start servers
3. Start gateway
4. Start proxy
5. Run client

Run managers to read from config automatically!

## Example
Terminal 1 (Backend servers):
python server_manager.py
Terminal 2 (B network gateways):
python b_network_manager.py
Terminal 3 (proxy):
python proxy_manager.py
Terminal 4 (client):
python client.py 1 page
You can run additional clients to observe session behaviour and load distribution, if they are defined in the client network config.
