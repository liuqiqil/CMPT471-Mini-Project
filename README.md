# CMPT471-Mini-Project

# Interoperation Bridge Using a Python Proxy

## Overview
This project implements an interoperation bridge between two distinct networks:

- **A-Network**: This is where the client and the proxies live. Each member is identified by a unique client ID. Clients only talk to and accept responses from proxies. UDP and CPP, a custom protocol, are used for transport and application layers respectively.
- **B-Network**: This network houses the gateway and the backend servers. It uses a secret shared authentication token to establish the authenticity of packets, and ports to identify membership. Each backend server only serves either PAGE or STREAM, but will respond to PINGs.

The two networks are connected through an **interoperation proxy** and a **B-network gateway**. Together, they allow requests to cross the network boundary while preserving session behavior and translating between protocols.

The system supports three services:
- `PING`
- `PAGE`
- `STREAM`

It also supports:
- backend failover
- proxy failover
- sticky session behavior
- explicit error/status handling
- end-to-end latency testing

---

### Core components
- `client.py` — client in the A-Network
- `proxy.py` — interoperation proxy
- `b_network_gateway.py` — forwarding member of the B-Network
- `backend_server.py` — backend endpoints
- `utils/` — protocol, transport, configuration, and shared helpers

### Supporting scripts
- `server_manager.py` — starts backend servers from config
- `proxy_manager.py` — starts proxy instances from config
- `b_network_manager.py` — starts the B-network gateway from config



## How to Run
1. Configure configs if you'd like
2. Start servers
3. Start gateway
4. Start proxy
5. Run client

Run managers to read from config automatically!

## Example
Terminal 1 (Backend servers):<br>
python server_manager.py<br>

Terminal 2 (B network gateways):<br>
python b_network_manager.py<br>

Terminal 3 (proxy):<br>
python proxy_manager.py<br>

Terminal 4 (client):<br>
python client.py 1 page<br>

You can also run:<br>
python client.py 1 ping<br>
python client.py 1 stream<br>

You can run additional clients to observe session behaviour and load distribution, if they are defined in the client network config.

### Tests
- `tests/test_interop_latency.py -v -s`
- `tests/test_proxy_failover.py -v`
- `tests/test_proxy_status_errors.py -v`

## Test Results

### Failover Test
![Failover Test](Test%20Results/Test%20Failover.png)

### Latency Test
![Latency Test](Test%20Results/Test%20Latency.png)

### Status Error Test
![Status Error Test](Test%20Results/Test%20Status%20Errors.png)
