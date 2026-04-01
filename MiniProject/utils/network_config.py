import json
import os

class ConfigBase:
    def __init__(self, filename):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, f"../configs/{filename}")
        with open(config_path, "r") as f:
            self._data = json.load(f)

class ServerNetworkConfig(ConfigBase):
    def __init__(self):
        super().__init__("server_network_config.json")
        
    @property
    def proxy_auth_token(self):
        return self._data["proxy"]["auth_token"]
    
    def server_auth_token(self, port):
        for srv in self._data["servers"]:
            if srv["port"] == port:
                return srv["auth_token"]
        return None

    @property
    def proxy_port(self):
        return self._data["proxy"]["port"]
    
    @property
    def server_ports(self):
        return [
                (srv["content_type"], srv["port"]) 
                for srv in self._data["servers"]
                ]

    def get_server_ports(self, content_type):
        return [
            srv["port"] for srv in self._data["servers"] 
            if srv["content_type"] == content_type
                ]

class ClientNetworkConfig(ConfigBase):
    def __init__(self):
        super().__init__("client_network_config.json")

    @property
    def proxy_id(self):
        return self._data["proxy"]["id"]
    
    @property
    def proxy_port(self):
        return self._data["proxy"]["port"]

    @property
    def client_ids(self):
        return self._data["client_ids"]
    
    def is_authorized(self, client_id):
        return client_id in self._data["client_ids"]