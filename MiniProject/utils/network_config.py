import json
import os
from utils.globals import Resource, CLIENT_CONFIG_PATH, SERVER_CONFIG_PATH

class ConfigBase:
    def __init__(self, filepath: str):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, filepath)
        with open(config_path, "r") as f:
            self._data = json.load(f)

class ServerNetworkConfig(ConfigBase):
    def __init__(self):
        super().__init__(SERVER_CONFIG_PATH)
        
    @property
    def proxy_auth_token(self):
        return self._data["proxy"]["auth_token"]

    @property
    def proxy_port(self):
        return self._data["proxy"]["port"]
    
    @property
    def server_ports(self):
        return [
            (Resource[srv["content_type"]], srv["port"]) 
            for srv in self._data["servers"]
        ]

    def get_server_ports(self, content_type: Resource):
        return [
            srv["port"] for srv in self._data["servers"] 
            if Resource[srv["content_type"]] == content_type
        ]

class ClientNetworkConfig(ConfigBase):
    def __init__(self):
        super().__init__(CLIENT_CONFIG_PATH)

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