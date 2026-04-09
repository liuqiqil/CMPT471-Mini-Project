import json
import os

from utils.globals import CLIENT_CONFIG_PATH, HOST, Resource, SERVER_CONFIG_PATH


class ConfigBase:
    def __init__(self, filepath: str):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, filepath)
        with open(config_path, "r", encoding="utf-8") as f:
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
    def b_gateway_host(self):
        return self._data.get("b_gateway", {}).get("host", HOST)

    @property
    def b_gateway_port(self):
        return self._data.get("b_gateway", {}).get("port", 8500)

    @property
    def b_gateway_endpoint(self):
        return self.b_gateway_host, self.b_gateway_port

    @staticmethod
    def default_service_id(resource: Resource) -> str:
        if resource == Resource.PAGE:
            return "content.page"
        if resource == Resource.STREAM:
            return "content.stream"
        if resource == Resource.PING:
            return "content.ping"
        raise ValueError(f"Unsupported resource: {resource}")

    @property
    def server_records(self):
        records = []
        for srv in self._data["servers"]:
            resource = Resource[srv["content_type"]]
            records.append(
                {
                    "resource": resource,
                    "service_id": srv.get(
                        "service_id",
                        self.default_service_id(resource),
                    ),
                    "host": srv.get("host", HOST),
                    "port": srv["port"],
                }
            )
        return records

    @property
    def server_ports(self):
        return [(record["resource"], record["port"]) for record in self.server_records]

    def get_server_ports(self, content_type: Resource):
        return [
            record["port"]
            for record in self.server_records
            if record["resource"] == content_type
        ]

    def get_server_records(self, content_type: Resource):
        return [
            record
            for record in self.server_records
            if record["resource"] == content_type
        ]

    def get_service_backends(self, service_id: str):
        return [
            record
            for record in self.server_records
            if record["service_id"] == service_id
        ]

    def get_service_id(self, content_type: Resource) -> str:
        records = self.get_server_records(content_type)
        if records:
            return records[0]["service_id"]
        return self.default_service_id(content_type)


class ClientNetworkConfig(ConfigBase):
    def __init__(self):
        super().__init__(CLIENT_CONFIG_PATH)

    @property
    def proxies(self):
        if "proxies" in self._data:
            return self._data["proxies"]

        if "proxy" in self._data:
            return [self._data["proxy"]]

        return []

    @property
    def proxy_ids(self):
        return [proxy["id"] for proxy in self.proxies]

    @property
    def proxy_ports(self):
        return {proxy["id"]: proxy["port"] for proxy in self.proxies}

    def get_proxy_port(self, proxy_id):
        return self.proxy_ports[proxy_id]

    @property
    def proxy_id(self):
        return self.proxy_ids[0]

    @property
    def proxy_port(self):
        return self.get_proxy_port(self.proxy_id)

    @property
    def client_ids(self):
        return self._data["client_ids"]

    def is_authorized(self, client_id):
        return client_id in self._data["client_ids"]