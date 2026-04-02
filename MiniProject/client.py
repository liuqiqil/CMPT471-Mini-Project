import socket
import sys
import time

from utils.network_config import ClientNetworkConfig
from utils.cpp import decode_cpp, encode_cpp, CPPStatus
from utils.client_transport_underlay import send_cpp_packet
from utils.globals import Resource, HOST, STREAM_RESPONSE_COUNT

BUFFER_SIZE = 4096
TIMEOUT = 5


def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python client.py <client_id> <resource> <port>")
        print("Example: python client.py 1 page 8080")
        return

    try:
        client_id = int(sys.argv[1])
        resource = Resource[sys.argv[2].upper()]
        port = int(sys.argv[3])
    except ValueError:
        print("Client ID and port must be integers.")
        return
    except KeyError:
        print("Invalid resource. Use ping, page, or stream.")
        return

    config = ClientNetworkConfig()
    if not config.is_authorized(client_id):
        print("Unauthorized client. Use another client ID.")
        return

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        client_socket.bind((HOST, port))
        client_socket.settimeout(TIMEOUT)

        cpp_message = encode_cpp(source_id=client_id, resource=resource)

        start_time = time.time()
        send_cpp_packet(dest_id=config.proxy_id, message=cpp_message, socket=client_socket)

        # For STREAM resource, expect multiple responses. Only 1 for the rest.
        response_count = STREAM_RESPONSE_COUNT if resource == Resource.STREAM else 1
        prev_response_time = None

        for i in range(response_count):
            try:
                data, _ = client_socket.recvfrom(BUFFER_SIZE)
                response = decode_cpp(data)
                now = time.time()

                if response["source_id"] != config.proxy_id:
                    print(
                        "Received response not from proxy (id = {})! Dubious Sender ID:".format(config.proxy_id),
                        response["source_id"]
                    )
                    return

                # Measure first-response latency
                if i == 0:
                    print(f"Latency: {(now - start_time) * 1000:.2f} ms")
                # Measure streaming interval
                elif resource == Resource.STREAM and prev_response_time is not None:
                    print(f"Interval: {(now - prev_response_time):.2f} s")

                match response["status"]:
                    case CPPStatus.SUCCESS:
                        print("Request successful:", response["payload"])
                    case CPPStatus.SERVER_BUSY:
                        print(
                            "Proxy indicates backend server is busy. Status code {}.".format(
                                CPPStatus.SERVER_BUSY.value
                            )
                        )
                        return
                    case CPPStatus.SERVER_UNREACHABLE:
                        print(
                            "Proxy indicates backend server is unreachable. Status code {}.".format(
                                CPPStatus.SERVER_UNREACHABLE.value
                            )
                        )
                        return
                    case CPPStatus.PROXY_BUSY:
                        print(
                            "Proxy is currently busy. Status code {}.".format(
                                CPPStatus.PROXY_BUSY.value
                            )
                        )
                        return
                    case CPPStatus.INVALID_REQUEST:
                        print(
                            "Invalid request sent to proxy. Status code {}.".format(
                                CPPStatus.INVALID_REQUEST.value
                            )
                        )
                        return
                    case _:
                        print(f"Unknown status: {response['status']}")
                        return

                prev_response_time = now

            except socket.timeout:
                print(
                    "Timed out: No response received within timeout period of {} seconds.".format(
                        TIMEOUT
                    )
                )
                return

    finally:
        client_socket.close()


if __name__ == "__main__":
    main()