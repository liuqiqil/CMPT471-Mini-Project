import sys
import uuid
from utils.network_config import ClientNetworkConfig
from utils.cpp import decode_cpp, encode_cpp, CPPStatus
from utils.client_transport_underlay import (
    MessageTimeoutError,
    cleanup_underlay,
    client_receive_cpp_packet,
    send_cpp_packet,
    setup_underlay,
)
from utils.globals import Resource


def try_one_proxy(client_id: int, proxy_id: int, resource: Resource, request_id: int) -> tuple[bool, bool]:
    """
    Returns:
        (success, try_next_proxy)
    """
    cpp_message = encode_cpp(
        source_id=client_id,
        resource=resource,
        request_id=request_id,
    )
    send_cpp_packet(dest_id=proxy_id, message=cpp_message)

    saw_partial = False

    try:
        while True:
            response = decode_cpp(client_receive_cpp_packet())

            # Ignore old / unrelated responses
            if response["request_id"] != request_id:
                continue

            # Ignore delayed response from a different proxy
            if response["source_id"] != proxy_id:
                continue

            status = response["status"]
            payload = response["payload"]

            match status:
                case CPPStatus.SUCCESS_DONE:
                    print(
                        "Request successful (Status code {}):".format(
                            CPPStatus.SUCCESS_DONE.value
                        ),
                        payload,
                    )
                    return True, False

                case CPPStatus.SUCCESS_PARTIAL:
                    saw_partial = True
                    print(
                        "Request partially successful (Status code {}):".format(
                            CPPStatus.SUCCESS_PARTIAL.value
                        ),
                        payload,
                    )

                case CPPStatus.PROXY_BUSY:
                    print(
                        "Proxy {} is currently busy. Status code {}.".format(
                            proxy_id,
                            CPPStatus.PROXY_BUSY.value,
                        )
                    )
                    return False, True

                case CPPStatus.INTERNAL_ERROR:
                    print(
                        "Proxy {} internal error. Status code {}.".format(
                            proxy_id,
                            CPPStatus.INTERNAL_ERROR.value,
                        )
                    )
                    if payload:
                        print(payload)

                    # If stream already started, do not switch proxies mid-stream
                    if saw_partial:
                        return False, False
                    return False, True

                case CPPStatus.SERVER_BUSY:
                    print(
                        "Proxy indicates backend server is busy. Status code {}.".format(
                            CPPStatus.SERVER_BUSY.value
                        )
                    )
                    return False, False

                case CPPStatus.SERVER_UNREACHABLE:
                    print(
                        "Proxy indicates backend server is unreachable. Status code {}.".format(
                            CPPStatus.SERVER_UNREACHABLE.value
                        )
                    )
                    return False, False

                case CPPStatus.INVALID_REQUEST:
                    print(
                        "Invalid request sent to proxy. Status code {}.".format(
                            CPPStatus.INVALID_REQUEST.value
                        )
                    )
                    return False, False

    except MessageTimeoutError as e:
        if saw_partial:
            print(f"Stream interrupted after partial response: {e}")
            return False, False

        print(f"Proxy {proxy_id} timed out. {e}")
        return False, True


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python client.py <client_id> <resource>")
        print("Example: python client.py 1 page")
        return

    client_id = int(sys.argv[1])

    try:
        resource = Resource[sys.argv[2].upper()]
    except KeyError:
        print("Invalid resource. Use one of: ping, page, stream")
        return

    config = ClientNetworkConfig()
    if not config.is_authorized(client_id):
        print("Unauthorized client. Use another client ID.")
        return

    proxy_ids = config.proxy_ids
    if not proxy_ids:
        print("No proxies configured.")
        return

    setup_underlay(client_id)
    request_id = uuid.uuid4().int & 0xFFFFFFFF

    try:
        for i, proxy_id in enumerate(proxy_ids):
            success, try_next = try_one_proxy(
                client_id=client_id,
                proxy_id=proxy_id,
                resource=resource,
                request_id=request_id,
            )

            if success:
                break

            if not try_next:
                break

            if i < len(proxy_ids) - 1:
                print(f"Failing over to next proxy: {proxy_ids[i + 1]}")
        else:
            print("All proxies failed.")
    finally:
        cleanup_underlay()


if __name__ == "__main__":
    main()