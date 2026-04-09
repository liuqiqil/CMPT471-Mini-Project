import sys
from utils.network_config import ClientNetworkConfig
from utils.cpp import decode_cpp, encode_cpp, CPPStatus
from utils.client_transport_underlay import MessageTimeoutError, cleanup_underlay, client_receive_cpp_packet, send_cpp_packet, setup_underlay
from utils.globals import Resource

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
    
    setup_underlay(client_id)
    cpp_message = encode_cpp(source_id=client_id, resource=resource)
    send_cpp_packet(dest_id=config.proxy_id, message=cpp_message)

    while True:
        try:
            response = decode_cpp(client_receive_cpp_packet())
            if (response['source_id'] != config.proxy_id):
                print("Received response not from proxy (id = {})! Dubious Sender ID:".format(config.proxy_id), response['source_id'])
                break
            match(response['status']):
                case CPPStatus.SUCCESS_DONE:
                    print("Request successful (Status code {}):".format(CPPStatus.SUCCESS_DONE.value), response['payload'])
                    break
                case CPPStatus.SUCCESS_PARTIAL:
                    print("Request partially successful (Status code {}):".format(CPPStatus.SUCCESS_PARTIAL.value), response['payload'])
                case CPPStatus.SERVER_BUSY:
                    print("Proxy indicates backend server is busy. Status code {}.".format(CPPStatus.SERVER_BUSY.value))
                    break
                case CPPStatus.SERVER_UNREACHABLE:
                    print("Proxy indicates backend server is unreachable. Status code {}.".format(CPPStatus.SERVER_UNREACHABLE.value))
                    break
                case CPPStatus.PROXY_BUSY:
                    print("Proxy is currently busy. Status code {}.".format(CPPStatus.PROXY_BUSY.value))
                    break
                case CPPStatus.INVALID_REQUEST:
                    print("Invalid request sent to proxy. Status code {}.".format(CPPStatus.INVALID_REQUEST.value))
                    break
        except MessageTimeoutError as e:
            print(e)
            break

    cleanup_underlay()
    
if __name__ == "__main__":
    main()