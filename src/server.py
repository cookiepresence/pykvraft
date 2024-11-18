import logging
import argparse

import raft_sever
import node


node = node.Node.instance(node_id='node1')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Start a Raft Node")
    parser.add_argument("--node-id", type=str, required=True, help="Node ID", dest="node_id")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--peers", nargs="+", type=int, help="List of peer ports")

    args = parser.parse_args()

    # set all required params
    assert node.node_id == 'node1'
    node.node_id = args.node_id
    node.port = args.port
    node.peers = args.peers

    # Start the node
    node.start()

    try:
        while True:
            print(f"Node {node.node_id}> ", end="")
            command = input().strip()
            match command.split():
                case ["send", target, "to", endpoint, *message]:
                    try:
                        target_port = int(target)
                        payload = " ".join(message).encode()
                        response = node.send_message(target_port, endpoint, payload)
                        if response:
                            print(f"Response from {target}:{endpoint}: {response.decode()}")
                        else:
                            print(f"Failed to send message to {target}:{endpoint}.")
                    except ValueError:
                        print(f"Invalid target port: {target}")
                case ["stop"]:
                    node.stop()
                    break
                case ["elect", target]:
                    out = raft_sever.ElectServer(int(target))
                    print(f"Elected successfully! output: {out}")
                case _:
                    print("Unknown command. Use 'send <target_port> to <endpoint> <message>' or 'stop'.")
    except KeyboardInterrupt:
        node.stop()
