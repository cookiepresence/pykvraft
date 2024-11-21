import logging

logging.basicConfig(level=logging.INFO)

import argparse
import raft_server
import node
import rpc  # MAKE SURE rpc is imported after raft_server is instantiated

node_instance = node.Node.instance()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a Raft Node")
    parser.add_argument(
        "--node-id", type=str, required=True, help="Node ID", dest="node_id"
    )
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--peers", nargs="+", type=int, help="List of peer ports")

    args = parser.parse_args()

    node_instance.node_id = args.node_id
    node_instance.port = args.port
    node_instance.peers = args.peers

    raft = raft_server.RaftServer(node_id=args.node_id, peers=args.peers)

    node_instance.raft_server = raft

    node_instance.start()

    try:
        while True:
            print(f"Node {node_instance.node_id}> ", end="")
            command = input().strip()
            match command.split():
                case ["send", target, "to", endpoint, *message]:
                    try:
                        target_port = int(target)
                        payload = " ".join(message).encode()
                        response = node_instance.send_message(
                            target_port, endpoint, payload
                        )
                        if response:
                            print(
                                f"Response from {target}:{endpoint}: {response.decode()}"
                            )
                        else:
                            print(f"Failed to send message to {target}:{endpoint}.")
                    except ValueError:
                        print(f"Invalid target port: {target}")
                case ["start", "raft"]:
                    raft.running = True
                case ["stop"]:
                    node_instance.stop()
                    break
                case ["hello", target, name, times]:
                    for _ in range(int(times)):
                        rpc.Hello(int(target), name)
                case ["set", key, value]:
                    if raft.client_set(key, value):
                        print(f"SET {key} = {value} succeeded.")
                    else:
                        print("Failed to set key. Not the leader.")
                case ["get", key]:
                    value = raft.client_get(key)
                    if value is not None:
                        print(f"{key} = {value}")
                    else:
                        print(f"{key} not found.")
                case ["make-leader"]:
                    raft.force_leader()
                    print("This node has been set as Leader.")
                case _:
                    print(
                        "Unknown command. Use 'send <target_port> to <endpoint> <message>', 'stop', 'set <key> <value>', 'get <key>', or 'make-leader'."
                    )
    except KeyboardInterrupt:
        node_instance.stop()
