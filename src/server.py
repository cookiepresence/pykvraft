import dataclasses
import socket
import threading
import random
from typing import Optional, List
import logging
import argparse


@dataclasses.dataclass(kw_only=True)
class NodeStatus:
    # Used in communication
    node_id: Optional[str] = None
    # Stores own port
    port: Optional[int] = None
    # is the service running?
    running: bool = False
    # how often do we drop the message
    message_success_rate: float = 1.0

    # Stores port for other peers (that may or may not be alive)
    # Ideally, we would want to get this from a dynamic discovery setup
    # however, we do not have the time :)
    peers: List[int]


def start_node(node: NodeStatus) -> NodeStatus:
    node.running = True
    logging.info(f"Starting node at port {node.port}")

    # Starts listening for messages on a separate thread
    threading.Thread(target=lambda : listen_for_messages(node), daemon=True).start()
    return node


# TODO: ideally, it would be nice to have a more http-like setup.
#       this is far too much work for too little benefit
def listen_for_messages(node: NodeStatus) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind(("localhost", node.port))
        print(f"Node {node.node_id} listening on port {node.port}")
        while node.running:
            try:
                message, address = server_socket.recvfrom(1024)
                handle_message(node, message, address)
            except socket.error as e:
                logging.error(e)
                continue


def handle_message(node: NodeStatus, msg: bytes | str, address):  # TODO: Find Address type
    # TODO: Find a way to convert the recived address to something more useful
    logging.info(f"Message received from server {address}: {msg}")
    # Also make sure that we do something with the recieved message


def send_message(node: NodeStatus, target: int, msg: str | bytes):
    if random.random() < node.message_success_rate:
        logging.debug(f"sending message to {target} with contents: {msg}")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
            client.sendto(msg.encode() if type(msg) is str else msg, ("localhost", target_port))
    else:
        logging.info(f"droppping message intended for {target} with contents: {msg}")


def stop_node(node: NodeStatus):
    logging.info("stopping node!")
    node.running = False
    return node


if __name__ == "__main__":
    # Make sure we are logging verbosely by default
    # TODO: Make this configurable
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Start a Raft Node")
    parser.add_argument("--node-id", type=str, required=True, help="Node ID", dest='node_id')
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--peers", nargs='+', type=int, help="List of peer ports")

    args = parser.parse_args()

    node = NodeStatus(
        node_id=args.node_id,
        port=args.port,
        running=False,
        message_success_rate=1.0,
        peers=args.peers or []
    )
    start_node(node)

    try:
        while True:
            command = input(f"Node {node.node_id}> ").strip()
            match command.split():
                case ["send", target, *message]:
                    try:
                        target_port = int(target)
                        send_message(node, target_port, "Hello world!")
                    except ValueError:
                        print(f"Invalid target port: {target}")
                case ["stop"]:
                    stop_node(node)
                    break
                case _:
                    print("Unknown command. Use 'send <target_port> <message>' or 'stop'.")
    except KeyboardInterrupt:
        stop_node(node)
