import dataclasses
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import random
from typing import Optional, List
import logging
import argparse

import requests


class NodeHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        """Handle POST requests to process incoming messages."""
        content_length = int(self.headers['Content-Length'])
        msg = self.rfile.read(content_length)
        handle_message(self.server.node_state, msg)
        logging.info(f"Node {self.server.node_state.node_id} received: {msg}")
        self.send_response(200)
        self.end_headers()


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
    class HTTPServerWithState(HTTPServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.node_state = node

    node.running = True
    server = HTTPServerWithState(("localhost", node.port), NodeHTTPRequestHandler)
    logging.info(f"http server for node {node.node_id} running on port {node.port}")

    def serve_forever():
        while node.running:
            server.handle_request()

    threading.Thread(target=serve_forever, daemon=True).start()
    return node


def handle_message(node: NodeStatus, msg: bytes | str):  # TODO: Find Address type
    # TODO: Find a way to convert the recived address to something more useful
    logging.info(f"Message received: {msg}")
    # Also make sure that we do something with the recieved message


def send_message(node: NodeStatus, target: int, msg: str | bytes):
    if random.random() < node.message_success_rate:
        logging.debug(f"sending message to {target} with contents: {msg}")
        url = f"http://localhost:{target}"
        try:
            response = requests.post(url, msg)
            if response.status_code == 200:
                logging.info("recieved success!")
            else:
                logging.info("oh no!")
        except requests.exceptions.RequestException as e:
            logging.error(f"unable to send message to {target}!", e)
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
    node = start_node(node)

    try:
        while True:
            print(f"Node {node.node_id}> ", end="")
            command = input().strip()
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
