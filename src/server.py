import dataclasses
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import random
from typing import Optional, List, Self, Tuple, Callable, Dict
import logging
import argparse
import requests

import raft_sever

class NodeHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        msg = self.rfile.read(content_length)
        path = self.path.lstrip("/")  # Get the endpoint name
        node = self.server.node

        # Pass message to the appropriate handler
        response = node.handle_message(path, msg)
        logging.info(f"Node {node.node_id} received at {path}: {msg}")

        self.send_response(204 if response is None else 200)
        self.end_headers()
        if response:
            self.wfile.write(response)


@dataclasses.dataclass(kw_only=True)
class Node:
    """Represents a Raft node."""
    node_id: str
    port: int
    peers: List[int]
    running: bool = False
    message_success_rate: float = 1.0

    _instance: Optional[Self] = None
    _on_instance_creation: List[Callable[[Self], None]] = dataclasses.field(default_factory=list)
    _endpoints: Dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        # calls all remaining callbacks and gets node ready
        for callback in self._on_instance_creation:
            logging.info("running callback for new endpoint!")
            callback(self)
        self._on_instance_creation.clear()

    @staticmethod
    def get_instance(node_id: str, port: int, peers: Optional[List[int]] = None) -> Self:
        # singleton pattern: make sure node is created only once
        if Node._instance is None:
            # don't have exisiting node: create a new node
            Node._instance = Node(node_id=node_id, port=port, peers=peers or [])
        return Node._instance

    def start(self):
        if self.running:
            logging.info(f"Node {self.node_id} is already running.")
            return

        class HTTPServerWithNode(HTTPServer):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.node = self

        self.running = True
        server = HTTPServerWithNode(("localhost", self.port), NodeHTTPRequestHandler)
        logging.info(f"HTTP server for node {self.node_id} running on port {self.port}")

        def serve_forever():
            while self.running:
                server.handle_request()

        threading.Thread(target=serve_forever, daemon=True).start()

    def stop(self):
        logging.info(f"Stopping node {self.node_id}.")
        self.running = False

    def handle_message(self, endpoint: str, msg: bytes) -> Tuple[Optional[int], Optional[bytes]]:
        # send message to correct function based on input
        logging.info(f"Received message from endpoint {endpoint}: {msg}")
        logging.info("")
        return 200, b'Hello world!'

    def send_message(self, target: int, endpoint: str, msg: str | bytes) -> Optional[bytes]:
        if not self.running:
            return None

        # Simulation of dropping random messages!
        # FIXME?: move it to the decorator?
        if random.random() < self.message_success_rate:
            url = f"http://localhost:{target}/{endpoint}"
            logging.debug(f"Sending message to {url} with contents: {msg}")
            try:
                response = requests.post(url, data=msg)
                if response.status_code == 200:
                    logging.info(f"Received success!: {response.content}")
                    return response.content
                else:
                    logging.error(f"Failed with status code: {response.status_code}")
                    return None
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to send message to {target}: {e}")
                return None
        else:
            logging.info(f"Dropping message intended for {target} with contents: {msg}")
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Start a Raft Node")
    parser.add_argument("--node-id", type=str, required=True, help="Node ID", dest="node_id")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--peers", nargs="+", type=int, help="List of peer ports")

    args = parser.parse_args()

    # Create a singleton node instance
    node = Node.get_instance(node_id=args.node_id, port=args.port, peers=args.peers)

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
                    out = raft_sever.ElectServer(target)
                    print(f"Elected successfully! output: {out}")
                case _:
                    print("Unknown command. Use 'send <target_port> to <endpoint> <message>' or 'stop'.")
    except KeyboardInterrupt:
        node.stop()
