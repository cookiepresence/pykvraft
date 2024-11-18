import logging
import dataclasses
import threading
import random
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler

from typing import Optional, List, Dict, Tuple


class NodeHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        msg = self.rfile.read(content_length)
        path = self.path.lstrip("/")  # Get the endpoint name
        node = Node.instance()

        # Pass message to the appropriate handler
        response = node.handle_message(path, msg)
        logging.info(f"Node {node.node_id} received at {path}: {msg}")

        self.send_response(204 if response is None else 200)
        self.end_headers()
        if response:
            self.wfile.write(response)


# Based on tornado.ioloop.IOLoop.instance() approach.
# See https://github.com/facebook/tornado
class SingletonMixin(object):
    __singleton_instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        print("-----------------------")
        print(cls.__singleton_instance)
        import traceback
        traceback.print_stack(limit=10)
        print('-----------------------')
        print()
        if cls.__singleton_instance is None:
            cls.__singleton_instance = cls(*args, **kwargs)
        return cls.__singleton_instance


@dataclasses.dataclass
class Node(SingletonMixin):
    """Represents a Raft node."""
    node_id: str = ''
    port: Optional[int] = None
    peers: List[int] = dataclasses.field(default_factory=list)
    running: bool = False
    message_success_rate: float = 1.0

    _endpoints: Dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        print(f"creating a new node: {self.node_id}")

    def start(self):
        if self.running:
            logging.info(f"Node {self.node_id} is already running.")
            return

        class HTTPServerWithNode(HTTPServer):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.node = Node.instance()

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

    def register_endpoint(self, endpoint, handler):
        self._endpoints[endpoint] = handler

    def handle_message(self, endpoint: str, msg: bytes) -> Tuple[Optional[int], Optional[bytes]]:
        # send message to correct function based on input
        logging.info(f"Received message from endpoint {endpoint}: {msg}")
        logging.info("")
        return b'Hello world!'

    def send_message(self, target: int, endpoint: str, msg: str | bytes) -> Optional[bytes]:
        if not self.running:
            return None

        # Simulation of dropping random messages!
        # FIXME?: move it to the decorator?
        if random.random() < self.message_success_rate:
            url = f"http://localhost:{target}/{endpoint}"
            logging.info(f"Sending message to {url} with contents: {msg}")
            try:
                response = requests.post(url, data=msg)
                print(f"Response recieved: {response}")
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
