import logging
import dataclasses
import threading
import random
import requests
import http.server

from typing import Optional, List, Dict, Tuple, Any


class NodeHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        msg = self.rfile.read(content_length)
        path = self.path.lstrip("/")  # Get the endpoint name
        node = Node.instance()

        # Pass message to the appropriate handler
        response = node.handle_message(path, msg)
        logging.debug(f"Node {node.node_id} received at {path}: {msg}")
        logging.debug(f"Node response: {response}")

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
        if cls.__singleton_instance is None:
            cls.__singleton_instance = cls(*args, **kwargs)
        return cls.__singleton_instance


@dataclasses.dataclass
class Node(SingletonMixin):
    """Represents a Raft node."""

    node_id: str = ""
    port: Optional[int] = None
    peers: List[int] = dataclasses.field(default_factory=list)
    running: bool = False
    message_success_rate: float = 1.0

    _endpoints: Dict = dataclasses.field(default_factory=dict)

    raft_server: Optional[Any] = None  # Use Any to avoid direct import

    def start(self):
        if self.running:
            logging.info(f"Node {self.node_id} is already running.")
            return

        class HTTPServerWithNode(http.server.ThreadingHTTPServer):
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

    def register_endpoint(self, endpoint: str, handler: Any):
        logging.info(f"registering endpoint at {endpoint}!")
        self._endpoints[endpoint] = handler

    def handle_message(self, endpoint: str, msg: bytes) -> Optional[bytes]:
        # send message to correct function based on input
        logging.debug(f"Received message from endpoint {endpoint}: {msg}")
        if endpoint not in self._endpoints:
            return None
        else:
            return self._endpoints[endpoint](msg)

    def send_message(
        self, target: int, endpoint: str, msg: str | bytes
    ) -> Optional[bytes]:
        if not self.running:
            return None

        # Simulation of dropping random messages!
        # FIXME?: move it to the decorator?
        if random.random() < self.message_success_rate:
            url = f"http://localhost:{target}/{endpoint}"
            logging.debug(f"Sending message to {url} with contents: {msg}")
            try:
                response = requests.post(url, data=msg)
                logging.debug(
                    f"Received response with status code: {response.status_code}"
                )
                if response.status_code == 200:
                    logging.debug(f"Received content: {response.content}")
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


node = Node.instance()
