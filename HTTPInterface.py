"""RNS-over-HTTP Interface

Custom HTTP interface for Reticulum that implements bidirectional communication using HTTP POST requests.
"""

import importlib
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Queue
from socketserver import ThreadingMixIn
from threading import Event, Thread
from time import sleep

import RNS
from RNS.Interfaces.Interface import Interface

MTU = 4096
DEFAULT_USER_AGENT = "RNS-HTTP-Tunnel/1.0"


class HTTPInterface(Interface):
    """HTTPInterface provides a Reticulum interface over HTTP using bidirectional communication via HTTP POST requests."""

    DEFAULT_IFAC_SIZE = 8

    owner = None
    mode = None
    listen_host = None
    listen_port = None
    server_url = None
    poll_interval = None
    check_user_agent = None
    _recv_queue = None
    _send_queue = None
    _stop_event = None
    _worker_thread = None
    _http_server = None
    session = None
    _consecutive_failures = None
    _max_backoff = None

    def __init__(self, owner, configuration):
        """Initialize the HTTPInterface with the given owner and configuration."""
        if importlib.util.find_spec("requests") is None:
            RNS.log(
                "Using this interface requires the requests module to be installed.",
                RNS.LOG_CRITICAL,
            )
            RNS.log(
                "You can install it with the command: python3 -m pip install requests",
                RNS.LOG_CRITICAL,
            )
            RNS.panic()

        import requests

        super().__init__()

        ifconf = Interface.get_config_obj(configuration)

        name = ifconf["name"]
        self.name = name

        mode = ifconf["mode"] if "mode" in ifconf else "client"
        listen_host = ifconf["listen_host"] if "listen_host" in ifconf else "0.0.0.0"
        listen_port = int(ifconf["listen_port"]) if "listen_port" in ifconf else 8080
        server_url = ifconf["server_url"] if "server_url" in ifconf else None
        poll_interval = (
            float(ifconf["poll_interval"]) if "poll_interval" in ifconf else 1.0
        )
        check_user_agent = (
            bool(ifconf["check_user_agent"]) if "check_user_agent" in ifconf else True
        )
        user_agent = (
            ifconf["user_agent"] if "user_agent" in ifconf else DEFAULT_USER_AGENT
        )
        mtu = int(ifconf["mtu"]) if "mtu" in ifconf else MTU

        if mode not in ["server", "client"]:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'server' or 'client'")
        if mode == "client" and server_url is None:
            raise ValueError("server_url is required for client mode")

        self.HW_MTU = mtu

        self.online = False
        self.bitrate = 1000000

        self.owner = owner
        self.mode = mode
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.server_url = server_url
        self.poll_interval = poll_interval
        self.check_user_agent = check_user_agent
        self.user_agent = user_agent

        self._recv_queue = Queue()
        self._send_queue = Queue()
        self._stop_event = Event()

        if self.mode == "server":
            self._http_server = None
            self._request_handler_class = self._create_request_handler()
        else:
            self.session = requests.Session()
            self.session.headers.update({"User-Agent": self.user_agent})
            self._consecutive_failures = 0
            self._max_backoff = 30.0

        try:
            self._start_interface()
            self._read_thread = Thread(target=self._read_loop, daemon=True)
            self._read_thread.start()
        except Exception as e:
            RNS.log("Could not start HTTP interface " + str(self), RNS.LOG_ERROR)
            raise e

    def _create_request_handler(self):
        """Create a custom HTTP request handler class for the server mode."""
        interface_instance = self

        class TunnelRequestHandler(BaseHTTPRequestHandler):
            """Handles HTTP POST requests for the HTTPInterface server."""

            def __init__(self, request, client_address, server):
                self.interface = interface_instance
                super().__init__(request, client_address, server)

            def do_POST(self):
                """Handle HTTP POST requests for bidirectional data exchange."""
                if self.path == "/":
                    if self.interface.check_user_agent:
                        user_agent = self.headers.get("User-Agent", "")
                        if user_agent != self.interface.user_agent:
                            RNS.log(
                                f"Rejected request with invalid User-Agent: {user_agent}",
                                RNS.LOG_WARNING,
                            )
                            self.send_response(403)
                            self.send_header("Content-Type", "text/plain")
                            self.end_headers()
                            self.wfile.write(b"Forbidden")
                            return

                    content_length = int(self.headers.get("Content-Length", 0))
                    client_data = (
                        self.rfile.read(content_length) if content_length > 0 else b""
                    )

                    if client_data:
                        RNS.log(
                            f"Received {len(client_data)} bytes from client",
                            RNS.LOG_DEBUG,
                        )
                        self.interface._recv_queue.put(client_data)

                    server_data_parts = []
                    while True:
                        try:
                            server_data_parts.append(
                                self.interface._send_queue.get_nowait(),
                            )
                        except Empty:
                            break

                    server_data = b"".join(server_data_parts)
                    if server_data:
                        RNS.log(
                            f"Sending {len(server_data)} bytes ({len(server_data_parts)} chunks) to client",
                            RNS.LOG_DEBUG,
                        )

                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(server_data)))
                    self.end_headers()
                    self.wfile.write(server_data)
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, fmt, *args):
                """Override to suppress default logging."""

        return TunnelRequestHandler

    def _start_interface(self):
        """Start the interface in either server or client mode."""
        if self.mode == "server":
            self._start_server()
        else:
            self._start_client()

    def _start_server(self):
        """Start the HTTP server in a separate thread."""

        class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
            """Threaded HTTP server for handling multiple connections."""

        def run_server():
            """Run the HTTP server and handle requests."""
            try:
                self._http_server = ThreadedHTTPServer(
                    (self.listen_host, self.listen_port),
                    self._request_handler_class,
                )
                self.online = True
                RNS.log(
                    f"HTTP server started on http://{self.listen_host}:{self.listen_port}",
                    RNS.LOG_INFO,
                )
                self._http_server.serve_forever()
            except Exception as e:
                if not self._stop_event.is_set():
                    RNS.log(f"Server error: {e}", RNS.LOG_ERROR)
                    self.online = False

        try:
            self._http_server = ThreadedHTTPServer(
                (self.listen_host, self.listen_port),
                self._request_handler_class,
            )
            self.online = True
            RNS.log(
                f"HTTP server started on http://{self.listen_host}:{self.listen_port}",
                RNS.LOG_INFO,
            )
            self._worker_thread = Thread(
                target=self._http_server.serve_forever, daemon=True
            )
            self._worker_thread.start()
        except Exception as e:
            RNS.log(f"Could not start HTTP server: {e}", RNS.LOG_ERROR)
            self.online = False
            raise e

    def _start_client(self):
        """Start the HTTP client loop in a separate thread."""
        self._worker_thread = Thread(target=self._client_loop, daemon=True)
        self._worker_thread.start()
        self.online = True
        RNS.log(f"HTTP client started, connecting to {self.server_url}", RNS.LOG_INFO)

    def _client_loop(self):
        """Main loop for the HTTP client, handling polling and data transfer."""
        while not self._stop_event.is_set():
            data_to_send = b""
            if not self._send_queue.empty():
                from contextlib import suppress

                with suppress(Empty):
                    data_to_send = self._send_queue.get_nowait()

            try:
                RNS.log(f"Sending {len(data_to_send)} bytes to server", RNS.LOG_DEBUG)
                response = self.session.post(
                    self.server_url,
                    data=data_to_send,
                    timeout=5,
                )
                response.raise_for_status()

                if response.content:
                    RNS.log(
                        f"Received {len(response.content)} bytes from server",
                        RNS.LOG_DEBUG,
                    )
                    self._recv_queue.put(response.content)

                if self._consecutive_failures > 0:
                    RNS.log("Reconnected to server", RNS.LOG_INFO)
                    self._consecutive_failures = 0

            except Exception as e:
                self._consecutive_failures += 1
                if self._consecutive_failures % 10 == 1:
                    RNS.log(
                        f"Error communicating with server (attempt {self._consecutive_failures}): {e}",
                        RNS.LOG_ERROR,
                    )

            if self._consecutive_failures > 0:
                delay = min(
                    self.poll_interval * (2 ** min(self._consecutive_failures - 1, 5)),
                    self._max_backoff,
                )
            else:
                delay = self.poll_interval

            sleep(delay)

    def process_incoming(self, data):
        """Process incoming data received from the underlying medium."""
        self.rxb += len(data)
        self.owner.inbound(data, self)

    def process_outgoing(self, data):
        """Process outgoing data to be transmitted by the interface."""
        if self.online:
            if len(data) > self.HW_MTU:
                RNS.log(
                    f"Packet too large ({len(data)} > {self.HW_MTU}), dropping",
                    RNS.LOG_WARNING,
                )
                return

            self._send_queue.put(data)
            self.txb += len(data)

    def should_ingress_limit(self):
        """Indicate that this interface should not perform ingress limiting."""
        return False

    def __str__(self):
        """Return a string representation of the HTTPInterface."""
        return f"HTTPInterface[{self.name}]"

    def _read_loop(self):
        """Read loop that processes incoming data from the queue."""
        try:
            while not self._stop_event.is_set():
                try:
                    data = self._recv_queue.get(timeout=0.1)
                    self.process_incoming(data)
                except Empty:
                    continue
        except Exception as e:
            if not self._stop_event.is_set():
                RNS.log(f"Error in read loop: {e}", RNS.LOG_ERROR)
                self.online = False


interface_class = HTTPInterface
