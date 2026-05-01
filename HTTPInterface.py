import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Queue
from socketserver import ThreadingMixIn

import requests
import RNS
from RNS.Interfaces.Interface import Interface


class HTTPTunnelInterface(Interface):
    """HTTP Tunnel Interface for Reticulum.

    This interface implements bidirectional communication over HTTP POST requests,
    allowing Reticulum to traverse firewalls and proxies that allow HTTP/HTTPS traffic.

    Configuration:
        mode: "client" or "server" (tunnel role; distinct from Reticulum interface_mode keywords)
        listen_host: IP address to bind server to (server mode)
        listen_port: Port to bind server to (server mode)
        server_url: URL of the HTTP server (client mode)
        poll_interval: Polling interval in seconds for client (default: 0.1)
        check_user_agent: Enable User-Agent validation (default: True)
        serve_html_page: Serve HTML page on GET requests (default: False)
        html_file_path: Path to HTML file to serve (optional)

    The config "type" must match the interface module basename (HTTPInterface.py -> type = HTTPInterface).

    Config example for server:
        [[HTTP Tunnel Server]]
          type = HTTPInterface
          interface_enabled = True
          mode = server
          listen_host = 0.0.0.0
          listen_port = 8080

    Config example for server with HTML:
        [[HTTP Tunnel Server]]
          type = HTTPInterface
          interface_enabled = True
          mode = server
          listen_host = 0.0.0.0
          listen_port = 8080
          serve_html_page = True
          html_file_path = index.html

    Config example for client:
        [[HTTP Tunnel Client]]
          type = HTTPInterface
          interface_enabled = True
          mode = client
          server_url = http://example.com:8080/
          poll_interval = 0.1
    """

    DEFAULT_IFAC_SIZE = 16
    BITRATE_GUESS = 10_000_000
    AUTOCONFIGURE_MTU = True

    DEFAULT_MTU = 4096
    TUNNEL_USER_AGENT = "RNS-HTTP-Tunnel/1.0"
    DEFAULT_POLL_INTERVAL = 0.1

    def __init__(self, owner, configuration):
        super().__init__()

        ifconf = Interface.get_config_obj(configuration)

        self.name = ifconf["name"]

        mode = str(ifconf["mode"]).lower() if "mode" in ifconf else "client"
        listen_host = ifconf["listen_host"] if "listen_host" in ifconf else "0.0.0.0"
        listen_port = int(ifconf["listen_port"]) if "listen_port" in ifconf else 8080
        server_url = ifconf["server_url"] if "server_url" in ifconf else None
        poll_interval = (
            float(ifconf["poll_interval"])
            if "poll_interval" in ifconf
            else self.DEFAULT_POLL_INTERVAL
        )
        check_user_agent = (
            ifconf.as_bool("check_user_agent") if "check_user_agent" in ifconf else True
        )
        mtu = int(ifconf["mtu"]) if "mtu" in ifconf else self.DEFAULT_MTU
        serve_html_page = (
            ifconf.as_bool("serve_html_page") if "serve_html_page" in ifconf else False
        )
        html_file_path = (
            ifconf["html_file_path"] if "html_file_path" in ifconf else None
        )

        self.mode = mode

        if mode not in ["client", "server"]:
            raise ValueError(
                f"Invalid mode '{mode}' for {self}. Must be 'client' or 'server'",
            )

        if mode == "client" and server_url is None:
            raise ValueError(f"No server_url specified for client mode in {self}")

        self.owner = owner
        self.IN = True
        self.mtu = mtu
        self.check_user_agent = check_user_agent
        self.serve_html_page = serve_html_page
        self.html_file_path = html_file_path
        self.html_content = None

        if self.serve_html_page and self.html_file_path:
            self._load_html_content()

        self._recv_queue = Queue()
        self._send_queue = Queue()
        self._stop_event = threading.Event()

        self.HW_MTU = mtu
        self.online = False
        self.bitrate = HTTPTunnelInterface.BITRATE_GUESS

        if mode == "server":
            self.listen_host = listen_host
            self.listen_port = listen_port
        else:
            self.server_url = server_url
            self.poll_interval = poll_interval

        self.optimise_mtu()

        if mode == "server":
            self.setup_server()
        else:
            self.setup_client()

    def _load_html_content(self):
        try:
            if os.path.isfile(self.html_file_path):
                with open(self.html_file_path, encoding="utf-8") as f:
                    self.html_content = f.read()
                RNS.log(f"Loaded HTML content from {self.html_file_path}", RNS.LOG_INFO)
            else:
                RNS.log(f"HTML file not found: {self.html_file_path}", RNS.LOG_WARNING)
                self.html_content = None
        except Exception as e:
            RNS.log(
                f"Error loading HTML file {self.html_file_path}: {e}",
                RNS.LOG_ERROR,
            )
            self.html_content = None

    def setup_server(self):
        interface_instance = self

        class TunnelRequestHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if (
                    self.path == "/"
                    and interface_instance.serve_html_page
                    and interface_instance.html_content
                ):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header(
                        "Content-Length",
                        str(len(interface_instance.html_content)),
                    )
                    self.end_headers()
                    self.wfile.write(interface_instance.html_content.encode("utf-8"))
                else:
                    self.send_response(404)
                    self.end_headers()

            def do_POST(self):
                if self.path == "/":
                    if interface_instance.check_user_agent:
                        user_agent = self.headers.get("User-Agent", "")
                        if user_agent != HTTPTunnelInterface.TUNNEL_USER_AGENT:
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
                            RNS.LOG_EXTREME,
                        )
                        interface_instance._recv_queue.put(client_data)

                    server_data_parts = []
                    while not interface_instance._send_queue.empty():
                        try:
                            server_data_parts.append(
                                interface_instance._send_queue.get_nowait(),
                            )
                        except Empty:
                            break

                    server_data = b"".join(server_data_parts)
                    if server_data:
                        RNS.log(
                            f"Sending {len(server_data)} bytes ({len(server_data_parts)} chunks) to client",
                            RNS.LOG_EXTREME,
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
                pass

        class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
            daemon_threads = True

        def run_server():
            try:
                self._http_server = ThreadedHTTPServer(
                    (self.listen_host, self.listen_port),
                    TunnelRequestHandler,
                )
                self._http_server.daemon_threads = True
                self._http_server.serve_forever()
            except Exception as e:
                if not self._stop_event.is_set():
                    RNS.log(f"HTTP server error for {self}: {e}", RNS.LOG_ERROR)
                    if RNS.Reticulum.panic_on_interface_error:
                        RNS.panic()

        self._server_thread = threading.Thread(target=run_server, daemon=True)
        self._server_thread.start()

        thread = threading.Thread(target=self.receive_loop)
        thread.daemon = True
        thread.start()

        self.online = True
        RNS.log(
            f"HTTP server started on http://{self.listen_host}:{self.listen_port}",
            RNS.LOG_NOTICE,
        )

    def setup_client(self):
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": HTTPTunnelInterface.TUNNEL_USER_AGENT},
        )
        self._consecutive_failures = 0
        self._max_backoff = 30.0

        thread = threading.Thread(target=self.client_loop)
        thread.daemon = True
        thread.start()

        self.online = True
        RNS.log(f"HTTP client started, connecting to {self.server_url}", RNS.LOG_NOTICE)

    def receive_loop(self):
        while not self._stop_event.is_set():
            try:
                received_data = self._recv_queue.get(timeout=1)
                if received_data:
                    self.process_incoming(received_data)
            except Empty:
                continue
            except Exception as e:
                if not self._stop_event.is_set():
                    RNS.log(f"Error in receive loop for {self}: {e}", RNS.LOG_ERROR)

    def client_loop(self):
        while not self._stop_event.is_set():
            data_to_send = b""
            if not self._send_queue.empty():
                try:
                    data_to_send = self._send_queue.get_nowait()
                except Empty:
                    pass

            try:
                RNS.log(f"Sending {len(data_to_send)} bytes to server", RNS.LOG_EXTREME)
                response = self.session.post(
                    self.server_url,
                    data=data_to_send,
                    timeout=5,
                )
                response.raise_for_status()

                if response.content:
                    RNS.log(
                        f"Received {len(response.content)} bytes from server",
                        RNS.LOG_EXTREME,
                    )
                    self.process_incoming(response.content)

                if self._consecutive_failures > 0:
                    RNS.log(f"Reconnected to server for {self}", RNS.LOG_INFO)
                    self._consecutive_failures = 0

            except requests.exceptions.RequestException as e:
                self._consecutive_failures += 1
                if self._consecutive_failures % 10 == 1:
                    RNS.log(
                        f"Error communicating with server for {self} (attempt {self._consecutive_failures}): {e}",
                        RNS.LOG_WARNING,
                    )

            if self._consecutive_failures > 0:
                delay = min(
                    self.poll_interval * (2 ** min(self._consecutive_failures - 1, 5)),
                    self._max_backoff,
                )
            else:
                delay = self.poll_interval

            time.sleep(delay)

    def process_incoming(self, data):
        if len(data) > 0 and self.online:
            self.rxb += len(data)
            self.owner.inbound(data, self)

    def process_outgoing(self, data):
        if self.online:
            if len(data) > self.mtu:
                RNS.log(
                    f"Payload too large ({len(data)} > {self.mtu}) for {self}",
                    RNS.LOG_ERROR,
                )
                return

            self._send_queue.put(data)
            self.txb += len(data)

    def detach(self):
        RNS.log(f"Detaching {self}", RNS.LOG_DEBUG)
        self._stop_event.set()
        self.online = False

        if self.mode == "client" and getattr(self, "session", None) is not None:
            try:
                self.session.close()
            except Exception as e:
                RNS.log(f"Error closing HTTP session for {self}: {e}", RNS.LOG_DEBUG)

        if self.mode == "server":
            if hasattr(self, "_http_server") and self._http_server:
                try:
                    self._http_server.shutdown()
                    self._http_server.server_close()
                except Exception as e:
                    RNS.log(
                        f"Error while shutting down HTTP server for {self}: {e}",
                        RNS.LOG_ERROR,
                    )

            if hasattr(self, "_server_thread") and self._server_thread:
                self._server_thread.join(timeout=5)

    def should_ingress_limit(self):
        return False

    def __str__(self):
        name = getattr(self, "name", "?")
        if self.mode == "server":
            lh = getattr(self, "listen_host", "?")
            lp = getattr(self, "listen_port", "?")
            return f"HTTPTunnelInterface[{name}/server/{lh}:{lp}]"
        if self.mode == "client" and getattr(self, "server_url", None) is not None:
            return f"HTTPTunnelInterface[{name}/client/{self.server_url}]"
        return f"HTTPTunnelInterface[{name}/{self.mode}]"


interface_class = HTTPTunnelInterface
