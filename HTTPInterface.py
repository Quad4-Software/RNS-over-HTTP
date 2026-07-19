import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Queue
from socketserver import ThreadingMixIn

import requests
import RNS
from requests.adapters import HTTPAdapter
from RNS.Interfaces.Interface import Interface


class HDLC:
    """Pipe-compatible simplified HDLC framing for HTTP bodies.

    Same FLAG/ESC scheme as RNS PipeInterface so multiple packets can
    share one HTTP request or response without losing boundaries.
    """

    FLAG = 0x7E
    ESC = 0x7D
    ESC_MASK = 0x20

    @staticmethod
    def escape(data):
        data = data.replace(
            bytes([HDLC.ESC]),
            bytes([HDLC.ESC, HDLC.ESC ^ HDLC.ESC_MASK]),
        )
        data = data.replace(
            bytes([HDLC.FLAG]),
            bytes([HDLC.ESC, HDLC.FLAG ^ HDLC.ESC_MASK]),
        )
        return data

    @staticmethod
    def frame(packet):
        return bytes([HDLC.FLAG]) + HDLC.escape(packet) + bytes([HDLC.FLAG])

    @staticmethod
    def deframe(buffer, max_frame_len):
        """Yield complete packets from a byte buffer.

        Returns (packets, remainder) where remainder is unconsumed bytes
        that may form the start of an incomplete frame.
        """
        packets = []
        in_frame = False
        escape = False
        data_buffer = bytearray()
        i = 0
        last_complete = 0

        while i < len(buffer):
            byte = buffer[i]
            i += 1

            if in_frame and byte == HDLC.FLAG:
                packets.append(bytes(data_buffer))
                in_frame = False
                escape = False
                data_buffer = bytearray()
                last_complete = i
            elif byte == HDLC.FLAG:
                in_frame = True
                escape = False
                data_buffer = bytearray()
            elif in_frame and len(data_buffer) < max_frame_len:
                if byte == HDLC.ESC:
                    escape = True
                else:
                    if escape:
                        if byte == HDLC.FLAG ^ HDLC.ESC_MASK:
                            byte = HDLC.FLAG
                        elif byte == HDLC.ESC ^ HDLC.ESC_MASK:
                            byte = HDLC.ESC
                        escape = False
                    data_buffer.append(byte)

        remainder = buffer[last_complete:] if in_frame else b""
        return packets, remainder


class HTTPTunnelInterface(Interface):
    """HTTP Tunnel Interface for Reticulum.

    Bidirectional RNS transport over HTTP POST with pipe-compatible HDLC
    framing on request and response bodies. Uses HTTP/1.1 keep-alive and a
    small urllib3 connection pool so the client reuses one TCP session.

    Configuration:
        mode: "client" or "server" (tunnel role)
        listen_host: bind address (server mode)
        listen_port: bind port (server mode)
        server_url: URL of the HTTP server (client mode)
        poll_interval: client poll interval in seconds (default: 0.1)
        check_user_agent: validate User-Agent on server (default: True)
        user_agent: User-Agent string (default: RNS-HTTP-Tunnel/1.0)
        serve_html_page: serve HTML on GET / (default: False)
        html_file_path: path to HTML camouflage file
        mtu: hardware MTU (default: 4096)
        pool_connections: urllib3 pools to cache (client, default: 1)
        pool_maxsize: max persistent connections per pool (client, default: 1)
        keepalive_timeout: Keep-Alive timeout seconds advertised by server (default: 60)

    Config type must match module basename (HTTPInterface.py -> HTTPInterface).
    """

    DEFAULT_IFAC_SIZE = 16
    BITRATE_GUESS = 10_000_000
    AUTOCONFIGURE_MTU = True

    DEFAULT_MTU = 4096
    TUNNEL_USER_AGENT = "RNS-HTTP-Tunnel/1.0"
    DEFAULT_POLL_INTERVAL = 0.1
    DEFAULT_POOL_CONNECTIONS = 1
    DEFAULT_POOL_MAXSIZE = 1
    DEFAULT_KEEPALIVE_TIMEOUT = 60

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
        user_agent = (
            str(ifconf["user_agent"])
            if "user_agent" in ifconf
            else self.TUNNEL_USER_AGENT
        )
        mtu = int(ifconf["mtu"]) if "mtu" in ifconf else self.DEFAULT_MTU
        serve_html_page = (
            ifconf.as_bool("serve_html_page") if "serve_html_page" in ifconf else False
        )
        html_file_path = (
            ifconf["html_file_path"] if "html_file_path" in ifconf else None
        )
        pool_connections = (
            int(ifconf["pool_connections"])
            if "pool_connections" in ifconf
            else self.DEFAULT_POOL_CONNECTIONS
        )
        pool_maxsize = (
            int(ifconf["pool_maxsize"])
            if "pool_maxsize" in ifconf
            else self.DEFAULT_POOL_MAXSIZE
        )
        keepalive_timeout = (
            int(ifconf["keepalive_timeout"])
            if "keepalive_timeout" in ifconf
            else self.DEFAULT_KEEPALIVE_TIMEOUT
        )

        self.mode = mode

        if mode not in ["client", "server"]:
            raise ValueError(
                f"Invalid mode '{mode}' for {self}. Must be 'client' or 'server'",
            )

        if mode == "client" and server_url is None:
            raise ValueError(f"No server_url specified for client mode in {self}")

        if pool_connections < 1 or pool_maxsize < 1:
            raise ValueError(
                f"pool_connections and pool_maxsize must be >= 1 for {self}",
            )

        self.owner = owner
        self.IN = True
        self.mtu = mtu
        self.check_user_agent = check_user_agent
        self.user_agent = user_agent
        self.serve_html_page = serve_html_page
        self.html_file_path = html_file_path
        self.html_content = None
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.keepalive_timeout = keepalive_timeout
        self._tcp_accepts = 0
        self._http_requests = 0
        self._stats_lock = threading.Lock()

        if self.serve_html_page and self.html_file_path:
            self._load_html_content()

        self._recv_queue = Queue()
        self._send_queue = Queue()
        self._stop_event = threading.Event()
        self._frame_remainder = b""
        self._frame_lock = threading.Lock()

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

    def _drain_send_frames(self):
        """Take all queued packets and return HDLC-framed wire bytes."""
        parts = []
        while not self._send_queue.empty():
            try:
                packet = self._send_queue.get_nowait()
            except Empty:
                break
            if packet:
                parts.append(HDLC.frame(packet))
        return b"".join(parts)

    def _ingest_wire_bytes(self, wire_data):
        """Deframe HDLC wire bytes and deliver each packet inbound."""
        if not wire_data:
            return

        with self._frame_lock:
            buffer = self._frame_remainder + wire_data
            packets, self._frame_remainder = HDLC.deframe(buffer, self.HW_MTU)

        for packet in packets:
            if packet:
                self._recv_queue.put(packet)

    def _record_http_request(self):
        with self._stats_lock:
            self._http_requests += 1

    def connection_stats(self):
        """Return TCP accept and HTTP request counters (server) or pool stats (client)."""
        with self._stats_lock:
            stats = {
                "tcp_accepts": self._tcp_accepts,
                "http_requests": self._http_requests,
            }
        if self.mode == "client" and getattr(self, "session", None) is not None:
            try:
                adapter = self.session.get_adapter(self.server_url)
                num_connections = 0
                num_requests = 0
                for key in list(adapter.poolmanager.pools.keys()):
                    pool = adapter.poolmanager.pools.get(key)
                    if pool is None:
                        continue
                    num_connections += getattr(pool, "num_connections", 0)
                    num_requests += getattr(pool, "num_requests", 0)
                stats["pool_num_connections"] = num_connections
                stats["pool_num_requests"] = num_requests
            except Exception:
                pass
        return stats

    def setup_server(self):
        interface_instance = self

        class TunnelRequestHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _send_common_headers(self, content_type, content_length):
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(content_length))
                self.send_header("Connection", "keep-alive")
                self.send_header(
                    "Keep-Alive",
                    f"timeout={interface_instance.keepalive_timeout}, max=1000",
                )

            def do_GET(self):
                interface_instance._record_http_request()
                if (
                    self.path == "/"
                    and interface_instance.serve_html_page
                    and interface_instance.html_content
                ):
                    body = interface_instance.html_content.encode("utf-8")
                    self.send_response(200)
                    self._send_common_headers("text/html; charset=utf-8", len(body))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_response(404)
                    self._send_common_headers("text/plain", 0)
                    self.end_headers()

            def do_POST(self):
                interface_instance._record_http_request()
                if self.path == "/":
                    if interface_instance.check_user_agent:
                        user_agent = self.headers.get("User-Agent", "")
                        if user_agent != interface_instance.user_agent:
                            RNS.log(
                                f"Rejected request with invalid User-Agent: {user_agent}",
                                RNS.LOG_WARNING,
                            )
                            body = b"Forbidden"
                            self.send_response(403)
                            self._send_common_headers("text/plain", len(body))
                            self.end_headers()
                            self.wfile.write(body)
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
                        interface_instance._ingest_wire_bytes(client_data)

                    server_data = interface_instance._drain_send_frames()
                    if server_data:
                        RNS.log(
                            f"Sending {len(server_data)} framed bytes to client",
                            RNS.LOG_EXTREME,
                        )

                    self.send_response(200)
                    self._send_common_headers(
                        "application/octet-stream",
                        len(server_data),
                    )
                    self.end_headers()
                    self.wfile.write(server_data)
                else:
                    self.send_response(404)
                    self._send_common_headers("text/plain", 0)
                    self.end_headers()

            def log_message(self, fmt, *args):
                pass

        class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
            daemon_threads = True
            allow_reuse_address = True

            def get_request(self):
                request, client_address = super().get_request()
                with interface_instance._stats_lock:
                    interface_instance._tcp_accepts += 1
                try:
                    request.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except OSError:
                    pass
                return request, client_address

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
            f"HTTP server started on http://{self.listen_host}:{self.listen_port} "
            f"(HTTP/1.1 keep-alive timeout={self.keepalive_timeout}s)",
            RNS.LOG_NOTICE,
        )

    def setup_client(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Connection": "keep-alive",
                "Accept-Encoding": "identity",
            }
        )
        adapter = HTTPAdapter(
            pool_connections=self.pool_connections,
            pool_maxsize=self.pool_maxsize,
            max_retries=0,
            pool_block=True,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self._http_adapter = adapter
        self._consecutive_failures = 0
        self._max_backoff = 30.0

        thread = threading.Thread(target=self.client_loop)
        thread.daemon = True
        thread.start()

        self.online = True
        RNS.log(
            f"HTTP client started, connecting to {self.server_url} "
            f"(pool_connections={self.pool_connections}, "
            f"pool_maxsize={self.pool_maxsize})",
            RNS.LOG_NOTICE,
        )

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
            data_to_send = self._drain_send_frames()

            try:
                RNS.log(
                    f"Sending {len(data_to_send)} framed bytes to server",
                    RNS.LOG_EXTREME,
                )
                response = self.session.post(
                    self.server_url,
                    data=data_to_send,
                    timeout=5,
                    headers={"Connection": "keep-alive"},
                )
                response.raise_for_status()

                if response.content:
                    RNS.log(
                        f"Received {len(response.content)} bytes from server",
                        RNS.LOG_EXTREME,
                    )
                    self._ingest_wire_bytes(response.content)

                    while not self._recv_queue.empty():
                        try:
                            packet = self._recv_queue.get_nowait()
                        except Empty:
                            break
                        if packet:
                            self.process_incoming(packet)

                if self._consecutive_failures > 0:
                    RNS.log(f"Reconnected to server for {self}", RNS.LOG_INFO)
                    self._consecutive_failures = 0

            except requests.exceptions.RequestException as e:
                if self._stop_event.is_set():
                    break
                self._consecutive_failures += 1
                if self._consecutive_failures % 10 == 1:
                    RNS.log(
                        f"Error communicating with server for {self} "
                        f"(attempt {self._consecutive_failures}): {e}",
                        RNS.LOG_WARNING,
                    )

            if self._stop_event.is_set():
                break

            if self._consecutive_failures > 0:
                delay = min(
                    self.poll_interval * (2 ** min(self._consecutive_failures - 1, 5)),
                    self._max_backoff,
                )
            else:
                delay = self.poll_interval

            self._stop_event.wait(delay)

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
            httpd = getattr(self, "_http_server", None)
            if httpd is not None:
                def _shutdown():
                    try:
                        httpd.shutdown()
                    except Exception:
                        pass
                    try:
                        httpd.server_close()
                    except Exception:
                        pass

                threading.Thread(target=_shutdown, daemon=True).start()

            if hasattr(self, "_server_thread") and self._server_thread:
                self._server_thread.join(timeout=2)
                if self._server_thread.is_alive() and httpd is not None:
                    try:
                        httpd.socket.close()
                    except Exception:
                        pass
                    self._server_thread.join(timeout=1)

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
