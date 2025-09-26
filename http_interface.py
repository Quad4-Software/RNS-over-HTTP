import logging
import logging.handlers
import sys
import threading
from abc import ABC, abstractmethod
from queue import Queue, Empty
from threading import Thread, Event
from time import sleep
from typing import Iterable
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import socket

import requests

MTU = 4096
TUNNEL_USER_AGENT = "RNS-HTTP-Tunnel/1.0"

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

setup_logging()

class AbstractTunnel(ABC):
    def __init__(self, mtu: int):
        self.mtu = mtu
        self._recv_queue: Queue[bytes] = Queue()
        self._send_queue: Queue[bytes] = Queue()
        self._stop_event = Event()
        self.logger = logging.getLogger(self.__class__.__name__)

    def send(self, pkt: bytes) -> None:
        if len(pkt) > self.mtu:
            raise ValueError(f"payload too large ({len(pkt)} > {self.mtu})")
        self._send_queue.put(pkt)

    def recv(self) -> Iterable[bytes]:
        while True:
            yield self._recv_queue.get(block=True)

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass

class Server(AbstractTunnel):
    def __init__(self, listen_host: str, listen_port: int, mtu: int, check_user_agent: bool = True):
        super().__init__(mtu)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.check_user_agent = check_user_agent
        self._server_thread: Thread | None = None
        self._http_server: HTTPServer | None = None
        
        class TunnelRequestHandler(BaseHTTPRequestHandler):
            def __init__(self, request, client_address, server, tunnel_instance=self):
                self.tunnel = tunnel_instance
                super().__init__(request, client_address, server)
            
            def do_POST(self):
                if self.path == "/":
                    if self.tunnel.check_user_agent:
                        user_agent = self.headers.get('User-Agent', '')
                        if user_agent != TUNNEL_USER_AGENT:
                            self.tunnel.logger.warning(f"Rejected request with invalid User-Agent: {user_agent}")
                            self.send_response(403)
                            self.send_header('Content-Type', 'text/plain')
                            self.end_headers()
                            self.wfile.write(b'Forbidden')
                            return
                    
                    content_length = int(self.headers.get('Content-Length', 0))
                    client_data = self.rfile.read(content_length) if content_length > 0 else b""
                    
                    if client_data:
                        self.tunnel.logger.debug(f"Received {len(client_data)} bytes from client")
                        self.tunnel._recv_queue.put(client_data)
                    
                    server_data = b""
                    if not self.tunnel._send_queue.empty():
                        try:
                            server_data = self.tunnel._send_queue.get_nowait()
                            self.tunnel.logger.debug(f"Sending {len(server_data)} bytes to client")
                        except Empty:
                            pass
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/octet-stream')
                    self.send_header('Content-Length', str(len(server_data)))
                    self.end_headers()
                    self.wfile.write(server_data)
                else:
                    self.send_response(404)
                    self.end_headers()
            
            def log_message(self, format, *args):
                pass
        
        self._request_handler_class = TunnelRequestHandler

    def start(self) -> None:
        def run_server():
            try:
                self._http_server = ThreadedHTTPServer((self.listen_host, self.listen_port), self._request_handler_class)
                self._http_server.serve_forever()
            except Exception as e:
                if not self._stop_event.is_set():
                    self.logger.error(f"Server error: {e}")
        
        self._server_thread = Thread(target=run_server, daemon=True)
        self._server_thread.start()
        self.logger.info(f"HTTP server started on http://{self.listen_host}:{self.listen_port}")

    def stop(self) -> None:
        self.logger.info("Stopping HTTP server...")
        self._stop_event.set()
        if self._http_server:
            self._http_server.shutdown()
            self._http_server.server_close()
        if self._server_thread:
            self._server_thread.join(timeout=2)

class Client(AbstractTunnel):
    def __init__(self, server_url: str, mtu: int, poll_interval: float = 1.0):
        super().__init__(mtu)
        self.server_url = server_url
        self.poll_interval = poll_interval
        self._client_thread: Thread | None = None
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': TUNNEL_USER_AGENT})
        self._consecutive_failures = 0
        self._max_backoff = 30.0  # Maximum backoff time in seconds

    def start(self) -> None:
        self._client_thread = Thread(target=self._run, daemon=True)
        self._client_thread.start()
        self.logger.info(f"HTTP client started, connecting to {self.server_url}")

    def stop(self) -> None:
        self.logger.info("Stopping HTTP client...")
        self._stop_event.set()
        if self._client_thread:
            self._client_thread.join(timeout=2)

    def _run(self):
        while not self._stop_event.is_set():
            data_to_send = b""
            if not self._send_queue.empty():
                try:
                    data_to_send = self._send_queue.get_nowait()
                except Empty:
                    pass

            try:
                self.logger.debug(f"Sending {len(data_to_send)} bytes to server")
                response = self.session.post(self.server_url, data=data_to_send, timeout=5)
                response.raise_for_status()

                if response.content:
                    self.logger.debug(f"Received {len(response.content)} bytes from server")
                    self._recv_queue.put(response.content)

                if self._consecutive_failures > 0:
                    self.logger.info("Reconnected to server")
                    self._consecutive_failures = 0

            except requests.exceptions.RequestException as e:
                self._consecutive_failures += 1
                if self._consecutive_failures % 10 == 1:
                    self.logger.error(f"Error communicating with server (attempt {self._consecutive_failures}): {e}")

            if self._consecutive_failures > 0:
                delay = min(self.poll_interval * (2 ** min(self._consecutive_failures - 1, 5)), self._max_backoff)
            else:
                delay = self.poll_interval

            sleep(delay)


if __name__ == "__main__":
    import argparse
    import os
    import errno

    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="HTTP Tunnel - Server/Client")
    parser.add_argument("mode", choices=["server", "client"], help="Run mode: server or client")
    parser.add_argument("--mtu", type=int, default=MTU, help=f"MTU size (default: {MTU})")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Listen host (for server mode)")
    parser.add_argument("--port", type=int, default=8080, help="Listen port (for server mode)")
    parser.add_argument("--url", type=str, help="Server URL (required for client mode)")
    parser.add_argument("--poll-interval", type=float, default=0.1, help="Client poll interval in seconds")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--disable-user-agent-check", action="store_true", help="Disable User-Agent validation (server mode only)")

    args = parser.parse_args()

    if args.verbose:
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.DEBUG)

    def receive_messages(tunnel, stop_event):
        stdout_fd = sys.stdout.fileno()
        try:
            for received_data in tunnel.recv():
                if stop_event.is_set():
                    break
                if received_data:
                    try:
                        os.write(stdout_fd, received_data)
                    except OSError as e:
                        if e.errno == errno.EPIPE:
                            stop_event.set()
                            return
                        else:
                            logger.error("Error writing to stdout: %s", e)
                            stop_event.set()
                            return
        except Exception:
            if not stop_event.is_set():
                logger.error("Error in receive thread", exc_info=True)
            return

    def read_stdin_bytes():
        try:
            return os.read(0, 4096)
        except (IOError, OSError):
            return None

    try:
        if args.mode == "server":
            server = Server(listen_host=args.host, listen_port=args.port, mtu=args.mtu, check_user_agent=not args.disable_user_agent_check)
            server.start()

            stop_event = threading.Event()
            receive_thread = threading.Thread(target=receive_messages, args=(server, stop_event), daemon=True)
            receive_thread.start()

            try:
                while not stop_event.is_set():
                    message = read_stdin_bytes()
                    if message:
                        server.send(message)
                    else:
                        sleep(0.01)

            except KeyboardInterrupt:
                logger.info("Stopping server...")
            finally:
                stop_event.set()
                server.stop()
                receive_thread.join(timeout=1)

        elif args.mode == "client":
            if not args.url:
                parser.error("--url is required for client mode")

            client = Client(server_url=args.url, mtu=args.mtu, poll_interval=args.poll_interval)
            client.start()

            stop_event = threading.Event()
            receive_thread = threading.Thread(target=receive_messages, args=(client, stop_event), daemon=True)
            receive_thread.start()

            try:
                while not stop_event.is_set():
                    message = read_stdin_bytes()
                    if message:
                        client.send(message)
                    else:
                        sleep(0.01)

            except KeyboardInterrupt:
                logger.info("Stopping client...")
            finally:
                stop_event.set()
                client.stop()
                receive_thread.join(timeout=1)

    except Exception as e:
        logger.error(f"A critical error occurred: {e}", exc_info=True)
        sys.exit(1)
