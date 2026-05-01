# RNS-over-HTTP

[Русский](README-RU.md)

A custom Reticulum interface that tunnels traffic over standard HTTP/S POST requests. This allows Reticulum to operate on networks where only web traffic is permitted, effectively bypassing firewalls, DPI, and other restrictions.

[Non-GitHub Mirror](https://lavaforge.org/Ivan/RNS-over-HTTP). Also available on the network `RNS-over-HTTP` node.

## Overview

RNS-over-HTTP creates a bidirectional transport layer using a simple client-server model:

-   **Server**: Runs on a machine with a public IP, listening for HTTP requests.
-   **Client**: Can be behind a firewall or NAT, only needing outbound internet access.

The client polls the server with HTTP POST requests, sending any outbound data in the request body and receiving inbound data in the response body. This makes the traffic appear as normal web activity.

## How It Works

The interface mimics a persistent connection using a long-polling-like mechanism:

1.  The client sends an HTTP POST request to the server, with any pending data in the request body.
2.  The server receives the request. It processes the data from the client and immediately sends back any data it has queued for the client in the HTTP response body.
3.  The client receives the response and processes the data.
4.  After a short, configurable polling interval, the client repeats the process.

This continuous cycle creates a reliable, albeit higher-latency, communication channel.

## Features

-   **Firewall & DPI Evasion**: Tunnels any traffic through standard HTTP/S ports (80/443).
-   **Bidirectional Communication**: Full-duplex data transfer.
-   **Simple setup**: Python, `requests`, and Reticulum (`rns`); use Poetry in this repo for a tidy dev environment and tests.
-   **Reliable**: Automatic connection retry with exponential backoff.
-   **Flexible**: Supports custom MTU sizes and configurable polling intervals.
-   **Proxy-Friendly**: Works seamlessly behind reverse proxies like Caddy or Nginx.

## Getting Started

### Requirements

-   Python 3.9 or later
-   [Poetry](https://python-poetry.org/docs/#installation) (for a reproducible dev environment and tests)

### Installation

1.  **Install dependencies with Poetry** (from this repository root):
    ```bash
    poetry install
    ```

2.  **Install the custom interface for Reticulum:**
    Copy `HTTPInterface.py` into your Reticulum interfaces directory: `~/.reticulum/interfaces/`.

### Tests

```bash
poetry run pytest
```

## Configuration

Add an interface entry to your Reticulum configuration file (`~/.reticulum/config`) on both the server and client machines.

### Server Configuration

The server listens for incoming connections from clients.

```ini
[[HTTP Server Interface]]
    type = HTTPInterface
    enabled = true
    mode = server
    listen_host = 0.0.0.0
    listen_port = 8080
    mtu = 4096
    check_user_agent = true
    user_agent = RNS-HTTP-Tunnel/1.0
```

### Client Configuration

The client connects to the server's public URL.

```ini
[[HTTP Client Interface]]
    type = HTTPInterface
    enabled = true
    mode = client
    server_url = http://your-server-ip-or-domain:8080
    poll_interval = 1.0
    mtu = 4096
    user_agent = RNS-HTTP-Tunnel/1.0
```

## Configuration Options

### Common Options

-   `mtu`: Maximum Transmission Unit in bytes (default: `4096`).
-   `name`: Interface name for logging and identification.
-   `user_agent`: User-Agent string to use for HTTP requests (default: `"RNS-HTTP-Tunnel/1.0"`).

### Server Mode Options

-   `mode`: Must be set to `server`.
-   `listen_host`: Host to bind the HTTP server to (default: `0.0.0.0`).
-   `listen_port`: Port to listen on (default: `8080`).
-   `check_user_agent`: Whether to validate User-Agent headers (default: `true`).

### Client Mode Options

-   `mode`: Must be set to `client`.
-   `server_url`: Full URL of the server to connect to (required for client mode).
-   `poll_interval`: Polling interval in seconds (default: `1.0`).

## Reverse Proxy Setup (Caddy Example)

### Subdomain

```caddy
# Caddyfile for example.yourdomain.com
example.yourdomain.com {
    reverse_proxy 127.0.0.1:8080

    header {
        # Hide the server software version
        -Server
        # Prevent MIME-type sniffing
        X-Content-Type-Options nosniff
    }
}
```

### Main Domain

```caddy
yourdomain.com {
    reverse_proxy 127.0.0.1:8080

    header {
        # Hide the server software version
        -Server
        # Prevent MIME-type sniffing
        X-Content-Type-Options nosniff
    }
}
```

## Security Considerations

-   **Use HTTPS**: Helps bypass some firewalls and DPI that could potentially see reticulum data. 
-   **User-Agent Check**: By default, the server validates the `User-Agent` header (`RNS-HTTP-Tunnel/1.0`). This provides basic protection against web crawlers and casual scanning. If you need to bypass sophisticated DPI, you might consider changing this header in the script to mimic a common browser and disabling the check on the server (`--disable-user-agent-check`).

## License

This project is licensed under the [MIT License](LICENSE).