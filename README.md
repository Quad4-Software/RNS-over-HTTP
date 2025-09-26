# RNS-over-HTTP

[Русский](README-RU.md)

A Reticulum interface that tunnels traffic over standard HTTP/S POST requests. This allows Reticulum to operate on networks where only web traffic is permitted, effectively bypassing firewalls, DPI, and other restrictions.

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
-   **Simple Setup**: No complex dependencies, just Python and `requests`.
-   **Reliable**: Automatic connection retry with exponential backoff.
-   **Flexible**: Supports custom MTU sizes and configurable polling intervals.
-   **Proxy-Friendly**: Works seamlessly behind reverse proxies like Caddy or Nginx.

## Getting Started

### Requirements

-   Python 3.9 or later
-   `pip` for installing packages

### Installation

1.  **Install the `requests` library if not already installed:**
    ```bash
    pip install requests
    ```

2.  **Download the interface script:**
    Place `http_interface.py` in a known location on both your client and server machines, for example, `~/.reticulum/interfaces/`.

## Configuration

Set up a `PipeInterface` in your `~/.reticulum/config` file on both the server and client machines.

### Server Configuration

The server listens for incoming connections from clients.

```ini
[[HTTP Interface]]
    type = PipeInterface
    enabled = True
    # The command to run the server script. Listens on all interfaces by default.
    command = python3 /path/to/your/http_interface.py server --host 0.0.0.0 --port 8080
    # Optional: delay before respawning the interface if it crashes.
    respawn_delay = 5
    name = HTTP Interface Server
```

### Client Configuration

The client connects to the server's public URL.

```ini
[[HTTP Interface]]
    type = PipeInterface
    enabled = True
    # The command to run the client script. Point --url to your server.
    command = python3 /path/to/your/http_interface.py client --url http://<your-server-ip-or-domain>
    # Optional: delay before respawning the interface if it crashes.
    respawn_delay = 5
    name = HTTP Interface Client
```

## Command-Line Options

You can customize the behavior of the script with these arguments:

-   `--mtu`: Maximum Transmission Unit in bytes (default: `4096`).
-   `--poll-interval`: Client polling interval in seconds (default: `0.1`).
-   `--verbose` or `-v`: Enable verbose debug logging.
-   `--host`: Server listen host (default: `0.0.0.0`).
-   `--port`: Server listen port (default: `8080`).
-   `--disable-user-agent-check`: Disable User-Agent validation on the server.

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