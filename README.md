# RNS-over-HTTP

This Reticulum Interface allows using HTTP POST requests as a bidirectional transport layer. It consists of two parts: a client and a server. The server must have a public IP address and be accessible via HTTP. The client only needs internet access. One server can serve any number of clients.

This could be used to bypass firewalls, DPI, and other restrictions. Make sure to adapt the user-agent accordingly or disable it. 

## Features

- Bidirectional communication
- User-Agent check for security (optional)
- Automatic retry on connection failures
- Configurable polling interval
- MTU support for large data transfers
- Runs over standard HTTP ports (typically 80/443)

## Setup

Dependencies:

Python 3.9+ and Requests

## Configuration

1. Download http_interface.py to `~/.reticulum/interfaces/` or wherever you want to store it.

2. Add a PipeInterface to your `~/.reticulum/config` file on both the server and the client and update the path to the http_interface.py file, as well as the server and client URLs.

### Client Configuration

```ini
[[HTTP Interface]]
    type = PipeInterface
    enabled = True
    command = python3 /path/to/your/http_interface.py client --url http://<server-host>:<port>
    # Optional: delay before respawn in seconds
    respawn_delay = 2
    # Optional: adjust polling interval (default 0.1s)
    # command = python3 /path/to/your/http_interface.py client --url http://<server-host>:<port> --poll-interval 0.5
    name = HTTP Interface
```

### Server Configuration

```ini
[[HTTP Interface]]
    type = PipeInterface
    enabled = True
    command = python3 /path/to/your/http_interface.py server --host 0.0.0.0 --port 8080
    # Optional: delay before respawn in seconds
    respawn_delay = 2
    name = HTTP Interface
```

### Example using Caddy

#### Using a main domain

```
example.com {
    reverse_proxy 127.0.0.1:8080

    header {
        # Remove server identification
        -Server
        # Add security headers
        X-Content-Type-Options nosniff
    }
}
```

#### Using a subdomain

```
api1.example.com {
    reverse_proxy 127.0.0.1:8080

    header {
        # Remove server identification
        -Server
        # Add security headers
        X-Content-Type-Options nosniff
    }
}
```

more examples will be available soon.

### Options

- `--mtu`: Maximum transmission unit (default: 4096 bytes)
- `--poll-interval`: Client polling interval in seconds (default: 0.1)
- `--verbose`: Enable verbose logging
- `--host`: Server listen host (default: 0.0.0.0)
- `--port`: Server listen port (default: 8080)
- `--disable-user-agent-check`: Disable User-Agent validation (server mode only)

## Security

By default, the server validates that incoming requests include the correct User-Agent header (`RNS-HTTP-Tunnel/1.0`) which can be changed but make sure the users that use your server have the correct User-Agent header in the interface. This helps prevent:
- Web crawlers and bots from accessing the tunnel
- Casual browsing attempts
- Unauthorized data collection

You can disable this check with `--disable-user-agent-check`

## License

[MIT License](LICENSE)