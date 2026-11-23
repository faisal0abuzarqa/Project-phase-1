import argparse
from datetime import datetime
import json
import math
import socket
import socketserver
import threading
import time
import uuid

MAX_REQUEST_BYTES = 65_536
MAX_RESPONSE_BYTES = 131_072
CLIENT_TIMEOUT_SECONDS = 5.0


def error_response(message, request_id=None):
    return {"type": "ERROR", "id": request_id, "error": message}


def process_request(server, data):
    request_id = data.get("id")
    with server.counter_lock:
        server.requests_count += 1
        completed = server.requests_count

    if not isinstance(request_id, str) or not request_id:
        return error_response("Missing or invalid request id", request_id)

    request_type = data.get("type")
    if request_type == "PING":
        return {"type": "PING", "id": request_id, "status": "OK", "message": "PONG"}
    if request_type == "ECHO":
        message = data.get("message")
        if not isinstance(message, str):
            return error_response("ECHO requires a string message", request_id)
        return {"type": "ECHO", "id": request_id, "status": "OK", "message": message}
    if request_type == "STATS":
        return {
            "type": "STATS", "id": request_id, "status": "OK",
            "uptime_seconds": round(time.monotonic() - server.started_at, 2),
            "completed_requests": completed,
        }
    return error_response("Invalid request type", request_id)


class ClientHandler(socketserver.StreamRequestHandler):
    def send_response(self, response):
        self.wfile.write((json.dumps(response, ensure_ascii=False) + "\n").encode("utf-8"))
        self.wfile.flush()

    def handle(self):
        self.connection.settimeout(CLIENT_TIMEOUT_SECONDS)
        client_addr = f"{self.client_address[0]}:{self.client_address[1]}"
        req_type = "UNKNOWN"
        req_id = "N/A"
        outcome = "FAILED"

        try:
            raw = self.rfile.readline(MAX_REQUEST_BYTES + 1)
            if not raw:
                return
            if len(raw) > MAX_REQUEST_BYTES:
                resp = error_response("Request is too large")
                outcome = "ERROR: Request too large"
                self.send_response(resp)
                self.server.log_event(client_addr, req_type, req_id, outcome)
                return
            if not raw.endswith(b"\n"):
                resp = error_response("Request must end with a newline")
                outcome = "ERROR: Missing newline"
                self.send_response(resp)
                self.server.log_event(client_addr, req_type, req_id, outcome)
                return
            try:
                request = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                resp = error_response("Invalid JSON request")
                outcome = "ERROR: Invalid JSON"
                self.send_response(resp)
                self.server.log_event(client_addr, req_type, req_id, outcome)
                return
            if not isinstance(request, dict):
                resp = error_response("Request must be a JSON object")
                outcome = "ERROR: Non-object JSON"
                self.send_response(resp)
                self.server.log_event(client_addr, req_type, req_id, outcome)
                return

            req_type = str(request.get("type", "UNKNOWN"))
            req_id = str(request.get("id", "N/A"))
            resp = process_request(self.server, request)
            outcome = resp.get("status", f"ERROR: {resp.get('error', 'Unknown')}")
            self.send_response(resp)
            self.server.log_event(client_addr, req_type, req_id, outcome)

        except (BrokenPipeError, ConnectionResetError, socket.timeout, OSError) as exc:
            outcome = f"CONNECTION_ERROR: {exc}"
            self.server.log_event(client_addr, req_type, req_id, outcome)
            print(f"Client connection ended: {exc}", flush=True)


class TCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, handler_class):
        super().__init__(address, handler_class)
        self.requests_count = 0
        self.counter_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.started_at = time.monotonic()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_filename = f"server_log_{timestamp}.log"
        self.log_event("SERVER", "STARTUP", "N/A", f"Listening on {address[0]}:{address[1]}")

    def log_event(self, client_addr, req_type, req_id, outcome):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        thread_name = threading.current_thread().name
        entry = f"[{now}] [{thread_name}] Client={client_addr} | Type={req_type} | ID={req_id} | Outcome={outcome}\n"
        try:
            with self.log_lock:
                with open(self.log_filename, "a", encoding="utf-8") as log_file:
                    log_file.write(entry)
        except OSError as exc:
            print(f"File logging error: {exc}", flush=True)


def send_request(host, port, payload, timeout):
    raw_request = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    start = time.perf_counter()
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(raw_request)
        data = sock.makefile("rb").readline(MAX_RESPONSE_BYTES + 1)
        rtt_ms = (time.perf_counter() - start) * 1000

    if not data:
        raise ConnectionError("Server closed connection without a reply")
    if len(data) > MAX_RESPONSE_BYTES:
        raise ValueError("Server response exceeded maximum allowed size")
    if not data.endswith(b"\n"):
        raise ConnectionError("Incomplete response (missing newline)")
    reply = json.loads(data.decode("utf-8"))
    if not isinstance(reply, dict):
        raise ValueError("Server response must be a JSON object")
    return reply, rtt_ms


def format_uptime(seconds):
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d} ({seconds:.2f}s)"


def check_reply(reply, kind, req_id, message=None):
    return (
        reply.get("type") == kind
        and reply.get("id") == req_id
        and reply.get("status") == "OK"
        and (message is None or reply.get("message") == message)
    )


def run_server(args):
    try:
        with TCPServer((args.host, args.port), ClientHandler) as server:
            print(f"Server running on {args.host}:{args.port}", flush=True)
            print(f"Logging requests to: {server.log_filename}", flush=True)
            print("Waiting for clients... (Ctrl+C to stop)", flush=True)
            try:
                server.serve_forever(poll_interval=0.2)
            except KeyboardInterrupt:
                print("\nServer stopped.")
        return 0
    except OSError as exc:
        print(f"Could not start server: {exc}")
        return 1


def run_ping(args):
    req_id = uuid.uuid4().hex[:8]
    try:
        reply, rtt_ms = send_request(args.host, args.port, {"type": "PING", "id": req_id}, args.timeout)
        if check_reply(reply, "PING", req_id, "PONG"):
            print(f"Reachable ({args.host}:{args.port})  server replied: PONG  RTT: {rtt_ms:.2f} ms")
            return 0
        print(f"Invalid response: {reply}")
    except (socket.timeout, TimeoutError):
        print("No response (timeout)")
    except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        print(f"PING failed: {exc}")
    return 1


def run_echo(args):
    rtts = []
    timeouts = []
    for attempt in range(1, args.count + 1):
        req_id = f"{attempt}-{uuid.uuid4().hex[:6]}"
        try:
            reply, rtt_ms = send_request(
                args.host, args.port,
                {"type": "ECHO", "id": req_id, "message": args.message}, args.timeout,
            )
            if check_reply(reply, "ECHO", req_id, args.message):
                rtts.append(rtt_ms)
                print(f"[{attempt}/{args.count}] id={req_id}  RTT={rtt_ms:.2f} ms  message={reply['message']}")
            else:
                print(f"[{attempt}/{args.count}] mismatched reply: {reply}")
        except (socket.timeout, TimeoutError):
            timeouts.append(attempt)
            print(f"[{attempt}/{args.count}] timeout after {args.timeout}s")
        except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            print(f"[{attempt}/{args.count}] ECHO failed: {exc}")
    print("\n--- Latency Summary ---")
    print(f"Successful replies: {len(rtts)}/{args.count}")
    if rtts:
        print(f"Min RTT: {min(rtts):.2f} ms")
        print(f"Avg RTT: {sum(rtts) / len(rtts):.2f} ms")
        print(f"Max RTT: {max(rtts):.2f} ms")
    else:
        print("Min/Avg/Max RTT: N/A (no successful replies)")
    print(f"Timeouts: {len(timeouts)}" + (f" (attempts: {timeouts})" if timeouts else ""))
    return 0 if len(rtts) == args.count else 1


def run_stats(args):
    successful = 0
    for attempt in range(1, args.repeat + 1):
        req_id = f"{attempt}-{uuid.uuid4().hex[:6]}"
        try:
            reply, _ = send_request(args.host, args.port, {"type": "STATS", "id": req_id}, args.timeout)
            uptime = reply.get("uptime_seconds")
            completed = reply.get("completed_requests")
            valid = (
                check_reply(reply, "STATS", req_id)
                and isinstance(uptime, (float, int)) and not isinstance(uptime, bool)
                and math.isfinite(uptime) and uptime >= 0
                and isinstance(completed, int) and not isinstance(completed, bool)
                and completed >= 0
            )
            if valid:
                successful += 1
                print(f"[{attempt}/{args.repeat}] uptime={format_uptime(uptime)}  completed_requests={completed}")
            else:
                print(f"[{attempt}/{args.repeat}] invalid response: {reply}")
        except (socket.timeout, TimeoutError):
            print(f"[{attempt}/{args.repeat}] No response (timeout after {args.timeout}s)")
        except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            print(f"[{attempt}/{args.repeat}] STATS failed: {exc}")
        if attempt < args.repeat:
            time.sleep(args.interval)
    return 0 if successful == args.repeat else 1


def port_value(value):
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def positive_int(value):
    n = int(value)
    if n < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return n


def finite_float(value, allow_zero=False):
    try:
        n = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a number") from exc
    if not math.isfinite(n) or (n < 0 if allow_zero else n <= 0):
        raise argparse.ArgumentTypeError("value must be finite and " + ("non-negative" if allow_zero else "positive"))
    return n


def main():
    parser = argparse.ArgumentParser(description="TCP project: server + PING, ECHO, STATS clients")
    sub = parser.add_subparsers(dest="mode", required=True)
    server = sub.add_parser("server", help="start the concurrent TCP server")
    server.add_argument("--host", default="127.0.0.1", help="server listen address (default 127.0.0.1)")
    server.add_argument("--port", type=port_value, required=True)
    server.set_defaults(func=run_server)

    for name, function in (("ping", run_ping), ("echo", run_echo), ("stats", run_stats)):
        client = sub.add_parser(name, help=f"run {name.upper()} client")
        client.add_argument("host", help="server IP address, e.g. 127.0.0.1")
        client.add_argument("port", type=port_value)
        client.add_argument("--timeout", type=lambda s: finite_float(s), default=3.0)
        if name == "echo":
            client.add_argument("--count", type=positive_int, default=4)
            client.add_argument("--message", default="hello")
        if name == "stats":
            client.add_argument("--repeat", type=positive_int, default=1)
            client.add_argument("--interval", type=lambda s: finite_float(s, allow_zero=True), default=2.0)
        client.set_defaults(func=function)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())