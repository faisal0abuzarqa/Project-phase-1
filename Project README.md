# Network Diagnostics Tool (TCP Client-Server Application)

**Course:** Information & Network Security Programming (605346)  
**Institution:** University of Petra — Faculty of Information Technology, Department of Information Security  
**Semester:** 2026-2027 (Semester 1) — Phase 1  

---

## Team Members & Roles

| Role | Task / Subcommand | Student Name | Student ID |
| :--- | :--- | :--- | :--- |
| **Student 1** | Concurrent TCP Server (`server`) | Etaf Alomari | 202511059 |
| **Student 2** | Reachability Client (`ping`) | Muhannad Qawasmi | 202420074 |
| **Student 3** | Latency Client (`echo`) | Hala Jabr | 202320571 |
| **Student 4** | Server Activity Client (`stats`) | Faisal Marwan | 201910617 |

---

## Project Overview

The **Network Diagnostics Tool** is a command-line client-server application built in Python using TCP sockets and multithreading. It verifies whether a diagnostic server is reachable (`PING`), measures round-trip response latency (`ECHO`), and monitors real-time server uptime and completed request counts (`STATS`).

---

## Setup Instructions

1. Ensure **Python 3.9 or higher** is installed (`python --version`).
2. Clone or download this repository. No external packages are required (standard library only).
3. Open multiple terminal windows to run the server and the three clients concurrently.

---

## Example Commands & Expected Output

### 1. Start the Server (Terminal 1) — Etaf Alomari
```bash
python "TCP_Project_phase 1.py" server --host 127.0.0.1 --port 5000
```
**Expected Output:**
```text
Server running on 127.0.0.1:5000
Logging requests to: server_log_20261123_191500.log
Waiting for clients... (Ctrl+C to stop)
```

### 2. Run Reachability Client — `PING` (Terminal 2) — Muhannad Qawasmi
```bash
python "TCP_Project_phase 1.py" ping 127.0.0.1 5000 --timeout 3.0
```
**Expected Output:**
```text
Reachable (127.0.0.1:5000)  server replied: PONG  RTT: 1.42 ms
```

### 3. Run Latency Client — `ECHO` (Terminal 3) — Hala Jabr
```bash
python "TCP_Project_phase 1.py" echo 127.0.0.1 5000 --count 4 --message "hello"
```
**Expected Output:**
```text
[1/4] id=1-a9c41f  RTT=1.35 ms  message=hello
[2/4] id=2-7b2e08  RTT=1.18 ms  message=hello
[3/4] id=3-4d91c2  RTT=1.24 ms  message=hello
[4/4] id=4-8f30a5  RTT=1.21 ms  message=hello

--- Latency Summary ---
Successful replies: 4/4
Min RTT: 1.18 ms
Avg RTT: 1.25 ms
Max RTT: 1.35 ms
Timeouts: 0
```

### 4. Run Server Activity Client — `STATS` (Terminal 4) — Faisal Marwan
```bash
python "TCP_Project_phase 1.py" stats 127.0.0.1 5000 --repeat 3 --interval 2
```
**Expected Output:**
```text
[1/3] uptime=00:00:14 (14.25s)  completed_requests=2
[2/3] uptime=00:00:16 (16.28s)  completed_requests=6
[3/3] uptime=00:00:18 (18.30s)  completed_requests=7
```

---

## Design Decisions & Concurrency Architecture

* **Protocol & Framing:** TCP (`SOCK_STREAM`) was selected for reliable, ordered delivery. Messages are encoded as UTF-8 newline-delimited JSON objects (`\n`), allowing strict message boundary detection (`readline`) and bounded buffer limits (`MAX_REQUEST_BYTES = 65536`).
* **Request-Response Correlation:** Every client generates a unique request identifier (`uuid.uuid4()`) and validates that the server's reply matches the expected `type`, `id`, and `status == "OK"`.
* **Multithreading (`ThreadingTCPServer`):** The server spawns a separate daemon thread per incoming client connection, allowing `PING`, `ECHO`, and `STATS` clients to execute concurrently without blocking one another.
* **Thread-Safe Shared State & Logging:**
  * A `threading.Lock()` (`counter_lock`) serializes increments to `requests_count` so no requests are lost under concurrent load.
  * A dedicated `threading.Lock()` (`log_lock`) serializes writes to a timestamped log file (`server_log_YYYYMMDD_HHMMSS.log`), preventing interleaved or corrupted log entries across worker threads.