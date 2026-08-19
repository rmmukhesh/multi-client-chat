# Assignment 2: Multi-Client Chat System with Performance Analytics

**Course:** CS 3205 — Computer Networks  
**Roll No:** CS23B040

---

## System Architecture

The system consists of four independent programs that communicate over TCP:

```
┌─────────────┐        MSG_REGISTER / MSG_DISCOVER
│   Chat      │ ──────────────────────────────────► ┌──────────────────┐
│   Client    │                                     │ Discovery Server │
│             │ ◄────────────────────────────────── │  (port 9000)     │
└──────┬──────┘        MSG_DISCOVER_RESP            └──────────────────┘
       │
       │ MSG_LOGIN / MSG_BROADCAST / MSG_PRIVATE / ...
       ▼
┌─────────────────────────────────────────────────────┐
│  Chat Server  (choose one variant)                  │
│   - chat_server_fork   (port 9001)  — fork()        │
│   - chat_server_thread (port 9002)  — pthreads      │
│   - chat_server_epoll  (port 9003)  — epoll ET      │
└─────────────────────────────────────────────────────┘
```

### Discovery Server (`discovery_server.c`)

- Maintains an in-memory registry: `username → { IP, port, password }`
- Thread-per-connection (short-lived); protected by a global mutex
- Supports `MSG_REGISTER` and `MSG_DISCOVER`

### Chat Server — Fork variant (`chat_server_fork.c`)

- Parent accepts connections; forks a **child process** per client
- Two pipes per client: `to_child` (parent→child) and `from_child` (child→parent)
- Parent's `select()` loop reads all `from_child` pipes and routes messages
- Shared client table lives in `mmap(MAP_SHARED | MAP_ANONYMOUS)`
- `SIGCHLD` handler reaps zombies automatically

### Chat Server — Thread variant (`chat_server_thread.c`)

- One **pthread** per client; all threads share the same address space
- Global `table_lock` mutex protects the client slot array
- Per-slot `send_lock` mutex serialises writes to each client's socket
- Routing happens directly inside each client's thread

### Chat Server — Epoll variant (`chat_server_epoll.c`)

- **Single-threaded event loop** using `epoll` in edge-triggered (`EPOLLET`) mode
- All sockets set to `O_NONBLOCK`
- Per-client receive buffer (`rbuf`) accumulates bytes until a full message is assembled
- Per-client send buffer (`SendBuf`) handles `EAGAIN` on writes; `EPOLLOUT` drains it
- No mutexes needed — single thread means no concurrent access

### Chat Client (`chat_client.c`)

- Prompts for credentials on startup; optionally registers with discovery server, then logs in
- Two threads: **main thread** reads stdin commands, **receiver thread** prints incoming messages
- Interactive CLI: `/broadcast`, `/msg`, `/list`, `/history`, `/status`, `/quit`, `/help`

---

## Protocol Specification

All messages use a **fixed 76-byte header** followed by a variable-length payload.

### Wire Format

```
Offset  Size  Field
──────  ────  ─────────────────────────────────────────────
0       1     msg_type   (uint8)
1       1     status     (uint8, 0=OK, non-zero=error)
2       32    sender     (null-terminated, null-padded)
34      32    receiver   (null-terminated; empty = broadcast)
66      8     timestamp  (uint64, big-endian, Unix epoch seconds)
74      2     payload_len (uint16, big-endian)
76      N     payload    (variable, max 1024 bytes)
```

### Message Types

| Hex  | Name                | Direction            | Payload format                      |
| ---- | ------------------- | -------------------- | ----------------------------------- |
| 0x01 | `MSG_REGISTER`      | client → discovery   | `"username:password:port"`          |
| 0x02 | `MSG_DISCOVER`      | client → discovery   | `"target_username"`                 |
| 0x03 | `MSG_DISCOVER_RESP` | discovery → client   | `"ip:port"`                         |
| 0x10 | `MSG_LOGIN`         | client → chat server | `"username:password"`               |
| 0x11 | `MSG_LOGOUT`        | client → chat server | _(empty)_                           |
| 0x20 | `MSG_BROADCAST`     | client ↔ server      | message text                        |
| 0x21 | `MSG_PRIVATE`       | client ↔ server      | message text (receiver set)         |
| 0x30 | `MSG_LIST_USERS`    | client → server      | _(empty)_                           |
| 0x31 | `MSG_USER_LIST`     | server → client      | `"user1,user2,user3"`               |
| 0x40 | `MSG_ACK`           | server → client      | optional description                |
| 0x41 | `MSG_ERROR`         | server → client      | error description                   |
| 0x50 | `MSG_HISTORY_REQ`   | client → server      | _(empty)_                           |
| 0x51 | `MSG_HISTORY_RESP`  | server → client      | history lines (newline-sep)         |
| 0x52 | `MSG_STATUS_CHANGE` | client → server      | `"available"` / `"busy"` / `"away"` |

### Message Framing

TCP is a stream protocol. Framing is handled by:

1. Always sending/receiving the fixed 76-byte header first
2. Reading `payload_len` from bytes 74–75 of the header
3. Reading exactly `payload_len` more bytes

This is implemented in `send_message()` / `recv_message()` in `protocol.c`.

---

## Compilation

**Requirements:** GCC, Linux (epoll is Linux-specific), Python 3, psutil, matplotlib, numpy

```bash
make          # builds all binaries
make clean    # removes binaries and protocol.o
```

---

## Execution

### 1. Start the Discovery Server

```bash
./discovery_server 9000
```

### 2. Start a Chat Server (pick one)

```bash
./chat_server_fork   9001                     # fork-based (no credential validation)
./chat_server_thread 9002                     # thread-based
./chat_server_epoll  9003                     # epoll-based

# Optionally pass discovery server host+port for credential validation:
./chat_server_fork   9001 127.0.0.1 9000
./chat_server_thread 9002 127.0.0.1 9000
./chat_server_epoll  9003 127.0.0.1 9000
```

### 3. Start a Client

```bash
# Usage: ./chat_client <disc_host> <disc_port> <chat_host> <chat_port>
./chat_client 127.0.0.1 9000 127.0.0.1 9001
```

The client then prompts interactively:

```
=== Chat Client ===
  1. Register (new user)
  2. Login
Choice: 1          ← choose 1 first time, 2 on subsequent logins
Username: alice
Password:          ← hidden input
```

### CLI Commands (once connected)

```
/broadcast <message>           Send to all online users
/msg <username> <message>      Send a private message
/list                          Show online users
/history                       View your chat history
/status <available|busy|away>  Change your status
/quit                          Logout and exit
/help                          Show command list
```

---

## Testing Guide

### Manual test (interactive)

Terminal 1:

```bash
./discovery_server 9000
```

Terminal 2:

```bash
./chat_server_fork 9001 127.0.0.1 9000
```

Terminal 3 (alice):

```bash
./chat_client 127.0.0.1 9000 127.0.0.1 9001
# Choose 1 (Register), enter username: alice, password: secret
```

Terminal 4 (bob):

```bash
./chat_client 127.0.0.1 9000 127.0.0.1 9001
# Choose 1 (Register), enter username: bob, password: secret2
```

### Run benchmarks

Install dependencies (one-time):

```bash
pip3 install psutil matplotlib numpy --break-system-packages
```

Run the full benchmark suite (builds all servers automatically):

```bash
python3 benchmark.py
```

**Flags:**

| Flag                                | Default | Description                                   |
| ----------------------------------- | ------- | --------------------------------------------- |
| `--variant {fork,thread,epoll,all}` | `all`   | Which server variant(s) to benchmark          |
| `--no-start`                        | off     | Skip server startup (servers already running) |
| `--load-only`                       | off     | Run only the load test, skip stress test      |
| `--stress-only`                     | off     | Run only the stress test, skip load test      |

**Examples:**

```bash
# Full run — all 3 variants (builds + starts servers automatically)
python3 benchmark.py

# Test only the epoll server
python3 benchmark.py --variant epoll

# Only run the load test (faster)
python3 benchmark.py --load-only

# Servers are already running externally
python3 benchmark.py --no-start
```

**What it measures:**

1. **Load test** — 10 concurrent clients × 20 broadcast messages each, barrier-synchronised start. Measures send→receive latency (ms) per message. Reports mean, median, P95, P99 and total message count.

2. **Stress test** — cumulative client ramp through `[2, 4, 6, 8, 10]` clients. Clients are connected and left running; each level adds more clients and samples metrics for a 6-second window. Reports CPU%, VmRSS, PSS and mean latency at each level.

**Metrics collected (per sample, every 1 s):**

- **CPU%** — measured across the full server process tree (psutil), correctly capturing fork children
- **VmRSS (MB)** — resident set size from `/proc/{pid}/status`, summed over all child processes
- **PSS (MB)** — proportional set size from `/proc/{pid}/smaps_rollup`, summed over all child processes
- **Latency (ms)** — time from `send()` to receiving the echoed broadcast back on the same socket

**Output files:**

| Path                                          | Contents                                                    |
| --------------------------------------------- | ----------------------------------------------------------- |
| `benchmark/load_results.csv`                  | One row per variant — latency percentiles, CPU, VmRSS, PSS  |
| `benchmark/latencies.csv`                     | Raw per-message latencies (ms) for each variant             |
| `benchmark/stress_{variant}.csv`              | One row per client-count level for each variant             |
| `benchmark/metrics_{variant}_load.log`        | 5-second sampled CPU/memory during load test                |
| `benchmark/metrics_{variant}_stress_{N}c.log` | 5-second sampled CPU/memory during stress test at N clients |

**Plots generated (`benchmark/plots/`):**

| File                       | What it shows                                                 |
| -------------------------- | ------------------------------------------------------------- |
| `latency_distribution.png` | Box-plot of message delivery time (Fork vs Threaded vs Epoll) |
| `cpu_vs_clients.png`       | CPU avg and CPU peak vs client count (side-by-side)           |
| `memory_vs_clients.png`    | VmRSS and PSS vs client count (side-by-side)                  |
| `latency_vs_clients.png`   | Average delivery time vs client count (all 3 variants)        |

---
