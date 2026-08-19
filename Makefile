# Makefile — Multi-Client Chat System
# CS 3205 — Assignment 2
# Roll No: CS23B038

CC      = gcc
CFLAGS  = -Wall -Wextra -g
PTHREAD = -pthread

# Common object
PROTO_OBJ = protocol.o

# Targets
TARGETS = discovery_server \
          chat_server_fork \
          chat_server_thread \
          chat_server_epoll \
          chat_client

.PHONY: all clean run-discovery run-fork run-thread run-epoll test benchmark

# ── Default: build everything ─────────────────────────────────────────────────
all: $(TARGETS)

# ── Shared protocol object ────────────────────────────────────────────────────
$(PROTO_OBJ): protocol.c protocol.h
	$(CC) $(CFLAGS) -c protocol.c -o $(PROTO_OBJ)

# ── Binaries ──────────────────────────────────────────────────────────────────
discovery_server: discovery_server.c $(PROTO_OBJ)
	$(CC) $(CFLAGS) $(PTHREAD) $^ -o $@

chat_server_fork: chat_server_fork.c $(PROTO_OBJ)
	$(CC) $(CFLAGS) $^ -o $@

chat_server_thread: chat_server_thread.c $(PROTO_OBJ)
	$(CC) $(CFLAGS) $(PTHREAD) $^ -o $@

chat_server_epoll: chat_server_epoll.c $(PROTO_OBJ)
	$(CC) $(CFLAGS) $^ -o $@

chat_client: chat_client.c $(PROTO_OBJ)
	$(CC) $(CFLAGS) $(PTHREAD) $^ -o $@

# ── Run targets (for quick testing) ──────────────────────────────────────────
run-discovery:
	./discovery_server 9000

run-fork:
	./chat_server_fork 9001

run-thread:
	./chat_server_thread 9002

run-epoll:
	./chat_server_epoll 9003

# ── Test ──────────────────────────────────────────────────────────────────────
# Starts all servers, runs full test suite, stops servers
test: all
	@echo "=== Starting all servers ==="
	@./discovery_server   9000 > /tmp/disc.log   2>&1 & echo $$! > /tmp/pid_disc.txt
	@./chat_server_fork   9001 > /tmp/fork.log   2>&1 & echo $$! > /tmp/pid_fork.txt
	@./chat_server_thread 9002 > /tmp/thread.log 2>&1 & echo $$! > /tmp/pid_thread.txt
	@./chat_server_epoll  9003 > /tmp/epoll.log  2>&1 & echo $$! > /tmp/pid_epoll.txt
	@sleep 1
	@echo "=== FORK (9001) ==="
	@python3 test_full_system.py 127.0.0.1 9000 127.0.0.1 9001
	@echo "=== THREAD (9002) ==="
	@python3 test_full_system.py 127.0.0.1 9000 127.0.0.1 9002
	@echo "=== EPOLL (9003) ==="
	@python3 test_full_system.py 127.0.0.1 9000 127.0.0.1 9003
	@kill $$(cat /tmp/pid_disc.txt /tmp/pid_fork.txt /tmp/pid_thread.txt /tmp/pid_epoll.txt) 2>/dev/null || true
	@echo "=== All servers stopped ==="

# ── Benchmark ─────────────────────────────────────────────────────────────────
benchmark: all
	@fuser -k 9001/tcp 9002/tcp 9003/tcp 2>/dev/null || true
	@sleep 0.3
	python3 benchmark.py --clients 10 --msgs 100

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -f $(TARGETS) $(PROTO_OBJ)
	rm -rf history/
