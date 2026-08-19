#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "protocol.h"

#define HISTORY_DIR "./history"

/* shared-memory client entry */
typedef struct {
    char   username[MAX_USERNAME_LEN];
    char   ip[INET_ADDRSTRLEN];
    int    active, status;
    pid_t  pid;
    int    to_child[2];    /* parent writes → child reads  */
    int    from_child[2];  /* child writes  → parent reads */
} ClientSlot;

static ClientSlot *clients = NULL;   /* mmap'd shared memory */
static char disc_host[64] = "";
static int  disc_port = 0;
static volatile int running = 1;

 

static int find_slot_by_username(const char *u) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].active && !strncmp(clients[i].username, u, MAX_USERNAME_LEN))
            return i;
    return -1;
}

static int find_free_slot(void) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (!clients[i].active && clients[i].pid == 0) return i;
    return -1;
}

/* pack a Message and write it to the pipe going to slot's child */
static void pipe_send(int slot, Message *m) {
    char wire[HEADER_SIZE + MAX_PAYLOAD_LEN];
    int  len = pack_message(m, wire);
    write(clients[slot].to_child[1], wire, len);
}

static int validate(const char *u, const char *p) {
    if (!disc_host[0]) return 0;
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_port = htons(disc_port);
    inet_pton(AF_INET, disc_host, &a.sin_addr);
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) return -1;
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) { close(fd); return -1; }
    char pl[MAX_PAYLOAD_LEN]; snprintf(pl, sizeof(pl), "%s:%s", u, p);
    Message req, resp; make_message(&req, MSG_VALIDATE, u, "discovery", pl);
    send_message(fd, &req);
    int ok = (recv_message(fd, &resp) == 0 && resp.status == 0) ? 0 : -1;
    close(fd); return ok;
}

static void save_history(const char *from, const char *to,
                          const char *text, time_t ts) {
    char path[128]; snprintf(path, sizeof(path), "%s/%s.log", HISTORY_DIR, from);
    FILE *f = fopen(path, "a"); if (!f) return;
    char tb[32]; strftime(tb, sizeof(tb), "%Y-%m-%d %H:%M:%S", localtime(&ts));
    if (to && to[0]) fprintf(f, "[%s] -> %s : %s\n",    tb, to, text);
    else             fprintf(f, "[%s] BROADCAST : %s\n", tb, text);
    fclose(f);
}

/*  parent: route a message received from a child */

static void route_message(int from, Message *msg) {
    Message resp;
    const char *sn[] = {"available", "busy", "away"};

    switch (msg->type) {

    case MSG_LOGIN: {
        char u[MAX_USERNAME_LEN]={0}, p[MAX_PASSWORD_LEN]={0};
        if (sscanf(msg->payload, "%31[^:]:%63s", u, p) != 2) {
            make_message(&resp, MSG_ERROR, "server", "", "Bad login format");
            resp.status = 1; pipe_send(from, &resp); return;
        }
        int dup = find_slot_by_username(u);
        if (dup != -1 && dup != from) {
            make_message(&resp, MSG_ERROR, "server", u, "Already logged in");
            resp.status = 1; pipe_send(from, &resp); return;
        }
        if (validate(u, p) != 0) {
            make_message(&resp, MSG_ERROR, "server", u, "Invalid credentials");
            resp.status = 1; pipe_send(from, &resp); return;
        }
        strncpy(clients[from].username, u, MAX_USERNAME_LEN - 1);
        clients[from].active = 1; clients[from].status = STATUS_AVAILABLE;
        printf("[FORK] Login: %s slot=%d\n", u, from);
        make_message(&resp, MSG_ACK, "server", u, "Login successful");
        pipe_send(from, &resp); break;
    }

    case MSG_LOGOUT: {
        char note[64]; snprintf(note, sizeof(note), "%s left", clients[from].username);
        printf("[FORK] Logout: %s\n", clients[from].username);
        clients[from].active = 0; memset(clients[from].username, 0, MAX_USERNAME_LEN);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (i != from && clients[i].active) {
                make_message(&resp, MSG_BROADCAST, "server", "", note);
                pipe_send(i, &resp);
            }
        }
        break;
    }

    case MSG_BROADCAST:
        if (!clients[from].active) {
            make_message(&resp, MSG_ERROR, "server", "", "Not authenticated");
            resp.status = 1; pipe_send(from, &resp); return;
        }
        printf("[FORK] Broadcast %s: %s\n", msg->sender, msg->payload);
        save_history(msg->sender, "", msg->payload, (time_t)msg->timestamp);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (i != from && clients[i].active) pipe_send(i, msg);
        make_message(&resp, MSG_ACK, "server", msg->sender, "");
        pipe_send(from, &resp); break;

    case MSG_PRIVATE: {
        if (!clients[from].active) {
            make_message(&resp, MSG_ERROR, "server", "", "Not authenticated");
            resp.status = 1; pipe_send(from, &resp); return;
        }
        int t = find_slot_by_username(msg->receiver);
        if (t == -1) {
            make_message(&resp, MSG_ERROR, "server", msg->sender, "User not online");
            resp.status = 1; pipe_send(from, &resp); return;
        }
        printf("[FORK] Private %s->%s: %s\n", msg->sender, msg->receiver, msg->payload);
        save_history(msg->sender, msg->receiver, msg->payload, (time_t)msg->timestamp);
        pipe_send(t, msg);
        make_message(&resp, MSG_ACK, "server", msg->sender, "");
        pipe_send(from, &resp); break;
    }

    case MSG_LIST_USERS: {
        char list[MAX_PAYLOAD_LEN] = {0}; int first = 1;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active || !clients[i].username[0]) continue;
            if (!first) strncat(list, ", ", MAX_PAYLOAD_LEN - strlen(list) - 1);
            char e[64]; snprintf(e, sizeof(e), "%s(%s)", clients[i].username, sn[clients[i].status]);
            strncat(list, e, MAX_PAYLOAD_LEN - strlen(list) - 1); first = 0;
        }
        make_message(&resp, MSG_USER_LIST, "server", msg->sender, list);
        pipe_send(from, &resp); break;
    }

    case MSG_HISTORY_REQ: {
        char path[128]; snprintf(path, sizeof(path), "%s/%s.log", HISTORY_DIR, msg->sender);
        char buf[MAX_PAYLOAD_LEN] = {0};
        FILE *f = fopen(path, "r");
        if (f) {
            size_t tot = 0; char line[256];
            while (fgets(line, sizeof(line), f) && tot + strlen(line) < MAX_PAYLOAD_LEN - 1)
                { strcat(buf, line); tot += strlen(line); }
            fclose(f);
        }
        make_message(&resp, MSG_HISTORY_RESP, "server", msg->sender, buf[0] ? buf : "No history.");
        pipe_send(from, &resp); break;
    }

    case MSG_STATUS_CHANGE:
        clients[from].status = strncmp(msg->payload,"busy",4)==0 ? STATUS_BUSY :
                               strncmp(msg->payload,"away",4)==0 ? STATUS_AWAY : STATUS_AVAILABLE;
        make_message(&resp, MSG_ACK, "server", msg->sender, "Status updated");
        pipe_send(from, &resp); break;

    default:
        make_message(&resp, MSG_ERROR, "server", msg->sender, "Unknown type");
        resp.status = 1; pipe_send(from, &resp); break;
    }
}

/*  read a complete wire-format message from a pipe  */

static int pipe_recv(int fd, Message *msg) {
    char hdr[HEADER_SIZE]; int got = 0;
    while (got < HEADER_SIZE) {
        int n = read(fd, hdr + got, HEADER_SIZE - got);
        if (n <= 0) return -1;
        got += n;
    }
    uint16_t plen = (uint16_t)(((unsigned char)hdr[74] << 8) | (unsigned char)hdr[75]);
    char wire[HEADER_SIZE + MAX_PAYLOAD_LEN];
    memcpy(wire, hdr, HEADER_SIZE);
    got = 0;
    while (got < (int)plen) {
        int n = read(fd, wire + HEADER_SIZE + got, plen - got);
        if (n <= 0) return -1;
        got += n;
    }
    return unpack_message(wire, HEADER_SIZE + plen, msg);
}

/* child: bridge between TCP socket and parent pipes  */

static void child_main(int sockfd, int slot) {
    close(clients[slot].to_child[1]);
    close(clients[slot].from_child[0]);
    int rd = clients[slot].to_child[0];     /* child reads from parent */
    int wr = clients[slot].from_child[1];   /* child writes to parent  */

    int maxfd = (sockfd > rd ? sockfd : rd) + 1;
    fd_set rfds;

    while (1) {
        FD_ZERO(&rfds); FD_SET(sockfd, &rfds); FD_SET(rd, &rfds);
        if (select(maxfd, &rfds, NULL, NULL, NULL) < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* client → parent */
        if (FD_ISSET(sockfd, &rfds)) {
            Message msg;
            if (recv_message(sockfd, &msg) != 0) {
                /* client disconnected  */
                make_message(&msg, MSG_LOGOUT, clients[slot].username, "", "");
                char wire[HEADER_SIZE + MAX_PAYLOAD_LEN];
                int len = pack_message(&msg, wire);
                write(wr, wire, len);
                break;
            }
            char wire[HEADER_SIZE + MAX_PAYLOAD_LEN];
            int  len = pack_message(&msg, wire);
            write(wr, wire, len);
        }

        /* parent → client */
        if (FD_ISSET(rd, &rfds)) {
            Message fwd;
            if (pipe_recv(rd, &fwd) == 0)
                send_message(sockfd, &fwd);
            else break;
        }
    }

    close(sockfd); close(rd); close(wr);
    exit(0);
}

/*  signal handlers  */

static void sigchld_handler(int sig) {
    (void)sig;
    pid_t pid; int status;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].pid == pid) {
                clients[i].pid = 0; clients[i].active = 0;
                memset(clients[i].username, 0, MAX_USERNAME_LEN);
                break;
            }
        }
    }
}
static void sig_int(int s) { (void)s; running = 0; }

/*  main  */

int main(int argc, char *argv[]) {
    int port = CHAT_PORT_FORK;
    if (argc >= 2) port = atoi(argv[1]);
    if (argc >= 4) {
        strncpy(disc_host, argv[2], sizeof(disc_host) - 1);
        disc_port = atoi(argv[3]);
    }

    signal(SIGPIPE,  SIG_IGN);
    signal(SIGCHLD,  sigchld_handler);
    signal(SIGINT,   sig_int);
    signal(SIGTERM,  sig_int);

    clients = mmap(NULL, MAX_CLIENTS * sizeof(ClientSlot),
                   PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (clients == MAP_FAILED) { perror("mmap"); exit(1); }
    memset(clients, 0, MAX_CLIENTS * sizeof(ClientSlot));

    struct stat st; if (stat(HISTORY_DIR, &st) != 0) mkdir(HISTORY_DIR, 0755);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); exit(1); }
    int opt = 1; setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(srv, MAX_CLIENTS) < 0) { perror("listen"); exit(1); }
    printf("[FORK SERVER] Listening on port %d\n", port);

    while (running) {
        /* watch server_fd + all from_child read ends */
        fd_set rfds; FD_ZERO(&rfds); FD_SET(srv, &rfds);
        int maxfd = srv;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].pid > 0) {
                int fd = clients[i].from_child[0];
                FD_SET(fd, &rfds); if (fd > maxfd) maxfd = fd;
            }
        }
        struct timeval tv = {1, 0};
        int ret = select(maxfd + 1, &rfds, NULL, NULL, &tv);
        if (ret < 0) { if (errno == EINTR) continue; perror("select"); break; }

        /* new connection */
        if (FD_ISSET(srv, &rfds)) {
            struct sockaddr_in peer; socklen_t plen = sizeof(peer);
            int cfd = accept(srv, (struct sockaddr *)&peer, &plen);
            if (cfd < 0) { if (errno != EINTR) perror("accept"); goto check_children; }

            int slot = find_free_slot();
            if (slot == -1) {
                Message err; make_message(&err, MSG_ERROR, "server", "", "Server full");
                err.status = 1; send_message(cfd, &err); close(cfd);
                printf("[FORK] Rejected: server full\n"); goto check_children;
            }
            if (pipe(clients[slot].to_child) < 0 || pipe(clients[slot].from_child) < 0) {
                perror("pipe"); close(cfd); goto check_children;
            }
            inet_ntop(AF_INET, &peer.sin_addr, clients[slot].ip, INET_ADDRSTRLEN);

            pid_t pid = fork();
            if (pid < 0) { perror("fork"); close(cfd); goto check_children; }

            if (pid == 0) {
                /* child */
                close(srv);
                child_main(cfd, slot);    /* never returns */
            }
            /* parent */
            clients[slot].pid = pid;
            close(clients[slot].to_child[0]);    /* child reads this */
            close(clients[slot].from_child[1]);  /* child writes this */
            close(cfd);
            printf("[FORK] New connection from %s slot=%d pid=%d\n", clients[slot].ip, slot, pid);
        }

check_children:
        /* messages from children */
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].pid <= 0) continue;
            int fd = clients[i].from_child[0];
            if (!FD_ISSET(fd, &rfds)) continue;
            Message msg;
            if (pipe_recv(fd, &msg) == 0) {
                route_message(i, &msg);
            } else {
                /* child pipe closed */
                clients[i].active = 0; clients[i].pid = 0;
                memset(clients[i].username, 0, MAX_USERNAME_LEN);
            }
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].pid > 0) kill(clients[i].pid, SIGTERM);
    close(srv);
    munmap(clients, MAX_CLIENTS * sizeof(ClientSlot));
    printf("[FORK SERVER] Shut down.\n");
    return 0;
}
