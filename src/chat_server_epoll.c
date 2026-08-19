#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "protocol.h"

#define HISTORY_DIR  "./history"
#define SBUF_SIZE    (64 * 1024)
#define RBUF_SIZE    (HEADER_SIZE + MAX_PAYLOAD_LEN + 16)

/*  per-client send buffer   */
typedef struct { char buf[SBUF_SIZE]; int head, tail; } SendBuf;

static int  sb_empty(SendBuf *sb)                  { return sb->head == sb->tail; }
static int  sb_push(SendBuf *sb, const char *d, int n) {
    if (sb->tail + n > SBUF_SIZE) {               
        int used = sb->tail - sb->head;
        memmove(sb->buf, sb->buf + sb->head, used);
        sb->head = 0; sb->tail = used;
    }
    if (sb->tail + n > SBUF_SIZE) return -1;      
    memcpy(sb->buf + sb->tail, d, n); sb->tail += n; return 0;
}
/* flush: 0=done, 1=partial(EAGAIN), -1=error */
static int  sb_flush(SendBuf *sb, int fd) {
    while (sb->head < sb->tail) {
        int n = write(fd, sb->buf + sb->head, sb->tail - sb->head);
        if (n > 0)  { sb->head += n; continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 1;
        return -1;
    }
    sb->head = sb->tail = 0; return 0;
}

/*  client slot  */
typedef struct {
    int    fd;
    int    active, in_use, status;
    char   username[MAX_USERNAME_LEN];
    char   ip[INET_ADDRSTRLEN];
    char   rbuf[RBUF_SIZE]; int rbuf_len;
    SendBuf sbuf;
} Client;

static Client clients[MAX_CLIENTS];
static int    epfd = -1;
static char   disc_host[64] = "";
static int    disc_port = 0;
static volatile int running = 1;

/* helpers  */

static void set_nb(int fd) { fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK); }
static void epmod(int fd, uint32_t ev) {
    struct epoll_event e; e.events = ev; e.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}
static int find_by_fd(int fd) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].in_use && clients[i].fd == fd) return i;
    return -1;
}
static int find_by_user(const char *u) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].active && !strncmp(clients[i].username, u, MAX_USERNAME_LEN)) return i;
    return -1;
}
static int find_free(void) {
    for (int i = 0; i < MAX_CLIENTS; i++) if (!clients[i].in_use) return i;
    return -1;
}

/* queue message into send buffer*/
static void slot_send(int s, const Message *m) {
    char wire[HEADER_SIZE + MAX_PAYLOAD_LEN];
    int  len = pack_message(m, wire);
    if (len <= 0) return;
    Client *c = &clients[s];
    if (sb_empty(&c->sbuf)) {
        int sent = 0;
        while (sent < len) {
            int n = write(c->fd, wire + sent, len - sent);
            if (n > 0) { sent += n; continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            return;
        }
        if (sent == len) return;
        sb_push(&c->sbuf, wire + sent, len - sent);
    } else {
        sb_push(&c->sbuf, wire, len);
    }
    epmod(c->fd, EPOLLIN | EPOLLOUT | EPOLLET);
}

static void bcast_note(int skip, const char *note) {
    Message m; make_message(&m, MSG_BROADCAST, "server", "", note);
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (i != skip && clients[i].active) slot_send(i, &m);
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

static void disconnect_client(int s) {
    Client *c = &clients[s]; if (!c->in_use) return;
    printf("[EPOLL] Disconnect: %s fd=%d\n", c->username, c->fd);
    if (c->active) {
        char note[64]; snprintf(note, sizeof(note), "%s disconnected", c->username);
        bcast_note(s, note);
    }
    epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd); memset(c, 0, sizeof(Client));
}

/*  message processor  */

static void process_message(int s, Message *msg) {
    Client  *c = &clients[s];
    Message  resp;
    const char *sn[] = {"available", "busy", "away"};

    switch (msg->type) {

    case MSG_LOGIN: {
        char u[MAX_USERNAME_LEN]={0}, p[MAX_PASSWORD_LEN]={0};
        if (sscanf(msg->payload, "%31[^:]:%63s", u, p) != 2) {
            make_message(&resp, MSG_ERROR, "server", "", "Bad login format");
            resp.status = 1; slot_send(s, &resp); return;
        }
        if (find_by_user(u) != -1) {
            make_message(&resp, MSG_ERROR, "server", u, "Already logged in");
            resp.status = 1; slot_send(s, &resp); return;
        }
        if (validate(u, p) != 0) {
            make_message(&resp, MSG_ERROR, "server", u, "Invalid credentials");
            resp.status = 1; slot_send(s, &resp); return;
        }
        strncpy(c->username, u, MAX_USERNAME_LEN - 1);
        c->active = 1; c->status = STATUS_AVAILABLE;
        printf("[EPOLL] Login: %s slot=%d\n", u, s);
        make_message(&resp, MSG_ACK, "server", u, "Login successful");
        slot_send(s, &resp); break;
    }

    case MSG_LOGOUT: {
        char note[64]; snprintf(note, sizeof(note), "%s left", c->username);
        printf("[EPOLL] Logout: %s\n", c->username);
        c->active = 0; memset(c->username, 0, MAX_USERNAME_LEN);
        bcast_note(s, note);
        disconnect_client(s); break;
    }

    case MSG_BROADCAST:
        if (!c->active) {
            make_message(&resp, MSG_ERROR, "server", "", "Not authenticated");
            resp.status = 1; slot_send(s, &resp); return;
        }
        printf("[EPOLL] Broadcast %s: %s\n", msg->sender, msg->payload);
        save_history(msg->sender, "", msg->payload, (time_t)msg->timestamp);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (i != s && clients[i].active) slot_send(i, msg);
        make_message(&resp, MSG_ACK, "server", msg->sender, "");
        slot_send(s, &resp); break;

    case MSG_PRIVATE: {
        if (!c->active) {
            make_message(&resp, MSG_ERROR, "server", "", "Not authenticated");
            resp.status = 1; slot_send(s, &resp); return;
        }
        int t = find_by_user(msg->receiver);
        if (t == -1) {
            make_message(&resp, MSG_ERROR, "server", msg->sender, "User not online");
            resp.status = 1; slot_send(s, &resp); return;
        }
        printf("[EPOLL] Private %s->%s: %s\n", msg->sender, msg->receiver, msg->payload);
        save_history(msg->sender, msg->receiver, msg->payload, (time_t)msg->timestamp);
        slot_send(t, msg);
        make_message(&resp, MSG_ACK, "server", msg->sender, "");
        slot_send(s, &resp); break;
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
        slot_send(s, &resp); break;
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
        slot_send(s, &resp); break;
    }

    case MSG_STATUS_CHANGE:
        c->status = strncmp(msg->payload,"busy",4)==0 ? STATUS_BUSY :
                    strncmp(msg->payload,"away",4)==0 ? STATUS_AWAY : STATUS_AVAILABLE;
        make_message(&resp, MSG_ACK, "server", msg->sender, "Status updated");
        slot_send(s, &resp); break;

    default:
        make_message(&resp, MSG_ERROR, "server", msg->sender, "Unknown type");
        resp.status = 1; slot_send(s, &resp); break;
    }
}

/*  I/O handlers  */

static void handle_read(int fd) {
    int s = find_by_fd(fd); if (s == -1) return;
    Client *c = &clients[s];

    while (1) {
        int room = RBUF_SIZE - c->rbuf_len;
        if (room <= 0) { disconnect_client(s); return; }
        int n = read(fd, c->rbuf + c->rbuf_len, room);
        if (n > 0)       { c->rbuf_len += n; continue; }
        if (n == 0)      { disconnect_client(s); return; }
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        disconnect_client(s); return;
    }

    /* process all complete messages from accumulation buffer */
    while (c->rbuf_len >= HEADER_SIZE) {
        uint16_t plen = (uint16_t)(((unsigned char)c->rbuf[74] << 8) |
                                    (unsigned char)c->rbuf[75]);
        if (plen > MAX_PAYLOAD_LEN) { disconnect_client(s); return; }
        int total = HEADER_SIZE + plen;
        if (c->rbuf_len < total) break;
        Message msg;
        if (unpack_message(c->rbuf, total, &msg) == 0)
            process_message(s, &msg);
        c->rbuf_len -= total;
        if (c->rbuf_len > 0) memmove(c->rbuf, c->rbuf + total, c->rbuf_len);
    }
}

static void handle_write(int fd) {
    int s = find_by_fd(fd); if (s == -1) return;
    Client *c = &clients[s];
    int ret = sb_flush(&c->sbuf, fd);
    if (ret < 0)  { disconnect_client(s); return; }
    if (ret == 0) { epmod(fd, EPOLLIN | EPOLLET); }  
}

/*  main  */

static void sig_handler(int s) { (void)s; running = 0; }

int main(int argc, char *argv[]) {
    int port = CHAT_PORT_EPOLL;
    if (argc >= 2) port = atoi(argv[1]);
    if (argc >= 4) {
        strncpy(disc_host, argv[2], sizeof(disc_host) - 1);
        disc_port = atoi(argv[3]);
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    memset(clients, 0, sizeof(clients));
    struct stat st; if (stat(HISTORY_DIR, &st) != 0) mkdir(HISTORY_DIR, 0755);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); exit(1); }
    int opt = 1; setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nb(srv);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(srv, MAX_CLIENTS * 2) < 0) { perror("listen"); exit(1); }

    epfd = epoll_create1(0); if (epfd < 0) { perror("epoll_create1"); exit(1); }
    struct epoll_event ev = { .events = EPOLLIN | EPOLLET, .data.fd = srv };
    epoll_ctl(epfd, EPOLL_CTL_ADD, srv, &ev);
    printf("[EPOLL SERVER] Listening on port %d\n", port);

    struct epoll_event events[MAX_CLIENTS + 4];
    while (running) {
        int nev = epoll_wait(epfd, events, MAX_CLIENTS + 4, 1000);
        if (nev < 0) { if (errno == EINTR) continue; perror("epoll_wait"); break; }

        for (int i = 0; i < nev; i++) {
            int      fd = events[i].data.fd;
            uint32_t e  = events[i].events;

            if (fd == srv) {
                /* accept all pending connections */
                while (1) {
                    struct sockaddr_in peer; socklen_t plen = sizeof(peer);
                    int cfd = accept(srv, (struct sockaddr *)&peer, &plen);
                    if (cfd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept"); break;
                    }
                    int slot = find_free();
                    if (slot == -1) {
                        Message err; make_message(&err, MSG_ERROR, "server", "", "Server full");
                        err.status = 1; send_message(cfd, &err); close(cfd);
                        printf("[EPOLL] Rejected: server full\n"); continue;
                    }
                    set_nb(cfd);
                    struct epoll_event cev = { .events = EPOLLIN | EPOLLET, .data.fd = cfd };
                    epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev);
                    Client *c = &clients[slot];
                    memset(c, 0, sizeof(Client));
                    c->fd = cfd; c->in_use = 1;
                    inet_ntop(AF_INET, &peer.sin_addr, c->ip, INET_ADDRSTRLEN);
                    printf("[EPOLL] New connection from %s slot=%d fd=%d\n", c->ip, slot, cfd);
                }
                continue;
            }

            if (e & (EPOLLERR | EPOLLHUP)) {
                int slot = find_by_fd(fd); if (slot != -1) disconnect_client(slot);
                continue;
            }
            if (e & EPOLLIN)  handle_read(fd);
            if (e & EPOLLOUT) handle_write(fd);
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++) if (clients[i].in_use) close(clients[i].fd);
    close(srv); close(epfd);
    printf("[EPOLL SERVER] Shut down.\n");
    return 0;
}
