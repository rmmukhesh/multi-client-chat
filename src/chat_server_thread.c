#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "protocol.h"

#define HISTORY_DIR "./history"

typedef struct {
    int      sockfd;
    char     username[MAX_USERNAME_LEN];
    int      active, in_use, status;
    pthread_t       thread;
    pthread_mutex_t send_lock;
} Client;

static Client          clients[MAX_CLIENTS];
static pthread_mutex_t table_lock = PTHREAD_MUTEX_INITIALIZER;
static char  disc_host[64] = "";
static int   disc_port = 0;
static volatile int running = 1;

/*  helpers  */

static int find_user(const char *u) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].active && !strncmp(clients[i].username, u, MAX_USERNAME_LEN))
            return i;
    return -1;
}

static int find_free(void) {
    for (int i = 0; i < MAX_CLIENTS; i++) if (!clients[i].in_use) return i;
    return -1;
}

static void slot_send(int s, Message *m) {
    pthread_mutex_lock(&clients[s].send_lock);
    send_message(clients[s].sockfd, m);
    pthread_mutex_unlock(&clients[s].send_lock);
}

static void bcast_note(int skip, const char *note) {
    Message m; make_message(&m, MSG_BROADCAST, "server", "", note);
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (i != skip && clients[i].active) slot_send(i, &m);
}

/* validate credentials via discovery server; returns 0=ok, -1=reject */
static int validate(const char *u, const char *p) {
    if (!disc_host[0]) return 0;   /* no discovery server — allow all */
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
    if (to && to[0]) fprintf(f, "[%s] -> %s : %s\n",       tb, to, text);
    else             fprintf(f, "[%s] BROADCAST : %s\n", tb, text);
    fclose(f);
}

/*  message router (runs in each client's thread) */

static void route(int s, Message *msg) {
    Message resp;
    const char *sn[] = {"available", "busy", "away"};

    switch (msg->type) {

    case MSG_LOGIN: {
        char u[MAX_USERNAME_LEN]={0}, p[MAX_PASSWORD_LEN]={0};
        if (sscanf(msg->payload, "%31[^:]:%63s", u, p) != 2) {
            make_message(&resp, MSG_ERROR, "server", "", "Bad login format");
            resp.status = 1; slot_send(s, &resp); return;
        }
        pthread_mutex_lock(&table_lock);
        if (find_user(u) != -1) {
            pthread_mutex_unlock(&table_lock);
            make_message(&resp, MSG_ERROR, "server", u, "Already logged in");
            resp.status = 1; slot_send(s, &resp); return;
        }
        if (validate(u, p) != 0) {
            pthread_mutex_unlock(&table_lock);
            make_message(&resp, MSG_ERROR, "server", u, "Invalid credentials");
            resp.status = 1; slot_send(s, &resp); return;
        }
        strncpy(clients[s].username, u, MAX_USERNAME_LEN - 1);
        clients[s].active = 1; clients[s].status = STATUS_AVAILABLE;
        pthread_mutex_unlock(&table_lock);
        printf("[THREAD] Login: %s slot=%d\n", u, s);
        make_message(&resp, MSG_ACK, "server", u, "Login successful");
        slot_send(s, &resp); break;
    }

    case MSG_LOGOUT: {
        char note[64]; snprintf(note, sizeof(note), "%s left", clients[s].username);
        printf("[THREAD] Logout: %s\n", clients[s].username);
        pthread_mutex_lock(&table_lock);
        clients[s].active = 0; memset(clients[s].username, 0, MAX_USERNAME_LEN);
        pthread_mutex_unlock(&table_lock);
        bcast_note(s, note); break;
    }

    case MSG_BROADCAST:
        if (!clients[s].active) {
            make_message(&resp, MSG_ERROR, "server", "", "Not authenticated");
            resp.status = 1; slot_send(s, &resp); return;
        }
        printf("[THREAD] Broadcast %s: %s\n", msg->sender, msg->payload);
        save_history(msg->sender, "", msg->payload, (time_t)msg->timestamp);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (i != s && clients[i].active) slot_send(i, msg);
        make_message(&resp, MSG_ACK, "server", msg->sender, "");
        slot_send(s, &resp); break;

    case MSG_PRIVATE: {
        if (!clients[s].active) {
            make_message(&resp, MSG_ERROR, "server", "", "Not authenticated");
            resp.status = 1; slot_send(s, &resp); return;
        }
        pthread_mutex_lock(&table_lock);
        int t = find_user(msg->receiver);
        pthread_mutex_unlock(&table_lock);
        if (t == -1) {
            make_message(&resp, MSG_ERROR, "server", msg->sender, "User not online");
            resp.status = 1; slot_send(s, &resp); return;
        }
        printf("[THREAD] Private %s->%s: %s\n", msg->sender, msg->receiver, msg->payload);
        save_history(msg->sender, msg->receiver, msg->payload, (time_t)msg->timestamp);
        slot_send(t, msg);
        make_message(&resp, MSG_ACK, "server", msg->sender, "");
        slot_send(s, &resp); break;
    }

    case MSG_LIST_USERS: {
        char list[MAX_PAYLOAD_LEN] = {0}; int first = 1;
        pthread_mutex_lock(&table_lock);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active || !clients[i].username[0]) continue;
            if (!first) strncat(list, ", ", MAX_PAYLOAD_LEN - strlen(list) - 1);
            char e[64]; snprintf(e, sizeof(e), "%s(%s)", clients[i].username, sn[clients[i].status]);
            strncat(list, e, MAX_PAYLOAD_LEN - strlen(list) - 1); first = 0;
        }
        pthread_mutex_unlock(&table_lock);
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
        pthread_mutex_lock(&table_lock);
        clients[s].status = strncmp(msg->payload,"busy",4)==0 ? STATUS_BUSY :
                            strncmp(msg->payload,"away",4)==0 ? STATUS_AWAY : STATUS_AVAILABLE;
        pthread_mutex_unlock(&table_lock);
        make_message(&resp, MSG_ACK, "server", msg->sender, "Status updated");
        slot_send(s, &resp); break;

    default:
        make_message(&resp, MSG_ERROR, "server", msg->sender, "Unknown type");
        resp.status = 1; slot_send(s, &resp); break;
    }
}

/*  per-client thread  */

static void *client_thread(void *arg) {
    int s = *(int *)arg; free(arg);
    pthread_detach(pthread_self());
    printf("[THREAD] Started slot=%d fd=%d\n", s, clients[s].sockfd);

    Message msg;
    while (recv_message(clients[s].sockfd, &msg) == 0) {
        route(s, &msg);
        if (msg.type == MSG_LOGOUT) break;
    }

    /* clean up on unexpected disconnect (recv returned error) */
    pthread_mutex_lock(&table_lock);
    int was_active = clients[s].active;
    char uname[MAX_USERNAME_LEN];
    strncpy(uname, clients[s].username, MAX_USERNAME_LEN - 1);
    clients[s].active = clients[s].in_use = 0;
    memset(clients[s].username, 0, MAX_USERNAME_LEN);
    pthread_mutex_unlock(&table_lock);

    if (was_active) {
        char note[64]; snprintf(note, sizeof(note), "%s disconnected", uname);
        bcast_note(s, note);
    }
    close(clients[s].sockfd);
    printf("[THREAD] Exiting slot=%d user=%s\n", s, uname);
    return NULL;
}

/* main  */

static void sig_handler(int s) { (void)s; running = 0; }

int main(int argc, char *argv[]) {
    int port = CHAT_PORT_THREAD;
    if (argc >= 2) port = atoi(argv[1]);
    if (argc >= 4) {
        strncpy(disc_host, argv[2], sizeof(disc_host) - 1);
        disc_port = atoi(argv[3]);
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    for (int i = 0; i < MAX_CLIENTS; i++) {
        memset(&clients[i], 0, sizeof(Client));
        pthread_mutex_init(&clients[i].send_lock, NULL);
    }
    struct stat st; if (stat(HISTORY_DIR, &st) != 0) mkdir(HISTORY_DIR, 0755);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); exit(1); }
    int opt = 1; setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(srv, MAX_CLIENTS) < 0) { perror("listen"); exit(1); }
    printf("[THREAD SERVER] Listening on port %d\n", port);

    while (running) {
        fd_set rfds; FD_ZERO(&rfds); FD_SET(srv, &rfds);
        struct timeval tv = {1, 0};
        if (select(srv + 1, &rfds, NULL, NULL, &tv) <= 0) continue;

        struct sockaddr_in peer; socklen_t plen = sizeof(peer);
        int cfd = accept(srv, (struct sockaddr *)&peer, &plen);
        if (cfd < 0) { if (errno == EINTR) continue; perror("accept"); continue; }

        pthread_mutex_lock(&table_lock);
        int slot = find_free();
        if (slot == -1) {
            pthread_mutex_unlock(&table_lock);
            Message err; make_message(&err, MSG_ERROR, "server", "", "Server full");
            err.status = 1; send_message(cfd, &err); close(cfd);
            printf("[THREAD] Rejected: server full\n"); continue;
        }
        clients[slot].sockfd  = cfd;
        clients[slot].in_use  = 1;
        clients[slot].active  = 0;
        clients[slot].status  = STATUS_AVAILABLE;
        memset(clients[slot].username, 0, MAX_USERNAME_LEN);
        pthread_mutex_unlock(&table_lock);

        int *arg = malloc(sizeof(int)); *arg = slot;
        if (pthread_create(&clients[slot].thread, NULL, client_thread, arg) != 0) {
            perror("pthread_create"); free(arg);
            clients[slot].in_use = 0; close(cfd); continue;
        }
        printf("[THREAD] New connection slot=%d\n", slot);
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].in_use) close(clients[i].sockfd);
        pthread_mutex_destroy(&clients[i].send_lock);
    }
    close(srv);
    printf("[THREAD SERVER] Shut down.\n");
    return 0;
}
