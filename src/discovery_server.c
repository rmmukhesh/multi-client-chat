#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <signal.h>
#include "protocol.h"

#define MAX_USERS 128

typedef struct {
    char username[MAX_USERNAME_LEN];
    char password[MAX_PASSWORD_LEN];
    char ip[INET_ADDRSTRLEN];
    int  port, active;
} UserRecord;

static UserRecord      registry[MAX_USERS];
static pthread_mutex_t reg_lock = PTHREAD_MUTEX_INITIALIZER;
static int             server_fd = -1;
static volatile int    running = 1;

/*  registry helpers */

/* caller must hold reg_lock */
static int find_user(const char *u) {
    for (int i = 0; i < MAX_USERS; i++)
        if (registry[i].active && !strncmp(registry[i].username, u, MAX_USERNAME_LEN))
            return i;
    return -1;
}

static int register_user(const char *u, const char *p, const char *ip, int port) {
    pthread_mutex_lock(&reg_lock);
    int idx = find_user(u);
    if (idx == -1) {                      /* new user — find free slot */
        for (idx = 0; idx < MAX_USERS; idx++) if (!registry[idx].active) break;
        if (idx == MAX_USERS) { pthread_mutex_unlock(&reg_lock); return -1; }
    }
    registry[idx].active = 1; registry[idx].port = port;
    strncpy(registry[idx].username, u,  MAX_USERNAME_LEN - 1);
    strncpy(registry[idx].password, p,  MAX_PASSWORD_LEN - 1);
    strncpy(registry[idx].ip,       ip, INET_ADDRSTRLEN  - 1);
    pthread_mutex_unlock(&reg_lock);
    printf("[DISCOVERY] Registered user='%s' ip=%s port=%d\n", u, ip, port);
    return 0;
}

/*  per-connection thread  */

typedef struct { int sockfd; char peer_ip[INET_ADDRSTRLEN]; } ConnArgs;

static void *handle_connection(void *arg) {
    ConnArgs *ca = (ConnArgs *)arg;
    int  fd = ca->sockfd;
    char peer_ip[INET_ADDRSTRLEN];
    strncpy(peer_ip, ca->peer_ip, INET_ADDRSTRLEN - 1);
    free(ca);

    pthread_detach(pthread_self());
    sigset_t mask; sigemptyset(&mask);
    sigaddset(&mask, SIGINT); sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    Message req, resp;
    if (recv_message(fd, &req) != 0) { close(fd); return NULL; }
    memset(&resp, 0, sizeof(resp));
    resp.timestamp = req.timestamp;

    if (req.type == MSG_REGISTER) {
        char u[MAX_USERNAME_LEN]={0}, p[MAX_PASSWORD_LEN]={0}; int port=0;
        if (sscanf(req.payload, "%31[^:]:%63[^:]:%d", u, p, &port) != 3) {
            make_message(&resp, MSG_ERROR, "discovery", req.sender, "Bad payload: need user:pass:port");
            resp.status = 1;
        } else if (register_user(u, p, peer_ip, port) != 0) {
            make_message(&resp, MSG_ERROR, "discovery", req.sender, "Registry full");
            resp.status = 1;
        } else {
            make_message(&resp, MSG_ACK, "discovery", req.sender, "Registered OK");
        }

    } else if (req.type == MSG_DISCOVER) {
        char target[MAX_USERNAME_LEN] = {0};
        strncpy(target, req.payload, MAX_USERNAME_LEN - 1);
        pthread_mutex_lock(&reg_lock);
        int idx = find_user(target);
        if (idx == -1) {
            pthread_mutex_unlock(&reg_lock);
            make_message(&resp, MSG_ERROR, "discovery", req.sender, "User not found");
            resp.status = 1;
        } else {
            char result[64]; snprintf(result, sizeof(result), "%s:%d", registry[idx].ip, registry[idx].port);
            pthread_mutex_unlock(&reg_lock);
            make_message(&resp, MSG_DISCOVER_RESP, "discovery", req.sender, result);
            printf("[DISCOVERY] Lookup '%s' -> %s\n", target, result);
        }

    } else if (req.type == MSG_VALIDATE) {
        char u[MAX_USERNAME_LEN]={0}, p[MAX_PASSWORD_LEN]={0};
        sscanf(req.payload, "%31[^:]:%63s", u, p);
        pthread_mutex_lock(&reg_lock);
        int idx = find_user(u);
        int ok  = (idx != -1 && !strncmp(registry[idx].password, p, MAX_PASSWORD_LEN));
        pthread_mutex_unlock(&reg_lock);
        make_message(&resp, ok ? MSG_ACK : MSG_ERROR, "discovery", req.sender,
                     ok ? "Valid" : "Invalid credentials");
        resp.status = ok ? 0 : 1;

    } else {
        make_message(&resp, MSG_ERROR, "discovery", req.sender, "Unknown message type");
        resp.status = 1;
    }

    send_message(fd, &resp);
    close(fd);
    return NULL;
}

/*  signal handler  */

static void sig_handler(int sig) {
    (void)sig; running = 0;
    if (server_fd >= 0) { close(server_fd); server_fd = -1; }
}

/* main */

int main(int argc, char *argv[]) {
    int port = DISCOVERY_PORT;
    if (argc >= 2) port = atoi(argv[1]);

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    memset(registry, 0, sizeof(registry));

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(1); }
    int opt = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(server_fd, 32) < 0) { perror("listen"); exit(1); }
    printf("[DISCOVERY] Listening on port %d\n", port);

    while (running) {
        struct sockaddr_in peer; socklen_t plen = sizeof(peer);
        int cfd = accept(server_fd, (struct sockaddr *)&peer, &plen);
        if (cfd < 0) { if (!running || errno == EINTR) continue; perror("accept"); continue; }

        ConnArgs *ca = malloc(sizeof(ConnArgs));
        if (!ca) { close(cfd); continue; }
        ca->sockfd = cfd;
        inet_ntop(AF_INET, &peer.sin_addr, ca->peer_ip, INET_ADDRSTRLEN);

        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_connection, ca) != 0) {
            perror("pthread_create"); free(ca); close(cfd);
        }
    }

    printf("[DISCOVERY] Shut down.\n");
    return 0;
}
