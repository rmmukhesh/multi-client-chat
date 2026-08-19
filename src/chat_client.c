#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "protocol.h"

#define GRN "\033[92m"
#define CYN "\033[96m"
#define YLW "\033[93m"
#define RED "\033[91m"
#define BLD "\033[1m"
#define RST "\033[0m"

static int          server_fd = -1;
static char         my_username[MAX_USERNAME_LEN] = {0};
static char         my_password[MAX_PASSWORD_LEN] = {0};
static volatile int running = 1;



static int tcp_connect(const char *host, int port) {
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        struct hostent *he = gethostbyname(host);
        if (!he) { fprintf(stderr, "Cannot resolve: %s\n", host); return -1; }
        memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0); if (fd < 0) { perror("socket"); return -1; }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect"); close(fd); return -1;
    }
    return fd;
}

/* build + send a message to the chat server */
static void send_cmd(msg_type_t type, const char *to, const char *payload) {
    Message m; make_message(&m, type, my_username, to, payload);
    send_message(server_fd, &m);
}

/* registration / login  */

static int register_with_discovery(const char *host, int port, int chat_port) {
    int fd = tcp_connect(host, port);
    if (fd < 0) { fprintf(stderr, RED "Cannot connect to discovery %s:%d\n" RST, host, port); return -1; }
    char pl[MAX_PAYLOAD_LEN];
    snprintf(pl, sizeof(pl), "%s:%s:%d", my_username, my_password, chat_port);
    Message req, resp; make_message(&req, MSG_REGISTER, my_username, "discovery", pl);
    send_message(fd, &req);
    int ok = recv_message(fd, &resp);
    close(fd);
    if (ok != 0 || resp.status != 0) {
        fprintf(stderr, RED "Registration failed: %s\n" RST, ok ? "no response" : resp.payload);
        return -1;
    }
    printf(GRN "[✓] Registered with discovery server\n" RST);
    return 0;
}

static int login_to_chat(const char *host, int port) {
    server_fd = tcp_connect(host, port);
    if (server_fd < 0) { fprintf(stderr, RED "Cannot connect to chat server %s:%d\n" RST, host, port); return -1; }
    char pl[MAX_PAYLOAD_LEN];
    snprintf(pl, sizeof(pl), "%s:%s", my_username, my_password);
    Message req, resp; make_message(&req, MSG_LOGIN, my_username, "", pl);
    send_message(server_fd, &req);
    if (recv_message(server_fd, &resp) != 0 || resp.status != 0 || resp.type == MSG_ERROR) {
        fprintf(stderr, RED "Login failed: %s\n" RST, resp.payload);
        close(server_fd); server_fd = -1; return -1;
    }
    printf(GRN "[✓] Logged in as '%s'\n" RST, my_username);
    return 0;
}

/* receiver thread  */

static void print_message(const Message *msg) {
    time_t ts = (time_t)msg->timestamp; char tb[20];
    strftime(tb, sizeof(tb), "%H:%M:%S", localtime(&ts));
    switch (msg->type) {
    case MSG_BROADCAST:
        printf("\n" CYN "[%s] " BLD "%s" RST CYN " (broadcast): " RST "%s\n> ", tb, msg->sender, msg->payload); break;
    case MSG_PRIVATE:
        printf("\n" YLW "[%s] " BLD "%s" RST YLW " (private): " RST "%s\n> ", tb, msg->sender, msg->payload); break;
    case MSG_USER_LIST:
        printf("\n" GRN "Online: " BLD "%s" RST "\n> ", msg->payload); break;
    case MSG_HISTORY_RESP:
        printf("\n" CYN "--- history ---\n%s--- end ---\n" RST "> ", msg->payload); break;
    case MSG_ACK:
        if (msg->payload[0]) printf("\n" GRN "[server]: %s\n" RST "> ", msg->payload);
        fflush(stdout); return;
    case MSG_ERROR:
        printf("\n" RED "[error]: %s\n" RST "> ", msg->payload); break;
    default:
        printf("\n[?] type=0x%02X %s: %s\n> ", msg->type, msg->sender, msg->payload); break;
    }
    fflush(stdout);
}

static void *receiver_thread(void *arg) {
    (void)arg;
    Message msg;
    while (running) {
        if (recv_message(server_fd, &msg) != 0) {
            if (running) { printf(RED "\n[!] Disconnected.\n" RST); running = 0; }
            break;
        }
        print_message(&msg);
    }
    return NULL;
}

/*  input loop  */

static void print_help(void) {
    printf(BLD "\nCommands:\n" RST
           "  /broadcast <msg>              Send to all\n"
           "  /msg <user> <msg>             Private message\n"
           "  /list                         Online users\n"
           "  /history                      Your chat history\n"
           "  /status <available|busy|away> Change status\n"
           "  /quit                         Exit\n"
           "  /help                         This help\n\n");
}

static void input_loop(void) {
    char line[MAX_PAYLOAD_LEN + 64];
    print_help(); printf("> "); fflush(stdout);

    while (running && fgets(line, sizeof(line), stdin)) {
        /* strip newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) line[--len] = '\0';
        if (len == 0) { printf("> "); fflush(stdout); continue; }

        if      (strncmp(line, "/broadcast ", 11) == 0) {
            send_cmd(MSG_BROADCAST, "", line + 11);
        } else if (strncmp(line, "/msg ", 5) == 0) {
            char *rest = line + 5, *sp = strchr(rest, ' ');
            if (!sp) { printf(RED "Usage: /msg <user> <msg>\n" RST); }
            else     { *sp = '\0'; send_cmd(MSG_PRIVATE, rest, sp + 1); }
        } else if (strcmp(line, "/list")    == 0) {
            send_cmd(MSG_LIST_USERS, "", "");
        } else if (strcmp(line, "/history") == 0) {
            send_cmd(MSG_HISTORY_REQ, "", "");
        } else if (strncmp(line, "/status ", 8) == 0) {
            const char *s = line + 8;
            if (strcmp(s,"available")!=0 && strcmp(s,"busy")!=0 && strcmp(s,"away")!=0)
                printf(RED "Usage: /status <available|busy|away>\n" RST);
            else send_cmd(MSG_STATUS_CHANGE, "", s);
        } else if (strcmp(line, "/quit")   == 0) {
            send_cmd(MSG_LOGOUT, "", "Bye"); running = 0; break;
        } else if (strcmp(line, "/help")   == 0) {
            print_help();
        } else {
            printf(RED "Unknown command. /help for usage.\n" RST);
        }
        if (running) { printf("> "); fflush(stdout); }
    }
}

/* signal handler  */

static void sig_handler(int s) {
    (void)s;
    if (running && server_fd >= 0) send_cmd(MSG_LOGOUT, "", "Bye");
    running = 0;
}

/*  main  */

int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <disc_host> <disc_port> <chat_host> <chat_port>\n"
                        "  e.g. %s 127.0.0.1 9000 127.0.0.1 9001\n", argv[0], argv[0]);
        return 1;
    }
    const char *disc_host = argv[1]; int disc_port = atoi(argv[2]);
    const char *chat_host = argv[3]; int chat_port = atoi(argv[4]);

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    printf(BLD "\n=== Chat Client ===\n" RST);
    printf("  1. Register (new user)\n  2. Login\nChoice: ");
    int choice = 0; scanf("%d", &choice); getchar();
    printf("Username: "); fgets(my_username, MAX_USERNAME_LEN, stdin);
    my_username[strcspn(my_username, "\n")] = '\0';
    char *pw = getpass("Password: ");
    strncpy(my_password, pw, MAX_PASSWORD_LEN - 1);
    printf(BLD "\n=== Connected as %s ===\n\n" RST, my_username);

    if (choice == 1 && register_with_discovery(disc_host, disc_port, chat_port) != 0) exit(1);
    if (login_to_chat(chat_host, chat_port) != 0) exit(1);

    pthread_t recv_tid;
    if (pthread_create(&recv_tid, NULL, receiver_thread, NULL) != 0) { perror("pthread_create"); exit(1); }

    input_loop();

    running = 0;
    shutdown(server_fd, SHUT_RDWR);
    pthread_join(recv_tid, NULL);
    close(server_fd);
    printf(GRN "\n[✓] Goodbye, %s!\n" RST, my_username);
    return 0;
}
