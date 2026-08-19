/*
 * protocol.h
 * Custom application-layer protocol for the Multi-Client Chat System.
 *
 * All communication between client <-> discovery_server and
 * client <-> chat_server uses this fixed-header message format.
 *
 * Wire format (all fields network byte order where applicable):
 *  [1 byte  msg_type ]
 *  [1 byte  status   ]  -- used in responses (0 = OK, non-zero = error)
 *  [32 bytes sender  ]  -- null-terminated username
 *  [32 bytes receiver]  -- null-terminated username; empty = broadcast
 *  [8 bytes timestamp]  -- Unix epoch seconds (uint64_t, big-endian)
 *  [2 bytes payload_len]-- length of payload that follows (uint16_t, big-endian)
 *  [N bytes payload  ]  -- variable-length, max MAX_PAYLOAD_LEN bytes
 *
 * Total fixed header size: 1+1+32+32+8+2 = 76 bytes
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <time.h>

//constants
#define MAX_USERNAME_LEN   32
#define MAX_PASSWORD_LEN   64
#define MAX_PAYLOAD_LEN    1024
#define HEADER_SIZE        76   /* fixed header: type+status+sender+receiver+ts+len */

/* Port defaults (can be overridden via CLI) */
#define DISCOVERY_PORT     9000
#define CHAT_PORT_FORK     9001
#define CHAT_PORT_THREAD   9002
#define CHAT_PORT_EPOLL    9003

#define MAX_CLIENTS        10

//message types
typedef enum {
    /* --- Discovery server messages --- */
    MSG_REGISTER        = 0x01,  /* client -> discovery: register username+password+port */
    MSG_DISCOVER        = 0x02,  /* client -> discovery: look up a username             */
    MSG_DISCOVER_RESP   = 0x03,  /* discovery -> client: IP:port of requested user       */
    MSG_VALIDATE        = 0x04,  /* chat server -> discovery: verify username+password   */

    /* --- Chat server: session --- */
    MSG_LOGIN           = 0x10,  /* client -> server: authenticate                      */
    MSG_LOGOUT          = 0x11,  /* client -> server: graceful disconnect                */

    /* --- Chat server: messaging --- */
    MSG_BROADCAST       = 0x20,  /* client -> server -> all clients                     */
    MSG_PRIVATE         = 0x21,  /* client -> server -> specific client                 */

    /* --- Chat server: user management --- */
    MSG_LIST_USERS      = 0x30,  /* client -> server: request online user list          */
    MSG_USER_LIST       = 0x31,  /* server -> client: online user list in payload       */

    /* --- Generic responses --- */
    MSG_ACK             = 0x40,  /* server -> client: success acknowledgement            */
    MSG_ERROR           = 0x41,  /* server -> client: error, payload has description    */

    /* --- Bonus features --- */
    MSG_HISTORY_REQ     = 0x50,  /* client -> server: request own chat history          */
    MSG_HISTORY_RESP    = 0x51,  /* server -> client: history entries in payload        */
    MSG_STATUS_CHANGE   = 0x52,  /* client -> server: change status string              */
} msg_type_t;

//user status
typedef enum {
    STATUS_AVAILABLE = 0,
    STATUS_BUSY      = 1,
    STATUS_AWAY      = 2,
} user_status_t;

//message structure
typedef struct {
    msg_type_t  type;                        /* message type                   */
    uint8_t     status;                      /* 0 = OK, non-zero = error code  */
    char        sender  [MAX_USERNAME_LEN];  /* sender's username              */
    char        receiver[MAX_USERNAME_LEN];  /* target username or "" for bcast*/
    uint64_t    timestamp;                   /* Unix epoch seconds             */
    uint16_t    payload_len;                 /* length of payload[]            */
    char        payload [MAX_PAYLOAD_LEN];   /* message body / data            */
} Message;





/*
 * pack_message()
 * Serialise a Message into a flat byte buffer ready for send().
 * Returns the number of bytes written into `buf`.
 * `buf` must be at least HEADER_SIZE + msg->payload_len bytes.
 */
int pack_message(const Message *msg, char *buf);

/*
 * unpack_message()
 * Deserialise a flat byte buffer received from recv() into a Message.
 * `buf` must contain at least HEADER_SIZE bytes.
 * Returns 0 on success, -1 on error.
 */
int unpack_message(const char *buf, int buf_len, Message *msg);

/*
 * send_message() / recv_message()
 * Convenience wrappers that handle partial send/recv on a TCP socket.
 * Return 0 on success, -1 on error/disconnect.
 */
int send_message(int sockfd, const Message *msg);
int recv_message(int sockfd, Message *msg);

/*
 * make_message()
 * Zero-init a Message and fill common fields in one call.
 */
void make_message(Message *msg,
                  msg_type_t type,
                  const char *sender,
                  const char *receiver,
                  const char *payload);

#endif /* PROTOCOL_H */
