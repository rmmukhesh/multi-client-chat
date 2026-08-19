
#include "protocol.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>   /* htonl / ntohl / htons / ntohs */
#include <errno.h>
#include <time.h>


 // write a uint64 in big-endian order into buf
 
static void write_u64_be(char *buf, uint64_t val)
{
    buf[0] = (char)((val >> 56) & 0xFF);
    buf[1] = (char)((val >> 48) & 0xFF);
    buf[2] = (char)((val >> 40) & 0xFF);
    buf[3] = (char)((val >> 32) & 0xFF);
    buf[4] = (char)((val >> 24) & 0xFF);
    buf[5] = (char)((val >> 16) & 0xFF);
    buf[6] = (char)((val >>  8) & 0xFF);
    buf[7] = (char)( val        & 0xFF);
}

static uint64_t read_u64_be(const char *buf)
{
    return ((uint64_t)(unsigned char)buf[0] << 56) |
           ((uint64_t)(unsigned char)buf[1] << 48) |
           ((uint64_t)(unsigned char)buf[2] << 40) |
           ((uint64_t)(unsigned char)buf[3] << 32) |
           ((uint64_t)(unsigned char)buf[4] << 24) |
           ((uint64_t)(unsigned char)buf[5] << 16) |
           ((uint64_t)(unsigned char)buf[6] <<  8) |
           ((uint64_t)(unsigned char)buf[7]);
}

int pack_message(const Message *msg, char *buf)
{
    if (!msg || !buf) return -1;

    int offset = 0;

    buf[offset++] = (char)msg->type;
    buf[offset++] = (char)msg->status;

    /* sender (32 bytes, null-padded) */
    memset(buf + offset, 0, MAX_USERNAME_LEN);
    strncpy(buf + offset, msg->sender, MAX_USERNAME_LEN - 1);
    offset += MAX_USERNAME_LEN;

    /* receiver (32 bytes, null-padded) */
    memset(buf + offset, 0, MAX_USERNAME_LEN);
    strncpy(buf + offset, msg->receiver, MAX_USERNAME_LEN - 1);
    offset += MAX_USERNAME_LEN;

    /* timestamp (8 bytes, big-endian) */
    write_u64_be(buf + offset, msg->timestamp);
    offset += 8;

    /* payload_len (2 bytes, big-endian) */
    uint16_t plen = (msg->payload_len <= MAX_PAYLOAD_LEN)
                    ? msg->payload_len : MAX_PAYLOAD_LEN;
    buf[offset++] = (char)((plen >> 8) & 0xFF);
    buf[offset++] = (char)( plen       & 0xFF);

    /* payload */
    if (plen > 0)
        memcpy(buf + offset, msg->payload, plen);
    offset += plen;

    return offset; /* total bytes written */
}

int unpack_message(const char *buf, int buf_len, Message *msg)
{
    if (!buf || !msg || buf_len < HEADER_SIZE) return -1;

    memset(msg, 0, sizeof(Message));
    int offset = 0;

    msg->type   = (msg_type_t)(unsigned char)buf[offset++];
    msg->status = (uint8_t)   (unsigned char)buf[offset++];

    memcpy(msg->sender,   buf + offset, MAX_USERNAME_LEN); offset += MAX_USERNAME_LEN;
    memcpy(msg->receiver, buf + offset, MAX_USERNAME_LEN); offset += MAX_USERNAME_LEN;

    msg->sender  [MAX_USERNAME_LEN - 1] = '\0';
    msg->receiver[MAX_USERNAME_LEN - 1] = '\0';

    msg->timestamp = read_u64_be(buf + offset); offset += 8;

    msg->payload_len  = (uint16_t)(((unsigned char)buf[offset] << 8) |
                                    (unsigned char)buf[offset + 1]);
    offset += 2;

    if (msg->payload_len > MAX_PAYLOAD_LEN) return -1;

    if (msg->payload_len > 0) {
        if (buf_len < HEADER_SIZE + msg->payload_len) return -1;
        memcpy(msg->payload, buf + offset, msg->payload_len);
    }

    return 0;
}


int send_message(int sockfd, const Message *msg)
{
    char buf[HEADER_SIZE + MAX_PAYLOAD_LEN];
    int total = pack_message(msg, buf);
    if (total < 0) return -1;

    int sent = 0;
    while (sent < total) {
        int n = write(sockfd, buf + sent, total - sent);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        sent += n;
    }
    return 0;
}


int recv_message(int sockfd, Message *msg)
{
    char header[HEADER_SIZE];
    int received = 0;

    while (received < HEADER_SIZE) {
        int n = read(sockfd, header + received, HEADER_SIZE - received);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        received += n;
    }

    /* Peek at payload_len from the header (bytes 74-75) */
    uint16_t plen = (uint16_t)(((unsigned char)header[74] << 8) |
                                (unsigned char)header[75]);
    if (plen > MAX_PAYLOAD_LEN) return -1;

    /* Assemble full buffer */
    char buf[HEADER_SIZE + MAX_PAYLOAD_LEN];
    memcpy(buf, header, HEADER_SIZE);

    received = 0;
    while (received < (int)plen) {
        int n = read(sockfd, buf + HEADER_SIZE + received, plen - received);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        received += n;
    }

    return unpack_message(buf, HEADER_SIZE + plen, msg);
}


void make_message(Message *msg,
                  msg_type_t type,
                  const char *sender,
                  const char *receiver,
                  const char *payload)
{
    memset(msg, 0, sizeof(Message));
    msg->type      = type;
    msg->timestamp = (uint64_t)time(NULL);

    if (sender)
        strncpy(msg->sender, sender, MAX_USERNAME_LEN - 1);
    if (receiver)
        strncpy(msg->receiver, receiver, MAX_USERNAME_LEN - 1);
    if (payload) {
        size_t plen = strlen(payload);
        if (plen > MAX_PAYLOAD_LEN) plen = MAX_PAYLOAD_LEN;
        memcpy(msg->payload, payload, plen);
        msg->payload_len = (uint16_t)plen;
    }
}
