#ifndef CLIENT_INCLUDED
#define CLIENT_INCLUDED

#include <lib/tcp.h>
#include <lib/basic.h>

#include "config.h"

typedef struct {

    TCP tcp;

    // True if we are waiting for a response
    bool pending;

    Address server_addrs[NODE_LIMIT];
    int num_servers;

    uint64_t client_id;
    uint64_t request_id;

    int current_leader;

    Time last_request_time;

} ClientState;

struct pollfd;

int client_init(void *state, int argc, char **argv,
    void **ctxs, struct pollfd *pdata, int pcap, int *pnum,
    int *timeout);

int client_tick(void *state, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout);

int client_free(void *state);

#endif // CLIENT_INCLUDED
