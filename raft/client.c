#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <stdint.h>
#include <assert.h>

#include "node.h"
#include "client.h"

//#define CLIENT_TRACE(fmt, ...) {}
#define CLIENT_TRACE(fmt, ...) fprintf(stderr, "CLIENT: " fmt "\n", ##__VA_ARGS__);

#define CLIENT_REQUEST_TIMEOUT_SEC 3
#define KEY_POOL_SIZE 128

static uint64_t next_client_id = 1;

static uint64_t client_random(void)
{
#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
    return quakey_random();
#else
    return (uint64_t)rand();
#endif
}

static KVStoreOper random_oper(void)
{
    KVStoreOper oper = {0};
    snprintf(oper.key, KVSTORE_KEY_SIZE, "k%d", (int)(client_random() % KEY_POOL_SIZE));

    switch (client_random() % 3) {
    case 0:
        oper.type = KVSTORE_OPER_SET;
        oper.val  = client_random();
        break;
    case 1:
        oper.type = KVSTORE_OPER_GET;
        break;
    case 2:
        oper.type = KVSTORE_OPER_DEL;
        break;
    }
    return oper;
}

static const char *oper_type_name(KVStoreOperType t)
{
    switch (t) {
    case KVSTORE_OPER_NOOP: return "NOOP";
    case KVSTORE_OPER_SET:  return "SET";
    case KVSTORE_OPER_GET:  return "GET";
    case KVSTORE_OPER_DEL:  return "DEL";
    }
    return "???";
}

static const char *result_type_name(KVStoreResultType t)
{
    switch (t) {
    case KVSTORE_RESULT_OK:      return "OK";
    case KVSTORE_RESULT_FULL:    return "FULL";
    case KVSTORE_RESULT_MISSING: return "MISSING";
    }
    return "???";
}

static int
process_message(ClientState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    (void) conn_idx;

    if (type == MESSAGE_TYPE_REDIRECT) {
        RedirectMessage redirect_message;
        if (msg.len != sizeof(RedirectMessage))
            return -1;
        memcpy(&redirect_message, msg.ptr, sizeof(redirect_message));

        // Ignore stale redirects from previous requests. A redirect
        // from server A for request N-1 can arrive after the client
        // has already moved on to request N with server B. Without
        // this check, the stale redirect would falsely cancel the
        // current request, causing the client and linearizability
        // checker to believe the request was rejected even though
        // the new server may have committed it.
        if (redirect_message.request_id != state->request_id)
            return 0;

        if (redirect_message.leader_idx >= 0 && redirect_message.leader_idx < state->num_servers) {
            CLIENT_TRACE("Redirected to leader %d", redirect_message.leader_idx);
            state->current_leader = redirect_message.leader_idx;
            // Retry immediately with the correct leader.
            // A redirect means the server did not process the request,
            // so mark as rejected (not timeout) for the linearizability
            // checker: the outcome is unambiguous (no effect).
            state->last_was_rejected = true;
            state->last_was_timeout = false;
            state->pending = false;
        }
        return 0;
    }

    if (!state->pending)
        return 0;

    if (type != MESSAGE_TYPE_REPLY)
        return 0;

    ReplyMessage reply_message;
    if (msg.len != sizeof(ReplyMessage))
        return -1;
    memcpy(&reply_message, msg.ptr, sizeof(reply_message));

    // Ignore stale replies from previous requests. After a timeout
    // the client moves to a new leader and sends a new request, but
    // the old leader may still deliver a reply for the old request
    // on the previous connection. Without this check the client
    // would accept the stale result for the wrong operation.
    if (reply_message.request_id != state->request_id)
        return 0;

    CLIENT_TRACE("REPLY: %s key=\"%.16s\" -> %s val=%lu (req_id=%lu)",
        oper_type_name(state->last_oper.type),
        state->last_oper.key,
        result_type_name(reply_message.result.type),
        (unsigned long)reply_message.result.val,
        (unsigned long)state->request_id);

    state->last_result = reply_message.result;
    state->last_was_timeout = false;
    state->last_was_rejected = false;
    state->pending = false;
    return 0;
}

int client_init(void *state_, int argc, char **argv,
    void **ctxs, struct pollfd *pdata, int pcap, int *pnum,
    int *timeout)
{
    ClientState *state = state_;

    state->num_servers = 0;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--server")) {
            i++;
            if (i == argc) {
                fprintf(stderr, "Option --server missing value. Usage is --server <addr>:<port>\n");
                return -1;
            }
            if (state->num_servers == NODE_LIMIT) {
                fprintf(stderr, "Node limit of %d reached\n", NODE_LIMIT);
                return -1;
            }
            if (parse_addr_arg(argv[i], &state->server_addrs[state->num_servers++]) < 0) {
                fprintf(stderr, "Malformed <addr>:<port> pair for --server option\n");
                return -1;
            }
        } else {
            printf("Ignoring option '%s'\n", argv[i]);
        }
    }

    // Now sort the addresses
    addr_sort(state->server_addrs, state->num_servers);

    if (tcp_context_init(&state->tcp) < 0) {
        fprintf(stderr, "Client :: Couldn't setup TCP context\n");
        return -1;
    }

    state->pending = false;
    state->client_id = next_client_id++;
    state->request_id = 0;
    state->current_leader = 0;

    Time now = get_current_time();
    if (now == INVALID_TIME) {
        fprintf(stderr, "Client :: Couldn't get current time\n");
        tcp_context_free(&state->tcp);
        return -1;
    }
    state->last_request_time = now;

    // Connect to all known servers
    for (int i = 0; i < state->num_servers; i++) {
        if (tcp_connect(&state->tcp, state->server_addrs[i], i, NULL) < 0) {
            fprintf(stderr, "Client :: Couldn't connect to server %d\n", i);
            tcp_context_free(&state->tcp);
            return -1;
        }
    }

    *timeout = 0;
    if (pcap < TCP_POLL_CAPACITY) {
        fprintf(stderr, "Client :: Not enough poll() capacity (got %d, needed %d)\n", pcap, TCP_POLL_CAPACITY);
        return -1;
    }
    *pnum = tcp_register_events(&state->tcp, ctxs, pdata);
    return 0;
}

int client_tick(void *state_, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout)
{
    ClientState *state = state_;

    Time now = get_current_time();
    if (now == INVALID_TIME) {
        assert(0);
    }

    Event events[TCP_EVENT_CAPACITY];
    int num_events = tcp_translate_events(&state->tcp, events, ctxs, pdata, *pnum);

    for (int i = 0; i < num_events; i++) {
        if (events[i].type != EVENT_MESSAGE)
            continue;
        int conn_idx = events[i].conn_idx;

        for (;;) {

            ByteView msg;
            uint16_t msg_type;
            int ret = tcp_next_message(&state->tcp, conn_idx, &msg, &msg_type);
            if (ret == 0)
                break;
            if (ret < 0) {
                tcp_close(&state->tcp, conn_idx);
                break;
            }

            ret = process_message(state, conn_idx, msg_type, msg);
            if (ret < 0) {
                tcp_close(&state->tcp, conn_idx);
                break;
            }

            tcp_consume_message(&state->tcp, conn_idx);
        }
    }

    // Timeout: if pending request has no response, try next server
    if (state->pending) {
        Time request_deadline = state->last_request_time + CLIENT_REQUEST_TIMEOUT_SEC * 1000000000ULL;
        if (now >= request_deadline) {
            CLIENT_TRACE("Request %lu timed out, trying next server",
                (unsigned long)state->request_id);
            state->last_was_timeout = true;
            state->last_was_rejected = false;
            state->pending = false;
            state->current_leader = (state->current_leader + 1) % state->num_servers;
        }
    }

    // Send a new request if not currently waiting for a response
    if (!state->pending) {
        int leader = state->current_leader;
        int conn_idx = tcp_index_from_tag(&state->tcp, leader);
        if (conn_idx < 0) {
            // Connection lost, try reconnecting
            tcp_connect(&state->tcp, state->server_addrs[leader], leader, NULL);
        } else {
            state->request_id++;
            state->last_oper = random_oper();

            RequestMessage request_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_REQUEST,
                    .length  = sizeof(RequestMessage),
                },
                .oper = state->last_oper,
                .client_id = state->client_id,
                .request_id = state->request_id,
            };

            CLIENT_TRACE("REQUEST: %s key=\"%.16s\" val=%lu (req_id=%lu, leader=%d)",
                oper_type_name(state->last_oper.type),
                state->last_oper.key,
                (unsigned long)state->last_oper.val,
                (unsigned long)state->request_id,
                leader);

            ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
            if (output)
                byte_queue_write(output, &request_message, sizeof(request_message));

            state->pending = true;
            state->last_request_time = now;
        }
    }

    // Set timeout for next tick
    if (state->pending) {
        Time request_deadline = state->last_request_time + CLIENT_REQUEST_TIMEOUT_SEC * 1000000000ULL;
        *timeout = deadline_to_timeout(request_deadline, now);
    } else {
        *timeout = 0; // Send next request immediately
    }

    if (pcap < TCP_POLL_CAPACITY)
        return -1;
    *pnum = tcp_register_events(&state->tcp, ctxs, pdata);
    return 0;
}

int client_free(void *state_)
{
    ClientState *state = state_;

    tcp_context_free(&state->tcp);
    return 0;
}
