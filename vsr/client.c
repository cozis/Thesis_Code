#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <stdint.h>
#include <assert.h>

#include <lib/file_system.h>

#include "node.h"
#include "client.h"

//#define CLIENT_TRACE(fmt, ...) {}
#define CLIENT_TRACE(fmt, ...) fprintf(stderr, "CLIENT: " fmt "\n", ##__VA_ARGS__);

// Format time as seconds with 3 decimal places for trace output
#define TIME_FMT "%.3fs"
#define TIME_VAL(t) ((double)(t) / 1000000000.0)

static int leader_idx(ClientState *state)
{
    return state->view_number % state->num_servers;
}

static int
process_message(ClientState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    (void) conn_idx;

    if (!state->pending)
        return -1;

    if (type != MESSAGE_TYPE_REPLY)
        return -1;

    ReplyMessage reply_message;
    if (msg.len != sizeof(ReplyMessage))
        return -1;
    memcpy(&reply_message, msg.ptr, sizeof(reply_message));

    {
        Time now = get_current_time();
        CLIENT_TRACE("[" TIME_FMT "] received REPLY (rejected=%s)",
            TIME_VAL(now),
            reply_message.rejected ? "true" : "false");
    }

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
            // TODO: Check address is not duplicated
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

    state->view_number = 0;
    state->request_id = 0;

    // Load or generate a persistent client_id from disk.
    // This ensures the client_id survives process restarts.
    {
        string path = S("client_id");
        Handle fd;
        bool loaded = false;
        if (file_exists(path)) {
            if (file_open(path, &fd) == 0) {
                file_set_offset(fd, 0);
                uint64_t saved_id;
                if (file_read_exact(fd, (char*)&saved_id, sizeof(saved_id)) == (int)sizeof(saved_id)) {
                    state->client_id = saved_id;
                    loaded = true;
                }
                file_close(fd);
            }
        }
        if (!loaded) {
            Time now = get_current_time();
            state->client_id = (uint64_t)now;
            if (file_open(path, &fd) == 0) {
                file_set_offset(fd, 0);
                file_write_exact(fd, (char*)&state->client_id, sizeof(state->client_id));
                file_sync(fd);
                file_close(fd);
            }
        }
    }

    // Connect to all known servers
    for (int i = 0; i < state->num_servers; i++) {
        if (tcp_connect(&state->tcp, state->server_addrs[i], i, NULL) < 0) {
            fprintf(stderr, "Client :: Couldn't connect to server %d\n", i);
            tcp_context_free(&state->tcp);
            return -1;
        }
    }

    {
        Time now = get_current_time();
        CLIENT_TRACE("[" TIME_FMT "] initialized: num_servers=%d, leader_idx=%d",
            TIME_VAL(now), state->num_servers, leader_idx(state));
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

    Event events[TCP_EVENT_CAPACITY];
    int num_events = tcp_translate_events(&state->tcp, events, ctxs, pdata, *pnum);

    for (int i = 0; i < num_events; i++) {
        if (events[i].type == EVENT_DISCONNECT) {
            int conn_idx = events[i].conn_idx;
            int tag = tcp_get_tag(&state->tcp, conn_idx);
            if (tag == leader_idx(state) && state->pending) {
                Time now = get_current_time();
                CLIENT_TRACE("[" TIME_FMT "] lost connection to leader (node %d), resetting pending request",
                    TIME_VAL(now), leader_idx(state));
                state->pending = false;
            }
            tcp_close(&state->tcp, conn_idx);
            continue;
        }
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

    Time now = get_current_time();

    // If we've been waiting too long for a response, give up and
    // try the next server (the current leader may have crashed and
    // a view change may have happened)
    if (state->pending) {
        Time request_deadline = state->request_time + PRIMARY_DEATH_TIMEOUT_SEC * 1000000000ULL;
        if (request_deadline <= now) {
            CLIENT_TRACE("[" TIME_FMT "] request to leader (node %d, view=%lu) timed out, trying next server",
                TIME_VAL(now), leader_idx(state),
                (unsigned long)state->view_number);
            state->view_number++;
            state->pending = false;
        }
    }

    if (!state->pending) {

        int conn_idx = tcp_index_from_tag(&state->tcp, leader_idx(state));
        if (conn_idx < 0) {
            // Leader connection not available, try reconnecting
            {
                CLIENT_TRACE("[" TIME_FMT "] leader (node %d) not connected, reconnecting",
                    TIME_VAL(now), leader_idx(state));
            }
            tcp_connect(&state->tcp, state->server_addrs[leader_idx(state)], leader_idx(state), NULL);
        } else {
            // Now start a new operation
            state->request_id++;

            RequestMessage request_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_REQUEST,
                    .length  = sizeof(RequestMessage),
                },
                .oper = OPERATION_A,
                .client_id = state->client_id,
                .request_id = state->request_id,
            };

            ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
            assert(output);

            byte_queue_write(output, &request_message, request_message.base.length);

            {
                CLIENT_TRACE("[" TIME_FMT "] sent REQUEST to leader (node %d, view=%lu, client_id=%lu, req_id=%lu)",
                    TIME_VAL(now), leader_idx(state),
                    (unsigned long)state->view_number,
                    (unsigned long)state->client_id,
                    (unsigned long)state->request_id);
            }

            state->pending = true;
            state->request_time = now;
        }
    }

    // Set timeout based on pending request deadline
    Time deadline = INVALID_TIME;
    if (state->pending) {
        nearest_deadline(&deadline, state->request_time + PRIMARY_DEATH_TIMEOUT_SEC * 1000000000ULL);
    }
    *timeout = deadline_to_timeout(deadline, now);
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
