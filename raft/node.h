#ifndef NODE_INCLUDED
#define NODE_INCLUDED

#include <lib/tcp.h>
#include <lib/basic.h>
#include <lib/message.h>

#include "wal.h"
#include "config.h"

enum {
    MESSAGE_TYPE_REQUEST_VOTE,
    MESSAGE_TYPE_VOTED,
    MESSAGE_TYPE_APPEND_ENTRIES,
    MESSAGE_TYPE_APPENDED,
    MESSAGE_TYPE_REQUEST,
    MESSAGE_TYPE_REPLY,
    MESSAGE_TYPE_REDIRECT,
};

typedef struct {
    MessageHeader base;
    uint64_t term;
    int      sender_idx;
    int      last_log_index;
    uint64_t last_log_term;
} RequestVoteMessage;

typedef struct {
    MessageHeader base;
    uint64_t term;
    uint8_t  value;
} VotedMessage;

typedef struct {
    MessageHeader base;
    uint64_t term;
    int      leader_idx;
    int      prev_log_index;
    uint64_t prev_log_term;
    int      leader_commit;
    int      entry_count;
    // Followed by: LogEntry entries[entry_count]
} AppendEntriesMessage;

typedef struct {
    MessageHeader base;
    int      sender_idx;
    uint64_t term;
    uint8_t  success;
    int      match_index;
} AppendedMessage;

typedef struct {
    MessageHeader base;
    Operation oper;
    uint64_t client_id;
    uint64_t request_id;
} RequestMessage;

typedef struct {
    MessageHeader base;
    OperationResult result;
} ReplyMessage;

typedef struct {
    MessageHeader base;
    int leader_idx;
} RedirectMessage;

typedef struct {
    uint64_t        client_id;
    uint64_t        last_request_id;
    OperationResult last_result;
    bool            pending;
    int             conn_tag;
} ClientTableEntry;

typedef struct {
    int count;
    int capacity;
    ClientTableEntry *entries;
} ClientTable;

typedef enum {
    ROLE_FOLLOWER,
    ROLE_CANDIDATE,
    ROLE_LEADER,
} Role;

typedef struct {

    TCP tcp;
    WAL wal;

    Address self_addr;
    Address node_addrs[NODE_LIMIT];
    int     num_nodes;

    Role role;

    uint64_t term;
    int      voted_for;
    Handle   term_and_vote_handle;

    // Index of the current leader
    int leader_idx;

    int commit_index;
    int last_applied;

    // When CANDIDATE
    int votes_received;

    // When LEADER
    int next_indices[NODE_LIMIT];
    int match_indices[NODE_LIMIT];

    // Relative timeout in nanoseconds
    uint64_t election_timeout;

    // Last heartbeat time
    uint64_t watchdog;

    // Keep track of client request order to enforce
    // linearizability
    ClientTable client_table;
    int next_client_tag;

    // The state machine to be replicated
    StateMachine state_machine;

} NodeState;

struct pollfd;

int node_init(void *state, int argc, char **argv,
    void **ctxs, struct pollfd *pdata, int pcap, int *pnum,
    int *timeout);

int node_tick(void *state, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout);

int node_free(void *state);

#endif // NODE_INCLUDED
