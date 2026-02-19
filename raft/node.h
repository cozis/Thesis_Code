#ifndef NODE_INCLUDED
#define NODE_INCLUDED

#include <lib/tcp.h>
#include <lib/basic.h>
#include <lib/message.h>

#include "wal.h"
#include "config.h"
#include "client_table.h"

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
    int      sender_idx;
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
    KVStoreOper oper;
    uint64_t client_id;
    uint64_t request_id;
} RequestMessage;

typedef struct {
    MessageHeader base;
    KVStoreResult result;
    uint64_t request_id;
} ReplyMessage;

typedef struct {
    MessageHeader base;
    int leader_idx;
    uint64_t request_id;
} RedirectMessage;

typedef enum {
    ROLE_FOLLOWER,
    ROLE_CANDIDATE,
    ROLE_LEADER,
} Role;

typedef struct {

    // In-memory copy of the term and vote values.
    // These must always be updated after the version
    // stored on disk.
    uint64_t term;
    int      voted_for;

    // Handle to the file backing the term and vote
    // values.
    Handle handle;
} TermAndVote;

typedef struct {

    // Networking subsystem
    TCP tcp;

    // Static list of cluster nodes
    Address self_addr;
    Address node_addrs[NODE_LIMIT];
    int     num_nodes;

    // Current loder of the node and the
    // current leader. If no leader, -1.
    Role role;
    int  leader_idx;

    // Votes received when candidate.
    // Each bit is associated to a node. The
    // number of votes is obtained by counting
    // set bits. This ensures nodes won't vote
    // twice.
    uint32_t votes;

    // Durable log subsystem
    WAL wal;

    // Durable fixed-size data subsystem
    TermAndVote term_and_vote;

    // Current election timeout. This value is relative
    // and must be summed to the heartbeat in order to
    // get the absolute election deadline.
    uint64_t election_timeout;

    // This is the current value of the node.
    // It is set at the start of each event
    // handler call.
    Time now;

    // If leader, this is the time when the last
    // heartbeat was sent. If follower, this is
    // the last time a heartbeat was received.
    Time heartbeat;

    // The state machine to be replicated.
    KVStore kvstore;

    // Table of client requests. This ensures request
    // are applied in order.
    ClientTable client_table;
    int next_client_tag;

    int commit_index;
    int last_applied;

    int next_indices[NODE_LIMIT];
    int match_indices[NODE_LIMIT];

} NodeState;

struct pollfd;

int node_init(void *state, int argc, char **argv,
    void **ctxs, struct pollfd *pdata, int pcap, int *pnum,
    int *timeout);

int node_tick(void *state, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout);

int node_free(void *state);

#endif // NODE_INCLUDED
