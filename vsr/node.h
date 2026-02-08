#ifndef NODE_INCLUDED
#define NODE_INCLUDED

#include <lib/tcp.h>
#include <lib/basic.h>
#include <lib/message.h>

#include <state_machine/state_machine.h>

#include "log.h"
#include "config.h"
#include "client_table.h"

enum {

    // Normal Protocol
    MESSAGE_TYPE_REQUEST,
    MESSAGE_TYPE_REPLY,
    MESSAGE_TYPE_PREPARE,
    MESSAGE_TYPE_PREPARE_OK,
    MESSAGE_TYPE_COMMIT,

    // View Change Protocol
    MESSAGE_TYPE_BEGIN_VIEW_CHANGE,
    MESSAGE_TYPE_DO_VIEW_CHANGE,
    MESSAGE_TYPE_BEGIN_VIEW,

    // Recovery Protocol
    MESSAGE_TYPE_RECOVERY,
    MESSAGE_TYPE_RECOVERY_RESPONSE,
};

typedef struct {
    MessageHeader base;
    Operation oper;
    uint64_t client_id;
    uint64_t request_id;
} RequestMessage;

typedef struct {
    MessageHeader base;
    Operation oper;
    int sender_idx;
    int log_index;
    int commit_index;
    uint64_t view_number;
} PrepareMessage;

typedef struct {
    MessageHeader base;
    int sender_idx;
    int log_index;
    uint64_t view_number;
} PrepareOKMessage;

typedef struct {
    MessageHeader base;
    int commit_index;
} CommitMessage;

typedef struct {
    MessageHeader base;
    bool rejected;
    OperationResult result;
} ReplyMessage;

typedef struct {
    MessageHeader base;
    uint64_t view_number;
    int sender_idx;
} BeginViewChangeMessage;

typedef struct {
    MessageHeader base;
    uint64_t view_number;       // The new view number
    uint64_t old_view_number;   // Last view number when replica was in normal status
    int op_number;              // Number of entries in the log
    int commit_index;
    int sender_idx;
    // Followed by: LogEntry log[op_number]
} DoViewChangeMessage;

typedef struct {
    MessageHeader base;
    uint64_t view_number;
    int commit_index;
    int op_number;  // Number of log entries that follow
    // Followed by: LogEntry log[op_number]
} BeginViewMessage;

typedef struct {
    MessageHeader base;
    int sender_idx;
    uint64_t nonce;
} RecoveryMessage;

typedef struct {
    MessageHeader base;
    uint64_t view_number;
    int op_number;
    uint64_t nonce;
    int commit_index;
    int sender_idx;
} RecoveryResponseMessage;

typedef enum {
    STATUS_NORMAL,
    STATUS_CHANGE_VIEW,
    STATUS_RECOVERY,
} Status;

typedef struct {

    TCP tcp;

    Address  self_addr;
    Address  node_addrs[NODE_LIMIT];
    int      num_nodes;

    Status status;

    ClientTable client_table;

    uint64_t view_number;

    // These fields are used in recovery mode
    uint32_t recovery_votes;
    uint64_t recovery_nonce;
    uint64_t max_view_seen;
    int      latest_primary_idx;
    bool     received_primary_state;
    Log      potential_primary_log;
    Time     recovery_start_time;
    int      recovery_attempt_count;

    ///////////////////////////////////////////////////////////
    // VIEW CHANGE

    uint32_t begin_view_change_votes;  // Bitmask of received BeginViewChange messages
    uint32_t do_view_change_votes;     // Bitmask of received DoViewChange messages
    uint64_t do_view_change_best_old_view;  // Best old_view_number seen in DoViewChange
    int      do_view_change_best_commit;    // Best commit_index seen
    Log      do_view_change_best_log;       // Best log seen

    ///////////////////////////////////////////////////////////

    // If this is the leader, commit_index is the index of the next uncommitted element
    // of the log (if no uncommitted elements are present, it's equal to the total number
    // of log elements)
    int commit_index;

    PrepareMessage future[FUTURE_LIMIT];
    int num_future;

    Log log;

    Time last_heartbeat_time;

    StateMachine state_machine;

} NodeState;

struct pollfd;

int node_init(void *state, int argc, char **argv,
    void **ctxs, struct pollfd *pdata, int pcap, int *pnum,
    int *timeout);

int node_tick(void *state, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout);

int node_free(void *state);

void check_vsr_invariants(NodeState **nodes, int num_nodes);

#endif // NODE_INCLUDED