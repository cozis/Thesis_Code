#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <assert.h>

#include "node.h"

typedef enum {
    HR_OK,
    HR_INVALID_MESSAGE,
    HR_OUT_OF_MEMORY,
    HR_IO_FAILURE,
} HandlerResult;

// Format time as seconds with 3 decimal places for trace output
#define TIME_FMT "%7.3fs"
#define TIME_VAL(t) ((double)(t) / 1000000000.0)

static const char *message_type_name(uint8_t type)
{
    switch (type) {
    case MESSAGE_TYPE_REQUEST_VOTE:   return "REQUEST_VOTE";
    case MESSAGE_TYPE_VOTED:          return "VOTED";
    case MESSAGE_TYPE_APPEND_ENTRIES: return "APPEND_ENTRIES";
    case MESSAGE_TYPE_APPENDED:       return "APPENDED";
    case MESSAGE_TYPE_REQUEST:        return "REQUEST";
    case MESSAGE_TYPE_REPLY:          return "REPLY";
    case MESSAGE_TYPE_REDIRECT:       return "REDIRECT";
    default:                          return "UNKNOWN";
    }
}

static const char *role_name(Role role)
{
    switch (role) {
    case ROLE_LEADER   : return "LEADER";
    case ROLE_FOLLOWER : return "FOLLOWER";
    case ROLE_CANDIDATE: return "CANDIDATE";
    }
    return "UNKNOWN";
}

static int self_idx(NodeState *state)
{
    for (int i = 0; i < state->num_nodes; i++)
        if (addr_eql(state->node_addrs[i], state->self_addr))
            return i;
    UNREACHABLE;
}

static void node_log_impl(NodeState *state, const char *event, const char *detail)
{
    printf("[" TIME_FMT "] NODE %d (%s) | T%-3lu C%-3d L%-3d | %-20s %s\n",
        TIME_VAL(state->now),
        self_idx(state),
        role_name(state->role),
        (unsigned long)state->term_and_vote.term,
        state->commit_index,
        wal_entry_count(&state->wal),
        event,
        detail ? detail : "");
}

#define node_log(state, event, fmt, ...) do {                    \
    char _detail[256];                                           \
    snprintf(_detail, sizeof(_detail), fmt, ##__VA_ARGS__);      \
    node_log_impl(state, event, _detail);                        \
} while (0)

#define node_log_simple(state, event) \
    node_log_impl(state, event, NULL)

static int count_set(uint32_t word)
{
    int n = 0;
    for (int i = 0; i < (int) sizeof(word) * 8; i++)
        if (word & (1 << i))
            n++;
    return n;
}

static bool reached_quorum(NodeState *state, uint32_t votes)
{
    return count_set(votes) > state->num_nodes/2;
}

static void add_vote(uint32_t *votes, int idx)
{
    *votes |= 1 << idx;
}

static uint64_t choose_election_timeout(void)
{
    uint64_t base = PRIMARY_DEATH_TIMEOUT_SEC * 1000000000ULL;
#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
    return base + quakey_random() % base;
#else
    return base + (uint64_t)rand() % base;
#endif
}

// Checksummed record format for the term_and_vote file:
//   uint64_t term     (8 bytes)
//   int      voted_for (4 bytes)
//   uint32_t checksum  (4 bytes)
// Total: 16 bytes per record.
#define TERM_AND_VOTE_RECORD_SIZE 16

static uint32_t term_vote_checksum(uint64_t term, int voted_for)
{
    // FNV-1a over the term and voted_for bytes
    uint32_t h = 2166136261u;
    const unsigned char *p;

    p = (const unsigned char *)&term;
    for (int i = 0; i < (int)sizeof(term); i++) {
        h ^= p[i];
        h *= 16777619u;
    }

    p = (const unsigned char *)&voted_for;
    for (int i = 0; i < (int)sizeof(voted_for); i++) {
        h ^= p[i];
        h *= 16777619u;
    }

    return h;
}

static int set_term_and_vote(NodeState *state, uint64_t term, int voted_for)
{
    uint32_t cksum = term_vote_checksum(term, voted_for);

    if (file_set_offset(state->term_and_vote.handle, 0) < 0)
        return -1;
    if (file_write_exact(state->term_and_vote.handle, (char*) &term, sizeof(term)))
        return -1;

    if (file_set_offset(state->term_and_vote.handle, 8) < 0)
        return -1;
    if (file_write_exact(state->term_and_vote.handle, (char*) &voted_for, sizeof(voted_for)))
        return -1;

    if (file_set_offset(state->term_and_vote.handle, 12) < 0)
        return -1;
    if (file_write_exact(state->term_and_vote.handle, (char*) &cksum, sizeof(cksum)))
        return -1;

    if (file_sync(state->term_and_vote.handle) < 0)
        return -1;

    state->term_and_vote.term = term;
    state->term_and_vote.voted_for = voted_for;
    return 0;
}


static void
send_to_peer_ex(NodeState *state, int peer_idx, MessageHeader *msg,
    void *extra, int extra_len)
{
    ByteQueue *output;
    int conn_idx = tcp_index_from_tag(&state->tcp, peer_idx);
    if (conn_idx < 0) {
        int ret = tcp_connect(&state->tcp, state->node_addrs[peer_idx], peer_idx, &output);
        if (ret < 0)
            return;
    } else {
        output = tcp_output_buffer(&state->tcp, conn_idx);
        if (output == NULL)
            return;
    }
    int header_len = msg->length - extra_len;
    byte_queue_write(output, msg, header_len);
    if (extra_len > 0)
        byte_queue_write(output, extra, extra_len);
}

static void
broadcast_to_peers_ex(NodeState *state, MessageHeader *msg,
    void *extra, int extra_len)
{
    for (int i = 0; i < state->num_nodes; i++) {
        if (i != self_idx(state))
            send_to_peer_ex(state, i, msg, extra, extra_len);
    }
}

static void
broadcast_to_peers(NodeState *state, MessageHeader *msg)
{
    broadcast_to_peers_ex(state, msg, NULL, 0);
}

static HandlerResult
send_vote_response(NodeState *state, int conn_idx, bool value, int candidate_idx)
{
    // Persist the vote BEFORE sending the response. If persistence
    // fails the node crashes, which is correct: we must never send
    // a vote grant that isn't durably recorded, otherwise after
    // restart the node could vote again in the same term, violating
    // the "at most one vote per term" invariant.
    if (value) {
        if (set_term_and_vote(state, state->term_and_vote.term, candidate_idx) < 0)
            return HR_IO_FAILURE;

        // Reset election timer when granting a vote (Raft Section 5.2)
        state->heartbeat = state->now;
        state->election_timeout = choose_election_timeout();
    }

    VotedMessage voted_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_VOTED,
            .length  = sizeof(VotedMessage),
        },
        .sender_idx = self_idx(state),
        .term = state->term_and_vote.term,
        .value = (value == true) ? 1 : 0,
    };

    node_log(state, "SEND VOTED", "-> node %d term=%lu granted=%s",
        tcp_get_tag(&state->tcp, conn_idx),
        (unsigned long)state->term_and_vote.term, value ? "yes" : "no");

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &voted_message, voted_message.base.length);
    return HR_OK;
}

static HandlerResult
send_appended_response(NodeState *state, int conn_idx, bool success, int match_index)
{
    AppendedMessage appended_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_APPENDED,
            .length  = sizeof(AppendedMessage),
        },
        .sender_idx = self_idx(state),
        .term = state->term_and_vote.term,
        .success = success ? 1 : 0,
        .match_index = match_index,
    };

    node_log(state, "SEND APPENDED", "-> node %d term=%lu success=%s match_index=%d",
        tcp_get_tag(&state->tcp, conn_idx), (unsigned long)state->term_and_vote.term, success ? "yes" : "no", match_index);

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &appended_message, appended_message.base.length);
    return HR_OK;
}

static HandlerResult send_redirect(NodeState *state, int conn_idx, ByteView msg)
{
    // Extract request_id from the client request so the client
    // can discard stale redirects that arrive after a new request
    // has already been sent (e.g. from a different server).
    uint64_t request_id = 0;
    if (msg.len == sizeof(RequestMessage)) {
        RequestMessage req;
        memcpy(&req, msg.ptr, sizeof(req));
        request_id = req.request_id;
    }

    RedirectMessage redirect_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_REDIRECT,
            .length  = sizeof(RedirectMessage),
        },
        .leader_idx = state->leader_idx,
        .request_id = request_id,
    };

    node_log(state, "SEND REDIRECT", "-> conn %d leader_idx=%d",
        tcp_get_tag(&state->tcp, conn_idx), state->leader_idx);

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &redirect_message, redirect_message.base.length);
    return HR_OK;
}

// Apply all committed but not-yet-applied entries to the state machine.
// Update the client table for ALL roles so that deduplication survives
// leader changes. If this node is the leader, also reply to waiting clients.
static void apply_committed(NodeState *state)
{
    while (state->last_applied < state->commit_index) {

        state->last_applied++;

        WALEntry *wal_entry = wal_peek_entry(&state->wal, state->last_applied);

        KVStoreResult result = kvstore_update(&state->kvstore, wal_entry->oper);

        // Update the client table with the committed result for ALL roles.
        // This ensures deduplication state survives leader changes: when a
        // follower becomes leader, its client table already knows about
        // committed operations and can reject duplicate requests.
        if (wal_entry->client_id != 0) {
            ClientTableEntry *entry = client_table_find(&state->client_table, wal_entry->client_id);
            if (entry == NULL) {
                client_table_add(&state->client_table, wal_entry->client_id,
                    wal_entry->request_id, -1);
                entry = client_table_find(&state->client_table, wal_entry->client_id);
                if (entry) {
                    entry->pending = false;
                    entry->last_result = result;
                }
            } else {
                entry->last_request_id = wal_entry->request_id;
                entry->pending = false;
                entry->last_result = result;
            }
        }

        // Leader: reply to waiting clients
        if (state->role == ROLE_LEADER && wal_entry->client_id != 0) {

            ClientTableEntry *entry = client_table_find(&state->client_table, wal_entry->client_id);
            if (entry) {

                ReplyMessage reply_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_REPLY,
                        .length  = sizeof(ReplyMessage),
                    },
                    .result = result,
                    .request_id = entry->last_request_id,
                };
                int ci = entry->conn_tag >= 0
                    ? tcp_index_from_tag(&state->tcp, entry->conn_tag)
                    : -1;
                if (ci > -1) {

                    node_log(state, "SEND REPLY", "-> client %lu entry %d",
                        (unsigned long)entry->client_id, state->last_applied);

                    ByteQueue *output = tcp_output_buffer(&state->tcp, ci);
                    if (output)
                        byte_queue_write(output, &reply_message, sizeof(reply_message));
                }
            }
        }
    }
}

static HandlerResult
handle_append_entries(NodeState *state, int conn_idx, AppendEntriesMessage *message,
    WALEntry *entries)
{
    // Reset election timer
    state->heartbeat = state->now;
    state->election_timeout = choose_election_timeout();

    state->leader_idx = message->leader_idx;

    int      prev_log_index = message->prev_log_index;
    uint64_t prev_log_term  = message->prev_log_term;

    // Log consistency check: verify prev_log_index/prev_log_term
    if (prev_log_index >= 0) {

        if (prev_log_index >= wal_entry_count(&state->wal)) {
            // We don't have the entry at prev_log_index
            return send_appended_response(state, conn_idx, false, -1);
        }

        if (wal_peek_entry(&state->wal, prev_log_index)->term != prev_log_term) {
            // Conflicting entry at prev_log_index
            return send_appended_response(state, conn_idx, false, -1);
        }
    }

    // Raft Section 5.3, rules 3 & 4:
    // Only truncate if an existing entry conflicts with a new one
    // (same index but different terms). Skip entries that already match.
    // Unconditionally truncating here would be wrong because a delayed
    // AppendEntries (with fewer entries) could arrive after a newer one
    // that already extended the log further. Blindly truncating would
    // discard those newer, valid entries, causing the follower to lose
    // committed data and forcing the leader to re-replicate them.
    int insert_idx = prev_log_index+1;
    int i = 0;
    for (; i < message->entry_count; i++) {
        int log_idx = insert_idx + i;

        if (log_idx >= wal_entry_count(&state->wal))
            break; // No more existing entries; append the rest

        WALEntry incoming;
        memcpy(&incoming, &entries[i], sizeof(WALEntry));

        if (wal_peek_entry(&state->wal, log_idx)->term != incoming.term) {
            // Conflict: truncate from this point onward and append the rest
            if (wal_truncate(&state->wal, log_idx) < 0)
                return HR_IO_FAILURE;
            break;
        }
        // Entry matches; skip it
    }

    for (; i < message->entry_count; i++) {

        // Copy in case it's unaligned
        WALEntry entry;
        memcpy(&entry, &entries[i], sizeof(WALEntry));
        if (wal_append(&state->wal, &entry) < 0)
            return HR_IO_FAILURE;
    }

    // Now we need to advance the local commit index
    // to sync with the leader.
    //
    // Usually the leader's commit index is greater or equal
    // to followers, in which case the follower will just need
    // to advance its own to match the leader. But in general,
    // it is possible for a follower to receive a greater commit
    // index
    //
    // Say we have a cluster of nodes A, B, C where A is the
    // leader:
    //   A: Replicates log entry 100 to a majority of nodes including B, but not C.
    //   A: Sets commit_index to 100 and sends AppendEntries to B
    //   B: Receives AppendEntries and sets commit_index to 100
    // Now A crashes and C is elected:
    //   C: Entry 100 must be in the log to win the election, but
    //      it is not committed yet.
    //   C: Sends AppendEntries message to B with commit_index of 99
    //   B: Receives a commit_index of 99 while its own is at 100
    //
    // Note that this is handled gracefully as B will
    // gradually commit messages until it's up to date
    // with other nodes.

    int leader_commit = message->leader_commit;
    int last_new_index = prev_log_index + message->entry_count;

    if (state->commit_index < leader_commit)
        state->commit_index = MIN(leader_commit, last_new_index);

    apply_committed(state);

    return send_appended_response(state, conn_idx, true, wal_entry_count(&state->wal)-1);
}

static HandlerResult start_election(NodeState *state)
{
    state->role = ROLE_CANDIDATE;
    state->votes = 1 << self_idx(state); // Vote for self

    if (set_term_and_vote(state, state->term_and_vote.term+1, self_idx(state)) < 0)
        return HR_IO_FAILURE;

    state->heartbeat = state->now;
    state->election_timeout = choose_election_timeout();

    node_log(state, "ELECTION", "starting for term %lu",
        (unsigned long)state->term_and_vote.term);

    RequestVoteMessage request_vote_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_REQUEST_VOTE,
            .length  = sizeof(RequestVoteMessage),
        },
        .term = state->term_and_vote.term,
        .sender_idx = self_idx(state),
        .last_log_index = wal_entry_count(&state->wal)-1,
        .last_log_term  = wal_last_term(&state->wal),
    };

    node_log(state, "SEND REQUEST_VOTE", "-> ALL last_log_index=%d last_log_term=%lu",
        wal_entry_count(&state->wal)-1,
        (unsigned long) wal_last_term(&state->wal));

    broadcast_to_peers(state, &request_vote_message.base);
    return HR_OK;
}

// Common pattern: step down to follower when we see a higher term.
static HandlerResult step_down(NodeState *state, uint64_t new_term)
{
    node_log(state, "STEP DOWN", "term %lu -> %lu",
        (unsigned long)state->term_and_vote.term, (unsigned long)new_term);

    state->role = ROLE_FOLLOWER;
    state->leader_idx = -1;

    if (new_term != state->term_and_vote.term) {
        if (set_term_and_vote(state, new_term, -1) < 0)
            return HR_IO_FAILURE;
    }

    return HR_OK;
}

// Send AppendEntries to a specific follower, including
// any log entries from next_indices[peer] onward.
static void send_append_entries_to_peer(NodeState *state, int peer_idx)
{
    int next = state->next_indices[peer_idx];
    int prev_index = next - 1;

    uint64_t prev_term = 0;
    if (prev_index >= 0 && prev_index < wal_entry_count(&state->wal))
        prev_term = wal_peek_entry(&state->wal, prev_index)->term;

    int count = MAX(wal_entry_count(&state->wal) - next, 0);

    AppendEntriesMessage append_entries_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_APPEND_ENTRIES,
            .length  = sizeof(AppendEntriesMessage) + count * sizeof(WALEntry),
        },
        .term = state->term_and_vote.term,
        .leader_idx = self_idx(state),
        .prev_log_index = prev_index,
        .prev_log_term = prev_term,
        .leader_commit = state->commit_index,
        .entry_count = count,
    };

    node_log(state, "SEND APPEND_ENTRIES", "-> node %d prev_idx=%d prev_term=%lu commit=%d entries=%d",
        peer_idx, prev_index, (unsigned long)prev_term,
        state->commit_index, count);

    WALEntry *entries = (count > 0) ? wal_peek_entry(&state->wal, next) : NULL;
    send_to_peer_ex(state, peer_idx, &append_entries_message.base, entries, count * sizeof(WALEntry));
}

static HandlerResult become_leader(NodeState *state)
{
    state->role = ROLE_LEADER;
    state->leader_idx = self_idx(state);

    // Initialize volatile leader state (Raft Section 5.3)
    for (int i = 0; i < state->num_nodes; i++) {
        state->next_indices[i] = wal_entry_count(&state->wal);
        state->match_indices[i] = -1;
    }

    // Append a no-op entry for the current term (Raft dissertation §6.4).
    // This ensures entries from previous terms can be committed, since
    // Section 5.4.2 only allows committing entries from the current term.
    WALEntry noop = {
        .term = state->term_and_vote.term,
        .oper = { .type = KVSTORE_OPER_NOOP },
        .client_id = 0,
    };
    if (wal_append(&state->wal, &noop) < 0)
        return HR_IO_FAILURE; // TODO: Restore previously set fields?

    node_log(state, "BECAME LEADER", "term=%lu votes=%d/%d",
        (unsigned long)state->term_and_vote.term, count_set(state->votes), state->num_nodes);

    state->match_indices[self_idx(state)] = wal_entry_count(&state->wal)-1;

    // Send AppendEntries (including the no-op) to establish authority
    for (int i = 0; i < state->num_nodes; i++) {
        if (i != self_idx(state))
            send_append_entries_to_peer(state, i);
    }

    state->heartbeat = state->now;
    return HR_OK;
}

// A candidate's log is "at least as up-to-date" if its last
// entry has a higher term, or the same term but equal or
// greater index.
static bool remote_has_recent_state(NodeState *state,
    int peer_index, uint64_t peer_term)
{
    uint64_t term = wal_last_term(&state->wal);

    if (peer_term != term)
        return peer_term > term;

    return peer_index >= wal_entry_count(&state->wal)-1;
}

static HandlerResult
process_request_vote_for_follower(NodeState *state, int conn_idx, ByteView msg)
{
    RequestVoteMessage request_vote_message;
    if (msg.len != sizeof(request_vote_message))
        return HR_INVALID_MESSAGE;
    memcpy(&request_vote_message, msg.ptr, sizeof(request_vote_message));

    // If the request's term is old, the peer is stale
    // and we let it know by replying with our own term
    // number.
    if (request_vote_message.term < state->term_and_vote.term)
        return send_vote_response(state, conn_idx, false, -1);

    // If the request's term is newer, we are staled
    // and need to move forward. Then, we can procede
    // with evaluating the peer for a vote.
    if (request_vote_message.term > state->term_and_vote.term) {
        if (set_term_and_vote(state, request_vote_message.term, -1) < 0)
            return HR_IO_FAILURE;
    }

    // Grant vote if we haven't voted yet (or already
    // voted for this candidate) and the candidate's
    // log is at least as recent as our own.
    if (state->term_and_vote.voted_for == -1 || state->term_and_vote.voted_for == request_vote_message.sender_idx) {
        if (remote_has_recent_state(state,
            request_vote_message.last_log_index,
            request_vote_message.last_log_term))
            return send_vote_response(state, conn_idx, true, request_vote_message.sender_idx);
    }

    return send_vote_response(state, conn_idx, false, -1);
}

static HandlerResult
process_append_entries_for_follower(NodeState *state, int conn_idx, ByteView msg)
{
    AppendEntriesMessage message;
    if (msg.len < (int)sizeof(message))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    // Stale leader?
    if (message.term < state->term_and_vote.term)
        return send_appended_response(state, conn_idx, false, -1);

    if (message.term > state->term_and_vote.term) {
        if (set_term_and_vote(state, message.term, -1) < 0)
            return HR_IO_FAILURE;
    }

    WALEntry *entries = (WALEntry*) (msg.ptr + sizeof(AppendEntriesMessage));
    return handle_append_entries(state, conn_idx, &message, entries);
}

static HandlerResult
process_request_vote_for_candidate(NodeState *state, int conn_idx, ByteView msg)
{
    RequestVoteMessage request_vote_message;
    if (msg.len != sizeof(request_vote_message))
        return HR_INVALID_MESSAGE;
    memcpy(&request_vote_message, msg.ptr, sizeof(request_vote_message));

    // If same term, we already voted for ourselves; deny
    if (request_vote_message.term == state->term_and_vote.term)
        return send_vote_response(state, conn_idx, false, -1);

    // Stale candidate
    if (request_vote_message.term < state->term_and_vote.term)
        return send_vote_response(state, conn_idx, false, -1);

    {
        HandlerResult hret = step_down(state, request_vote_message.term);
        if (hret != HR_OK) return hret;
    }

    if (remote_has_recent_state(state,
        request_vote_message.last_log_index,
        request_vote_message.last_log_term))
        return send_vote_response(state, conn_idx, true, request_vote_message.sender_idx);

    return send_vote_response(state, conn_idx, false, -1);
}

static HandlerResult
process_voted_for_candidate(NodeState *state, int conn_idx, ByteView msg)
{
    VotedMessage message;
    if (msg.len != sizeof(message))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    // Local state is stale
    if (message.term > state->term_and_vote.term) {
        return step_down(state, message.term);
    }

    // Ignore votes from old terms
    if (message.term < state->term_and_vote.term)
        return HR_OK;

    if (message.value) {

        add_vote(&state->votes, message.sender_idx);

        node_log(state, "RECV VOTE", "from node %d (%d/%d for term %lu)",
            tcp_get_tag(&state->tcp, conn_idx), count_set(state->votes), state->num_nodes, (unsigned long)state->term_and_vote.term);

        if (reached_quorum(state, state->votes)) {
            HandlerResult hret = become_leader(state);
            if (hret != HR_OK)
                return hret;
        }
    }

    return HR_OK;
}

static HandlerResult
process_append_entries_for_candidate(NodeState *state, int conn_idx, ByteView msg)
{
    AppendEntriesMessage append_entries_message;
    if (msg.len < (int)sizeof(append_entries_message))
        return HR_INVALID_MESSAGE;
    memcpy(&append_entries_message, msg.ptr, sizeof(append_entries_message));

    // Stale leader?
    if (append_entries_message.term < state->term_and_vote.term)
        return send_appended_response(state, conn_idx, false, -1);

    // A valid leader exists for this or a higher term; step down
    {
        HandlerResult hret = step_down(state, append_entries_message.term);
        if (hret != HR_OK) return hret;
    }

    WALEntry *entries = (WALEntry*) (msg.ptr + sizeof(AppendEntriesMessage));
    return handle_append_entries(state, conn_idx, &append_entries_message, entries);
}

// Leader: advance commit_index to the highest index replicated
// on a majority of servers, provided that entry's term matches
// the current term (Raft Section 5.4.2).
static void advance_commit_index(NodeState *state)
{
    int arr[NODE_LIMIT];
    for (int i = 0; i < state->num_nodes; i++)
        arr[i] = state->match_indices[i];

    // Simple insertion sort (ascending)
    for (int i = 1; i < state->num_nodes; i++) {
        int key = arr[i];
        int j = i - 1;
        while (j >= 0 && arr[j] > key) {
            arr[j + 1] = arr[j];
            j--;
        }
        arr[j + 1] = key;
    }

    // The median value is the highest index replicated on a majority.
    // For num_nodes=3: arr[1], for num_nodes=5: arr[2], etc.
    int candidate = arr[state->num_nodes / 2];

    if (candidate <= state->commit_index)
        return;

    assert(candidate < wal_entry_count(&state->wal));

    if (wal_peek_entry(&state->wal, candidate)->term == state->term_and_vote.term)
        state->commit_index = candidate;
}

static HandlerResult
process_request_vote_for_leader(NodeState *state, int conn_idx, ByteView msg)
{
    RequestVoteMessage request_vote_message;
    if (msg.len != sizeof(request_vote_message))
        return HR_INVALID_MESSAGE;
    memcpy(&request_vote_message, msg.ptr, sizeof(request_vote_message));

    if (request_vote_message.term > state->term_and_vote.term) {

        {
            HandlerResult hret = step_down(state, request_vote_message.term);
            if (hret != HR_OK) return hret;
        }

        if (remote_has_recent_state(state,
            request_vote_message.last_log_index,
            request_vote_message.last_log_term))
            return send_vote_response(state, conn_idx, true, request_vote_message.sender_idx);

        return send_vote_response(state, conn_idx, false, -1);
    }

    // Our term is at least as high; reject
    return send_vote_response(state, conn_idx, false, -1);
}

static HandlerResult
process_append_entries_for_leader(NodeState *state, int conn_idx, ByteView msg)
{
    AppendEntriesMessage append_entries_message;
    if (msg.len < (int)sizeof(append_entries_message))
        return HR_INVALID_MESSAGE;
    memcpy(&append_entries_message, msg.ptr, sizeof(append_entries_message));

    // Leader with a higher term exists? Step down
    if (append_entries_message.term > state->term_and_vote.term) {

        {
            HandlerResult hret = step_down(state, append_entries_message.term);
            if (hret != HR_OK) return hret;
        }

        WALEntry *entries = (WALEntry*) (msg.ptr + sizeof(AppendEntriesMessage));
        return handle_append_entries(state, conn_idx, &append_entries_message, entries);
    }

    // Same or lower term: reject (two leaders in the same term is impossible)
    return send_appended_response(state, conn_idx, false, -1);
}

static HandlerResult
process_appended_for_leader(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    AppendedMessage appended_message;
    if (msg.len != sizeof(appended_message))
        return HR_INVALID_MESSAGE;
    memcpy(&appended_message, msg.ptr, sizeof(appended_message));

    // Our state is stale
    if (appended_message.term > state->term_and_vote.term) {
        return step_down(state, appended_message.term);
    }

    int follower_idx = appended_message.sender_idx;
    assert(follower_idx > -1);
    assert(follower_idx < state->num_nodes);

    if (appended_message.success) {

        // Only advance monotonically: a stale success response (from
        // an older AppendEntries) may carry a lower match_index than
        // what we already know. Blindly overwriting would move
        // next_index backward, causing the leader to re-send entries
        // the follower already has and triggering spurious rejections
        // that make next_index oscillate instead of converging.
        if (appended_message.match_index > state->match_indices[follower_idx]) {
            state->match_indices[follower_idx] = appended_message.match_index;
            state->next_indices[follower_idx] = appended_message.match_index + 1;
        }

        int old_commit_index = state->commit_index;

        advance_commit_index(state);

        if (state->commit_index > old_commit_index)
            node_log(state, "COMMIT ADVANCE", "%d -> %d", old_commit_index, state->commit_index);

        apply_committed(state);

    } else {

        // Ignore stale rejections: only decrement if next_index
        // hasn't already been advanced past this point by a success
        if (appended_message.match_index == -1 && state->next_indices[follower_idx] > state->match_indices[follower_idx] + 1) {

            int new_next = MAX(state->match_indices[follower_idx] + 1, state->next_indices[follower_idx] - 1);

            node_log(state, "LOG INCONSISTENCY", "node %d next_index -> %d", follower_idx, new_next);

            state->next_indices[follower_idx] = new_next;
            send_append_entries_to_peer(state, follower_idx);
        }
    }

    return HR_OK;
}

static HandlerResult
process_request_for_leader(NodeState *state, int conn_idx, ByteView msg)
{
    RequestMessage request_message;
    if (msg.len != sizeof(request_message))
        return HR_INVALID_MESSAGE;
    memcpy(&request_message, msg.ptr, sizeof(request_message));

    // Assign a unique tag to this client connection so we can
    // find it later even if the connection array is reordered.
    int tag = tcp_get_tag(&state->tcp, conn_idx);
    if (tag == -1) {
        tag = state->next_client_tag++;
        tcp_set_tag(&state->tcp, conn_idx, tag, true);
    }

    // Client table deduplication (same pattern as VSR)
    ClientTableEntry *entry = client_table_find(&state->client_table, request_message.client_id);
    if (entry == NULL) {

        if (client_table_add(&state->client_table, request_message.client_id, request_message.request_id, tag) < 0)
            return HR_OK;

    } else {

        if (entry->pending)
            return HR_OK; // Already processing a request for this client

        if (entry->last_request_id > request_message.request_id)
            return HR_OK; // Stale request

        if (entry->last_request_id == request_message.request_id) {
            // Return cached result
            ReplyMessage reply_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_REPLY,
                    .length  = sizeof(ReplyMessage),
                },
                .result = entry->last_result,
                .request_id = entry->last_request_id,
            };
            ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
            if (output)
                byte_queue_write(output, &reply_message, sizeof(reply_message));
            return HR_OK;
        }

        entry->last_request_id = request_message.request_id;
        entry->pending = true;
        entry->conn_tag = tag;
    }

    WALEntry wal_entry = {
        .term = state->term_and_vote.term,
        .client_id = request_message.client_id,
        .request_id = request_message.request_id,
        .oper = request_message.oper,
    };

    if (wal_append(&state->wal, &wal_entry) < 0)
        return HR_IO_FAILURE;

    // Update own match index
    state->match_indices[self_idx(state)] = wal_entry_count(&state->wal)-1;

    // Replicate to all followers
    for (int i = 0; i < state->num_nodes; i++) {
        if (i != self_idx(state))
            send_append_entries_to_peer(state, i);
    }

    return HR_OK;
}

static HandlerResult
process_message(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    node_log(state, "RECV", "<- node/conn %d %s (%d bytes)",
        tcp_get_tag(&state->tcp, conn_idx),
        message_type_name(type), (int)msg.len);

    switch (state->role) {

    case ROLE_LEADER:
        switch (type) {
        case MESSAGE_TYPE_REQUEST_VOTE:
            return process_request_vote_for_leader(state, conn_idx, msg);
        case MESSAGE_TYPE_APPEND_ENTRIES:
            return process_append_entries_for_leader(state, conn_idx, msg);
        case MESSAGE_TYPE_APPENDED:
            return process_appended_for_leader(state, conn_idx, msg);
        case MESSAGE_TYPE_REQUEST:
            return process_request_for_leader(state, conn_idx, msg);
        }
        return HR_OK;

    case ROLE_FOLLOWER:
        switch (type) {
        case MESSAGE_TYPE_REQUEST_VOTE:
            return process_request_vote_for_follower(state, conn_idx, msg);
        case MESSAGE_TYPE_APPEND_ENTRIES:
            return process_append_entries_for_follower(state, conn_idx, msg);
        case MESSAGE_TYPE_REQUEST:
            return send_redirect(state, conn_idx, msg);
        }
        return HR_OK;

    case ROLE_CANDIDATE:
        switch (type) {
        case MESSAGE_TYPE_REQUEST_VOTE:
            return process_request_vote_for_candidate(state, conn_idx, msg);
        case MESSAGE_TYPE_VOTED:
            return process_voted_for_candidate(state, conn_idx, msg);
        case MESSAGE_TYPE_APPEND_ENTRIES:
            return process_append_entries_for_candidate(state, conn_idx, msg);
        case MESSAGE_TYPE_REQUEST:
            return send_redirect(state, conn_idx, msg);
        }
        return HR_OK;
    }

    UNREACHABLE;
}

static int term_and_vote_init(TermAndVote *term_and_vote, string file)
{
    // Do NOT use file_exists() here — it calls access() which is
    // not mocked by quakey, so it checks the real filesystem instead
    // of the mock. Instead, open the file (creates if new) and check
    // its size, matching the pattern used by wal_init.

    Handle handle;
    if (file_open(file, &handle) < 0)
        return -1;

    size_t len;
    if (file_size(handle, &len) < 0) {
        file_close(handle);
        return -1;
    }

    uint64_t term;
    int voted_for;

    if (len == 0) {

        term = 0;
        voted_for = -1;

    } else {

        if (len != TERM_AND_VOTE_RECORD_SIZE) {
            file_close(handle);
            return -1;
        }

        if (file_set_offset(handle, 0) < 0) {
            file_close(handle);
            return -1;
        }

        if (file_read_exact(handle, (char*) &term, sizeof(term)) < 0) {
            file_close(handle);
            return -1;
        }

        if (file_read_exact(handle, (char*) &voted_for, sizeof(voted_for)) < 0) {
            file_close(handle);
            return -1;
        }

        uint32_t checksum;
        if (file_read_exact(handle, (char*) &checksum, sizeof(checksum)) < 0) {
            file_close(handle);
            return -1;
        }

        if (checksum != term_vote_checksum(term, voted_for)) {
            file_close(handle);
            return -1;
        }
    }

    term_and_vote->handle = handle;
    term_and_vote->term = term;
    term_and_vote->voted_for = voted_for;
    return 0;
}

static void term_and_vote_free(TermAndVote *term_and_vote)
{
    file_close(term_and_vote->handle);
}

int node_init(void *state_, int argc, char **argv,
    void **ctxs, struct pollfd *pdata, int pcap, int *pnum,
    int *timeout)
{
    NodeState *state = state_;

    Time now = get_current_time();
    if (now == INVALID_TIME) {
        fprintf(stderr, "Node :: Couldn't get current time\n");
        return -1;
    }

    string wal_file = S("raft.wal");
    string term_and_vote_file = S("term_and_vote.wal");

    state->num_nodes = 0;
    bool self_addr_set = false;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--addr")) {
            if (self_addr_set) {
                fprintf(stderr, "Option --addr specified twice\n");
                return -1;
            }
            self_addr_set = true;
            i++;
            if (i == argc) {
                fprintf(stderr, "Option --addr missing value. Usage is --addr <addr>:<port>\n");
                return -1;
            }
            int ret = parse_addr_arg(argv[i], &state->self_addr);
            if (ret < 0) {
                fprintf(stderr, "Malformed <addr>:<port> pair for --addr option\n");
                return -1;
            }
            if (state->num_nodes == NODE_LIMIT) {
                fprintf(stderr, "Node limit of %d reached\n", NODE_LIMIT);
                return -1;
            }
            state->node_addrs[state->num_nodes++] = state->self_addr;
        } else if (!strcmp(argv[i], "--peer")) {
            i++;
            if (i == argc) {
                fprintf(stderr, "Option --peer missing value. Usage is --peer <addr>:<port>\n");
                return -1;
            }
            if (state->num_nodes == NODE_LIMIT) {
                fprintf(stderr, "Node limit of %d reached\n", NODE_LIMIT);
                return -1;
            }
            int ret = parse_addr_arg(argv[i], &state->node_addrs[state->num_nodes]);
            if (ret < 0) {
                fprintf(stderr, "Malformed <addr>:<port> pair for --peer option\n");
                return -1;
            }
            state->num_nodes++;
        } else if (!strcmp(argv[i], "--wal-file")) {
            i++;
            if (i == argc) {
                fprintf(stderr, "Option --wal-file missing value. Usage is --wal-file <path>\n");
                return -1;
            }
            wal_file = (string) { argv[i], strlen(argv[i]) };
        } else if (!strcmp(argv[i], "--term-and-vote-file")) {
            i++;
            if (i == argc) {
                fprintf(stderr, "Option --term-and-vote-file missing value. Usage is --term-and-vote-file <path>\n");
                return -1;
            }
            term_and_vote_file = (string) { argv[i], strlen(argv[i]) };
        } else {
            printf("Ignoring option '%s'\n", argv[i]);
        }
    }

    if (!self_addr_set) {
        printf("Option --addr not specified\n");
        return -1;
    }

    // Sort cluster addresses. This allows us to
    // globally refer to nodes by their index.
    addr_sort(state->node_addrs, state->num_nodes);

    if (term_and_vote_init(&state->term_and_vote, term_and_vote_file) < 0)
        return -1;

    kvstore_init(&state->kvstore);

    if (wal_init(&state->wal, wal_file) < 0)
        return -1;

    state->role = ROLE_FOLLOWER;
    state->now = now;
    state->heartbeat = now;
    state->leader_idx = -1;
    state->commit_index = -1;
    state->last_applied = -1;
    state->votes = 0;
    state->election_timeout = choose_election_timeout();
    state->commit_index = -1;
    state->last_applied = -1;

    for (int i = 0; i < NODE_LIMIT; i++) {
        state->next_indices[i] = 0;
        state->match_indices[i] = -1;
    }

    client_table_init(&state->client_table);
    state->next_client_tag = NODE_LIMIT;

    if (tcp_context_init(&state->tcp) < 0) {
        fprintf(stderr, "Node :: Couldn't setup TCP context\n");
        wal_free(&state->wal);
        return -1;
    }

    int ret = tcp_listen(&state->tcp, state->self_addr);
    if (ret < 0) {
        fprintf(stderr, "Node :: Couldn't setup TCP listener\n");
        tcp_context_free(&state->tcp);
        wal_free(&state->wal);
        return -1;
    }

    *timeout = -1; // No timeout until we have chunk servers
    if (pcap < TCP_POLL_CAPACITY) {
        fprintf(stderr, "Node :: Not enough poll() capacity (got %d, needed %d)\n", pcap, TCP_POLL_CAPACITY);
        return -1;
    }
    *pnum = tcp_register_events(&state->tcp, ctxs, pdata);
    return 0;
}

int node_free(void *state_)
{
    NodeState *state = state_;

    term_and_vote_free(&state->term_and_vote);
    tcp_context_free(&state->tcp);
    client_table_free(&state->client_table);
    wal_free(&state->wal);
    kvstore_free(&state->kvstore);
    return 0;
}

int node_tick(void *state_, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout)
{
    NodeState *state = state_;

    Time now = get_current_time();
    if (now == INVALID_TIME)
        return -1;
    if (now > state->now)
        state->now = now;

    /////////////////////////////////////////////////////////////////
    // Network events

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

            HandlerResult hret = process_message(state, conn_idx, msg_type, msg);
            if (hret == HR_INVALID_MESSAGE) {
                tcp_close(&state->tcp, conn_idx);
                break;
            }
            if (hret == HR_OUT_OF_MEMORY || hret == HR_IO_FAILURE) {
                return -1;
            }
            assert(hret == HR_OK);

            tcp_consume_message(&state->tcp, conn_idx);
        }
    }

    /////////////////////////////////////////////////////////////////
    // Time events

    Time deadline = INVALID_TIME;

    if (state->role == ROLE_LEADER) {
        Time heartbeat_deadline = state->heartbeat + HEARTBEAT_INTERVAL_SEC * 1000000000ULL;
        if (now >= heartbeat_deadline) {
            node_log_simple(state, "HEARTBEAT");
            for (int i = 0; i < state->num_nodes; i++) {
                if (i != self_idx(state))
                    send_append_entries_to_peer(state, i);
            }
            state->heartbeat = now;
            nearest_deadline(&deadline, now + HEARTBEAT_INTERVAL_SEC * 1000000000ULL);
        } else {
            nearest_deadline(&deadline, heartbeat_deadline);
        }
    } else {
        // Follower/Candidate: start election on leader timeout
        Time death_deadline = state->heartbeat + state->election_timeout;
        if (now >= death_deadline) {
            node_log_simple(state, "ELECTION TIMEOUT");
            HandlerResult hret = start_election(state);
            if (hret != HR_OK)
                return -1;
            // start_election resets heartbeat and election_timeout
            nearest_deadline(&deadline, state->heartbeat + state->election_timeout);
        } else {
            nearest_deadline(&deadline, death_deadline);
        }
    }

    *timeout = deadline_to_timeout(deadline, now);
    if (pcap < TCP_POLL_CAPACITY)
        return -1;
    *pnum = tcp_register_events(&state->tcp, ctxs, pdata);
    return 0;
}