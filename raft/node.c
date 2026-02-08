#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <stdint.h>
#include <assert.h>

#include "node.h"

//#define NODE_TRACE(fmt, ...) {}
#define NODE_TRACE(fmt, ...) fprintf(stderr, "NODE: " fmt "\n", ##__VA_ARGS__);

// Format time as seconds with 3 decimal places for trace output
#define TIME_FMT "%.3fs"
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

static void client_table_init(ClientTable *ct)
{
    ct->count = 0;
    ct->capacity = 0;
    ct->entries = NULL;
}

static void client_table_free(ClientTable *ct)
{
    free(ct->entries);
}

static ClientTableEntry *client_table_find(ClientTable *ct, uint64_t client_id)
{
    for (int i = 0; i < ct->count; i++)
        if (ct->entries[i].client_id == client_id)
            return &ct->entries[i];
    return NULL;
}

static int client_table_add(ClientTable *ct, uint64_t client_id, uint64_t request_id, int conn_tag)
{
    if (ct->count == ct->capacity) {
        int n = ct->capacity ? 2 * ct->capacity : 8;
        void *p = realloc(ct->entries, n * sizeof(ClientTableEntry));
        if (p == NULL) return -1;
        ct->capacity = n;
        ct->entries = p;
    }
    ct->entries[ct->count++] = (ClientTableEntry) {
        .client_id = client_id,
        .last_request_id = request_id,
        .pending = true,
        .conn_tag = conn_tag,
    };
    return 0;
}

static int self_idx(NodeState *state)
{
    for (int i = 0; i < state->num_nodes; i++)
        if (addr_eql(state->node_addrs[i], state->self_addr))
            return i;
    UNREACHABLE;
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

static int set_term_and_vote(NodeState *state, uint64_t term, int voted_for)
{
    state->term = term;
    state->voted_for = voted_for;
    if (file_set_offset(state->term_and_vote_handle, 0) < 0)
        return -1;
    if (file_write_exact(state->term_and_vote_handle, (char*) &term, sizeof(term)))
        return -1;
    if (file_write_exact(state->term_and_vote_handle, (char*) &voted_for, sizeof(voted_for)))
        return -1;
    if (file_sync(state->term_and_vote_handle) < 0)
        return -1;
    return 0;
}

static int send_to_peer_ex(NodeState *state, int peer_idx, MessageHeader *msg, void *extra, int extra_len)
{
    ByteQueue *output;
    int conn_idx = tcp_index_from_tag(&state->tcp, peer_idx);
    if (conn_idx < 0) {
        int ret = tcp_connect(&state->tcp, state->node_addrs[peer_idx], peer_idx, &output);
        if (ret < 0)
            return -1;
    } else {
        output = tcp_output_buffer(&state->tcp, conn_idx);
        if (output == NULL) {
            assert(0);
        }
    }
    int header_len = msg->length - extra_len;
    byte_queue_write(output, msg, header_len);
    if (extra_len > 0)
        byte_queue_write(output, extra, extra_len);
    return 0;
}

static int broadcast_to_peers_ex(NodeState *state, MessageHeader *msg, void *extra, int extra_len)
{
    for (int i = 0; i < state->num_nodes; i++) {
        if (i != self_idx(state))
            if (send_to_peer_ex(state, i, msg, extra, extra_len) < 0)
                return -1;
    }
    return 0;
}

static int broadcast_to_peers(NodeState *state, MessageHeader *msg)
{
    return broadcast_to_peers_ex(state, msg, NULL, 0);
}

static void send_vote_response(NodeState *state, int conn_idx, bool value, int candidate_idx)
{
    VotedMessage voted_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_VOTED,
            .length  = sizeof(VotedMessage),
        },
        .term = state->term,
        .value = (value == true) ? 1 : 0,
    };

    {
        Time t = get_current_time();
        int peer = tcp_get_tag(&state->tcp, conn_idx);
        NODE_TRACE("[" TIME_FMT "] node %d (%s) -> node %d: VOTED term=%lu granted=%s",
            TIME_VAL(t), self_idx(state), role_name(state->role),
            peer, (unsigned long)state->term, value ? "yes" : "no");
    }

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &voted_message, voted_message.base.length);

    if (value) {

        if (set_term_and_vote(state, state->term, candidate_idx) < 0) {
            // I/O error persisting vote; proceed anyway
        }

        // Reset election timer when granting a vote (Raft Section 5.2)
        Time now = get_current_time();
        if (now == INVALID_TIME)
            return;
        state->watchdog = now;
        state->election_timeout = choose_election_timeout();
    }
}

static void send_appended_response(NodeState *state, int conn_idx, bool success, int match_index)
{
    AppendedMessage appended_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_APPENDED,
            .length  = sizeof(AppendedMessage),
        },
        .sender_idx = self_idx(state),
        .term = state->term,
        .success = success ? 1 : 0,
        .match_index = match_index,
    };

    {
        Time t = get_current_time();
        int peer = tcp_get_tag(&state->tcp, conn_idx);
        NODE_TRACE("[" TIME_FMT "] node %d (%s) -> node %d: APPENDED term=%lu success=%s match_index=%d",
            TIME_VAL(t), self_idx(state), role_name(state->role),
            peer, (unsigned long)state->term, success ? "yes" : "no", match_index);
    }

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &appended_message, appended_message.base.length);
}

static void send_redirect(NodeState *state, int conn_idx)
{
    RedirectMessage redirect_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_REDIRECT,
            .length  = sizeof(RedirectMessage),
        },
        .leader_idx = state->leader_idx,
    };

    {
        Time t = get_current_time();
        int tag = tcp_get_tag(&state->tcp, conn_idx);
        NODE_TRACE("[" TIME_FMT "] node %d (%s) -> conn %d: REDIRECT leader_idx=%d",
            TIME_VAL(t), self_idx(state), role_name(state->role),
            tag, state->leader_idx);
    }

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &redirect_message, redirect_message.base.length);
}

// Apply all committed but not-yet-applied entries to the state machine.
// If this node is the leader, also reply to waiting clients.
static void apply_committed(NodeState *state)
{
    while (state->last_applied < state->commit_index) {

        state->last_applied++;

        {
            Time t = get_current_time();
            NODE_TRACE("[" TIME_FMT "] node %d (%s): applying log entry %d (term %lu)",
                TIME_VAL(t), self_idx(state), role_name(state->role),
                state->last_applied,
                (unsigned long) wal_peek_entry(&state->wal, state->last_applied)->term);
        }

        OperationResult result = state_machine_update(&state->state_machine, wal_peek_entry(&state->wal, state->last_applied)->oper);

        if (state->role == ROLE_LEADER) {

            ClientTableEntry *entry = client_table_find(&state->client_table, wal_peek_entry(&state->wal, state->last_applied)->client_id);
            if (entry && entry->pending) {

                entry->pending = false;
                entry->last_result = result;

                ReplyMessage reply_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_REPLY,
                        .length  = sizeof(ReplyMessage),
                    },
                    .result = result,
                };
                int ci = tcp_index_from_tag(&state->tcp, entry->conn_tag);
                if (ci >= 0) {

                    Time t = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (LEADER) -> client %lu: REPLY for log entry %d",
                        TIME_VAL(t), self_idx(state),
                        (unsigned long)entry->client_id, state->last_applied);

                    ByteQueue *output = tcp_output_buffer(&state->tcp, ci);
                    if (output)
                        byte_queue_write(output, &reply_message, sizeof(reply_message));
                }
            }
        }
    }
}

static int handle_append_entries(NodeState *state, int conn_idx,
    AppendEntriesMessage *append_entries_message, WALEntry *entries)
{
    // Reset election timer
    Time now = get_current_time();
    if (now == INVALID_TIME)
        return -1;
    state->watchdog = now;
    state->election_timeout = choose_election_timeout();

    state->leader_idx = append_entries_message->leader_idx;

    int      prev_log_index = append_entries_message->prev_log_index;
    uint64_t prev_log_term  = append_entries_message->prev_log_term;

    // Log consistency check: verify prev_log_index/prev_log_term
    if (prev_log_index >= 0) {

        if (prev_log_index >= wal_entry_count(&state->wal)) {
            // We don't have the entry at prev_log_index
            send_appended_response(state, conn_idx, false, -1);
            return 0;
        }

        if (wal_peek_entry(&state->wal, prev_log_index)->term != prev_log_term) {
            // Conflicting entry at prev_log_index
            send_appended_response(state, conn_idx, false, -1);
            return 0;
        }
    }

    int insert_idx = prev_log_index+1;
    if (insert_idx < wal_entry_count(&state->wal)) {
        // TODO: What if we're truncating operations that were already applied to the state machine?
        if (wal_truncate(&state->wal, insert_idx) < 0) {
            send_appended_response(state, conn_idx, false, -1);
            return 0;
        }
    }

    for (int i = 0; i < append_entries_message->entry_count; i++) {

        // Copy in case it's unaligned
        WALEntry entry;
        memcpy(&entry, &entries[i], sizeof(WALEntry));
        if (wal_append(&state->wal, &entry) < 0) {
            send_appended_response(state, conn_idx, false, -1);
            return 0;
        }
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

    int leader_commit = append_entries_message->leader_commit;

    if (state->commit_index < leader_commit)
        state->commit_index = MIN(leader_commit, wal_entry_count(&state->wal)-1);

    apply_committed(state);

    send_appended_response(state, conn_idx, true, wal_entry_count(&state->wal)-1);
    return 0;
}

static void start_election(NodeState *state)
{
    {
        Time t = get_current_time();
        NODE_TRACE("[" TIME_FMT "] node %d: starting election for term %lu",
            TIME_VAL(t), self_idx(state), (unsigned long)state->term);
    }

    state->role = ROLE_CANDIDATE;
    state->votes_received = 1; // Vote for self

    if (set_term_and_vote(state, state->term+1, self_idx(state)) < 0) {
        return; // I/O error; retry on next election timeout
    }

    Time now = get_current_time();
    if (now != INVALID_TIME) {
        state->watchdog = now;
        state->election_timeout = choose_election_timeout();
    }

    RequestVoteMessage request_vote_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_REQUEST_VOTE,
            .length  = sizeof(RequestVoteMessage),
        },
        .term = state->term,
        .sender_idx = self_idx(state),
        .last_log_index = wal_entry_count(&state->wal)-1,
        .last_log_term  = wal_last_term(&state->wal),
    };

    {
        Time t = get_current_time();
        NODE_TRACE("[" TIME_FMT "] node %d (%s) -> ALL: REQUEST_VOTE term=%lu last_log_index=%d last_log_term=%lu",
            TIME_VAL(t), self_idx(state), role_name(state->role),
            (unsigned long) state->term, wal_entry_count(&state->wal)-1,
            (unsigned long) wal_last_term(&state->wal));
    }

    broadcast_to_peers(state, &request_vote_message.base);
}

// Common pattern: step down to follower when we see a higher term.
static void step_down(NodeState *state, uint64_t new_term)
{
    Time t = get_current_time();
    NODE_TRACE("[" TIME_FMT "] node %d (%s): stepping down to FOLLOWER, term %lu -> %lu",
        TIME_VAL(t), self_idx(state), role_name(state->role),
        (unsigned long) state->term, (unsigned long) new_term);

    state->role = ROLE_FOLLOWER;

    if (set_term_and_vote(state, new_term, -1) < 0) {
        // I/O error persisting term; in-memory state already updated
    }
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
        .term = state->term,
        .leader_idx = self_idx(state),
        .prev_log_index = prev_index,
        .prev_log_term = prev_term,
        .leader_commit = state->commit_index,
        .entry_count = count,
    };

    {
        Time t = get_current_time();
        NODE_TRACE("[" TIME_FMT "] node %d (%s) -> node %d: APPEND_ENTRIES term=%lu prev_log_index=%d prev_log_term=%lu leader_commit=%d entries=%d",
            TIME_VAL(t), self_idx(state), role_name(state->role),
            peer_idx, (unsigned long)state->term, prev_index, (unsigned long)prev_term,
            state->commit_index, count);
    }

    WALEntry *entries = (count > 0) ? wal_peek_entry(&state->wal, next) : NULL;
    send_to_peer_ex(state, peer_idx, &append_entries_message.base, entries, count * sizeof(WALEntry));
}

static void become_leader(NodeState *state)
{
    Time t = get_current_time();
    NODE_TRACE("[" TIME_FMT "] node %d: became LEADER for term %lu (votes=%d/%d)",
        TIME_VAL(t), self_idx(state), (unsigned long)state->term,
        state->votes_received, state->num_nodes);

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
        .term = state->term,
        .oper = OPERATION_NOOP,
        .client_id = 0,
    };
    if (wal_append(&state->wal, &noop) < 0) {
        // I/O error; step down and let another node become leader
        state->role = ROLE_FOLLOWER;
        return;
    }

    state->match_indices[self_idx(state)] = wal_entry_count(&state->wal)-1;

    // Send AppendEntries (including the no-op) to establish authority
    for (int i = 0; i < state->num_nodes; i++) {
        if (i != self_idx(state))
            send_append_entries_to_peer(state, i);
    }

    Time now = get_current_time();
    if (now == INVALID_TIME)
        return;
    state->watchdog = now;
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

static int
process_message_as_follower(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    switch (type) {
    case MESSAGE_TYPE_REQUEST_VOTE:
        {
            RequestVoteMessage request_vote_message;
            if (msg.len != sizeof(request_vote_message))
                return -1;
            memcpy(&request_vote_message, msg.ptr, sizeof(request_vote_message));

            // If the request's term is old, the peer is stale
            // and we let it know by replying with our own term
            // number.
            if (request_vote_message.term < state->term) {
                send_vote_response(state, conn_idx, false, -1);
                break;
            }

            // If the request's term is newer, we are staled
            // and need to move forward. Then, we can procede
            // with evaluating the peer for a vote.
            if (request_vote_message.term > state->term) {
                if (set_term_and_vote(state, request_vote_message.term, -1) < 0)
                    break; // I/O error; ignore this request
            }

            // Grant vote if we haven't voted yet (or already
            // voted for this candidate) and the candidate's
            // log is at least as recent as our own.
            if (state->voted_for == -1 || state->voted_for == request_vote_message.sender_idx) {
                if (remote_has_recent_state(state,
                    request_vote_message.last_log_index,
                    request_vote_message.last_log_term)) {
                    send_vote_response(state, conn_idx, true, request_vote_message.sender_idx);
                    break;
                }
            }

            send_vote_response(state, conn_idx, false, -1);
        }
        break;
    case MESSAGE_TYPE_VOTED:
        {
            // Followers don't expect vote responses. Ignore.
        }
        break;
    case MESSAGE_TYPE_APPEND_ENTRIES:
        {
            AppendEntriesMessage append_entries_message;
            if (msg.len < (int)sizeof(append_entries_message))
                return -1;
            memcpy(&append_entries_message, msg.ptr, sizeof(append_entries_message));

            if (append_entries_message.term < state->term) {
                // Stale leader
                send_appended_response(state, conn_idx, false, -1);
                break;
            }

            if (append_entries_message.term > state->term) {
                if (set_term_and_vote(state, append_entries_message.term, -1) < 0)
                    break; // I/O error; ignore this message
            }

            return handle_append_entries(state, conn_idx, &append_entries_message, (WALEntry*) (msg.ptr + sizeof(AppendEntriesMessage)));
        }
        break;
    case MESSAGE_TYPE_APPENDED:
        {
            // Followers don't expect append responses. Ignore.
        }
        break;
    case MESSAGE_TYPE_REQUEST:
        {
            // Redirect client to the current leader.
            // If no leader exists at this time, we tell the client.
            send_redirect(state, conn_idx);
        }
        break;
    case MESSAGE_TYPE_REPLY:
        {
            return -1;
        }
        break;
    case MESSAGE_TYPE_REDIRECT:
        {
            return -1;
        }
        break;
    }

    return 0;
}

static int
process_message_as_candidate(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    switch (type) {
    case MESSAGE_TYPE_REQUEST_VOTE:
        {
            RequestVoteMessage request_vote_message;
            if (msg.len != sizeof(request_vote_message))
                return -1;
            memcpy(&request_vote_message, msg.ptr, sizeof(request_vote_message));

            // If same term, we already voted for ourselves; deny
            if (request_vote_message.term == state->term) {
                send_vote_response(state, conn_idx, false, -1);
                break;
            }

            // Stale candidate
            if (request_vote_message.term < state->term) {
                send_vote_response(state, conn_idx, false, -1);
                break;
            }

            // Higher term: step down and consider the vote
            step_down(state, request_vote_message.term);

            if (remote_has_recent_state(state,
                    request_vote_message.last_log_index,
                    request_vote_message.last_log_term)) {
                send_vote_response(state, conn_idx, true, request_vote_message.sender_idx);
            } else {
                send_vote_response(state, conn_idx, false, -1);
            }
        }
        break;
    case MESSAGE_TYPE_VOTED:
        {
            VotedMessage voted_message;
            if (msg.len != sizeof(voted_message))
                return -1;
            memcpy(&voted_message, msg.ptr, sizeof(voted_message));

            // Local state is stale
            if (voted_message.term > state->term) {
                step_down(state, voted_message.term);
                break;
            }

            // Ignore votes from old terms
            if (voted_message.term < state->term)
                break;

            if (voted_message.value) {
                {
                    Time t = get_current_time();
                    int sender = tcp_get_tag(&state->tcp, conn_idx);
                    NODE_TRACE("[" TIME_FMT "] node %d (CANDIDATE): received vote from node %d (%d/%d votes for term %lu)",
                        TIME_VAL(t), self_idx(state), sender,
                        state->votes_received+1, state->num_nodes,
                        (unsigned long)state->term);
                }
                state->votes_received++;
                if (state->votes_received > state->num_nodes/2) {
                    become_leader(state);
                }
            }
        }
        break;
    case MESSAGE_TYPE_APPEND_ENTRIES:
        {
            AppendEntriesMessage append_entries_message;
            if (msg.len < (int)sizeof(append_entries_message))
                return -1;
            memcpy(&append_entries_message, msg.ptr, sizeof(append_entries_message));

            if (append_entries_message.term < state->term) {
                // Stale leader; reject
                send_appended_response(state, conn_idx, false, -1);
                break;
            }

            // A valid leader exists for this or a higher term; step down
            step_down(state, append_entries_message.term);

            return handle_append_entries(state, conn_idx, &append_entries_message, (WALEntry*) (msg.ptr + sizeof(AppendEntriesMessage)));
        }
        break;
    case MESSAGE_TYPE_APPENDED:
        {
            // Candidates don't expect append responses. Ignore.
        }
        break;
    case MESSAGE_TYPE_REQUEST:
        {
            // Redirect client to the current leader.
            // If no leader exists at this time, we tell the client.
            send_redirect(state, conn_idx);
        }
        break;
    case MESSAGE_TYPE_REPLY:
        {
            return -1;
        }
        break;
    case MESSAGE_TYPE_REDIRECT:
        {
            return -1;
        }
        break;
    }

    return 0;
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

    if (wal_peek_entry(&state->wal, candidate)->term == state->term)
        state->commit_index = candidate;
}

static int
process_message_as_leader(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    switch (type) {
    case MESSAGE_TYPE_REQUEST_VOTE:
        {
            RequestVoteMessage request_vote_message;
            if (msg.len != sizeof(request_vote_message))
                return -1;
            memcpy(&request_vote_message, msg.ptr, sizeof(request_vote_message));

            if (request_vote_message.term > state->term) {

                step_down(state, request_vote_message.term);

                if (remote_has_recent_state(state,
                        request_vote_message.last_log_index,
                        request_vote_message.last_log_term)) {
                    send_vote_response(state, conn_idx, true, request_vote_message.sender_idx);
                } else {
                    send_vote_response(state, conn_idx, false, -1);
                }

            } else {

                // Our term is at least as high; reject
                send_vote_response(state, conn_idx, false, -1);
            }
        }
        break;
    case MESSAGE_TYPE_VOTED:
        {
            // Already leader, ignore stray vote responses
        }
        break;
    case MESSAGE_TYPE_APPEND_ENTRIES:
        {
            AppendEntriesMessage append_entries_message;
            if (msg.len < (int)sizeof(append_entries_message))
                return -1;
            memcpy(&append_entries_message, msg.ptr, sizeof(append_entries_message));

            if (append_entries_message.term > state->term) {
                // A leader with a higher term exists; step down
                step_down(state, append_entries_message.term);
                return handle_append_entries(state, conn_idx, &append_entries_message, (WALEntry*) (msg.ptr + sizeof(AppendEntriesMessage)));
            }

            // Same or lower term: reject (two leaders in the same term is impossible)
            send_appended_response(state, conn_idx, false, -1);
        }
        break;
    case MESSAGE_TYPE_APPENDED:
        {
            AppendedMessage appended_message;
            if (msg.len != sizeof(appended_message))
                return -1;
            memcpy(&appended_message, msg.ptr, sizeof(appended_message));

            // Our state is stale
            if (appended_message.term > state->term) {
                step_down(state, appended_message.term);
                break;
            }

            int follower_idx = appended_message.sender_idx;
            assert(follower_idx > -1);
            assert(follower_idx < state->num_nodes);

            if (appended_message.success) {

                state->match_indices[follower_idx] = appended_message.match_index;
                state->next_indices[follower_idx] = appended_message.match_index + 1;

                int old_commit_index = state->commit_index;

                advance_commit_index(state);

                if (state->commit_index > old_commit_index) {
                    Time t = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (LEADER): commit_index advanced %d -> %d",
                        TIME_VAL(t), self_idx(state), old_commit_index, state->commit_index);
                }

                apply_committed(state);

            } else {

                // Log inconsistency: decrement nextIndex and retry

                Time t = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (LEADER): log inconsistency with node %d, decrementing next_index to %d",
                    TIME_VAL(t), self_idx(state), follower_idx,
                    state->next_indices[follower_idx] > 0 ? state->next_indices[follower_idx] - 1 : 0);

                state->next_indices[follower_idx] = MAX(0, state->next_indices[follower_idx]-1);
                send_append_entries_to_peer(state, follower_idx);
            }
        }
        break;
    case MESSAGE_TYPE_REQUEST:
        {
            RequestMessage request_message;
            if (msg.len != sizeof(request_message))
                return -1;
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
                    break;
            } else {
                if (entry->pending)
                    break; // Already processing a request for this client

                if (entry->last_request_id > request_message.request_id)
                    break; // Stale request

                if (entry->last_request_id == request_message.request_id) {
                    // Return cached result
                    ReplyMessage reply_message = {
                        .base = {
                            .version = MESSAGE_VERSION,
                            .type    = MESSAGE_TYPE_REPLY,
                            .length  = sizeof(ReplyMessage),
                        },
                        .result = entry->last_result,
                    };
                    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
                    if (output)
                        byte_queue_write(output, &reply_message, sizeof(reply_message));
                    break;
                }

                entry->last_request_id = request_message.request_id;
                entry->pending = true;
                entry->conn_tag = tag;
            }

            WALEntry wal_entry = {
                .term = state->term,
                .client_id = request_message.client_id,
                .oper = request_message.oper,
            };
            if (wal_append(&state->wal, &wal_entry) < 0)
                break; // I/O error; client will retry

            // Update own match index
            state->match_indices[self_idx(state)] = wal_entry_count(&state->wal)-1;

            // Replicate to all followers
            for (int i = 0; i < state->num_nodes; i++) {
                if (i != self_idx(state))
                    send_append_entries_to_peer(state, i);
            }
        }
        break;
    case MESSAGE_TYPE_REPLY:
        {
            return -1;
        }
        break;
    case MESSAGE_TYPE_REDIRECT:
        {
            return -1;
        }
        break;
    }

    return 0;
}

static int
process_message(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    {
        Time t = get_current_time();
        int sender = tcp_get_tag(&state->tcp, conn_idx);
        NODE_TRACE("[" TIME_FMT "] node %d (%s) <- node/conn %d: recv %s (%d bytes)",
            TIME_VAL(t), self_idx(state), role_name(state->role),
            sender, message_type_name(type), (int)msg.len);
    }

    switch (state->role) {
    case ROLE_LEADER:
        return process_message_as_leader(state, conn_idx, type, msg);
    case ROLE_FOLLOWER:
        return process_message_as_follower(state, conn_idx, type, msg);
    case ROLE_CANDIDATE:
        return process_message_as_candidate(state, conn_idx, type, msg);
    }
    UNREACHABLE;
}

int node_init(void *state_, int argc, char **argv,
    void **ctxs, struct pollfd *pdata, int pcap, int *pnum,
    int *timeout)
{
    NodeState *state = state_;

    string wal_file = S("raft.wal");
    string term_and_vote_file = S("term_and_vote.wal");

    Time now = get_current_time();
    if (now == INVALID_TIME) {
        fprintf(stderr, "Node :: Couldn't get current time\n");
        return -1;
    }

    ///////////////////////////////////////////////////////////////
    // Parse arguments

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

    ///////////////////////////////////////////////////////////////
    // Initialize term and vote

    bool existed = false;
    if (file_exists(term_and_vote_file))
        existed = true;

    Handle term_and_vote_handle;
    if (file_open(term_and_vote_file, &term_and_vote_handle) < 0)
        return -1;

    uint64_t term = 0;
    int voted_for = -1;
    if (existed) {
        if (file_read_exact(term_and_vote_handle, (char*) &term, sizeof(term)) < 0) {
            file_close(term_and_vote_handle);
            return -1;
        }
        if (file_read_exact(term_and_vote_handle, (char*) &voted_for, sizeof(voted_for)) < 0) {
            file_close(term_and_vote_handle);
            return -1;
        }
    }

    state->term_and_vote_handle = term_and_vote_handle;
    state->term = term;
    state->voted_for = voted_for;

    ///////////////////////////////////////////////////////////////
    // Initialize WAL and state machine

    state_machine_init(&state->state_machine);

    if (wal_init(&state->wal, wal_file) < 0) {
        printf("Couldn't initialize the WAL");
        return -1;
    }

    WALReplay wal_replay;
    wal_replay_init(&wal_replay, &state->wal);
    for (WALEntry *entry; (entry = wal_replay_next(&wal_replay)); ) {
        state_machine_update(&state->state_machine, entry->oper);
    }
    wal_replay_free(&wal_replay);

    ///////////////////////////////////////////////////////////////
    // Initialize volatile state

    // Current role of the node
    state->role = ROLE_FOLLOWER;

    // The time an AppendEntries was last sent or received
    state->watchdog = now;

    // The index of the current leader
    state->leader_idx = -1;

    // Index of the last committed operation
    state->commit_index = -1;

    // Index of the last operation applied to the state machine
    state->last_applied = -1;

    // Number of votes received in the current term
    state->votes_received = 0;

    // Nodes pick different election timeouts to reduce the risk
    // of split votes
    state->election_timeout = choose_election_timeout();

    for (int i = 0; i < NODE_LIMIT; i++) {
        state->next_indices[i] = 0;
        state->match_indices[i] = -1;
    }

    ///////////////////////////////////////////////////////////////
    // Initialize client table

    client_table_init(&state->client_table);
    state->next_client_tag = NODE_LIMIT;

    ///////////////////////////////////////////////////////////////
    // Initialize networking

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

    tcp_context_free(&state->tcp);
    client_table_free(&state->client_table);
    wal_free(&state->wal);
    state_machine_free(&state->state_machine);
    return 0;
}

int node_tick(void *state_, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout)
{
    NodeState *state = state_;

    Time now = get_current_time();
    if (now == INVALID_TIME)
        return -1;

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

            ret = process_message(state, conn_idx, msg_type, msg);
            if (ret < 0) {
                tcp_close(&state->tcp, conn_idx);
                break;
            }

            tcp_consume_message(&state->tcp, conn_idx);
        }
    }

    /////////////////////////////////////////////////////////////////
    // Time events

    Time deadline = INVALID_TIME;

    if (state->role == ROLE_LEADER) {
        Time watchdog_deadline = state->watchdog + HEARTBEAT_INTERVAL_SEC * 1000000000ULL;
        if (now >= watchdog_deadline) {
            NODE_TRACE("[" TIME_FMT "] node %d (LEADER): heartbeat timeout, sending APPEND_ENTRIES to all peers",
                TIME_VAL(now), self_idx(state));
            for (int i = 0; i < state->num_nodes; i++) {
                if (i != self_idx(state))
                    send_append_entries_to_peer(state, i);
            }
            state->watchdog = now;
        } else {
            nearest_deadline(&deadline, watchdog_deadline);
        }
    } else {
        // Follower/Candidate: start election on leader timeout
        Time death_deadline = state->watchdog + state->election_timeout;
        if (now >= death_deadline) {
            NODE_TRACE("[" TIME_FMT "] node %d (%s): election timeout expired, triggering election",
                TIME_VAL(now), self_idx(state), role_name(state->role));
            start_election(state);
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