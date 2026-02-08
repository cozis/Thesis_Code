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
    case MESSAGE_TYPE_REQUEST:              return "REQUEST";
    case MESSAGE_TYPE_REPLY:                return "REPLY";
    case MESSAGE_TYPE_PREPARE:              return "PREPARE";
    case MESSAGE_TYPE_PREPARE_OK:           return "PREPARE_OK";
    case MESSAGE_TYPE_COMMIT:               return "COMMIT";
    case MESSAGE_TYPE_BEGIN_VIEW_CHANGE:    return "BEGIN_VIEW_CHANGE";
    case MESSAGE_TYPE_DO_VIEW_CHANGE:       return "DO_VIEW_CHANGE";
    case MESSAGE_TYPE_BEGIN_VIEW:           return "BEGIN_VIEW";
    case MESSAGE_TYPE_RECOVERY:             return "RECOVERY";
    case MESSAGE_TYPE_RECOVERY_RESPONSE:    return "RECOVERY_RESPONSE";
    default:                                return "UNKNOWN";
    }
}

static const char *status_name(Status status)
{
    switch (status) {
    case STATUS_NORMAL:      return "NORMAL";
    case STATUS_CHANGE_VIEW: return "CHANGE_VIEW";
    case STATUS_RECOVERY:    return "RECOVERY";
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

static int leader_idx(NodeState *state)
{
    return state->view_number % state->num_nodes;
}

static bool is_leader(NodeState *state)
{
    return self_idx(state) == leader_idx(state);
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
    byte_queue_write(output, msg, msg->length - extra_len);
    byte_queue_write(output, extra, extra_len);
    return 0;
}

static int send_to_peer(NodeState *state, int peer_idx, MessageHeader *msg)
{
    return send_to_peer_ex(state, peer_idx, msg, NULL, 0);
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

// TODO: test this function
static int count_set(uint32_t word)
{
    int n = 0;
    for (int i = 0; i < sizeof(word) * 8; i++)
        if (word & (1 << i))
            n++;
    return n;
}

static int
process_message_as_leader(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    switch (type) {
    case MESSAGE_TYPE_REQUEST:
        {
            // Don't accept new requests during a view change.
            // The leader must complete the view change first.
            if (state->status != STATUS_NORMAL)
                break;

            RequestMessage request_message;

            if (msg.len != sizeof(request_message))
                return -1;
            memcpy(&request_message, msg.ptr, sizeof(request_message));

            // We must first add or update the client table to
            // invalidate the request ID. This makes it so any
            // subsequent requests with the same ID are rejected
            // while the first one is in progress.
            //
            // If the request ID is lower than the one stored in
            // the table, the request is rejected.
            //
            // If the request ID is the same as the one in the table
            // but no result was saved as the original one is still
            // in progress, the request is rejected.
            //
            // If the request ID is the same and a result is available,
            // it is returned immediately.
            {
                ClientTableEntry *entry = client_table_find(&state->client_table, request_message.client_id);
                if (entry == NULL) {

                    if (client_table_add(&state->client_table, request_message.client_id, request_message.request_id, conn_idx) < 0) {
                        ReplyMessage reply_message = {
                            .base = {
                                .version = MESSAGE_VERSION,
                                .type    = MESSAGE_TYPE_REPLY,
                                .length  = sizeof(ReplyMessage),
                            },
                            .rejected = true,
                        };

                        ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
                        assert(output);

                        byte_queue_write(output, &reply_message, sizeof(reply_message));
                        break;
                    }

                } else {

                    if (entry->pending)
                        break; // Only one pending operation per client is allowed. Ignore the message.

                    if (entry->last_request_id > request_message.request_id)
                        break; // Request is old. Ignore.

                    if (entry->last_request_id == request_message.request_id) {

                        // This request was already processed and its value was cached.
                        // Respond with the cached value.

                        ReplyMessage reply_message = {
                            .base = {
                                .version = MESSAGE_VERSION,
                                .type    = MESSAGE_TYPE_REPLY,
                                .length  = sizeof(ReplyMessage),
                            },
                            .rejected = false,
                            .result = entry->last_result,
                        };

                        ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
                        assert(output);

                        byte_queue_write(output, &reply_message, sizeof(reply_message));
                        break;
                    }

                    entry->last_request_id = request_message.request_id;
                    entry->pending = true;
                }
            }

            LogEntry log_entry = {
                .oper = request_message.oper,
                .votes = 1 << self_idx(state),
                .view_number = state->view_number,
                .client_id = request_message.client_id,
            };
            if (log_append(&state->log, log_entry) < 0) {
                assert(0); // TODO
            }

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (LEADER): REQUEST from client %lu (req_id=%lu), appended to log[%d], view=%lu",
                    TIME_VAL(now), self_idx(state),
                    (unsigned long)request_message.client_id,
                    (unsigned long)request_message.request_id,
                    state->log.count-1, (unsigned long)state->view_number);
            }

            PrepareMessage prepare_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_PREPARE,
                    .length  = sizeof(PrepareMessage),
                },
                .oper = request_message.oper,
                .sender_idx = self_idx(state),
                .log_index = state->log.count-1,
                .commit_index = state->commit_index,
                .view_number = state->view_number,
            };

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (LEADER) -> ALL: PREPARE log_index=%d commit_index=%d view=%lu",
                    TIME_VAL(now), self_idx(state),
                    state->log.count-1, state->commit_index,
                    (unsigned long)state->view_number);
            }

            if (broadcast_to_peers(state, &prepare_message.base) < 0) {
                assert(0);
            }

            // We forwarded the message to all peers. As soon as
            // we get enough PREPARE_OK responses, we'll commit
            // and reply to the client.
        }
        break;
    case MESSAGE_TYPE_PREPARE_OK:
        {
            PrepareOKMessage prepare_ok_message;

            if (msg.len != sizeof(prepare_ok_message))
                return -1;
            memcpy(&prepare_ok_message, msg.ptr, sizeof(prepare_ok_message));

            if (prepare_ok_message.view_number != state->view_number) {
                assert(0);
            }

            // TODO: check log_index
            int log_index = prepare_ok_message.log_index;
            if (log_index < 0 || log_index >= state->log.count) {
                assert(0); // TODO
            }

            // When the primary appends an entry to its log, it sends a
            // PREPARE message to all backups. Backups add the entry to
            // their own logs and reply with PREPARE_OK messages. When
            // the primary receives a quorum of PREPARE_OKs, it commits
            // the entry.
            //
            // In a reliable network and with no node crashes, we would
            // expect entries to be committed linearly in the log. If
            // log entries A and B are added to the log in that order,
            // we expect A to reach PREPARE_OK messages before B.
            //
            // Unfortunately, we must assume messages will be lost (*).
            // If that happens, instead of worrying about resending the
            // PREPARE_OK for that entry, we rely on the fact that OK
            // messages for future messages imply OK messages for the
            // previous ones. The first message for which a quorum of
            // OK messages is reached can work as an OK for all previous
            // entries.
            //
            // For this reason, we allow "holes" in the log and if an
            // entry reached quorum we advance the commit index to it.
            //
            // If the log index is lower than the log, it means we
            // received an OK message that was not necessary anymore
            // so we can ignore it.
            //
            // (*) This implementation uses TCP as a transport protocol,
            // which means messages will be retransmitted if lost on
            // the network. Nevertheless, if a node crashes while receiving
            // a node or we crash before sending it, the message will
            // be lost.

            if (log_index < state->commit_index)
                break; // Already processed
            LogEntry *entry = &state->log.entries[log_index];

            entry->votes |= 1 << prepare_ok_message.sender_idx;

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (LEADER): PREPARE_OK from node %d for log[%d], prep_oks=%d/%d view=%lu",
                    TIME_VAL(now), self_idx(state),
                    prepare_ok_message.sender_idx, log_index,
                    count_set(entry->votes), state->num_nodes/2 + 1,
                    (unsigned long)prepare_ok_message.view_number);
            }

            // Quorum reached for this log entry?
            if (count_set(entry->votes) > state->num_nodes/2) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (LEADER): quorum reached for log[%d], advancing commit_index %d -> %d",
                        TIME_VAL(now), self_idx(state),
                        log_index, state->commit_index, log_index + 1);
                }

                // Okay, we can advance the commit index to here
                while (state->commit_index <= log_index) {

                    uint64_t  client_id = state->log.entries[state->commit_index].client_id;
                    Operation oper      = state->log.entries[state->commit_index].oper;
                    state->commit_index++;

                    OperationResult result = state_machine_update(&state->state_machine, oper);

                    ClientTableEntry *table_entry = client_table_find(&state->client_table, client_id);
                    if (table_entry) {

                        assert(table_entry->pending);
                        table_entry->pending = false;
                        table_entry->last_result = result;

                        ReplyMessage reply_message = {
                            .base = {
                                .version = MESSAGE_VERSION,
                                .type    = MESSAGE_TYPE_REPLY,
                                .length  = sizeof(ReplyMessage),
                            },
                            .result = result,
                        };

                        ByteQueue *output = tcp_output_buffer(&state->tcp, table_entry->conn_idx);
                        assert(output);

                        byte_queue_write(output, &reply_message, sizeof(reply_message));
                    }
                }
            }
        }
        break;
    case MESSAGE_TYPE_DO_VIEW_CHANGE:
        {
            DoViewChangeMessage do_view_change_message;
            if (msg.len < sizeof(do_view_change_message))
                return -1;
            memcpy(&do_view_change_message, msg.ptr, sizeof(do_view_change_message));

            // Parse the variable-sized log from the message
            int num_entries = (msg.len - sizeof(DoViewChangeMessage)) / sizeof(LogEntry);
            if (num_entries != do_view_change_message.op_number)
                return -1; // Message size mismatch

            // Only process if the view matches what we're transitioning to
            if (do_view_change_message.view_number != state->view_number)
                break;

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d: DO_VIEW_CHANGE from node %d, view=%lu old_view=%lu op_number=%d commit_index=%d",
                    TIME_VAL(now), self_idx(state),
                    do_view_change_message.sender_idx,
                    (unsigned long)do_view_change_message.view_number,
                    (unsigned long)do_view_change_message.old_view_number,
                    do_view_change_message.op_number,
                    do_view_change_message.commit_index);
            }

            // Track this vote
            uint32_t vote_mask = 1 << do_view_change_message.sender_idx;
            if ((state->do_view_change_votes & vote_mask) == 0) {

                state->do_view_change_votes |= vote_mask;

                // Check if this log is better than the one we have stored
                // Best log: highest old_view_number, then highest op_number
                bool is_better = (do_view_change_message.old_view_number > state->do_view_change_best_old_view) ||
                                 (do_view_change_message.old_view_number == state->do_view_change_best_old_view &&
                                  do_view_change_message.op_number > state->do_view_change_best_log.count);

                if (is_better) {
                    state->do_view_change_best_old_view = do_view_change_message.old_view_number;

                    // Copy the log entries from the message
                    LogEntry *entries = malloc(num_entries * sizeof(LogEntry));
                    if (entries == NULL) {
                        assert(0);
                    }
                    memcpy(entries, (uint8_t*)msg.ptr + sizeof(DoViewChangeMessage), num_entries * sizeof(LogEntry));

                    log_free(&state->do_view_change_best_log);
                    state->do_view_change_best_log.count = num_entries;
                    state->do_view_change_best_log.capacity = num_entries;
                    state->do_view_change_best_log.entries = entries;
                }

                // Track the maximum commit index
                if (do_view_change_message.commit_index > state->do_view_change_best_commit) {
                    state->do_view_change_best_commit = do_view_change_message.commit_index;
                }
            }

            // Count votes
            int vote_count = 0;
            for (int i = 0; i < state->num_nodes; i++) {
                if (state->do_view_change_votes & (1 << i)) vote_count++;
            }

            // Need f + 1 DoViewChange messages (including own)
            if (vote_count >= state->num_nodes / 2 + 1) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d: view change complete, becoming LEADER for view %lu (votes=%d/%d)",
                        TIME_VAL(now), self_idx(state),
                        (unsigned long)state->view_number,
                        vote_count, state->num_nodes);
                }

                // Use the best log we collected
                log_free(&state->log);
                state->log = state->do_view_change_best_log;
                state->do_view_change_best_log = (Log){0}; // Clear to avoid double-free

                state->commit_index = state->do_view_change_best_commit;
                state->status = STATUS_NORMAL;

                // Reset vote tracking for uncommitted entries. The log
                // entries inherited from DO_VIEW_CHANGE have stale
                // votes from the previous view. The new leader starts
                // with its own vote for each entry.
                for (int i = state->commit_index; i < state->log.count; i++)
                    state->log.entries[i].votes = 1 << self_idx(state);

                BeginViewMessage begin_view_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_BEGIN_VIEW,
                        .length  = sizeof(BeginViewMessage) + state->log.count * sizeof(LogEntry),
                    },
                    .view_number = state->view_number,
                    .commit_index = state->commit_index,
                    .op_number = state->log.count,
                };

                if (broadcast_to_peers_ex(state, &begin_view_message.base, state->log.entries, state->log.count * sizeof(LogEntry)) < 0) {
                    assert(0);
                }

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (LEADER) -> ALL: BEGIN_VIEW view=%lu commit_index=%d log_count=%d",
                        TIME_VAL(now), self_idx(state),
                        (unsigned long)state->view_number,
                        state->commit_index, state->log.count);
                }

                // Reset view change state for next time
                state->do_view_change_votes = 0;
                state->do_view_change_best_old_view = 0;
                state->do_view_change_best_commit = 0;
            }
        }
        break;
    case MESSAGE_TYPE_RECOVERY:
        {
            if (state->status != STATUS_NORMAL)
                break; // Ignore message.

            RecoveryMessage recovery_message;
            if (msg.len != sizeof(RecoveryMessage))
                return -1;
            memcpy(&recovery_message, msg.ptr, sizeof(recovery_message));

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (LEADER): RECOVERY from node %d (nonce=%lu), sending RECOVERY_RESPONSE with log_count=%d",
                    TIME_VAL(now), self_idx(state),
                    recovery_message.sender_idx,
                    (unsigned long)recovery_message.nonce,
                    state->log.count);
            }

            RecoveryResponseMessage recovery_response_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_RECOVERY_RESPONSE,
                    .length  = sizeof(RecoveryResponseMessage) + state->log.count * sizeof(LogEntry),
                },
                .view_number = state->view_number,
                .op_number = state->log.count-1, // TODO: What if the log is empty?
                .nonce = recovery_message.nonce,
                .commit_index = state->commit_index,
                .sender_idx = self_idx(state),
            };

            if (send_to_peer_ex(state, recovery_message.sender_idx, &recovery_response_message.base, state->log.entries, state->log.count * sizeof(LogEntry)) < 0) {
                assert(0);
            }
        }
        break;
    case MESSAGE_TYPE_BEGIN_VIEW_CHANGE:
        {
            if (state->status == STATUS_RECOVERY) break;

            BeginViewChangeMessage begin_view_change_message;
            if (msg.len != sizeof(BeginViewChangeMessage))
                return -1;
            memcpy(&begin_view_change_message, msg.ptr, sizeof(begin_view_change_message));

            if (begin_view_change_message.view_number > state->view_number) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (LEADER): BEGIN_VIEW_CHANGE from node %d, transitioning view %lu -> %lu",
                        TIME_VAL(now), self_idx(state),
                        begin_view_change_message.sender_idx,
                        (unsigned long)state->view_number,
                        (unsigned long)begin_view_change_message.view_number);
                }

                // Reset vote tracking when transitioning to a new view
                state->begin_view_change_votes = 0;
                state->view_number = begin_view_change_message.view_number;
                state->status = STATUS_CHANGE_VIEW;

                // Send our own BeginViewChange to all peers
                BeginViewChangeMessage own_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_BEGIN_VIEW_CHANGE,
                        .length  = sizeof(BeginViewChangeMessage),
                    },
                    .view_number = state->view_number,
                    .sender_idx = self_idx(state),
                };

                if (broadcast_to_peers(state, &own_message.base) < 0) {
                    assert(0);
                }
                // Count our own vote
                state->begin_view_change_votes |= (1 << self_idx(state));
            }

            // Track this sender's vote (only if view matches)
            if (begin_view_change_message.view_number == state->view_number) {
                state->begin_view_change_votes |= (1 << begin_view_change_message.sender_idx);
            }

            // Count votes
            int vote_count = 0;
            for (int i = 0; i < state->num_nodes; i++) {
                if (state->begin_view_change_votes & (1 << i)) vote_count++;
            }

            // Need f + 1 votes to send DoViewChange
            if (vote_count >= state->num_nodes / 2 + 1) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (LEADER): f+1 BEGIN_VIEW_CHANGE votes reached (%d/%d), sending DO_VIEW_CHANGE to node %d for view %lu",
                        TIME_VAL(now), self_idx(state),
                        vote_count, state->num_nodes,
                        (int)(state->view_number % state->num_nodes),
                        (unsigned long)state->view_number);
                }

                DoViewChangeMessage do_view_change_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_DO_VIEW_CHANGE,
                        .length  = sizeof(DoViewChangeMessage) + state->log.count * sizeof(LogEntry),
                    },
                    .view_number = state->view_number,
                    .old_view_number = state->view_number - 1,  // View before we started view change
                    .op_number = state->log.count,
                    .commit_index = state->commit_index,
                    .sender_idx = self_idx(state),
                };
                if (leader_idx(state) == self_idx(state)) {
                    // We are the new leader: count our own vote directly
                    // since send_to_peer_ex skips self-sends.
                    state->do_view_change_votes |= (1 << self_idx(state));
                } else {
                    if (send_to_peer_ex(state, leader_idx(state), &do_view_change_message.base, state->log.entries, state->log.count * sizeof(LogEntry)) < 0) {
                        assert(0);
                    }
                }

                // Clear the future array since we're changing views
                state->num_future = 0;
            }
        }
        break;
    case MESSAGE_TYPE_BEGIN_VIEW:
        {
            BeginViewMessage message;

            if (msg.len < sizeof(message))
                return -1;
            memcpy(&message, msg.ptr, sizeof(message));

            // Only process messages containing a view-number
            // that matches or advances the one we know.
            if (message.view_number < state->view_number)
                break;

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (LEADER): BEGIN_VIEW received, stepping down and adopting view %lu (commit_index=%d, log_entries=%d)",
                    TIME_VAL(now), self_idx(state),
                    (unsigned long)message.view_number,
                    message.commit_index, message.op_number);
            }

            // Update view and status
            state->view_number = message.view_number;
            state->status = STATUS_NORMAL; // Paper state: change status to normal

            // Replace the local log with the authoritative log from the primary
            {
                int num_entries = (msg.len - sizeof(BeginViewMessage)) / sizeof(LogEntry);
                LogEntry *entries = malloc(num_entries * sizeof(LogEntry));
                if (entries == NULL) {
                    assert(0);
                }
                memcpy(entries, (uint8_t*)msg.ptr + sizeof(BeginViewMessage), num_entries * sizeof(LogEntry));

                log_free(&state->log);
                state->log.count = num_entries;
                state->log.capacity = num_entries;
                state->log.entries = entries;
            }

            // Reset view change state
            state->begin_view_change_votes = 0;

            // If there are non-committed operations in the log,
            // send a PREPAREOK to the new primary
            if (state->log.count > message.commit_index) {
                PrepareOKMessage ok_msg = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_PREPARE_OK,
                        .length  = sizeof(PrepareOKMessage),
                    },
                    .sender_idx = self_idx(state),
                    .log_index  = state->log.count - 1,
                    .view_number = state->view_number,
                };
                if (send_to_peer(state, leader_idx(state), &ok_msg.base) < 0) {
                    assert(0);
                }
            }

            // Execute all operations known to be committed that haven't
            // been executed previously
            while (state->commit_index < message.commit_index && state->commit_index < state->log.count) {
                state_machine_update(&state->state_machine, state->log.entries[state->commit_index].oper);
                state->commit_index++;
            }

            Time now = get_current_time();
            if (now == INVALID_TIME) {
                assert(0);
            }
            state->last_heartbeat_time = now;
        }
        break;
    case MESSAGE_TYPE_RECOVERY_RESPONSE:
        {
            RecoveryResponseMessage message;

            if (msg.len < sizeof(message))
                return -1;
            memcpy(&message, msg.ptr, sizeof(message));

            // 1. Only process responses if we are actually in the recovering state
            // 2. Ensure the nonce matches the one we sent to prevent accepting
            //    delayed responses from previous recovery attempts.
            if (state->status != STATUS_RECOVERY || message.nonce != state->recovery_nonce) {
                break;
            }

            // Track this response
            state->recovery_votes |= (1 << message.sender_idx);

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (RECOVERY): RECOVERY_RESPONSE from node %d, view=%lu op_number=%d commit_index=%d",
                    TIME_VAL(now), self_idx(state),
                    message.sender_idx,
                    (unsigned long)message.view_number,
                    message.op_number, message.commit_index);
            }

            // Track the highest view number we've seen and who the primary is for it
            if (message.view_number > state->max_view_seen) {
                state->max_view_seen = message.view_number;
                state->latest_primary_idx = (message.view_number % state->num_nodes);
            }

            // Store the state from the primary if this message is from them
            if (message.sender_idx == (int) (message.view_number % state->num_nodes)) {
                // Copy the log from the variable-sized portion of the message
                int num_entries = (msg.len - sizeof(RecoveryResponseMessage)) / sizeof(LogEntry);
                LogEntry *entries = malloc(num_entries * sizeof(LogEntry));
                if (entries == NULL) {
                    assert(0);
                }
                memcpy(entries, (uint8_t*)msg.ptr + sizeof(RecoveryResponseMessage), num_entries * sizeof(LogEntry));

                log_free(&state->potential_primary_log);
                state->potential_primary_log.count = num_entries;
                state->potential_primary_log.capacity = num_entries;
                state->potential_primary_log.entries = entries;

                state->received_primary_state = true;
            }

            // Check if we have f + 1 responses
            int response_count = 0;
            for (int i = 0; i < state->num_nodes; i++) {
                if (state->recovery_votes & (1 << i)) response_count++;
            }

            // The threshold f is derived from 2f + 1 = num_nodes
            int f = (state->num_nodes - 1) / 2;

            if (response_count >= f + 1 && state->received_primary_state) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d: recovery complete (responses=%d, f+1=%d), adopting view %lu",
                        TIME_VAL(now), self_idx(state),
                        response_count, f + 1,
                        (unsigned long)state->max_view_seen);
                }

                // Update state using information from the primary
                state->view_number = state->max_view_seen;

                // Move the log (no need to copy since we're transferring ownership)
                log_free(&state->log);
                state->log = state->potential_primary_log;
                state->potential_primary_log = (Log){0}; // Clear to avoid double-free

                state->commit_index = message.commit_index;

                // Change status to normal; the recovery protocol is complete
                state->status = STATUS_NORMAL;

                // Update heartbeat to avoid immediate view change timeout
                Time now = get_current_time();
                if (now == INVALID_TIME) {
                    assert(0);
                }
                state->last_heartbeat_time = now;
            }
        }
        break;
    default:
        break;
    }
    return 0;
}

static int
process_message_as_replica(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    (void) conn_idx;

    switch (type) {
    case MESSAGE_TYPE_REQUEST:
        {
            // Do nothing. Replicas ignore client requests.
        }
        break;
    case MESSAGE_TYPE_PREPARE:
        {
            PrepareMessage prepare_message;
            if (msg.len != sizeof(prepare_message))
                return -1;
            memcpy(&prepare_message, msg.ptr, sizeof(prepare_message));

            // Stale peer
            if (prepare_message.view_number < state->view_number)
                break;

            if (prepare_message.view_number > state->view_number) {
                // The new leader has started a view we haven't seen.
                // Adopt the new view and process the PREPARE normally.
                {
                    Time t = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (REPLICA): adopting view %lu from PREPARE (was view %lu, status %s)",
                        TIME_VAL(t), self_idx(state),
                        (unsigned long)prepare_message.view_number,
                        (unsigned long)state->view_number,
                        status_name(state->status));
                }
                state->view_number = prepare_message.view_number;
                state->status = STATUS_NORMAL;
                state->begin_view_change_votes = 0;
                state->num_future = 0;
            }

            if (prepare_message.log_index > state->log.count) {
                // The prepare message for a previous log entry was not received yet.
                // Buffer this message to process it later
                if (state->num_future == FUTURE_LIMIT) {
                    assert(0); // TODO
                }
                state->future[state->num_future++] = prepare_message;

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (REPLICA): PREPARE log_index=%d buffered (expected %d), num_future=%d",
                        TIME_VAL(now), self_idx(state),
                        prepare_message.log_index, state->log.count,
                        state->num_future);
                }

            } else if (prepare_message.log_index < state->log.count) {
                // Message refers to an old entry. Ignore.
            } else {

                LogEntry log_entry = {
                    .oper = prepare_message.oper,
                    .votes = 0,
                    .view_number = state->view_number,
                };

                if (log_append(&state->log, log_entry) < 0) {
                    assert(0); // TODO
                }

                PrepareOKMessage prepare_ok_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_PREPARE_OK,
                        .length  = sizeof(PrepareOKMessage),
                    },
                    .sender_idx = self_idx(state),
                    .log_index  = state->log.count-1,
                    .view_number = state->view_number,
                };

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (REPLICA) -> node %d: PREPARE_OK log_index=%d view=%lu",
                        TIME_VAL(now), self_idx(state),
                        prepare_message.sender_idx,
                        state->log.count-1,
                        (unsigned long)state->view_number);
                }

                if (send_to_peer(state, prepare_message.sender_idx, &prepare_ok_message.base) < 0) {
                    assert(0);
                }

                // Now try to process future log
                bool processed_at_least_one;
                do {
                    processed_at_least_one = false;
                    for (int i = 0; i < state->num_future; i++) {

                        if (state->future[i].log_index < state->log.count) {
                            state->future[i--] = state->future[--state->num_future];
                            continue;
                        }

                        if (state->future[i].log_index == state->log.count) {

                            LogEntry future_log_entry = {
                                .oper = state->future[i].oper,
                                .votes = 0,
                                .view_number = state->view_number,
                            };
                            if (log_append(&state->log, future_log_entry) < 0) {
                                assert(0); // TODO
                            }

                            PrepareOKMessage prepare_ok_message = {
                                .base = {
                                    .version = MESSAGE_VERSION,
                                    .type    = MESSAGE_TYPE_PREPARE_OK,
                                    .length  = sizeof(PrepareOKMessage),
                                },
                                .sender_idx  = self_idx(state),
                                .log_index   = state->log.count-1,
                                .view_number = state->view_number,
                            };
                            if (send_to_peer(state, state->future[i].sender_idx, &prepare_ok_message.base) < 0) {
                                assert(0);
                            }

                            processed_at_least_one = true;
                            break;
                        }
                    }
                } while (processed_at_least_one);

                while (state->commit_index < prepare_message.commit_index && state->commit_index < state->log.count) {
                    state_machine_update(&state->state_machine, state->log.entries[state->commit_index].oper);
                    state->commit_index++;
                }

                Time now = get_current_time();
                if (now == INVALID_TIME) {
                    assert(0);
                }
                state->last_heartbeat_time = now;
            }
        }
        break;
    case MESSAGE_TYPE_COMMIT:
        {
            CommitMessage message;

            if (msg.len != sizeof(CommitMessage))
                return -1;
            memcpy(&message, msg.ptr, sizeof(message));

            // Don't process heartbeats from a stale leader during
            // a view change, as it would reset the retry timer and
            // prevent the view change from completing.
            if (state->status == STATUS_CHANGE_VIEW)
                break;

            if (message.commit_index > state->commit_index) {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (REPLICA): COMMIT advancing commit_index %d -> %d",
                    TIME_VAL(now), self_idx(state),
                    state->commit_index, message.commit_index);
            }

            while (state->commit_index < message.commit_index && state->commit_index < state->log.count) {
                state_machine_update(&state->state_machine, state->log.entries[state->commit_index].oper);
                state->commit_index++;
            }

            Time now = get_current_time();
            if (now == INVALID_TIME) {
                assert(0);
            }
            state->last_heartbeat_time = now;
        }
        break;
    case MESSAGE_TYPE_BEGIN_VIEW_CHANGE:
        {
            if (state->status == STATUS_RECOVERY) break;

            BeginViewChangeMessage begin_view_change_message;
            if (msg.len != sizeof(BeginViewChangeMessage))
                return -1;
            memcpy(&begin_view_change_message, msg.ptr, sizeof(begin_view_change_message));

            if (begin_view_change_message.view_number > state->view_number) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (REPLICA): BEGIN_VIEW_CHANGE from node %d, transitioning view %lu -> %lu",
                        TIME_VAL(now), self_idx(state),
                        begin_view_change_message.sender_idx,
                        (unsigned long)state->view_number,
                        (unsigned long)begin_view_change_message.view_number);
                }

                // Reset vote tracking when transitioning to a new view
                state->begin_view_change_votes = 0;
                state->view_number = begin_view_change_message.view_number;
                state->status = STATUS_CHANGE_VIEW;

                // Send our own BeginViewChange to all peers
                BeginViewChangeMessage own_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_BEGIN_VIEW_CHANGE,
                        .length  = sizeof(BeginViewChangeMessage),
                    },
                    .view_number = state->view_number,
                    .sender_idx = self_idx(state),
                };

                if (broadcast_to_peers(state, &own_message.base) < 0) {
                    assert(0);
                }
                // Count our own vote
                state->begin_view_change_votes |= (1 << self_idx(state));
            }

            // Track this sender's vote (only if view matches)
            if (begin_view_change_message.view_number == state->view_number) {
                state->begin_view_change_votes |= (1 << begin_view_change_message.sender_idx);
            }

            // Count votes
            int vote_count = 0;
            for (int i = 0; i < state->num_nodes; i++) {
                if (state->begin_view_change_votes & (1 << i)) vote_count++;
            }

            // Need f + 1 votes to send DoViewChange
            if (vote_count >= state->num_nodes / 2 + 1) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d (REPLICA): f+1 BEGIN_VIEW_CHANGE votes reached (%d/%d), sending DO_VIEW_CHANGE to node %d for view %lu",
                        TIME_VAL(now), self_idx(state),
                        vote_count, state->num_nodes,
                        (int)(state->view_number % state->num_nodes),
                        (unsigned long)state->view_number);
                }

                DoViewChangeMessage do_view_change_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_DO_VIEW_CHANGE,
                        .length  = sizeof(DoViewChangeMessage) + state->log.count * sizeof(LogEntry),
                    },
                    .view_number = state->view_number,
                    .old_view_number = state->view_number - 1,  // View before we started view change
                    .op_number = state->log.count,
                    .commit_index = state->commit_index,
                    .sender_idx = self_idx(state),
                };
                if (leader_idx(state) == self_idx(state)) {
                    // We are the new leader: count our own vote directly
                    // since send_to_peer_ex skips self-sends.
                    state->do_view_change_votes |= (1 << self_idx(state));
                } else {
                    if (send_to_peer_ex(state, leader_idx(state), &do_view_change_message.base, state->log.entries, state->log.count * sizeof(LogEntry)) < 0) {
                        assert(0);
                    }
                }

                // Clear the future array since we're changing views
                state->num_future = 0;
            }
        }
        break;
    case MESSAGE_TYPE_BEGIN_VIEW:
        {
            BeginViewMessage message;

            if (msg.len < sizeof(message))
                return -1;
            memcpy(&message, msg.ptr, sizeof(message));

            // Replicas only process messages containing a view-number
            // that matches or advances the one they know[cite: 738, 812].
            if (message.view_number < state->view_number)
                break;

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (REPLICA): BEGIN_VIEW received, adopting view %lu (commit_index=%d, log_entries=%d)",
                    TIME_VAL(now), self_idx(state),
                    (unsigned long)message.view_number,
                    message.commit_index, message.op_number);
            }

            // Update view and status
            state->view_number = message.view_number;
            state->status = STATUS_NORMAL; // Paper state: change status to normal

            // Replace the local log with the authoritative log from the primary
            {
                int num_entries = (msg.len - sizeof(BeginViewMessage)) / sizeof(LogEntry);
                LogEntry *entries = malloc(num_entries * sizeof(LogEntry));
                if (entries == NULL) {
                    assert(0);
                }
                memcpy(entries, (uint8_t*)msg.ptr + sizeof(BeginViewMessage), num_entries * sizeof(LogEntry));

                log_free(&state->log);
                state->log.count = num_entries;
                state->log.capacity = num_entries;
                state->log.entries = entries;
            }

            // Reset view change state
            state->begin_view_change_votes = 0;

            // If there are non-committed operations in the log,
            // send a PREPAREOK to the new primary
            if (state->log.count > message.commit_index) {
                PrepareOKMessage ok_msg = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_PREPARE_OK,
                        .length  = sizeof(PrepareOKMessage),
                    },
                    .sender_idx = self_idx(state),
                    .log_index  = state->log.count - 1,
                    .view_number = state->view_number,
                };
                if (send_to_peer(state, leader_idx(state), &ok_msg.base) < 0) {
                    assert(0);
                }
            }

            // Execute all operations known to be committed that haven't
            // been executed previously
            while (state->commit_index < message.commit_index && state->commit_index < state->log.count) {
                state_machine_update(&state->state_machine, state->log.entries[state->commit_index].oper);
                state->commit_index++;
            }

            Time now = get_current_time();
            if (now == INVALID_TIME) {
                assert(0);
            }
            state->last_heartbeat_time = now;
        }
        break;
    case MESSAGE_TYPE_RECOVERY:
        {
            if (state->status != STATUS_NORMAL)
                break; // Ignore message.

            RecoveryMessage recovery_message;
            if (msg.len != sizeof(RecoveryMessage))
                return -1;
            memcpy(&recovery_message, msg.ptr, sizeof(recovery_message));

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (REPLICA): RECOVERY from node %d (nonce=%lu), sending RECOVERY_RESPONSE",
                    TIME_VAL(now), self_idx(state),
                    recovery_message.sender_idx,
                    (unsigned long)recovery_message.nonce);
            }

            RecoveryResponseMessage recovery_response_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_RECOVERY_RESPONSE,
                    .length  = sizeof(RecoveryResponseMessage),
                },
                .view_number = state->view_number,
                .op_number = state->log.count-1, // TODO: What if the log is empty?
                .nonce = recovery_message.nonce,
                .commit_index = state->commit_index,
                .sender_idx = self_idx(state),
            };

            if (send_to_peer(state, recovery_message.sender_idx, &recovery_response_message.base) < 0) {
                assert(0);
            }
        }
        break;
    case MESSAGE_TYPE_RECOVERY_RESPONSE:
        {
            RecoveryResponseMessage message;

            if (msg.len < sizeof(message))
                return -1;
            memcpy(&message, msg.ptr, sizeof(message));

            // 1. Only process responses if we are actually in the recovering state [cite: 237]
            // 2. Ensure the nonce matches the one we sent to prevent accepting
            //    delayed responses from previous recovery attempts[cite: 253].
            if (state->status != STATUS_RECOVERY || message.nonce != state->recovery_nonce) {
                break;
            }

            // Track this response (e.g., in a bitmask or array)
            state->recovery_votes |= (1 << message.sender_idx);

            {
                Time now = get_current_time();
                NODE_TRACE("[" TIME_FMT "] node %d (RECOVERY): RECOVERY_RESPONSE from node %d, view=%lu op_number=%d commit_index=%d",
                    TIME_VAL(now), self_idx(state),
                    message.sender_idx,
                    (unsigned long)message.view_number,
                    message.op_number, message.commit_index);
            }

            // Track the highest view number we've seen and who the primary is for it
            if (message.view_number > state->max_view_seen) {
                state->max_view_seen = message.view_number;
                state->latest_primary_idx = (message.view_number % state->num_nodes);
            }

            // Store the state from the primary if this message is from them
            if (message.sender_idx == (int) (message.view_number % state->num_nodes)) {
                // Copy the log from the variable-sized portion of the message
                int num_entries = (msg.len - sizeof(RecoveryResponseMessage)) / sizeof(LogEntry);
                LogEntry *entries = malloc(num_entries * sizeof(LogEntry));
                if (entries == NULL) {
                    assert(0);
                }
                memcpy(entries, (uint8_t*)msg.ptr + sizeof(RecoveryResponseMessage), num_entries * sizeof(LogEntry));

                log_free(&state->potential_primary_log);
                state->potential_primary_log.count = num_entries;
                state->potential_primary_log.capacity = num_entries;
                state->potential_primary_log.entries = entries;

                state->received_primary_state = true;
            }

            // Check if we have f + 1 responses
            int response_count = 0;
            for (int i = 0; i < state->num_nodes; i++) {
                if (state->recovery_votes & (1 << i)) response_count++;
            }

            // The threshold f is derived from 2f + 1 = num_nodes [cite: 66, 73]
            int f = (state->num_nodes - 1) / 2;

            if (response_count >= f + 1 && state->received_primary_state) {

                {
                    Time now = get_current_time();
                    NODE_TRACE("[" TIME_FMT "] node %d: recovery complete (responses=%d, f+1=%d), adopting view %lu",
                        TIME_VAL(now), self_idx(state),
                        response_count, f + 1,
                        (unsigned long)state->max_view_seen);
                }

                // Update state using information from the primary
                state->view_number = state->max_view_seen;

                // Move the log (no need to copy since we're transferring ownership)
                log_free(&state->log);
                state->log = state->potential_primary_log;
                state->potential_primary_log = (Log){0}; // Clear to avoid double-free

                state->commit_index = message.commit_index;

                // Change status to normal; the recovery protocol is complete
                state->status = STATUS_NORMAL;

                // Update heartbeat to avoid immediate view change timeout
                Time now = get_current_time();
                if (now == INVALID_TIME) {
                    assert(0);
                }
                state->last_heartbeat_time = now;
            }
        }
        break;
    default:
        break;
    }
    return 0;
}

static int
process_message(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    Time now = get_current_time();
    NODE_TRACE("[" TIME_FMT "] node %d (%s, %s) <- conn %d: recv %s (%d bytes)",
        TIME_VAL(now), self_idx(state),
        is_leader(state) ? "LEADER" : "REPLICA",
        status_name(state->status),
        conn_idx, message_type_name(type), (int)msg.len);

    // Tag incoming connections with the sender's node index so that
    // the connection can be used bidirectionally. Without this, when
    // node A connects to node B and sends a message, node B can't
    // send back to node A through the same connection (the tag is
    // only set on the connector's side).
    {
        int sender_idx = -1;
        switch (type) {
        case MESSAGE_TYPE_PREPARE:
            if (msg.len >= sizeof(PrepareMessage)) {
                PrepareMessage m; memcpy(&m, msg.ptr, sizeof(m));
                sender_idx = m.sender_idx;
            }
            break;
        case MESSAGE_TYPE_PREPARE_OK:
            if (msg.len >= sizeof(PrepareOKMessage)) {
                PrepareOKMessage m; memcpy(&m, msg.ptr, sizeof(m));
                sender_idx = m.sender_idx;
            }
            break;
        case MESSAGE_TYPE_BEGIN_VIEW_CHANGE:
            if (msg.len >= sizeof(BeginViewChangeMessage)) {
                BeginViewChangeMessage m; memcpy(&m, msg.ptr, sizeof(m));
                sender_idx = m.sender_idx;
            }
            break;
        case MESSAGE_TYPE_DO_VIEW_CHANGE:
            if (msg.len >= sizeof(DoViewChangeMessage)) {
                DoViewChangeMessage m; memcpy(&m, msg.ptr, sizeof(m));
                sender_idx = m.sender_idx;
            }
            break;
        case MESSAGE_TYPE_RECOVERY:
            if (msg.len >= sizeof(RecoveryMessage)) {
                RecoveryMessage m; memcpy(&m, msg.ptr, sizeof(m));
                sender_idx = m.sender_idx;
            }
            break;
        case MESSAGE_TYPE_RECOVERY_RESPONSE:
            if (msg.len >= sizeof(RecoveryResponseMessage)) {
                RecoveryResponseMessage m; memcpy(&m, msg.ptr, sizeof(m));
                sender_idx = m.sender_idx;
            }
            break;
        }
        if (sender_idx >= 0 && sender_idx < state->num_nodes) {
            int existing = tcp_index_from_tag(&state->tcp, sender_idx);
            if (existing < 0) {
                // No connection tagged with this peer yet, tag this one
                tcp_set_tag(&state->tcp, conn_idx, sender_idx, false);
            } else if (existing != conn_idx) {
                // A different (possibly stale) connection has this tag.
                // Close the old one and tag the current one.
                tcp_close(&state->tcp, existing);
                tcp_set_tag(&state->tcp, conn_idx, sender_idx, false);
            }
            // If existing == conn_idx, already tagged correctly
        }
    }

    if (is_leader(state)) {
        return process_message_as_leader(state, conn_idx, type, msg);
    } else {
        return process_message_as_replica(state, conn_idx, type, msg);
    }
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
            // TODO: Check address is not duplicated
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
            // TODO: Check address is not duplicated
            int ret = parse_addr_arg(argv[i], &state->node_addrs[state->num_nodes]);
            if (ret < 0) {
                fprintf(stderr, "Malformed <addr>:<port> pair for --peer option\n");
                return -1;
            }
            state->num_nodes++;
        } else {
            printf("Ignoring option '%s'\n", argv[i]);
        }
    }

    // Now sort the addresses
    addr_sort(state->node_addrs, state->num_nodes);

    Time deadline = INVALID_TIME;

    state->view_number = 0;
    state->last_heartbeat_time = now;
    state->commit_index = 0;
    state->num_future = 0;

    // View change state
    state->begin_view_change_votes = 0;
    state->do_view_change_votes = 0;
    state->do_view_change_best_old_view = 0;
    state->do_view_change_best_commit = 0;
    log_init(&state->do_view_change_best_log);

    // Recovery state
    state->recovery_votes = 0;
    state->max_view_seen = 0;
    state->latest_primary_idx = 0;
    state->received_primary_state = false;
    state->recovery_attempt_count = 0;
    log_init(&state->potential_primary_log);

    // Detect whether this is a restart after a crash by checking for a
    // boot marker file on disk. The disk persists across crashes, so if
    // the marker exists, this node previously ran and crashed. In that
    // case, enter recovery mode to learn the current view from peers
    // before participating in the protocol.
    //
    // We use open() directly (without O_CREAT) instead of file_exists()
    // because access() is not available in the simulation environment.
    int marker_fd = open("vsr_boot_marker", O_RDONLY, 0);
    bool previously_crashed = (marker_fd >= 0);
    if (previously_crashed)
        close(marker_fd);
    if (previously_crashed) {
        state->status = STATUS_RECOVERY;
        state->recovery_nonce = now;
        state->recovery_start_time = now;
    } else {
        state->status = STATUS_NORMAL;
    }

    client_table_init(&state->client_table);

    state_machine_init(&state->state_machine);

    if (tcp_context_init(&state->tcp) < 0) {
        fprintf(stderr, "Node :: Couldn't setup TCP context\n");
        return -1;
    }

    int ret = tcp_listen(&state->tcp, state->self_addr);
    if (ret < 0) {
        fprintf(stderr, "Node :: Couldn't setup TCP listener\n");
        tcp_context_free(&state->tcp);
        return -1;
    }

    log_init(&state->log);

    // Write the boot marker to disk so that future restarts can detect
    // a previous crash. This must happen after TCP init so that the
    // marker is only written if the node successfully started.
    if (!previously_crashed) {
        int fd = open("vsr_boot_marker", O_WRONLY | O_CREAT, 0644);
        if (fd >= 0)
            close(fd);
    }

    if (previously_crashed) {
        // Broadcast RECOVERY to all peers to learn the current view
        RecoveryMessage recovery_message = {
            .base = {
                .version = MESSAGE_VERSION,
                .type    = MESSAGE_TYPE_RECOVERY,
                .length  = sizeof(RecoveryMessage),
            },
            .sender_idx = self_idx(state),
            .nonce = state->recovery_nonce,
        };

        if (broadcast_to_peers(state, &recovery_message.base) < 0) {
            assert(0); // TODO
        }

        nearest_deadline(&deadline, state->recovery_start_time + RECOVERY_TIMEOUT_SEC * 1000000000ULL);
    }

    NODE_TRACE("[" TIME_FMT "] node %d initialized: %s, view=%lu, num_nodes=%d, status=%s",
        TIME_VAL(now), self_idx(state),
        is_leader(state) ? "LEADER" : "REPLICA",
        (unsigned long)state->view_number,
        state->num_nodes,
        status_name(state->status));

    *timeout = deadline_to_timeout(deadline, now);
    if (pcap < TCP_POLL_CAPACITY) {
        fprintf(stderr, "Node :: Not enough poll() capacity (got %d, needed %d)\n", pcap, TCP_POLL_CAPACITY);
        return -1;
    }
    *pnum = tcp_register_events(&state->tcp, ctxs, pdata);
    return 0;
}

int node_tick(void *state_, void **ctxs,
    struct pollfd *pdata, int pcap, int *pnum, int *timeout)
{
    NodeState *state = state_;

    Time now = get_current_time();
    if (now == INVALID_TIME) {
        assert(0); // TODO
    }

    /////////////////////////////////////////////////////////////////
    // NETWORK EVENTS
    /////////////////////////////////////////////////////////////////

    Event events[TCP_EVENT_CAPACITY];
    int num_events = tcp_translate_events(&state->tcp, events, ctxs, pdata, *pnum);

    for (int i = 0; i < num_events; i++) {

        if (events[i].type == EVENT_DISCONNECT) {
            tcp_close(&state->tcp, events[i].conn_idx);
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

    /////////////////////////////////////////////////////////////////
    // TIME EVENTS
    /////////////////////////////////////////////////////////////////

    Time deadline = INVALID_TIME;

    if (state->status == STATUS_RECOVERY) {

        // Recovery handling runs regardless of leader/replica position,
        // since a recovering node must not act as leader until it learns
        // the current view from its peers.
        Time recovery_deadline = state->recovery_start_time + RECOVERY_TIMEOUT_SEC * 1000000000ULL;
        if (recovery_deadline <= now) {

            state->recovery_attempt_count++;
            if (state->recovery_attempt_count >= RECOVERY_ATTEMPT_LIMIT) {
                assert(0); // TODO
            } else {
                RecoveryMessage recovery_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_RECOVERY,
                        .length  = sizeof(RecoveryMessage),
                    },
                    .sender_idx = self_idx(state),
                    .nonce = state->recovery_nonce,
                };

                if (broadcast_to_peers(state, &recovery_message.base) < 0) {
                    assert(0); // TODO
                }

                state->recovery_start_time = now;
            }
        } else {
            nearest_deadline(&deadline, recovery_deadline);
        }

    } else if (is_leader(state)) {

        Time heartbeat_deadline = state->last_heartbeat_time + HEARTBEAT_INTERVAL_SEC * 1000000000ULL;
        if (heartbeat_deadline <= now) { // TODO: check the time conversion here

            NODE_TRACE("[" TIME_FMT "] node %d (LEADER): heartbeat timeout, sending COMMIT to all peers (commit_index=%d)",
                TIME_VAL(now), self_idx(state), state->commit_index);

            CommitMessage commit_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_COMMIT,
                    .length  = sizeof(CommitMessage),
                },
                .commit_index = state->commit_index,
            };
            if (broadcast_to_peers(state, &commit_message.base)) {
                assert(0); // TODO
            }
            state->last_heartbeat_time = now;
        } else {
            nearest_deadline(&deadline, heartbeat_deadline);
        }

    } else {

        Time death_deadline = state->last_heartbeat_time + PRIMARY_DEATH_TIMEOUT_SEC * 1000000000ULL;
        if (death_deadline <= now) {

            NODE_TRACE("[" TIME_FMT "] node %d (REPLICA, %s): primary death timeout, initiating view change to view %lu",
                TIME_VAL(now), self_idx(state),
                status_name(state->status),
                (unsigned long)(state->view_number + 1));

            BeginViewChangeMessage begin_view_change_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_BEGIN_VIEW_CHANGE,
                    .length  = sizeof(BeginViewChangeMessage),
                },
                .view_number = state->view_number + 1,
                .sender_idx = self_idx(state),
            };
            if (broadcast_to_peers(state, &begin_view_change_message.base)) {
                assert(0); // TODO
            }
            state->status = STATUS_CHANGE_VIEW;
            state->last_heartbeat_time = now;
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

int node_free(void *state_)
{
    NodeState *state = state_;

    log_free(&state->log);
    log_free(&state->potential_primary_log);
    log_free(&state->do_view_change_best_log);
    tcp_context_free(&state->tcp);
    client_table_free(&state->client_table);
    state_machine_free(&state->state_machine);
    return 0;
}

void check_vsr_invariants(NodeState **nodes, int num_nodes)
{
    for (int i = 0; i < num_nodes; i++) {
        NodeState *s = nodes[i];
        if (s == NULL)
            continue;

        // 1. commit_index <= log.count
        //    A node cannot have committed more entries than it has in its log.
        if (s->commit_index > s->log.count) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: commit_index (%d) > log.count (%d)\n",
                i, s->commit_index, s->log.count);
            __builtin_trap();
        }

        // 2. commit_index >= 0
        if (s->commit_index < 0) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: commit_index (%d) < 0\n",
                i, s->commit_index);
            __builtin_trap();
        }

        // 4. Future buffer count is in valid range.
        if (s->num_future < 0 || s->num_future > FUTURE_LIMIT) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: num_future (%d) out of range [0, %d]\n",
                i, s->num_future, FUTURE_LIMIT);
            __builtin_trap();
        }
    }

    // Cross-node invariants

    // 5. At most one leader in normal status per view.
    for (int i = 0; i < num_nodes; i++) {
        if (nodes[i] == NULL || nodes[i]->status != STATUS_NORMAL || !is_leader(nodes[i]))
            continue;
        for (int j = i + 1; j < num_nodes; j++) {
            if (nodes[j] == NULL || nodes[j]->status != STATUS_NORMAL || !is_leader(nodes[j]))
                continue;
            if (nodes[i]->view_number == nodes[j]->view_number) {
                fprintf(stderr, "INVARIANT VIOLATED: two normal leaders in view %lu: node %d and node %d\n",
                    (unsigned long)nodes[i]->view_number, i, j);
                __builtin_trap();
            }
        }
    }

    // 6. Committed prefix agreement (State Machine Safety).
    //    For any two nodes, their logs must agree on all entries up to
    //    min(commit_index_i, commit_index_j). This is the core safety
    //    property of VSR: all committed operations are identical across
    //    replicas.
    for (int i = 0; i < num_nodes; i++) {
        if (nodes[i] == NULL)
            continue;
        for (int j = i + 1; j < num_nodes; j++) {
            if (nodes[j] == NULL)
                continue;

            int min_commit = nodes[i]->commit_index;
            if (nodes[j]->commit_index < min_commit)
                min_commit = nodes[j]->commit_index;

            for (int k = 0; k < min_commit; k++) {
                if (memcmp(&nodes[i]->log.entries[k].oper, &nodes[j]->log.entries[k].oper, sizeof(Operation)) != 0) {
                    fprintf(stderr, "INVARIANT VIOLATED: committed log operation mismatch at index %d "
                        "between node %d and node %d\n", k, i, j);
                    __builtin_trap();
                }
            }
        }
    }
}