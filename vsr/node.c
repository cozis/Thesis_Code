#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <stdint.h>
#include <assert.h>

#include "node.h"

typedef enum {
    HR_OK,
    HR_OUT_OF_MEMORY,
    HR_INVALID_MESSAGE,
} HandlerResult;

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
    if (state->status == STATUS_RECOVERY)
        return false;
    return self_idx(state) == leader_idx(state);
}

// ---- Logging infrastructure ----

#define TIME_FMT "%7.3fs"
#define TIME_VAL(t) ((double)(t) / 1000000000.0)

static const char *status_name(Status s)
{
    switch (s) {
    case STATUS_NORMAL:      return "NR";
    case STATUS_CHANGE_VIEW: return "CV";
    case STATUS_RECOVERY:    return "RC";
    }
    return "??";
}

static void node_log_impl(NodeState *state, const char *event, const char *detail)
{
    printf("[" TIME_FMT "] NODE %d (%s) %s | V%-3lu C%-3d L%-3d | %-20s %s\n",
        TIME_VAL(state->now),
        self_idx(state),
        is_leader(state) ? "PR" : "RE",
        status_name(state->status),
        state->view_number,
        state->commit_index,
        state->log.count,
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

static bool already_voted(uint32_t votes, int idx)
{
    uint32_t mask = 1 << idx;
    return (votes & mask) == mask;
}

static void add_vote(uint32_t *votes, int idx)
{
    *votes |= 1 << idx;
}

static void send_to_peer_ex(NodeState *state, int peer_idx, MessageHeader *msg, void *extra, int extra_len)
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
    byte_queue_write(output, msg, msg->length - extra_len);
    byte_queue_write(output, extra, extra_len);
}

static void send_to_peer(NodeState *state, int peer_idx, MessageHeader *msg)
{
    send_to_peer_ex(state, peer_idx, msg, NULL, 0);
}

static void broadcast_to_peers_ex(NodeState *state, MessageHeader *msg, void *extra, int extra_len)
{
    for (int i = 0; i < state->num_nodes; i++) {
        if (i != self_idx(state))
            send_to_peer_ex(state, i, msg, extra, extra_len);
    }
}

static void broadcast_to_peers(NodeState *state, MessageHeader *msg)
{
    broadcast_to_peers_ex(state, msg, NULL, 0);
}

static void begin_state_transfer(NodeState *state, int sender_idx)
{
    if (state->state_transfer_pending)
        return;
    state->state_transfer_pending = true;

    GetStateMessage message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_GET_STATE,
            .length  = sizeof(GetStateMessage),
        },
        .view_number = state->view_number,
        .op_number   = state->log.count,
        .sender_idx  = self_idx(state),
    };
    node_log(state, "SEND GET_STATE", "to=%d op=%d", sender_idx, state->log.count);
    send_to_peer(state, sender_idx, &message.base);

    state->state_transfer_time = state->now;
}

static HandlerResult
process_request(NodeState *state, int conn_idx, ByteView msg)
{
    // Don't accept new requests during a view change.
    // The leader must complete the view change first.
    if (state->status != STATUS_NORMAL)
        return HR_OK;

    RequestMessage request_message;
    if (msg.len != sizeof(request_message))
        return HR_INVALID_MESSAGE;
    memcpy(&request_message, msg.ptr, sizeof(request_message));

    {
        char oper_buf[64];
        kvstore_snprint_oper(oper_buf, sizeof(oper_buf), request_message.oper);
        node_log(state, "RECV REQUEST", "client=%lu req=%lu key=%.16s %s",
            request_message.client_id, request_message.request_id,
            request_message.oper.key, oper_buf);
    }

    // Ensure a tag is associated to this connection
    int conn_tag = tcp_get_tag(&state->tcp, conn_idx);
    if (conn_tag == -1) {
        conn_tag = state->next_client_tag++;
        tcp_set_tag(&state->tcp, conn_idx, conn_tag, true);
    }

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

            int ret = client_table_add(
                &state->client_table,
                request_message.client_id,
                request_message.request_id,
                conn_tag);

            if (ret < 0) {

                ReplyMessage reply_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_REPLY,
                        .length  = sizeof(ReplyMessage),
                    },
                    .rejected = true,
                    .request_id = request_message.request_id,
                };

                int conn_idx = tcp_index_from_tag(&state->tcp, conn_tag);
                assert(conn_idx > -1);

                ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
                assert(output);

                node_log(state, "SEND REPLY", "client=%lu key=%.16s REJECTED (table full)", request_message.client_id, request_message.oper.key);
                byte_queue_write(output, &reply_message, sizeof(reply_message));
                return HR_OK;
            }

        } else {

            if (entry->pending)
                return HR_OK; // Only one pending operation per client is allowed. Ignore the message.

            if (entry->last_request_id > request_message.request_id)
                return HR_OK; // Request is old. Ignore.

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
                    .request_id = request_message.request_id,
                };

                ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
                assert(output);

                {
                    char result_buf[64];
                    kvstore_snprint_result(result_buf, sizeof(result_buf), entry->last_result);
                    node_log(state, "SEND REPLY", "client=%lu key=%.16s cached %s", request_message.client_id, request_message.oper.key, result_buf);
                }
                byte_queue_write(output, &reply_message, sizeof(reply_message));
                return HR_OK;
            }

            entry->last_request_id = request_message.request_id;
            entry->conn_tag = conn_tag;
            entry->pending = true;
        }
    }

    LogEntry log_entry = {
        .oper        = request_message.oper,
        .votes       = 1 << self_idx(state),
        .view_number = state->view_number,
        .client_id   = request_message.client_id,
        .request_id  = request_message.request_id,
    };
    if (log_append(&state->log, log_entry) < 0)
        return HR_OUT_OF_MEMORY;

    // We forwarded the message to all peers. As soon as
    // we get enough PREPARE_OK responses, we'll commit
    // and reply to the client.
    PrepareMessage prepare_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_PREPARE,
            .length  = sizeof(PrepareMessage),
        },
        .oper         = request_message.oper,
        .sender_idx   = self_idx(state),
        .log_index    = state->log.count-1,
        .commit_index = state->commit_index,
        .view_number  = state->view_number,
        .client_id    = request_message.client_id,
        .request_id   = request_message.request_id,
    };
    {
        char oper_buf[64];
        kvstore_snprint_oper(oper_buf, sizeof(oper_buf), request_message.oper);
        node_log(state, "SEND PREPARE", "to=* idx=%d key=%.16s %s", state->log.count-1, request_message.oper.key, oper_buf);
    }
    broadcast_to_peers(state, &prepare_message.base);
    return HR_OK;
}

static void reply_to_client(NodeState *state, uint64_t client_id,
    uint64_t request_id, KVStoreOper *oper, KVStoreResult result)
{
    // After a view change, the new leader inherits log
    // entries but not the client table's pending state.
    // A client table entry may exist from a previous view
    // (when this node was leader before) with pending=false.
    // Only reply if this leader received the original REQUEST.
    ClientTableEntry *table_entry = client_table_find(&state->client_table, client_id);
    if (table_entry == NULL)
       return;

    if (!table_entry->pending)
        return;

    // Verify this reply is for the request the client is actually
    // waiting for. After a view change, the new leader inherits
    // uncommitted log entries from the old view carrying a stale
    // request_id. If the client has since sent a newer request,
    // we must not confuse the old result with the new request.
    if (table_entry->last_request_id != request_id)
        return;

    table_entry->pending = false;
    table_entry->last_result = result;

    int conn_idx = tcp_index_from_tag(&state->tcp, table_entry->conn_tag);
    if (conn_idx < 0)
        return;

    ReplyMessage message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_REPLY,
            .length  = sizeof(ReplyMessage),
        },
        .result = result,
        .request_id = request_id,
    };

    {
        char result_buf[64];
        kvstore_snprint_result(result_buf, sizeof(result_buf), result);
        node_log(state, "SEND REPLY", "client=%lu req=%lu key=%.16s %s",
            client_id, request_id, oper->key, result_buf);
    }

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &message, sizeof(message));
}

static void advance_commit_index(NodeState *state, int target_index, bool send_replies)
{
    target_index = MIN(target_index, state->log.count);

    while (state->commit_index < target_index) {

        LogEntry *entry = &state->log.entries[state->commit_index++];

        KVStoreResult result = kvstore_update(&state->kvstore, entry->oper);
        {
            char oper_buf[64], result_buf[64];
            kvstore_snprint_oper(oper_buf, sizeof(oper_buf), entry->oper);
            kvstore_snprint_result(result_buf, sizeof(result_buf), result);
            node_log(state, "APPLY", "idx=%d key=%.16s %s -> %s", state->commit_index-1, entry->oper.key, oper_buf, result_buf);
        }
        if (send_replies) {
            reply_to_client(state, entry->client_id, entry->request_id, &entry->oper, result);
        }
    }
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
static HandlerResult
process_prepare_ok(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    PrepareOKMessage message;
    if (msg.len != sizeof(message))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    node_log(state, "RECV PREPARE_OK", "from=%d idx=%d view=%lu",
        message.sender_idx, message.log_index, message.view_number);

    if (message.view_number < state->view_number)
        return HR_OK; // Drop

    if (message.view_number > state->view_number) {
        // TODO: When can this happen?
        state->view_number = message.view_number;
        begin_state_transfer(state, message.sender_idx);
        return HR_OK;
    }

    assert(message.log_index > -1);
    assert(message.log_index < state->log.count);

    if (message.log_index < state->commit_index)
        return HR_OK; // Already processed

    LogEntry *entry = &state->log.entries[message.log_index];
    add_vote(&entry->votes, message.sender_idx);
    if (reached_quorum(state, entry->votes)) {
        node_log(state, "QUORUM", "idx=%d key=%.16s", message.log_index, entry->oper.key);
        advance_commit_index(state, message.log_index+1, true);
    }
    return HR_OK;
}

static bool
should_store_view_change_log(NodeState *state, DoViewChangeMessage message)
{
    if (message.old_view_number > state->view_change_old_view)
        return true;

    if (message.old_view_number < state->view_change_old_view)
        return false;

    if (message.op_number <= state->view_change_log.count)
        return false;

    return true;
}

static void
clear_view_change_fields(NodeState *state)
{
    state->view_change_begin_votes = 0;
    state->view_change_apply_votes = 0;
    state->view_change_old_view = 0;
    state->view_change_commit = 0;
    log_free(&state->view_change_log);
    log_init(&state->view_change_log);
}

static HandlerResult
complete_view_change_and_become_primary(NodeState *state)
{
    assert(state->commit_index <= state->view_change_commit);

    log_move(&state->log, &state->view_change_log);

    state->status = STATUS_NORMAL;
    state->last_normal_view = state->view_number;

    // Apply committed entries that haven't been executed yet.
    // The state machine has only been updated up to the old
    // commit_index. The best log may contain additional
    // committed entries (up to view_change_commit)
    // that must be executed before we start processing new
    // requests, otherwise the state machine will be stale.
    advance_commit_index(state, state->view_change_commit, false);

    // Reset vote tracking for uncommitted entries. The log
    // entries inherited from DO_VIEW_CHANGE have stale
    // votes from the previous view. The new leader starts
    // with its own vote for each entry.
    for (int i = state->commit_index; i < state->log.count; i++) {

        LogEntry *entry = &state->log.entries[i];

        entry->votes = 0;
        add_vote(&entry->votes, self_idx(state));
    }

    node_log(state, "STATUS NORMAL", "became primary (view change complete)");

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
    node_log(state, "SEND BEGIN_VIEW", "to=* view=%lu log=%d commit=%d",
        state->view_number, state->log.count, state->commit_index);
    broadcast_to_peers_ex(state, &begin_view_message.base, state->log.entries, state->log.count * sizeof(LogEntry));

    clear_view_change_fields(state);
    return HR_OK;
}

static HandlerResult
process_do_view_change(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    DoViewChangeMessage message;
    if (msg.len < sizeof(message))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    node_log(state, "RECV DO_VIEW_CHANGE", "from=%d view=%lu old_view=%lu ops=%d commit=%d",
        message.sender_idx, message.view_number, message.old_view_number,
        message.op_number, message.commit_index);

    // Only process if we're still doing a view change.
    // After completing a view change, status becomes NORMAL and
    // we must not re-process stale DO_VIEW_CHANGE messages.
    if (state->status != STATUS_CHANGE_VIEW)
        return HR_OK;

    // Only process if the view matches what we're transitioning to
    if (message.view_number != state->view_number)
        return HR_OK;

    if (!already_voted(state->view_change_apply_votes, message.sender_idx)) {

        if (should_store_view_change_log(state, message)) {

            state->view_change_old_view = message.old_view_number;

            // Parse the variable-sized log from the message
            int num_entries = (msg.len - sizeof(DoViewChangeMessage)) / sizeof(LogEntry);
            if (num_entries != message.op_number)
                return HR_INVALID_MESSAGE; // Message size mismatch

            log_free(&state->view_change_log);
            if (log_init_from_network(&state->view_change_log, msg.ptr + sizeof(DoViewChangeMessage), num_entries) < 0)
                return HR_OUT_OF_MEMORY;
        }

        state->view_change_commit = MAX(state->view_change_commit, message.commit_index);
        add_vote(&state->view_change_apply_votes, message.sender_idx);
    }

    if (reached_quorum(state, state->view_change_apply_votes)) {
        HandlerResult ret = complete_view_change_and_become_primary(state);
        if (ret != HR_OK)
            return ret;
    }

    return HR_OK;
}

static HandlerResult
process_recovery(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    RecoveryMessage recovery_message;
    if (msg.len != sizeof(RecoveryMessage))
        return HR_INVALID_MESSAGE;
    memcpy(&recovery_message, msg.ptr, sizeof(recovery_message));

    node_log(state, "RECV RECOVERY", "from=%d nonce=%lu", recovery_message.sender_idx, recovery_message.nonce);

    if (state->status != STATUS_NORMAL)
        return HR_OK; // Ignore message.

    node_log(state, "SEND RECOVERY_RESP", "to=%d view=%lu is_primary=%s",
        recovery_message.sender_idx, state->view_number, is_leader(state) ? "yes" : "no");

    RecoveryResponseMessage recovery_response_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_RECOVERY_RESPONSE,
            .length  = sizeof(RecoveryResponseMessage),
        },
        .view_number  = state->view_number,
        .op_number    = state->log.count-1, // TODO: What if the log is empty?
        .nonce        = recovery_message.nonce,
        .commit_index = state->commit_index,
        .sender_idx   = self_idx(state),
    };
    if (is_leader(state)) {
        recovery_response_message.base.length += state->log.count * sizeof(LogEntry);
        send_to_peer_ex(state, recovery_message.sender_idx, &recovery_response_message.base,
            state->log.entries, state->log.count * sizeof(LogEntry));
    } else {
        send_to_peer(state, recovery_message.sender_idx, &recovery_response_message.base);
    }
    return HR_OK;
}

static HandlerResult
perform_log_transfer_for_view_change(NodeState *state)
{
    if (is_leader(state)) {
        // We are the new leader: count our own vote directly
        // since send_to_peer_ex skips self-sends.

        add_vote(&state->view_change_apply_votes, self_idx(state));

        state->view_change_old_view = state->last_normal_view;
        state->view_change_commit = state->commit_index;

        // TODO: This should use copy-on-write
        if (log_init_from_network(&state->view_change_log, state->log.entries, state->log.count) < 0)
            return HR_OUT_OF_MEMORY;

    } else {
        DoViewChangeMessage do_view_change_message = {
            .base = {
                .version = MESSAGE_VERSION,
                .type    = MESSAGE_TYPE_DO_VIEW_CHANGE,
                .length  = sizeof(DoViewChangeMessage) + state->log.count * sizeof(LogEntry),
            },
            .view_number = state->view_number,
            .old_view_number = state->last_normal_view,
            .op_number = state->log.count,
            .commit_index = state->commit_index,
            .sender_idx = self_idx(state),
        };
        node_log(state, "SEND DO_VIEW_CHANGE", "to=%d view=%lu old_view=%lu log=%d commit=%d",
            leader_idx(state), state->view_number, state->last_normal_view,
            state->log.count, state->commit_index);
        send_to_peer_ex(state, leader_idx(state), &do_view_change_message.base, state->log.entries, state->log.count * sizeof(LogEntry));
    }

    // Clear the future array since we're changing views
    state->num_future = 0;
    state->state_transfer_pending = false;
    return HR_OK;
}

static HandlerResult
process_begin_view_change(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    BeginViewChangeMessage message;
    if (msg.len != sizeof(BeginViewChangeMessage))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    node_log(state, "RECV BEGIN_VIEW_CHG", "from=%d view=%lu", message.sender_idx, message.view_number);

    if (state->status == STATUS_RECOVERY)
        return HR_OK;

    assert(state->status == STATUS_NORMAL
        || state->status == STATUS_CHANGE_VIEW);

    // Ignore old messages
    if (message.view_number < state->view_number)
        return HR_OK;

    // BeginViewChange messages hold the view number of the view
    // they are transitioning into. If this node is in NORMAL
    // state and has the same view as the BeginViewChange message,
    // it means the transition already happened and the message
    // is stale.
    if (state->status == STATUS_NORMAL) {
        if (state->view_number == message.view_number)
            return HR_OK;
    }

    // If the peer's view number is larger, we need to transition
    // to the view change state.
    if (message.view_number > state->view_number) {

        // Send our own BeginViewChange to all peers
        BeginViewChangeMessage message_2 = {
            .base = {
                .version = MESSAGE_VERSION,
                .type    = MESSAGE_TYPE_BEGIN_VIEW_CHANGE,
                .length  = sizeof(BeginViewChangeMessage),
            },
            .view_number = message.view_number,
            .sender_idx = self_idx(state),
        };
        node_log(state, "SEND BEGIN_VIEW_CHG", "to=* view=%lu", message.view_number);
        broadcast_to_peers(state, &message_2.base);

        clear_view_change_fields(state);
        state->view_number = message.view_number;
        node_log(state, "STATUS CHANGE_VIEW", "view=%lu", state->view_number);
        state->status = STATUS_CHANGE_VIEW;
        state->heartbeat = state->now;
    }

    add_vote(&state->view_change_begin_votes, message.sender_idx);
    add_vote(&state->view_change_begin_votes, self_idx(state));
    if (reached_quorum(state, state->view_change_begin_votes)) {
        HandlerResult ret = perform_log_transfer_for_view_change(state);
        if (ret != HR_OK)
            return ret;
    }

    return HR_OK;
}

static HandlerResult
process_begin_view(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    BeginViewMessage message;
    if (msg.len < sizeof(message))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    node_log(state, "RECV BEGIN_VIEW", "view=%lu commit=%d ops=%d",
        message.view_number, message.commit_index, message.op_number);

    if (message.view_number < state->view_number)
        return HR_OK;

    state->view_number = message.view_number;

    state->status = STATUS_NORMAL;
    state->last_normal_view = state->view_number;
    node_log(state, "STATUS NORMAL", "new view=%lu (follower)", state->view_number);

    // Replace the local log with the authoritative log from the primary
    int num_entries = (msg.len - sizeof(BeginViewMessage)) / sizeof(LogEntry);
    log_free(&state->log);
    if (log_init_from_network(&state->log, msg.ptr + sizeof(BeginViewMessage), num_entries) < 0)
        return HR_OUT_OF_MEMORY;

    state->num_future = 0;
    state->state_transfer_pending = false;

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
        node_log(state, "SEND PREPARE_OK", "to=%d idx=%d key=%.16s", leader_idx(state), state->log.count - 1,
            state->log.entries[state->log.count - 1].oper.key);
        send_to_peer(state, leader_idx(state), &ok_msg.base);
    }

    advance_commit_index(state, message.commit_index, false);

    clear_view_change_fields(state);
    state->heartbeat = state->now;
    return HR_OK;
}

static HandlerResult
process_get_state(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    GetStateMessage get_state_message;
    if (msg.len != sizeof(GetStateMessage))
        return HR_INVALID_MESSAGE;
    memcpy(&get_state_message, msg.ptr, sizeof(get_state_message));

    node_log(state, "RECV GET_STATE", "from=%d op=%d view=%lu",
        get_state_message.sender_idx, get_state_message.op_number, get_state_message.view_number);

    if (state->status != STATUS_NORMAL)
        return HR_OK;

    // Only respond if the requester is in the same view
    if (get_state_message.view_number != state->view_number)
        return HR_OK;

    // Compute the suffix of log entries the requester is missing
    int start = get_state_message.op_number;
    if (start < 0 || start >= state->log.count)
        return HR_OK; // Nothing to send

    int num_entries = state->log.count - start;

    NewStateMessage new_state_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_NEW_STATE,
            .length  = sizeof(NewStateMessage) + num_entries * sizeof(LogEntry),
        },
        .view_number  = state->view_number,
        .op_number    = num_entries,
        .commit_index = state->commit_index,
    };
    node_log(state, "SEND NEW_STATE", "to=%d entries=%d commit=%d",
        get_state_message.sender_idx, num_entries, state->commit_index);
    send_to_peer_ex(state, get_state_message.sender_idx, &new_state_message.base,
        state->log.entries + start, num_entries * sizeof(LogEntry));
    return HR_OK;
}

static HandlerResult
process_message_as_leader(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    switch (type) {
    case MESSAGE_TYPE_REQUEST:
        return process_request(state, conn_idx, msg);

    case MESSAGE_TYPE_PREPARE_OK:
        return process_prepare_ok(state, conn_idx, msg);

    case MESSAGE_TYPE_DO_VIEW_CHANGE:
        return process_do_view_change(state, conn_idx, msg);

    case MESSAGE_TYPE_RECOVERY:
        return process_recovery(state, conn_idx, msg);

    case MESSAGE_TYPE_BEGIN_VIEW_CHANGE:
        return process_begin_view_change(state, conn_idx, msg);

    case MESSAGE_TYPE_BEGIN_VIEW:
        return process_begin_view(state, conn_idx, msg);

    case MESSAGE_TYPE_RECOVERY_RESPONSE:
        // Ignore message.
        break;

    case MESSAGE_TYPE_GET_STATE:
        return process_get_state(state, conn_idx, msg);

    default:
        break;
    }

    return HR_OK;
}

static HandlerResult
complete_recovery(NodeState *state)
{
    assert(state->commit_index <= state->recovery_commit);

    state->view_number = state->recovery_view;
    log_move(&state->log, &state->recovery_log);
    advance_commit_index(state, state->recovery_commit, false);

    state->status = STATUS_NORMAL;
    state->last_normal_view = state->view_number;
    node_log(state, "STATUS NORMAL", "recovery complete view=%lu commit=%d",
        state->view_number, state->commit_index);

    // Reset stale votes
    if (is_leader(state)) {
        for (int i = state->commit_index; i < state->log.count; i++) {
            LogEntry *entry = &state->log.entries[i];
            entry->votes = 0;
            add_vote(&entry->votes, self_idx(state));
        }
    }

    // Update heartbeat to avoid immediate view change timeout
    state->heartbeat = state->now;
    return HR_OK;
}

static bool
received_recovery_primary(NodeState *state)
{
    assert(state->status == STATUS_RECOVERY);

    int primary_idx = state->recovery_view % state->num_nodes;
    uint32_t primary_mask = 1 << primary_idx;
    return (state->recovery_votes & primary_mask) != 0;
}

static bool
sender_thinks_he_is_primary(NodeState *state, RecoveryResponseMessage message)
{
    return message.sender_idx == (int) (message.view_number % state->num_nodes);
}

// When a node is trying to recover state, it expects
// a response from each replica and one from the primary
// containing the log.
//
// The recovering node is not aware of what the current
// view number is, and therefore doesn't know who is the
// primary.
//
// The only way a recovering node can infer whether a
// node is the primary or not, is by inferring it from
// the message itself.
//
// This function determines whether the message from a
// node contains a log and if it does, whether it should
// be stored.
//
// If no log was previously received, it is stored
// unconditionally. If a log is already stored, the
// newly received one is only stored if its view number
// is greater than the one associated to the stored
// log.
static bool
should_store_recovery_log(NodeState *state, RecoveryResponseMessage message)
{
    return sender_thinks_he_is_primary(state, message)
        && (!received_recovery_primary(state) || state->recovery_log_view < message.view_number);
}

static HandlerResult
process_recovery_response(NodeState *state, ByteView msg)
{
    RecoveryResponseMessage message;
    if (msg.len < sizeof(message))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    node_log(state, "RECV RECOVERY_RESP", "from=%d view=%lu commit=%d nonce=%lu",
        message.sender_idx, message.view_number, message.commit_index, message.nonce);

    // 1. Only process responses if we are actually in the recovering state [cite: 237]
    // 2. Ensure the nonce matches the one we sent to prevent accepting
    //    delayed responses from previous recovery attempts[cite: 253].
    if (state->status != STATUS_RECOVERY)
        return HR_OK;

    if (message.nonce != state->recovery_nonce)
        return HR_OK;

    if (should_store_recovery_log(state, message)) {
        log_free(&state->recovery_log);
        if (log_init_from_network(&state->recovery_log, msg.ptr + sizeof(RecoveryResponseMessage),
            (msg.len - sizeof(RecoveryResponseMessage)) / sizeof(LogEntry)) < 0)
            return HR_OUT_OF_MEMORY;

        state->recovery_log_view = message.view_number;
        state->recovery_commit = message.commit_index;
    }

    add_vote(&state->recovery_votes, message.sender_idx);
    state->recovery_view = MAX(state->recovery_view, message.view_number);

    if (reached_quorum(state, state->recovery_votes) && received_recovery_primary(state)) {
        HandlerResult ret = complete_recovery(state);
        if (ret != HR_OK)
            return ret;
    }
    return HR_OK;
}

static int
process_single_future_list_entry(NodeState *state)
{
    // Look for an entry with the current log index
    int i = 0;
    while (i < state->num_future && state->future[i].log_index != state->log.count)
        i++;

    if (i == state->num_future)
        return 0; // No entry

    LogEntry entry = {
        .oper = state->future[i].oper,
        .votes = 0,
        .view_number = state->view_number,
        .client_id  = state->future[i].client_id,
        .request_id = state->future[i].request_id,
    };
    if (log_append(&state->log, entry) < 0)
        return -1;

    PrepareOKMessage message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_PREPARE_OK,
            .length  = sizeof(PrepareOKMessage),
        },
        .sender_idx  = self_idx(state),
        .log_index   = state->log.count-1,
        .view_number = state->view_number,
    };
    send_to_peer(state, state->future[i].sender_idx, &message.base);
    return 1;
}

static void
remove_old_future_list_entries(NodeState *state)
{
    for (int i = 0; i < state->num_future; i++) {
        if (state->future[i].log_index < state->log.count) {
            state->future[i--] = state->future[--state->num_future];
        }
    }
}

static int process_future_list(NodeState *state)
{
    for (;;) {
        int ret = process_single_future_list_entry(state);
        if (ret != 1)
            break;
    }
    remove_old_future_list_entries(state);
    return 0;
}

static HandlerResult
process_prepare(NodeState *state, ByteView msg)
{
    // A recovering node must not process PREPAREs. It doesn't have
    // the full log yet and adopting a view from a PREPARE would set
    // last_normal_view to a high value with an empty log, which can
    // cause a subsequent view change to select this empty log over
    // the correct one from other replicas.
    if (state->status == STATUS_RECOVERY)
        return HR_OK;

    PrepareMessage message;
    if (msg.len != sizeof(message))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    {
        char oper_buf[64];
        kvstore_snprint_oper(oper_buf, sizeof(oper_buf), message.oper);
        node_log(state, "RECV PREPARE", "from=%d idx=%d commit=%d view=%lu key=%.16s %s",
            message.sender_idx, message.log_index, message.commit_index,
            message.view_number, message.oper.key, oper_buf);
    }

    if (message.view_number < state->view_number)
        return HR_OK; // Stale peer

    if (message.view_number > state->view_number) {

        // The new leader has started a view we haven't seen.
        // Adopt the new view and process the PREPARE normally.

        state->view_number = message.view_number;

        state->status = STATUS_NORMAL;
        state->last_normal_view = state->view_number;

        state->view_change_begin_votes = 0;
        state->num_future = 0;
        state->state_transfer_pending = false;
    }

    // A replica that has entered CHANGE_VIEW must not process PREPAREs
    // for the current view. It has already committed to the view change
    // by broadcasting BEGIN_VIEW_CHANGE and must wait for the view
    // change to complete via BEGIN_VIEW. Processing PREPAREs in this
    // state would advance the log and commit_index without updating
    // last_normal_view, causing a stale old_view_number in any
    // subsequent DO_VIEW_CHANGE and potentially leading the new leader
    // to select a shorter log over this replica's longer one.
    if (state->status == STATUS_CHANGE_VIEW)
        return HR_OK;

    if (message.log_index < state->log.count)
        return HR_OK; // Message refers to an old entry. Ignore.

    if (message.log_index > state->log.count) {

        // The prepare message for a previous log entry was not received yet.
        // Buffer this message to process it later (drop if buffer full)
        if (state->num_future < FUTURE_LIMIT) {
            state->future[state->num_future++] = message;
        }

        // Request missing entries from the primary via State Transfer
        begin_state_transfer(state, leader_idx(state));
        return HR_OK;
    }

    LogEntry log_entry = {
        .oper = message.oper,
        .votes = 0,
        .view_number = state->view_number,
        .client_id  = message.client_id,
        .request_id = message.request_id,
    };
    if (log_append(&state->log, log_entry) < 0)
        return HR_OUT_OF_MEMORY;

    PrepareOKMessage ok_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_PREPARE_OK,
            .length  = sizeof(PrepareOKMessage),
        },
        .sender_idx = self_idx(state),
        .log_index  = state->log.count-1,
        .view_number = state->view_number,
    };
    node_log(state, "SEND PREPARE_OK", "to=%d idx=%d key=%.16s", message.sender_idx, state->log.count-1, message.oper.key);
    send_to_peer(state, message.sender_idx, &ok_message.base);

    process_future_list(state);
    advance_commit_index(state, message.commit_index, false);

    state->heartbeat = state->now;
    return HR_OK;
}

static HandlerResult
process_commit(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    CommitMessage message;
    if (msg.len != sizeof(CommitMessage))
        return HR_INVALID_MESSAGE;
    memcpy(&message, msg.ptr, sizeof(message));

    node_log(state, "RECV COMMIT", "commit=%d", message.commit_index);

    // In RECOVERY and VIEW_CHANGE we ignore COMMIT messages.
    // It's important we don't reset the timer so that the
    // recovery and view change mechanism can retry on timeout.
    if (state->status != STATUS_NORMAL)
        return HR_OK;

    advance_commit_index(state, message.commit_index, false);
    state->heartbeat = state->now;
    return HR_OK;
}

static HandlerResult
process_new_state(NodeState *state, int conn_idx, ByteView msg)
{
    (void) conn_idx;

    NewStateMessage new_state_message;
    if (msg.len < sizeof(NewStateMessage))
        return HR_INVALID_MESSAGE;
    memcpy(&new_state_message, msg.ptr, sizeof(new_state_message));

    node_log(state, "RECV NEW_STATE", "entries=%d commit=%d view=%lu",
        new_state_message.op_number, new_state_message.commit_index, new_state_message.view_number);

    // Ignore if we're in a different view
    if (new_state_message.view_number != state->view_number)
        return HR_OK;

    int num_entries = (msg.len - sizeof(NewStateMessage)) / sizeof(LogEntry);
    if (num_entries != new_state_message.op_number)
        return HR_INVALID_MESSAGE;

    if (num_entries == 0)
        return HR_OK;

    // Append received entries to our log
    LogEntry *entries = (LogEntry *)((uint8_t *)msg.ptr + sizeof(NewStateMessage));
    for (int i = 0; i < num_entries; i++) {

        LogEntry entry = {
            .oper = entries[i].oper,
            .votes = 0,
            .view_number = state->view_number,
            .client_id = entries[i].client_id,
            .request_id = entries[i].request_id,
        };
        if (log_append(&state->log, entry) < 0)
            return HR_OUT_OF_MEMORY;

        // Send PREPARE_OK for each appended entry
        PrepareOKMessage prepare_ok_message = {
            .base = {
                .version = MESSAGE_VERSION,
                .type    = MESSAGE_TYPE_PREPARE_OK,
                .length  = sizeof(PrepareOKMessage),
            },
            .sender_idx  = self_idx(state),
            .log_index   = state->log.count - 1,
            .view_number = state->view_number,
        };
        node_log(state, "SEND PREPARE_OK", "to=%d idx=%d key=%.16s", leader_idx(state), state->log.count - 1,
            state->log.entries[state->log.count - 1].oper.key);
        send_to_peer(state, leader_idx(state), &prepare_ok_message.base);
    }

    process_future_list(state);
    advance_commit_index(state, new_state_message.commit_index, false);

    state->state_transfer_pending = false;
    state->heartbeat = state->now; // TODO: Should only do this if the sender is the primary
    return HR_OK;
}

static HandlerResult
send_redirect(NodeState *state, int conn_idx)
{
    RedirectMessage redirect_message = {
        .base = {
            .version = MESSAGE_VERSION,
            .type    = MESSAGE_TYPE_REDIRECT,
            .length  = sizeof(RedirectMessage),
        },
        .view_number = state->view_number,
    };

    node_log(state, "SEND REDIRECT", "-> conn %d view=%lu leader=%d",
        tcp_get_tag(&state->tcp, conn_idx),
        (unsigned long)state->view_number, leader_idx(state));

    ByteQueue *output = tcp_output_buffer(&state->tcp, conn_idx);
    assert(output);

    byte_queue_write(output, &redirect_message, redirect_message.base.length);
    return HR_OK;
}

static HandlerResult
process_message_as_replica(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
    switch (type) {
    case MESSAGE_TYPE_REQUEST:
        if (state->status == STATUS_NORMAL)
            return send_redirect(state, conn_idx);
        break;

    case MESSAGE_TYPE_PREPARE:
        return process_prepare(state, msg);

    case MESSAGE_TYPE_COMMIT:
        return process_commit(state, conn_idx, msg);

    case MESSAGE_TYPE_BEGIN_VIEW_CHANGE:
        return process_begin_view_change(state, conn_idx, msg);

    case MESSAGE_TYPE_BEGIN_VIEW:
        return process_begin_view(state, conn_idx, msg);

    case MESSAGE_TYPE_RECOVERY:
        return process_recovery(state, conn_idx, msg);

    case MESSAGE_TYPE_RECOVERY_RESPONSE:
        return process_recovery_response(state, msg);

    case MESSAGE_TYPE_NEW_STATE:
        return process_new_state(state, conn_idx, msg);

    default:
        break;
    }
    return HR_OK;
}

static HandlerResult
process_message(NodeState *state,
    int conn_idx, uint8_t type, ByteView msg)
{
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
        case MESSAGE_TYPE_GET_STATE:
            if (msg.len >= sizeof(GetStateMessage)) {
                GetStateMessage m; memcpy(&m, msg.ptr, sizeof(m));
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
    if (now > state->now)
        state->now = now;

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
    state->last_normal_view = 0;
    state->heartbeat = now;
    state->commit_index = 0;
    state->num_future = 0;
    state->state_transfer_pending = false;
    state->state_transfer_time = 0;

    // View change state
    state->view_change_begin_votes = 0;
    state->view_change_apply_votes = 0;
    state->view_change_old_view = 0;
    state->view_change_commit = 0;
    log_init(&state->view_change_log);

    // Recovery state
    state->recovery_votes = 0;
    state->recovery_commit = 0;
    state->recovery_view = 0;
    state->recovery_log_view = 0;
    log_init(&state->recovery_log);

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
        state->recovery_time = now;
    } else {
        state->status = STATUS_NORMAL;
    }
    log_init(&state->log); // Initialize early so node_log can read log.count
    node_log(state, "INIT", "nodes=%d%s", state->num_nodes, previously_crashed ? " (recovering)" : "");

    client_table_init(&state->client_table);
    state->next_client_tag = NODE_LIMIT; // Make sure they don't overlap with node indices

    kvstore_init(&state->kvstore);

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

    // Write the boot marker to disk so that future restarts can detect
    // a previous crash. This must happen after TCP init so that the
    // marker is only written if the node successfully started.
    if (!previously_crashed) {
        int fd = open("vsr_boot_marker", O_WRONLY | O_CREAT, 0644);
        if (fd >= 0)
            close(fd);
    }

    if (previously_crashed) {
        node_log(state, "STATUS RECOVERY", "nonce=%lu (crash detected)", state->recovery_nonce);
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
        node_log(state, "SEND RECOVERY", "to=* nonce=%lu", state->recovery_nonce);
        broadcast_to_peers(state, &recovery_message.base);
        nearest_deadline(&deadline, state->recovery_time + RECOVERY_TIMEOUT_SEC * 1000000000ULL);
    }

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

    state->now = get_current_time();
    if (state->now == INVALID_TIME)
        return -1;

    /////////////////////////////////////////////////////////////////
    // NETWORK EVENTS
    /////////////////////////////////////////////////////////////////

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
            if (hret == HR_OUT_OF_MEMORY)
                return -1;
            assert(hret == HR_OK);

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
        Time recovery_deadline = state->recovery_time + RECOVERY_TIMEOUT_SEC * 1000000000ULL;
        if (recovery_deadline <= state->now) {
            node_log_simple(state, "TIMEOUT RECOVERY");
            RecoveryMessage recovery_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_RECOVERY,
                    .length  = sizeof(RecoveryMessage),
                },
                .sender_idx = self_idx(state),
                .nonce = state->recovery_nonce,
            };
            node_log(state, "SEND RECOVERY", "to=* nonce=%lu", state->recovery_nonce);
            broadcast_to_peers(state, &recovery_message.base);
            state->recovery_votes = 0;
            state->recovery_log_view = 0;
            state->recovery_time = state->now;
        } else {
            nearest_deadline(&deadline, recovery_deadline);
        }

    } else if (is_leader(state)) {

        Time heartbeat_deadline = state->heartbeat + HEARTBEAT_INTERVAL_SEC * 1000000000ULL;
        if (heartbeat_deadline <= state->now) { // TODO: check the time conversion here
            CommitMessage commit_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_COMMIT,
                    .length  = sizeof(CommitMessage),
                },
                .commit_index = state->commit_index,
            };
            node_log(state, "SEND COMMIT", "to=* commit=%d", state->commit_index);
            broadcast_to_peers(state, &commit_message.base);
            state->heartbeat = state->now;
        } else {
            nearest_deadline(&deadline, heartbeat_deadline);
        }

    } else {

        // State transfer retry: if we're waiting for missing log entries
        // and the timeout has elapsed, re-send GET_STATE to the primary.
        if (state->state_transfer_pending) {

            Time st_deadline = state->state_transfer_time + STATE_TRANSFER_TIMEOUT_SEC * 1000000000ULL;
            if (st_deadline <= state->now) {
                node_log(state, "TIMEOUT STATE_TRANSFER", "op=%d", state->log.count);
                GetStateMessage get_state_message = {
                    .base = {
                        .version = MESSAGE_VERSION,
                        .type    = MESSAGE_TYPE_GET_STATE,
                        .length  = sizeof(GetStateMessage),
                    },
                    .view_number = state->view_number,
                    .op_number   = state->log.count,
                    .sender_idx  = self_idx(state),
                };
                node_log(state, "SEND GET_STATE", "to=%d op=%d", leader_idx(state), state->log.count);
                send_to_peer(state, leader_idx(state), &get_state_message.base);
                state->state_transfer_time = state->now;
            } else {
                nearest_deadline(&deadline, st_deadline);
            }
        }

        int death_timeout = (state->status == STATUS_CHANGE_VIEW)
            ? VIEW_CHANGE_TIMEOUT_SEC : PRIMARY_DEATH_TIMEOUT_SEC;
        Time death_deadline = state->heartbeat + death_timeout * 1000000000ULL;
        if (death_deadline <= state->now) {

            node_log_simple(state, "TIMEOUT PRIMARY_DEATH");

            BeginViewChangeMessage begin_view_change_message = {
                .base = {
                    .version = MESSAGE_VERSION,
                    .type    = MESSAGE_TYPE_BEGIN_VIEW_CHANGE,
                    .length  = sizeof(BeginViewChangeMessage),
                },
                .view_number = state->view_number + 1,
                .sender_idx = self_idx(state),
            };
            node_log(state, "SEND BEGIN_VIEW_CHG", "to=* view=%lu", state->view_number + 1);
            broadcast_to_peers(state, &begin_view_change_message.base);

            state->status = STATUS_CHANGE_VIEW;
            node_log(state, "STATUS CHANGE_VIEW", "view=%lu", state->view_number + 1);
            state->heartbeat = state->now;

        } else {
            nearest_deadline(&deadline, death_deadline);
        }
    }

    *timeout = deadline_to_timeout(deadline, state->now);
    if (pcap < TCP_POLL_CAPACITY)
        return -1;
    *pnum = tcp_register_events(&state->tcp, ctxs, pdata);
    return 0;
}

int node_free(void *state_)
{
    NodeState *state = state_;

    node_log_simple(state, "CRASHED");

    log_free(&state->log);
    log_free(&state->recovery_log);
    log_free(&state->view_change_log);
    tcp_context_free(&state->tcp);
    client_table_free(&state->client_table);
    kvstore_free(&state->kvstore);
    return 0;
}

