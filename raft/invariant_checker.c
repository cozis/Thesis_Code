// Raft invariant checker with external shadow log tracking.
//
// This file runs in the main simulation loop outside Quakey-scheduled
// processes. It includes node.h for struct definitions, then restores
// real allocators since mock_malloc/realloc/free abort outside process
// context.

#include "invariant_checker.h"

#include <assert.h>
#include <string.h>

// Restore real allocators (see checker/linearizability.c for precedent).
// The header chain (invariant_checker.h -> node.h -> wal.h ->
// lib/file_system.h) defines QUAKEY_ENABLE_MOCKS and replaces
// malloc/realloc/free with mock versions. We need the real ones
// because this code runs outside any Quakey-scheduled process.
#undef malloc
#undef realloc
#undef free
#include <stdio.h>
#include <stdlib.h>

static int self_idx(NodeState *state)
{
    for (int i = 0; i < state->num_nodes; i++)
        if (addr_eql(state->node_addrs[i], state->self_addr))
            return i;
    UNREACHABLE;
}

static int shadow_log_append(InvariantChecker *ic, KVStoreOper oper)
{
    if (ic->shadow_count == ic->shadow_capacity) {
        int n = 2 * ic->shadow_capacity;
        if (n < 8)
            n = 8;
        KVStoreOper *p = realloc(ic->shadow_log, n * sizeof(KVStoreOper));
        if (p == NULL)
            return -1;
        ic->shadow_log = p;
        ic->shadow_capacity = n;
    }
    ic->shadow_log[ic->shadow_count++] = oper;
    return 0;
}

void invariant_checker_init(InvariantChecker *ic)
{
    ic->shadow_log = NULL;
    ic->shadow_count = 0;
    ic->shadow_capacity = 0;
}

void invariant_checker_free(InvariantChecker *ic)
{
    fprintf(stderr, "INVARIANT CHECKER: shadow log tracked %d committed entries\n",
        ic->shadow_count);
    free(ic->shadow_log);
}

void invariant_checker_run(InvariantChecker *ic, NodeState **nodes, int num_nodes)
{
    for (int i = 0; i < num_nodes; i++) {
        NodeState *s = nodes[i];
        if (s == NULL)
            continue;

        int log_count = wal_entry_count(&s->wal);

        // The commit index starts at -1 (nothing committed) and only
        // increases. It must never go below -1.
        if (s->commit_index < -1) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: commit_index (%d) < -1\n",
                i, s->commit_index);
            __builtin_trap();
        }

        // A node cannot have committed an entry beyond what exists
        // in its log. With an empty log (count=0), commit_index
        // must be -1.
        if (s->commit_index >= log_count) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: commit_index (%d) >= log_count (%d)\n",
                i, s->commit_index, log_count);
            __builtin_trap();
        }

        // Entries are applied to the state machine in order, up to
        // the commit index. The last applied index must never exceed
        // the commit index.
        if (s->last_applied > s->commit_index) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: last_applied (%d) > commit_index (%d)\n",
                i, s->last_applied, s->commit_index);
            __builtin_trap();
        }

        if (s->last_applied < -1) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: last_applied (%d) < -1\n",
                i, s->last_applied);
            __builtin_trap();
        }

        // voted_for is either -1 (no vote) or a valid node index.
        if (s->term_and_vote.voted_for != -1 && (s->term_and_vote.voted_for < 0 || s->term_and_vote.voted_for >= s->num_nodes)) {
            fprintf(stderr, "INVARIANT VIOLATED: node %d: voted_for (%d) is not -1 "
                "and not in [0, %d)\n",
                i, s->term_and_vote.voted_for, s->num_nodes);
            __builtin_trap();
        }

        // Leaders append entries with their current term, and terms
        // only increase. Truncation preserves this property because
        // replacement entries come from a leader whose log already
        // satisfies non-decreasing terms.
        for (int k = 1; k < log_count; k++) {
            if (wal_peek_entry(&s->wal, k)->term < wal_peek_entry(&s->wal, k-1)->term) {
                fprintf(stderr, "INVARIANT VIOLATED: node %d: wal[%d].term (%lu) "
                    "< wal[%d].term (%lu) (non-monotonic)\n",
                    i, k, (unsigned long)wal_peek_entry(&s->wal, k)->term,
                    k-1, (unsigned long)wal_peek_entry(&s->wal, k-1)->term);
                __builtin_trap();
            }
        }

        // A leader must have voted for itself in the current term.
        // A node becomes leader by winning an election, which requires
        // voting for itself. The voted_for value is only reset when
        // stepping down (which changes the role to FOLLOWER).
        if (s->role == ROLE_LEADER) {
            if (s->term_and_vote.voted_for != self_idx(s)) {
                fprintf(stderr, "INVARIANT VIOLATED: node %d: role is LEADER but "
                    "voted_for (%d) != self_idx (%d)\n",
                    i, s->term_and_vote.voted_for, self_idx(s));
                __builtin_trap();
            }
        }

        // A candidate must have voted for itself in the current term.
        // A node enters candidate state only through start_election(),
        // which increments the term and sets voted_for to self_idx.
        if (s->role == ROLE_CANDIDATE) {
            if (s->term_and_vote.voted_for != self_idx(s)) {
                fprintf(stderr, "INVARIANT VIOLATED: node %d: role is CANDIDATE but "
                    "voted_for (%d) != self_idx (%d)\n",
                    i, s->term_and_vote.voted_for, self_idx(s));
                __builtin_trap();
            }
        }

        // For a leader, match_indices[self] must equal the last log
        // index. The leader always has its own entries matched.
        if (s->role == ROLE_LEADER) {
            int expected_match = log_count - 1;
            if (s->match_indices[self_idx(s)] != expected_match) {
                fprintf(stderr, "INVARIANT VIOLATED: node %d (LEADER): "
                    "match_indices[self] (%d) != last log index (%d)\n",
                    i, s->match_indices[self_idx(s)], expected_match);
                __builtin_trap();
            }
        }

        // For a leader, next_indices[k] must be >= match_indices[k] + 1
        // for all followers (k != self). The next index to send is
        // always at least one past the highest known replicated index.
        // The leader's own next_indices[self] is not maintained since
        // the leader never sends entries to itself.
        if (s->role == ROLE_LEADER) {
            int si = self_idx(s);
            for (int k = 0; k < s->num_nodes; k++) {
                if (k == si)
                    continue;
                if (s->next_indices[k] < s->match_indices[k] + 1) {
                    fprintf(stderr, "INVARIANT VIOLATED: node %d (LEADER): "
                        "next_indices[%d] (%d) < match_indices[%d] + 1 (%d)\n",
                        i, k, s->next_indices[k], k, s->match_indices[k] + 1);
                    __builtin_trap();
                }
            }
        }
    }

    // Election Safety: at most one leader per term.
    // Each term has at most one leader because a candidate needs
    // a majority of votes, and each node votes at most once per term.
    for (int i = 0; i < num_nodes; i++) {
        if (nodes[i] == NULL || nodes[i]->role != ROLE_LEADER)
            continue;
        for (int j = i + 1; j < num_nodes; j++) {
            if (nodes[j] == NULL || nodes[j]->role != ROLE_LEADER)
                continue;
            if (nodes[i]->term_and_vote.term == nodes[j]->term_and_vote.term) {
                fprintf(stderr, "INVARIANT VIOLATED: two leaders in term %lu: "
                    "node %d and node %d\n",
                    (unsigned long)nodes[i]->term_and_vote.term, i, j);
                __builtin_trap();
            }
        }
    }

    // State Machine Safety (committed prefix agreement).
    // For any two nodes, their logs must agree on all entries up to
    // min(commit_index_i, commit_index_j). This is the core safety
    // property: all committed operations are identical across replicas.
    for (int i = 0; i < num_nodes; i++) {
        if (nodes[i] == NULL)
            continue;
        for (int j = i + 1; j < num_nodes; j++) {
            if (nodes[j] == NULL)
                continue;

            int min_commit = nodes[i]->commit_index;
            if (nodes[j]->commit_index < min_commit)
                min_commit = nodes[j]->commit_index;

            for (int k = 0; k <= min_commit; k++) {
                WALEntry *ei = wal_peek_entry(&nodes[i]->wal, k);
                WALEntry *ej = wal_peek_entry(&nodes[j]->wal, k);

                if (memcmp(&ei->oper, &ej->oper, sizeof(KVStoreOper)) != 0) {
                    fprintf(stderr, "INVARIANT VIOLATED: committed log operation "
                        "mismatch at index %d between node %d and node %d\n",
                        k, i, j);
                    __builtin_trap();
                }

                if (ei->term != ej->term) {
                    fprintf(stderr, "INVARIANT VIOLATED: committed log term "
                        "mismatch at index %d between node %d (term %lu) "
                        "and node %d (term %lu)\n",
                        k, i, (unsigned long)ei->term,
                        j, (unsigned long)ej->term);
                    __builtin_trap();
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////
    // Shadow log: external commit tracking
    ////////////////////////////////////////////////////////////////////

    // Phase 1: Find the observed max commit index and a source node.
    //
    // In Raft, commit_index == -1 means nothing committed, 0 means the
    // first entry is committed, etc. We track the number of committed
    // entries as (commit_index + 1) so it aligns with shadow_count.
    int observed_committed = 0;  // number of committed entries observed
    int source_node_idx = -1;

    for (int i = 0; i < num_nodes; i++) {
        if (nodes[i] == NULL)
            continue;
        int node_committed = nodes[i]->commit_index + 1;
        if (node_committed > observed_committed) {
            observed_committed = node_committed;
            source_node_idx = i;
        }
    }

    // Phase 2: Append newly committed entries to the shadow log.
    if (source_node_idx >= 0 && observed_committed > ic->shadow_count) {

        NodeState *source = nodes[source_node_idx];
        assert(wal_entry_count(&source->wal) >= observed_committed);

        for (int k = ic->shadow_count; k < observed_committed; k++) {

            KVStoreOper *source_oper = &wal_peek_entry(&source->wal, k)->oper;

            // Cross-validate against other live nodes that have also
            // committed this entry.
            for (int j = 0; j < num_nodes; j++) {
                if (j == source_node_idx)
                    continue;
                if (nodes[j] == NULL)
                    continue;
                if (nodes[j]->commit_index < k)
                    continue;
                if (wal_entry_count(&nodes[j]->wal) <= k)
                    continue;

                if (memcmp(&wal_peek_entry(&nodes[j]->wal, k)->oper, source_oper, sizeof(KVStoreOper)) != 0) {
                    fprintf(stderr, "INVARIANT VIOLATED: committed entry mismatch at index %d "
                        "between source node %d and node %d during shadow log append\n",
                        k, source_node_idx, j);
                    __builtin_trap();
                }
            }

            if (shadow_log_append(ic, *source_oper) < 0) {
                fprintf(stderr, "INVARIANT CHECKER: shadow log allocation failed\n");
                __builtin_trap();
            }
        }
    }

    // Phase 3: Verify shadow log against the cluster.

    // Sub-check A: Committed entries must match the shadow log.
    for (int k = 0; k < ic->shadow_count; k++) {
        for (int i = 0; i < num_nodes; i++) {
            if (nodes[i] == NULL)
                continue;
            if (wal_entry_count(&nodes[i]->wal) <= k)
                continue;
            if (nodes[i]->commit_index < k)
                continue;

            if (memcmp(&wal_peek_entry(&nodes[i]->wal, k)->oper, &ic->shadow_log[k], sizeof(KVStoreOper)) != 0) {
                char shadow_buf[128], node_buf[128];
                kvstore_snprint_oper(shadow_buf, sizeof(shadow_buf), ic->shadow_log[k]);
                kvstore_snprint_oper(node_buf, sizeof(node_buf), wal_peek_entry(&nodes[i]->wal, k)->oper);
                fprintf(stderr, "INVARIANT VIOLATED: shadow log mismatch at index %d on node %d\n"
                    "  shadow: %s\n"
                    "  node:   %s\n",
                    k, i, shadow_buf, node_buf);
                __builtin_trap();
            }
        }
    }

    // Sub-check B: When commit regresses, previously committed entries
    // must still be held by a majority of the cluster.
    if (observed_committed < ic->shadow_count) {
        for (int k = observed_committed; k < ic->shadow_count; k++) {
            int holders = 0;
            int num_dead = 0;

            for (int i = 0; i < num_nodes; i++) {
                if (nodes[i] == NULL) {
                    num_dead++;
                    continue;
                }
                if (wal_entry_count(&nodes[i]->wal) <= k)
                    continue;
                if (memcmp(&wal_peek_entry(&nodes[i]->wal, k)->oper, &ic->shadow_log[k], sizeof(KVStoreOper)) == 0)
                    holders++;
            }

            if (holders + num_dead <= num_nodes / 2) {
                char oper_buf[128];
                kvstore_snprint_oper(oper_buf, sizeof(oper_buf), ic->shadow_log[k]);
                fprintf(stderr, "INVARIANT VIOLATED: previously committed entry at index %d "
                    "no longer held by majority (holders=%d, dead=%d, total=%d)\n"
                    "  entry: %s\n",
                    k, holders, num_dead, num_nodes, oper_buf);
                __builtin_trap();
            }
        }
    }
}
