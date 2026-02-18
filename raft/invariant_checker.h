#ifndef INVARIANT_CHECKER_INCLUDED
#define INVARIANT_CHECKER_INCLUDED

#include "node.h"

typedef struct {
    // External shadow log of committed operations (unbounded, dynamically allocated)
    KVStoreOper *shadow_log;
    int shadow_count;
    int shadow_capacity;
} InvariantChecker;

void invariant_checker_init(InvariantChecker *ic);
void invariant_checker_free(InvariantChecker *ic);
void invariant_checker_run(InvariantChecker *ic, NodeState **nodes, int num_nodes);

#endif // INVARIANT_CHECKER_INCLUDED