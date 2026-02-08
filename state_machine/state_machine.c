#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <assert.h>

#include "state_machine.h"

void state_machine_init(StateMachine *sm)
{
    (void)sm;
}

void state_machine_free(StateMachine *sm)
{
    (void)sm;
}

OperationResult state_machine_update(StateMachine *sm, Operation op)
{
    (void)sm;
    (void)op;
    return OPERATION_RESULT_X;
}
