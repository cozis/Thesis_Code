#ifndef STATE_MACHINE_INCLUDED
#define STATE_MACHINE_INCLUDED

typedef enum {
    OPERATION_NOOP,
    OPERATION_A,
    OPERATION_B,
    OPERATION_C,
} Operation;

typedef enum {
    OPERATION_RESULT_X,
    OPERATION_RESULT_Y,
    OPERATION_RESULT_Z,
} OperationResult;

typedef struct {

} StateMachine;

void state_machine_init(StateMachine *sm);
void state_machine_free(StateMachine *sm);

OperationResult state_machine_update(StateMachine *sm, Operation op);

#endif // STATE_MACHINE_INCLUDED