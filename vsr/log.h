#ifndef LOG_INCLUDED
#define LOG_INCLUDED

#include <state_machine/state_machine.h>

#include "config.h"

typedef struct {
    Operation oper;
    uint32_t votes;
    int view_number;
    uint64_t client_id;
} LogEntry;

_Static_assert(NODE_LIMIT <= 32, "");

typedef struct {
    int count;
    int capacity;
    LogEntry *entries;
} Log;

void log_init(Log *log);
void log_free(Log *log);
int  log_append(Log *log, LogEntry entry);

#endif // LOG_INCLUDED