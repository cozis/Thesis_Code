#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>

#include "log.h"

void log_init(Log *log)
{
    log->count = 0;
    log->capacity = 0;
    log->entries = NULL;
}

void log_free(Log *log)
{
    free(log->entries);
}

int log_append(Log *log, LogEntry entry)
{
    if (log->count == log->capacity) {
        int n= 2 * log->capacity;
        if (n < 8)
            n = 8;
        LogEntry *p = realloc(log->entries, n * sizeof(LogEntry));
        if (p == NULL)
            return -1;

        log->entries = p;
        log->capacity = n;
    }

    log->entries[log->count++] = entry;
    return 0;
}