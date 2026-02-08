#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <assert.h>
#include <stdlib.h>

#include "wal.h"

int wal_init(WAL *wal, string file)
{
    Handle handle;
    if (file_open(file, &handle) < 0)
        return -1;
    wal->handle = handle;

    size_t size;
    if (file_size(handle, &size) < 0) {
        file_close(handle);
        return -1;
    }

    if (size % sizeof(WALEntry) != 0) {
        file_close(handle);
        return -1;
    }
    int count = size / sizeof(WALEntry);

    wal->count = count;
    wal->capacity = count;
    wal->entries = malloc(count * sizeof(WALEntry));
    if (wal->entries == NULL) {
        file_close(handle);
        return -1;
    }

    if (file_read_exact(handle, (char*) wal->entries, count * sizeof(WALEntry)) < 0) {
        file_close(handle);
        return -1;
    }

    return 0;
}

void wal_free(WAL *wal)
{
    free(wal->entries);
    file_close(wal->handle);
}

int wal_append(WAL *wal, WALEntry *entry)
{
    if (wal->count == wal->capacity) {
        int n = 2 * wal->capacity;
        if (n < 8) n = 8;
        void *p = realloc(wal->entries, n * sizeof(WALEntry));
        if (p == NULL)
            return -1;
        wal->capacity = n;
        wal->entries = p;
    }

    // TODO: Should truncate file on partial writes
    if (file_write_exact(wal->handle, (char*) entry, sizeof(*entry)) < 0)
        return -1;

    if (file_sync(wal->handle) < 0)
        return -1;

    wal->entries[wal->count++] = *entry;
    return 0;
}

int wal_truncate(WAL *wal, int new_count)
{
    assert(new_count <= wal->count);
    if (wal->count == new_count)
        return 0;

    if (file_truncate(wal->handle, new_count * sizeof(WALEntry)) < 0)
        return -1;

    if (file_set_offset(wal->handle, new_count * sizeof(WALEntry)) < 0)
        return -1;

    wal->count = new_count;
    return 0;
}

uint64_t wal_last_term(WAL *wal)
{
    if (wal->count == 0)
        return 0;
    return wal->entries[wal->count-1].term;
}

int wal_entry_count(WAL *wal)
{
    return wal->count;
}

WALEntry *wal_peek_entry(WAL *wal, int idx)
{
    assert(idx > -1);
    assert(idx < wal->count);
    return &wal->entries[idx];
}

void wal_replay_init(WALReplay *wal_replay, WAL *wal)
{
    wal_replay->count = wal->count;
    wal_replay->current = 0;
    wal_replay->entries = wal->entries;
}

void wal_replay_free(WALReplay *wal_replay)
{
    (void) wal_replay;
}

WALEntry *wal_replay_next(WALReplay *wal_replay)
{
    if (wal_replay->current == wal_replay->count)
        return NULL;
    return &wal_replay->entries[wal_replay->current++];
}
