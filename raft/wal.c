#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "wal.h"

// FNV-1a checksum over all WALEntry fields except the checksum itself.
uint32_t wal_entry_checksum(WALEntry *entry)
{
    uint32_t h = 2166136261u;
    const unsigned char *p = (const unsigned char *)entry;
    // Hash all bytes up to (but not including) the checksum field
    size_t len = offsetof(WALEntry, checksum);
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

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

    // Discard any partial trailing entry (crash during write).
    int raw_count = size / sizeof(WALEntry);
    size_t valid_size = raw_count * sizeof(WALEntry);
    if (valid_size < size) {
        file_truncate(handle, valid_size);
    }

    WALEntry *entries = malloc(raw_count * sizeof(WALEntry));
    if (entries == NULL && raw_count > 0) {
        file_close(handle);
        return -1;
    }

    if (file_set_offset(handle, 0) < 0) {
        file_close(handle);
        free(entries);
        return -1;
    }

    if (raw_count > 0 && file_read_exact(handle, (char*) entries, raw_count * sizeof(WALEntry)) < 0) {
        file_close(handle);
        free(entries);
        return -1;
    }

    // Verify checksums: truncate at the first corrupted entry.
    // All entries from a corrupted one onward are discarded.
    int count = raw_count;
    for (int i = 0; i < raw_count; i++) {
        if (entries[i].checksum != wal_entry_checksum(&entries[i])) {
            count = i;
            file_truncate(handle, count * sizeof(WALEntry));
            break;
        }
    }

    wal->count = count;
    wal->capacity = raw_count; // capacity >= count
    wal->entries = entries;
    wal->handle = handle;
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

    // Compute checksum before writing to disk
    entry->checksum = wal_entry_checksum(entry);

    if (file_write_exact(wal->handle, (char*) entry, sizeof(*entry)) < 0) {
        // A partial write may have advanced the file offset. Truncate
        // and rewind so the file stays consistent with the in-memory
        // entry count.
        file_truncate(wal->handle, wal->count * sizeof(WALEntry));
        file_set_offset(wal->handle, wal->count * sizeof(WALEntry));
        return -1;
    }

    if (file_sync(wal->handle) < 0) {
        // The write succeeded but sync failed. Rewind the offset and
        // truncate the phantom entry so the next append writes to the
        // correct position.
        file_truncate(wal->handle, wal->count * sizeof(WALEntry));
        file_set_offset(wal->handle, wal->count * sizeof(WALEntry));
        return -1;
    }

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
