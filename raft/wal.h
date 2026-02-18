#ifndef WAL_INCLUDED
#define WAL_INCLUDED

#include <lib/file_system.h>

#include <state_machine/kvstore.h>

typedef struct {
    uint64_t    term;
    uint64_t    client_id;
    uint64_t    request_id;
    KVStoreOper oper;
    uint32_t    checksum;
} WALEntry;

uint32_t wal_entry_checksum(WALEntry *entry);

typedef struct {
    int       count;
    int       capacity;
    WALEntry *entries;
    Handle    handle;
} WAL;

typedef struct {
    int count;
    int current;
    WALEntry *entries;
} WALReplay;

int       wal_init(WAL *wal, string file);
void      wal_free(WAL *wal);
int       wal_append(WAL *wal, WALEntry *entry);
int       wal_truncate(WAL *wal, int new_count);
uint64_t  wal_last_term(WAL *wal);
int       wal_entry_count(WAL *wal);
WALEntry *wal_peek_entry(WAL *wal, int idx);
void      wal_replay_init(WALReplay *wal_replay, WAL *wal);
void      wal_replay_free(WALReplay *wal_replay);
WALEntry *wal_replay_next(WALReplay *wal_replay);

#endif // WAL_INCLUDED