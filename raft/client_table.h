#ifndef CLIENT_TABLE_INCLUDED
#define CLIENT_TABLE_INCLUDED

#include <stdint.h>
#include <stdbool.h>

#include <state_machine/kvstore.h>

typedef struct {
    uint64_t        client_id;
    uint64_t        last_request_id;
    KVStoreResult   last_result;
    bool            pending;
    int             conn_tag;
} ClientTableEntry;

typedef struct {
    int count;
    int capacity;
    ClientTableEntry *entries;
} ClientTable;

void client_table_init(ClientTable *ct);
void client_table_free(ClientTable *ct);
ClientTableEntry *client_table_find(ClientTable *ct, uint64_t client_id);
int client_table_add(ClientTable *ct, uint64_t client_id, uint64_t request_id, int conn_tag);

#endif // CLIENT_TABLE_INCLUDED