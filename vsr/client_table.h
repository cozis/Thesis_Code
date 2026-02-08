#ifndef CLIENT_TABLE_INCLUDED
#define CLIENT_TABLE_INCLUDED

#include <stdint.h>

#include <state_machine/state_machine.h>

typedef struct {
    uint64_t        client_id;
    uint64_t        last_request_id;
    OperationResult last_result;
    bool            pending;
    int             conn_idx;
} ClientTableEntry;

typedef struct {
    int count;
    int capacity;
    ClientTableEntry *entries;
} ClientTable;

void client_table_init(ClientTable *client_table);
void client_table_free(ClientTable *client_table);
ClientTableEntry *client_table_find(ClientTable *client_table, uint64_t client_id);
int client_table_add(ClientTable *client_table, uint64_t client_id, uint64_t request_id, int conn_idx);
int client_table_insert(ClientTable *client_table, uint64_t client_id, uint64_t request_id);

#endif // CLIENT_TABLE_INCLUDED