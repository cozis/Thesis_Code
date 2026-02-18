#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>

#include "client_table.h"

void client_table_init(ClientTable *ct)
{
    ct->count = 0;
    ct->capacity = 0;
    ct->entries = NULL;
}

void client_table_free(ClientTable *ct)
{
    free(ct->entries);
}

ClientTableEntry *client_table_find(ClientTable *ct, uint64_t client_id)
{
    for (int i = 0; i < ct->count; i++)
        if (ct->entries[i].client_id == client_id)
            return &ct->entries[i];
    return NULL;
}

int client_table_add(ClientTable *ct, uint64_t client_id, uint64_t request_id, int conn_tag)
{
    if (ct->count == ct->capacity) {
        int n = ct->capacity ? 2 * ct->capacity : 8;
        void *p = realloc(ct->entries, n * sizeof(ClientTableEntry));
        if (p == NULL) return -1;
        ct->capacity = n;
        ct->entries = p;
    }
    ct->entries[ct->count++] = (ClientTableEntry) {
        .client_id = client_id,
        .last_request_id = request_id,
        .pending = true,
        .conn_tag = conn_tag,
    };
    return 0;
}
