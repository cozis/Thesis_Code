#ifndef KVSTORE_INCLUDED
#define KVSTORE_INCLUDED

#include <stdint.h>

#define KVSTORE_KEY_SIZE 16
#define KVSTORE_ENTRY_LIMIT 64

typedef enum {
    KVSTORE_OPER_NOOP,
    KVSTORE_OPER_SET,
    KVSTORE_OPER_GET,
    KVSTORE_OPER_DEL,
} KVStoreOperType;

typedef enum {
    KVSTORE_RESULT_OK,
    KVSTORE_RESULT_FULL,
    KVSTORE_RESULT_MISSING,
} KVStoreResultType;

typedef struct {
    KVStoreOperType type;
    char key[KVSTORE_KEY_SIZE];
    uint64_t val;
} KVStoreOper;

typedef struct {
    KVStoreResultType type;
    uint64_t val;
} KVStoreResult;

typedef struct {
    char     key[KVSTORE_KEY_SIZE];
    uint64_t val;
} KVEntry;

typedef struct {
    int count;
    KVEntry entries[KVSTORE_ENTRY_LIMIT];
} KVStore;

void          kvstore_init(KVStore *kvs);
void          kvstore_free(KVStore *kvs);
KVStoreResult kvstore_update(KVStore *kvs, KVStoreOper oper);

void kvstore_dump_oper(KVStoreOper oper);
int  kvstore_snprint_oper(char *buf, int size, KVStoreOper oper);
int  kvstore_snprint_result(char *buf, int size, KVStoreResult result);

#endif // KVSTORE_INCLUDED
