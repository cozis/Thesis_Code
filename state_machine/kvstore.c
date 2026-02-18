#if defined(MAIN_SIMULATION) || defined(MAIN_TEST)
#define QUAKEY_ENABLE_MOCKS
#endif

#include <quakey.h>
#include <assert.h>
#include <stdio.h>

#include "kvstore.h"

void kvstore_init(KVStore *kvs)
{
    memset(kvs, 0, sizeof(*kvs));
}

void kvstore_free(KVStore *kvs)
{
    (void) kvs;
}

static int find_entry(KVStore *kvs, char *key)
{
    for (int i = 0; i < kvs->count; i++)
        if (!memcmp(kvs->entries[i].key, key, KVSTORE_KEY_SIZE))
            return i;
    return -1;
}

KVStoreResult kvstore_update(KVStore *kvs, KVStoreOper oper)
{
    KVStoreResult result;
    switch (oper.type) {
    case KVSTORE_OPER_NOOP:
        result.type = KVSTORE_RESULT_OK;
        result.val = 0;
        break;
    case KVSTORE_OPER_SET:
        {
            int i = find_entry(kvs, oper.key);
            if (i < 0) {
                if (kvs->count >= KVSTORE_ENTRY_LIMIT) {
                    result.type = KVSTORE_RESULT_FULL;
                    result.val = 0;
                    break;
                }
                i = kvs->count++;
                memcpy(kvs->entries[i].key, oper.key, KVSTORE_KEY_SIZE);
            }
            kvs->entries[i].val = oper.val;

            result.type = KVSTORE_RESULT_OK;
            result.val = 0;
        }
        break;
    case KVSTORE_OPER_GET:
        {
            int i = find_entry(kvs, oper.key);
            if (i < 0) {
                result.type = KVSTORE_RESULT_MISSING;
                result.val = 0;
            } else {
                result.type = KVSTORE_RESULT_OK;
                result.val  = kvs->entries[i].val;
            }
        }
        break;
    case KVSTORE_OPER_DEL:
        {
            int i = find_entry(kvs, oper.key);
            if (i < 0) {
                result.type = KVSTORE_RESULT_MISSING;
                result.val = 0;
            } else {
                result.type = KVSTORE_RESULT_OK;
                result.val  = kvs->entries[i].val;
                kvs->entries[i] = kvs->entries[--kvs->count];
            }
        }
        break;
    default:
        assert(0);
        break;
    }

    return result;
}

void kvstore_dump_oper(KVStoreOper oper)
{
    switch (oper.type) {
        case KVSTORE_OPER_NOOP: printf("NOOP"); break;
        case KVSTORE_OPER_SET: printf("SET(%.16s, %lu)", oper.key, oper.val); break;
        case KVSTORE_OPER_GET: printf("GET(%.16s)", oper.key); break;
        case KVSTORE_OPER_DEL: printf("DEL(%.16s)", oper.key); break;
        default: printf("???\n");
    }
}

int kvstore_snprint_oper(char *buf, int size, KVStoreOper oper)
{
    switch (oper.type) {
    case KVSTORE_OPER_NOOP: return snprintf(buf, size, "NOOP");
    case KVSTORE_OPER_SET:  return snprintf(buf, size, "SET(%.16s, %lu)", oper.key, oper.val);
    case KVSTORE_OPER_GET:  return snprintf(buf, size, "GET(%.16s)", oper.key);
    case KVSTORE_OPER_DEL:  return snprintf(buf, size, "DEL(%.16s)", oper.key);
    default:                return snprintf(buf, size, "???");
    }
}

int kvstore_snprint_result(char *buf, int size, KVStoreResult result)
{
    switch (result.type) {
    case KVSTORE_RESULT_OK:      return snprintf(buf, size, "OK(%lu)", result.val);
    case KVSTORE_RESULT_FULL:    return snprintf(buf, size, "FULL");
    case KVSTORE_RESULT_MISSING: return snprintf(buf, size, "MISSING");
    default:                     return snprintf(buf, size, "???");
    }
}
