#include "server.h"
#include "sds.h"
#include "pmem.h"
#include "obj.h"
#include "libpmemobj.h"
#include "util.h"
#include <math.h> /* isnan(), isinf() */


/*-----------------------------------------------------------------------------
 * Check Persistent Buffer Command
 *----------------------------------------------------------------------------*/
void getDramStatusCommand(client *c) {
    long long used_dram_memory = (long long) zmalloc_used_memory();
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;
    char str_buf[1024];

    dictIterator *di;
    dictEntry *de;

    addReplyBulkCString(c, "used dram memory:");
    numreplies++;
    addReplyBulkLongLong(c, used_dram_memory);
    numreplies++;

    addReplyBulkCString(c, "dram entries:");
    numreplies++;
    addReplyBulkLongLong(c, dictSize(c->db->dict));
    numreplies++;

    di = dictGetSafeIterator(c->db->dict);
    while ((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        robj *keyobj;
        robj *valobj = dictGetVal(de);

        keyobj = createStringObject(sdsdup(key), sdslen(key));
        if (expireIfNeeded(c->db, keyobj) == 0) {
            sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) valobj->ptr);
            addReplyBulkCString(c, str_buf);
            numreplies++;
        }
        decrRefCount(keyobj);
    }

    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c, replylen, numreplies);
}

void getPBListStatusCommand(client *c) {
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;
    char str_buf[1024];
    TOID(struct persistent_aof_log) log_toid;
    struct persistent_aof_log *log_obj;
    void *cmd;
    void *pmem_base_addr;

    pmem_base_addr = (void *)server.pm_pool->addr;
    log_toid.oid = getCurrentHead();
    if (TOID_IS_NULL(log_toid)) {
        setDeferredMultiBulkLength(c, replylen, numreplies);
        return;
    }
    while (1) {
        log_obj = (persistent_aof_log *)(
                log_toid.oid.off + (uint64_t) pmem_base_addr
        );
        cmd = (void *)(log_obj->cmd_oid.off + (uint64_t) pmem_base_addr);

        sprintf(str_buf, "cmd: %s", (sds) cmd);
        addReplyBulkCString(c, str_buf);
        numreplies++;

        log_toid = D_RO(log_toid)->next;
        if (TOID_IS_NULL(log_toid))
            break;
    }

    setDeferredMultiBulkLength(c, replylen, numreplies);
}

void addPBListCommand(client *c) {
    void* cmd_oid;
    int error = 0;

    TX_BEGIN(server.pm_pool) {
        sds cmd = sdsdupPM(c->argv[1]->ptr, &cmd_oid);
        pmemAddToPBList(cmd);
    } TX_ONABORT {
        error = 1;
    } TX_END

    if (error) {
        addReplyError(c, "Add PB List in PM failed!");
        return;
    }
    addReply(c, shared.ok);
}

void switchPBListCommand(client *c) {
    int error = 0;

    TX_BEGIN(server.pm_pool) {
        pmemSwitchDoubleBuffer();
    } TX_ONABORT {
        error = 1;
    } TX_END

    if (error) {
        addReplyError(c, "Switch PB in PM failed!");
        return;
    }
    addReply(c, shared.ok);
}

void clearCurrentPBListCommand(client *c) {
    int error = 0;

    TX_BEGIN(server.pm_pool) {
        pmemClearPBList(getCurrentHead());
    } TX_ONABORT {
        error = 1;
    } TX_END

    if (error) {
        addReplyError(c, "Clear current PB in PM failed!");
        return;
    }
    addReply(c, shared.ok);
}
/*void getReverseListPmemStatusCommand(client *c) {*/
    /*void *replylen = addDeferredMultiBulkLength(c);*/
    /*unsigned long numreplies = 0;*/
    /*char str_buf[1024];*/
    /*TOID(struct redis_pmem_root) root;*/
    /*TOID(struct key_val_pair_PM) node_toid;*/
    /*struct key_val_pair_PM *node_obj;*/
    /*void *key;*/
    /*void *val;*/
    /*void *pmem_base_addr;*/

    /*root = server.pm_rootoid;*/
    /*pmem_base_addr = (void *)server.pm_pool->addr;*/
    /*node_toid = D_RO(root)->pe_last;*/
    /*if (TOID_IS_NULL(node_toid)) {*/
        /*setDeferredMultiBulkLength(c, replylen, numreplies);*/
        /*return;*/
    /*}*/
    /*while (1) {*/
        /*node_obj = (key_val_pair_PM *)(*/
                /*node_toid.oid.off + (uint64_t) pmem_base_addr*/
        /*);*/
        /*key = (void *)(node_obj->key_oid.off + (uint64_t) pmem_base_addr);*/
        /*val = (void *)(node_obj->val_oid.off + (uint64_t) pmem_base_addr);*/

        /*sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) val);*/
        /*addReplyBulkCString(c, str_buf);*/
        /*numreplies++;*/

        /*if (TOID_EQUALS(node_toid, D_RO(root)->pe_first))*/
            /*break;*/

        /*node_toid = D_RO(node_toid)->pmem_list_prev;*/
    /*}*/

    /*setDeferredMultiBulkLength(c, replylen, numreplies);*/
/*}*/

/*void getListVictimStatusCommand(client *c) {*/
    /*void *replylen = addDeferredMultiBulkLength(c);*/
    /*unsigned long numreplies = 0;*/
    /*char str_buf[1024];*/
    /*TOID(struct redis_pmem_root) root;*/
    /*TOID(struct key_val_pair_PM) victim_toid;*/
    /*struct key_val_pair_PM *victim_obj;*/
    /*void *key;*/
    /*void *val;*/
    /*void *pmem_base_addr;*/

    /*root = server.pm_rootoid;*/
    /*pmem_base_addr = (void *)server.pm_pool->addr;*/
    /*for (victim_toid = D_RO(root)->victim_first;*/
        /*TOID_IS_NULL(victim_toid) == 0;*/
        /*victim_toid = D_RO(victim_toid)->pmem_list_next*/
    /*) {*/
        /*victim_obj = (key_val_pair_PM *)(victim_toid.oid.off + (uint64_t) pmem_base_addr);*/
        /*key = (void *)(victim_obj->key_oid.off + (uint64_t) pmem_base_addr);*/
        /*val = (void *)(victim_obj->val_oid.off + (uint64_t) pmem_base_addr);*/

        /*sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) val);*/
        /*addReplyBulkCString(c, str_buf);*/
        /*numreplies++;*/
    /*}*/

    /*setDeferredMultiBulkLength(c, replylen, numreplies);*/
/*}*/
