/*
 * Copyright (c) 2017, Andreas Bluemle <andreas dot bluemle at itxperts dot de>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifdef USE_PB
#include "server.h"
#include "obj.h"
#include "libpmemobj.h"
#include "util.h"

int pmemReconstructPB(void) {
    loadAppendOnlyPersistentBuffer();
    pmemClearPBList(getCurrentHead());
    pmemClearPBList(getAnotherHead());
    return C_OK;
}

PMEMoid getCurrentHead() {
    struct redis_pmem_root *root = pmemobj_direct(server.pm_rootoid.oid);
    if (root->current_head == 0) return root->first_head.oid;
    return root->second_head.oid;
}

PMEMoid getAnotherHead() {
    struct redis_pmem_root *root = pmemobj_direct(server.pm_rootoid.oid);
    if (root->current_head == 0) return root->second_head.oid;
    return root->first_head.oid;
}

void setCurrentHead(PMEMoid new_head_oid) {
    struct redis_pmem_root *root = pmemobj_direct(server.pm_rootoid.oid);
    TX_ADD_DIRECT(root);
    if (root->current_head == 0) {
        root->first_head.oid = new_head_oid;
    } else {
        root->second_head.oid = new_head_oid;
    }
}

void setAnotherHead(PMEMoid new_head_oid) {
    struct redis_pmem_root *root = pmemobj_direct(server.pm_rootoid.oid);
    TX_ADD_DIRECT(root);
    if (root->current_head == 0) {
        root->second_head.oid = new_head_oid;
    } else {
        root->first_head.oid = new_head_oid;
    }
}
PMEMoid pmemAddToPBList(void *cmd) {
    // TODO(totoro): Implement Add persistent_aof_log logics.
    PMEMoid cmd_oid;
    PMEMoid paof_log_oid;
    struct persistent_aof_log *paof_log_ptr;
    TOID(struct persistent_aof_log) paof_log_toid;
    struct redis_pmem_root *root;

    cmd_oid.pool_uuid_lo = server.pool_uuid_lo;
    cmd_oid.off = (uint64_t)cmd - (uint64_t)server.pm_pool->addr;

    paof_log_oid = pmemobj_tx_zalloc(sizeof(struct persistent_aof_log), pm_type_persistent_aof_log);
    paof_log_ptr = (struct persistent_aof_log *) pmemobj_direct(paof_log_oid);
    paof_log_ptr->cmd_oid = cmd_oid;
    paof_log_toid.oid = paof_log_oid;

    root = pmemobj_direct(server.pm_rootoid.oid);

    TOID(struct persistent_aof_log) head;
    head.oid = getCurrentHead();

    paof_log_ptr->next = head;
    if(!TOID_IS_NULL(head)) {
        struct persistent_aof_log *head_ptr = D_RW(head);
        TX_ADD_FIELD_DIRECT(head_ptr, prev);
    	head_ptr->prev = paof_log_toid;
    }

    setCurrentHead(paof_log_toid.oid);
    TX_ADD_DIRECT(root);
    root->num_logs++;

    return paof_log_oid;
}

void pmemSwitchDoubleBuffer() {
    struct redis_pmem_root *root = pmemobj_direct(server.pm_rootoid.oid);
    TX_ADD_DIRECT(root);
    root->current_head = !root->current_head;
}

void pmemClearPBList(PMEMoid head) {
    struct redis_pmem_root *root = pmemobj_direct(server.pm_rootoid.oid);
    TOID(struct persistent_aof_log) log_toid;
    int freed = 0;
    log_toid.oid = head;

    void *pmem_base_addr = (void *)server.pm_pool->addr;
    while (1) {
        if (TOID_IS_NULL(log_toid))
            break;
        TOID(struct persistent_aof_log) next_toid = D_RO(log_toid)->next;
        struct persistent_aof_log *log_obj = (persistent_aof_log *)(
                log_toid.oid.off + (uint64_t) pmem_base_addr
        );
        sds cmd = (sds)(log_obj->cmd_oid.off + (uint64_t) pmem_base_addr);
        sdsfreePM(cmd);
        TX_FREE(log_toid);
        log_toid = next_toid;
        freed++;
    }
    if (OID_EQUALS(head, getCurrentHead())) {
        setCurrentHead(OID_NULL);
    } else {
        setAnotherHead(OID_NULL);
    }
    TX_ADD_DIRECT(root);
    root->num_logs -= freed;
}

#endif

