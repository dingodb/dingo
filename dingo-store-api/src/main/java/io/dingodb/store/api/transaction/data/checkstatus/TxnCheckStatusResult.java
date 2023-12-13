/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.store.api.transaction.data.checkstatus;

import io.dingodb.store.api.transaction.data.LockInfo;
import io.dingodb.store.api.transaction.data.TxnResultInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TxnCheckStatusResult {
    // the txn_result is one of the following:
    // 1. PrimaryMismatch: CheckTxnStatus is sent to a lock that's not the primary.
    // 2. TxnNotFound: Txn not found when checking txn status.
    // 3. otherwise, txn_result is empty
    private TxnResultInfo txnResultInfo;
    // Three kinds of transaction status:
    //   locked: lock_ttl > 0
    //   committed: commit_ts > 0
    //   rollbacked: lock_ttl = 0 && commit_ts = 0
    private long lockTtl = 3;
    // if the transaction of the lock is committed, the commit_ts is returned
    private long commitTs = 4;
    // The action performed by dingo-store (and why if the action is to rollback).
    private Action action;
    private LockInfo lockInfo;
}
