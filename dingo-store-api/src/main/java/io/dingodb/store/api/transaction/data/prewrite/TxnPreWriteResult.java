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

package io.dingodb.store.api.transaction.data.prewrite;

import io.dingodb.store.api.transaction.data.TxnResultInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
public class TxnPreWriteResult {
    // for prewrite, txn_result will be one of the following:
    // 1. LockInfo: prewrite meet a lock and can't proceed, the lock is returned
    // 2. WriteConflict: Write conflict with key which is already written after start_ts
    //    2.1 Optimistic: in optimistic transactions.
    //    2.2 SelfRolledBack: the transaction itself has been rolled back when it tries to prewrite.
    // 3. otherwise, txn_result is empty
    // for success prewrite, txn_result is empty
    // for failure prewrite, txn_result is not empty
    // if there is a WriteConflict in txn_result, client should backoff or cleanup the lock then retry
    private List<TxnResultInfo> txnResult;
    // if there is PutIfAbsent in mutation, and if there is key conflict, the conflict key will be returned
    private List<AlreadyExist> keysAlreadyExist;
    // When the transaction is successfully committed with 1PC protocol, this
    // field will be set to the commit ts of the transaction. Otherwise, if dingo-store
    // failed to commit it with 1PC or the transaction is not 1PC, the value will be 0.
    private long one_pc_commit_ts;  // NOT IMPLEMENTED
}

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
class AlreadyExist {
    private byte[] key;
}
