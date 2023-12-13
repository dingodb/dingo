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

package io.dingodb.store.api.transaction.data.commit;

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
public class TxnCommitResult {

    // the txn_result is one of the following:
    // 1. LockInfo: Commit meets a lock and can't proceed, the lock is returned
    // 2. TxnNotFound: if committing a key but not found its lock, the lock may be cleaned up by gc or resolved by. If
    // committing primary key meet this error, the transaction may be rolled back. If committing secondary key meet this,
    // Executor can think its lock is resolved, and continue to commit.
    // 3. WriteConflict: SelfRolledBack: the transaction itself has been rolled back when it tries to commit.
    // 4. otherwise, txn_result is empty
    private TxnResultInfo txnResult;
    // The commit_ts of the transaction.
    private long commitTs;
}
