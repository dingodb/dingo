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

package io.dingodb.store.api.transaction.data.pessimisticlock;

import io.dingodb.store.api.transaction.data.TxnResultInfo;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
public class TxnPessimisticLockResult {
    // if there are many keys can't be locked, there will be many txn_result, each txn_result is for one key
    // the txn_result is one of the following:
    // 1. LockInfo: lock meet a lock and can't proceed, the lock is returned
    // 2. WriteConflict: Write conflict with key which is already written after for_update_ts
    private List<TxnResultInfo> txnResult;
}
