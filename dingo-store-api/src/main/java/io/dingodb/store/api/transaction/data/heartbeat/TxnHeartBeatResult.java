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

package io.dingodb.store.api.transaction.data.heartbeat;

import io.dingodb.store.api.transaction.data.TxnResultInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TxnHeartBeatResult {
    // the txn_result is one of the following:
    // 1. PrimaryMismatch: Heartbeat is sent to a lock that's not the primary.
    // 2. TxnNotFound: Txn not found when heartbeat.
    // 3. otherwise, txn_result is empty
    private TxnResultInfo txnResult;
    // The TTL actually set on the requested lock.
    private long lockTtl;
}
