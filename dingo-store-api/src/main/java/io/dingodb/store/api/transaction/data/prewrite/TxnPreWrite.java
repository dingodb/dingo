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

import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

@Getter
@Setter
@Builder
@ToString
public class TxnPreWrite {
    @Builder.Default
    private IsolationLevel isolationLevel = IsolationLevel.ReadCommitted;
    // The data to be written to the database.
    private List<Mutation> mutations;
    // The primary lock of the transaction is setup by client
    private byte[] primaryLock;
    // Identifies the transaction being written.
    private long startTs;
    // the lock's ttl is timestamp in milisecond.
    @Builder.Default
    private long lockTtl = 1000L;
    // the number of keys involved in the transaction
    private long txnSize;
    // When the transaction involves only one region, it's possible to commit the
    // transaction directly with 1PC protocol.
    @Builder.Default
    boolean tryOnePc = false;  // NOT IMPLEMENTED
    // The max commit ts is reserved for limiting the commit ts of 1PC, which can be used to avoid inconsistency with
    // schema change. This field is unused now.
    @Builder.Default
    private long maxCommitTs = 0L;  // NOT IMPLEMENTED
    // for pessimistic transaction
    // check if the keys is locked by pessimistic transaction
    @Builder.Default
    List<PessimisticCheck> pessimisticChecks = Collections.emptyList();
    // fo pessimistic transaction
    // for_update_ts constriants that should be checked when prewriting a pessimistic transaction.
    @Builder.Default
    List<ForUpdateTsCheck> forUpdateTsChecks = Collections.emptyList();
    // for both pessimistic and optimistic transaction
    // the extra_data executor want to store in lock
    @Builder.Default
    List<LockExtraData> lockExtraDatas = Collections.emptyList();
}
