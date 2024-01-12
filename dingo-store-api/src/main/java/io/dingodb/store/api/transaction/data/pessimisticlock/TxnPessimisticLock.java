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

import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@Builder
@ToString
public class TxnPessimisticLock {
    @Builder.Default
    private IsolationLevel isolationLevel = IsolationLevel.ReadCommitted;
    // In this case every `Op` of the mutations must be `PessimisticLock`.
    private List<Mutation> mutations;
    private byte[] primaryLock;
    private long startTs;
    // the lock's ttl is timestamp in milisecond, it's the absolute timestamp when the lock is expired.
    private long lockTtl;
    // Each locking command in a pessimistic transaction has its own timestamp.
    // If locking fails, then the corresponding SQL statement can be retried with a later timestamp, Executor does not
    // need to retry the whole transaction.
    // The name comes from the `SELECT ... FOR UPDATE` SQL statement which is a locking read. Each `SELECT ... FOR UPDATE`
    // in a transaction will be assigned its own timestamp.
    private long forUpdateTs;
}
