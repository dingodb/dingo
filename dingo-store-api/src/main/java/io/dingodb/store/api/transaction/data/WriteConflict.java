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

package io.dingodb.store.api.transaction.data;

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
public class WriteConflict {
    public enum Reason{
        Unknown, Optimistic, PessimisticRetry, SelfRolledBack, RcCheckTs
    }
    private Reason reason;
    private long start_ts;
    // the lock_ts conflicted with start_ts
    private long conflict_ts;
    // the commit_ts of the transaction which meets read conflict
    private long conflict_commit_ts;
    private byte[] key;
    // the conflict lock's primary key
    private byte[] primary_key;
}
