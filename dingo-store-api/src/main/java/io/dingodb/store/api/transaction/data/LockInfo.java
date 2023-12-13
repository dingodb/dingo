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
public class LockInfo {
    // the primary lock of the transaction
    private byte[] primaryKey;
    // the key of the lock
    private byte[] key;
    // the start_ts of the transaction
    private long lockTs;
    // the for_update_ts of the pessimistic lock
    private long forUpdateTs;
    // the lock ttl timestamp in milisecond
    private long lockTtl;
    // the number of keys involved in the transaction
    private long txnSize;
    // the type of the lock, it can be put, delete, lock
    private Op lockType;
    // the short value will persist to lock_info, and do not write data, commit will set it to
    // write_info.short_value
    private byte[] shortValue;
    // the extra_data executor want to store in lock
    private byte[] extraData;
}
