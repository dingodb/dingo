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

package io.dingodb.store.api.transaction.data.resolvelock;

import io.dingodb.store.api.transaction.data.IsolationLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
public class TxnResolveLock {
    private IsolationLevel isolationLevel;
    private long startTs;
    // `commit_ts == 0` means the transaction was rolled back.
    // `commit_ts > 0` means the transaction was committed at the given timestamp oracle.
    private long commitTs;
    // Only resolve specified keys.
    private List<byte[]> keys;
}
