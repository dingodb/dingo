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

import io.dingodb.store.api.transaction.data.IsolationLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
public class TxnCommit {
    private IsolationLevel isolationLevel;
    // The start_ts of the transaction.
    private long startTs;
    // The commit_ts of transaction. Must be greater than `start_ts`.
    private long commitTs;
    // All keys in the transaction to be committed.
    private List<byte[]> keys;
}
