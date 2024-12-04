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

package io.dingodb.exec.transaction.base;

import io.dingodb.common.CommonId;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@Getter
@Setter
@ToString
public class TwoPhaseCommitData {
    private final CommonId txnId;

    private final byte[] primaryKey;

    private final boolean isPessimistic;

    private final int isolationLevel;
    @Builder.Default
    private final long commitTs = 0L;

    private final TransactionType type;
    @Builder.Default
    private final long lockTimeOut = 50 * 1000L;

    public boolean isPessimistic() {
        return type == TransactionType.PESSIMISTIC;
    }
}
