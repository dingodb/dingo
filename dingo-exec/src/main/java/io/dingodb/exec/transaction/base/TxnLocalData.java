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
import io.dingodb.store.api.transaction.data.Op;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class TxnLocalData {
    private final CommonId.CommonType dataType;
    private final CommonId txnId;
    private final CommonId tableId;
    private final CommonId partId;
    private final CommonId jobId;
    private final Op op;
    private final byte[] key;
    private final byte[] value;
    private final long forUpdateTs;
}
