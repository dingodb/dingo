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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.Getter;

import java.util.NavigableMap;

@Getter
public abstract class TxnPartModifyParam extends PartModifyParam {
    @JsonProperty("pessimisticTxn")
    protected final boolean pessimisticTxn;
    @JsonProperty("isolationLevel")
    protected final int isolationLevel;
    @JsonProperty("primaryLockKey")
    private final byte[] primaryLockKey;
    @JsonProperty("startTs")
    private final long startTs;
    @JsonProperty("forUpdateTs")
    private final long forUpdateTs;
    @JsonProperty("lockTimeOut")
    private final long lockTimeOut;

    public TxnPartModifyParam(
        CommonId tableId,
        CommonId partId,
        DingoType schema,
        TupleMapping keyMapping,
        TableDefinition tableDefinition,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions,
        boolean pessimisticTxn,
        int isolationLevel,
        byte[] primaryLockKey,
        long startTs,
        long forUpdateTs,
        long lockTimeOut
    ) {
        super(tableId, partId, schema, keyMapping, tableDefinition, distributions);
        this.isolationLevel = isolationLevel;
        this.pessimisticTxn = pessimisticTxn;
        this.primaryLockKey = primaryLockKey;
        this.startTs = startTs;
        this.forUpdateTs = forUpdateTs;
        this.lockTimeOut = lockTimeOut;
    }
}
